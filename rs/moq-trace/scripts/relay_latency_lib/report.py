"""Build aggregate statistics and representative object timelines."""

from __future__ import annotations

import duckdb

from .errors import AnalyzeError


def _count(connection: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(connection.execute(query).fetchone()[0])


def _statistics(connection: duckdb.DuckDBPyConnection, table: str) -> dict:
    rows = connection.execute(
        f"""SELECT metric, count(*) AS count, avg(latency_us) AS mean,
                   quantile_cont(latency_us, 0.50) AS p50,
                   quantile_cont(latency_us, 0.95) AS p95,
                   quantile_cont(latency_us, 0.99) AS p99,
                   max(latency_us) AS max
            FROM {table} GROUP BY metric ORDER BY metric"""
    ).fetchall()
    if not rows:
        raise AnalyzeError(f"cannot summarize empty {table}")
    return {
        metric: {
            "count": int(count),
            "mean": float(mean),
            "p50": float(p50),
            "p95": float(p95),
            "p99": float(p99),
            "max": float(maximum),
        }
        for metric, count, mean, p50, p95, p99, maximum in rows
    }


def _intervals(
    connection: duckdb.DuckDBPyConnection,
    trace_id: int,
    direction: str,
    session_id: int,
    origin: int,
) -> list[dict]:
    lifecycle = connection.execute(
        "SELECT start_ns, end_ns FROM object_lifecycles WHERE trace_id = ?", [trace_id]
    ).fetchone()
    values = [
        {
            "direction": direction,
            "session_id": session_id,
            "phase": "object",
            "occurrence": 0,
            "start_us": (int(lifecycle[0]) - origin) / 1000.0,
            "end_us": (int(lifecycle[1]) - origin) / 1000.0,
        }
    ]
    for phase, occurrence, start, end in connection.execute(
        """SELECT phase, occurrence, start_ns, end_ns
           FROM object_phase_intervals
           WHERE trace_id = ? AND outcome = 'success'
           ORDER BY phase, occurrence""",
        [trace_id],
    ).fetchall():
        values.append(
            {
                "direction": direction,
                "session_id": session_id,
                "phase": phase,
                "occurrence": int(occurrence),
                "start_us": (int(start) - origin) / 1000.0,
                "end_us": (int(end) - origin) / 1000.0,
            }
        )
    packet_ids = connection.execute(
        "SELECT packet_ids FROM object_packet_coverage WHERE trace_id = ?", [trace_id]
    ).fetchone()[0]
    for packet_occurrence, packet_id in enumerate(packet_ids):
        start, end = connection.execute(
            "SELECT start_ns, end_ns FROM packet_lifecycles WHERE trace_id = ?", [packet_id]
        ).fetchone()
        values.append(
            {
                "direction": direction,
                "session_id": session_id,
                "phase": "quic_packet",
                "occurrence": packet_occurrence,
                "start_us": (int(start) - origin) / 1000.0,
                "end_us": (int(end) - origin) / 1000.0,
            }
        )
        for phase, occurrence, phase_start, phase_end in connection.execute(
            """SELECT phase, occurrence, start_ns, end_ns
               FROM packet_phase_intervals
               WHERE trace_id = ? AND outcome = 'success'
               ORDER BY phase, occurrence""",
            [packet_id],
        ).fetchall():
            values.append(
                {
                    "direction": direction,
                    "session_id": session_id,
                    "phase": f"quic_{phase}",
                    "occurrence": int(occurrence),
                    "start_us": (int(phase_start) - origin) / 1000.0,
                    "end_us": (int(phase_end) - origin) / 1000.0,
                }
            )
    return values


def _timelines(connection: duckdb.DuckDBPyConnection) -> list[dict]:
    connection.execute(
        """CREATE TEMP TABLE object_slowest AS
           SELECT rx.trace_id, rx.logical_group, rx.logical_frame, rx.group_id,
                  rx.object_id, rx.start_ns,
                  max((tx.end_ns - rx.start_ns) / 1000.0) AS actual_us
           FROM selected_rx AS rx
           JOIN object_lifecycles AS tx
             ON tx.logical_group = rx.logical_group
            AND tx.logical_frame = rx.logical_frame
            AND tx.direction = 'tx'
           GROUP BY ALL"""
    )
    mean, median, p99 = connection.execute(
        """SELECT avg(actual_us), quantile_cont(actual_us, 0.5),
                  quantile_cont(actual_us, 0.99) FROM object_slowest"""
    ).fetchone()
    timelines = []
    for statistic, target in (("mean", mean), ("median", median), ("p99", p99)):
        rx = connection.execute(
            """SELECT * FROM object_slowest
               ORDER BY abs(actual_us - ?), trace_id LIMIT 1""",
            [target],
        ).fetchone()
        trace_id, logical_group, logical_frame, group_id, object_id, origin, actual = rx
        rx_session = connection.execute(
            "SELECT session_id FROM object_lifecycles WHERE trace_id = ?", [trace_id]
        ).fetchone()[0]
        if rx_session is None:
            raise AnalyzeError(f"RX object trace {trace_id} is missing session_id")
        intervals = _intervals(
            connection, int(trace_id), "rx", int(rx_session), int(origin)
        )
        tx_rows = connection.execute(
            """SELECT trace_id, session_id, (end_ns - ?) / 1000.0 AS full_span_us
               FROM object_lifecycles
               WHERE logical_group = ? AND logical_frame = ? AND direction = 'tx'
               ORDER BY session_id, trace_id""",
            [origin, logical_group, logical_frame],
        ).fetchall()
        copies = []
        for ordinal, (tx_trace_id, session_id, full_span) in enumerate(tx_rows, 1):
            if session_id is None:
                raise AnalyzeError(f"TX object trace {tx_trace_id} is missing session_id")
            intervals.extend(
                _intervals(
                    connection, int(tx_trace_id), "tx", int(session_id), int(origin)
                )
            )
            copies.append(
                {
                    "session_id": int(session_id),
                    "subscriber_ordinal": ordinal,
                    "full_span_us": float(full_span),
                }
            )
        timelines.append(
            {
                "selection": {
                    "statistic": statistic,
                    "target_us": float(target),
                    "group_id": int(group_id),
                    "object_id": int(object_id),
                    "actual_us": float(actual),
                },
                "intervals": intervals,
                "first_copy": copies[0],
                "last_copy": copies[-1],
                "slowest_copy": max(copies, key=lambda copy: copy["full_span_us"]),
            }
        )
    return timelines


def build(connection: duckdb.DuckDBPyConnection, database: str) -> dict:
    """Return the compact manifest consumed by Rust and plotting."""

    packet_statistics = _statistics(connection, "packet_samples")
    for required in ("rx_routing", "rx_scheduling"):
        if required not in packet_statistics:
            raise AnalyzeError(f"Quinn trace is missing packet metric: {required}")
    return {
        "database": database,
        "statistics": _statistics(connection, "object_samples"),
        "quic_object_statistics": _statistics(connection, "quic_object_samples"),
        "packet_statistics": packet_statistics,
        "packet_count": _count(connection, "SELECT count(*) FROM packet_lifecycles"),
        "group_count": _count(connection, "SELECT count(DISTINCT group_id) FROM selected_rx"),
        "correlated_objects": _count(connection, "SELECT count(*) FROM selected_rx"),
        "correlated_object_copies": _count(
            connection,
            "SELECT count(*) FROM quic_object_samples WHERE metric = 'quic_full_span'",
        ),
        "timelines": _timelines(connection),
    }
