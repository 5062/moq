"""Build the queryable DuckDB analysis model from a moq_trace CTF trace."""

from __future__ import annotations

import json
import os
import pathlib
import tempfile

import duckdb
import pyarrow as pa

from . import ctf
from .coverage import resolve as _coverage
from .errors import TraceError
from .report import build as _build_report
from .schema import SCHEMA_REVISION

SQL = pathlib.Path(__file__).with_name("sql") / "analysis.sql"


def _count(connection: duckdb.DuckDBPyConnection, query: str, parameters=()) -> int:
    return int(connection.execute(query, parameters).fetchone()[0])


def _require_zero(connection: duckdb.DuckDBPyConnection, query: str, message: str) -> None:
    count = _count(connection, query)
    if count:
        raise TraceError(f"{message}: {count}")


def _ingest(
    connection: duckdb.DuckDBPyConnection,
    input_path: pathlib.Path,
    expected_pid: int | None,
) -> None:
    for name, schema in ctf.SCHEMAS.items():
        empty = pa.Table.from_batches([], schema=schema)
        connection.register("arrow_batch", empty)
        connection.execute(f"CREATE TABLE {name} AS SELECT * FROM arrow_batch")
        connection.unregister("arrow_batch")
    for name, batch in ctf.batches(input_path, expected_pid):
        connection.register("arrow_batch", batch)
        connection.execute(f"INSERT INTO {name} SELECT * FROM arrow_batch")
        connection.unregister("arrow_batch")


def _validate_raw(connection: duckdb.DuckDBPyConnection) -> None:
    for table in ("moq_object_start", "moq_object_end", "quic_packet_start", "quic_packet_end"):
        _require_zero(
            connection,
            f"SELECT count(*) FROM (SELECT trace_id FROM {table} GROUP BY trace_id HAVING count(*) <> 1)",
            f"{table} contains duplicate trace IDs",
        )
    _require_zero(
        connection,
        "SELECT count(*) FROM moq_object_end ANTI JOIN moq_object_start USING (trace_id)",
        "object completions without starts",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM quic_packet_end ANTI JOIN quic_packet_start USING (trace_id)",
        "packet completions without starts",
    )
    _require_zero(
        connection,
        """SELECT count(*) FROM moq_object_start AS start
           ANTI JOIN moq_object_end USING (trace_id)""",
        "object starts without completions",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM quic_packet_start ANTI JOIN quic_packet_end USING (trace_id)",
        "packet starts without completions",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM object_lifecycles WHERE end_ns < start_ns",
        "objects completing before they start",
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM packet_lifecycles WHERE end_ns < start_ns",
        "packets completing before they start",
    )
    for table, intervals in (
        ("moq_object_phase", "object_phase_intervals"),
        ("quic_packet_phase", "packet_phase_intervals"),
    ):
        boundaries = _count(connection, f"SELECT count(*) FROM {table}")
        pairs = _count(connection, f"SELECT count(*) * 2 FROM {intervals}")
        if boundaries != pairs:
            raise TraceError(f"{table} contains unmatched phase boundaries")
        _require_zero(
            connection,
            f"SELECT count(*) FROM {intervals} WHERE end_ns < start_ns",
            f"{table} contains phases completing before they start",
        )
    _require_zero(
        connection,
        """SELECT count(*) FROM object_phase_intervals AS phase
           JOIN object_lifecycles AS object USING (trace_id)
           WHERE NOT ((object.direction = 'rx' AND phase.phase IN
               ('header_parse', 'create', 'payload_read', 'frame_commit')) OR
              (object.direction = 'tx' AND phase.phase IN
               ('clone', 'header_encode', 'payload_write')))""",
        "object phases have invalid directions",
    )
    _require_zero(
        connection,
        """SELECT count(*) FROM udp_socket_start AS start
           FULL JOIN udp_socket_end AS finish USING (trace_id)
           WHERE start.trace_id IS NULL OR finish.trace_id IS NULL""",
        "socket operations are incomplete or duplicated",
    )
    for table in ("udp_socket_start", "udp_socket_end"):
        _require_zero(
            connection,
            f"SELECT count(*) FROM (SELECT trace_id FROM {table} GROUP BY trace_id HAVING count(*) <> 1)",
            f"{table} contains duplicate trace IDs",
        )
    _require_zero(
        connection,
        """SELECT count(*) FROM (
             SELECT logical_group, logical_frame,
                    count(*) FILTER (direction = 'rx') AS ingress
             FROM object_lifecycles GROUP BY ALL HAVING ingress <> 1
           )""",
        "logical objects do not have exactly one ingress lifecycle",
    )


def _select_workload(
    connection: duckdb.DuckDBPyConnection,
    object_size: int,
    subscribers: int,
    warmup_ns: int,
    cooldown_ns: int,
) -> int:
    bounds = connection.execute(
        """SELECT min(start_ns), max(start_ns)
           FROM object_lifecycles
           WHERE direction = 'rx' AND outcome = 'success' AND payload_bytes = ?""",
        [object_size],
    ).fetchone()
    if bounds[0] is None:
        raise TraceError(f"trace has no completed {object_size}-byte inbound objects")
    origin, last = map(int, bounds)
    start = min(origin + warmup_ns, 2**64 - 1)
    end = max(last - cooldown_ns, 0)
    connection.execute(
        """CREATE TABLE selected_rx AS
           SELECT * FROM object_lifecycles
           WHERE direction = 'rx' AND outcome = 'success'
             AND payload_bytes = ? AND start_ns BETWEEN ? AND ?""",
        [object_size, start, end],
    )
    if _count(connection, "SELECT count(*) FROM selected_rx") == 0:
        raise TraceError("steady-state window contains no complete objects")
    bad = _count(
        connection,
        """SELECT count(*) FROM (
             SELECT rx.trace_id, count(tx.trace_id) AS copies
             FROM selected_rx AS rx
             LEFT JOIN object_lifecycles AS tx
              ON tx.logical_group = rx.logical_group
             AND tx.logical_frame = rx.logical_frame
              AND tx.direction = 'tx' AND tx.outcome = 'success'
             GROUP BY rx.trace_id HAVING copies <> ?
           )""",
        [subscribers],
    )
    if bad:
        raise TraceError(f"{bad} steady-state objects do not have exactly {subscribers} outbound copies")
    groups = connection.execute(
        "SELECT count(DISTINCT group_id), min(group_id), max(group_id) FROM selected_rx"
    ).fetchone()
    if int(groups[0]) != int(groups[2]) - int(groups[1]) + 1:
        raise TraceError("steady-state groups are not contiguous")
    connection.execute(
        """CREATE TABLE object_samples AS
           SELECT rx.group_id, rx.object_id, 'full_span' AS metric,
                  row_number() OVER (
                    PARTITION BY rx.trace_id ORDER BY tx.session_id, tx.trace_id
                  ) - 1 AS copy_ordinal,
                  greatest(rx.start_ns::HUGEINT - ?, 0) AS elapsed_ns,
                  tx.end_ns - rx.start_ns AS latency_ns
           FROM selected_rx AS rx
           JOIN object_lifecycles AS tx
            ON tx.logical_group = rx.logical_group
           AND tx.logical_frame = rx.logical_frame
            AND tx.direction = 'tx' AND tx.outcome = 'success'""",
        [origin],
    )
    return origin


def _derive_samples(connection: duckdb.DuckDBPyConnection, origin: int) -> None:
    connection.execute(
        """CREATE TABLE quic_object_samples AS
           WITH copies AS (
             SELECT rx.group_id, rx.object_id,
                    row_number() OVER (
                      PARTITION BY rx.trace_id ORDER BY tx.session_id, tx.trace_id
                    ) - 1 AS copy_ordinal,
                    inbound.first_start_ns, inbound.first_end_ns,
                    outbound.first_end_ns AS outbound_first_end_ns,
                    inbound.complete_end_ns AS inbound_complete_end_ns,
                    outbound.complete_end_ns AS outbound_complete_end_ns
             FROM selected_rx AS rx
             JOIN object_packet_coverage AS inbound ON inbound.trace_id = rx.trace_id
             JOIN object_lifecycles AS tx
               ON tx.logical_group = rx.logical_group
              AND tx.logical_frame = rx.logical_frame
              AND tx.direction = 'tx' AND tx.outcome = 'success'
             JOIN object_packet_coverage AS outbound ON outbound.trace_id = tx.trace_id
           )
           SELECT group_id, object_id, metric, copy_ordinal,
                  greatest(first_start_ns::HUGEINT - ?, 0) AS elapsed_ns,
                  finish_ns - start_ns AS latency_ns
           FROM copies
           CROSS JOIN LATERAL (VALUES
             ('quic_forward_start', first_start_ns, outbound_first_end_ns),
             ('quic_tail_gap', inbound_complete_end_ns, outbound_complete_end_ns),
             ('quic_full_span', first_start_ns, outbound_complete_end_ns)
           ) AS metric(metric, start_ns, finish_ns)""",
        [origin],
    )
    _require_zero(
        connection,
        "SELECT count(*) FROM quic_object_samples WHERE latency_ns < 0",
        "QUIC object metrics are negative",
    )
    connection.execute(
        """CREATE TABLE packet_samples AS
           SELECT direction || '_packet_span' AS metric, direction, connection_id,
                  trace_id, 0 AS occurrence,
                  greatest(start_ns::HUGEINT - ?, 0) AS elapsed_ns,
                  end_ns - start_ns AS latency_ns
           FROM packet_lifecycles WHERE outcome = 'success'
           UNION ALL
           SELECT packet.direction || '_' || phase.phase, packet.direction,
                  packet.connection_id, packet.trace_id, phase.occurrence,
                  greatest(phase.start_ns::HUGEINT - ?, 0),
                  phase.end_ns - phase.start_ns
           FROM packet_phase_intervals AS phase
           JOIN packet_lifecycles AS packet USING (trace_id)
           WHERE phase.outcome = 'success'
           UNION ALL
           SELECT 'rx_packet_processing_span', packet.direction, packet.connection_id,
                  packet.trace_id, 0,
                  greatest(schedule.end_ns::HUGEINT - ?, 0),
                  packet.end_ns - schedule.end_ns
           FROM packet_lifecycles AS packet
           JOIN (
             SELECT trace_id, max(end_ns) AS end_ns
             FROM packet_phase_intervals
             WHERE phase = 'scheduling' AND outcome = 'success'
             GROUP BY trace_id
           ) AS schedule USING (trace_id)
           WHERE packet.direction = 'rx' AND packet.outcome = 'success'
             AND packet.end_ns >= schedule.end_ns""",
        [origin, origin, origin],
    )


def _define_metrics(connection: duckdb.DuckDBPyConnection) -> None:
    definitions = (
        ("object", "full_span", "Full relay span", 0),
        ("quic_object", "quic_forward_start", "QUIC forward start", 0),
        ("quic_object", "quic_tail_gap", "QUIC tail gap", 1),
        ("quic_object", "quic_full_span", "QUIC full span", 2),
        ("packet", "rx_packet_span", "RX packet span", 0),
        ("packet", "rx_header_parse", "RX header parse", 1),
        ("packet", "rx_routing", "RX routing", 2),
        ("packet", "rx_scheduling", "RX scheduling", 3),
        ("packet", "rx_header_unprotect", "RX header unprotect", 4),
        ("packet", "rx_payload_decrypt", "RX payload decrypt", 5),
        ("packet", "rx_frame_process", "RX frame process", 6),
        ("packet", "rx_packet_processing_span", "RX packet processing span", 7),
        ("packet", "tx_packet_span", "TX packet span", 8),
        ("packet", "tx_frame_encode", "TX frame encode", 9),
        ("packet", "tx_packet_encrypt", "TX packet encrypt", 10),
    )
    connection.execute(
        """CREATE TABLE metric_definitions(
               domain VARCHAR NOT NULL,
               metric VARCHAR PRIMARY KEY,
               label VARCHAR NOT NULL,
               display_order INTEGER NOT NULL
           )"""
    )
    connection.executemany("INSERT INTO metric_definitions VALUES (?, ?, ?, ?)", definitions)
    connection.execute(
        """CREATE VIEW latency_samples AS
           SELECT 'object' AS domain, metric, elapsed_ns, latency_ns FROM object_samples
           UNION ALL
           SELECT 'quic_object', metric, elapsed_ns, latency_ns FROM quic_object_samples
           UNION ALL
           SELECT 'packet', metric, elapsed_ns, latency_ns FROM packet_samples"""
    )


def run(
    input_path: pathlib.Path,
    output: pathlib.Path,
    *,
    object_size: int,
    subscribers: int,
    warmup_ns: int,
    cooldown_ns: int,
    expected_pid: int | None = None,
    metadata: dict | None = None,
) -> dict:
    """Analyze CTF into one atomically published DuckDB database."""

    if output.exists():
        raise TraceError(f"analysis database already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".moq-trace-analysis-", dir=output.parent) as staging_name:
        staging = pathlib.Path(staging_name)
        database = staging / output.name
        connection = duckdb.connect(str(database))
        try:
            _ingest(connection, input_path, expected_pid)
            connection.execute(SQL.read_text())
            _validate_raw(connection)
            origin = _select_workload(connection, object_size, subscribers, warmup_ns, cooldown_ns)
            _coverage(connection)
            _derive_samples(connection, origin)
            _define_metrics(connection)
            report = _build_report(connection)
            value = dict(metadata or {})
            value.setdefault(
                "workload",
                {
                    "subscribers": subscribers,
                    "object_size": object_size,
                    "warmup_seconds": warmup_ns / 1_000_000_000,
                    "cooldown_seconds": cooldown_ns / 1_000_000_000,
                },
            )
            value["counts"] = {
                "groups": report["group_count"],
                "packets": report["packet_count"],
                "correlated_objects": report["correlated_objects"],
                "correlated_object_copies": report["correlated_object_copies"],
            }
            connection.execute(
                "CREATE TABLE metadata(schema_revision INTEGER PRIMARY KEY, kind VARCHAR NOT NULL, value JSON NOT NULL)"
            )
            connection.execute(
                "INSERT INTO metadata VALUES (?, 'run', ?)",
                [SCHEMA_REVISION, json.dumps(value)],
            )
            connection.execute("CHECKPOINT")
        finally:
            connection.close()
        os.replace(database, output)
    return report
