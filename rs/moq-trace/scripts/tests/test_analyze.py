from __future__ import annotations

import pathlib
import sys
import unittest

import duckdb
import pyarrow as pa

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from relay_latency_lib import ctf  # noqa: E402
from relay_latency_lib.analyze import (  # noqa: E402
    DATABASE,
    SQL,
    _coverage,
    _derive_samples,
    _select_workload,
    _validate_raw,
)
from relay_latency_lib.report import build as build_report  # noqa: E402


class SqlAnalysisTests(unittest.TestCase):
    """Exercise lifecycle validation and correlation through DuckDB."""

    def setUp(self) -> None:
        self.connection = duckdb.connect(":memory:")
        for name, schema in ctf.SCHEMAS.items():
            self.connection.register("rows", pa.Table.from_batches([], schema=schema))
            self.connection.execute(f"CREATE TABLE {name} AS SELECT * FROM rows")
            self.connection.unregister("rows")
        self.connection.execute(SQL.read_text())

    def tearDown(self) -> None:
        self.connection.close()

    def insert(self, table: str, **row) -> None:
        self.connection.register(
            "rows", pa.Table.from_pylist([row], schema=ctf.SCHEMAS[table])
        )
        self.connection.execute(f"INSERT INTO {table} SELECT * FROM rows")
        self.connection.unregister("rows")

    def object_start(self, trace_id: int, direction: str, connection_id: int) -> None:
        self.insert(
            "moq_object_start",
            ctf_timestamp_ns=trace_id * 1_000,
            timestamp_ns=100_000 if direction == "rx" else 210_000,
            trace_id=trace_id,
            logical_group=7,
            logical_frame=0,
            session_id=trace_id,
            connection_id=connection_id,
            direction=direction,
            protocol="moq_transport",
            track_alias=1,
            group_id=4,
            object_id=0,
            stream_id=connection_id * 10,
            stream_offset_start=0,
            sample_rate=1,
        )
        self.insert(
            "moq_object_end",
            ctf_timestamp_ns=trace_id * 1_000 + 1,
            timestamp_ns=200_000 if direction == "rx" else 300_000,
            trace_id=trace_id,
            stream_offset_end=16,
            payload_bytes=16,
        )

    def packet(self, trace_id: int, direction: str, connection_id: int) -> None:
        self.insert(
            "quic_packet_start",
            ctf_timestamp_ns=trace_id * 1_000,
            timestamp_ns=90_000 if direction == "rx" else 220_000,
            trace_id=trace_id,
            connection_id=connection_id,
            direction=direction,
            packet_number=1,
            packet_space="data",
            byte_len=1200,
            sample_rate=1,
        )
        self.insert(
            "quic_packet_end",
            ctf_timestamp_ns=trace_id * 1_000 + 2,
            timestamp_ns=205_000 if direction == "rx" else 310_000,
            trace_id=trace_id,
            packet_number=1,
            packet_space="data",
            byte_len=1200,
            outcome="success",
        )
        self.insert(
            "quic_stream_frame",
            ctf_timestamp_ns=trace_id * 1_000 + 1,
            timestamp_ns=190_000 if direction == "rx" else 290_000,
            trace_id=trace_id,
            stream_id=connection_id * 10,
            offset_start=0,
            offset_end=16,
            outcome="success",
        )

    def phase(self, trace_id: int, phase: str, start: int, end: int) -> None:
        for index, (edge, timestamp, outcome) in enumerate(
            (("start", start, None), ("done", end, "success"))
        ):
            self.insert(
                "quic_packet_phase",
                ctf_timestamp_ns=trace_id * 1_000 + index,
                timestamp_ns=timestamp,
                trace_id=trace_id,
                span_id=trace_id * 100 + {"routing": 1, "scheduling": 2}[phase],
                phase=phase,
                edge=edge,
                outcome=outcome,
            )

    def test_derives_correlated_metrics(self) -> None:
        self.object_start(1, "rx", 1)
        self.object_start(2, "tx", 2)
        self.packet(3, "rx", 1)
        self.packet(4, "tx", 2)
        self.phase(3, "routing", 110_000, 120_000)
        self.phase(3, "scheduling", 120_000, 130_000)

        _validate_raw(self.connection)
        origin = _select_workload(self.connection, 16, 1, 0, 0)
        _coverage(self.connection)
        _derive_samples(self.connection, origin)
        report = build_report(self.connection, DATABASE)

        self.assertEqual(report["correlated_objects"], 1)
        self.assertEqual(report["correlated_object_copies"], 1)
        self.assertEqual(report["quic_object_statistics"]["quic_full_span"]["p50"], 220.0)
        self.assertEqual(len(report["timelines"]), 3)

    def test_pairs_overlapping_phase_occurrences_by_span_id(self) -> None:
        for ctf_timestamp, timestamp, span_id, edge, outcome in (
            (1, 100, 10, "start", None),
            (2, 110, 11, "start", None),
            (3, 120, 11, "done", "success"),
            (4, 130, 10, "done", "success"),
        ):
            self.insert(
                "quic_packet_phase",
                ctf_timestamp_ns=ctf_timestamp,
                timestamp_ns=timestamp,
                trace_id=1,
                span_id=span_id,
                phase="frame_process",
                edge=edge,
                outcome=outcome,
            )

        intervals = self.connection.execute(
            """SELECT span_id, start_ns, end_ns FROM packet_phase_intervals
               ORDER BY span_id"""
        ).fetchall()

        self.assertEqual(intervals, [(10, 100, 130), (11, 110, 120)])


if __name__ == "__main__":
    unittest.main()
