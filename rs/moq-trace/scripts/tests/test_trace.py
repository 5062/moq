from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from relay_latency_lib.trace import TraceError, TraceIndex  # noqa: E402


def packet_event(
    event_type: str,
    trace_id: int,
    timestamp_ns: int,
    outcome: str | None = None,
) -> dict:
    """Build one packet lifecycle event."""

    event = {
        "type": event_type,
        "timestamp_ns": timestamp_ns,
        "trace_id": trace_id,
        "connection_id": 7,
        "direction": "rx",
        "sample_rate": 1,
    }
    if outcome is not None:
        event["outcome"] = outcome
    return event


def read_trace(events: list[dict]) -> TraceIndex:
    """Write and read one temporary JSONL trace."""

    with tempfile.TemporaryDirectory() as directory:
        path = pathlib.Path(directory) / "trace.jsonl"
        path.write_text("".join(json.dumps(event) + "\n" for event in events))
        return TraceIndex.read(path)


class PacketOutcomeTests(unittest.TestCase):
    """Packet indexing behavior for completed non-success outcomes."""

    def test_dropped_packet_is_excluded(self) -> None:
        events = [
            packet_event("quic_packet_start", 1, 10),
            {
                **packet_event("quic_packet_phase", 1, 11),
                "phase": "header_unprotect",
                "edge": "start",
            },
            {
                **packet_event("quic_packet_phase", 1, 12, "dropped"),
                "phase": "header_unprotect",
                "edge": "done",
            },
            packet_event("quic_packet_end", 1, 13, "dropped"),
            packet_event("quic_packet_start", 2, 20),
            packet_event("quic_packet_end", 2, 25, "success"),
        ]

        trace = read_trace(events)

        self.assertEqual(trace.packet_index.count, 1)
        self.assertEqual(trace.packet_index.packet(2).trace_id, 2)
        self.assertEqual(trace.packet_index.samples["trace_id"].to_list(), [2])
        with self.assertRaisesRegex(TraceError, "packet index has no packet 1"):
            trace.packet_index.packet(1)

    def test_successful_stream_frame_cannot_reference_dropped_packet(self) -> None:
        events = [
            packet_event("quic_packet_start", 1, 10),
            packet_event("quic_packet_end", 1, 13, "dropped"),
            {
                **packet_event("quic_stream_frame", 1, 12, "success"),
                "stream_id": 4,
                "offset_start": 0,
                "offset_end": 10,
            },
        ]

        with self.assertRaisesRegex(
            TraceError,
            "successful STREAM frame references unsuccessful packet 1",
        ):
            read_trace(events)


if __name__ == "__main__":
    unittest.main()
