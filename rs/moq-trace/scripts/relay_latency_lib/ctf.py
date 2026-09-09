"""Load moq_trace CTF events into typed Arrow batches."""

from __future__ import annotations

import pathlib
from collections.abc import Iterator

import bt2
import pyarrow as pa


def _schema(**fields: pa.DataType) -> pa.Schema:
    return pa.schema(tuple(fields.items()))


SCHEMAS = {
    "moq_object_start": _schema(
        ctf_timestamp_ns=pa.uint64(), timestamp_ns=pa.uint64(), trace_id=pa.uint64(),
        logical_group=pa.uint64(), logical_frame=pa.uint64(), session_id=pa.uint64(),
        connection_id=pa.uint64(), direction=pa.string(), protocol=pa.string(),
        track_alias=pa.uint64(), group_id=pa.uint64(), object_id=pa.uint64(),
        stream_id=pa.uint64(), stream_offset_start=pa.uint64(),
    ),
    "moq_object_end": _schema(
        ctf_timestamp_ns=pa.uint64(), timestamp_ns=pa.uint64(), trace_id=pa.uint64(),
        stream_offset_end=pa.uint64(), payload_bytes=pa.uint64(),
    ),
    "moq_object_phase": _schema(
        ctf_timestamp_ns=pa.uint64(), timestamp_ns=pa.uint64(), trace_id=pa.uint64(),
        span_id=pa.uint64(), phase=pa.string(), edge=pa.string(), outcome=pa.string(),
    ),
    "quic_packet_start": _schema(
        ctf_timestamp_ns=pa.uint64(), timestamp_ns=pa.uint64(), trace_id=pa.uint64(),
        connection_id=pa.uint64(), direction=pa.string(), packet_number=pa.uint64(),
        packet_space=pa.string(), byte_len=pa.uint64(),
    ),
    "quic_packet_end": _schema(
        ctf_timestamp_ns=pa.uint64(), timestamp_ns=pa.uint64(), trace_id=pa.uint64(),
        packet_number=pa.uint64(), packet_space=pa.string(), byte_len=pa.uint64(),
        outcome=pa.string(),
    ),
    "quic_packet_phase": _schema(
        ctf_timestamp_ns=pa.uint64(), timestamp_ns=pa.uint64(), trace_id=pa.uint64(),
        span_id=pa.uint64(), phase=pa.string(), edge=pa.string(), outcome=pa.string(),
    ),
    "quic_stream_frame": _schema(
        ctf_timestamp_ns=pa.uint64(), timestamp_ns=pa.uint64(), trace_id=pa.uint64(),
        stream_id=pa.uint64(), offset_start=pa.uint64(), offset_end=pa.uint64(),
        outcome=pa.string(),
    ),
    "udp_socket_start": _schema(
        ctf_timestamp_ns=pa.uint64(), timestamp_ns=pa.uint64(), trace_id=pa.uint64(),
        connection_id=pa.uint64(), direction=pa.string(),
    ),
    "udp_socket_end": _schema(
        ctf_timestamp_ns=pa.uint64(), timestamp_ns=pa.uint64(), trace_id=pa.uint64(),
        outcome=pa.string(), buffers=pa.uint64(), datagrams=pa.uint64(), bytes=pa.uint64(),
    ),
}


class CtfError(RuntimeError):
    """The CTF input is incomplete or incompatible with the analyzer schema."""


def _scalar(value):
    labels = tuple(getattr(value, "labels", ()))
    if labels:
        if len(labels) != 1:
            raise CtfError(f"ambiguous CTF enumeration labels: {labels}")
        return labels[0]
    return int(value)


def _record(message, name: str) -> dict:
    payload = message.event.payload_field
    record = {key: _scalar(payload[key]) for key in payload}
    for key in tuple(record):
        if key.startswith("has_"):
            value_key = key[4:]
            record[value_key] = record[value_key] if record.pop(key) else None
    record["ctf_timestamp_ns"] = int(message.default_clock_snapshot.ns_from_origin)
    expected = set(SCHEMAS[name].names)
    actual = set(record)
    if actual != expected:
        raise CtfError(
            f"moq_trace:{name} fields do not match the analyzer schema: "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    return record


def _discarded_count(message) -> int:
    return 1 if message.count is None else int(message.count)


def _event_pid(event) -> int | None:
    context = event.common_context_field
    if context is None or "vpid" not in context:
        return None
    return _scalar(context["vpid"])


def batches(
    input_path: pathlib.Path,
    expected_pid: int | None = None,
    batch_size: int = 65_536,
) -> Iterator[tuple[str, pa.RecordBatch]]:
    """Yield bounded, typed Arrow batches from one LTTng CTF trace."""

    rows = {name: [] for name in SCHEMAS}
    discarded_events = 0
    discarded_packets = 0
    event_count = 0
    for message in bt2.TraceCollectionMessageIterator(str(input_path)):
        if isinstance(message, bt2._DiscardedEventsMessageConst):
            discarded_events += _discarded_count(message)
            continue
        if isinstance(message, bt2._DiscardedPacketsMessageConst):
            discarded_packets += _discarded_count(message)
            continue
        if not isinstance(message, bt2._EventMessageConst):
            continue
        provider, separator, name = message.event.name.partition(":")
        if provider != "moq_trace" or not separator:
            continue
        if name not in SCHEMAS:
            raise CtfError(f"unsupported event moq_trace:{name}")
        if expected_pid is not None and _event_pid(message.event) != expected_pid:
            raise CtfError(
                f"expected relay VPID {expected_pid}, found {_event_pid(message.event)}"
            )
        rows[name].append(_record(message, name))
        event_count += 1
        if len(rows[name]) == batch_size:
            yield name, pa.RecordBatch.from_pylist(rows[name], schema=SCHEMAS[name])
            rows[name].clear()

    if discarded_events or discarded_packets:
        raise CtfError(
            f"LTTng discarded {discarded_events} events and {discarded_packets} packets"
        )
    if event_count == 0:
        raise CtfError("CTF trace contains no moq_trace events")
    for name, values in rows.items():
        if values:
            yield name, pa.RecordBatch.from_pylist(values, schema=SCHEMAS[name])
