#!/usr/bin/env python3
"""Run and analyze a local MoQ relay latency experiment."""

from __future__ import annotations

import dataclasses
import datetime
import json
import os
import pathlib
import re
import signal
import subprocess
import time
from typing import Annotated, BinaryIO

import matplotlib
# The headless backend must be selected before importing pyplot.
matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
import polars as pl
import typer
from pydantic import BaseModel, ConfigDict, Field, ValidationError


PROTOCOL = "moq-transport-19"
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")

METRICS = {
    "full_span": "Full relay span",
}

QUIC_OBJECT_METRICS = {
    "quic_forward_start": "QUIC forward start",
    "quic_tail_gap": "QUIC tail gap",
    "quic_full_span": "QUIC full span",
}

PACKET_METRICS = {
    "rx_packet_span": "RX packet span",
    "rx_header_parse": "RX header parse",
    "rx_header_unprotect": "RX header unprotect",
    "rx_payload_decrypt": "RX payload decrypt",
    "rx_frame_process": "RX frame process",
    "tx_packet_span": "TX packet span",
    "tx_frame_encode": "TX frame encode",
    "tx_packet_encrypt": "TX packet encrypt",
}

BOUNDARIES = (
    "rx_starts",
    "rx_ends",
    "tx_starts",
    "tx_ends",
)

SAMPLE_SCHEMA = {
    "group_id": pl.Int64,
    "object_id": pl.Int64,
    "metric": pl.String,
    "copy_ordinal": pl.Int64,
    "elapsed_ms": pl.Float64,
    "latency_us": pl.Float64,
}

TRACE_SCHEMA = {
    "type": pl.String,
    "timestamp_ns": pl.Int64,
    "trace_id": pl.UInt64,
    "session_id": pl.UInt64,
    "direction": pl.String,
    "group_id": pl.Int64,
    "object_id": pl.Int64,
    "phase": pl.String,
    "edge": pl.String,
    "outcome": pl.String,
    "payload_bytes": pl.Int64,
    "connection_id": pl.UInt64,
    "packet_number": pl.UInt64,
    "packet_space": pl.String,
    "byte_len": pl.UInt64,
    "sample_rate": pl.UInt64,
    "stream_id": pl.UInt64,
    "offset_start": pl.UInt64,
    "offset_end": pl.UInt64,
    "stream_offset_start": pl.UInt64,
    "stream_offset_end": pl.UInt64,
}

PACKET_SAMPLE_SCHEMA = {
    "metric": pl.String,
    "direction": pl.String,
    "connection_id": pl.UInt64,
    "trace_id": pl.UInt64,
    "occurrence": pl.UInt64,
    "elapsed_ms": pl.Float64,
    "latency_us": pl.Float64,
}


class ExperimentConfig(BaseModel):
    """Configuration for one local relay latency experiment."""

    model_config = ConfigDict(frozen=True)

    repo: pathlib.Path
    output: pathlib.Path
    relay_bin: pathlib.Path
    bench_bin: pathlib.Path
    relay_cpu: int | None = Field(default=None, ge=0)
    subscribers: int = Field(default=1, gt=0)
    fps: int = Field(default=30, gt=0)
    object_size: int = Field(default=16 * 1024, gt=0)
    duration: float = Field(default=20.0, gt=0)
    warmup: float = Field(default=1.0, ge=0)
    cooldown: float = Field(default=1.0, ge=0)
    port: int = Field(default=4443, ge=1, le=65535)
    release: bool = True
    skip_build: bool = False


def validate_cpu_affinity(
    config: ExperimentConfig,
    allowed_cpus: set[int] | None = None,
) -> None:
    """Reject a relay CPU outside the process affinity set."""

    if config.relay_cpu is None:
        return
    allowed_cpus = allowed_cpus or set(os.sched_getaffinity(0))
    if config.relay_cpu not in allowed_cpus:
        allowed = ", ".join(str(cpu) for cpu in sorted(allowed_cpus))
        raise ValueError(f"relay CPU {config.relay_cpu} is unavailable; allowed CPUs: {allowed}")


def build_relay_command(config: ExperimentConfig) -> list[str]:
    """Build the relay command, optionally pinning only the relay process."""

    command = [
        str(config.relay_bin),
        "--server-bind",
        f"[::]:{config.port}",
        "--server-backend",
        "quinn",
        "--server-version",
        PROTOCOL,
        "--tls-generate",
        "localhost",
        "--auth-public",
        "",
        "--trace-path",
        str((config.output / "relay.jsonl").resolve()),
    ]
    if config.relay_cpu is not None:
        command[:0] = ["taskset", "-c", str(config.relay_cpu)]
    return command


def _bench_base(config: ExperimentConfig) -> list[str]:
    return [
        str(config.bench_bin),
        "--client-connect",
        f"https://localhost:{config.port}",
        "--client-backend",
        "quinn",
        "--client-version",
        PROTOCOL,
        "--client-tls-disable-verify",
        "--startup",
        "0s",
        "--report",
        "200ms",
        "--fps",
        str(config.fps),
        "--frame-size",
        str(config.object_size),
        "--group-size",
        "0",
    ]


def build_publisher_command(config: ExperimentConfig) -> list[str]:
    """Build the single-publisher benchmark command."""

    return [
        *_bench_base(config),
        "--name",
        "relay-latency",
        "--connections",
        "1",
        "--broadcasts",
        "1",
        "--subscribe",
        "0",
    ]


def build_subscriber_command(config: ExperimentConfig) -> list[str]:
    """Build the configured subscriber fanout benchmark command."""

    total = config.warmup + config.duration + config.cooldown
    return [
        *_bench_base(config),
        "--name",
        "relay-latency-subscribers",
        "--connections",
        str(config.subscribers),
        "--broadcasts",
        "0",
        "--subscribe",
        "1",
        "--duration",
        f"{total:g}s",
    ]


@dataclasses.dataclass(frozen=True)
class TimelineSelection:
    """A real object selected nearest one full-span statistic."""

    statistic: str
    target_us: float
    group_id: int
    object_id: int
    actual_us: float


@dataclasses.dataclass(frozen=True)
class TimelineInterval:
    """One paired object lifecycle or processing phase interval."""

    direction: str
    session_id: int
    phase: str
    occurrence: int
    start_us: float
    end_us: float


@dataclasses.dataclass(frozen=True)
class TimelineCopy:
    """One subscriber copy and its creation order and full-span latency."""

    session_id: int
    subscriber_ordinal: int
    full_span_us: float


@dataclasses.dataclass(frozen=True)
class ObjectTimeline:
    """All traced intervals for one selected logical object."""

    selection: TimelineSelection
    intervals: tuple[TimelineInterval, ...]
    first_copy: TimelineCopy
    last_copy: TimelineCopy
    slowest_copy: TimelineCopy


@dataclasses.dataclass(frozen=True)
class Packet:
    """One successful sampled QUIC packet lifecycle."""

    trace_id: int
    connection_id: int
    direction: str
    packet_number: int | None
    start_ns: int
    end_ns: int
    byte_len: int | None


@dataclasses.dataclass(frozen=True)
class StreamFrame:
    """One successful STREAM frame and its containing packet."""

    packet: Packet
    stream_id: int
    offset_start: int
    offset_end: int


@dataclasses.dataclass(frozen=True)
class ObjectRange:
    """One traced object copy in transport stream coordinates."""

    direction: str
    session_id: int
    connection_id: int
    stream_id: int
    offset_start: int
    offset_end: int


@dataclasses.dataclass(frozen=True)
class Coverage:
    """The first packet set that completely covers an object range."""

    first_start_ns: int
    first_end_ns: int
    complete_end_ns: int
    packet_trace_ids: tuple[int, ...]


@dataclasses.dataclass(frozen=True)
class Analysis:
    """Validated latency samples and aggregate trace counts."""

    samples: pl.DataFrame
    statistics: dict[str, dict[str, float | int]]
    quic_object_samples: pl.DataFrame
    quic_object_statistics: dict[str, dict[str, float | int]]
    packet_samples: pl.DataFrame
    packet_statistics: dict[str, dict[str, float | int]]
    packet_count: int
    socket_count: int
    group_count: int
    events: pl.DataFrame
    steady_keys: tuple[tuple[int, int], ...]
    selections: tuple[TimelineSelection, ...]


class TraceError(RuntimeError):
    """A trace is malformed, incomplete, or does not match the workload."""


def _parse_packets(events: pl.DataFrame) -> tuple[tuple[Packet, ...], tuple[StreamFrame, ...], pl.DataFrame]:
    """Parse successful packet lifecycles, STREAM frames, and phase samples."""

    packet_rows = [
        row
        for row in events.iter_rows(named=True)
        if row.get("type") in {"quic_packet_start", "quic_packet_end"}
    ]
    grouped: dict[int, dict[str, list[dict]]] = {}
    for row in packet_rows:
        trace_id = row.get("trace_id")
        if trace_id is None:
            raise TraceError("packet lifecycle contains a missing trace ID")
        grouped.setdefault(int(trace_id), {"quic_packet_start": [], "quic_packet_end": []})[
            str(row["type"])
        ].append(row)

    packets: list[Packet] = []
    packet_by_id: dict[int, Packet] = {}
    for trace_id, scope in sorted(grouped.items()):
        starts = scope["quic_packet_start"]
        ends = scope["quic_packet_end"]
        if len(starts) != 1 or len(ends) != 1:
            raise TraceError(f"packet {trace_id} has {len(starts)} starts and {len(ends)} completions")
        start, end = starts[0], ends[0]
        if end.get("outcome") != "success":
            raise TraceError(f"packet {trace_id} did not complete successfully")
        for row in (start, end):
            if row.get("sample_rate") != 1:
                raise TraceError("QUIC object correlation requires packet_sample = 1")
            if row.get("connection_id") is None or row.get("direction") not in {"rx", "tx"}:
                raise TraceError(f"packet {trace_id} is missing transport identity")
            if row.get("timestamp_ns") is None:
                raise TraceError(f"packet {trace_id} is missing a timestamp")
        if start["connection_id"] != end["connection_id"] or start["direction"] != end["direction"]:
            raise TraceError(f"packet {trace_id} lifecycle identity changed")
        start_ns = int(start["timestamp_ns"])
        end_ns = int(end["timestamp_ns"])
        if end_ns < start_ns:
            raise TraceError(f"packet {trace_id} completes before it starts")
        packet_number = end.get("packet_number")
        if packet_number is None:
            packet_number = start.get("packet_number")
        byte_len = end.get("byte_len")
        if byte_len is None:
            byte_len = start.get("byte_len")
        packet = Packet(
            trace_id=trace_id,
            connection_id=int(start["connection_id"]),
            direction=str(start["direction"]),
            packet_number=None if packet_number is None else int(packet_number),
            start_ns=start_ns,
            end_ns=end_ns,
            byte_len=None if byte_len is None else int(byte_len),
        )
        packets.append(packet)
        packet_by_id[trace_id] = packet

    frames: list[StreamFrame] = []
    for row in events.iter_rows(named=True):
        if row.get("type") != "quic_stream_frame" or row.get("outcome") != "success":
            continue
        trace_id = row.get("trace_id")
        packet = packet_by_id.get(int(trace_id)) if trace_id is not None else None
        if packet is None:
            raise TraceError(f"STREAM frame references unknown packet {trace_id}")
        if row.get("sample_rate") != 1:
            raise TraceError("QUIC object correlation requires packet_sample = 1")
        if row.get("connection_id") != packet.connection_id or row.get("direction") != packet.direction:
            raise TraceError(f"packet {packet.trace_id} STREAM frame identity changed")
        values = (row.get("stream_id"), row.get("offset_start"), row.get("offset_end"))
        if any(value is None for value in values):
            raise TraceError(f"packet {packet.trace_id} STREAM frame is missing a byte range")
        stream_id, offset_start, offset_end = (int(value) for value in values)
        if offset_end < offset_start:
            raise TraceError(f"packet {packet.trace_id} STREAM frame has a negative byte range")
        # QUIC permits FIN-only STREAM frames. They carry no bytes and cannot
        # contribute to object coverage.
        if offset_end == offset_start:
            continue
        frames.append(StreamFrame(packet, stream_id, offset_start, offset_end))

    phases: dict[tuple[int, str], dict[str, list[dict]]] = {}
    for row in events.iter_rows(named=True):
        if row.get("type") != "quic_packet_phase":
            continue
        trace_id = row.get("trace_id")
        phase = row.get("phase")
        edge = row.get("edge")
        if trace_id is None or phase is None or edge not in {"start", "done"}:
            raise TraceError("packet phase is missing its scope identity")
        phases.setdefault((int(trace_id), str(phase)), {"start": [], "done": []})[str(edge)].append(row)

    first_packet_ns = min((packet.start_ns for packet in packets), default=0)
    metric_rows: list[dict] = []
    phase_order = {
        "header_parse": 1,
        "header_unprotect": 2,
        "payload_decrypt": 3,
        "frame_process": 4,
        "frame_encode": 1,
        "packet_encrypt": 2,
    }
    for packet in sorted(packets, key=lambda item: (item.start_ns, item.trace_id)):
        metric_rows.append(
            {
                "metric": f"{packet.direction}_packet_span",
                "direction": packet.direction,
                "connection_id": packet.connection_id,
                "trace_id": packet.trace_id,
                "occurrence": 0,
                "elapsed_ms": (packet.start_ns - first_packet_ns) / 1_000_000,
                "latency_us": (packet.end_ns - packet.start_ns) / 1_000,
            }
        )
        packet_phases = sorted(
            ((phase, scope) for (trace_id, phase), scope in phases.items() if trace_id == packet.trace_id),
            key=lambda item: (phase_order.get(item[0], 99), item[0]),
        )
        allowed = {
            "rx": {"header_parse", "header_unprotect", "payload_decrypt", "frame_process"},
            "tx": {"frame_encode", "packet_encrypt"},
        }[packet.direction]
        for phase, scope in packet_phases:
            if phase not in allowed:
                raise TraceError(f"packet {packet.trace_id} has invalid {packet.direction} phase {phase}")
            starts = sorted(scope["start"], key=lambda row: int(row["timestamp_ns"]))
            ends = sorted(scope["done"], key=lambda row: int(row["timestamp_ns"]))
            if len(starts) != len(ends):
                raise TraceError(
                    f"packet {packet.trace_id} {phase} has {len(starts)} starts and {len(ends)} completions"
                )
            for occurrence, (start, end) in enumerate(zip(starts, ends, strict=True)):
                if end.get("outcome") != "success":
                    raise TraceError(f"packet {packet.trace_id} {phase} did not complete successfully")
                start_ns = int(start["timestamp_ns"])
                end_ns = int(end["timestamp_ns"])
                if end_ns < start_ns:
                    raise TraceError(f"packet {packet.trace_id} {phase} completes before it starts")
                metric_rows.append(
                    {
                        "metric": f"{packet.direction}_{phase}",
                        "direction": packet.direction,
                        "connection_id": packet.connection_id,
                        "trace_id": packet.trace_id,
                        "occurrence": occurrence,
                        "elapsed_ms": (start_ns - first_packet_ns) / 1_000_000,
                        "latency_us": (end_ns - start_ns) / 1_000,
                    }
                )

    samples = pl.DataFrame(metric_rows, schema=PACKET_SAMPLE_SCHEMA)
    return tuple(packets), tuple(frames), samples


def _merge_interval(intervals: list[tuple[int, int]], new: tuple[int, int]) -> list[tuple[int, int]]:
    """Insert one interval into a normalized half-open interval union."""

    merged: list[tuple[int, int]] = []
    start, end = new
    for current_start, current_end in intervals:
        if current_end < start:
            merged.append((current_start, current_end))
        elif end < current_start:
            merged.append((start, end))
            start, end = current_start, current_end
        else:
            start = min(start, current_start)
            end = max(end, current_end)
    merged.append((start, end))
    return merged


def _first_complete_coverage(object_range: ObjectRange, frames: tuple[StreamFrame, ...]) -> Coverage:
    """Find the earliest completed packet set covering an object byte range."""

    if object_range.offset_end <= object_range.offset_start:
        raise TraceError(f"{object_range.direction} object has an empty or negative byte range")
    candidates: list[tuple[int, int, int, StreamFrame]] = []
    for frame in frames:
        if (
            frame.packet.direction != object_range.direction
            or frame.packet.connection_id != object_range.connection_id
            or frame.stream_id != object_range.stream_id
        ):
            continue
        start = max(object_range.offset_start, frame.offset_start)
        end = min(object_range.offset_end, frame.offset_end)
        if start < end:
            candidates.append((frame.packet.end_ns, start, end, frame))
    candidates.sort(key=lambda item: (item[0], item[3].packet.trace_id, item[1], item[2]))

    intervals: list[tuple[int, int]] = []
    selected: dict[int, Packet] = {}
    index = 0
    while index < len(candidates):
        completion = candidates[index][0]
        while index < len(candidates) and candidates[index][0] == completion:
            _end_ns, start, end, frame = candidates[index]
            intervals = _merge_interval(intervals, (start, end))
            selected[frame.packet.trace_id] = frame.packet
            index += 1
        if intervals == [(object_range.offset_start, object_range.offset_end)]:
            packets = tuple(sorted(selected.values(), key=lambda item: (item.end_ns, item.trace_id)))
            return Coverage(
                first_start_ns=min(packet.start_ns for packet in packets),
                first_end_ns=min(packet.end_ns for packet in packets),
                complete_end_ns=max(packet.end_ns for packet in packets),
                packet_trace_ids=tuple(packet.trace_id for packet in packets),
            )
    raise TraceError(
        "incomplete packet coverage for "
        f"{object_range.direction} connection {object_range.connection_id} stream {object_range.stream_id} "
        f"range [{object_range.offset_start}, {object_range.offset_end})"
    )


def select_timeline_objects(samples: pl.DataFrame) -> tuple[TimelineSelection, ...]:
    """Select real objects nearest mean, median, and p99 slowest-copy full span."""

    # Fanout produces one full-span sample per subscriber, so represent each
    # logical object by the copy that finished last.
    objects = (
        samples.filter(pl.col("metric") == "full_span")
        .group_by("group_id", "object_id")
        .agg(pl.col("latency_us").max().alias("actual_us"))
        .sort("group_id", "object_id")
    )
    if objects.is_empty():
        raise TraceError("no steady-state full-span samples for timeline selection")
    targets = objects.select(
        pl.col("actual_us").mean().alias("mean"),
        pl.col("actual_us").quantile(0.50, interpolation="linear").alias("median"),
        pl.col("actual_us").quantile(0.99, interpolation="linear").alias("p99"),
    ).row(0, named=True)
    rows = list(objects.iter_rows(named=True))
    selected = []
    for statistic in ("mean", "median", "p99"):
        target = float(targets[statistic])
        # Object identity makes equal-distance selections reproducible.
        nearest = min(
            rows,
            key=lambda row: (
                abs(float(row["actual_us"]) - target),
                int(row["group_id"]),
                int(row["object_id"]),
            ),
        )
        selected.append(
            TimelineSelection(
                statistic=statistic,
                target_us=target,
                group_id=int(nearest["group_id"]),
                object_id=int(nearest["object_id"]),
                actual_us=float(nearest["actual_us"]),
            )
        )
    return tuple(selected)


def extract_object_timeline(events: pl.DataFrame, selection: TimelineSelection) -> ObjectTimeline:
    """Pair every lifecycle boundary for one selected object."""

    selected = events.filter(
        pl.col("type").str.starts_with("moq_object_")
        & (pl.col("group_id") == selection.group_id)
        & (pl.col("object_id") == selection.object_id)
    )
    if selected.is_empty():
        raise TraceError(f"selected object ({selection.group_id}, {selection.object_id}) has no events")
    required = selected.filter(
        pl.any_horizontal(
            pl.col("timestamp_ns").is_null(),
            pl.col("direction").is_null(),
            pl.col("session_id").is_null(),
        )
    )
    if not required.is_empty():
        raise TraceError(f"selected object ({selection.group_id}, {selection.object_id}) has missing timeline identity")

    rows = list(selected.iter_rows(named=True))
    grouped: dict[tuple[str, int], list[dict]] = {}
    # Subscriber tasks run concurrently, so events must be separated by
    # session before lifecycle boundaries can be paired safely.
    for row in rows:
        key = (str(row["direction"]), int(row["session_id"]))
        grouped.setdefault(key, []).append(row)

    raw_intervals: list[tuple[str, int, str, int, int, int]] = []
    for (direction, session_id), session_rows in sorted(grouped.items()):
        lifecycle_starts = sorted(
            int(row["timestamp_ns"]) for row in session_rows if row["type"] == "moq_object_start"
        )
        lifecycle_ends = sorted(
            int(row["timestamp_ns"]) for row in session_rows if row["type"] == "moq_object_end"
        )
        if len(lifecycle_starts) != len(lifecycle_ends):
            raise TraceError(
                f"{direction} session {session_id} object has "
                f"{len(lifecycle_starts)} starts and {len(lifecycle_ends)} completions"
            )
        for occurrence, (start, end) in enumerate(zip(lifecycle_starts, lifecycle_ends, strict=True)):
            if end < start:
                raise TraceError(f"{direction} session {session_id} object completes before it starts")
            raw_intervals.append((direction, session_id, "object", occurrence, start, end))

        phases = sorted({str(row["phase"]) for row in session_rows if row["type"] == "moq_object_phase"})
        for phase in phases:
            # Repeated phase instances are sequential within one object session,
            # making timestamp order their stable occurrence identity.
            starts = sorted(
                int(row["timestamp_ns"])
                for row in session_rows
                if row["type"] == "moq_object_phase" and row["phase"] == phase and row["edge"] == "start"
            )
            ends = sorted(
                int(row["timestamp_ns"])
                for row in session_rows
                if row["type"] == "moq_object_phase" and row["phase"] == phase and row["edge"] == "done"
            )
            if len(starts) != len(ends):
                raise TraceError(
                    f"{direction} session {session_id} {phase} has "
                    f"{len(starts)} starts and {len(ends)} completions"
                )
            for occurrence, (start, end) in enumerate(zip(starts, ends, strict=True)):
                if end < start:
                    raise TraceError(f"{direction} session {session_id} {phase} completes before it starts")
                raw_intervals.append((direction, session_id, phase, occurrence, start, end))

    rx_objects = [interval for interval in raw_intervals if interval[0] == "rx" and interval[2] == "object"]
    if len(rx_objects) != 1:
        raise TraceError(
            f"selected object ({selection.group_id}, {selection.object_id}) has {len(rx_objects)} RX lifecycles"
        )
    # A single RX start provides a shared zero point for every subscriber copy.
    rx_start = rx_objects[0][4]
    phase_order = {
        "object": 0,
        "header_parse": 1,
        "create": 2,
        "payload_read": 3,
        "clone": 1,
        "header_encode": 2,
        "payload_write": 3,
    }
    intervals = tuple(
        sorted(
            (
                TimelineInterval(
                    direction=direction,
                    session_id=session_id,
                    phase=phase,
                    occurrence=occurrence,
                    start_us=(start - rx_start) / 1_000,
                    end_us=(end - rx_start) / 1_000,
                )
                for direction, session_id, phase, occurrence, start, end in raw_intervals
            ),
            key=lambda interval: (
                0 if interval.direction == "rx" else 1,
                interval.session_id,
                phase_order.get(interval.phase, 99),
                interval.occurrence,
            ),
        )
    )
    tx_objects = sorted(
        (interval for interval in intervals if interval.direction == "tx" and interval.phase == "object"),
        key=lambda interval: interval.session_id,
    )
    if not tx_objects:
        raise TraceError(f"selected object ({selection.group_id}, {selection.object_id}) has no TX lifecycle")
    copies = tuple(
        TimelineCopy(interval.session_id, ordinal, interval.end_us)
        for ordinal, interval in enumerate(tx_objects, start=1)
    )
    # Break equal completion times by session ID so summary metadata is stable.
    slowest = min(copies, key=lambda copy: (-copy.full_span_us, copy.session_id))
    return ObjectTimeline(
        selection=selection,
        intervals=intervals,
        first_copy=copies[0],
        last_copy=copies[-1],
        slowest_copy=slowest,
    )


def _read_trace(path: pathlib.Path) -> pl.DataFrame:
    """Read the consumed trace fields with stable nullable types."""

    try:
        return pl.read_ndjson(path, schema=TRACE_SCHEMA)
    except (OSError, pl.exceptions.PolarsError) as error:
        raise TraceError(f"failed to read trace {path}: {error}") from error


def summarize(samples: pl.DataFrame) -> dict[str, dict[str, float | int]]:
    """Summarize latency samples by metric."""

    if samples.is_empty():
        raise ValueError("cannot summarize an empty sample")
    statistics = samples.group_by("metric", maintain_order=True).agg(
        pl.len().alias("count"),
        pl.col("latency_us").mean().alias("mean"),
        pl.col("latency_us").quantile(0.50, interpolation="linear").alias("p50"),
        pl.col("latency_us").quantile(0.95, interpolation="linear").alias("p95"),
        pl.col("latency_us").quantile(0.99, interpolation="linear").alias("p99"),
        pl.col("latency_us").max().alias("max"),
    )
    return {
        row["metric"]: {
            "count": int(row["count"]),
            "mean": round(float(row["mean"]), 12),
            "p50": round(float(row["p50"]), 12),
            "p95": round(float(row["p95"]), 12),
            "p99": round(float(row["p99"]), 12),
            "max": round(float(row["max"]), 12),
        }
        for row in statistics.iter_rows(named=True)
    }


def _validate_scopes(
    events: pl.DataFrame,
    start_type: str,
    end_type: str,
    require_success: bool,
) -> int:
    """Require one matching end record for every sampled start record."""

    start_events = events.filter(pl.col("type") == start_type)
    end_events = events.filter(pl.col("type") == end_type)
    if start_events["trace_id"].null_count() or end_events["trace_id"].null_count():
        raise TraceError(f"{start_type}/{end_type} contains missing trace IDs")
    starts = set(start_events["trace_id"].to_list())
    ends = set(end_events["trace_id"].to_list())
    if starts != ends:
        raise TraceError(f"{start_type}/{end_type} trace IDs do not match: {len(starts)} starts, {len(ends)} ends")
    if require_success and not end_events.filter((pl.col("outcome") != "success").fill_null(True)).is_empty():
        raise TraceError(f"{end_type} contains unsuccessful outcomes")
    return len(starts)


def _group_objects(events: pl.DataFrame, object_size: int) -> pl.DataFrame:
    """Reduce object events to sorted boundary timestamp lists."""

    objects = events.filter(pl.col("type").str.starts_with("moq_object_"))
    missing = objects.filter(
        pl.any_horizontal(
            pl.col("group_id").is_null(),
            pl.col("object_id").is_null(),
            pl.col("timestamp_ns").is_null(),
        )
    )
    if not missing.is_empty():
        raise TraceError(f"object event is missing identity: {missing.row(0, named=True)}")

    boundary = (
        pl.when((pl.col("type") == "moq_object_start") & (pl.col("direction") == "rx"))
        .then(pl.lit("rx_starts"))
        .when((pl.col("type") == "moq_object_start") & (pl.col("direction") == "tx"))
        .then(pl.lit("tx_starts"))
        .when((pl.col("type") == "moq_object_end") & (pl.col("direction") == "rx"))
        .then(pl.lit("rx_ends"))
        .when((pl.col("type") == "moq_object_end") & (pl.col("direction") == "tx"))
        .then(pl.lit("tx_ends"))
        .otherwise(pl.lit(None, dtype=pl.String))
        .alias("boundary")
    )
    objects = objects.with_columns(boundary)
    grouped = objects.group_by("group_id", "object_id", maintain_order=True).agg(
        *(pl.col("timestamp_ns").filter(pl.col("boundary") == name).sort().alias(name) for name in BOUNDARIES),
        (
            (pl.col("type") == "moq_object_end")
            & (pl.col("direction") == "rx")
            & (pl.col("payload_bytes") == object_size)
        )
        .any()
        .alias("payload_matches"),
    )
    return grouped.filter("payload_matches").drop("payload_matches")


def _object_range(row: dict) -> ObjectRange:
    """Validate and extract one completed object's transport byte range."""

    required = (
        "direction",
        "session_id",
        "connection_id",
        "stream_id",
        "stream_offset_start",
        "stream_offset_end",
    )
    if any(row.get(name) is None for name in required):
        raise TraceError(f"completed object is missing transport identity: {row}")
    return ObjectRange(
        direction=str(row["direction"]),
        session_id=int(row["session_id"]),
        connection_id=int(row["connection_id"]),
        stream_id=int(row["stream_id"]),
        offset_start=int(row["stream_offset_start"]),
        offset_end=int(row["stream_offset_end"]),
    )


def _quic_object_samples(
    events: pl.DataFrame,
    keys: tuple[tuple[int, int], ...],
    subscribers: int,
    frames: tuple[StreamFrame, ...],
    first_rx_ns: int,
) -> pl.DataFrame:
    """Calculate QUIC-inclusive metrics from completely covered object ranges."""

    key_set = set(keys)
    ends = [
        row
        for row in events.iter_rows(named=True)
        if row.get("type") == "moq_object_end"
        and row.get("group_id") is not None
        and row.get("object_id") is not None
        and (int(row["group_id"]), int(row["object_id"])) in key_set
    ]
    grouped: dict[tuple[int, int], list[dict]] = {}
    for row in ends:
        grouped.setdefault((int(row["group_id"]), int(row["object_id"])), []).append(row)

    rows: list[dict] = []
    for group_id, object_id in keys:
        object_rows = grouped.get((group_id, object_id), [])
        rx_ranges = [_object_range(row) for row in object_rows if row.get("direction") == "rx"]
        tx_ranges = sorted(
            (_object_range(row) for row in object_rows if row.get("direction") == "tx"),
            key=lambda item: item.session_id,
        )
        if len(rx_ranges) != 1:
            raise TraceError(f"{(group_id, object_id)} has {len(rx_ranges)} completed RX transport ranges")
        if len(tx_ranges) != subscribers:
            raise TraceError(
                f"{(group_id, object_id)} has {len(tx_ranges)} completed TX transport ranges, expected {subscribers}"
            )
        rx = _first_complete_coverage(rx_ranges[0], frames)
        elapsed_ms = (rx.first_start_ns - first_rx_ns) / 1_000_000
        for copy_ordinal, tx_range in enumerate(tx_ranges):
            tx = _first_complete_coverage(tx_range, frames)
            boundaries = {
                "quic_forward_start": tx.first_end_ns - rx.first_start_ns,
                "quic_tail_gap": tx.complete_end_ns - rx.complete_end_ns,
                "quic_full_span": tx.complete_end_ns - rx.first_start_ns,
            }
            for metric, latency_ns in boundaries.items():
                if latency_ns < 0:
                    raise TraceError(f"{metric} is negative for object {(group_id, object_id)} copy {copy_ordinal}")
                rows.append(
                    {
                        "group_id": group_id,
                        "object_id": object_id,
                        "metric": metric,
                        "copy_ordinal": copy_ordinal,
                        "elapsed_ms": elapsed_ms,
                        "latency_us": latency_ns / 1_000,
                    }
                )
    return pl.DataFrame(rows, schema=SAMPLE_SCHEMA)


def analyze_trace(
    path: pathlib.Path,
    subscribers: int,
    object_size: int,
    warmup: float,
    cooldown: float,
) -> Analysis:
    """Parse, validate, trim, and summarize a relay JSONL trace."""

    events = _read_trace(path)
    packets, frames, packet_samples = _parse_packets(events)
    packet_count = len(packets)
    socket_count = _validate_scopes(events, "udp_socket_start", "udp_socket_end", False)
    objects = _group_objects(events, object_size)
    if objects.is_empty():
        raise TraceError(f"trace has no completed {object_size}-byte inbound objects")
    object_index = {(int(row["group_id"]), int(row["object_id"])): row for row in objects.iter_rows(named=True)}
    payload_keys = sorted(object_index)
    for key in payload_keys:
        rx_starts = object_index[key]["rx_starts"]
        if len(rx_starts) != 1:
            raise TraceError(f"{key} has {len(rx_starts)} rx_starts")

    first_rx = min(object_index[key]["rx_starts"][0] for key in payload_keys)
    last_rx = max(object_index[key]["rx_starts"][0] for key in payload_keys)
    # RX starts define the workload clock, independent of fanout completion.
    window_start = first_rx + int(warmup * 1_000_000_000)
    window_end = last_rx - int(cooldown * 1_000_000_000)
    keys = [key for key in payload_keys if window_start <= object_index[key]["rx_starts"][0] <= window_end]
    if not keys:
        raise TraceError("steady-state window contains no complete objects")
    for key in keys:
        obj = object_index[key]
        if len(obj["rx_ends"]) != 1:
            raise TraceError(f"{key} has {len(obj['rx_ends'])} rx_ends")
        for name in ("tx_starts", "tx_ends"):
            if len(obj[name]) != subscribers:
                raise TraceError(f"{key} has {len(obj[name])} {name}, expected {subscribers}")
    groups = sorted({group for group, _object in keys})
    if groups != list(range(groups[0], groups[-1] + 1)):
        raise TraceError("steady-state groups are not contiguous")

    rows = []
    for group_id, object_id in keys:
        key = (group_id, object_id)
        obj = object_index[key]
        rx_start = obj["rx_starts"][0]
        elapsed_ms = (rx_start - first_rx) / 1_000_000
        # Ordinals describe timestamp order only. Session IDs are not
        # available in this boundary-reduced table.
        for ordinal, target in enumerate(sorted(obj["tx_ends"])):
            rows.append(
                {
                    "group_id": group_id,
                    "object_id": object_id,
                    "metric": "full_span",
                    "copy_ordinal": ordinal,
                    "elapsed_ms": elapsed_ms,
                    "latency_us": (target - rx_start) / 1_000,
                }
            )

    samples = pl.DataFrame(rows, schema=SAMPLE_SCHEMA)
    steady_keys = tuple(keys)
    quic_object_samples = _quic_object_samples(
        events,
        steady_keys,
        subscribers,
        frames,
        first_rx,
    )
    return Analysis(
        samples=samples,
        statistics=summarize(samples),
        quic_object_samples=quic_object_samples,
        quic_object_statistics=summarize(quic_object_samples),
        packet_samples=packet_samples,
        packet_statistics=summarize(packet_samples),
        packet_count=packet_count,
        socket_count=socket_count,
        group_count=len(groups),
        events=events,
        steady_keys=steady_keys,
        selections=select_timeline_objects(samples),
    )


def write_csv(path: pathlib.Path, analysis: Analysis) -> None:
    """Write deterministic long-form object latency samples."""

    write_samples(path, analysis.samples, ("group_id", "object_id", "metric", "copy_ordinal"))


def write_samples(path: pathlib.Path, samples: pl.DataFrame, sort_by: tuple[str, ...]) -> None:
    """Write one deterministic long-form latency sample table."""

    path.parent.mkdir(parents=True, exist_ok=True)
    samples.sort(*sort_by).write_csv(path, float_precision=6)


def write_summary(
    path: pathlib.Path,
    config: ExperimentConfig,
    analysis: Analysis,
    commands: dict[str, list[str]],
    timelines: tuple[ObjectTimeline, ...],
) -> None:
    """Write experiment configuration, commands, counts, and statistics."""

    affinity = {"mode": "unpinned"} if config.relay_cpu is None else {"mode": "single-core", "cpu": config.relay_cpu}
    summary = {
        "protocol": PROTOCOL,
        "affinity": affinity,
        "workload": {
            "publishers": 1,
            "subscribers": config.subscribers,
            "objects_per_group": 1,
            "object_size": config.object_size,
            "fps": config.fps,
            "duration_seconds": config.duration,
            "warmup_seconds": config.warmup,
            "cooldown_seconds": config.cooldown,
        },
        "binaries": {
            "relay": str(config.relay_bin),
            "bench": str(config.bench_bin),
        },
        "commands": commands,
        "counts": {
            "groups": analysis.group_count,
            "packets": analysis.packet_count,
            "socket_operations": analysis.socket_count,
            "correlated_objects": len(analysis.quic_object_samples) // len(QUIC_OBJECT_METRICS),
            "quic_object_samples": len(analysis.quic_object_samples),
            "quic_packet_samples": len(analysis.packet_samples),
        },
        "statistics_us": analysis.statistics,
        "quic_object_statistics_us": analysis.quic_object_statistics,
        "quic_packet_statistics_us": analysis.packet_statistics,
        "timeline_objects": [
            {
                "statistic": timeline.selection.statistic,
                "target_us": timeline.selection.target_us,
                "group_id": timeline.selection.group_id,
                "object_id": timeline.selection.object_id,
                "actual_us": timeline.selection.actual_us,
                "copies": {
                    "first": dataclasses.asdict(timeline.first_copy),
                    "last": dataclasses.asdict(timeline.last_copy),
                    "slowest": dataclasses.asdict(timeline.slowest_copy),
                },
            }
            for timeline in timelines
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def plot_analysis(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis) -> None:
    """Render ECDF, percentile, and time-series latency panels."""

    plot_metrics(
        path,
        config,
        analysis.samples,
        analysis.statistics,
        METRICS,
        "MoQ relay latency",
    )


def plot_quic_analysis(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis) -> None:
    """Render QUIC-inclusive object metric panels."""

    plot_metrics(
        path,
        config,
        analysis.quic_object_samples,
        analysis.quic_object_statistics,
        QUIC_OBJECT_METRICS,
        "QUIC-inclusive relay latency",
    )


def plot_packet_analysis(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis) -> None:
    """Render QUIC packet span and phase diagnostic panels."""

    plot_metrics(
        path,
        config,
        analysis.packet_samples,
        analysis.packet_statistics,
        PACKET_METRICS,
        "QUIC packet diagnostics",
    )


def plot_metrics(
    path: pathlib.Path,
    config: ExperimentConfig,
    samples: pl.DataFrame,
    statistics: dict[str, dict[str, float | int]],
    labels: dict[str, str],
    title: str,
) -> None:
    """Render distribution, percentile, and time-series panels for one metric layer."""

    fig, axes = plt.subplots(1, 3, figsize=(17, 5.5))
    colors = plt.get_cmap("tab10").colors

    present = [metric for metric in labels if metric in statistics]
    for index, metric in enumerate(present):
        metric_samples = samples.filter(pl.col("metric") == metric)
        values_ms = (metric_samples["latency_us"] / 1_000).to_numpy()
        axes[0].ecdf(
            values_ms,
            label=labels.get(metric, metric),
            color=colors[index],
            linewidth=2,
        )
    axes[0].set_title("Latency distribution")
    axes[0].set_xlabel("Latency (ms)")
    axes[0].set_ylabel("ECDF")
    axes[0].grid(alpha=0.25)
    axes[0].legend(fontsize=8)

    percentiles = ("p50", "p95", "p99")
    width = 0.8 / len(statistics)
    x_positions = list(range(len(percentiles)))
    for index, metric in enumerate(present):
        summary = statistics[metric]
        offset = (index - (len(statistics) - 1) / 2) * width
        axes[1].bar(
            [position + offset for position in x_positions],
            [float(summary[name]) / 1_000 for name in percentiles],
            width=width,
            label=labels.get(metric, metric),
            color=colors[index],
        )
    axes[1].set_xticks(x_positions, percentiles)
    axes[1].set_title("Tail percentiles")
    axes[1].set_ylabel("Latency (ms)")
    axes[1].grid(axis="y", alpha=0.25)

    for index, metric in enumerate(present):
        metric_samples = samples.filter(pl.col("metric") == metric)
        axes[2].scatter(
            (metric_samples["elapsed_ms"] / 1_000).to_numpy(),
            (metric_samples["latency_us"] / 1_000).to_numpy(),
            label=labels.get(metric, metric),
            color=colors[index],
            s=8,
            alpha=0.55,
        )
    axes[2].set_title("Latency over time")
    axes[2].set_xlabel("Elapsed time (s)")
    axes[2].set_ylabel("Latency (ms)")
    axes[2].grid(alpha=0.25)

    affinity = "unpinned" if config.relay_cpu is None else f"pinned CPU {config.relay_cpu}"
    fig.suptitle(
        f"{title} | "
        f"{affinity} | {config.subscribers} subscriber(s) | "
        f"{config.object_size} bytes | {config.fps} fps | {PROTOCOL}"
    )
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=160)
    plt.close(fig)


def plot_object_timelines(
    path: pathlib.Path,
    config: ExperimentConfig,
    timelines: tuple[ObjectTimeline, ...],
) -> None:
    """Render aligned lifecycle timelines for representative objects."""

    if not timelines:
        raise ValueError("cannot plot an empty object timeline selection")
    phase_rows = (
        ("rx", "header_parse", "RX Header Parse"),
        ("rx", "create", "RX Create"),
        ("rx", "payload_read", "RX Payload Read"),
        ("tx", "clone", "TX Clone"),
        ("tx", "header_encode", "TX Header Encode"),
        ("tx", "payload_write", "TX Payload Write"),
    )
    positions = {(direction, phase): index for index, (direction, phase, _label) in enumerate(phase_rows)}
    labels = [label for _direction, _phase, label in phase_rows]

    figure_height = max(11.0, len(timelines) * len(phase_rows) * 0.28 + 2.5)
    fig, axes = plt.subplots(len(timelines), 1, figsize=(15, figure_height), sharex=True, squeeze=False)
    axes = axes[:, 0]
    maximum = max(interval.end_us for timeline in timelines for interval in timeline.intervals)
    x_limit = max(1.0, maximum * 1.05)
    rx_color = "#2563EB"
    tx_palette = plt.get_cmap("Oranges")

    for axis, timeline in zip(axes, timelines, strict=True):
        displayed_copies = (timeline.first_copy,)
        if timeline.last_copy.session_id != timeline.first_copy.session_id:
            displayed_copies += (timeline.last_copy,)
        tx_sessions = [copy.session_id for copy in displayed_copies]
        copy_by_session = {copy.session_id: copy for copy in displayed_copies}
        tx_colors = {
            session_id: tx_palette(0.5 + 0.4 * index / max(1, len(tx_sessions) - 1))
            for index, session_id in enumerate(tx_sessions)
        }
        lane_height = min(0.52, 0.62 / max(1, len(tx_sessions)))
        tx_offsets = {
            session_id: (index - (len(tx_sessions) - 1) / 2) * lane_height
            for index, session_id in enumerate(tx_sessions)
        }

        for interval in timeline.intervals:
            if interval.direction == "tx" and interval.session_id not in copy_by_session:
                continue
            color = rx_color if interval.direction == "rx" else tx_colors[interval.session_id]
            if interval.phase == "object":
                axis.axvline(interval.start_us, color=color, linestyle=":", linewidth=0.9, alpha=0.55)
                axis.axvline(interval.end_us, color=color, linestyle="--", linewidth=0.9, alpha=0.55)
                continue

            key = (interval.direction, interval.phase)
            if key not in positions:
                raise TraceError(f"unsupported timeline phase {interval.direction} {interval.phase}")
            y = positions[key]
            height = 0.52
            if interval.direction == "tx":
                y += tx_offsets[interval.session_id]
                height = lane_height * 0.82
            axis.broken_barh(
                [(interval.start_us, interval.end_us - interval.start_us)],
                (y - height / 2, height),
                facecolors=color,
                edgecolors="#334155",
                linewidth=0.7,
                alpha=0.88,
            )

        legend_handles = [Line2D([0], [0], color=rx_color, linewidth=5, label="RX")]
        legend_handles.extend(
            Line2D(
                [0],
                [0],
                color=tx_colors[session_id],
                linewidth=5,
                label=f"TX #{copy_by_session[session_id].subscriber_ordinal}",
            )
            for session_id in tx_sessions
        )
        axis.legend(
            handles=legend_handles,
            loc="upper right",
            fontsize=7,
            ncols=min(4, len(legend_handles)),
        )
        axis.set_yticks(range(len(phase_rows)), labels, fontsize=8)
        axis.set_ylim(len(phase_rows) - 0.5, -0.5)
        axis.set_xlim(0, x_limit)
        axis.grid(axis="x", color="#CBD5E1", alpha=0.7, linewidth=0.7)
        axis.set_axisbelow(True)
        selected = timeline.selection
        axis.set_title(
            f"{selected.statistic} target {selected.target_us:.2f} µs | "
            f"object ({selected.group_id}, {selected.object_id}) | "
            f"actual {selected.actual_us:.2f} µs | "
            f"selected by TX #{timeline.slowest_copy.subscriber_ordinal}: "
            f"{timeline.slowest_copy.full_span_us:.2f} µs",
            fontsize=10,
            loc="left",
        )

    axes[-1].set_xlabel("Elapsed from RX object start (µs)")
    fig.suptitle(
        "MoQ relay object lifecycle timelines\n"
        f"Mean, median, and p99 of per-object latency | {config.object_size} bytes | "
        f"{config.subscribers} subscriber(s) | {PROTOCOL}",
        fontsize=14,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.965))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=160)
    plt.close(fig)


class ExperimentError(RuntimeError):
    """The experiment could not complete or validate successfully."""


def default_output(repo: pathlib.Path) -> pathlib.Path:
    """Return a UTC timestamped run path under the repository target directory."""

    timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    return repo / "target" / "moq-trace" / timestamp


def wait_for_log(
    path: pathlib.Path,
    pattern: re.Pattern[str],
    process: subprocess.Popen[bytes],
    timeout: float,
) -> None:
    """Wait for a log expression while also watching for early child exit."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            contents = path.read_text(errors="replace")
        except FileNotFoundError:
            contents = ""
        # tracing-subscriber can color redirected logs, which would split the
        # word-boundary expressions used for readiness checks.
        contents = ANSI_ESCAPE.sub("", contents)
        if pattern.search(contents):
            return
        status = process.poll()
        if status is not None:
            raise ExperimentError(f"process exited with status {status} before log matched {pattern.pattern!r}")
        time.sleep(0.05)
    raise ExperimentError(f"timed out after {timeout:g}s waiting for {pattern.pattern!r} in {path}")


def validate_protocol(binary: pathlib.Path, flag: str) -> None:
    """Require a binary to accept the forced moq-transport-19 CLI value."""

    result = subprocess.run(
        [str(binary), flag, PROTOCOL, "--help"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        raise ExperimentError(f"{binary} does not accept {flag} {PROTOCOL}")


def _stop(
    process: subprocess.Popen[bytes] | None,
    name: str,
    graceful: bool = False,
) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGINT if graceful else signal.SIGKILL)
    except ProcessLookupError:
        return
    if not graceful:
        process.wait()
        return
    try:
        status = process.wait(timeout=10)
    except subprocess.TimeoutExpired as error:
        raise ExperimentError(f"{name} did not stop after SIGINT") from error
    if status != 0:
        raise ExperimentError(f"{name} exited with status {status}")


def _launch(command: list[str], log: pathlib.Path) -> tuple[subprocess.Popen[bytes], BinaryIO]:
    handle = log.open("wb")
    try:
        process = subprocess.Popen(
            command,
            cwd=log.parent,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception:
        handle.close()
        raise
    return process, handle


def run_experiment(config: ExperimentConfig) -> pathlib.Path:
    commands = {
        "relay": build_relay_command(config),
        "publisher": build_publisher_command(config),
        "subscriber": build_subscriber_command(config),
    }
    output = config.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    build_log = output / "build.log"
    if not config.skip_build:
        build_command = ["cargo", "build"]
        if config.release:
            build_command.append("--release")
        build_command.extend(["-p", "moq-relay", "--features", "trace", "-p", "moq-bench"])
        with build_log.open("wb") as log:
            result = subprocess.run(
                build_command,
                cwd=config.repo,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
            )
        if result.returncode != 0:
            raise ExperimentError(f"build failed with status {result.returncode}; see {build_log}")

    validate_protocol(config.relay_bin, "--server-version")
    validate_protocol(config.bench_bin, "--client-version")

    relay = publisher = subscriber = None
    handles = []
    try:
        relay, handle = _launch(commands["relay"], output / "relay.log")
        handles.append(handle)
        wait_for_log(output / "relay.log", re.compile(r"\blistening\b"), relay, 15)

        publisher, handle = _launch(commands["publisher"], output / "publisher.log")
        handles.append(handle)
        wait_for_log(
            output / "publisher.log",
            re.compile(r"\bconnections=1\b"),
            publisher,
            15,
        )

        subscriber, handle = _launch(commands["subscriber"], output / "subscriber.log")
        handles.append(handle)
        subscriber_ready = re.compile(
            rf"\bconnections={config.subscribers}\b.*"
            rf"\bsubscriptions={config.subscribers}\b"
        )
        wait_for_log(output / "subscriber.log", subscriber_ready, subscriber, 20)
        try:
            subscriber_status = subscriber.wait(timeout=config.warmup + config.duration + config.cooldown + 15)
        except subprocess.TimeoutExpired as error:
            raise ExperimentError("subscriber did not finish on schedule") from error
        if subscriber_status != 0:
            raise ExperimentError(f"subscriber exited with status {subscriber_status}")

        # Stop production first, then let the relay flush its JSONL writer
        # during graceful shutdown before the trace is analyzed.
        _stop(publisher, "publisher", graceful=True)
        _stop(relay, "relay", graceful=True)
    except Exception:
        _stop(subscriber, "subscriber")
        _stop(publisher, "publisher")
        _stop(relay, "relay")
        raise
    finally:
        for handle in handles:
            handle.close()

    relay_log = (output / "relay.log").read_text(errors="replace")
    expected_sessions = config.subscribers + 1
    if relay_log.count(PROTOCOL) < expected_sessions:
        raise ExperimentError(f"relay log does not confirm {PROTOCOL} for every connection")
    trace_path = output / "relay.jsonl"
    if not trace_path.read_bytes().endswith(b"\n"):
        raise ExperimentError("relay trace does not end with a complete newline")
    analysis = analyze_trace(
        trace_path,
        config.subscribers,
        config.object_size,
        config.warmup,
        config.cooldown,
    )
    timelines = tuple(extract_object_timeline(analysis.events, selection) for selection in analysis.selections)
    write_csv(output / "objects.csv", analysis)
    write_samples(
        output / "quic_objects.csv",
        analysis.quic_object_samples,
        ("group_id", "object_id", "metric", "copy_ordinal"),
    )
    write_samples(
        output / "quic_packets.csv",
        analysis.packet_samples,
        ("trace_id", "metric", "occurrence"),
    )
    write_summary(output / "summary.json", config, analysis, commands, timelines)
    plot_analysis(output / "latency.png", config, analysis)
    plot_quic_analysis(output / "quic_latency.png", config, analysis)
    plot_packet_analysis(output / "packet_latency.png", config, analysis)
    plot_object_timelines(output / "object_timeline.png", config, timelines)
    return output


def print_statistics(title: str, statistics: dict[str, dict[str, float | int]]) -> None:
    """Print one labeled latency statistics table."""

    typer.echo(title)
    typer.echo("metric              count    mean_us     p50_us     p95_us     p99_us")
    for metric, values in statistics.items():
        typer.echo(
            f"{metric:18} {values['count']:6d} "
            f"{values['mean']:10.2f} {values['p50']:10.2f} "
            f"{values['p95']:10.2f} {values['p99']:10.2f}"
        )


app = typer.Typer(add_completion=False, help="Measure local MoQ relay processing latency.")


@app.command()
def main(
    relay_cpu: Annotated[int | None, typer.Option("--relay-cpu")] = None,
    subscribers: Annotated[int, typer.Option("--subscribers")] = 1,
    fps: Annotated[int, typer.Option("--fps")] = 30,
    object_size: Annotated[int, typer.Option("--object-size")] = 16 * 1024,
    duration: Annotated[float, typer.Option("--duration")] = 20,
    warmup: Annotated[float, typer.Option("--warmup")] = 1,
    cooldown: Annotated[float, typer.Option("--cooldown")] = 1,
    port: Annotated[int, typer.Option("--port")] = 4443,
    output: Annotated[pathlib.Path | None, typer.Option("--output")] = None,
    skip_build: Annotated[bool, typer.Option("--skip-build")] = False,
    debug_build: Annotated[bool, typer.Option("--debug-build")] = False,
    relay_bin: Annotated[
        pathlib.Path | None,
        typer.Option("--relay-bin", help="Override the relay binary path."),
    ] = None,
    bench_bin: Annotated[
        pathlib.Path | None,
        typer.Option("--bench-bin", help="Override the benchmark binary path."),
    ] = None,
) -> None:
    """Parse arguments, run the experiment, and report artifacts."""

    repo = pathlib.Path(__file__).resolve().parents[3]
    profile = "debug" if debug_build else "release"
    output = output or default_output(repo)
    try:
        config = ExperimentConfig(
            repo=repo,
            output=output,
            relay_bin=relay_bin or repo / "target" / profile / "moq-relay",
            bench_bin=bench_bin or repo / "target" / profile / "moq-bench",
            relay_cpu=relay_cpu,
            subscribers=subscribers,
            fps=fps,
            object_size=object_size,
            duration=duration,
            warmup=warmup,
            cooldown=cooldown,
            port=port,
            release=not debug_build,
            skip_build=skip_build,
        )
        validate_cpu_affinity(config)
        result = run_experiment(config)
        summary = json.loads((result / "summary.json").read_text())
    except (ExperimentError, OSError, TraceError, ValidationError, ValueError) as error:
        typer.echo(f"error: {error}", err=True)
        typer.echo(f"run directory: {output.resolve()}", err=True)
        raise typer.Exit(1) from error

    print_statistics("MoQ object metrics", summary["statistics_us"])
    print_statistics("QUIC-inclusive object metrics", summary["quic_object_statistics_us"])
    print_statistics("QUIC packet diagnostics", summary["quic_packet_statistics_us"])
    for name in (
        "objects.csv",
        "quic_objects.csv",
        "quic_packets.csv",
        "summary.json",
        "latency.png",
        "quic_latency.png",
        "packet_latency.png",
        "object_timeline.png",
    ):
        typer.echo(f"{name}: {(result / name).resolve()}")


if __name__ == "__main__":
    app()
