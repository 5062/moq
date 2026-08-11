#!/usr/bin/env python3
"""Run and analyze a local MoQ relay latency experiment."""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import json
import os
import pathlib
import re
import signal
import subprocess
import time
from collections.abc import Iterator, Mapping
from typing import Annotated, Literal, TypedDict, cast

import matplotlib

# The headless backend must be selected before importing pyplot.
matplotlib.use("Agg")
import polars as pl
import typer
from matplotlib import pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
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
    "rx_routing": "RX routing",
    "rx_scheduling": "RX scheduling",
    "rx_header_unprotect": "RX header unprotect",
    "rx_payload_decrypt": "RX payload decrypt",
    "rx_frame_process": "RX frame process",
    "tx_packet_span": "TX packet span",
    "tx_frame_encode": "TX frame encode",
    "tx_packet_encrypt": "TX packet encrypt",
}

Direction = Literal["rx", "tx"]
ObjectPhaseName = Literal[
    "header_parse",
    "create",
    "payload_read",
    "clone",
    "header_encode",
    "payload_write",
]
PacketPhaseName = Literal[
    "header_parse",
    "routing",
    "scheduling",
    "header_unprotect",
    "payload_decrypt",
    "frame_process",
    "frame_encode",
    "packet_encrypt",
]

OBJECT_PHASES: dict[Direction, frozenset[str]] = {
    "rx": frozenset({"header_parse", "create", "payload_read"}),
    "tx": frozenset({"clone", "header_encode", "payload_write"}),
}

PACKET_PHASES: dict[Direction, frozenset[str]] = {
    "rx": frozenset(
        {
            "header_parse",
            "routing",
            "scheduling",
            "header_unprotect",
            "payload_decrypt",
            "frame_process",
        }
    ),
    "tx": frozenset({"frame_encode", "packet_encrypt"}),
}

PACKET_PHASE_ORDER = {
    "header_parse": 1,
    "routing": 2,
    "scheduling": 3,
    "header_unprotect": 4,
    "payload_decrypt": 5,
    "frame_process": 6,
    "frame_encode": 1,
    "packet_encrypt": 2,
}

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
    quinn_path: pathlib.Path | None = None
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
    if allowed_cpus is None:
        allowed_cpus = set(os.sched_getaffinity(0))
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


def build_workspace_command(config: ExperimentConfig) -> list[str]:
    """Build the experiment binaries, optionally using a local Quinn checkout."""

    command = ["cargo", "build"]
    if config.release:
        command.append("--release")
    command.extend(["-p", "moq-relay", "--features", "trace", "-p", "moq-bench"])
    if config.quinn_path is not None:
        for crate in ("quinn", "quinn-proto"):
            path = (config.quinn_path / crate).resolve()
            command.extend(
                [
                    "--config",
                    f"patch.crates-io.{crate}.path={json.dumps(str(path))}",
                ]
            )
    return command


def validate_quinn_path(config: ExperimentConfig) -> None:
    """Validate an optional local Quinn workspace override."""

    if config.quinn_path is None:
        return
    if config.skip_build:
        raise ValueError("--quinn-path cannot be combined with --skip-build")
    for crate in ("quinn", "quinn-proto"):
        manifest = config.quinn_path / crate / "Cargo.toml"
        if not manifest.is_file():
            raise ValueError(f"local Quinn checkout is missing {manifest}")


class TraceRow(TypedDict, total=False):
    """One nullable row read from the heterogeneous trace schema."""

    type: str | None
    timestamp_ns: int | None
    trace_id: int | None
    session_id: int | None
    direction: str | None
    group_id: int | None
    object_id: int | None
    phase: str | None
    edge: str | None
    outcome: str | None
    payload_bytes: int | None
    connection_id: int | None
    sample_rate: int | None
    stream_id: int | None
    offset_start: int | None
    offset_end: int | None
    stream_offset_start: int | None
    stream_offset_end: int | None


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
    """One traced or derived object lifecycle interval."""

    direction: Direction
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
    """All traced and derived intervals for one selected logical object."""

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
    direction: Direction
    start_ns: int
    end_ns: int


@dataclasses.dataclass(frozen=True)
class PacketPhase:
    """One successful phase interval within a QUIC packet."""

    packet: Packet
    phase: PacketPhaseName
    occurrence: int
    start_ns: int
    end_ns: int


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

    direction: Direction
    session_id: int
    connection_id: int
    stream_id: int
    offset_start: int
    offset_end: int


@dataclasses.dataclass(frozen=True)
class ObjectBoundaries:
    """Validated lifecycle boundaries for one logical object."""

    group_id: int
    object_id: int
    rx_starts: tuple[int, ...]
    rx_ends: tuple[int, ...]
    tx_starts: tuple[int, ...]
    tx_ends: tuple[int, ...]


@dataclasses.dataclass(frozen=True, order=True)
class ObjectKey:
    """Stable group and object identity within one trace."""

    group_id: int
    object_id: int

    def __str__(self) -> str:
        """Format the identity like the previous tuple-based errors."""

        return f"({self.group_id}, {self.object_id})"


@dataclasses.dataclass(frozen=True)
class IndexedObject:
    """All trace rows and boundaries for one logical object."""

    key: ObjectKey
    rows: tuple[TraceRow, ...]
    boundaries: ObjectBoundaries
    rx_payload_bytes: frozenset[int]

    def matches_payload(self, payload_bytes: int) -> bool:
        """Return whether any completed inbound copy has the requested payload size."""

        return payload_bytes in self.rx_payload_bytes


@dataclasses.dataclass(frozen=True)
class StreamKey:
    """Transport identity shared by object ranges and STREAM frames."""

    direction: Direction
    connection_id: int
    stream_id: int


@dataclasses.dataclass(frozen=True)
class Coverage:
    """The first packet set that completely covers an object range."""

    first_start_ns: int
    first_end_ns: int
    complete_end_ns: int
    packet_trace_ids: tuple[int, ...]


@dataclasses.dataclass(frozen=True)
class TimeRangeNs:
    """One validated inclusive-start, inclusive-end time range."""

    start_ns: int
    end_ns: int


@dataclasses.dataclass(frozen=True)
class IntervalNs:
    """One traced or derived object lifecycle interval in nanoseconds."""

    direction: Direction
    session_id: int
    phase: str
    occurrence: int
    start_ns: int
    end_ns: int


@dataclasses.dataclass(frozen=True)
class ByteRange:
    """One normalized half-open stream byte range."""

    start: int
    end: int


@dataclasses.dataclass(frozen=True)
class FrameCoverage:
    """The portion of an object range covered by one STREAM frame."""

    completion_ns: int
    start: int
    end: int
    frame: StreamFrame


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
    group_count: int
    timelines: tuple[ObjectTimeline, ...]


@dataclasses.dataclass(frozen=True)
class SteadyState:
    """Validated objects and workload origin for the analysis window."""

    objects: dict[ObjectKey, ObjectBoundaries]
    first_rx_ns: int
    group_count: int


@dataclasses.dataclass(frozen=True)
class ParsedPackets:
    """Validated packets with reusable identity and phase indexes."""

    packets: tuple[Packet, ...]
    by_id: dict[int, Packet]
    frames: tuple[StreamFrame, ...]
    phases: tuple[PacketPhase, ...]
    phases_by_packet: dict[int, tuple[PacketPhase, ...]]
    samples: pl.DataFrame


@dataclasses.dataclass(frozen=True)
class MetricPlot:
    """One metric layer and its plot presentation."""

    samples: pl.DataFrame
    statistics: dict[str, dict[str, float | int]]
    labels: dict[str, str]
    title: str


class TraceError(RuntimeError):
    """A trace is malformed, incomplete, or does not match the workload."""


def _trace_rows(events: pl.DataFrame) -> Iterator[TraceRow]:
    """Yield typed views over rows from the heterogeneous trace dataframe."""

    for row in events.iter_rows(named=True):
        yield cast(TraceRow, row)


def _required_int(row: Mapping[str, object], field: str, label: str) -> int:
    """Read one required integer trace field."""

    value = row.get(field)
    if value is None:
        raise TraceError(f"{label} is missing {field}")
    if not isinstance(value, int):
        raise TraceError(f"{label} has invalid {field} {value!r}")
    return value


def _required_text(row: Mapping[str, object], field: str, label: str) -> str:
    """Read one required string trace field."""

    value = row.get(field)
    if value is None:
        raise TraceError(f"{label} is missing {field}")
    if not isinstance(value, str):
        raise TraceError(f"{label} has invalid {field} {value!r}")
    return value


def _parse_direction(value: object, label: str) -> Direction:
    """Validate one trace direction."""

    if value not in ("rx", "tx"):
        raise TraceError(f"{label} has invalid direction {value!r}")
    return cast(Direction, value)


def _parse_packet_phase(value: object, label: str) -> PacketPhaseName:
    """Validate one packet phase name independently of direction."""

    if not isinstance(value, str) or not any(value in phases for phases in PACKET_PHASES.values()):
        raise TraceError(f"{label} has invalid packet phase {value!r}")
    return cast(PacketPhaseName, value)


def _parse_object_phase(value: object, direction: Direction, label: str) -> ObjectPhaseName:
    """Validate one object phase for its trace direction."""

    if not isinstance(value, str) or value not in OBJECT_PHASES[direction]:
        raise TraceError(f"{label} has invalid {direction} phase {value!r}")
    return cast(ObjectPhaseName, value)


def _time_range(start_ns: int, end_ns: int, label: str) -> TimeRangeNs:
    """Validate and construct one time range."""

    if end_ns < start_ns:
        raise TraceError(f"{label} completes before it starts")
    return TimeRangeNs(start_ns, end_ns)


def _validate_packet_identity(row: TraceRow, packet: Packet, label: str) -> None:
    """Validate packet metadata repeated on a child trace row."""

    if _required_int(row, "sample_rate", label) != 1:
        raise TraceError("QUIC object correlation requires packet_sample = 1")
    direction = _parse_direction(row.get("direction"), label)
    if direction != packet.direction:
        raise TraceError(f"{label} has direction {direction}, expected {packet.direction}")
    connection_id = _required_int(row, "connection_id", label)
    if connection_id != packet.connection_id:
        raise TraceError(f"{label} has connection {connection_id}, expected {packet.connection_id}")


@dataclasses.dataclass
class PhaseScope:
    """Collected start and completion rows for one phase."""

    starts: list[TraceRow] = dataclasses.field(default_factory=list)
    completions: list[TraceRow] = dataclasses.field(default_factory=list)

    def add(self, row: TraceRow, label: str) -> None:
        """Add a phase boundary after validating its edge."""

        edge = _required_text(row, "edge", label)
        if edge == "start":
            self.starts.append(row)
        elif edge == "done":
            self.completions.append(row)
        else:
            raise TraceError(f"{label} has invalid edge {edge!r}")


@dataclasses.dataclass
class PacketScope:
    """Collected lifecycle rows for one packet trace ID."""

    starts: list[TraceRow] = dataclasses.field(default_factory=list)
    completions: list[TraceRow] = dataclasses.field(default_factory=list)

    def add(self, row: TraceRow, label: str) -> None:
        """Add one packet lifecycle row."""

        event_type = _required_text(row, "type", label)
        if event_type == "quic_packet_start":
            self.starts.append(row)
        elif event_type == "quic_packet_end":
            self.completions.append(row)
        else:
            raise TraceError(f"{label} has invalid event type {event_type!r}")


@dataclasses.dataclass
class ObjectScope:
    """Collected lifecycle and phase records for one object session copy."""

    starts_ns: list[int] = dataclasses.field(default_factory=list)
    completions_ns: list[int] = dataclasses.field(default_factory=list)
    completed_rows: list[TraceRow] = dataclasses.field(default_factory=list)
    phases: dict[ObjectPhaseName, PhaseScope] = dataclasses.field(default_factory=dict)

    def add(self, row: TraceRow, direction: Direction, label: str) -> None:
        """Add one object lifecycle or phase row."""

        event_type = _required_text(row, "type", label)
        if event_type == "moq_object_start":
            self.starts_ns.append(_required_int(row, "timestamp_ns", label))
        elif event_type == "moq_object_end":
            self.completions_ns.append(_required_int(row, "timestamp_ns", label))
            self.completed_rows.append(row)
        elif event_type == "moq_object_phase":
            phase = _parse_object_phase(row.get("phase"), direction, label)
            self.phases.setdefault(phase, PhaseScope()).add(row, f"{label} {phase}")
        else:
            raise TraceError(f"{label} has invalid event type {event_type!r}")


@dataclasses.dataclass
class IndexedObjectBuilder:
    """Mutable object accumulator used only during trace ingestion."""

    rows: list[TraceRow] = dataclasses.field(default_factory=list)
    rx_starts: list[int] = dataclasses.field(default_factory=list)
    rx_ends: list[int] = dataclasses.field(default_factory=list)
    tx_starts: list[int] = dataclasses.field(default_factory=list)
    tx_ends: list[int] = dataclasses.field(default_factory=list)
    rx_payload_bytes: set[int] = dataclasses.field(default_factory=set)

    def add(self, row: TraceRow, direction: Direction, label: str) -> None:
        """Validate and collect one logical object event."""

        event_type = _required_text(row, "type", label)
        if event_type == "moq_object_start":
            timestamp_ns = _required_int(row, "timestamp_ns", label)
            (self.rx_starts if direction == "rx" else self.tx_starts).append(timestamp_ns)
        elif event_type == "moq_object_end":
            timestamp_ns = _required_int(row, "timestamp_ns", label)
            (self.rx_ends if direction == "rx" else self.tx_ends).append(timestamp_ns)
            if direction == "rx":
                self.rx_payload_bytes.add(_required_int(row, "payload_bytes", label))
        elif event_type == "moq_object_phase":
            phase = _parse_object_phase(row.get("phase"), direction, label)
            edge = _required_text(row, "edge", f"{label} {phase}")
            if edge not in {"start", "done"}:
                raise TraceError(f"{label} {phase} has invalid edge {edge!r}")
            _required_int(row, "timestamp_ns", f"{label} {phase}")
        else:
            raise TraceError(f"{label} has invalid event type {event_type!r}")
        self.rows.append(row)

    def finish(self, key: ObjectKey) -> IndexedObject:
        """Freeze the accumulated rows into one indexed object."""

        boundaries = ObjectBoundaries(
            group_id=key.group_id,
            object_id=key.object_id,
            rx_starts=tuple(sorted(self.rx_starts)),
            rx_ends=tuple(sorted(self.rx_ends)),
            tx_starts=tuple(sorted(self.tx_starts)),
            tx_ends=tuple(sorted(self.tx_ends)),
        )
        return IndexedObject(
            key=key,
            rows=tuple(self.rows),
            boundaries=boundaries,
            rx_payload_bytes=frozenset(self.rx_payload_bytes),
        )


def _pair_phase_scope(scope: PhaseScope, label: str) -> tuple[TimeRangeNs, ...]:
    """Pair successful phase boundaries in timestamp order."""

    starts = sorted(scope.starts, key=lambda row: _required_int(row, "timestamp_ns", label))
    completions = sorted(scope.completions, key=lambda row: _required_int(row, "timestamp_ns", label))
    if len(starts) != len(completions):
        raise TraceError(f"{label} has {len(starts)} starts and {len(completions)} completions")

    intervals = []
    for start, completion in zip(starts, completions, strict=True):
        if completion.get("outcome") != "success":
            raise TraceError(f"{label} did not complete successfully")
        intervals.append(
            _time_range(
                _required_int(start, "timestamp_ns", label),
                _required_int(completion, "timestamp_ns", label),
                label,
            )
        )
    return tuple(intervals)


def _finish_packets(
    packet_scopes: dict[int, PacketScope],
    phase_scopes: dict[tuple[int, PacketPhaseName], PhaseScope],
    stream_rows: list[TraceRow],
) -> ParsedPackets:
    """Validate and freeze packet records collected during trace ingestion."""

    packets: list[Packet] = []
    packet_by_id: dict[int, Packet] = {}
    for trace_id, scope in sorted(packet_scopes.items()):
        if len(scope.starts) != 1 or len(scope.completions) != 1:
            raise TraceError(
                f"packet {trace_id} has {len(scope.starts)} starts and {len(scope.completions)} completions"
            )
        start, completion = scope.starts[0], scope.completions[0]
        if completion.get("outcome") != "success":
            raise TraceError(f"packet {trace_id} did not complete successfully")
        for row in (start, completion):
            if _required_int(row, "sample_rate", f"packet {trace_id}") != 1:
                raise TraceError("QUIC object correlation requires packet_sample = 1")
        direction = _parse_direction(start.get("direction"), f"packet {trace_id}")
        completion_direction = _parse_direction(completion.get("direction"), f"packet {trace_id} completion")
        if completion_direction != direction:
            raise TraceError(f"packet {trace_id} changes direction from {direction} to {completion_direction}")
        connection_id = _required_int(start, "connection_id", f"packet {trace_id}")
        completion_connection_id = _required_int(completion, "connection_id", f"packet {trace_id} completion")
        if completion_connection_id != connection_id:
            raise TraceError(f"packet {trace_id} changes connection from {connection_id} to {completion_connection_id}")
        time_range = _time_range(
            _required_int(start, "timestamp_ns", f"packet {trace_id}"),
            _required_int(completion, "timestamp_ns", f"packet {trace_id}"),
            f"packet {trace_id}",
        )
        packet = Packet(
            trace_id=trace_id,
            connection_id=connection_id,
            direction=direction,
            start_ns=time_range.start_ns,
            end_ns=time_range.end_ns,
        )
        packets.append(packet)
        packet_by_id[trace_id] = packet

    frames: list[StreamFrame] = []
    for row in stream_rows:
        if row.get("outcome") != "success":
            continue
        trace_id = _required_int(row, "trace_id", "STREAM frame")
        packet = packet_by_id.get(trace_id)
        if packet is None:
            raise TraceError(f"STREAM frame references unknown packet {trace_id}")
        _validate_packet_identity(row, packet, f"packet {trace_id} STREAM frame")
        stream_id = _required_int(row, "stream_id", f"packet {trace_id} STREAM frame")
        offset_start = _required_int(row, "offset_start", f"packet {trace_id} STREAM frame")
        offset_end = _required_int(row, "offset_end", f"packet {trace_id} STREAM frame")
        if offset_end < offset_start:
            raise TraceError(f"packet {packet.trace_id} STREAM frame has a negative byte range")
        # QUIC permits FIN-only STREAM frames. They carry no bytes and cannot
        # contribute to object coverage.
        if offset_end == offset_start:
            continue
        frames.append(StreamFrame(packet, stream_id, offset_start, offset_end))

    phases_by_packet: dict[int, list[tuple[PacketPhaseName, PhaseScope]]] = {}
    for (trace_id, phase), scope in phase_scopes.items():
        if trace_id not in packet_by_id:
            raise TraceError(f"phase {phase} references unknown packet {trace_id}")
        phases_by_packet.setdefault(trace_id, []).append((phase, scope))

    first_packet_ns = min((packet.start_ns for packet in packets), default=0)
    phase_intervals: list[PacketPhase] = []
    indexed_phases: dict[int, list[PacketPhase]] = {}
    metric_rows: list[dict] = []
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
            phases_by_packet.get(packet.trace_id, []),
            key=lambda item: (PACKET_PHASE_ORDER[item[0]], item[0]),
        )
        for phase, scope in packet_phases:
            if phase not in PACKET_PHASES[packet.direction]:
                raise TraceError(f"packet {packet.trace_id} has invalid {packet.direction} phase {phase}")
            label = f"packet {packet.trace_id} {phase}"
            for row in (*scope.starts, *scope.completions):
                _validate_packet_identity(row, packet, label)
            intervals = _pair_phase_scope(scope, label)
            for occurrence, interval in enumerate(intervals):
                packet_phase = PacketPhase(packet, phase, occurrence, interval.start_ns, interval.end_ns)
                phase_intervals.append(packet_phase)
                indexed_phases.setdefault(packet.trace_id, []).append(packet_phase)
                metric_rows.append(
                    {
                        "metric": f"{packet.direction}_{phase}",
                        "direction": packet.direction,
                        "connection_id": packet.connection_id,
                        "trace_id": packet.trace_id,
                        "occurrence": occurrence,
                        "elapsed_ms": (interval.start_ns - first_packet_ns) / 1_000_000,
                        "latency_us": (interval.end_ns - interval.start_ns) / 1_000,
                    }
                )

    return ParsedPackets(
        packets=tuple(packets),
        by_id=packet_by_id,
        frames=tuple(frames),
        phases=tuple(phase_intervals),
        phases_by_packet={trace_id: tuple(phases) for trace_id, phases in indexed_phases.items()},
        samples=pl.DataFrame(metric_rows, schema=PACKET_SAMPLE_SCHEMA),
    )


def _merge_interval(intervals: list[ByteRange], new: ByteRange) -> list[ByteRange]:
    """Insert one interval into a normalized half-open interval union."""

    merged: list[ByteRange] = []
    start, end = new.start, new.end
    for current in intervals:
        if current.end < start:
            merged.append(current)
        elif end < current.start:
            merged.append(ByteRange(start, end))
            start, end = current.start, current.end
        else:
            start = min(start, current.start)
            end = max(end, current.end)
    merged.append(ByteRange(start, end))
    return merged


@dataclasses.dataclass
class CoverageIndex:
    """STREAM frames indexed by transport identity with object-range caching."""

    frames_by_stream: dict[StreamKey, tuple[StreamFrame, ...]]
    cache: dict[ObjectRange, Coverage] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_frames(cls, frames: tuple[StreamFrame, ...]) -> CoverageIndex:
        """Build one transport-stream index from validated STREAM frames."""

        grouped: dict[StreamKey, list[StreamFrame]] = {}
        for frame in frames:
            key = StreamKey(frame.packet.direction, frame.packet.connection_id, frame.stream_id)
            grouped.setdefault(key, []).append(frame)
        indexed = {
            key: tuple(
                sorted(
                    stream_frames,
                    key=lambda frame: (
                        frame.packet.end_ns,
                        frame.packet.trace_id,
                        frame.offset_start,
                        frame.offset_end,
                    ),
                )
            )
            for key, stream_frames in grouped.items()
        }
        return cls(indexed)

    def first_complete(self, object_range: ObjectRange) -> Coverage:
        """Return the earliest packet coverage, reusing a previous range result."""

        cached = self.cache.get(object_range)
        if cached is not None:
            return cached
        coverage = self._calculate(object_range)
        self.cache[object_range] = coverage
        return coverage

    def _calculate(self, object_range: ObjectRange) -> Coverage:
        """Calculate coverage from frames on the matching transport stream."""

        if object_range.offset_end <= object_range.offset_start:
            raise TraceError(f"{object_range.direction} object has an empty or negative byte range")
        key = StreamKey(object_range.direction, object_range.connection_id, object_range.stream_id)
        candidates: list[FrameCoverage] = []
        for frame in self.frames_by_stream.get(key, ()):
            start = max(object_range.offset_start, frame.offset_start)
            end = min(object_range.offset_end, frame.offset_end)
            if start < end:
                candidates.append(FrameCoverage(frame.packet.end_ns, start, end, frame))
        candidates.sort(key=lambda item: (item.completion_ns, item.frame.packet.trace_id, item.start, item.end))

        intervals: list[ByteRange] = []
        selected: dict[int, Packet] = {}
        index = 0
        while index < len(candidates):
            completion_ns = candidates[index].completion_ns
            while index < len(candidates) and candidates[index].completion_ns == completion_ns:
                candidate = candidates[index]
                intervals = _merge_interval(intervals, ByteRange(candidate.start, candidate.end))
                selected[candidate.frame.packet.trace_id] = candidate.frame.packet
                index += 1
            if intervals == [ByteRange(object_range.offset_start, object_range.offset_end)]:
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


@dataclasses.dataclass(frozen=True)
class TraceIndex:
    """Validated packet, object, and stream indexes for one trace file."""

    packets: ParsedPackets
    objects: dict[ObjectKey, IndexedObject]
    coverage: CoverageIndex

    @staticmethod
    def read(path: pathlib.Path) -> TraceIndex:
        """Read and index one relay JSONL trace."""

        return _build_trace_index(_read_trace(path))


def _build_trace_index(events: pl.DataFrame) -> TraceIndex:
    """Build every analysis index in one pass over the trace dataframe."""

    packet_scopes: dict[int, PacketScope] = {}
    phase_scopes: dict[tuple[int, PacketPhaseName], PhaseScope] = {}
    stream_rows: list[TraceRow] = []
    object_builders: dict[ObjectKey, IndexedObjectBuilder] = {}

    for row in _trace_rows(events):
        event_type = row.get("type")
        if event_type in {"quic_packet_start", "quic_packet_end"}:
            trace_id = _required_int(row, "trace_id", "packet lifecycle event")
            packet_scopes.setdefault(trace_id, PacketScope()).add(row, f"packet {trace_id}")
        elif event_type == "quic_packet_phase":
            trace_id = _required_int(row, "trace_id", "packet phase event")
            phase = _parse_packet_phase(row.get("phase"), f"packet {trace_id}")
            phase_scopes.setdefault((trace_id, phase), PhaseScope()).add(row, f"packet {trace_id} {phase}")
        elif event_type == "quic_stream_frame":
            stream_rows.append(row)
        elif event_type in {"moq_object_start", "moq_object_end", "moq_object_phase"}:
            key = ObjectKey(
                _required_int(row, "group_id", "object event"),
                _required_int(row, "object_id", "object event"),
            )
            direction = _parse_direction(row.get("direction"), f"object {key}")
            object_builders.setdefault(key, IndexedObjectBuilder()).add(row, direction, f"object {key}")
        elif isinstance(event_type, str) and (event_type.startswith("quic_") or event_type.startswith("moq_object_")):
            raise TraceError(f"unknown trace event type {event_type!r}")

    packets = _finish_packets(packet_scopes, phase_scopes, stream_rows)
    objects = {key: builder.finish(key) for key, builder in object_builders.items()}
    return TraceIndex(
        packets=packets,
        objects=objects,
        coverage=CoverageIndex.from_frames(packets.frames),
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


def _quic_timeline_intervals(
    object_ranges: tuple[ObjectRange, ...],
    trace: TraceIndex,
) -> tuple[IntervalNs, ...]:
    """Return packet and packet-phase intervals covering selected object copies."""

    intervals: list[IntervalNs] = []
    for object_range in sorted(object_ranges, key=lambda item: (item.direction, item.session_id)):
        coverage = trace.coverage.first_complete(object_range)
        trace_ids = set(coverage.packet_trace_ids)
        covered_packets = sorted(
            (trace.packets.by_id[trace_id] for trace_id in trace_ids),
            key=lambda packet: (packet.start_ns, packet.trace_id),
        )
        for occurrence, packet in enumerate(covered_packets):
            intervals.append(
                IntervalNs(
                    direction=object_range.direction,
                    session_id=object_range.session_id,
                    phase="quic_packet",
                    occurrence=occurrence,
                    start_ns=packet.start_ns,
                    end_ns=packet.end_ns,
                )
            )
        covered_phases = sorted(
            (phase for trace_id in trace_ids for phase in trace.packets.phases_by_packet.get(trace_id, ())),
            key=lambda phase: (phase.start_ns, phase.packet.trace_id, phase.phase, phase.occurrence),
        )
        for phase in covered_phases:
            intervals.append(
                IntervalNs(
                    direction=object_range.direction,
                    session_id=object_range.session_id,
                    phase=f"quic_{phase.phase}",
                    occurrence=phase.occurrence,
                    start_ns=phase.start_ns,
                    end_ns=phase.end_ns,
                )
            )
    return tuple(intervals)


def extract_object_timeline(trace: TraceIndex, selection: TimelineSelection) -> ObjectTimeline:
    """Pair every lifecycle boundary for one selected object."""

    key = ObjectKey(selection.group_id, selection.object_id)
    indexed_object = trace.objects.get(key)
    object_label = f"selected object {key}"
    if indexed_object is None:
        raise TraceError(f"{object_label} has no lifecycle events")

    grouped: dict[tuple[Direction, int], ObjectScope] = {}
    # Subscriber tasks run concurrently, so events must be separated by
    # session before lifecycle boundaries can be paired safely.
    for row in indexed_object.rows:
        direction = _parse_direction(row.get("direction"), object_label)
        session_id = _required_int(row, "session_id", object_label)
        label = f"{direction} session {session_id} object"
        grouped.setdefault((direction, session_id), ObjectScope()).add(row, direction, label)

    raw_intervals: list[IntervalNs] = []
    for (direction, session_id), scope in sorted(grouped.items()):
        starts = sorted(scope.starts_ns)
        completions = sorted(scope.completions_ns)
        if len(starts) != len(completions):
            raise TraceError(
                f"{direction} session {session_id} object has {len(starts)} starts and {len(completions)} completions"
            )
        for occurrence, (start_ns, end_ns) in enumerate(zip(starts, completions, strict=True)):
            interval = _time_range(start_ns, end_ns, f"{direction} session {session_id} object")
            raw_intervals.append(
                IntervalNs(direction, session_id, "object", occurrence, interval.start_ns, interval.end_ns)
            )

        for phase, phase_scope in sorted(scope.phases.items()):
            # Repeated phase instances are sequential within one object session,
            # making timestamp order their stable occurrence identity.
            intervals = _pair_phase_scope(phase_scope, f"{direction} session {session_id} {phase}")
            for occurrence, interval in enumerate(intervals):
                raw_intervals.append(
                    IntervalNs(direction, session_id, phase, occurrence, interval.start_ns, interval.end_ns)
                )

    object_ranges = tuple(_object_range(row) for key in sorted(grouped) for row in grouped[key].completed_rows)
    raw_intervals.extend(_quic_timeline_intervals(object_ranges, trace))

    rx_objects = [interval for interval in raw_intervals if interval.direction == "rx" and interval.phase == "object"]
    if len(rx_objects) != 1:
        raise TraceError(f"{object_label} has {len(rx_objects)} completed RX lifecycles")
    rx_object_start_ns = rx_objects[0].start_ns
    rx_packets = [
        interval for interval in raw_intervals if interval.direction == "rx" and interval.phase == "quic_packet"
    ]
    if not rx_packets:
        raise TraceError(f"{object_label} has no covering RX packets")
    # The first contributing RX packet provides a shared origin for the full
    # QUIC-to-MoQ lifecycle while the latency metric still starts at the object.
    timeline_start_ns = min(interval.start_ns for interval in rx_packets)
    rx_object_start_us = (rx_object_start_ns - timeline_start_ns) / 1_000
    phase_order = {
        "quic_packet": 0,
        "quic_header_parse": 1,
        "quic_routing": 2,
        "quic_scheduling": 3,
        "quic_header_unprotect": 4,
        "quic_payload_decrypt": 5,
        "quic_frame_process": 6,
        "quic_frame_encode": 1,
        "quic_packet_encrypt": 2,
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
                    direction=interval.direction,
                    session_id=interval.session_id,
                    phase=interval.phase,
                    occurrence=interval.occurrence,
                    start_us=(interval.start_ns - timeline_start_ns) / 1_000,
                    end_us=(interval.end_ns - timeline_start_ns) / 1_000,
                )
                for interval in raw_intervals
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
    copies = tuple(
        TimelineCopy(interval.session_id, ordinal, interval.end_us - rx_object_start_us)
        for ordinal, interval in enumerate(tx_objects, start=1)
    )
    if not copies:
        raise TraceError(f"{object_label} has no completed TX lifecycles")
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


def _object_range(row: TraceRow) -> ObjectRange:
    """Validate and extract one completed object's transport byte range."""

    label = "completed object"
    return ObjectRange(
        direction=_parse_direction(row.get("direction"), label),
        session_id=_required_int(row, "session_id", label),
        connection_id=_required_int(row, "connection_id", label),
        stream_id=_required_int(row, "stream_id", label),
        offset_start=_required_int(row, "stream_offset_start", label),
        offset_end=_required_int(row, "stream_offset_end", label),
    )


def _quic_object_samples(
    trace: TraceIndex,
    keys: tuple[ObjectKey, ...],
    subscribers: int,
    first_rx_ns: int,
) -> pl.DataFrame:
    """Calculate QUIC-inclusive metrics from indexed object ranges."""

    rows: list[dict] = []
    for key in keys:
        indexed_object = trace.objects[key]
        object_ranges = tuple(_object_range(row) for row in indexed_object.rows if row.get("type") == "moq_object_end")
        rx_ranges = [object_range for object_range in object_ranges if object_range.direction == "rx"]
        tx_ranges = sorted(
            (object_range for object_range in object_ranges if object_range.direction == "tx"),
            key=lambda item: item.session_id,
        )
        if len(rx_ranges) != 1:
            raise TraceError(f"{key} has {len(rx_ranges)} completed RX transport ranges")
        if len(tx_ranges) != subscribers:
            raise TraceError(f"{key} has {len(tx_ranges)} completed TX transport ranges, expected {subscribers}")
        rx = trace.coverage.first_complete(rx_ranges[0])
        elapsed_ms = (rx.first_start_ns - first_rx_ns) / 1_000_000
        for copy_ordinal, tx_range in enumerate(tx_ranges):
            tx = trace.coverage.first_complete(tx_range)
            boundaries = {
                "quic_forward_start": tx.first_end_ns - rx.first_start_ns,
                "quic_tail_gap": tx.complete_end_ns - rx.complete_end_ns,
                "quic_full_span": tx.complete_end_ns - rx.first_start_ns,
            }
            for metric, latency_ns in boundaries.items():
                if latency_ns < 0:
                    raise TraceError(f"{metric} is negative for object {key} copy {copy_ordinal}")
                rows.append(
                    {
                        "group_id": key.group_id,
                        "object_id": key.object_id,
                        "metric": metric,
                        "copy_ordinal": copy_ordinal,
                        "elapsed_ms": elapsed_ms,
                        "latency_us": latency_ns / 1_000,
                    }
                )
    return pl.DataFrame(rows, schema=SAMPLE_SCHEMA)


def _select_steady_state(trace: TraceIndex, config: ExperimentConfig) -> SteadyState:
    """Select and validate payload objects inside the steady-state window."""

    object_index = {
        key: indexed_object.boundaries
        for key, indexed_object in trace.objects.items()
        if indexed_object.matches_payload(config.object_size)
    }
    if not object_index:
        raise TraceError(f"trace has no completed {config.object_size}-byte inbound objects")
    payload_keys = sorted(object_index)
    for key in payload_keys:
        rx_starts = object_index[key].rx_starts
        if len(rx_starts) != 1:
            raise TraceError(f"{key} has {len(rx_starts)} rx_starts")

    first_rx = min(object_index[key].rx_starts[0] for key in payload_keys)
    last_rx = max(object_index[key].rx_starts[0] for key in payload_keys)
    # RX starts define the workload clock, independent of fanout completion.
    window_start = first_rx + int(config.warmup * 1_000_000_000)
    window_end = last_rx - int(config.cooldown * 1_000_000_000)
    keys = [key for key in payload_keys if window_start <= object_index[key].rx_starts[0] <= window_end]
    if not keys:
        raise TraceError("steady-state window contains no complete objects")
    for key in keys:
        obj = object_index[key]
        if len(obj.rx_ends) != 1:
            raise TraceError(f"{key} has {len(obj.rx_ends)} rx_ends")
        counts = {
            "tx_starts": len(obj.tx_starts),
            "tx_ends": len(obj.tx_ends),
        }
        for name, count in counts.items():
            if count != config.subscribers:
                raise TraceError(f"{key} has {count} {name}, expected {config.subscribers}")
    groups = sorted({key.group_id for key in keys})
    if groups != list(range(groups[0], groups[-1] + 1)):
        raise TraceError("steady-state groups are not contiguous")
    return SteadyState(
        objects={key: object_index[key] for key in keys},
        first_rx_ns=first_rx,
        group_count=len(groups),
    )


def _moq_object_samples(steady_state: SteadyState) -> pl.DataFrame:
    """Build MoQ lifecycle samples for validated steady-state objects."""

    rows = []
    for key, obj in steady_state.objects.items():
        rx_start = obj.rx_starts[0]
        elapsed_ms = (rx_start - steady_state.first_rx_ns) / 1_000_000
        # Ordinals describe timestamp order only. Session IDs are not
        # available in this boundary-reduced table.
        for ordinal, target in enumerate(sorted(obj.tx_ends)):
            rows.append(
                {
                    "group_id": key.group_id,
                    "object_id": key.object_id,
                    "metric": "full_span",
                    "copy_ordinal": ordinal,
                    "elapsed_ms": elapsed_ms,
                    "latency_us": (target - rx_start) / 1_000,
                }
            )
    return pl.DataFrame(rows, schema=SAMPLE_SCHEMA)


def _analyze_index(trace: TraceIndex, config: ExperimentConfig) -> Analysis:
    """Calculate analysis outputs from one validated trace index."""

    steady_state = _select_steady_state(trace, config)
    samples = _moq_object_samples(steady_state)
    steady_keys = tuple(steady_state.objects)
    quic_object_samples = _quic_object_samples(
        trace,
        steady_keys,
        config.subscribers,
        steady_state.first_rx_ns,
    )
    selections = select_timeline_objects(samples)
    timelines = tuple(extract_object_timeline(trace, selection) for selection in selections)
    return Analysis(
        samples=samples,
        statistics=summarize(samples),
        quic_object_samples=quic_object_samples,
        quic_object_statistics=summarize(quic_object_samples),
        packet_samples=trace.packets.samples,
        packet_statistics=summarize(trace.packets.samples),
        packet_count=len(trace.packets.packets),
        group_count=steady_state.group_count,
        timelines=timelines,
    )


def analyze_trace(
    path: pathlib.Path,
    config: ExperimentConfig,
) -> Analysis:
    """Read, index, and analyze a relay JSONL trace."""

    return _analyze_index(TraceIndex.read(path), config)


def write_samples(path: pathlib.Path, samples: pl.DataFrame, sort_by: tuple[str, ...]) -> None:
    """Write one deterministic long-form latency sample table."""

    path.parent.mkdir(parents=True, exist_ok=True)
    samples.sort(*sort_by).write_csv(path, float_precision=6)


def write_summary(
    path: pathlib.Path,
    config: ExperimentConfig,
    analysis: Analysis,
    commands: dict[str, list[str]],
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
            "correlated_objects": analysis.samples.select("group_id", "object_id").unique().height,
            "correlated_object_copies": len(analysis.quic_object_samples) // len(QUIC_OBJECT_METRICS),
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
            for timeline in analysis.timelines
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def plot_analysis(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis) -> None:
    """Render ECDF, percentile, and time-series latency panels."""

    plot_metrics(
        path,
        config,
        MetricPlot(analysis.samples, analysis.statistics, METRICS, "MoQ relay latency"),
    )


def plot_quic_analysis(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis) -> None:
    """Render QUIC-inclusive object metric panels."""

    plot_metrics(
        path,
        config,
        MetricPlot(
            analysis.quic_object_samples,
            analysis.quic_object_statistics,
            QUIC_OBJECT_METRICS,
            "QUIC-inclusive relay latency",
        ),
    )


def plot_packet_analysis(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis) -> None:
    """Render QUIC packet span and phase diagnostic panels."""

    plot_metrics(
        path,
        config,
        MetricPlot(
            analysis.packet_samples,
            analysis.packet_statistics,
            PACKET_METRICS,
            "QUIC packet diagnostics",
        ),
    )


def plot_metrics(
    path: pathlib.Path,
    config: ExperimentConfig,
    plot: MetricPlot,
) -> None:
    """Render distribution, percentile, and time-series panels for one metric layer."""

    fig, axes = plt.subplots(1, 3, figsize=(17, 5.5))
    colors = cast(tuple[tuple[float, ...], ...], getattr(plt.get_cmap("tab10"), "colors"))

    present = [metric for metric in plot.labels if metric in plot.statistics]
    if not present:
        raise ValueError("cannot plot a metric layer without samples")
    for index, metric in enumerate(present):
        metric_samples = plot.samples.filter(pl.col("metric") == metric)
        values_ms = (metric_samples["latency_us"] / 1_000).to_numpy()
        axes[0].ecdf(
            values_ms,
            label=plot.labels[metric],
            color=colors[index],
            linewidth=2,
        )
    axes[0].set_title("Latency distribution")
    axes[0].set_xlabel("Latency (ms)")
    axes[0].set_ylabel("ECDF")
    axes[0].grid(alpha=0.25)
    axes[0].legend(fontsize=8)

    percentiles = ("p50", "p95", "p99")
    width = 0.8 / len(present)
    x_positions = list(range(len(percentiles)))
    for index, metric in enumerate(present):
        summary = plot.statistics[metric]
        offset = (index - (len(present) - 1) / 2) * width
        axes[1].bar(
            [position + offset for position in x_positions],
            [float(summary[name]) / 1_000 for name in percentiles],
            width=width,
            label=plot.labels[metric],
            color=colors[index],
        )
    axes[1].set_xticks(x_positions, percentiles)
    axes[1].set_title("Tail percentiles")
    axes[1].set_ylabel("Latency (ms)")
    axes[1].grid(axis="y", alpha=0.25)

    for index, metric in enumerate(present):
        metric_samples = plot.samples.filter(pl.col("metric") == metric)
        axes[2].scatter(
            (metric_samples["elapsed_ms"] / 1_000).to_numpy(),
            (metric_samples["latency_us"] / 1_000).to_numpy(),
            label=plot.labels[metric],
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
        f"{plot.title} | "
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
    rx_quic_rows = (
        ("rx", "quic_header_parse", "RX QUIC Header Parse"),
        ("rx", "quic_routing", "RX QUIC Routing"),
        ("rx", "quic_header_unprotect", "RX QUIC Header Unprotect"),
        ("rx", "quic_payload_decrypt", "RX QUIC Payload Decrypt"),
        ("rx", "quic_frame_process", "RX QUIC Frame Process"),
    )
    moq_rows = (
        ("rx", "header_parse", "RX Header Parse"),
        ("rx", "create", "RX Create"),
        ("rx", "payload_read", "RX Payload Read"),
        ("tx", "clone", "TX Clone"),
        ("tx", "header_encode", "TX Header Encode"),
        ("tx", "payload_write", "TX Payload Write"),
    )
    tx_quic_rows = (
        ("tx", "quic_frame_encode", "TX QUIC Frame Encode"),
        ("tx", "quic_packet_encrypt", "TX QUIC Packet Encrypt"),
    )
    present = {(interval.direction, interval.phase) for timeline in timelines for interval in timeline.intervals}
    rx_quic_rows = tuple(row for row in rx_quic_rows if row[:2] in present)
    tx_quic_rows = tuple(row for row in tx_quic_rows if row[:2] in present)
    phase_rows = rx_quic_rows + moq_rows + tx_quic_rows
    positions = {(direction, phase): index for index, (direction, phase, _label) in enumerate(phase_rows)}
    labels = [label for _direction, _phase, label in phase_rows]

    figure_height = max(11.0, len(timelines) * len(phase_rows) * 0.28 + 2.5)
    fig, axes = plt.subplots(len(timelines), 1, figsize=(15, figure_height), sharex=True, squeeze=False)
    axes = axes[:, 0]
    minimum = min(interval.start_us for timeline in timelines for interval in timeline.intervals)
    maximum = max(interval.end_us for timeline in timelines for interval in timeline.intervals)
    padding = max(1.0, (maximum - minimum) * 0.03)
    x_min = min(0.0, minimum - padding)
    label_space = max(4.0, (maximum - minimum) * 0.12)
    x_max = max(1.0, maximum + label_space)
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
        phase_labels: dict[tuple[str, int, str], tuple[float, float, float]] = {}

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
                continue
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
            duration_us = interval.end_us - interval.start_us
            label_key = (interval.direction, interval.session_id, interval.phase)
            previous_total, previous_end, _ = phase_labels.get(label_key, (0.0, interval.end_us, y))
            phase_labels[label_key] = (previous_total + duration_us, max(previous_end, interval.end_us), y)

        for total_us, end_us, y in phase_labels.values():
            axis.annotate(
                f"{total_us:.2f}",
                xy=(end_us, y),
                xytext=(4, 0),
                textcoords="offset points",
                va="center",
                fontsize=8,
                color="#334155",
            )

        section_boundaries = (
            len(rx_quic_rows) - 0.5,
            len(rx_quic_rows) + len(moq_rows) - 0.5,
        )
        for boundary in section_boundaries:
            axis.axhline(boundary, color="#94A3B8", linewidth=0.8, alpha=0.8)

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
        axis.set_xlim(x_min, x_max)
        axis.grid(axis="x", color="#CBD5E1", alpha=0.7, linewidth=0.7)
        axis.set_axisbelow(True)
        selected = timeline.selection
        axis.set_title(
            f"{selected.statistic} {selected.target_us:.2f} µs | object ({selected.group_id}, {selected.object_id}) ",
            fontsize=10,
            loc="left",
        )

    axes[-1].set_xlabel("Elapsed from first RX QUIC packet start (µs)")
    fig.suptitle(
        f"QUIC packet and MoQ object timelines | {config.object_size} bytes | "
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

    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
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


@contextlib.contextmanager
def _managed_process(
    command: list[str],
    log: pathlib.Path,
    name: str,
) -> Iterator[subprocess.Popen[bytes]]:
    """Launch one process and always release its process group and log handle."""

    handle = log.open("wb")
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            command,
            cwd=log.parent,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        yield process
    finally:
        try:
            _stop(process, name)
        finally:
            handle.close()


def _build_experiment(
    config: ExperimentConfig,
    command: list[str],
    output: pathlib.Path,
) -> None:
    """Build the experiment binaries while keeping a local Quinn override isolated."""

    if config.skip_build:
        return

    build_log = output / "build.log"
    lock_path = config.repo / "Cargo.lock"
    lock_contents = lock_path.read_bytes() if config.quinn_path is not None else None
    try:
        with build_log.open("wb") as log:
            result = subprocess.run(
                command,
                cwd=config.repo,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
            )
    finally:
        # A path override changes Cargo's source identity. Keep it local to this build.
        if lock_contents is not None:
            lock_path.write_bytes(lock_contents)
    if result.returncode != 0:
        raise ExperimentError(f"build failed with status {result.returncode}; see {build_log}")


def _capture_trace(
    config: ExperimentConfig,
    commands: dict[str, list[str]],
    output: pathlib.Path,
) -> pathlib.Path:
    """Run the relay workload and return the flushed trace path."""

    with _managed_process(commands["relay"], output / "relay.log", "relay") as relay:
        wait_for_log(output / "relay.log", re.compile(r"\blistening\b"), relay, 15)

        with _managed_process(commands["publisher"], output / "publisher.log", "publisher") as publisher:
            wait_for_log(
                output / "publisher.log",
                re.compile(r"\bconnections=1\b"),
                publisher,
                15,
            )

            with _managed_process(commands["subscriber"], output / "subscriber.log", "subscriber") as subscriber:
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

    return output / "relay.jsonl"


def _validate_analysis(config: ExperimentConfig, analysis: Analysis) -> None:
    """Validate analysis requirements specific to a local Quinn checkout."""

    if config.quinn_path is None:
        return
    required = {"rx_routing", "rx_scheduling"}
    missing = sorted(required - analysis.packet_statistics.keys())
    if missing:
        raise TraceError(f"local Quinn trace is missing packet metrics: {', '.join(missing)}")


def _write_artifacts(
    output: pathlib.Path,
    config: ExperimentConfig,
    analysis: Analysis,
    commands: dict[str, list[str]],
) -> None:
    """Write every tabular, summary, and plot artifact for one analysis."""

    write_samples(
        output / "objects.csv",
        analysis.samples,
        ("group_id", "object_id", "metric", "copy_ordinal"),
    )
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
    write_summary(output / "summary.json", config, analysis, commands)
    plot_analysis(output / "latency.png", config, analysis)
    plot_quic_analysis(output / "quic_latency.png", config, analysis)
    plot_packet_analysis(output / "packet_latency.png", config, analysis)
    plot_object_timelines(output / "object_timeline.png", config, analysis.timelines)


def run_experiment(config: ExperimentConfig) -> pathlib.Path:
    """Build, capture, analyze, and report one relay latency experiment."""

    commands = {
        "build": build_workspace_command(config),
        "relay": build_relay_command(config),
        "publisher": build_publisher_command(config),
        "subscriber": build_subscriber_command(config),
    }
    output = config.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    _build_experiment(config, commands["build"], output)
    trace_path = _capture_trace(config, commands, output)
    analysis = analyze_trace(trace_path, config)
    _validate_analysis(config, analysis)
    _write_artifacts(output, config, analysis, commands)
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
    quinn_path: Annotated[
        pathlib.Path | None,
        typer.Option("--quinn-path", help="Build with a local Quinn checkout."),
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
            quinn_path=quinn_path,
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
        validate_quinn_path(config)
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
