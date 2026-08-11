from __future__ import annotations

import dataclasses
import pathlib
from collections.abc import Iterator, Mapping
from typing import Literal, TypedDict, cast

import polars as pl

Direction = Literal["rx", "tx"]

ObjectPhaseName = Literal[
    "header_parse",
    "create",
    "payload_read",
    "frame_commit",
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
    "rx": frozenset({"header_parse", "create", "payload_read", "frame_commit"}),
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
class ObjectPhaseInterval:
    """One paired object phase interval within a session."""

    phase: ObjectPhaseName
    occurrence: int
    start_ns: int
    end_ns: int


@dataclasses.dataclass(frozen=True)
class ObjectSession:
    """Validated lifecycle, phases, and transport ranges for one object session."""

    direction: Direction
    session_id: int
    lifecycles: tuple[TimeRangeNs, ...]
    phases: tuple[ObjectPhaseInterval, ...]
    ranges: tuple[ObjectRange, ...]


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
    """Validated sessions and boundaries for one logical object."""

    key: ObjectKey
    sessions: tuple[ObjectSession, ...]
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
class PacketIndex:
    """Validated packet data exposed through identity-based queries."""

    _by_id: dict[int, Packet]
    _frames: tuple[StreamFrame, ...]
    _phases_by_packet: dict[int, tuple[PacketPhase, ...]]
    _samples: pl.DataFrame

    @property
    def count(self) -> int:
        """Return the number of indexed packets."""

        return len(self._by_id)

    @property
    def samples(self) -> pl.DataFrame:
        """Return packet and packet-phase latency samples."""

        return self._samples

    def packet(self, trace_id: int) -> Packet:
        """Return one packet by trace ID."""

        packet = self._by_id.get(trace_id)
        if packet is None:
            raise TraceError(f"packet index has no packet {trace_id}")
        return packet

    def phases(self, trace_id: int) -> tuple[PacketPhase, ...]:
        """Return the traced phases for one packet."""

        return self._phases_by_packet.get(trace_id, ())

    def stream_frames(self) -> Iterator[StreamFrame]:
        """Iterate over validated STREAM frames."""

        return iter(self._frames)

    def covering_packets(self, trace_ids: tuple[int, ...]) -> tuple[Packet, ...]:
        """Return covering packets in lifecycle order."""

        return tuple(
            sorted(
                (self.packet(trace_id) for trace_id in trace_ids),
                key=lambda packet: (packet.start_ns, packet.trace_id),
            )
        )

    def covering_phases(self, trace_ids: tuple[int, ...]) -> tuple[PacketPhase, ...]:
        """Return phases for covering packets in lifecycle order."""

        return tuple(
            sorted(
                (phase for trace_id in trace_ids for phase in self.phases(trace_id)),
                key=lambda phase: (phase.start_ns, phase.packet.trace_id, phase.phase, phase.occurrence),
            )
        )


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


@dataclasses.dataclass
class IndexedObjectBuilder:
    """Mutable object accumulator used only during trace ingestion."""

    scopes: dict[tuple[Direction, int], ObjectScope] = dataclasses.field(default_factory=dict)
    rx_starts: list[int] = dataclasses.field(default_factory=list)
    rx_ends: list[int] = dataclasses.field(default_factory=list)
    tx_starts: list[int] = dataclasses.field(default_factory=list)
    tx_ends: list[int] = dataclasses.field(default_factory=list)
    rx_payload_bytes: set[int] = dataclasses.field(default_factory=set)

    def add(self, row: TraceRow, direction: Direction, label: str) -> None:
        """Validate and collect one logical object event."""

        session_id = _required_int(row, "session_id", label)
        scope_label = f"{direction} session {session_id} object"
        self.scopes.setdefault((direction, session_id), ObjectScope()).add(row, direction, scope_label)

        event_type = _required_text(row, "type", label)
        if event_type == "moq_object_start":
            timestamp_ns = _required_int(row, "timestamp_ns", label)
            (self.rx_starts if direction == "rx" else self.tx_starts).append(timestamp_ns)
        elif event_type == "moq_object_end":
            timestamp_ns = _required_int(row, "timestamp_ns", label)
            (self.rx_ends if direction == "rx" else self.tx_ends).append(timestamp_ns)
            if direction == "rx":
                self.rx_payload_bytes.add(_required_int(row, "payload_bytes", label))

    def finish(self, key: ObjectKey) -> IndexedObject:
        """Freeze the accumulated sessions into one indexed object."""

        sessions = []
        for (direction, session_id), scope in sorted(self.scopes.items()):
            starts = sorted(scope.starts_ns)
            completions = sorted(scope.completions_ns)
            label = f"{direction} session {session_id} object"
            if len(starts) != len(completions):
                raise TraceError(f"{label} has {len(starts)} starts and {len(completions)} completions")
            lifecycles = tuple(
                _time_range(start_ns, end_ns, label) for start_ns, end_ns in zip(starts, completions, strict=True)
            )
            phases = tuple(
                ObjectPhaseInterval(phase, occurrence, interval.start_ns, interval.end_ns)
                for phase, phase_scope in sorted(scope.phases.items())
                for occurrence, interval in enumerate(_pair_phase_scope(phase_scope, f"{label} {phase}"))
            )
            ranges = tuple(_object_range(row) for row in scope.completed_rows)
            sessions.append(ObjectSession(direction, session_id, lifecycles, phases, ranges))

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
            sessions=tuple(sessions),
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
) -> PacketIndex:
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

    return PacketIndex(
        _by_id=packet_by_id,
        _frames=tuple(frames),
        _phases_by_packet={trace_id: tuple(phases) for trace_id, phases in indexed_phases.items()},
        _samples=pl.DataFrame(metric_rows, schema=PACKET_SAMPLE_SCHEMA),
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
    def from_packets(cls, packets: PacketIndex) -> CoverageIndex:
        """Build one transport-stream index from validated packets."""

        grouped: dict[StreamKey, list[StreamFrame]] = {}
        for frame in packets.stream_frames():
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

    packet_index: PacketIndex
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

    packet_index = _finish_packets(packet_scopes, phase_scopes, stream_rows)
    objects = {key: builder.finish(key) for key, builder in object_builders.items()}
    return TraceIndex(
        packet_index=packet_index,
        objects=objects,
        coverage=CoverageIndex.from_packets(packet_index),
    )


def _read_trace(path: pathlib.Path) -> pl.DataFrame:
    """Read the consumed trace fields with stable nullable types."""

    try:
        return pl.read_ndjson(path, schema=TRACE_SCHEMA)
    except (OSError, pl.exceptions.PolarsError) as error:
        raise TraceError(f"failed to read trace {path}: {error}") from error
