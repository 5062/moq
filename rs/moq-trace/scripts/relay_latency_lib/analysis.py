from __future__ import annotations

import dataclasses
import pathlib

import polars as pl

from .trace import Direction, ObjectBoundaries, ObjectKey, ObjectRange, TraceError, TraceIndex

SAMPLE_SCHEMA = {
    "group_id": pl.Int64,
    "object_id": pl.Int64,
    "metric": pl.String,
    "copy_ordinal": pl.Int64,
    "elapsed_ms": pl.Float64,
    "latency_us": pl.Float64,
}


@dataclasses.dataclass(frozen=True)
class AnalysisOptions:
    """Workload parameters that affect trace analysis."""

    object_size: int
    subscribers: int
    warmup_seconds: float
    cooldown_seconds: float


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
class IntervalNs:
    """One traced or derived object lifecycle interval in nanoseconds."""

    direction: Direction
    session_id: int
    phase: str
    occurrence: int
    start_ns: int
    end_ns: int


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
        trace_ids = coverage.packet_trace_ids
        covered_packets = trace.packet_index.covering_packets(trace_ids)
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
        covered_phases = trace.packet_index.covering_phases(trace_ids)
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

    raw_intervals: list[IntervalNs] = []
    for session in indexed_object.sessions:
        for occurrence, interval in enumerate(session.lifecycles):
            raw_intervals.append(
                IntervalNs(
                    session.direction,
                    session.session_id,
                    "object",
                    occurrence,
                    interval.start_ns,
                    interval.end_ns,
                )
            )
        for phase in session.phases:
            raw_intervals.append(
                IntervalNs(
                    session.direction,
                    session.session_id,
                    phase.phase,
                    phase.occurrence,
                    phase.start_ns,
                    phase.end_ns,
                )
            )

    object_ranges = tuple(object_range for session in indexed_object.sessions for object_range in session.ranges)
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
        object_ranges = tuple(object_range for session in indexed_object.sessions for object_range in session.ranges)
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


def _select_steady_state(trace: TraceIndex, options: AnalysisOptions) -> SteadyState:
    """Select and validate payload objects inside the steady-state window."""

    object_index = {
        key: indexed_object.boundaries
        for key, indexed_object in trace.objects.items()
        if indexed_object.matches_payload(options.object_size)
    }
    if not object_index:
        raise TraceError(f"trace has no completed {options.object_size}-byte inbound objects")
    payload_keys = sorted(object_index)
    for key in payload_keys:
        rx_starts = object_index[key].rx_starts
        if len(rx_starts) != 1:
            raise TraceError(f"{key} has {len(rx_starts)} rx_starts")

    first_rx = min(object_index[key].rx_starts[0] for key in payload_keys)
    last_rx = max(object_index[key].rx_starts[0] for key in payload_keys)
    # RX starts define the workload clock, independent of fanout completion.
    window_start = first_rx + int(options.warmup_seconds * 1_000_000_000)
    window_end = last_rx - int(options.cooldown_seconds * 1_000_000_000)
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
            if count != options.subscribers:
                raise TraceError(f"{key} has {count} {name}, expected {options.subscribers}")
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


def analyze(trace: TraceIndex, options: AnalysisOptions) -> Analysis:
    """Calculate analysis outputs from one validated trace index."""

    steady_state = _select_steady_state(trace, options)
    samples = _moq_object_samples(steady_state)
    steady_keys = tuple(steady_state.objects)
    quic_object_samples = _quic_object_samples(
        trace,
        steady_keys,
        options.subscribers,
        steady_state.first_rx_ns,
    )
    selections = select_timeline_objects(samples)
    timelines = tuple(extract_object_timeline(trace, selection) for selection in selections)
    return Analysis(
        samples=samples,
        statistics=summarize(samples),
        quic_object_samples=quic_object_samples,
        quic_object_statistics=summarize(quic_object_samples),
        packet_samples=trace.packet_index.samples,
        packet_statistics=summarize(trace.packet_index.samples),
        packet_count=trace.packet_index.count,
        group_count=steady_state.group_count,
        timelines=timelines,
    )


def analyze_trace(
    path: pathlib.Path,
    options: AnalysisOptions,
) -> Analysis:
    """Read, index, and analyze a relay JSONL trace."""

    return analyze(TraceIndex.read(path), options)
