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
import polars as pl
import typer
from pydantic import BaseModel, ConfigDict, Field, ValidationError

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402

PROTOCOL = "moq-transport-19"
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")

METRICS = {
    "forward_start": "Forward start",
    "model_handoff": "Model handoff",
    "drain_gap": "Drain gap",
    "full_span": "Full relay span",
}

BOUNDARIES = (
    "rx_starts",
    "rx_create_done",
    "rx_ends",
    "tx_starts",
    "tx_clone_starts",
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
class ObjectTimeline:
    """All traced intervals for one selected logical object."""

    selection: TimelineSelection
    intervals: tuple[TimelineInterval, ...]
    slowest_session_id: int


@dataclasses.dataclass(frozen=True)
class Analysis:
    """Validated latency samples and aggregate trace counts."""

    samples: pl.DataFrame
    statistics: dict[str, dict[str, float | int]]
    packet_count: int
    socket_count: int
    group_count: int
    events: pl.DataFrame
    steady_keys: tuple[tuple[int, int], ...]
    selections: tuple[TimelineSelection, ...]


class TraceError(RuntimeError):
    """A trace is malformed, incomplete, or does not match the workload."""


def select_timeline_objects(samples: pl.DataFrame) -> tuple[TimelineSelection, ...]:
    """Select real objects nearest mean, median, and p99 slowest-copy full span."""

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
    tx_objects = [interval for interval in intervals if interval.direction == "tx" and interval.phase == "object"]
    if not tx_objects:
        raise TraceError(f"selected object ({selection.group_id}, {selection.object_id}) has no TX lifecycle")
    slowest = min(tx_objects, key=lambda interval: (-interval.end_us, interval.session_id))
    return ObjectTimeline(selection=selection, intervals=intervals, slowest_session_id=slowest.session_id)


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
        .when(
            (pl.col("type") == "moq_object_phase")
            & (pl.col("direction") == "rx")
            & (pl.col("phase") == "create")
            & (pl.col("edge") == "done")
        )
        .then(pl.lit("rx_create_done"))
        .when(
            (pl.col("type") == "moq_object_phase")
            & (pl.col("direction") == "tx")
            & (pl.col("phase") == "clone")
            & (pl.col("edge") == "start")
        )
        .then(pl.lit("tx_clone_starts"))
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


def analyze_trace(
    path: pathlib.Path,
    subscribers: int,
    object_size: int,
    warmup: float,
    cooldown: float,
) -> Analysis:
    """Parse, validate, trim, and summarize a relay JSONL trace."""

    events = _read_trace(path)
    packet_count = _validate_scopes(events, "quic_packet_start", "quic_packet_end", True)
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
    window_start = first_rx + int(warmup * 1_000_000_000)
    window_end = last_rx - int(cooldown * 1_000_000_000)
    keys = [key for key in payload_keys if window_start <= object_index[key]["rx_starts"][0] <= window_end]
    if not keys:
        raise TraceError("steady-state window contains no complete objects")
    for key in keys:
        obj = object_index[key]
        for name in ("rx_create_done", "rx_ends"):
            if len(obj[name]) != 1:
                raise TraceError(f"{key} has {len(obj[name])} {name}")
        for name in ("tx_starts", "tx_clone_starts", "tx_ends"):
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
        rx_create = obj["rx_create_done"][0]
        rx_end = obj["rx_ends"][0]
        elapsed_ms = (rx_start - first_rx) / 1_000_000
        metric_boundaries = {
            "forward_start": (rx_start, obj["tx_starts"]),
            "model_handoff": (rx_create, obj["tx_clone_starts"]),
            "drain_gap": (rx_end, obj["tx_ends"]),
            "full_span": (rx_start, obj["tx_ends"]),
        }
        for metric in METRICS:
            origin, targets = metric_boundaries[metric]
            for ordinal, target in enumerate(sorted(targets)):
                rows.append(
                    {
                        "group_id": group_id,
                        "object_id": object_id,
                        "metric": metric,
                        "copy_ordinal": ordinal,
                        "elapsed_ms": elapsed_ms,
                        "latency_us": (target - origin) / 1_000,
                    }
                )

    samples = pl.DataFrame(rows, schema=SAMPLE_SCHEMA)
    return Analysis(
        samples=samples,
        statistics=summarize(samples),
        packet_count=packet_count,
        socket_count=socket_count,
        group_count=len(groups),
        events=events,
        steady_keys=tuple(keys),
        selections=select_timeline_objects(samples),
    )


def write_csv(path: pathlib.Path, analysis: Analysis) -> None:
    """Write deterministic long-form object latency samples."""

    path.parent.mkdir(parents=True, exist_ok=True)
    analysis.samples.sort("group_id", "object_id", "metric", "copy_ordinal").write_csv(path, float_precision=6)


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
        },
        "statistics_us": analysis.statistics,
        "timeline_objects": [
            {
                "statistic": timeline.selection.statistic,
                "target_us": timeline.selection.target_us,
                "group_id": timeline.selection.group_id,
                "object_id": timeline.selection.object_id,
                "actual_us": timeline.selection.actual_us,
                "slowest_session_id": timeline.slowest_session_id,
            }
            for timeline in timelines
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def plot_analysis(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis) -> None:
    """Render ECDF, percentile, and time-series latency panels."""

    fig, axes = plt.subplots(1, 3, figsize=(17, 5.5))
    colors = plt.get_cmap("tab10").colors

    for index, metric in enumerate(METRICS):
        samples = analysis.samples.filter(pl.col("metric") == metric)
        values_ms = (samples["latency_us"] / 1_000).to_numpy()
        axes[0].ecdf(
            values_ms,
            label=METRICS.get(metric, metric),
            color=colors[index],
            linewidth=2,
        )
    axes[0].set_title("Latency distribution")
    axes[0].set_xlabel("Latency (ms)")
    axes[0].set_ylabel("ECDF")
    axes[0].grid(alpha=0.25)
    axes[0].legend(fontsize=8)

    percentiles = ("p50", "p95", "p99")
    width = 0.8 / len(analysis.statistics)
    x_positions = list(range(len(percentiles)))
    for index, (metric, summary) in enumerate(analysis.statistics.items()):
        offset = (index - (len(analysis.statistics) - 1) / 2) * width
        axes[1].bar(
            [position + offset for position in x_positions],
            [float(summary[name]) / 1_000 for name in percentiles],
            width=width,
            label=METRICS.get(metric, metric),
            color=colors[index],
        )
    axes[1].set_xticks(x_positions, percentiles)
    axes[1].set_title("Tail percentiles")
    axes[1].set_ylabel("Latency (ms)")
    axes[1].grid(axis="y", alpha=0.25)

    for index, metric in enumerate(METRICS):
        samples = analysis.samples.filter(pl.col("metric") == metric)
        axes[2].scatter(
            (samples["elapsed_ms"] / 1_000).to_numpy(),
            (samples["latency_us"] / 1_000).to_numpy(),
            label=METRICS.get(metric, metric),
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
        "MoQ relay latency | "
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
    phase_order = {
        "object": 0,
        "header_parse": 1,
        "create": 2,
        "payload_read": 3,
        "clone": 1,
        "header_encode": 2,
        "payload_write": 3,
    }

    def row_keys(timeline: ObjectTimeline) -> list[tuple[str, int, str]]:
        return sorted(
            {(interval.direction, interval.session_id, interval.phase) for interval in timeline.intervals},
            key=lambda key: (
                0 if key[0] == "rx" else 1,
                key[1],
                phase_order.get(key[2], 99),
                key[2],
            ),
        )

    rows_by_timeline = [row_keys(timeline) for timeline in timelines]
    max_rows = max(len(rows) for rows in rows_by_timeline)
    figure_height = max(11.0, len(timelines) * max_rows * 0.28 + 2.5)
    fig, axes = plt.subplots(len(timelines), 1, figsize=(15, figure_height), sharex=True, squeeze=False)
    axes = axes[:, 0]
    maximum = max(interval.end_us for timeline in timelines for interval in timeline.intervals)
    x_limit = max(1.0, maximum * 1.05)
    colors = {"rx": "#2563EB", "tx": "#D97706"}
    lifecycle_color = "#64748B"

    for axis, timeline, keys in zip(axes, timelines, rows_by_timeline, strict=True):
        positions = {key: index for index, key in enumerate(keys)}
        for interval in timeline.intervals:
            key = (interval.direction, interval.session_id, interval.phase)
            y = positions[key]
            color = lifecycle_color if interval.phase == "object" else colors[interval.direction]
            axis.broken_barh(
                [(interval.start_us, interval.end_us - interval.start_us)],
                (y - 0.32, 0.64),
                facecolors=color,
                edgecolors="#334155",
                linewidth=0.7,
                alpha=0.88,
            )
            if interval.phase == "object":
                axis.scatter(interval.start_us, y, marker=">", color="#0F172A", s=22, zorder=3)
                axis.scatter(interval.end_us, y, marker="|", color="#0F172A", s=55, zorder=3)

        labels = []
        for direction, session_id, phase in keys:
            prefix = "RX" if direction == "rx" else f"TX s{session_id}"
            labels.append(f"{prefix} {phase}")
        axis.set_yticks(range(len(keys)), labels, fontsize=8)
        axis.set_ylim(len(keys) - 0.5, -0.5)
        axis.set_xlim(0, x_limit)
        axis.grid(axis="x", color="#CBD5E1", alpha=0.7, linewidth=0.7)
        axis.set_axisbelow(True)
        selected = timeline.selection
        axis.set_title(
            f"{selected.statistic} target {selected.target_us:.2f} µs | "
            f"object ({selected.group_id}, {selected.object_id}) | "
            f"actual {selected.actual_us:.2f} µs | slowest TX s{timeline.slowest_session_id}",
            fontsize=10,
            loc="left",
        )

    axes[-1].set_xlabel("Elapsed from RX object start (µs)")
    fig.suptitle(
        "MoQ relay object lifecycle timelines\n"
        f"Slowest-copy mean, median, and p99 representatives | {config.object_size} bytes | "
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
    """Build, run, validate, analyze, and visualize one experiment."""

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
    write_summary(output / "summary.json", config, analysis, commands, timelines)
    plot_analysis(output / "latency.png", config, analysis)
    plot_object_timelines(output / "object_timeline.png", config, timelines)
    return output


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

    typer.echo("metric              count    mean_us     p50_us     p95_us     p99_us")
    for metric, values in summary["statistics_us"].items():
        typer.echo(
            f"{metric:18} {values['count']:6d} "
            f"{values['mean']:10.2f} {values['p50']:10.2f} "
            f"{values['p95']:10.2f} {values['p99']:10.2f}"
        )
    for name in ("objects.csv", "summary.json", "latency.png", "object_timeline.png"):
        typer.echo(f"{name}: {(result / name).resolve()}")


if __name__ == "__main__":
    app()
