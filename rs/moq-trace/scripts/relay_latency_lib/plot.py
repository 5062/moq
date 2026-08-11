from __future__ import annotations

import dataclasses
import pathlib
from typing import cast

import matplotlib

matplotlib.use("Agg")
import polars as pl
from matplotlib import pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

from .analysis import Analysis, ObjectTimeline

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


@dataclasses.dataclass(frozen=True)
class PlotOptions:
    """Run metadata displayed in plot titles."""

    relay_cpu: int | None
    subscribers: int
    object_size: int
    fps: int
    protocol: str


@dataclasses.dataclass(frozen=True)
class MetricPlot:
    """One metric layer and its plot presentation."""

    samples: pl.DataFrame
    statistics: dict[str, dict[str, float | int]]
    labels: dict[str, str]
    title: str


def plot_analysis(path: pathlib.Path, options: PlotOptions, analysis: Analysis) -> None:
    """Render ECDF, percentile, and time-series latency panels."""

    plot_metrics(
        path,
        options,
        MetricPlot(analysis.samples, analysis.statistics, METRICS, "MoQ relay latency"),
    )


def plot_quic_analysis(path: pathlib.Path, options: PlotOptions, analysis: Analysis) -> None:
    """Render QUIC-inclusive object metric panels."""

    plot_metrics(
        path,
        options,
        MetricPlot(
            analysis.quic_object_samples,
            analysis.quic_object_statistics,
            QUIC_OBJECT_METRICS,
            "QUIC-inclusive relay latency",
        ),
    )


def plot_packet_analysis(path: pathlib.Path, options: PlotOptions, analysis: Analysis) -> None:
    """Render QUIC packet span and phase diagnostic panels."""

    plot_metrics(
        path,
        options,
        MetricPlot(
            analysis.packet_samples,
            analysis.packet_statistics,
            PACKET_METRICS,
            "QUIC packet diagnostics",
        ),
    )


def plot_metrics(
    path: pathlib.Path,
    options: PlotOptions,
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

    affinity = "unpinned" if options.relay_cpu is None else f"pinned CPU {options.relay_cpu}"
    fig.suptitle(
        f"{plot.title} | "
        f"{affinity} | {options.subscribers} subscriber(s) | "
        f"{options.object_size} bytes | {options.fps} fps | {options.protocol}"
    )
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=160)
    plt.close(fig)


def plot_object_timelines(
    path: pathlib.Path,
    options: PlotOptions,
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
        f"QUIC packet and MoQ object timelines | {options.object_size} bytes | "
        f"{options.subscribers} subscriber(s) | {options.protocol}",
        fontsize=14,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.965))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=160)
    plt.close(fig)
