from __future__ import annotations

import json
import pathlib

from .analysis import AnalysisError, load_analysis
from .plot import (
    PerCopyCdfRun,
    PlotOptions,
    plot_analysis,
    plot_latency_cdf,
    plot_object_timelines,
    plot_packet_analysis,
    plot_packet_latency_cdf,
    plot_per_copy_latency_cdf,
    plot_quic_analysis,
)


class RenderError(RuntimeError):
    """Rust-produced experiment artifacts cannot be rendered."""


def _load_json(path: pathlib.Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RenderError(f"failed to load {path}: {error}") from error
    if not isinstance(value, dict):
        raise RenderError(f"{path} must contain a JSON object")
    return value


def _options(summary: dict) -> PlotOptions:
    workload = summary["workload"]
    affinity = summary["affinity"]
    return PlotOptions(
        relay_cpu=affinity.get("cpu") if affinity["mode"] == "single-core" else None,
        subscribers=int(workload["subscribers"]),
        object_size=int(workload["object_size"]),
        fps=int(workload["fps"]),
        protocol=str(summary["protocol"]),
    )


def render_run(output: pathlib.Path) -> None:
    """Render every figure for one Rust-produced experiment."""

    summary = _load_json(output / "summary.json")
    analysis = load_analysis(output / "analysis")
    options = _options(summary)
    plot_analysis(output / "latency.png", options, analysis)
    plot_quic_analysis(output / "quic_latency.png", options, analysis)
    plot_packet_analysis(output / "packet_latency.png", options, analysis)
    plot_latency_cdf(output / "latency_cdf.png", options, analysis)
    plot_packet_latency_cdf(output / "packet_latency_cdf.png", options, analysis)
    plot_object_timelines(output / "object_timeline.png", options, analysis.timelines)


def _format_byte_size(value: int) -> str:
    for divisor, suffix in ((1024 * 1024, "MiB"), (1024, "KiB")):
        if value % divisor == 0:
            return f"{value // divisor} {suffix}"
    return f"{value} bytes"


def render_comparison(output: pathlib.Path, stem: str, dimension: str) -> None:
    """Render one Rust-produced subscriber or object-size comparison."""

    summary = _load_json(output / f"{stem}_summary.json")
    values_key = "subscriber_counts" if dimension == "subscribers" else "object_sizes_bytes"
    values = [int(value) for value in summary[values_key]]
    entries = summary["runs"]
    if len(values) != len(entries):
        raise RenderError("comparison values and run directories have different lengths")

    runs = []
    run_summaries = []
    for value, entry in zip(values, entries, strict=True):
        run_output = output / entry["directory"]
        run_summary = _load_json(run_output / "summary.json")
        analysis = load_analysis(run_output / "analysis")
        label = (
            f"{value} {'subscriber' if value == 1 else 'subscribers'}"
            if dimension == "subscribers"
            else _format_byte_size(value)
        )
        runs.append(
            PerCopyCdfRun(
                label=label,
                samples=analysis.samples,
                quic_object_samples=analysis.quic_object_samples,
                statistics=analysis.statistics,
                quic_object_statistics=analysis.quic_object_statistics,
            )
        )
        run_summaries.append(run_summary)

    options = _options(run_summaries[0])
    if dimension == "subscribers":
        comparison = f"{options.object_size} bytes"
    else:
        subscribers = options.subscribers
        comparison = f"{subscribers} {'subscriber' if subscribers == 1 else 'subscribers'}"
    plot_per_copy_latency_cdf(
        output / f"{stem}_cdf.png",
        options,
        tuple(runs),
        comparison,
    )


def render(output: pathlib.Path) -> None:
    """Render one experiment or comparison directory."""

    output = output.resolve()
    try:
        if (output / "summary.json").is_file():
            render_run(output)
        elif (output / "per_copy_latency_summary.json").is_file():
            render_comparison(output, "per_copy_latency", "subscribers")
        elif (output / "object_size_latency_summary.json").is_file():
            render_comparison(output, "object_size_latency", "object_size")
        else:
            raise RenderError(f"{output} is not a Rust-produced experiment or comparison")
    except (AnalysisError, KeyError, TypeError, ValueError) as error:
        raise RenderError(f"failed to render {output}: {error}") from error
