#!/usr/bin/env python3

from __future__ import annotations

import json
import pathlib
from typing import Annotated

import typer
from pydantic import ValidationError
from relay_latency_lib.runner import (
    ExperimentConfig,
    ExperimentError,
    default_output,
    run_experiment,
    run_object_size_comparison,
    run_subscriber_comparison,
    validate_cpu_affinity,
    validate_quinn_path,
)
from relay_latency_lib.analysis import AnalysisError

app = typer.Typer(add_completion=False, help="Measure local MoQ relay processing latency.")


def parse_subscriber_counts(value: str) -> tuple[int, ...]:
    """Parse a comma-separated subscriber comparison."""

    try:
        return tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as error:
        raise ValueError("--compare-subscribers must contain comma-separated integers") from error


def parse_object_sizes(value: str) -> tuple[int, ...]:
    """Parse comma-separated byte sizes with optional binary suffixes."""

    units = {"": 1, "b": 1, "k": 1024, "kb": 1024, "kib": 1024}
    sizes = []
    for item in value.split(","):
        token = item.strip().lower()
        if not token:
            continue
        suffix = next((unit for unit in ("kib", "kb", "k", "b") if token.endswith(unit)), "")
        number = token[: -len(suffix)] if suffix else token
        if not number.isdigit():
            raise ValueError("--compare-object-sizes must contain comma-separated byte sizes")
        sizes.append(int(number) * units[suffix])
    return tuple(sizes)


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


@app.command()
def main(
    relay_cpu: Annotated[int | None, typer.Option("--relay-cpu")] = None,
    subscribers: Annotated[int, typer.Option("--subscribers")] = 1,
    compare_subscribers: Annotated[
        str | None,
        typer.Option(
            "--compare-subscribers",
            help="Run a per-copy latency comparison, for example 1,50,100.",
        ),
    ] = None,
    compare_object_sizes: Annotated[
        str | None,
        typer.Option(
            "--compare-object-sizes",
            help="Run a per-copy latency comparison, for example 16k,64k,256k.",
        ),
    ] = None,
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
        if compare_subscribers is not None and compare_object_sizes is not None:
            raise ValueError("comparison options are mutually exclusive")
        if compare_subscribers is not None:
            counts = parse_subscriber_counts(compare_subscribers)
            result = run_subscriber_comparison(config, counts)
        elif compare_object_sizes is not None:
            sizes = parse_object_sizes(compare_object_sizes)
            result = run_object_size_comparison(config, sizes)
        else:
            result = run_experiment(config)
        summary_path = result / "summary.json"
        summary = json.loads(summary_path.read_text()) if summary_path.is_file() else None
    except (AnalysisError, ExperimentError, OSError, ValidationError, ValueError) as error:
        typer.echo(f"error: {error}", err=True)
        typer.echo(f"run directory: {output.resolve()}", err=True)
        raise typer.Exit(1) from error

    if summary is None:
        artifact_stem = (
            "object_size_latency" if compare_object_sizes is not None else "per_copy_latency"
        )
        for name in (
            f"{artifact_stem}.csv",
            f"{artifact_stem}_summary.json",
            f"{artifact_stem}_cdf.png",
        ):
            typer.echo(f"{name}: {(result / name).resolve()}")
        return

    print_statistics("MoQ object metrics", summary["statistics_us"])
    print_statistics("QUIC-inclusive object metrics", summary["quic_object_statistics_us"])
    print_statistics("QUIC packet diagnostics", summary["quic_packet_statistics_us"])
    for name in (
        "objects.csv",
        "quic_objects.csv",
        "quic_packets.csv",
        "analysis.json",
        "summary.json",
        "latency.png",
        "quic_latency.png",
        "packet_latency.png",
        "latency_cdf.png",
        "packet_latency_cdf.png",
        "object_timeline.png",
    ):
        typer.echo(f"{name}: {(result / name).resolve()}")


if __name__ == "__main__":
    app()
