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
from collections import defaultdict
from typing import Annotated, BinaryIO

import matplotlib
import pandas as pd
import typer
from pydantic import BaseModel, ConfigDict, Field, ValidationError

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402

PROTOCOL = "moq-transport-19"

METRICS = {
    "forward_start": "Forward start",
    "model_handoff": "Model handoff",
    "drain_gap": "Drain gap",
    "full_span": "Full relay span",
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
class Analysis:
    """Validated latency samples and aggregate trace counts."""

    samples: pd.DataFrame
    statistics: dict[str, dict[str, float | int]]
    packet_count: int
    socket_count: int
    group_count: int


class TraceError(RuntimeError):
    """A trace is malformed, incomplete, or does not match the workload."""


def summarize(samples: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    """Summarize latency samples by metric."""

    if samples.empty:
        raise ValueError("cannot summarize an empty sample")
    grouped = samples.groupby("metric", sort=False)["latency_us"]
    statistics = grouped.agg(count="count", mean="mean", max="max").join(
        grouped.quantile((0.50, 0.95, 0.99)).unstack().rename(columns={0.50: "p50", 0.95: "p95", 0.99: "p99"})
    )
    statistics = statistics[["count", "mean", "p50", "p95", "p99", "max"]].round(12)
    return {
        str(metric): {
            "count": int(row["count"]),
            "mean": float(row["mean"]),
            "p50": float(row["p50"]),
            "p95": float(row["p95"]),
            "p99": float(row["p99"]),
            "max": float(row["max"]),
        }
        for metric, row in statistics.iterrows()
    }


def _load_events(path: pathlib.Path) -> list[dict]:
    events = []
    try:
        with path.open() as trace:
            for line_number, line in enumerate(trace, 1):
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as error:
                    raise TraceError(f"malformed JSON on line {line_number}: {error}") from error
    except OSError as error:
        raise TraceError(f"failed to read trace {path}: {error}") from error
    return events


def _validate_scopes(
    events: list[dict],
    start_type: str,
    end_type: str,
    require_success: bool,
) -> int:
    starts = {event["trace_id"] for event in events if event.get("type") == start_type}
    ends = {event["trace_id"] for event in events if event.get("type") == end_type}
    if starts != ends:
        raise TraceError(f"{start_type}/{end_type} trace IDs do not match: {len(starts)} starts, {len(ends)} ends")
    if require_success and any(event.get("outcome") != "success" for event in events if event.get("type") == end_type):
        raise TraceError(f"{end_type} contains unsuccessful outcomes")
    return len(starts)


def analyze_trace(
    path: pathlib.Path,
    subscribers: int,
    object_size: int,
    warmup: float,
    cooldown: float,
) -> Analysis:
    """Parse, validate, trim, and summarize a relay JSONL trace."""

    events = _load_events(path)
    packet_count = _validate_scopes(events, "quic_packet_start", "quic_packet_end", True)
    socket_count = _validate_scopes(events, "udp_socket_start", "udp_socket_end", False)
    boundaries = {
        name: defaultdict(list)
        for name in (
            "rx_starts",
            "rx_create_done",
            "rx_ends",
            "tx_starts",
            "tx_clone_starts",
            "tx_ends",
        )
    }
    payload_keys = set()
    for event in events:
        event_type = event.get("type")
        if not event_type or not event_type.startswith("moq_object_"):
            continue
        try:
            key = (int(event["group_id"]), int(event["object_id"]))
            timestamp = int(event["timestamp_ns"])
        except (KeyError, TypeError, ValueError) as error:
            raise TraceError(f"object event is missing identity: {event}") from error
        direction = event.get("direction")
        if event_type == "moq_object_start":
            boundary = "rx_starts" if direction == "rx" else "tx_starts"
            boundaries[boundary][key].append(timestamp)
        elif event_type == "moq_object_end":
            boundary = "rx_ends" if direction == "rx" else "tx_ends"
            boundaries[boundary][key].append(timestamp)
            if direction == "rx" and event.get("payload_bytes") == object_size:
                payload_keys.add(key)
        elif event_type == "moq_object_phase":
            phase = event.get("phase")
            edge = event.get("edge")
            if direction == "rx" and phase == "create" and edge == "done":
                boundaries["rx_create_done"][key].append(timestamp)
            elif direction == "tx" and phase == "clone" and edge == "start":
                boundaries["tx_clone_starts"][key].append(timestamp)

    if not payload_keys:
        raise TraceError(f"trace has no completed {object_size}-byte inbound objects")
    for key in payload_keys:
        if len(boundaries["rx_starts"][key]) != 1:
            raise TraceError(f"{key} has {len(boundaries['rx_starts'][key])} rx_starts")

    first_rx = min(boundaries["rx_starts"][key][0] for key in payload_keys)
    last_rx = max(boundaries["rx_starts"][key][0] for key in payload_keys)
    window_start = first_rx + int(warmup * 1_000_000_000)
    window_end = last_rx - int(cooldown * 1_000_000_000)
    keys = sorted(key for key in payload_keys if window_start <= boundaries["rx_starts"][key][0] <= window_end)
    if not keys:
        raise TraceError("steady-state window contains no complete objects")
    for key in keys:
        for name in ("rx_create_done", "rx_ends"):
            if len(boundaries[name][key]) != 1:
                raise TraceError(f"{key} has {len(boundaries[name][key])} {name}")
        for name in ("tx_starts", "tx_clone_starts", "tx_ends"):
            if len(boundaries[name][key]) != subscribers:
                raise TraceError(f"{key} has {len(boundaries[name][key])} {name}, expected {subscribers}")
    groups = sorted({group for group, _object in keys})
    if groups != list(range(groups[0], groups[-1] + 1)):
        raise TraceError("steady-state groups are not contiguous")

    rows = []
    for group_id, object_id in keys:
        key = (group_id, object_id)
        rx_start = boundaries["rx_starts"][key][0]
        rx_create = boundaries["rx_create_done"][key][0]
        rx_end = boundaries["rx_ends"][key][0]
        elapsed_ms = (rx_start - first_rx) / 1_000_000
        metric_boundaries = {
            "forward_start": (rx_start, boundaries["tx_starts"][key]),
            "model_handoff": (rx_create, boundaries["tx_clone_starts"][key]),
            "drain_gap": (rx_end, boundaries["tx_ends"][key]),
            "full_span": (rx_start, boundaries["tx_ends"][key]),
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

    samples = pd.DataFrame.from_records(
        rows,
        columns=("group_id", "object_id", "metric", "copy_ordinal", "elapsed_ms", "latency_us"),
    )
    return Analysis(
        samples=samples,
        statistics=summarize(samples),
        packet_count=packet_count,
        socket_count=socket_count,
        group_count=len(groups),
    )


def write_csv(path: pathlib.Path, analysis: Analysis) -> None:
    """Write deterministic long-form object latency samples."""

    path.parent.mkdir(parents=True, exist_ok=True)
    analysis.samples.sort_values(["group_id", "object_id", "metric", "copy_ordinal"]).to_csv(
        path, index=False, float_format="%.6f"
    )


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
            "socket_operations": analysis.socket_count,
        },
        "statistics_us": analysis.statistics,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def plot_analysis(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis) -> None:
    """Render ECDF, percentile, and time-series latency panels."""

    fig, axes = plt.subplots(1, 3, figsize=(17, 5.5))
    colors = plt.get_cmap("tab10").colors

    for index, (metric, samples) in enumerate(analysis.samples.groupby("metric", sort=False)):
        values_ms = samples["latency_us"] / 1_000
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

    for index, (metric, samples) in enumerate(analysis.samples.groupby("metric", sort=False)):
        axes[2].scatter(
            samples["elapsed_ms"] / 1_000,
            samples["latency_us"] / 1_000,
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
    write_csv(output / "objects.csv", analysis)
    write_summary(output / "summary.json", config, analysis, commands)
    plot_analysis(output / "latency.png", config, analysis)
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
    except (ExperimentError, OSError, ValidationError, ValueError) as error:
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
    for name in ("objects.csv", "summary.json", "latency.png"):
        typer.echo(f"{name}: {(result / name).resolve()}")


if __name__ == "__main__":
    app()
