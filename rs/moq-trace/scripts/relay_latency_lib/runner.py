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
from collections.abc import Iterator

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from .analysis import Analysis, AnalysisOptions, analyze
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
from .trace import TraceError, TraceIndex

PROTOCOL = "moq-transport-19"

ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


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
            "correlated_object_copies": analysis.quic_object_samples.select("group_id", "object_id", "copy_ordinal")
            .unique()
            .height,
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


def _validate_analysis(analysis: Analysis) -> None:
    """Validate that Quinn emitted every required packet metric."""

    required = {"rx_routing", "rx_scheduling"}
    missing = sorted(required - analysis.packet_statistics.keys())
    if missing:
        raise TraceError(f"Quinn trace is missing packet metrics: {', '.join(missing)}")


def _write_artifacts(
    output: pathlib.Path,
    config: ExperimentConfig,
    analysis: Analysis,
    commands: dict[str, list[str]],
) -> None:
    """Write every tabular, summary, and plot artifact for one analysis."""

    plot_options = PlotOptions(
        relay_cpu=config.relay_cpu,
        subscribers=config.subscribers,
        object_size=config.object_size,
        fps=config.fps,
        protocol=PROTOCOL,
    )
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
    plot_analysis(output / "latency.png", plot_options, analysis)
    plot_quic_analysis(output / "quic_latency.png", plot_options, analysis)
    plot_packet_analysis(output / "packet_latency.png", plot_options, analysis)
    plot_latency_cdf(output / "latency_cdf.png", plot_options, analysis)
    plot_packet_latency_cdf(output / "packet_latency_cdf.png", plot_options, analysis)
    plot_object_timelines(output / "object_timeline.png", plot_options, analysis.timelines)


def run_subscriber_comparison(
    config: ExperimentConfig,
    subscriber_counts: tuple[int, ...],
) -> pathlib.Path:
    """Run one workload per subscriber count and compare delivery-copy latency."""

    if len(subscriber_counts) < 2:
        raise ValueError("subscriber comparison requires at least two counts")
    if len(set(subscriber_counts)) != len(subscriber_counts):
        raise ValueError("subscriber comparison counts must be unique")
    if any(count <= 0 for count in subscriber_counts):
        raise ValueError("subscriber comparison counts must be positive")

    output = config.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    runs: list[PerCopyCdfRun] = []
    combined_samples: list[pl.DataFrame] = []
    run_summaries = []
    for index, subscribers in enumerate(subscriber_counts):
        run_config = config.model_copy(
            update={
                "output": output / f"subscribers-{subscribers}",
                "subscribers": subscribers,
                "skip_build": config.skip_build or index > 0,
            }
        )
        run_output = run_experiment(run_config)
        summary = json.loads((run_output / "summary.json").read_text())
        object_samples = pl.read_csv(run_output / "objects.csv")
        quic_object_samples = pl.read_csv(run_output / "quic_objects.csv")
        runs.append(
            PerCopyCdfRun(
                subscribers=subscribers,
                samples=object_samples,
                quic_object_samples=quic_object_samples,
                statistics=summary["statistics_us"],
                quic_object_statistics=summary["quic_object_statistics_us"],
            )
        )
        for layer, metric, samples in (
            ("moq", "full_span", object_samples),
            ("quic", "quic_full_span", quic_object_samples),
        ):
            combined_samples.append(
                samples.filter(pl.col("metric") == metric).with_columns(
                    pl.lit(subscribers).alias("subscribers"),
                    pl.lit(layer).alias("layer"),
                )
            )
        run_summaries.append(
            {
                "subscribers": subscribers,
                "directory": run_output.name,
                "delivery_copies": summary["counts"]["correlated_object_copies"],
                "statistics_us": {
                    "full_span": summary["statistics_us"]["full_span"],
                    "quic_full_span": summary["quic_object_statistics_us"]["quic_full_span"],
                },
            }
        )

    samples = pl.concat(combined_samples)
    write_samples(
        output / "per_copy_latency.csv",
        samples,
        ("subscribers", "layer", "group_id", "object_id", "copy_ordinal"),
    )
    comparison_summary = {
        "sample_unit": "delivery copy",
        "subscriber_counts": list(subscriber_counts),
        "runs": run_summaries,
    }
    (output / "per_copy_latency_summary.json").write_text(
        json.dumps(comparison_summary, indent=2, sort_keys=True) + "\n"
    )
    plot_per_copy_latency_cdf(
        output / "per_copy_latency_cdf.png",
        PlotOptions(
            relay_cpu=config.relay_cpu,
            subscribers=subscriber_counts[0],
            object_size=config.object_size,
            fps=config.fps,
            protocol=PROTOCOL,
        ),
        tuple(runs),
    )
    return output


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
    trace = TraceIndex.read(trace_path)
    analysis = analyze(
        trace,
        AnalysisOptions(
            object_size=config.object_size,
            subscribers=config.subscribers,
            warmup_seconds=config.warmup,
            cooldown_seconds=config.cooldown,
        ),
    )
    _validate_analysis(analysis)
    _write_artifacts(output, config, analysis, commands)
    return output
