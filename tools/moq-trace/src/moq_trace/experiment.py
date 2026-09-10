"""Run local or two-host relay latency experiments."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import pathlib
import shlex

from .analyze import run as analyze
from .capture import LttngSession, ManagedProcess, wait_for_log
from .config import ComparisonConfig, ExperimentConfig
from .render import render

PROTOCOL = "moq-transport-19"


class ExperimentError(RuntimeError):
    """Experiment configuration, capture, or analysis failed."""


@dataclasses.dataclass(frozen=True)
class Commands:
    """Exact commands used by one workload."""

    relay: tuple[str, ...]
    publisher: tuple[str, ...]
    subscriber: tuple[str, ...]


def _bench_command(config: ExperimentConfig, url: str, binary: str) -> list[str]:
    return [
        binary,
        "--client-connect",
        url,
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


def commands(config: ExperimentConfig) -> Commands:
    """Construct exact argv arrays without invoking a shell."""

    relay_binary = str(config.relay_bin)
    bench_binary = str(config.bench_bin)
    relay = [
        relay_binary,
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
    ]
    if config.relay_cpu is not None:
        relay[:0] = ["taskset", "-c", str(config.relay_cpu)]

    local_url = f"https://localhost:{config.port}"
    publisher = _bench_command(config, local_url, bench_binary)
    publisher.extend(["--name", "relay-latency", "--connections", "1", "--broadcasts", "1", "--subscribe", "0"])

    subscriber_binary = config.subscriber.binary if config.subscriber is not None else bench_binary
    subscriber = _bench_command(config, config.relay_url or local_url, subscriber_binary)
    total = config.warmup_seconds + config.duration_seconds + config.cooldown_seconds
    subscriber.extend(
        [
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
    )
    if config.subscriber is not None:
        remote = f"exec {shlex.join(subscriber)}"
        if config.subscriber.workdir is not None:
            remote = f"cd {shlex.quote(config.subscriber.workdir)} && {remote}"
        subscriber = [
            "ssh",
            "-T",
            "-o",
            "BatchMode=yes",
            config.subscriber.ssh,
            remote,
        ]
    return Commands(tuple(relay), tuple(publisher), tuple(subscriber))


def _validate_environment(config: ExperimentConfig) -> None:
    if config.relay_cpu is not None and hasattr(os, "sched_getaffinity"):
        if config.relay_cpu not in os.sched_getaffinity(0):
            raise ExperimentError(f"relay CPU {config.relay_cpu} is unavailable to this process")


def _capture(config: ExperimentConfig, command: Commands, output: pathlib.Path) -> tuple[pathlib.Path, int]:
    ctf = output / "trace"
    session = LttngSession(ctf)
    processes: list[ManagedProcess] = []
    try:
        relay = ManagedProcess("relay", command.relay, output, output / "relay.log")
        processes.append(relay)
        wait_for_log(output / "relay.log", relay, lambda value: "listening" in value, "listening", 15)
        session.wait_for_provider(relay.pid)
        session.start(relay.pid)

        subscriber = ManagedProcess("subscriber", command.subscriber, output, output / "subscriber.log")
        processes.append(subscriber)
        connections = f"connections={config.subscribers}"
        wait_for_log(
            output / "subscriber.log",
            subscriber,
            lambda value: connections in value,
            "subscriber connections",
            20,
        )

        publisher = ManagedProcess("publisher", command.publisher, output, output / "publisher.log")
        processes.append(publisher)
        wait_for_log(
            output / "publisher.log",
            publisher,
            lambda value: "connections=1" in value,
            "publisher connection",
            15,
        )
        subscriptions = f"subscriptions={config.subscribers}"
        wait_for_log(
            output / "subscriber.log",
            subscriber,
            lambda value: connections in value and subscriptions in value,
            "subscriber connections and subscriptions",
            20,
        )
        subscriber.wait(config.warmup_seconds + config.duration_seconds + config.cooldown_seconds + 15)
        processes.remove(subscriber)
        subscriber.log_handle.close()
        publisher.stop(True)
        processes.remove(publisher)
        relay.stop(True)
        processes.remove(relay)
        session.finish()
        return ctf, relay.pid
    finally:
        for process in reversed(processes):
            process.close()
        session.close()


def _file_hash(path: pathlib.Path) -> str | None:
    try:
        with path.open("rb") as source:
            digest = hashlib.sha256()
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
            return digest.hexdigest()
    except OSError:
        return None


def _write_json(path: pathlib.Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n")


def run(config: ExperimentConfig) -> pathlib.Path:
    """Capture, analyze, and optionally render one workload."""

    _validate_environment(config)
    output = config.output.resolve()
    if output.exists():
        raise ExperimentError(f"run output already exists: {output}")
    output.mkdir(parents=True)
    command = commands(config)
    ctf, relay_pid = _capture(config, command, output)
    report = analyze(
        ctf,
        output / "analysis.duckdb",
        config.object_size,
        config.subscribers,
        int(config.warmup_seconds * 1_000_000_000),
        int(config.cooldown_seconds * 1_000_000_000),
        relay_pid,
    )

    run_record = {
        "schema_revision": 1,
        "protocol": PROTOCOL,
        "affinity": (
            {"mode": "unpinned"} if config.relay_cpu is None else {"mode": "single-core", "cpu": config.relay_cpu}
        ),
        "workload": {
            "publishers": 1,
            "subscribers": config.subscribers,
            "objects_per_group": 1,
            "object_size": config.object_size,
            "fps": config.fps,
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "cooldown_seconds": config.cooldown_seconds,
        },
        "binaries": {
            "relay": str(config.relay_bin),
            "relay_sha256": _file_hash(config.relay_bin),
            "bench": str(config.bench_bin),
            "bench_sha256": _file_hash(config.bench_bin),
        },
        "commands": dataclasses.asdict(command),
        "counts": {
            "groups": report["group_count"],
            "packets": report["packet_count"],
            "correlated_objects": report["correlated_objects"],
            "correlated_object_copies": report["correlated_object_copies"],
        },
    }
    _write_json(output / "run.json", run_record)
    if config.render:
        render(output)
    return output


def compare(config: ComparisonConfig) -> pathlib.Path:
    """Run every value for one comparison dimension."""

    output = config.experiment.output.resolve()
    if output.exists():
        raise ExperimentError(f"comparison output already exists: {output}")
    output.mkdir(parents=True)
    runs = []
    for value in config.values:
        field = config.dimension
        run_config = config.experiment.model_copy(
            update={
                field: value,
                "output": output / f"{field.replace('_', '-')}-{value}",
                "render": False,
            }
        )
        runs.append(run(run_config).name)
    _write_json(
        output / "comparison.json",
        {
            "schema_revision": 1,
            "dimension": config.dimension,
            "values": config.values,
            "runs": runs,
        },
    )
    if config.experiment.render:
        render(output)
    return output
