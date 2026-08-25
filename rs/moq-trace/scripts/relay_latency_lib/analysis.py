from __future__ import annotations

import csv
import dataclasses
import json
import pathlib
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError

Direction = Literal["rx", "tx"]


class StrictModel(BaseModel):
    """Base model for the exact analyzer artifact schema."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class Statistics(StrictModel):
    """Aggregate latency statistics in microseconds."""

    count: int
    mean: float
    p50: float
    p95: float
    p99: float
    max: float


class TimelineSelection(StrictModel):
    """A real object selected nearest one full-span statistic."""

    statistic: str
    target_us: float
    group_id: int
    object_id: int
    actual_us: float


class TimelineInterval(StrictModel):
    """One traced or derived object lifecycle interval."""

    direction: Direction
    session_id: int
    phase: str
    occurrence: int
    start_us: float
    end_us: float


class TimelineCopy(StrictModel):
    """One subscriber copy and its creation order and full-span latency."""

    session_id: int
    subscriber_ordinal: int
    full_span_us: float


class ObjectTimeline(StrictModel):
    """All traced intervals for one selected logical object."""

    selection: TimelineSelection
    intervals: tuple[TimelineInterval, ...]
    first_copy: TimelineCopy
    last_copy: TimelineCopy
    slowest_copy: TimelineCopy


class ArtifactFiles(StrictModel):
    """CSV members named by the analyzer manifest."""

    objects: Literal["objects.csv"]
    quic_objects: Literal["quic_objects.csv"]
    quic_packets: Literal["quic_packets.csv"]


class Manifest(StrictModel):
    """Versioned Rust analyzer manifest."""

    artifact_revision: Literal[1]
    files: ArtifactFiles
    statistics: dict[str, Statistics]
    quic_object_statistics: dict[str, Statistics]
    packet_statistics: dict[str, Statistics]
    packet_count: int
    group_count: int
    timelines: tuple[ObjectTimeline, ...]


@dataclasses.dataclass(frozen=True)
class Sample:
    """One Rust-produced object latency sample."""

    group_id: int
    object_id: int
    metric: str
    copy_ordinal: int
    elapsed_ms: float
    latency_us: float


@dataclasses.dataclass(frozen=True)
class PacketSample:
    """One Rust-produced packet latency sample."""

    metric: str
    direction: Direction
    connection_id: int
    trace_id: int
    occurrence: int
    elapsed_ms: float
    latency_us: float


@dataclasses.dataclass(frozen=True)
class Analysis:
    """Validated Rust-produced samples and aggregate trace counts."""

    samples: tuple[Sample, ...]
    statistics: dict[str, dict[str, float | int]]
    quic_object_samples: tuple[Sample, ...]
    quic_object_statistics: dict[str, dict[str, float | int]]
    packet_samples: tuple[PacketSample, ...]
    packet_statistics: dict[str, dict[str, float | int]]
    packet_count: int
    group_count: int
    timelines: tuple[ObjectTimeline, ...]


class AnalysisError(RuntimeError):
    """The Rust analyzer bundle is invalid or cannot be read."""


def _read_object_samples(path: pathlib.Path) -> tuple[Sample, ...]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        expected = ("group_id", "object_id", "metric", "copy_ordinal", "elapsed_ms", "latency_us")
        if tuple(reader.fieldnames or ()) != expected:
            raise ValueError(f"{path} has unexpected columns")
        return tuple(
            Sample(
                group_id=int(row["group_id"]),
                object_id=int(row["object_id"]),
                metric=row["metric"],
                copy_ordinal=int(row["copy_ordinal"]),
                elapsed_ms=float(row["elapsed_ms"]),
                latency_us=float(row["latency_us"]),
            )
            for row in reader
        )


def _read_packet_samples(path: pathlib.Path) -> tuple[PacketSample, ...]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        expected = (
            "metric",
            "direction",
            "connection_id",
            "trace_id",
            "occurrence",
            "elapsed_ms",
            "latency_us",
        )
        if tuple(reader.fieldnames or ()) != expected:
            raise ValueError(f"{path} has unexpected columns")
        rows = []
        for row in reader:
            direction = row["direction"]
            if direction not in ("rx", "tx"):
                raise ValueError(f"{path} has invalid direction {direction!r}")
            rows.append(
                PacketSample(
                    metric=row["metric"],
                    direction=direction,
                    connection_id=int(row["connection_id"]),
                    trace_id=int(row["trace_id"]),
                    occurrence=int(row["occurrence"]),
                    elapsed_ms=float(row["elapsed_ms"]),
                    latency_us=float(row["latency_us"]),
                )
            )
        return tuple(rows)


def load_analysis(output: pathlib.Path) -> Analysis:
    """Load and strictly validate one atomic Rust analyzer bundle."""

    try:
        manifest = Manifest.model_validate_json((output / "manifest.json").read_text())
        return Analysis(
            samples=_read_object_samples(output / manifest.files.objects),
            statistics={key: value.model_dump() for key, value in manifest.statistics.items()},
            quic_object_samples=_read_object_samples(output / manifest.files.quic_objects),
            quic_object_statistics={
                key: value.model_dump() for key, value in manifest.quic_object_statistics.items()
            },
            packet_samples=_read_packet_samples(output / manifest.files.quic_packets),
            packet_statistics={
                key: value.model_dump() for key, value in manifest.packet_statistics.items()
            },
            packet_count=manifest.packet_count,
            group_count=manifest.group_count,
            timelines=manifest.timelines,
        )
    except (OSError, ValidationError, ValueError, csv.Error, json.JSONDecodeError) as error:
        raise AnalysisError(f"failed to load analyzer artifacts from {output}: {error}") from error
