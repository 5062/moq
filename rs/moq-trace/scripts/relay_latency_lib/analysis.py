from __future__ import annotations

import json
import pathlib
from typing import Literal

import polars as pl
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


class Analysis(StrictModel):
    """Validated Rust-produced latency samples and aggregate trace counts."""

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    samples: pl.DataFrame
    statistics: dict[str, dict[str, float | int]]
    quic_object_samples: pl.DataFrame
    quic_object_statistics: dict[str, dict[str, float | int]]
    packet_samples: pl.DataFrame
    packet_statistics: dict[str, dict[str, float | int]]
    packet_count: int
    group_count: int
    timelines: tuple[ObjectTimeline, ...]


class AnalysisError(RuntimeError):
    """The Rust trace analyzer failed or produced invalid artifacts."""


def load_analysis(output: pathlib.Path) -> Analysis:
    """Load and strictly validate one atomic artifact bundle."""

    try:
        manifest = Manifest.model_validate_json((output / "manifest.json").read_text())
        return Analysis(
            samples=pl.read_csv(output / manifest.files.objects),
            statistics={key: value.model_dump() for key, value in manifest.statistics.items()},
            quic_object_samples=pl.read_csv(output / manifest.files.quic_objects),
            quic_object_statistics={
                key: value.model_dump() for key, value in manifest.quic_object_statistics.items()
            },
            packet_samples=pl.read_csv(output / manifest.files.quic_packets),
            packet_statistics={
                key: value.model_dump() for key, value in manifest.packet_statistics.items()
            },
            packet_count=manifest.packet_count,
            group_count=manifest.group_count,
            timelines=manifest.timelines,
        )
    except (OSError, ValidationError, json.JSONDecodeError, pl.exceptions.PolarsError) as error:
        raise AnalysisError(f"failed to load analyzer artifacts from {output}: {error}") from error
