from __future__ import annotations

import dataclasses
import json
import pathlib
from typing import Literal

import polars as pl

Direction = Literal["rx", "tx"]


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
class Analysis:
    """Rust-produced latency samples and aggregate trace counts."""

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


def _copy(value: dict) -> TimelineCopy:
    return TimelineCopy(**value)


def _timeline(value: dict) -> ObjectTimeline:
    return ObjectTimeline(
        selection=TimelineSelection(**value["selection"]),
        intervals=tuple(TimelineInterval(**interval) for interval in value["intervals"]),
        first_copy=_copy(value["first_copy"]),
        last_copy=_copy(value["last_copy"]),
        slowest_copy=_copy(value["slowest_copy"]),
    )


def load_analysis(output: pathlib.Path) -> Analysis:
    """Load the stable artifact bundle emitted by the Rust analyzer."""

    try:
        metadata = json.loads((output / "analysis.json").read_text())
        return Analysis(
            samples=pl.read_csv(output / "objects.csv"),
            statistics=metadata["statistics"],
            quic_object_samples=pl.read_csv(output / "quic_objects.csv"),
            quic_object_statistics=metadata["quic_object_statistics"],
            packet_samples=pl.read_csv(output / "quic_packets.csv"),
            packet_statistics=metadata["packet_statistics"],
            packet_count=metadata["packet_count"],
            group_count=metadata["group_count"],
            timelines=tuple(_timeline(value) for value in metadata["timelines"]),
        )
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError, pl.exceptions.PolarsError) as error:
        raise AnalysisError(f"failed to load analyzer artifacts from {output}: {error}") from error
