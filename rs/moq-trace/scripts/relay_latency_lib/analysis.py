from __future__ import annotations

import dataclasses
import pathlib
from typing import Literal

import duckdb
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


class Manifest(StrictModel):
    """DuckDB analyzer manifest."""

    database: Literal["analysis.duckdb"]
    statistics: dict[str, Statistics]
    quic_object_statistics: dict[str, Statistics]
    packet_statistics: dict[str, Statistics]
    packet_count: int
    group_count: int
    correlated_objects: int
    correlated_object_copies: int
    timelines: tuple[ObjectTimeline, ...]


class Sample(StrictModel):
    """One SQL-derived object latency sample."""

    group_id: int
    object_id: int
    metric: str
    copy_ordinal: int
    elapsed_ms: float
    latency_us: float


class PacketSample(StrictModel):
    """One SQL-derived packet latency sample."""

    metric: str
    direction: Direction
    connection_id: int
    trace_id: int
    occurrence: int
    elapsed_ms: float
    latency_us: float


@dataclasses.dataclass(frozen=True)
class Analysis:
    """Validated SQL-derived samples and aggregate trace counts."""

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
    """The DuckDB analyzer bundle is invalid or cannot be read."""


def _rows(connection: duckdb.DuckDBPyConnection, table: str, model):
    result = connection.execute(f"SELECT * FROM {table} ORDER BY ALL")
    columns = [column[0] for column in result.description]
    return tuple(model.model_validate(dict(zip(columns, row, strict=True))) for row in result.fetchall())


def load_analysis(output: pathlib.Path) -> Analysis:
    """Load and strictly validate one atomic DuckDB analyzer bundle."""

    try:
        manifest = Manifest.model_validate_json((output / "manifest.json").read_text())
        connection = duckdb.connect(str(output / manifest.database), read_only=True)
        try:
            return Analysis(
                samples=_rows(connection, "object_samples", Sample),
                statistics={key: value.model_dump() for key, value in manifest.statistics.items()},
                quic_object_samples=_rows(connection, "quic_object_samples", Sample),
                quic_object_statistics={
                    key: value.model_dump() for key, value in manifest.quic_object_statistics.items()
                },
                packet_samples=_rows(connection, "packet_samples", PacketSample),
                packet_statistics={
                    key: value.model_dump() for key, value in manifest.packet_statistics.items()
                },
                packet_count=manifest.packet_count,
                group_count=manifest.group_count,
                timelines=manifest.timelines,
            )
        finally:
            connection.close()
    except (OSError, duckdb.Error, ValidationError, ValueError) as error:
        raise AnalysisError(f"failed to load analyzer artifacts from {output}: {error}") from error
