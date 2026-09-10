"""Open the authoritative DuckDB analysis artifact."""

from __future__ import annotations

import dataclasses
import pathlib
from typing import Literal

import duckdb

from .report import build as build_report
from .schema import SCHEMA_REVISION

Direction = Literal["rx", "tx"]


@dataclasses.dataclass(frozen=True)
class Sample:
    """One SQL-derived object latency sample."""

    group_id: int
    object_id: int
    metric: str
    copy_ordinal: int
    elapsed_ns: int
    latency_ns: int

    @property
    def elapsed_ms(self) -> float:
        """Return the presentation offset in milliseconds."""

        return self.elapsed_ns / 1_000_000

    @property
    def latency_us(self) -> float:
        """Return the presentation latency in microseconds."""

        return self.latency_ns / 1_000


@dataclasses.dataclass(frozen=True)
class PacketSample:
    """One SQL-derived packet latency sample."""

    metric: str
    direction: Direction
    connection_id: int
    trace_id: int
    occurrence: int
    elapsed_ns: int
    latency_ns: int

    @property
    def elapsed_ms(self) -> float:
        """Return the presentation offset in milliseconds."""

        return self.elapsed_ns / 1_000_000

    @property
    def latency_us(self) -> float:
        """Return the presentation latency in microseconds."""

        return self.latency_ns / 1_000


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
    """One subscriber copy and its order and full-span latency."""

    session_id: int
    subscriber_ordinal: int
    full_span_us: float


@dataclasses.dataclass(frozen=True)
class ObjectTimeline:
    """All traced intervals for one selected logical object."""

    selection: TimelineSelection
    intervals: tuple[TimelineInterval, ...]
    first_copy: TimelineCopy
    last_copy: TimelineCopy
    slowest_copy: TimelineCopy


@dataclasses.dataclass(frozen=True)
class Analysis:
    """Queryable samples and summaries loaded from one current-schema database."""

    samples: tuple[Sample, ...]
    statistics: dict[str, dict[str, float | int]]
    labels: dict[str, str]
    quic_object_samples: tuple[Sample, ...]
    quic_object_statistics: dict[str, dict[str, float | int]]
    quic_object_labels: dict[str, str]
    packet_samples: tuple[PacketSample, ...]
    packet_statistics: dict[str, dict[str, float | int]]
    packet_labels: dict[str, str]
    packet_count: int
    group_count: int
    timelines: tuple[ObjectTimeline, ...]


class AnalysisError(RuntimeError):
    """The DuckDB analysis artifact is invalid or cannot be read."""


def _samples(connection: duckdb.DuckDBPyConnection, table: str) -> tuple[Sample, ...]:
    rows = connection.execute(
        f"""SELECT group_id, object_id, metric, copy_ordinal, elapsed_ns, latency_ns
            FROM {table} ORDER BY group_id, object_id, metric, copy_ordinal"""
    ).fetchall()
    return tuple(Sample(*row) for row in rows)


def _packet_samples(connection: duckdb.DuckDBPyConnection) -> tuple[PacketSample, ...]:
    rows = connection.execute(
        """SELECT metric, direction, connection_id, trace_id, occurrence,
                  elapsed_ns, latency_ns
           FROM packet_samples
           ORDER BY metric, direction, connection_id, trace_id, occurrence"""
    ).fetchall()
    return tuple(PacketSample(*row) for row in rows)


def _timeline(value: dict) -> ObjectTimeline:
    def copy(name: str) -> TimelineCopy:
        return TimelineCopy(**value[name])

    return ObjectTimeline(
        selection=TimelineSelection(**value["selection"]),
        intervals=tuple(TimelineInterval(**interval) for interval in value["intervals"]),
        first_copy=copy("first_copy"),
        last_copy=copy("last_copy"),
        slowest_copy=copy("slowest_copy"),
    )


def _labels(connection: duckdb.DuckDBPyConnection, domain: str) -> dict[str, str]:
    rows = connection.execute(
        """SELECT metric, label FROM metric_definitions
           WHERE domain = ? ORDER BY display_order""",
        [domain],
    ).fetchall()
    return dict(rows)


def load_analysis(database: pathlib.Path) -> Analysis:
    """Open and strictly require the current analysis schema."""

    try:
        connection = duckdb.connect(str(database), read_only=True)
        try:
            revision = connection.execute("SELECT schema_revision FROM metadata").fetchone()
            if revision != (SCHEMA_REVISION,):
                found = None if revision is None else revision[0]
                raise AnalysisError(f"unsupported analysis schema {found!r}; expected {SCHEMA_REVISION}")
            report = build_report(connection)
            return Analysis(
                samples=_samples(connection, "object_samples"),
                statistics=report["statistics"],
                labels=_labels(connection, "object"),
                quic_object_samples=_samples(connection, "quic_object_samples"),
                quic_object_statistics=report["quic_object_statistics"],
                quic_object_labels=_labels(connection, "quic_object"),
                packet_samples=_packet_samples(connection),
                packet_statistics=report["packet_statistics"],
                packet_labels=_labels(connection, "packet"),
                packet_count=report["packet_count"],
                group_count=report["group_count"],
                timelines=tuple(_timeline(value) for value in report["timelines"]),
            )
        finally:
            connection.close()
    except AnalysisError:
        raise
    except (OSError, duckdb.Error, TypeError, ValueError) as error:
        raise AnalysisError(f"failed to load analysis database {database}: {error}") from error
