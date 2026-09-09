from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest

import duckdb

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from relay_latency_lib.analysis import AnalysisError, load_analysis  # noqa: E402


class AnalysisBundleTests(unittest.TestCase):
    """Strict validation for the DuckDB analyzer artifact boundary."""

    def write_bundle(self, path: pathlib.Path, extra: dict | None = None) -> None:
        manifest = {
            "database": "analysis.duckdb",
            "statistics": {},
            "quic_object_statistics": {},
            "packet_statistics": {},
            "packet_count": 0,
            "group_count": 0,
            "correlated_objects": 0,
            "correlated_object_copies": 0,
            "timelines": [],
        }
        manifest.update(extra or {})
        (path / "manifest.json").write_text(json.dumps(manifest))
        connection = duckdb.connect(str(path / "analysis.duckdb"))
        connection.execute(
            "CREATE TABLE object_samples(group_id UBIGINT, object_id UBIGINT, metric VARCHAR, "
            "copy_ordinal UBIGINT, elapsed_ms DOUBLE, latency_us DOUBLE)"
        )
        connection.execute("CREATE TABLE quic_object_samples AS SELECT * FROM object_samples")
        connection.execute(
            "CREATE TABLE packet_samples(metric VARCHAR, direction VARCHAR, connection_id UBIGINT, "
            "trace_id UBIGINT, occurrence UBIGINT, elapsed_ms DOUBLE, latency_us DOUBLE)"
        )
        connection.close()

    def test_loads_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            self.write_bundle(path)

            analysis = load_analysis(path)

            self.assertEqual(analysis.packet_count, 0)

    def test_rejects_unknown_manifest_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            self.write_bundle(path, {"legacy": True})

            with self.assertRaises(AnalysisError):
                load_analysis(path)

    def test_reads_object_samples_from_duckdb(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            self.write_bundle(path)
            connection = duckdb.connect(str(path / "analysis.duckdb"))
            connection.execute(
                "INSERT INTO object_samples VALUES (7, 3, 'full_span', 1, 12.5, 42.25)"
            )
            connection.close()

            analysis = load_analysis(path)

            self.assertEqual(len(analysis.samples), 1)
            self.assertEqual(analysis.samples[0].group_id, 7)
            self.assertEqual(analysis.samples[0].latency_us, 42.25)

    def test_rejects_invalid_packet_sample(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            self.write_bundle(path)
            connection = duckdb.connect(str(path / "analysis.duckdb"))
            connection.execute(
                "INSERT INTO packet_samples VALUES ('packet_span', 'sideways', 1, 2, 0, 12.5, 42.25)"
            )
            connection.close()

            with self.assertRaises(AnalysisError):
                load_analysis(path)


if __name__ == "__main__":
    unittest.main()
