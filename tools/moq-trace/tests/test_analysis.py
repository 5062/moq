from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

import duckdb

SOURCE = pathlib.Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SOURCE))

from moq_trace.analysis import AnalysisError, load_analysis  # noqa: E402


class AnalysisDatabaseTests(unittest.TestCase):
    """Validate the strict database revision seam."""

    def test_rejects_an_old_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = pathlib.Path(directory) / "analysis.duckdb"
            connection = duckdb.connect(str(database))
            connection.execute("CREATE TABLE metadata(schema_revision INTEGER)")
            connection.execute("INSERT INTO metadata VALUES (0)")
            connection.close()

            with self.assertRaisesRegex(AnalysisError, "unsupported analysis schema"):
                load_analysis(database)

    def test_rejects_a_database_without_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = pathlib.Path(directory) / "analysis.duckdb"
            duckdb.connect(str(database)).close()

            with self.assertRaisesRegex(AnalysisError, "failed to load analysis database"):
                load_analysis(database)


if __name__ == "__main__":
    unittest.main()
