from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from relay_latency_lib.analysis import AnalysisError, load_analysis  # noqa: E402


class AnalysisBundleTests(unittest.TestCase):
    """Strict validation for the Rust analyzer artifact boundary."""

    def write_bundle(self, path: pathlib.Path, extra: dict | None = None) -> None:
        manifest = {
            "artifact_revision": 1,
            "files": {
                "objects": "objects.csv",
                "quic_objects": "quic_objects.csv",
                "quic_packets": "quic_packets.csv",
            },
            "statistics": {},
            "quic_object_statistics": {},
            "packet_statistics": {},
            "packet_count": 0,
            "group_count": 0,
            "timelines": [],
        }
        manifest.update(extra or {})
        (path / "manifest.json").write_text(json.dumps(manifest))
        (path / "objects.csv").write_text("group_id,object_id,metric,copy_ordinal,elapsed_ms,latency_us\n")
        (path / "quic_objects.csv").write_text(
            "group_id,object_id,metric,copy_ordinal,elapsed_ms,latency_us\n"
        )
        (path / "quic_packets.csv").write_text(
            "metric,direction,connection_id,trace_id,occurrence,elapsed_ms,latency_us\n"
        )

    def test_loads_exact_revision_one_bundle(self) -> None:
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

    def test_reads_object_samples_without_dataframe_dependency(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            self.write_bundle(path)
            (path / "objects.csv").write_text(
                "group_id,object_id,metric,copy_ordinal,elapsed_ms,latency_us\n"
                "7,3,full_span,1,12.5,42.25\n"
            )

            analysis = load_analysis(path)

            self.assertEqual(len(analysis.samples), 1)
            self.assertEqual(analysis.samples[0].group_id, 7)
            self.assertEqual(analysis.samples[0].latency_us, 42.25)


if __name__ == "__main__":
    unittest.main()
