from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

import duckdb

SOURCE = pathlib.Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SOURCE))

from moq_trace.plot import MetricPlot, PlotOptions, plot_metrics  # noqa: E402


class MetricPlotTests(unittest.TestCase):
    """Exercise plot sizing independently of the current metric catalog."""

    def test_supports_more_series_than_the_old_fixed_palette(self) -> None:
        metrics = tuple(f"metric_{index}" for index in range(12))
        statistics = {
            metric: {"count": 1, "mean": 1.0, "p50": 1.0, "p95": 1.0, "p99": 1.0, "max": 1.0} for metric in metrics
        }
        connection = duckdb.connect(":memory:")
        try:
            connection.execute("CREATE TABLE samples(metric VARCHAR, elapsed_ns BIGINT, latency_ns BIGINT)")
            connection.executemany("INSERT INTO samples VALUES (?, 0, 1000)", [(metric,) for metric in metrics])
            plot = MetricPlot(connection, "samples", statistics, dict(zip(metrics, metrics, strict=True)), "Metrics")
            options = PlotOptions(None, 1, 1_024, 30, "test")

            with tempfile.TemporaryDirectory() as directory:
                output = pathlib.Path(directory) / "metrics.png"
                plot_metrics(output, options, plot)
                self.assertGreater(output.stat().st_size, 0)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
