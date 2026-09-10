from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

SOURCE = pathlib.Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SOURCE))

from moq_trace.analysis import Sample  # noqa: E402
from moq_trace.plot import MetricPlot, PlotOptions, plot_metrics  # noqa: E402


class MetricPlotTests(unittest.TestCase):
    """Exercise plot sizing independently of the current metric catalog."""

    def test_supports_more_series_than_the_old_fixed_palette(self) -> None:
        metrics = tuple(f"metric_{index}" for index in range(12))
        samples = tuple(Sample(1, 1, metric, 0, 0, 1_000) for metric in metrics)
        statistics = {
            metric: {"count": 1, "mean": 1.0, "p50": 1.0, "p95": 1.0, "p99": 1.0, "max": 1.0} for metric in metrics
        }
        plot = MetricPlot(samples, statistics, dict(zip(metrics, metrics, strict=True)), "Metrics")
        options = PlotOptions(None, 1, 1_024, 30, "test")

        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "metrics.png"
            plot_metrics(output, options, plot)
            self.assertGreater(output.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
