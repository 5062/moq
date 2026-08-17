from __future__ import annotations

import pathlib
import sys
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from relay_latency import parse_object_sizes  # noqa: E402
from relay_latency_lib.runner import (  # noqa: E402
    ExperimentConfig,
    _format_byte_size,
    build_relay_command,
)


class TraceQueueTests(unittest.TestCase):
    """Trace queue sizing for subscriber fanout."""

    def test_queue_capacity_scales_with_subscribers(self) -> None:
        config = ExperimentConfig(
            repo=pathlib.Path("/repo"),
            output=pathlib.Path("/output"),
            relay_bin=pathlib.Path("/relay"),
            bench_bin=pathlib.Path("/bench"),
            subscribers=50,
        )

        command = build_relay_command(config)
        option = command.index("--trace-queue-capacity")

        self.assertEqual(command[option + 1], "204800")


class ObjectSizeComparisonTests(unittest.TestCase):
    """Object-size comparison parsing and presentation."""

    def test_parses_binary_size_suffixes(self) -> None:
        self.assertEqual(
            parse_object_sizes("16kb, 64KiB, 262144"),
            (16 * 1024, 64 * 1024, 256 * 1024),
        )

    def test_rejects_invalid_size(self) -> None:
        with self.assertRaisesRegex(ValueError, "comma-separated byte sizes"):
            parse_object_sizes("16k,large")

    def test_formats_integral_binary_sizes(self) -> None:
        self.assertEqual(_format_byte_size(256 * 1024), "256 KiB")


if __name__ == "__main__":
    unittest.main()
