from __future__ import annotations

import pathlib
import sys
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from relay_latency_lib.runner import ExperimentConfig, build_relay_command  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
