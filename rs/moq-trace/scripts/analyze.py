#!/usr/bin/env python3
"""Analyze one moq_trace LTTng CTF recording with DuckDB SQL."""

import argparse
import pathlib
import sys

from relay_latency_lib.analyze import run


def main() -> int:
    """Parse analyzer arguments and publish a DuckDB analysis bundle."""

    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--object-size", type=int, required=True)
    parser.add_argument("--subscribers", type=int, required=True)
    parser.add_argument("--warmup-ns", type=int, default=0)
    parser.add_argument("--cooldown-ns", type=int, default=0)
    parser.add_argument("--expected-pid", type=int)
    args = parser.parse_args()
    try:
        run(
            args.input,
            args.output,
            args.object_size,
            args.subscribers,
            args.warmup_ns,
            args.cooldown_ns,
            args.expected_pid,
        )
    except Exception as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
