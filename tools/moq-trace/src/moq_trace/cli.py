"""Command-line interface for MoQ relay trace experiments."""

from __future__ import annotations

import argparse
import pathlib
import sys

import tomllib
from pydantic import ValidationError

from .config import ComparisonConfig, ExperimentConfig


def _model(path: pathlib.Path, model):
    try:
        return model.model_validate(tomllib.loads(path.read_text()))
    except (OSError, tomllib.TOMLDecodeError, ValidationError) as error:
        raise ValueError(f"failed to load {path}: {error}") from error


def parser() -> argparse.ArgumentParser:
    """Build the complete command parser."""

    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Run one experiment from TOML configuration.")
    run.add_argument("config", type=pathlib.Path)

    compare = commands.add_parser("compare", help="Run one comparison from TOML configuration.")
    compare.add_argument("config", type=pathlib.Path)

    analyze = commands.add_parser("analyze", help="Analyze one LTTng CTF trace.")
    analyze.add_argument("input", type=pathlib.Path)
    analyze.add_argument("--output", type=pathlib.Path, required=True)
    analyze.add_argument("--object-size", type=int, required=True)
    analyze.add_argument("--subscribers", type=int, required=True)
    analyze.add_argument("--warmup-seconds", type=float, default=0.0)
    analyze.add_argument("--cooldown-seconds", type=float, default=0.0)
    analyze.add_argument("--expected-pid", type=int)

    plot = commands.add_parser("plot", help="Render figures from an experiment directory.")
    plot.add_argument("input", type=pathlib.Path)
    return root


def _run(args: argparse.Namespace) -> None:
    if args.command == "run":
        from .experiment import run

        print(run(_model(args.config, ExperimentConfig)))
    elif args.command == "compare":
        from .experiment import compare

        print(compare(_model(args.config, ComparisonConfig)))
    elif args.command == "analyze":
        from .analyze import run

        if args.object_size <= 0 or args.subscribers <= 0:
            raise ValueError("object size and subscribers must be positive")
        if args.warmup_seconds < 0 or args.cooldown_seconds < 0:
            raise ValueError("warmup and cooldown must be nonnegative")
        run(
            args.input,
            args.output,
            args.object_size,
            args.subscribers,
            int(args.warmup_seconds * 1_000_000_000),
            int(args.cooldown_seconds * 1_000_000_000),
            args.expected_pid,
        )
        print(args.output.resolve())
    elif args.command == "plot":
        from .render import render

        render(args.input)


def main() -> None:
    """Run the selected command with concise expected-error reporting."""

    try:
        _run(parser().parse_args())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
