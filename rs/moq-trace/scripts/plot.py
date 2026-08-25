#!/usr/bin/env python3

from __future__ import annotations

import argparse
import pathlib

from relay_latency_lib.render import RenderError, render


def main() -> None:
    """Render Matplotlib figures from Rust-produced experiment artifacts."""

    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("input", type=pathlib.Path, help="Experiment or comparison directory.")
    args = parser.parse_args()
    try:
        render(args.input)
    except (OSError, RenderError) as error:
        parser.exit(1, f"error: {error}\n")


if __name__ == "__main__":
    main()
