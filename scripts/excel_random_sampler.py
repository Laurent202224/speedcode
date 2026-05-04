#!/usr/bin/env python3
"""Create a random sample from an Excel workbook."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_INPUT = Path("data/data_source/VF_Hackathon_Dataset_India_Large_scored.xlsx")
DEFAULT_OUTPUT = Path("data/samples/Small_Dataset_N=50.xlsx")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample rows from an Excel workbook and write a smaller workbook."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--samples", type=int, default=50)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible samples.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.samples <= 0:
        raise ValueError("--samples must be greater than 0")
    if not args.input.exists():
        raise FileNotFoundError(f"Input workbook not found: {args.input}")

    data_frame = pd.read_excel(args.input)
    if args.samples > len(data_frame):
        raise ValueError(
            f"Requested {args.samples} rows, but dataset only has {len(data_frame)} rows."
        )

    sampled = data_frame.sample(n=args.samples, random_state=args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    sampled.to_excel(args.output, index=False)
    print(f"Saved {args.samples} random rows to {args.output}")


if __name__ == "__main__":
    main()
