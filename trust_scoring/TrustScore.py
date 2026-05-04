#!/usr/bin/env python3
"""Compute simple trust scores for a provider spreadsheet."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_INPUT = Path("data/data_source/VF_Hackathon_Dataset_India_Large_scored.xlsx")
DEFAULT_OUTPUT = Path(
    "data/data_source/VF_Hackathon_Dataset_India_Large_trust_scored.xlsx"
)

SCORING_CONFIG = [
    {"name": "completeness", "weight": 0.3},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append trust score columns to a workbook.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def score_completeness(row: pd.Series) -> tuple[float, list[str]]:
    def is_filled(value: object) -> bool:
        if pd.isna(value):
            return False
        text = str(value).strip().casefold()
        return text not in {"", "nan", "none", "null"}

    total_fields = len(row.index)
    filled_fields = sum(is_filled(row.get(column)) for column in row.index)
    score = (filled_fields / total_fields) if total_fields else 0.0

    reasons: list[str] = []
    if score < 0.5:
        reasons.append("Many fields are missing overall.")

    critical_groups = {
        "name": ["doctor_name", "name"],
        "latitude": ["latitude"],
        "longitude": ["longitude"],
    }
    missing_critical = [
        label
        for label, candidates in critical_groups.items()
        if not any(is_filled(row.get(field)) for field in candidates)
    ]

    if missing_critical:
        reasons.append(
            f"FLAG: Critical information missing ({', '.join(missing_critical)})."
        )
        score = min(score, 0.2)

    return score, reasons


METHODS = {
    "completeness": score_completeness,
}


def compute_trust_score(row: pd.Series) -> pd.Series:
    total_weight = sum(item["weight"] for item in SCORING_CONFIG)
    weighted_sum = 0.0
    all_reasons: list[str] = []
    subscores: dict[str, float] = {}

    for item in SCORING_CONFIG:
        method_name = item["name"]
        weight = item["weight"]
        score, reasons = METHODS[method_name](row)
        score = max(0.0, min(1.0, score))
        weighted_sum += weight * score
        subscores[method_name] = score
        all_reasons.extend(reasons)

    final_score = 10 * weighted_sum / total_weight if total_weight else 0.0
    critical_missing_flag = any(reason.startswith("FLAG:") for reason in all_reasons)

    return pd.Series(
        {
            "trust_score": round(final_score, 2),
            "critical_missing_flag": critical_missing_flag,
            "subscores": subscores,
            "reasons": all_reasons,
        }
    )


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input workbook not found: {args.input}")

    data_frame = pd.read_excel(args.input)
    results = data_frame.apply(compute_trust_score, axis=1)
    scored = pd.concat([data_frame, results], axis=1)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    scored.to_excel(args.output, index=False)
    print(f"Saved trust-scored workbook to {args.output}")


if __name__ == "__main__":
    main()
