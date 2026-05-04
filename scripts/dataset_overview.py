#!/usr/bin/env python3
"""Generate a static HTML overview for the provider spreadsheet."""

from __future__ import annotations

import argparse
import html
import json
from collections import Counter
from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_INPUT = Path("data/data_source/VF_Hackathon_Dataset_India_Large_scored.xlsx")
DEFAULT_OUTPUT = Path("scripts/dataset_overview.html")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a missing-data and specialty overview from an Excel workbook."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def split_specialties(value: object) -> Iterable[str]:
    if pd.isna(value):
        return []

    text = str(value).strip()
    if not text or text.casefold() in {"nan", "none", "null"}:
        return []

    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]

    separator = ";" if ";" in text else ","
    return [part.strip() for part in text.split(separator) if part.strip()]


def missing_data_table(data_frame: pd.DataFrame) -> pd.DataFrame:
    missing = pd.DataFrame(
        {
            "Column": data_frame.columns,
            "Missing_Count": data_frame.isnull().sum().values,
            "Missing_Percentage": (
                data_frame.isnull().sum().values / len(data_frame) * 100
            ).round(2),
        }
    )
    return missing.sort_values("Missing_Count", ascending=False).reset_index(drop=True)


def specialty_table(data_frame: pd.DataFrame) -> pd.DataFrame:
    if "specialties" not in data_frame:
        return pd.DataFrame(columns=["Specialty", "Count", "Percentage"])

    counter: Counter[str] = Counter()
    for value in data_frame["specialties"].dropna():
        counter.update(split_specialties(value))

    total = sum(counter.values())
    rows = [
        {
            "Specialty": specialty,
            "Count": count,
            "Percentage": round((count / total * 100) if total else 0, 2),
        }
        for specialty, count in counter.items()
    ]
    return (
        pd.DataFrame(rows)
        .sort_values("Count", ascending=False)
        .reset_index(drop=True)
        if rows
        else pd.DataFrame(columns=["Specialty", "Count", "Percentage"])
    )


def row_class(percentage: float) -> str:
    if percentage > 50:
        return "missing-high"
    if percentage > 20:
        return "missing-medium"
    return "missing-low"


def build_html(data_frame: pd.DataFrame) -> str:
    missing = missing_data_table(data_frame)
    specialties = specialty_table(data_frame)
    records_with_specialties = (
        int(data_frame["specialties"].notnull().sum())
        if "specialties" in data_frame
        else 0
    )
    max_specialty_count = int(specialties["Count"].max()) if len(specialties) else 0

    missing_rows = []
    for _, row in missing.iterrows():
        percentage = float(row["Missing_Percentage"])
        missing_rows.append(
            f"""
            <tr class="{row_class(percentage)}">
              <td>{html.escape(str(row["Column"]))}</td>
              <td>{int(row["Missing_Count"])}</td>
              <td>{percentage:.2f}%</td>
            </tr>"""
        )

    specialty_rows = []
    for _, row in specialties.iterrows():
        count = int(row["Count"])
        percentage = float(row["Percentage"])
        width = (count / max_specialty_count * 100) if max_specialty_count else 0
        specialty_rows.append(
            f"""
            <tr>
              <td class="specialty-name">{html.escape(str(row["Specialty"]))}</td>
              <td>{count}</td>
              <td>{percentage:.2f}%</td>
              <td><div class="bar-container"><div class="bar" style="width: {width:.2f}%">{percentage:.1f}%</div></div></td>
            </tr>"""
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Dataset Overview</title>
  <style>
    body {{ margin: 0; background: #f4f6fb; color: #172033; font-family: Arial, sans-serif; }}
    .container {{ max-width: 1080px; margin: 0 auto; padding: 28px 20px 44px; }}
    h1 {{ margin: 0 0 24px; }}
    h2 {{ margin: 30px 0 12px; padding-bottom: 8px; border-bottom: 2px solid #2563eb; color: #344054; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border: 1px solid #dce3ee; }}
    th, td {{ padding: 11px 12px; border-bottom: 1px solid #e5eaf3; text-align: left; }}
    th {{ background: #2563eb; color: white; }}
    .stats {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }}
    .stat-box {{ padding: 16px; border: 1px solid #dce3ee; background: white; }}
    .stat-box h3 {{ margin: 0 0 6px; color: #697386; font-size: 14px; }}
    .value {{ color: #1d4ed8; font-size: 26px; font-weight: 700; }}
    .missing-high {{ background: #fee2e2; }}
    .missing-medium {{ background: #fef3c7; }}
    .missing-low {{ background: #dcfce7; }}
    .bar-container {{ height: 24px; overflow: hidden; background: #e5eaf3; }}
    .bar {{ display: flex; align-items: center; justify-content: flex-end; height: 100%; padding-right: 8px; background: #2563eb; color: white; font-size: 12px; font-weight: 700; }}
    .specialty-name {{ font-weight: 600; }}
    @media (max-width: 720px) {{ .stats {{ grid-template-columns: 1fr; }} th, td {{ font-size: 13px; }} }}
  </style>
</head>
<body>
  <main class="container">
    <h1>Dataset Overview</h1>

    <h2>Missing Data</h2>
    <table>
      <thead><tr><th>Column</th><th>Missing Count</th><th>Missing Percentage</th></tr></thead>
      <tbody>{''.join(missing_rows)}</tbody>
    </table>

    <h2>Specialties</h2>
    <section class="stats">
      <div class="stat-box"><h3>Total Records</h3><div class="value">{len(data_frame)}</div></div>
      <div class="stat-box"><h3>Records With Specialties</h3><div class="value">{records_with_specialties}</div></div>
      <div class="stat-box"><h3>Unique Specialties</h3><div class="value">{len(specialties)}</div></div>
    </section>
    <table>
      <thead><tr><th>Specialty</th><th>Count</th><th>Percentage</th><th>Visual</th></tr></thead>
      <tbody>{''.join(specialty_rows)}</tbody>
    </table>
  </main>
</body>
</html>
"""


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input workbook not found: {args.input}")

    data_frame = pd.read_excel(args.input)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(build_html(data_frame), encoding="utf-8")
    specialties = specialty_table(data_frame)
    print(f"Dataset overview saved to {args.output}")
    print(f"Records: {len(data_frame)}")
    print(f"Unique specialties: {len(specialties)}")


if __name__ == "__main__":
    main()
