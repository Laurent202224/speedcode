#!/usr/bin/env python3
"""Run an OpenAI consistency check over provider spreadsheet rows."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd
from openai import OpenAI
from tqdm import tqdm


DEFAULT_INPUT = Path("data/samples/Small_Dataset_N=50.xlsx")
DEFAULT_OUTPUT = Path("data/samples/Small_Dataset_N=50_checked.xlsx")
DEFAULT_MODEL = "gpt-5.4-mini"

COLUMNS = [
    "numberDoctors",
    "description",
    "capacity",
    "specialties",
    "procedure",
    "equipment",
    "capability",
]

SYSTEM_PROMPT = """
You are an expert healthcare data validation agent.

Evaluate the internal consistency of ONE medical facility row in India.

Return only:
- consistency: exactly one of "Valid", "Suspicious", "Contradictory"
- consistency_flags: max ~10 words

Be strict but realistic.
Do NOT hallucinate missing information.
Missing numberDoctors, equipment, procedure, or capacity alone is NOT suspicious.
Only flag when provided fields create a clear mismatch.
Prefer "Suspicious" over "Contradictory" if uncertain.

Reason across:
- staff vs services
- procedure vs equipment
- capacity vs staff
- capability vs equipment
- description vs structured fields
- overclaiming
"""

SCHEMA = {
    "type": "object",
    "properties": {
        "consistency": {
            "type": "string",
            "enum": ["Valid", "Suspicious", "Contradictory"],
        },
        "consistency_flags": {
            "type": "string",
        },
    },
    "required": ["consistency", "consistency_flags"],
    "additionalProperties": False,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append OpenAI consistency labels to a provider workbook."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    return parser.parse_args()


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def clean_value(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value)[:3000]


def classify_row(client: OpenAI, model: str, row: pd.Series) -> dict[str, str]:
    row_payload = {column: clean_value(row[column]) if column in row else "" for column in COLUMNS}
    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(row_payload, ensure_ascii=False)},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "consistency_check",
                "schema": SCHEMA,
                "strict": True,
            }
        },
    )
    return json.loads(response.output_text)


def main() -> None:
    args = parse_args()
    load_env_file(Path(__file__).resolve().parents[1] / ".env")

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for the consistency check.")
    if not args.input.exists():
        raise FileNotFoundError(f"Input workbook not found: {args.input}")

    client = OpenAI(api_key=api_key)
    data_frame = pd.read_excel(args.input)

    results = []
    for _, row in tqdm(data_frame.iterrows(), total=len(data_frame)):
        try:
            result = classify_row(client, args.model, row)
        except Exception as error:
            result = {
                "consistency": "Suspicious",
                "consistency_flags": f"API error: {str(error)[:40]}",
            }
            time.sleep(2)
        results.append(result)

    data_frame["consistency"] = [result["consistency"] for result in results]
    data_frame["consistency_flags"] = [result["consistency_flags"] for result in results]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    data_frame.to_excel(args.output, index=False)
    print(f"Saved consistency-checked workbook to {args.output}")


if __name__ == "__main__":
    main()
