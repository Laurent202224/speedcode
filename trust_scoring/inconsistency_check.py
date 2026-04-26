import os, json, time
import pandas as pd
from tqdm import tqdm
from openai import OpenAI

INPUT_XLSX = "Small_Dataset_N=50.xlsx"
OUTPUT_XLSX = "Small_Dataset_N=50_checked.xlsx"
MODEL = "gpt-4.1-mini"

COLUMNS = [
    "numberDoctors",
    "description",
    "capacity",
    "specialties",
    "procedure",
    "equipment",
    "capability",
]

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

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

def clean_value(x):
    if pd.isna(x):
        return ""
    return str(x)[:3000]

def classify_row(row):
    row_payload = {col: clean_value(row[col]) if col in row else "" for col in COLUMNS}

    response = client.responses.create(
        model=MODEL,
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
        temperature=0,
    )

    return json.loads(response.output_text)

df = pd.read_excel(INPUT_XLSX)

results = []
for _, row in tqdm(df.iterrows(), total=len(df)):
    try:
        result = classify_row(row)
    except Exception as e:
        result = {
            "consistency": "Suspicious",
            "consistency_flags": f"API error: {str(e)[:40]}",
        }
        time.sleep(2)

    results.append(result)

df["consistency"] = [r["consistency"] for r in results]
df["consistency_flags"] = [r["consistency_flags"] for r in results]

df.to_excel(OUTPUT_XLSX, index=False)
print(f"Saved: {OUTPUT_XLSX}")