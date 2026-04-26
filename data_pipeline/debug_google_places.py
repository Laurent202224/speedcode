#!/usr/bin/env python3
"""Debug script to test Google Places API call."""

import json
import os
from pathlib import Path
from urllib.request import Request, urlopen

# Load .env
PROJECT_ROOT = Path(__file__).resolve().parents[1]
dotenv_path = PROJECT_ROOT / ".env"
if dotenv_path.exists():
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value

api_key = os.environ.get("GOOGLE_PLACES_API_KEY")

if not api_key:
    print("ERROR: GOOGLE_PLACES_API_KEY not found in environment")
    exit(1)

print(f"API Key found: {api_key[:10]}...{api_key[-5:]}")

# Test with a simple known place
body = {
    "textQuery": "Lebaron Aesthetic Clinic, Delhi, India",
    "pageSize": 1,
    "locationBias": {
        "circle": {
            "center": {
                "latitude": 28.61421394,
                "longitude": 77.20902252,
            },
            "radius": 1000.0,
        }
    }
}

url = "https://places.googleapis.com/v1/places:searchText"
payload = json.dumps(body).encode("utf-8")
headers = {
    "Content-Type": "application/json",
    "X-Goog-Api-Key": api_key,
    "X-Goog-FieldMask": "places.id,places.displayName,places.rating,places.userRatingCount",
}

print(f"\nMaking request to: {url}")
print(f"Query: {body['textQuery']}")
print(f"Headers: {headers}")

try:
    request = Request(url, data=payload, headers=headers, method="POST")
    with urlopen(request, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))
        print(f"\nResponse status: {response.status}")
        print(f"Response data: {json.dumps(data, indent=2)}")
except Exception as e:
    print(f"\nERROR: {type(e).__name__}: {e}")
    if hasattr(e, 'read'):
        print(f"Error details: {e.read().decode('utf-8')}")
