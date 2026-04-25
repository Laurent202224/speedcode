#!/usr/bin/env python3
"""
Fetch Google Places reviews for a selected hospital from data/data_source.

Set SELECTED_HOSPITAL_NUMBER below for the default row to fetch. You can also
override it from the terminal with:

    python backend/google_places_reviews.py --hospital-number 7

Provide your key later with either:

    export GOOGLE_PLACES_API_KEY="your-api-key"

or:

    python backend/google_places_reviews.py --api-key "your-api-key"

You can also create a local .env file in the repo root:

    GOOGLE_PLACES_API_KEY="your-api-key"
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# Change this number to choose the default hospital. This is 1-based, so 7
# means the 7th data row in data/data_source/data_full.csv.
SELECTED_HOSPITAL_NUMBER = 7

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_SOURCE = REPO_ROOT / "data" / "data_source" / "data_full.csv"

PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
PLACES_DETAILS_URL = "https://places.googleapis.com/v1/places/{place_id}"


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    with dotenv_path.open(encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = value


def load_hospitals(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Data source not found: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8") as file:
        cleaned_lines = (line.replace("\x00", "") for line in file)
        return list(csv.DictReader(cleaned_lines))


def build_address(row: dict[str, str]) -> str:
    address_fields = [
        "address_line1",
        "address_line2",
        "address_line3",
        "address_city",
        "address_stateOrRegion",
        "address_zipOrPostcode",
        "address_country",
    ]
    ignored_values = {"", "null", "none", "nan"}
    parts = []
    for field in address_fields:
        value = row.get(field, "").strip()
        if value.lower() not in ignored_values:
            parts.append(value)
    return ", ".join(parts)


def build_search_query(row: dict[str, str]) -> str:
    parts = [row.get("name", "").strip(), build_address(row)]
    return ", ".join(part for part in parts if part)


def request_json(
    url: str,
    api_key: str,
    field_mask: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = None
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": field_mask,
    }

    if body is not None:
        payload = json.dumps(body).encode("utf-8")

    request = Request(url, data=payload, headers=headers, method=method)

    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Google Places API returned HTTP {error.code}: {details}") from error
    except URLError as error:
        raise RuntimeError(f"Could not reach Google Places API: {error.reason}") from error


def find_place(row: dict[str, str], api_key: str) -> dict[str, Any]:
    body: dict[str, Any] = {
        "textQuery": build_search_query(row),
        "pageSize": 1,
    }

    latitude = row.get("latitude", "").strip()
    longitude = row.get("longitude", "").strip()
    if latitude and longitude:
        body["locationBias"] = {
            "circle": {
                "center": {
                    "latitude": float(latitude),
                    "longitude": float(longitude),
                },
                "radius": 1000.0,
            }
        }

    data = request_json(
        PLACES_TEXT_SEARCH_URL,
        api_key,
        "places.id,places.displayName,places.formattedAddress,places.rating,places.userRatingCount",
        method="POST",
        body=body,
    )
    places = data.get("places", [])
    if not places:
        raise RuntimeError(f"No Google Places match found for query: {body['textQuery']}")

    return places[0]


def fetch_place_details(place_id: str, api_key: str) -> dict[str, Any]:
    fields = ",".join(
        [
            "id",
            "displayName",
            "formattedAddress",
            "rating",
            "userRatingCount",
            "googleMapsUri",
            "reviews",
        ]
    )
    return request_json(PLACES_DETAILS_URL.format(place_id=place_id), api_key, fields)


def localized_text(value: dict[str, Any] | None) -> str:
    if not value:
        return ""
    return str(value.get("text", "")).strip()


def print_reviews(source_row: dict[str, str], place: dict[str, Any], details: dict[str, Any]) -> None:
    display_name = localized_text(details.get("displayName")) or localized_text(place.get("displayName"))
    formatted_address = details.get("formattedAddress") or place.get("formattedAddress") or ""
    rating = details.get("rating", place.get("rating", "N/A"))
    rating_count = details.get("userRatingCount", place.get("userRatingCount", "N/A"))
    reviews = details.get("reviews", [])

    print(f"Selected hospital: {source_row.get('name', '').strip()}")
    print(f"Matched Google place: {display_name}")
    print(f"Address: {formatted_address}")
    print(f"Rating: {rating} ({rating_count} reviews)")
    if details.get("googleMapsUri"):
        print(f"Google Maps: {details['googleMapsUri']}")
    print()

    if not reviews:
        print("No reviews returned by Google Places API for this place.")
        return

    print(f"Reviews returned: {len(reviews)}")
    for index, review in enumerate(reviews, start=1):
        author = review.get("authorAttribution", {}).get("displayName", "Unknown author")
        review_rating = review.get("rating", "N/A")
        published = review.get("relativePublishTimeDescription") or review.get("publishTime", "")
        text = localized_text(review.get("text")) or localized_text(review.get("originalText"))

        print(f"\nReview {index}")
        print(f"Author: {author}")
        print(f"Rating: {review_rating}")
        if published:
            print(f"Published: {published}")
        print(f"Text: {text or '[No text returned]'}")


def parse_args() -> argparse.Namespace:
    load_dotenv(REPO_ROOT / ".env")

    parser = argparse.ArgumentParser(
        description="Fetch Google Places reviews for a selected hospital from data/data_source/data_full.csv."
    )
    parser.add_argument(
        "-n",
        "--hospital-number",
        type=int,
        default=SELECTED_HOSPITAL_NUMBER,
        help="1-based hospital row number to fetch. Defaults to SELECTED_HOSPITAL_NUMBER at the top of the script.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("GOOGLE_PLACES_API_KEY"),
        help="Google Places API key. Defaults to the GOOGLE_PLACES_API_KEY environment variable.",
    )
    parser.add_argument(
        "--data-source",
        type=Path,
        default=DATA_SOURCE,
        help="Path to the CSV data source.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.api_key:
        print(
            "Missing API key. Set GOOGLE_PLACES_API_KEY or pass --api-key.",
            file=sys.stderr,
        )
        return 2

    hospitals = load_hospitals(args.data_source)
    if args.hospital_number < 1 or args.hospital_number > len(hospitals):
        print(
            f"Hospital number must be between 1 and {len(hospitals)}.",
            file=sys.stderr,
        )
        return 2

    selected_hospital = hospitals[args.hospital_number - 1]
    place = find_place(selected_hospital, args.api_key)
    details = fetch_place_details(place["id"], args.api_key)
    print_reviews(selected_hospital, place, details)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
