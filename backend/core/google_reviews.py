from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
PLACES_DETAILS_URL = "https://places.googleapis.com/v1/places/{place_id}"


def enrich_hospitals_with_google_reviews(
    hospitals: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str | None]:
    api_key = (os.environ.get("GOOGLE_PLACES_API_KEY") or "").strip()
    if not api_key or api_key.startswith("your-google-"):
        return hospitals, "Google reviews are not configured. Set GOOGLE_PLACES_API_KEY."

    enriched_hospitals: list[dict[str, Any]] = []
    errors: list[str] = []
    for hospital in hospitals:
        enriched = dict(hospital)
        try:
            enriched["google_reviews"] = fetch_google_review_summary(
                _build_search_query(enriched),
                _coerce_float(enriched.get("latitude")),
                _coerce_float(enriched.get("longitude")),
                api_key,
            )
        except RuntimeError as error:
            errors.append(f"{hospital.get('name')}: {error}")
            enriched["google_reviews"] = {
                "available": False,
                "error": str(error),
            }
        enriched_hospitals.append(enriched)

    return enriched_hospitals, "; ".join(errors) if errors else None


@lru_cache(maxsize=256)
def fetch_google_review_summary(
    search_query: str,
    latitude: float | None,
    longitude: float | None,
    api_key: str,
) -> dict[str, Any]:
    place = _find_place(search_query, latitude, longitude, api_key)
    details = _fetch_place_details(str(place["id"]), api_key)
    return _review_summary(place, details)


def _find_place(
    search_query: str,
    latitude: float | None,
    longitude: float | None,
    api_key: str,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "textQuery": search_query,
        "pageSize": 1,
    }
    if latitude is not None and longitude is not None:
        body["locationBias"] = {
            "circle": {
                "center": {
                    "latitude": latitude,
                    "longitude": longitude,
                },
                "radius": 1000.0,
            }
        }

    data = _request_json(
        PLACES_TEXT_SEARCH_URL,
        api_key,
        "places.id,places.displayName,places.formattedAddress,places.rating,places.userRatingCount",
        method="POST",
        body=body,
    )
    places = data.get("places")
    if not isinstance(places, list) or not places:
        raise RuntimeError(f"No Google Places match found for {search_query}")

    place = places[0]
    if not isinstance(place, dict) or not place.get("id"):
        raise RuntimeError("Google Places match did not include an id")
    return place


def _fetch_place_details(place_id: str, api_key: str) -> dict[str, Any]:
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
    return _request_json(PLACES_DETAILS_URL.format(place_id=place_id), api_key, fields)


def _request_json(
    url: str,
    api_key: str,
    field_mask: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = json.dumps(body).encode("utf-8") if body is not None else None
    request = Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": field_mask,
        },
        method=method,
    )

    try:
        with urlopen(request, timeout=20) as response:
            loaded = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Google Places API returned HTTP {error.code}: {detail}") from error
    except URLError as error:
        raise RuntimeError(f"Could not reach Google Places API: {error.reason}") from error

    if not isinstance(loaded, dict):
        raise RuntimeError("Google Places response was not a JSON object")
    return loaded


def _review_summary(place: dict[str, Any], details: dict[str, Any]) -> dict[str, Any]:
    reviews = details.get("reviews")
    review_items = reviews if isinstance(reviews, list) else []
    return {
        "available": True,
        "matched_name": _localized_text(details.get("displayName"))
        or _localized_text(place.get("displayName")),
        "formatted_address": details.get("formattedAddress")
        or place.get("formattedAddress"),
        "rating": details.get("rating", place.get("rating")),
        "user_rating_count": details.get("userRatingCount", place.get("userRatingCount")),
        "google_maps_url": details.get("googleMapsUri"),
        "reviews": [_compact_review(review) for review in review_items[:3]],
    }


def _compact_review(review: Any) -> dict[str, Any]:
    review_data = review if isinstance(review, dict) else {}
    return {
        "rating": review_data.get("rating"),
        "published": review_data.get("relativePublishTimeDescription"),
        "text": (
            _localized_text(review_data.get("text"))
            or _localized_text(review_data.get("originalText"))
        )[:500],
    }


def _build_search_query(hospital: dict[str, Any]) -> str:
    parts = [
        hospital.get("name"),
        hospital.get("address"),
        hospital.get("address_city"),
        hospital.get("address_stateOrRegion"),
        hospital.get("address_country"),
    ]
    return ", ".join(
        str(part).strip()
        for part in parts
        if part and str(part).strip().casefold() != "null"
    )


def _localized_text(value: Any) -> str:
    if isinstance(value, dict):
        text = value.get("text")
        return text.strip() if isinstance(text, str) else ""
    return ""


def _coerce_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
