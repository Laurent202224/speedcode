from __future__ import annotations

import json
import os
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


GOOGLE_GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"


@dataclass(frozen=True)
class GeocodedLocation:
    latitude: float
    longitude: float
    formatted_address: str
    provider: str


def geocode_address(address: str) -> GeocodedLocation:
    api_key = (os.environ.get("GOOGLE_PLACES_API_KEY") or "").strip()
    if not api_key or api_key.startswith("your-google-"):
        raise RuntimeError("Geocoding is not configured. Set GOOGLE_PLACES_API_KEY.")

    query = urlencode({"address": address, "key": api_key})
    request = Request(f"{GOOGLE_GEOCODING_URL}?{query}", method="GET")

    try:
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Geocoding request failed with HTTP {error.code}: {detail}") from error
    except URLError as error:
        raise RuntimeError(f"Geocoding request failed: {error.reason}") from error

    if not isinstance(payload, dict):
        raise RuntimeError("Geocoding response was not a JSON object")

    status = payload.get("status")
    if status != "OK":
        message = payload.get("error_message")
        detail = f": {message}" if isinstance(message, str) and message else ""
        raise RuntimeError(f"Geocoding failed with status {status}{detail}")

    results = payload.get("results")
    if not isinstance(results, list) or not results:
        raise RuntimeError("Geocoding returned no results")

    first_result = results[0]
    if not isinstance(first_result, dict):
        raise RuntimeError("Geocoding result was not an object")

    geometry = first_result.get("geometry")
    location = geometry.get("location") if isinstance(geometry, dict) else None
    if not isinstance(location, dict):
        raise RuntimeError("Geocoding result did not include coordinates")

    try:
        latitude = float(location["lat"])
        longitude = float(location["lng"])
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("Geocoding coordinates were invalid") from error

    formatted_address = first_result.get("formatted_address")
    return GeocodedLocation(
        latitude=latitude,
        longitude=longitude,
        formatted_address=formatted_address if isinstance(formatted_address, str) else address,
        provider="google_geocoding",
    )
