from __future__ import annotations

import argparse
import json
import os
import re
import sys
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = PROJECT_ROOT / "frontend"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.core.env import load_env_file
from backend.core.diagnosis import available_diagnosis_names, classify_diagnosis
from backend.core.geocoding import geocode_address
from backend.core.google_reviews import enrich_hospitals_with_google_reviews
from backend.core.llm import (
    choose_doctor_category,
    is_llm_configured,
    load_hospital_selection_llm_config,
    load_llm_config,
    select_best_hospital,
)
from backend.core.matching import (
    enrich_hospitals_from_full_csv,
    recommend_hospitals_for_diagnosis,
)
from manager.pipeline import write_user_timestamp

load_env_file(PROJECT_ROOT / ".env")
load_env_file(PROJECT_ROOT / "configs" / "api_keys.env")

COORDINATE_PAIR_RE = re.compile(
    r"(?P<latitude>[+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*,\s*"
    r"(?P<longitude>[+-]?(?:\d+(?:\.\d*)?|\.\d+))"
)
LOCATION_HINT_RE = re.compile(
    r"\b(?:near|nearby|around|at|in)\s*[:,]?\s+(?P<location>.+)$",
    re.IGNORECASE,
)


class AppHandler(SimpleHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/categories":
            self._write_json(
                {
                    "categories": available_diagnosis_names(),
                }
            )
            return

        if parsed.path == "/api/config":
            llm_config = load_llm_config()
            hospital_selection_config = load_hospital_selection_llm_config()
            google_key = (os.environ.get("GOOGLE_PLACES_API_KEY") or "").strip()
            self._write_json(
                {
                    "llm": {
                        "enabled": llm_config.is_configured,
                        "model": llm_config.model or None,
                    },
                    "hospital_selection_llm": {
                        "enabled": hospital_selection_config.is_configured,
                        "model": hospital_selection_config.model or None,
                    },
                    "google_reviews": {
                        "enabled": bool(
                            google_key and not google_key.startswith("your-google-")
                        ),
                    },
                }
            )
            return

        if parsed.path == "/":
            self.path = "/index.html"

        super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/recommend":
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")
            return

        try:
            payload = self._read_json_body()
            query = str(payload.get("query", "")).strip()
            limit = int(payload.get("limit", 5))
            radius_km = payload.get("radius_km")
            radius_value = float(radius_km) if radius_km not in (None, "") else None
        except (TypeError, ValueError):
            self._write_json(
                {"error": "query, radius_km, and limit must be valid values"},
                status=400,
            )
            return

        if not query:
            self._write_json({"error": "query must not be empty"}, status=400)
            return

        llm_error = None
        if is_llm_configured():
            try:
                doctor_decision = choose_doctor_category(query)
            except RuntimeError as error:
                llm_error = str(error)
                diagnosis_match = classify_diagnosis(query)
                doctor_decision = None
        else:
            diagnosis_match = classify_diagnosis(query)
            doctor_decision = None

        if doctor_decision is not None and not doctor_decision.is_medical_request:
            response = {
                "input": {
                    "query": query,
                },
                "diagnosis": None,
                "location": None,
                "hospital": None,
                "matches": [],
                "llm": {"enabled": True, "used": True, "error": None},
                "message": format_non_medical_message(),
            }
            self._write_json(response)
            return

        geocoding_error = None
        geocoded_location = None
        if (
            doctor_decision is not None
            and not doctor_decision.has_location
            and doctor_decision.location_text
        ):
            try:
                geocoded_location = geocode_address(doctor_decision.location_text)
            except RuntimeError as error:
                geocoding_error = str(error)

        if (
            doctor_decision is not None
            and not doctor_decision.has_location
            and geocoded_location is None
        ):
            response = {
                "input": {"query": query},
                "diagnosis": {
                    "name": doctor_decision.category,
                    "confidence_score": doctor_decision.confidence_score,
                    "reason": doctor_decision.reason,
                    "urgency": doctor_decision.urgency,
                    "source": "openai",
                },
                "location": {
                    "latitude": None,
                    "longitude": None,
                    "text": doctor_decision.location_text,
                    "source": "openai",
                    "error": geocoding_error,
                },
                "hospital": None,
                "matches": [],
                "llm": {"enabled": True, "used": True, "error": None},
                "message": format_location_needed_message(
                    doctor_decision,
                    geocoding_error=geocoding_error,
                ),
            }
            self._write_json(response)
            return

        if doctor_decision is not None:
            selected_category = doctor_decision.category or "Primary Care / General Practice"
            latitude = (
                geocoded_location.latitude
                if geocoded_location is not None
                else doctor_decision.latitude
            )
            longitude = (
                geocoded_location.longitude
                if geocoded_location is not None
                else doctor_decision.longitude
            )
            diagnosis = {
                "name": selected_category,
                "confidence_score": doctor_decision.confidence_score,
                "reason": doctor_decision.reason,
                "urgency": doctor_decision.urgency,
                "source": "openai",
            }
        else:
            selected_category = diagnosis_match.english_name
            fallback_location_text = None
            latitude = _payload_float(payload, "latitude")
            longitude = _payload_float(payload, "longitude")
            if latitude is None or longitude is None:
                latitude, longitude = _coordinates_from_text(query)
            if latitude is None or longitude is None:
                fallback_location_text = _location_text_from_query(query)
                if fallback_location_text:
                    try:
                        geocoded_location = geocode_address(fallback_location_text)
                        latitude = geocoded_location.latitude
                        longitude = geocoded_location.longitude
                    except RuntimeError as error:
                        geocoding_error = str(error)
            if latitude is None or longitude is None:
                response = {
                    "input": {"query": query},
                    "diagnosis": {
                        "name": selected_category,
                        "confidence_score": diagnosis_match.score,
                        "reason": diagnosis_match.reason,
                        "urgency": _fallback_urgency(selected_category),
                        "source": "local_fallback",
                    },
                    "location": {
                        "latitude": None,
                        "longitude": None,
                        "text": fallback_location_text,
                        "source": "payload",
                        "geocoding_error": geocoding_error,
                    },
                    "hospital": None,
                    "matches": [],
                    "llm": {
                        "enabled": is_llm_configured(),
                        "used": False,
                        "error": llm_error,
                    },
                    "message": (
                        "Please include the patient's latitude and longitude so I can "
                        "find the 5 closest matching providers."
                    ),
                }
                self._write_json(response)
                return
            diagnosis = {
                "name": diagnosis_match.english_name,
                "confidence_score": diagnosis_match.score,
                "reason": diagnosis_match.reason,
                "urgency": _fallback_urgency(selected_category),
                "source": "local_fallback",
            }

        hospitals = recommend_hospitals_for_diagnosis(
            selected_category,
            latitude,
            longitude,
            limit=limit,
            radius_km=radius_value,
        )
        enriched_hospitals = enrich_hospitals_from_full_csv(hospitals)
        enriched_hospitals, google_reviews_error = enrich_hospitals_with_google_reviews(
            enriched_hospitals[:5]
        )

        selection_error = None
        hospital_selection = None
        selected_hospital = enriched_hospitals[0] if enriched_hospitals else None
        if enriched_hospitals and is_llm_configured():
            try:
                hospital_selection = select_best_hospital(
                    patient_message=query,
                    diagnosis=str(diagnosis["name"]),
                    latitude=latitude,
                    longitude=longitude,
                    hospitals=enriched_hospitals[:5],
                )
                selected_hospital = _hospital_by_name(
                    enriched_hospitals,
                    hospital_selection.selected_name,
                ) or selected_hospital
            except RuntimeError as error:
                selection_error = str(error)
        elif enriched_hospitals:
            selection_error = (
                "OpenAI is not configured. Set OPENAI_API_KEY, OPENAI_MODEL, "
                "and OPENAI_HOSPITAL_SELECTION_MODEL."
            )

        response = {
            "input": {
                "query": query,
                "diagnosis": diagnosis,
                "latitude": latitude,
                "longitude": longitude,
            },
            "diagnosis": diagnosis,
            "location": {
                "latitude": latitude,
                "longitude": longitude,
                "text": _location_text(doctor_decision, geocoded_location),
                "source": _location_source(doctor_decision, geocoded_location),
                "geocoding_error": geocoding_error,
            },
            "hospital": selected_hospital,
            "hospital_selection": _hospital_selection_payload(
                hospital_selection,
                selected_hospital,
                selection_error,
            ),
            "matches": enriched_hospitals,
            "google_reviews_error": google_reviews_error,
        }
        response["message"] = format_recommendation_message(
            diagnosis,
            response["location"],
            enriched_hospitals,
            selected_hospital=selected_hospital,
            hospital_selection=response["hospital_selection"],
            google_reviews_error=google_reviews_error,
        )
        response["llm"] = {
            "enabled": is_llm_configured(),
            "used": doctor_decision is not None,
            "hospital_selection_used": hospital_selection is not None,
            "error": None,
        }

        if llm_error and not response["llm"]["error"]:
            response["llm"]["error"] = llm_error

        self._write_json(response)

    def _read_json_body(self) -> dict[str, object]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        loaded = json.loads(raw_body.decode("utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("JSON body must be an object")
        return loaded

    def _write_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def format_non_medical_message() -> str:
    return (
        "I can help choose the right doctor category from symptoms or a care need. "
        "Please describe what the patient is experiencing, how long it has been "
        "happening, and any urgent warning signs."
    )


def format_location_needed_message(
    doctor_decision: object,
    *,
    geocoding_error: str | None = None,
) -> str:
    category = getattr(doctor_decision, "category", None)
    confidence = getattr(doctor_decision, "confidence_score", None)
    reason = getattr(doctor_decision, "reason", None)
    lines = [
        "I can route this to a doctor category, but I need the patient's location before I can find nearby providers.",
    ]
    if category:
        lines.extend(
            [
                "",
                f"Doctor category: {category}",
                f"Confidence: {confidence}",
                f"Reason: {reason}",
            ]
        )
    if geocoding_error:
        lines.extend(["", f"Address lookup issue: {geocoding_error}"])
    lines.extend(
        [
            "",
            "Please reply with a full address or latitude/longitude, for example: 28.6139, 77.2090.",
        ]
    )
    return "\n".join(lines)


def format_recommendation_message(
    diagnosis: dict[str, object],
    location: dict[str, object],
    hospitals: list[dict[str, object]],
    *,
    selected_hospital: dict[str, object] | None = None,
    hospital_selection: dict[str, object] | None = None,
    google_reviews_error: str | None = None,
) -> str:
    lines = [
        "Routing guidance only (not a diagnosis).",
        "",
        f"Doctor category: {diagnosis['name']}",
        f"Confidence: {diagnosis['confidence_score']}",
        f"Urgency: {diagnosis['urgency']}",
        f"Reason: {diagnosis['reason']}",
        f"Location: {location['latitude']}, {location['longitude']}",
    ]

    if selected_hospital:
        distance = selected_hospital.get("distance_km")
        distance_text = (
            f"{distance:.2f} km" if isinstance(distance, (int, float)) else "distance unavailable"
        )
        selection = hospital_selection or {}
        treats = selection.get("treats") or _hospital_treatment_summary(selected_hospital)
        hospital_location = selection.get("location") or _hospital_location_summary(selected_hospital)
        reason = selection.get("reason") or "This is the closest matching provider in the dataset."
        reviews = _google_reviews_summary(selected_hospital)
        lines.extend(
            [
                "",
                f"Best hospital: {selected_hospital.get('name')} ({selected_hospital.get('type')})",
                f"Distance: {distance_text}",
                f"What they treat: {treats}",
                f"Where it is situated: {hospital_location}",
                f"Why this hospital: {reason}",
            ]
        )
        if reviews:
            lines.append(f"Google reviews: {reviews}")
        website = selection.get("website") or _first_hospital_value(
            selected_hospital,
            "officialWebsite",
            "websites",
        )
        phone = selection.get("phone") or _first_hospital_value(
            selected_hospital,
            "officialPhone",
            "phone_numbers",
        )
        if website:
            lines.append(f"Website: {website}")
        if phone:
            lines.append(f"Phone: {phone}")
        selection_error = selection.get("error")
        if selection_error:
            lines.append(f"Second LLM issue: {selection_error}")

    if google_reviews_error:
        lines.extend(["", f"Google review lookup issue: {google_reviews_error}"])

    if hospitals:
        lines.extend(["", "5 closest matching providers considered:"])
        for index, hospital in enumerate(hospitals[:5], start=1):
            distance = hospital.get("distance_km")
            distance_text = (
                f"{distance:.2f} km" if isinstance(distance, (int, float)) else "unavailable"
            )
            description = hospital.get("description")
            description_text = (
                f"\n   Why matched: {description}"
                if description and description != "null"
                else ""
            )
            lines.append(
                f"{index}. {hospital.get('name')} ({hospital.get('type')}) - "
                f"{distance_text}{description_text}"
            )
    else:
        lines.extend(
            [
                "",
                "No nearby provider in the dataset matched this category.",
            ]
        )

    lines.extend(["", _next_step_for_urgency(str(diagnosis["urgency"]))])
    return "\n".join(lines)


def _hospital_selection_payload(
    hospital_selection: object,
    selected_hospital: dict[str, object] | None,
    error: str | None,
) -> dict[str, object]:
    if hospital_selection is None:
        return {
            "source": "distance_fallback",
            "selected_name": selected_hospital.get("name") if selected_hospital else None,
            "confidence_score": None,
            "reason": "Used the closest matching provider because the second LLM selection was unavailable.",
            "treats": _hospital_treatment_summary(selected_hospital or {}),
            "location": _hospital_location_summary(selected_hospital or {}),
            "website": _first_hospital_value(selected_hospital or {}, "officialWebsite", "websites"),
            "phone": _first_hospital_value(selected_hospital or {}, "officialPhone", "phone_numbers"),
            "error": error,
        }

    return {
        "source": "openai",
        "selected_name": getattr(hospital_selection, "selected_name", None),
        "confidence_score": getattr(hospital_selection, "confidence_score", None),
        "reason": getattr(hospital_selection, "reason", None),
        "treats": getattr(hospital_selection, "treats", None),
        "location": getattr(hospital_selection, "location", None),
        "website": getattr(hospital_selection, "website", None),
        "phone": getattr(hospital_selection, "phone", None),
        "error": error,
    }


def _hospital_by_name(
    hospitals: list[dict[str, object]],
    name: str | None,
) -> dict[str, object] | None:
    if not name:
        return None
    for hospital in hospitals:
        if str(hospital.get("name", "")).strip() == name:
            return hospital
    return None


def _hospital_treatment_summary(hospital: dict[str, object]) -> str:
    for key in ("description", "procedure", "capability", "specialties", "diagnosis"):
        value = hospital.get(key)
        if value and str(value).strip().casefold() != "null":
            return str(value)
    return "Treatment details are limited in the dataset."


def _hospital_location_summary(hospital: dict[str, object]) -> str:
    address = hospital.get("address")
    if address:
        return str(address)
    parts = [
        hospital.get("address_line1"),
        hospital.get("address_line2"),
        hospital.get("address_line3"),
        hospital.get("address_city"),
        hospital.get("address_stateOrRegion"),
        hospital.get("address_zipOrPostcode"),
        hospital.get("address_country"),
    ]
    address_text = ", ".join(
        str(part).strip()
        for part in parts
        if part and str(part).strip().casefold() != "null"
    )
    if address_text:
        return address_text
    latitude = hospital.get("latitude")
    longitude = hospital.get("longitude")
    if latitude is not None and longitude is not None:
        return f"{latitude}, {longitude}"
    return "Location details are limited in the dataset."


def _first_hospital_value(hospital: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        value = hospital.get(key)
        if value and str(value).strip().casefold() != "null":
            return str(value)
    return None


def _google_reviews_summary(hospital: dict[str, object]) -> str | None:
    google_reviews = hospital.get("google_reviews")
    if not isinstance(google_reviews, dict) or not google_reviews.get("available"):
        return None
    rating = google_reviews.get("rating")
    rating_count = google_reviews.get("user_rating_count")
    maps_url = google_reviews.get("google_maps_url")
    parts = []
    if rating is not None:
        parts.append(f"{rating}/5")
    if rating_count is not None:
        parts.append(f"{rating_count} reviews")
    if maps_url:
        parts.append(str(maps_url))
    return ", ".join(parts) if parts else None


def _next_step_for_urgency(urgency: str) -> str:
    if urgency == "emergency":
        return "Next step: seek emergency medical care now or call local emergency services."
    if urgency == "urgent":
        return "Next step: arrange urgent care today."
    if urgency == "soon":
        return "Next step: contact the matched provider or another suitable clinician soon."
    return "Next step: schedule a routine appointment with the matched provider or another suitable clinician."


def _payload_float(payload: dict[str, object], key: str) -> float | None:
    try:
        return float(payload[key])
    except (KeyError, TypeError, ValueError):
        return None


def _coordinates_from_text(text: str) -> tuple[float | None, float | None]:
    match = COORDINATE_PAIR_RE.search(text)
    if match is None:
        return None, None

    latitude = _bounded_float(match.group("latitude"), -90, 90)
    longitude = _bounded_float(match.group("longitude"), -180, 180)
    if latitude is None or longitude is None:
        return None, None
    return latitude, longitude


def _bounded_float(value: str, minimum: float, maximum: float) -> float | None:
    try:
        number = float(value)
    except ValueError:
        return None
    if not minimum <= number <= maximum:
        return None
    return number


def _location_text_from_query(query: str) -> str | None:
    match = LOCATION_HINT_RE.search(query)
    if match is not None:
        location = match.group("location").strip(" .")
        return location or None
    return None


def _fallback_urgency(category: str) -> str:
    if category == "Emergency Medicine":
        return "emergency"
    return "routine"


def _location_text(doctor_decision: object, geocoded_location: object) -> str | None:
    formatted_address = getattr(geocoded_location, "formatted_address", None)
    if isinstance(formatted_address, str):
        return formatted_address
    if doctor_decision is not None:
        location_text = getattr(doctor_decision, "location_text", None)
        return location_text if isinstance(location_text, str) else None
    return None


def _location_source(doctor_decision: object, geocoded_location: object) -> str:
    provider = getattr(geocoded_location, "provider", None)
    if isinstance(provider, str):
        return provider
    return "openai" if doctor_decision else "payload"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve the frontend together with the diagnosis recommendation API."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    handler = partial(AppHandler, directory=str(FRONTEND_DIR))
    with ThreadingHTTPServer((args.host, args.port), handler) as server:
        print(f"Serving app on http://{args.host}:{args.port}")
        server.serve_forever()


if __name__ == "__main__":
    main()
