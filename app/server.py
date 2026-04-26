from __future__ import annotations

import argparse
import json
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
from backend.core.llm import (
    choose_doctor_category,
    is_llm_configured,
    load_llm_config,
)
from backend.core.matching import recommend_hospitals_for_diagnosis
from manager.pipeline import write_user_timestamp

load_env_file(PROJECT_ROOT / ".env")


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
            self._write_json(
                {
                    "llm": {
                        "enabled": llm_config.is_configured,
                        "model": llm_config.model or None,
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
            latitude = _payload_float(payload, "latitude")
            longitude = _payload_float(payload, "longitude")
            if latitude is None or longitude is None:
                response = {
                    "input": {"query": query},
                    "diagnosis": {
                        "name": selected_category,
                        "confidence_score": diagnosis_match.score,
                        "reason": diagnosis_match.reason,
                        "urgency": "routine",
                        "source": "local_fallback",
                    },
                    "location": None,
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
                "urgency": "routine",
                "source": "local_fallback",
            }

        hospitals = recommend_hospitals_for_diagnosis(
            selected_category,
            latitude,
            longitude,
            limit=limit,
            radius_km=radius_value,
        )

        response = {
            "input": {
                "query": query,
                "doctor_type": doctor_type,
                "diagnosis": diagnosis,
                "description": description,
                "latitude": latitude,
                "longitude": longitude,
                "user_record_path": str(user_record_path),
            },
            "diagnosis": diagnosis,
            "location": {
                "latitude": latitude,
                "longitude": longitude,
                "text": _location_text(doctor_decision, geocoded_location),
                "source": _location_source(doctor_decision, geocoded_location),
                "geocoding_error": geocoding_error,
            },
            "hospital": hospitals[0] if hospitals else None,
            "matches": hospitals,
        }
        response["message"] = format_recommendation_message(
            diagnosis,
            response["location"],
            hospitals,
        )
        response["llm"] = {
            "enabled": is_llm_configured(),
            "used": doctor_decision is not None,
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

    if hospitals:
        lines.extend(["", "5 closest matching providers:"])
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
