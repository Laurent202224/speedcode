from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = PROJECT_ROOT / "frontend"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.core.env import load_env_file
from backend.core.diagnosis import available_diagnosis_names, classify_diagnosis
from backend.core.env import load_env_file
from backend.core.matching import recommend_hospitals_for_diagnosis
from backend.core.openai_pipeline import PipelineError, find_and_rerank_matches

load_env_file(PROJECT_ROOT / ".env")

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "config.yaml"
TEST_MODE_LIMIT = 5


@dataclass(frozen=True)
class AppConfig:
    test_mode: bool


def load_app_config(config_path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    if not config_path.exists():
        return AppConfig(test_mode=False)

    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        return AppConfig(test_mode=False)

    app_data = loaded.get("app")
    if not isinstance(app_data, dict):
        return AppConfig(test_mode=False)

    return AppConfig(test_mode=bool(app_data.get("test_mode", False)))


def build_recommendation_response(
    query: str,
    latitude: float,
    longitude: float,
    limit: int,
) -> dict[str, object]:
    app_config = load_app_config()
    if app_config.test_mode:
        exact_diagnosis = resolve_supported_diagnosis(query)
        if exact_diagnosis is None:
            return {
                "input": {
                    "query": query,
                    "latitude": latitude,
                    "longitude": longitude,
                },
                "test_mode": True,
                "diagnosis": None,
                "matches": [],
                "message": "This diagnosis cannot be treated because it is not in the supported diagnosis set.",
            }

        diagnosis_name = exact_diagnosis
        diagnosis_payload = {
            "name": exact_diagnosis,
            "confidence_score": 1.0,
            "reason": "exact supported diagnosis match",
        }
    else:
        diagnosis_match = classify_diagnosis(query)
        diagnosis_name = diagnosis_match.english_name
        diagnosis_payload = {
            "name": diagnosis_match.english_name,
            "confidence_score": diagnosis_match.score,
            "reason": diagnosis_match.reason,
        }

    hospitals = recommend_hospitals_for_diagnosis(
        diagnosis_name,
        latitude,
        longitude,
        limit=limit,
    )
    return {
        "input": {
            "query": query,
            "latitude": latitude,
            "longitude": longitude,
        },
        "test_mode": app_config.test_mode,
        "diagnosis": diagnosis_payload,
        "matches": hospitals,
        "best_match": hospitals[0] if hospitals else None,
    }


def resolve_supported_diagnosis(query: str) -> str | None:
    normalized_query = query.strip().casefold()
    if not normalized_query:
        return None

    for diagnosis in available_diagnosis_names():
        if diagnosis.casefold() == normalized_query:
            return diagnosis
    return None


class AppHandler(SimpleHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/app-config":
            app_config = load_app_config()
            self._write_json(
                {
                    "test_mode": app_config.test_mode,
                }
            )
            return
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
            limit = int(payload.get("limit", TEST_MODE_LIMIT))
        except (TypeError, ValueError):
            self._write_json(
                {"error": "query and limit must be valid values"},
                status=400,
            )
            return

        if not query:
            self._write_json({"error": "query must not be empty"}, status=400)
            return

        app_config = load_app_config()
        if app_config.test_mode:
            try:
                latitude = float(payload["latitude"])
                longitude = float(payload["longitude"])
            except KeyError as error:
                self._write_json({"error": f"Missing field: {error.args[0]}"}, status=400)
                return
            except (TypeError, ValueError):
                self._write_json(
                    {"error": "latitude and longitude must be valid values"},
                    status=400,
                )
                return
        else:
            try:
                response = build_non_test_recommendation_response(query, limit)
            except (PipelineError, ValueError) as error:
                self._write_json({"error": str(error)}, status=400)
                return
            self._write_json(response)
            return

        try:
            response = build_recommendation_response(
                query, latitude, longitude, limit
            )
        except ValueError as error:
            self._write_json({"error": str(error)}, status=400)
            return

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


def build_non_test_recommendation_response(query: str, limit: int) -> dict[str, object]:
    pipeline_result = find_and_rerank_matches(query, limit=limit)
    extraction = pipeline_result["extraction"]
    return {
        "input": {"query": query},
        "test_mode": False,
        "extraction": {
            "diagnosis_name": extraction.diagnosis_name,
            "location_text": extraction.location_text,
            "latitude": extraction.latitude,
            "longitude": extraction.longitude,
            "need_description": extraction.need_description,
            "geocoding_used": extraction.geocoding_used,
            "geocoding_source": extraction.geocoding_source,
        },
        "diagnosis": {
            "name": extraction.diagnosis_name,
            "confidence_score": None,
            "reason": "extracted by OpenAI",
        },
        "matches": pipeline_result["matches"],
        "best_match": pipeline_result["best_match"],
        "rerank": (
            {
                "selected_index": pipeline_result["rerank"].selected_index,
                "reason": pipeline_result["rerank"].reason,
                "match_strength": pipeline_result["rerank"].match_strength,
            }
            if pipeline_result.get("rerank") is not None
            else None
        ),
    }


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
