from __future__ import annotations

import argparse
import json
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

from backend.core.diagnosis import available_diagnosis_names, classify_diagnosis
from backend.core.matching import recommend_hospitals_for_diagnosis

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "config.yaml"
TEST_MODE_QUERY = "tooth pain"
TEST_MODE_LATITUDE = 28.6139
TEST_MODE_LONGITUDE = 77.2090
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


def build_recommendation_response(query: str, limit: int) -> dict[str, object]:
    app_config = load_app_config()
    if app_config.test_mode:
        effective_query = TEST_MODE_QUERY
        latitude = TEST_MODE_LATITUDE
        longitude = TEST_MODE_LONGITUDE
        effective_limit = TEST_MODE_LIMIT
        test_mode = True
    else:
        effective_query = query
        diagnosis_match = classify_diagnosis(query)
        latitude = TEST_MODE_LATITUDE
        longitude = TEST_MODE_LONGITUDE
        effective_limit = limit
        hospitals = recommend_hospitals_for_diagnosis(
            diagnosis_match.english_name,
            latitude,
            longitude,
            limit=effective_limit,
        )
        return {
            "input": {"query": query},
            "test_mode": False,
            "diagnosis": {
                "name": diagnosis_match.english_name,
                "confidence_score": diagnosis_match.score,
                "reason": diagnosis_match.reason,
            },
            "matches": hospitals,
        }

    diagnosis_match = classify_diagnosis(effective_query)
    hospitals = recommend_hospitals_for_diagnosis(
        diagnosis_match.english_name,
        latitude,
        longitude,
        limit=effective_limit,
    )
    return {
        "input": {"query": query},
        "test_mode": test_mode,
        "test_scenario": {
            "query": effective_query,
            "latitude": latitude,
            "longitude": longitude,
        },
        "diagnosis": {
            "name": diagnosis_match.english_name,
            "confidence_score": diagnosis_match.score,
            "reason": diagnosis_match.reason,
        },
        "matches": hospitals,
    }


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

        response = build_recommendation_response(query, limit)
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
