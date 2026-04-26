from __future__ import annotations

import csv
import json
import os
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml

from backend.core.diagnosis import available_diagnosis_names
from backend.core.geocoding import GeocodedLocation, geocode_address
from backend.core.matching import recommend_hospitals_for_diagnosis


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "config.yaml"
DEFAULT_SOURCE_CSV_PATH = PROJECT_ROOT / "data" / "data_source" / "data_full.csv"
RESPONSES_API_URL = "https://api.openai.com/v1/responses"


class PipelineError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExtractionResult:
    diagnosis_name: str
    location_text: str
    latitude: float
    longitude: float
    need_description: str
    geocoding_used: bool = False
    geocoding_source: str | None = None


@dataclass(frozen=True)
class RerankResult:
    selected_index: int
    reason: str
    match_strength: str


@dataclass(frozen=True)
class OpenAISettings:
    api_key: str
    extraction_model: str
    rerank_model: str
    source_csv_path: Path


def load_openai_settings(config_path: Path = DEFAULT_CONFIG_PATH) -> OpenAISettings:
    load_dotenv(PROJECT_ROOT / ".env")

    config = _read_yaml_mapping(config_path)
    paths = _mapping_value(config.get("paths"))
    app = _mapping_value(config.get("app"))
    openai_config = _mapping_value(app.get("openai"))

    api_key_env = str(openai_config.get("api_key_env") or "OPENAI_API_KEY")
    api_key = os.environ.get(api_key_env, "").strip()
    if not api_key:
        raise PipelineError(
            f"Missing OpenAI API key. Set {api_key_env} in your environment or .env file."
        )

    source_csv_path = paths.get("raw_source_csv") or DEFAULT_SOURCE_CSV_PATH
    return OpenAISettings(
        api_key=api_key,
        extraction_model=str(openai_config.get("extraction_model") or "gpt-5.4-mini"),
        rerank_model=str(openai_config.get("rerank_model") or "gpt-5.4-mini"),
        source_csv_path=_resolve_project_path(source_csv_path),
    )


def extract_need_from_prompt(prompt: str, *, config_path: Path = DEFAULT_CONFIG_PATH) -> ExtractionResult:
    settings = load_openai_settings(config_path)
    schema = {
        "type": "object",
        "properties": {
            "diagnosis_name": {
                "type": "string",
                "enum": available_diagnosis_names(),
            },
            "location_text": {"type": "string"},
            "latitude": {"type": "number"},
            "longitude": {"type": "number"},
            "need_description": {"type": "string"},
        },
        "required": [
            "diagnosis_name",
            "location_text",
            "latitude",
            "longitude",
            "need_description",
        ],
        "additionalProperties": False,
    }
    messages = [
        {
            "role": "system",
            "content": (
                "Extract structured healthcare search intent from the user's prompt. "
                "Choose the closest supported diagnosis category from the provided enum, "
                "extract the location phrase (address, city, or place name), estimate "
                "approximate latitude and longitude for that location, and write a compact "
                "need description for downstream matching."
            ),
        },
        {"role": "user", "content": prompt},
    ]
    parsed = _responses_json_schema(
        settings.api_key,
        settings.extraction_model,
        messages,
        "healthcare_search_extraction",
        schema,
    )
    
    # Extract LLM-estimated coordinates
    location_text = str(parsed["location_text"])
    llm_latitude = float(parsed["latitude"])
    llm_longitude = float(parsed["longitude"])
    
    # Try to geocode the location text for more accurate coordinates
    geocoded_location: GeocodedLocation | None = None
    if location_text and location_text.strip():
        try:
            geocoded_location = geocode_address(location_text.strip())
        except RuntimeError:
            # Geocoding failed, will use LLM estimates
            pass
    
    # Use geocoded coordinates if available, otherwise fall back to LLM estimates
    final_latitude = geocoded_location.latitude if geocoded_location else llm_latitude
    final_longitude = geocoded_location.longitude if geocoded_location else llm_longitude
    geocoding_used = geocoded_location is not None
    geocoding_source = geocoded_location.provider if geocoded_location else "llm_estimate"
    
    return ExtractionResult(
        diagnosis_name=str(parsed["diagnosis_name"]),
        location_text=location_text,
        latitude=final_latitude,
        longitude=final_longitude,
        need_description=str(parsed["need_description"]),
        geocoding_used=geocoding_used,
        geocoding_source=geocoding_source,
    )


def find_and_rerank_matches(
    prompt: str,
    *,
    limit: int = 5,
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> dict[str, Any]:
    settings = load_openai_settings(config_path)
    extraction = extract_need_from_prompt(prompt, config_path=config_path)
    candidates = recommend_hospitals_for_diagnosis(
        extraction.diagnosis_name,
        extraction.latitude,
        extraction.longitude,
        limit=limit,
        config_path=config_path,
    )
    enriched_candidates = enrich_candidates_with_source_context(
        candidates, settings.source_csv_path
    )

    if not enriched_candidates:
        return {
            "extraction": extraction,
            "matches": [],
            "best_match": None,
        }

    rerank = rerank_candidates(
        prompt,
        extraction,
        enriched_candidates,
        settings,
    )
    best_match = None
    if 0 <= rerank.selected_index < len(enriched_candidates):
        best_match = dict(enriched_candidates[rerank.selected_index])
        best_match["selection_reason"] = rerank.reason
        best_match["match_strength"] = rerank.match_strength

    return {
        "extraction": extraction,
        "matches": enriched_candidates,
        "best_match": best_match,
        "rerank": rerank,
    }


def rerank_candidates(
    prompt: str,
    extraction: ExtractionResult,
    candidates: list[dict[str, Any]],
    settings: OpenAISettings,
) -> RerankResult:
    schema = {
        "type": "object",
        "properties": {
            "selected_index": {"type": "integer"},
            "reason": {"type": "string"},
            "match_strength": {
                "type": "string",
                "enum": ["strong", "moderate", "weak"],
            },
        },
        "required": ["selected_index", "reason", "match_strength"],
        "additionalProperties": False,
    }

    candidate_context = []
    for index, candidate in enumerate(candidates):
        candidate_context.append(
            {
                "index": index,
                "name": candidate.get("name"),
                "diagnosis": candidate.get("diagnosis"),
                "type": candidate.get("type"),
                "distance_km": candidate.get("distance_km"),
                "trustworthy_score": candidate.get("trustworthy_score"),
                "description": candidate.get("description"),
                "source_context": candidate.get("source_context"),
            }
        )

    messages = [
        {
            "role": "system",
            "content": (
                "Choose the single best hospital candidate for the user's need. "
                "Use the need description, extracted diagnosis, estimated location, "
                "distance, trust score, and source context fields such as specialties, "
                "procedures, capability, equipment, doctor count, capacity, and profile details. "
                "Return the best candidate index and a short reason."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "user_prompt": prompt,
                    "extraction": {
                        "diagnosis_name": extraction.diagnosis_name,
                        "location_text": extraction.location_text,
                        "latitude": extraction.latitude,
                        "longitude": extraction.longitude,
                        "need_description": extraction.need_description,
                    },
                    "candidates": candidate_context,
                },
                ensure_ascii=False,
            ),
        },
    ]
    parsed = _responses_json_schema(
        settings.api_key,
        settings.rerank_model,
        messages,
        "hospital_rerank_decision",
        schema,
    )
    return RerankResult(
        selected_index=int(parsed["selected_index"]),
        reason=str(parsed["reason"]),
        match_strength=str(parsed["match_strength"]),
    )


def enrich_candidates_with_source_context(
    candidates: list[dict[str, Any]],
    source_csv_path: Path,
) -> list[dict[str, Any]]:
    source_index = load_source_index(source_csv_path)
    enriched: list[dict[str, Any]] = []
    for candidate in candidates:
        enriched_candidate = dict(candidate)
        source_record = source_index.get(_normalize_name(str(candidate.get("name", ""))))
        if source_record is not None:
            enriched_candidate["source_context"] = build_source_context(source_record)
        else:
            enriched_candidate["source_context"] = {}
        enriched.append(enriched_candidate)
    return enriched


@lru_cache(maxsize=2)
def load_source_index(source_csv_path: Path) -> dict[str, dict[str, str]]:
    with source_csv_path.open(encoding="utf-8", newline="") as csv_file:
        cleaned_lines = (line.replace("\x00", "") for line in csv_file)
        reader = csv.DictReader(cleaned_lines)
        indexed: dict[str, dict[str, str]] = {}
        for row in reader:
            name = _normalize_name(row.get("name", ""))
            if name and name not in indexed:
                indexed[name] = row
        return indexed


def build_source_context(row: MappingABC[str, Any]) -> dict[str, Any]:
    return {
        "address": ", ".join(
            str(row.get(field, "")).strip()
            for field in (
                "address_line1",
                "address_line2",
                "address_line3",
                "address_city",
                "address_stateOrRegion",
                "address_country",
            )
            if _filled(row.get(field))
        ),
        "specialties": _parse_jsonish_list(row.get("specialties")),
        "procedure": _parse_jsonish_list(row.get("procedure")),
        "equipment": _parse_jsonish_list(row.get("equipment")),
        "capability": _parse_jsonish_list(row.get("capability")),
        "numberDoctors": row.get("numberDoctors"),
        "capacity": row.get("capacity"),
        "officialWebsite": row.get("officialWebsite"),
        "officialPhone": row.get("officialPhone"),
        "email": row.get("email"),
        "facilityTypeId": row.get("facilityTypeId"),
        "description": row.get("description"),
    }


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def _responses_json_schema(
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    schema_name: str,
    schema: dict[str, Any],
) -> dict[str, Any]:
    body = {
        "model": model,
        "input": messages,
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "schema": schema,
                "strict": True,
            }
        },
    }
    response = _request_json(api_key, body)
    raw_text = _response_output_text(response)
    if not raw_text:
        raise PipelineError("OpenAI returned no structured output text.")
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as error:
        raise PipelineError("OpenAI returned invalid JSON output.") from error
    if not isinstance(parsed, dict):
        raise PipelineError("OpenAI structured output must be a JSON object.")
    return parsed


def _request_json(api_key: str, body: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(body).encode("utf-8")
    request = Request(
        RESPONSES_API_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise PipelineError(
            f"OpenAI API request failed with HTTP {error.code}: {details}"
        ) from error
    except URLError as error:
        raise PipelineError(f"Could not reach OpenAI API: {error.reason}") from error


def _response_output_text(response: dict[str, Any]) -> str:
    output_text = response.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    output = response.get("output")
    if not isinstance(output, list):
        return ""

    text_chunks: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for chunk in content:
            if not isinstance(chunk, dict):
                continue
            text = chunk.get("text")
            if isinstance(text, str):
                text_chunks.append(text)
    return "".join(text_chunks).strip()


def _read_yaml_mapping(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    return loaded if isinstance(loaded, dict) else {}


def _mapping_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _resolve_project_path(value: str | Path | Any) -> Path:
    path = Path(str(value)).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (PROJECT_ROOT / path).resolve()


def _normalize_name(name: str) -> str:
    return " ".join(name.casefold().split())


def _filled(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip().casefold()
    return text not in {"", "null", "none", "nan", "[]"}


def _parse_jsonish_list(value: Any) -> list[str]:
    if not _filled(value):
        return []
    text = str(value).strip()
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if _filled(item)]
    return [text]
