from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


TRIAGE_SYSTEM_PROMPT = (
    "You are a healthcare routing agent. Your job is to listen to a patient's "
    "symptoms or care request and choose the single best doctor category from "
    "the allowed list. You are not diagnosing disease. If the message is not "
    "about human or animal health, mark it as not medical. If symptoms suggest "
    "immediate danger, route to Emergency Medicine."
)

ALLOWED_DOCTOR_CATEGORIES = (
    "Primary Care / General Practice",
    "Internal Medicine",
    "Pediatrics",
    "Gynecology",
    "Dermatology",
    "Cardiology",
    "Orthopedics",
    "Neurology",
    "Psychiatry / Psychotherapy",
    "ENT",
    "Ophthalmology",
    "Urology",
    "Gastroenterology",
    "Endocrinology",
    "Rheumatology",
    "Pulmonology",
    "Oncology",
    "Dentistry",
    "Orthodontics",
    "Oral Surgery",
    "Emergency Medicine",
    "Surgery",
    "Radiology",
    "Anesthesiology",
    "Intensive Care",
    "Pathology / Laboratory Medicine",
    "Physiotherapy",
    "Occupational Therapy",
    "Nutrition Counseling",
    "Midwifery",
    "Alternative Medicine",
    "Pharmacy",
    "Veterinary Medicine",
)


@dataclass(frozen=True)
class LlmConfig:
    api_key: str
    model: str
    base_url: str
    max_tokens: int
    temperature: float

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key and self.model and self.base_url)


@dataclass(frozen=True)
class DoctorCategoryDecision:
    is_medical_request: bool
    category: str | None
    confidence_score: float
    reason: str
    urgency: str
    patient_message: str
    latitude: float | None
    longitude: float | None
    location_text: str | None

    @property
    def has_location(self) -> bool:
        return self.latitude is not None and self.longitude is not None


def load_llm_config() -> LlmConfig:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    model = (os.environ.get("OPENAI_MODEL") or "").strip()
    base_url = (os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip()

    return LlmConfig(
        api_key=api_key,
        model=model,
        base_url=base_url,
        max_tokens=_env_int("LLM_MAX_TOKENS", 300),
        temperature=_env_float("LLM_TEMPERATURE", 0.2),
    )


def is_llm_configured() -> bool:
    return load_llm_config().is_configured


def choose_doctor_category(query: str) -> DoctorCategoryDecision:
    config = load_llm_config()
    if not config.is_configured:
        raise RuntimeError(
            "LLM is not configured. Set OPENAI_API_KEY and OPENAI_MODEL."
        )

    payload = {
        "messages": [
            {"role": "system", "content": TRIAGE_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Allowed doctor categories:\n"
                    f"{json.dumps(ALLOWED_DOCTOR_CATEGORIES)}\n\n"
                    "Patient message:\n"
                    f"{query}\n\n"
                    "Return JSON only with these fields: "
                    "is_medical_request boolean, category string or null, "
                    "confidence_score number from 0 to 1, reason string, "
                    "urgency one of routine/soon/urgent/emergency, "
                    "patient_message string, location object. The location object "
                    "must contain latitude number or null, longitude number or null, "
                    "and text string or null. Extract latitude and longitude when "
                    "the user provides coordinates. If the user gives an address, "
                    "landmark, city, or place name, put that full location string "
                    "in location.text and keep latitude/longitude null. "
                    "The category must exactly match one allowed category when "
                    "is_medical_request is true."
                ),
            },
        ],
        "max_completion_tokens": min(config.max_tokens, 500),
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    response_payload = _post_json(
        _chat_completions_url(config.base_url),
        config.api_key,
        {"model": config.model, **payload},
    )
    content = _extract_message_content(response_payload)
    return _parse_doctor_category_decision(content)


def _post_json(url: str, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=30) as response:
            loaded = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"LLM request failed with HTTP {error.code}: {detail}") from error
    except URLError as error:
        raise RuntimeError(f"LLM request failed: {error.reason}") from error

    if not isinstance(loaded, dict):
        raise RuntimeError("LLM response was not a JSON object")
    return loaded


def _chat_completions_url(base_url: str) -> str:
    normalized_url = base_url.rstrip("/")
    if normalized_url.endswith("/chat/completions"):
        return normalized_url
    return f"{normalized_url}/chat/completions"


def _extract_message_content(payload: dict[str, Any]) -> str:
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        if isinstance(first_choice, dict):
            message = first_choice.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()
                if isinstance(content, list):
                    text_parts = [
                        item.get("text", "")
                        for item in content
                        if isinstance(item, dict) and isinstance(item.get("text"), str)
                    ]
                    combined = "\n".join(part for part in text_parts if part).strip()
                    if combined:
                        return combined
            text = first_choice.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()

    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    raise RuntimeError("LLM response did not include assistant text")


def _parse_doctor_category_decision(content: str) -> DoctorCategoryDecision:
    try:
        loaded = json.loads(content)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"LLM triage response was not valid JSON: {content}") from error

    if not isinstance(loaded, dict):
        raise RuntimeError("LLM triage response was not a JSON object")

    is_medical_request = bool(loaded.get("is_medical_request"))
    raw_category = loaded.get("category")
    category = raw_category if isinstance(raw_category, str) else None
    if is_medical_request and category not in ALLOWED_DOCTOR_CATEGORIES:
        raise RuntimeError(f"LLM selected unsupported doctor category: {category}")

    confidence_score = loaded.get("confidence_score", 0)
    try:
        confidence_value = max(0.0, min(1.0, float(confidence_score)))
    except (TypeError, ValueError):
        confidence_value = 0.0

    reason = loaded.get("reason")
    urgency = loaded.get("urgency")
    patient_message = loaded.get("patient_message")
    location = loaded.get("location")
    location_data = location if isinstance(location, dict) else {}
    latitude = _coerce_coordinate(location_data.get("latitude"), -90, 90)
    longitude = _coerce_coordinate(location_data.get("longitude"), -180, 180)
    location_text = location_data.get("text")
    return DoctorCategoryDecision(
        is_medical_request=is_medical_request,
        category=category,
        confidence_score=round(confidence_value, 3),
        reason=reason if isinstance(reason, str) else "No reason provided.",
        urgency=urgency if urgency in {"routine", "soon", "urgent", "emergency"} else "routine",
        patient_message=(
            patient_message
            if isinstance(patient_message, str)
            else "Please describe the patient's symptoms or care request."
        ),
        latitude=latitude,
        longitude=longitude,
        location_text=location_text if isinstance(location_text, str) else None,
    )


def _coerce_coordinate(value: object, minimum: float, maximum: float) -> float | None:
    try:
        coordinate = float(value)
    except (TypeError, ValueError):
        return None
    if not minimum <= coordinate <= maximum:
        return None
    return coordinate


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default
