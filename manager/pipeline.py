from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = PROJECT_ROOT / "data" / "template" / "template.json"
USER_DIR = PROJECT_ROOT / "data" / "user"


def write_user_timestamp(payload: dict[str, Any]) -> Path:
    template = _read_json(TEMPLATE_PATH)
    user_record = _empty_record_from_template(template)

    specified_values = {
        "longitude": payload.get("longitude"),
        "latitude": payload.get("latitude"),
        "type": payload.get("doctor_type"),
        "diagnosis": payload.get("diagnosis"),
        "description": payload.get("description"),
    }

    for key, value in specified_values.items():
        if key in user_record and value not in (None, ""):
            user_record[key] = value

    USER_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S_%f")
    output_path = USER_DIR / f"user_{timestamp}.json"
    output_path.write_text(
        json.dumps(user_record, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return output_path


def _read_json(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return loaded


def _empty_record_from_template(template: dict[str, Any]) -> dict[str, Any]:
    return {key: _empty_value(value) for key, value in template.items()}


def _empty_value(value: Any) -> Any:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return None
    if isinstance(value, str):
        return ""
    if isinstance(value, list):
        return []
    if isinstance(value, dict):
        return {key: _empty_value(nested_value) for key, nested_value in value.items()}
    return None
