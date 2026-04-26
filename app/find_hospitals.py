from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.core.diagnosis import available_diagnosis_names, classify_diagnosis
from backend.core.matching import recommend_hospitals_for_diagnosis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find the closest hospitals for a diagnosis and location."
    )
    parser.add_argument("--diagnosis", help="Diagnosis or symptom description in English.")
    parser.add_argument("--latitude", type=float, help="Latitude of the patient location.")
    parser.add_argument(
        "--longitude", type=float, help="Longitude of the patient location."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of hospitals to return. Defaults to 10.",
    )
    parser.add_argument(
        "--radius-km",
        type=float,
        default=None,
        help="Optional search radius in kilometers.",
    )
    parser.add_argument(
        "--exact-category",
        action="store_true",
        help="Treat --diagnosis as an exact supported category and skip classification.",
    )
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="Print the supported diagnosis categories and exit.",
    )
    return parser.parse_args()


def prompt_if_missing(value: str | None, label: str) -> str:
    if value is not None and value.strip():
        return value.strip()
    return input(f"{label}: ").strip()


def prompt_float_if_missing(value: float | None, label: str) -> float:
    if value is not None:
        return value

    while True:
        raw_value = input(f"{label}: ").strip()
        try:
            return float(raw_value)
        except ValueError:
            print(f"Please enter a valid number for {label.lower()}.")


def resolve_category(diagnosis_input: str, exact_category: bool) -> tuple[str, str]:
    if exact_category:
        return diagnosis_input, "used exact category"

    diagnosis_match = classify_diagnosis(diagnosis_input)
    return diagnosis_match.english_name, (
        f"classified as '{diagnosis_match.english_name}' "
        f"(score {diagnosis_match.score}, {diagnosis_match.reason})"
    )


def print_results(
    diagnosis_input: str,
    category: str,
    resolution_note: str,
    latitude: float,
    longitude: float,
    hospitals: list[dict[str, object]],
) -> None:
    print()
    print(f"Diagnosis input: {diagnosis_input}")
    print(f"Matched category: {category}")
    print(f"Resolution: {resolution_note}")
    print(f"Location: {latitude}, {longitude}")
    print()

    if not hospitals:
        print("No hospitals found for that diagnosis and location.")
        return

    print(f"Top {len(hospitals)} closest hospitals:")
    for index, hospital in enumerate(hospitals, start=1):
        name = hospital.get("name", "Unknown")
        facility_type = hospital.get("type", "Unknown")
        diagnosis = hospital.get("diagnosis", "Unknown")
        distance = hospital.get("distance_km")
        trust_score = hospital.get("trustworthy_score")
        trust_adjusted_distance = hospital.get("trust_adjusted_distance_km")
        description = str(hospital.get("description", "") or "").strip()

        details = [f"{index}. {name}", str(facility_type), str(diagnosis)]
        if isinstance(distance, (int, float)):
            details.append(f"distance={distance:.3f} km")
        if isinstance(trust_score, (int, float)):
            details.append(f"trust={trust_score:.2f}")
        if isinstance(trust_adjusted_distance, (int, float)):
            details.append(f"adjusted={trust_adjusted_distance:.3f} km")
        print(" | ".join(details))
        if description and description.casefold() != "null":
            print(f"   {description}")


def main() -> None:
    args = parse_args()

    if args.list_categories:
        for category in available_diagnosis_names():
            print(category)
        return

    diagnosis_input = prompt_if_missing(args.diagnosis, "Diagnosis")
    latitude = prompt_float_if_missing(args.latitude, "Latitude")
    longitude = prompt_float_if_missing(args.longitude, "Longitude")

    category, resolution_note = resolve_category(
        diagnosis_input, args.exact_category
    )
    hospitals = recommend_hospitals_for_diagnosis(
        category,
        latitude,
        longitude,
        limit=args.limit,
        radius_km=args.radius_km,
    )
    print_results(
        diagnosis_input,
        category,
        resolution_note,
        latitude,
        longitude,
        hospitals,
    )


if __name__ == "__main__":
    main()
