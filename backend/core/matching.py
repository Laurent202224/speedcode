from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

try:
    from sklearn.neighbors import BallTree
except ModuleNotFoundError:
    BallTree = None


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "config.yaml"
DEFAULT_DATA_RELATIVE_PATH = Path("data") / "dataset.json"
DEFAULT_FULL_DATA_RELATIVE_PATH = Path("data") / "data_source" / "data_full.csv"
DEFAULT_TEMPLATE_RELATIVE_PATH = Path("data") / "template" / "template.json"
DEFAULT_TRUST_SCORE_KM_EQUIVALENT = 0.0

DESCRIPTION_FIELD = "description"
LATITUDE_FIELD = "latitude"
LONGITUDE_FIELD = "longitude"
DISTANCE_FIELD = "distance_km"
TRUSTWORTHY_SCORE_FIELD = "trustworthy_score"
TRUST_ADJUSTED_DISTANCE_FIELD = "trust_adjusted_distance_km"
TOKEN_RE = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class HospitalRecord:
    id: int
    data: Mapping[str, Any]
    latitude: float | None
    longitude: float | None


@dataclass(frozen=True)
class MatchingConfig:
    data_path: Path
    template_path: Path
    trust_score_km_equivalent: float


@dataclass(frozen=True)
class _Point:
    latitude: float
    longitude: float
    record_id: int


class SpatialPointIndex:
    """Library-backed spatial index over latitude/longitude points."""

    EARTH_RADIUS_KM = 6371.0088

    def __init__(self, points: Iterable[_Point]) -> None:
        self._points = tuple(points)
        self._points_by_id = {point.record_id: point for point in self._points}
        self._record_ids = tuple(point.record_id for point in self._points)
        self._coordinates = [
            _coordinate_radians(point.latitude, point.longitude)
            for point in self._points
        ]
        self._tree = (
            BallTree(self._coordinates, metric="haversine")
            if self._coordinates and BallTree is not None
            else None
        )
        self.record_ids = frozenset(self._points_by_id)

    def ids_within_radius(
        self, latitude: float, longitude: float, radius_km: float
    ) -> set[int]:
        if radius_km < 0:
            raise ValueError("radius_km must be greater than or equal to 0")

        if self._tree is None:
            return {
                point.record_id
                for point in self._points
                if _haversine_km(latitude, longitude, point.latitude, point.longitude)
                <= radius_km
            }

        indexes = self._tree.query_radius(
            [_coordinate_radians(latitude, longitude)],
            r=radius_km / self.EARTH_RADIUS_KM,
        )[0]
        return {self._record_ids[index] for index in indexes}

    def distance_km(self, record_id: int, latitude: float, longitude: float) -> float:
        point = self._point_by_id(record_id)
        return _haversine_km(latitude, longitude, point.latitude, point.longitude)

    def nearest_ids(self, latitude: float, longitude: float, limit: int) -> list[int]:
        if limit <= 0:
            return []

        if self._tree is None:
            ranked_points = sorted(
                self._points,
                key=lambda point: (
                    _haversine_km(latitude, longitude, point.latitude, point.longitude),
                    point.record_id,
                ),
            )
            return [point.record_id for point in ranked_points[:limit]]

        k = min(limit, len(self._record_ids))
        _, indexes = self._tree.query([_coordinate_radians(latitude, longitude)], k=k)
        return [self._record_ids[index] for index in indexes[0]]

    def _point_by_id(self, record_id: int) -> _Point:
        return self._points_by_id[record_id]


class HospitalIndex:
    """Indexes hospitals by template fields for fast structured lookup."""

    def __init__(
        self,
        records: Iterable[HospitalRecord],
        searchable_fields: Iterable[str],
        trust_score_km_equivalent: float = DEFAULT_TRUST_SCORE_KM_EQUIVALENT,
    ) -> None:
        self.records = {record.id: record for record in records}
        self.searchable_fields = tuple(
            field for field in searchable_fields if field != DESCRIPTION_FIELD
        )
        self.trust_score_km_equivalent = trust_score_km_equivalent
        self._exact_indexes: dict[str, dict[str, set[int]]] = {
            field: defaultdict(set)
            for field in self.searchable_fields
            if field not in {LATITUDE_FIELD, LONGITUDE_FIELD}
        }
        self._token_indexes: dict[str, dict[str, set[int]]] = {
            field: defaultdict(set)
            for field in self.searchable_fields
            if field not in {LATITUDE_FIELD, LONGITUDE_FIELD}
        }

        points: list[_Point] = []
        for record in self.records.values():
            self._index_record(record)
            if record.latitude is not None and record.longitude is not None:
                points.append(_Point(record.latitude, record.longitude, record.id))

        self.spatial_index = SpatialPointIndex(points)

    @classmethod
    def from_dataset(
        cls,
        data_path: str | Path | None = None,
        template_path: str | Path | None = None,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
    ) -> "HospitalIndex":
        config = (
            load_matching_config(config_path) if data_path is None or template_path is None else None
        )
        if data_path is None:
            assert config is not None
            resolved_data_path = config.data_path
        else:
            resolved_data_path = _resolve_project_path(data_path)

        if template_path is None:
            assert config is not None
            resolved_template_path = config.template_path
        else:
            resolved_template_path = _resolve_project_path(template_path)

        searchable_fields = load_searchable_fields(resolved_template_path)
        records = load_records(resolved_data_path)
        trust_score_km_equivalent = (
            config.trust_score_km_equivalent
            if config is not None
            else DEFAULT_TRUST_SCORE_KM_EQUIVALENT
        )
        return cls(records, searchable_fields, trust_score_km_equivalent)

    @classmethod
    def from_config(
        cls,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        *,
        data_path: str | Path | None = None,
        template_path: str | Path | None = None,
    ) -> "HospitalIndex":
        config = load_matching_config(config_path)
        resolved_data_path = (
            _resolve_project_path(data_path)
            if data_path is not None
            else config.data_path
        )
        resolved_template_path = (
            _resolve_project_path(template_path)
            if template_path is not None
            else config.template_path
        )
        return cls.from_dataset(
            resolved_data_path,
            resolved_template_path,
            config_path,
        )

    def search(
        self,
        query: Mapping[str, Any],
        *,
        limit: int | None = None,
        radius_km: float | None = None,
    ) -> list[dict[str, Any]]:
        """Return hospitals matching all provided indexed criteria.

        `latitude` and `longitude` must be supplied together. With `radius_km`,
        they filter to hospitals inside that circle. Without it, results are
        sorted by distance to the point.
        """

        query = dict(query)
        query_limit = query.pop("limit", None)
        query_radius = query.pop("radius_km", query.pop("max_distance_km", None))
        limit = _coerce_limit(limit if limit is not None else query_limit)
        radius_km = _coerce_optional_float_arg(
            radius_km if radius_km is not None else query_radius,
            "radius_km",
        )

        latitude = query.pop(LATITUDE_FIELD, None)
        longitude = query.pop(LONGITUDE_FIELD, None)
        latitude_value = _coerce_optional_float_arg(latitude, LATITUDE_FIELD)
        longitude_value = _coerce_optional_float_arg(longitude, LONGITUDE_FIELD)
        has_spatial_query = latitude is not None or longitude is not None

        if has_spatial_query and (latitude_value is None or longitude_value is None):
            raise ValueError(
                "latitude and longitude must be valid numbers and supplied together"
            )
        if has_spatial_query:
            _validate_coordinates(latitude_value, longitude_value)
        if radius_km is not None and not has_spatial_query:
            raise ValueError("radius_km requires latitude and longitude")
        if limit == 0:
            return []

        candidate_ids: set[int] | None = None
        for field, value in query.items():
            if field == DESCRIPTION_FIELD:
                continue
            self._validate_query_field(field)
            field_matches = self._match_field(field, value)
            candidate_ids = (
                field_matches
                if candidate_ids is None
                else candidate_ids.intersection(field_matches)
            )

        has_indexed_filters = candidate_ids is not None
        if candidate_ids is None:
            candidate_ids = set(self.records)

        if has_spatial_query:
            coordinate_ids = set(self.spatial_index.record_ids)
            candidate_ids.intersection_update(coordinate_ids)
            if radius_km is not None:
                candidate_ids.intersection_update(
                    self.spatial_index.ids_within_radius(
                        latitude_value,
                        longitude_value,
                        radius_km,
                    )
                )

        ranked_ids = self._rank_records(
            candidate_ids, latitude_value, longitude_value
        )
        if limit is not None:
            ranked_ids = ranked_ids[:limit]

        return [
            self._serialize_record(record_id, latitude_value, longitude_value)
            for record_id in ranked_ids
        ]

    def _index_record(self, record: HospitalRecord) -> None:
        for field in self._exact_indexes:
            for value in self._field_values(record.data, field):
                for normalized in _normalized_values(value):
                    self._exact_indexes[field][normalized].add(record.id)
                for token in _tokens(value):
                    self._token_indexes[field][token].add(record.id)

    def _match_field(self, field: str, value: Any) -> set[int]:
        if isinstance(value, (list, tuple, set)):
            matches: set[int] = set()
            for item in value:
                matches.update(self._match_field(field, item))
            return matches

        direct_matches: set[int] = set()
        for normalized in _normalized_values(value):
            direct_matches.update(self._exact_indexes[field].get(normalized, set()))
        if direct_matches:
            return direct_matches

        token_matches: set[int] | None = None
        for token in _tokens(value):
            matches = self._token_indexes[field].get(token, set())
            token_matches = (
                set(matches)
                if token_matches is None
                else token_matches.intersection(matches)
            )
        return token_matches or set()

    def _field_values(self, data: Mapping[str, Any], field: str) -> Iterable[Any]:
        value = data.get(field)
        if not _is_empty(value):
            yield value

    def _rank_records(
        self,
        record_ids: Iterable[int],
        latitude: float | None,
        longitude: float | None,
    ) -> list[int]:
        if latitude is not None and longitude is not None:
            return sorted(
                record_ids,
                key=lambda record_id: (
                    self._trust_adjusted_distance_km(record_id, latitude, longitude),
                    self.spatial_index.distance_km(record_id, latitude, longitude),
                    -self._trustworthy_score(record_id),
                    self.records[record_id].data.get("name", ""),
                ),
            )

        return sorted(
            record_ids,
            key=lambda record_id: (
                -self._trustworthy_score(record_id),
                self.records[record_id].data.get("name", ""),
            ),
        )

    def _serialize_record(
        self,
        record_id: int,
        latitude: float | None,
        longitude: float | None,
    ) -> dict[str, Any]:
        record = self.records[record_id]
        result: dict[str, Any] = dict(record.data)
        trustworthy_score = self._trustworthy_score(record_id)
        result[TRUSTWORTHY_SCORE_FIELD] = trustworthy_score
        if latitude is not None and longitude is not None:
            distance_km = self.spatial_index.distance_km(record_id, latitude, longitude)
            result[DISTANCE_FIELD] = round(distance_km, 3)
            result[TRUST_ADJUSTED_DISTANCE_FIELD] = round(
                self._trust_adjusted_distance_km(record_id, latitude, longitude), 3
            )
        return result

    def _trustworthy_score(self, record_id: int) -> float:
        value = self.records[record_id].data.get(TRUSTWORTHY_SCORE_FIELD)
        score = _coerce_optional_float(value)
        return score if score is not None else 0.0

    def _trust_adjusted_distance_km(
        self, record_id: int, latitude: float, longitude: float
    ) -> float:
        distance_km = self.spatial_index.distance_km(record_id, latitude, longitude)
        trust_bonus_km = self._trustworthy_score(record_id) * self.trust_score_km_equivalent
        return distance_km - trust_bonus_km

    def _validate_query_field(self, field: str) -> None:
        if field not in self.searchable_fields:
            available = ", ".join(self.searchable_fields)
            raise ValueError(
                f"Unsupported query field {field!r}. Use one of: {available}"
            )
        if field not in self._exact_indexes:
            raise ValueError(
                f"{field!r} is a spatial field. Query latitude and longitude together."
            )


def find_hospitals(
    query: Mapping[str, Any],
    *,
    limit: int | None = None,
    radius_km: float | None = None,
    data_path: str | Path | None = None,
    template_path: str | Path | None = None,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> list[dict[str, Any]]:
    """Convenience wrapper for one-off searches.

    For repeated queries, build the index once with `load_hospital_index()` and
    call `index.search(...)`.
    """

    index = load_hospital_index(data_path, template_path, config_path)
    return index.search(query, limit=limit, radius_km=radius_km)


def recommend_hospitals_for_diagnosis(
    diagnosis: str,
    latitude: float,
    longitude: float,
    *,
    limit: int = 3,
    radius_km: float | None = None,
    data_path: str | Path | None = None,
    template_path: str | Path | None = None,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> list[dict[str, Any]]:
    return find_hospitals(
        {
            "diagnosis": diagnosis,
            LATITUDE_FIELD: latitude,
            LONGITUDE_FIELD: longitude,
        },
        limit=limit,
        radius_km=radius_km,
        data_path=data_path,
        template_path=template_path,
        config_path=config_path,
    )


def enrich_hospitals_from_full_csv(
    hospitals: list[dict[str, Any]],
    *,
    data_path: str | Path = DEFAULT_FULL_DATA_RELATIVE_PATH,
) -> list[dict[str, Any]]:
    full_records = _load_full_records_by_name(_resolve_project_path(data_path))
    enriched_hospitals: list[dict[str, Any]] = []
    for hospital in hospitals:
        enriched = dict(hospital)
        name = str(hospital.get("name", "")).strip().casefold()
        full_record = full_records.get(name)
        if full_record:
            for key, value in full_record.items():
                if key not in enriched or _is_empty(enriched.get(key)):
                    enriched[key] = value
            enriched["address"] = _format_address(full_record)
        enriched_hospitals.append(enriched)
    return enriched_hospitals


def load_hospital_index(
    data_path: str | Path | None = None,
    template_path: str | Path | None = None,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> HospitalIndex:
    config = load_matching_config(config_path)
    resolved_data_path = (
        _resolve_project_path(data_path)
        if data_path is not None
        else config.data_path
    )
    resolved_template_path = (
        _resolve_project_path(template_path)
        if template_path is not None
        else config.template_path
    )
    return _load_hospital_index(
        str(resolved_data_path),
        str(resolved_template_path),
        config.trust_score_km_equivalent,
    )


@lru_cache(maxsize=8)
def _load_hospital_index(
    data_path: str,
    template_path: str,
    trust_score_km_equivalent: float,
) -> HospitalIndex:
    searchable_fields = load_searchable_fields(template_path)
    records = load_records(data_path)
    return HospitalIndex(
        records,
        searchable_fields,
        trust_score_km_equivalent,
    )


def load_matching_config(
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> MatchingConfig:
    config_path = _resolve_project_path(config_path)
    data: Mapping[str, Any] = {}
    if config_path.exists():
        data = _read_config_mapping(config_path)

    paths = _mapping_value(data.get("paths"))
    matching = _mapping_value(data.get("matching"))
    data_path = (
        data.get("data_path")
        or paths.get("data_path")
        or paths.get("data")
        or DEFAULT_DATA_RELATIVE_PATH
    )
    template_path = (
        data.get("template_path")
        or paths.get("template_json")
        or paths.get("template_path")
        or paths.get("template")
        or DEFAULT_TEMPLATE_RELATIVE_PATH
    )

    return MatchingConfig(
        data_path=_resolve_project_path(data_path),
        template_path=_resolve_project_path(template_path),
        trust_score_km_equivalent=_coerce_trust_score_km_equivalent(
            matching.get("trust_score_km_equivalent")
        ),
    )


def _read_config_mapping(config_path: Path) -> Mapping[str, Any]:
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    if not isinstance(loaded, MappingABC):
        raise ValueError(f"Config file must contain a mapping: {config_path}")
    return loaded


def _mapping_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, MappingABC) else {}


def _resolve_project_path(value: str | Path | Any) -> Path:
    path = Path(str(value)).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (PROJECT_ROOT / path).resolve()


def load_records(data_path: str | Path) -> list[HospitalRecord]:
    path = Path(data_path)
    if path.suffix.casefold() != ".json":
        raise ValueError(f"Matching expects a JSON dataset: {data_path}")
    return _load_json_records(path)


@lru_cache(maxsize=4)
def _load_full_records_by_name(data_path: Path) -> dict[str, dict[str, Any]]:
    with data_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        records: dict[str, dict[str, Any]] = {}
        for row in reader:
            name = str(row.get("name", "")).strip()
            if name:
                records[name.casefold()] = row
        return records


def _format_address(record: Mapping[str, Any]) -> str | None:
    parts = [
        record.get("address_line1"),
        record.get("address_line2"),
        record.get("address_line3"),
        record.get("address_city"),
        record.get("address_stateOrRegion"),
        record.get("address_zipOrPostcode"),
        record.get("address_country"),
    ]
    address = ", ".join(
        str(part).strip()
        for part in parts
        if not _is_empty(part) and str(part).strip().casefold() != "null"
    )
    return address or None


def _load_json_records(data_path: Path) -> list[HospitalRecord]:
    with data_path.open(encoding="utf-8") as json_file:
        loaded = json.load(json_file)

    if isinstance(loaded, MappingABC):
        raw_records = loaded.get("records", [])
    else:
        raw_records = loaded

    if not isinstance(raw_records, list):
        raise ValueError(f"Dataset JSON must be a list of records: {data_path}")

    records: list[HospitalRecord] = []
    for row_id, row in enumerate(raw_records):
        if not isinstance(row, MappingABC):
            continue
        record_data = {str(key): value for key, value in row.items()}
        records.append(
            HospitalRecord(
                id=row_id,
                data=record_data,
                latitude=_coerce_optional_float(record_data.get(LATITUDE_FIELD)),
                longitude=_coerce_optional_float(record_data.get(LONGITUDE_FIELD)),
            )
        )
    return records


def load_searchable_fields(template_path: str | Path) -> tuple[str, ...]:
    with Path(template_path).open(encoding="utf-8") as template_file:
        template = json.load(template_file)
    return tuple(field for field in template if field != DESCRIPTION_FIELD)


def _normalized_values(value: Any) -> Iterable[str]:
    parsed = _parse_collection(value)
    values = parsed if isinstance(parsed, list) else [parsed]
    for item in values:
        if _is_empty(item):
            continue
        yield str(item).strip().casefold()


def _tokens(value: Any) -> Iterable[str]:
    parsed = _parse_collection(value)
    values = parsed if isinstance(parsed, list) else [parsed]
    for item in values:
        if _is_empty(item):
            continue
        yield from TOKEN_RE.findall(str(item).casefold())


def _parse_collection(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    stripped = value.strip()
    if not stripped.startswith("["):
        return value

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return value

    return parsed if isinstance(parsed, list) else value


def _coerce_optional_float(value: Any) -> float | None:
    if _is_empty(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_trust_score_km_equivalent(value: Any) -> float:
    result = _coerce_optional_float(value)
    if result is None:
        return DEFAULT_TRUST_SCORE_KM_EQUIVALENT
    if result < 0:
        raise ValueError("trust_score_km_equivalent must be greater than or equal to 0")
    return result


def _coerce_optional_float_arg(value: Any, name: str) -> float | None:
    if _is_empty(value):
        return None
    result = _coerce_optional_float(value)
    if result is None:
        raise ValueError(f"{name} must be a number")
    return result


def _coerce_limit(value: Any) -> int | None:
    if _is_empty(value):
        return None
    limit = int(value)
    if limit < 0:
        raise ValueError("limit must be greater than or equal to 0")
    return limit


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == "" or value.strip().casefold() == "null"
    return False


def _validate_coordinates(latitude: float, longitude: float) -> None:
    if not -90 <= latitude <= 90:
        raise ValueError("latitude must be between -90 and 90")
    if not -180 <= longitude <= 180:
        raise ValueError("longitude must be between -180 and 180")


def _coordinate_radians(latitude: float, longitude: float) -> tuple[float, float]:
    return math.radians(latitude), math.radians(longitude)


def _haversine_km(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    radius_earth_km = 6371.0088
    lat_a = math.radians(latitude_a)
    lat_b = math.radians(latitude_b)
    lat_delta = math.radians(latitude_b - latitude_a)
    lon_delta = math.radians(longitude_b - longitude_a)

    haversine = (
        math.sin(lat_delta / 2) ** 2
        + math.cos(lat_a) * math.cos(lat_b) * math.sin(lon_delta / 2) ** 2
    )
    return 2 * radius_earth_km * math.asin(math.sqrt(haversine))
