from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

import httpx
import pandas as pd

from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import AppConfig, get_config
from trafficpulse.utils.cache import FileCache
from trafficpulse.utils.time import parse_datetime, to_utc
from trafficpulse.ingestion.tdx_auth import TdxTokenProvider, load_tdx_credentials


class TdxClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class ODataQuery:
    endpoint: str
    params: dict[str, Any]

    def cache_key(self) -> str:
        return json.dumps({"endpoint": self.endpoint, "params": self.params}, sort_keys=True)


def _isoformat_z(dt: datetime) -> str:
    utc = to_utc(dt)
    return utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_time_filter(field: str, start: datetime, end: datetime) -> str:
    start_text = _isoformat_z(start)
    end_text = _isoformat_z(end)
    return f"{field} ge {start_text} and {field} lt {end_text}"


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_by_path(record: dict[str, Any], path: str) -> Any:
    if not path:
        return None
    if "." not in path:
        return record.get(path)
    current: Any = record
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _coerce_datetime_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return to_utc(parse_datetime(str(value)))
    except Exception:
        return None


class TdxTrafficClient:
    def __init__(
        self,
        config: Optional[AppConfig] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self.config = (config or get_config()).resolve_paths()

        self._http = http_client or httpx.Client(
            base_url=self.config.tdx.base_url,
            timeout=self.config.tdx.request_timeout_seconds,
            headers={"accept": "application/json"},
        )

        client_id, client_secret = load_tdx_credentials()
        self._auth_http = httpx.Client(timeout=self.config.tdx.request_timeout_seconds)
        self._token_provider = TdxTokenProvider(
            token_url=self.config.tdx.token_url,
            client_id=client_id,
            client_secret=client_secret,
            http_client=self._auth_http,
            timeout_seconds=self.config.tdx.request_timeout_seconds,
        )

        self._cache = FileCache(
            directory=self.config.paths.cache_dir,
            ttl_seconds=self.config.cache.ttl_seconds,
            enabled=self.config.cache.enabled,
        )

    def close(self) -> None:
        self._http.close()
        self._auth_http.close()

    def _request_json(self, query: ODataQuery) -> list[dict[str, Any]]:
        cached = self._cache.get_json("tdx", query.cache_key())
        if isinstance(cached, list):
            return cached

        token = self._token_provider.get_access_token()
        headers = {"authorization": f"Bearer {token}"}

        max_retries = max(0, int(self.config.tdx.max_retries))
        backoff_seconds = float(self.config.tdx.retry_backoff_seconds)

        last_error: Optional[Exception] = None
        for attempt in range(max_retries + 1):
            try:
                response = self._http.get(query.endpoint, params=query.params, headers=headers)
                response.raise_for_status()
                payload = response.json()
                items = self._extract_items(payload)
                self._cache.set_json("tdx", query.cache_key(), items)
                return items
            except Exception as exc:  # noqa: BLE001 - surface detailed error after retries
                last_error = exc
                if attempt >= max_retries:
                    break
                time.sleep(backoff_seconds * (2**attempt))

        raise TdxClientError(f"TDX request failed after retries: {last_error}") from last_error

    @staticmethod
    def _extract_items(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("value"), list):
            return [item for item in payload["value"] if isinstance(item, dict)]
        raise TdxClientError("Unexpected TDX response shape; expected a list or an OData {value:[...]} object.")

    def fetch_vd_raw(
        self,
        start: datetime,
        end: datetime,
        cities: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        config = self.config.ingestion.vd
        selected_cities = cities or config.cities

        all_items: list[dict[str, Any]] = []
        for city in selected_cities:
            all_items.extend(self._fetch_vd_city_raw(city=city, start=start, end=end))
        return all_items

    def fetch_events_raw(
        self,
        start: datetime,
        end: datetime,
        cities: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        config = self.config.ingestion.events
        selected_cities = cities or config.cities

        all_items: list[dict[str, Any]] = []
        for city in selected_cities:
            all_items.extend(self._fetch_events_city_raw(city=city, start=start, end=end))
        return all_items

    def download_vd(
        self,
        start: datetime,
        end: datetime,
        cities: Optional[list[str]] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        config = self.config.ingestion.vd
        selected_cities = cities or config.cities

        segment_rows: list[dict[str, Any]] = []
        observation_rows: list[dict[str, Any]] = []

        for city in selected_cities:
            raw = self._fetch_vd_city_raw(city=city, start=start, end=end)
            segments_df, observations_df = self._normalize_vd_records(raw, city=city)
            if not segments_df.empty:
                segment_rows.extend(segments_df.to_dict(orient="records"))
            if not observations_df.empty:
                observation_rows.extend(observations_df.to_dict(orient="records"))

        segments = pd.DataFrame(segment_rows)
        if not segments.empty:
            def first_non_null(series: pd.Series) -> Any:
                non_null = series.dropna()
                return non_null.iloc[0] if not non_null.empty else None

            segments = segments.groupby("segment_id", as_index=False).agg(first_non_null)
            segments = segments.sort_values("segment_id")

        observations = pd.DataFrame(observation_rows)
        if not observations.empty:
            def normalize_timestamp(value: Any) -> Any:
                if value is None:
                    return pd.NaT
                try:
                    return to_utc(parse_datetime(str(value)))
                except Exception:
                    return pd.NaT

            observations["timestamp"] = observations["timestamp"].map(normalize_timestamp)
            observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
            observations = observations.dropna(subset=["timestamp", "segment_id"]).sort_values(
                ["segment_id", "timestamp"]
            )

        return segments, observations

    def download_events(
        self,
        start: datetime,
        end: datetime,
        cities: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        config = self.config.ingestion.events
        selected_cities = cities or config.cities

        rows: list[dict[str, Any]] = []
        for city in selected_cities:
            raw = self._fetch_events_city_raw(city=city, start=start, end=end)
            rows.extend(self._normalize_event_records(raw, city=city))

        events = pd.DataFrame(rows)
        if events.empty:
            return events

        events["start_time"] = pd.to_datetime(events["start_time"], errors="coerce", utc=True)
        if "end_time" in events.columns:
            events["end_time"] = pd.to_datetime(events["end_time"], errors="coerce", utc=True)
        events = events.dropna(subset=["event_id", "start_time"])

        def first_non_null(series: pd.Series) -> Any:
            non_null = series.dropna()
            return non_null.iloc[0] if not non_null.empty else None

        events = events.groupby("event_id", as_index=False).agg(first_non_null)
        events = events.sort_values(["start_time", "event_id"]).reset_index(drop=True)
        return events

    def _fetch_vd_city_raw(self, city: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        chunk_minutes = int(self.config.ingestion.query_chunk_minutes)
        if chunk_minutes <= 0:
            raise ValueError("ingestion.query_chunk_minutes must be > 0")

        results: list[dict[str, Any]] = []
        cursor = start
        while cursor < end:
            chunk_end = min(cursor + timedelta(minutes=chunk_minutes), end)
            results.extend(self._fetch_vd_city_chunk_raw(city=city, start=cursor, end=chunk_end))
            cursor = chunk_end
        return results

    def _fetch_vd_city_chunk_raw(
        self, city: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        config = self.config.ingestion.vd
        page_size = int(config.paging.page_size)
        if page_size <= 0:
            raise ValueError("ingestion.vd.paging.page_size must be > 0")

        filter_text = _build_time_filter(config.time_field, start=start, end=end)
        base_params: dict[str, Any] = {"$format": "JSON", "$filter": filter_text, "$top": page_size}

        last_error: Optional[Exception] = None
        for template in config.endpoint_templates:
            endpoint = template.format(city=city)
            try:
                return self._fetch_paginated(endpoint=endpoint, base_params=base_params, page_size=page_size)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue

        raise TdxClientError(f"All VD endpoint templates failed for city={city}: {last_error}") from last_error

    def _fetch_events_city_raw(self, city: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        chunk_minutes = int(self.config.ingestion.query_chunk_minutes)
        if chunk_minutes <= 0:
            raise ValueError("ingestion.query_chunk_minutes must be > 0")

        results: list[dict[str, Any]] = []
        cursor = start
        while cursor < end:
            chunk_end = min(cursor + timedelta(minutes=chunk_minutes), end)
            results.extend(self._fetch_events_city_chunk_raw(city=city, start=cursor, end=chunk_end))
            cursor = chunk_end
        return results

    def _fetch_events_city_chunk_raw(
        self, city: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        config = self.config.ingestion.events
        page_size = int(config.paging.page_size)
        if page_size <= 0:
            raise ValueError("ingestion.events.paging.page_size must be > 0")

        filter_text = _build_time_filter(config.start_time_field, start=start, end=end)
        base_params: dict[str, Any] = {"$format": "JSON", "$filter": filter_text, "$top": page_size}

        last_error: Optional[Exception] = None
        for template in config.endpoint_templates:
            endpoint = template.format(city=city)
            try:
                return self._fetch_paginated(endpoint=endpoint, base_params=base_params, page_size=page_size)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue

        raise TdxClientError(
            f"All event endpoint templates failed for city={city}: {last_error}"
        ) from last_error

    def _fetch_paginated(
        self, endpoint: str, base_params: dict[str, Any], page_size: int
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        skip = 0
        while True:
            params = dict(base_params)
            params["$skip"] = skip
            page = self._request_json(ODataQuery(endpoint=endpoint, params=params))
            items.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        return items

    def _normalize_event_records(self, records: list[dict[str, Any]], city: str) -> list[dict[str, Any]]:
        config = self.config.ingestion.events

        rows: list[dict[str, Any]] = []
        for record in records:
            event_id = _get_by_path(record, config.id_field)
            if event_id is None:
                continue

            start_time = _coerce_datetime_utc(_get_by_path(record, config.start_time_field))
            if start_time is None:
                continue

            end_time = _coerce_datetime_utc(_get_by_path(record, config.end_time_field))

            rows.append(
                {
                    "event_id": str(event_id),
                    "start_time": start_time,
                    "end_time": end_time,
                    "event_type": _get_by_path(record, config.type_field),
                    "description": _get_by_path(record, config.description_field),
                    "road_name": _get_by_path(record, config.road_name_field),
                    "direction": _get_by_path(record, config.direction_field),
                    "severity": _coerce_float(_get_by_path(record, config.severity_field)),
                    "lat": _coerce_float(_get_by_path(record, config.lat_field)),
                    "lon": _coerce_float(_get_by_path(record, config.lon_field)),
                    "city": city,
                    "source": "tdx",
                }
            )

        return rows

    def _normalize_vd_records(
        self, records: list[dict[str, Any]], city: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        config = self.config.ingestion.vd

        segment_rows: list[dict[str, Any]] = []
        observation_rows: list[dict[str, Any]] = []

        for record in records:
            segment_id = record.get(config.segment_id_field)
            if segment_id is None:
                continue
            segment_id = str(segment_id)

            segment_rows.append(self._extract_vd_segment_metadata(record, city=city, segment_id=segment_id))

            timestamp = record.get(config.time_field)
            if timestamp is None:
                continue

            speed_kph, volume, occupancy = self._extract_vd_observation_values(record)
            observation_rows.append(
                {
                    "timestamp": timestamp,
                    "segment_id": segment_id,
                    "speed_kph": speed_kph,
                    "volume": volume,
                    "occupancy_pct": occupancy,
                }
            )

        segments = pd.DataFrame(segment_rows)
        observations = pd.DataFrame(observation_rows)
        return segments, observations

    def _extract_vd_segment_metadata(
        self, record: dict[str, Any], city: str, segment_id: str
    ) -> dict[str, Any]:
        fields = self.config.ingestion.vd.metadata_fields
        return {
            "segment_id": segment_id,
            "city": city,
            "name": record.get(fields.name_field),
            "direction": record.get(fields.direction_field),
            "road_name": record.get(fields.road_name_field),
            "link_id": record.get(fields.link_id_field),
            "lat": _coerce_float(record.get(fields.lat_field)),
            "lon": _coerce_float(record.get(fields.lon_field)),
        }

    def _extract_vd_observation_values(
        self, record: dict[str, Any]
    ) -> tuple[Optional[float], Optional[float], Optional[float]]:
        config = self.config.ingestion.vd

        lane_list = record.get(config.lane_list_field)
        if isinstance(lane_list, list) and lane_list:
            return self._aggregate_lanes(lane_list)

        speed = _coerce_float(record.get(config.lane_speed_field))
        volume = _coerce_float(record.get(config.lane_volume_field))
        occupancy = _coerce_float(record.get(config.lane_occupancy_field))
        return speed, volume, occupancy

    def _aggregate_lanes(
        self, lanes: list[dict[str, Any]]
    ) -> tuple[Optional[float], Optional[float], Optional[float]]:
        config = self.config.ingestion.vd

        lane_speeds: list[float] = []
        lane_volumes: list[float] = []
        lane_occupancies: list[float] = []

        weighted_speed_sum = 0.0
        weighted_volume_sum = 0.0

        for lane in lanes:
            if not isinstance(lane, dict):
                continue
            speed = _coerce_float(lane.get(config.lane_speed_field))
            volume = _coerce_float(lane.get(config.lane_volume_field))
            occupancy = _coerce_float(lane.get(config.lane_occupancy_field))

            if speed is not None:
                lane_speeds.append(speed)
            if volume is not None:
                lane_volumes.append(volume)
            if speed is not None and volume is not None and volume > 0:
                weighted_speed_sum += speed * volume
                weighted_volume_sum += volume
            if occupancy is not None:
                lane_occupancies.append(occupancy)

        speed_kph = None
        if lane_speeds:
            if config.lane_speed_aggregation == "volume_weighted_mean" and weighted_volume_sum > 0:
                speed_kph = weighted_speed_sum / weighted_volume_sum
            else:
                speed_kph = sum(lane_speeds) / len(lane_speeds)

        volume_value = None
        if lane_volumes:
            if config.lane_volume_aggregation == "sum":
                volume_value = sum(v for v in lane_volumes if v is not None)
            elif config.lane_volume_aggregation == "mean":
                volume_value = sum(v for v in lane_volumes if v is not None) / len(lane_volumes)
            else:
                volume_value = sum(v for v in lane_volumes if v is not None)

        occupancy_value = None
        if lane_occupancies:
            if config.lane_occupancy_aggregation == "mean":
                occupancy_value = sum(lane_occupancies) / len(lane_occupancies)
            elif config.lane_occupancy_aggregation == "sum":
                occupancy_value = sum(lane_occupancies)
            else:
                occupancy_value = sum(lane_occupancies) / len(lane_occupancies)

        return speed_kph, volume_value, occupancy_value


def build_vd_dataset(
    start: datetime, end: datetime, cities: Optional[list[str]] = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    configure_logging()
    client = TdxTrafficClient()
    try:
        return client.download_vd(start=start, end=end, cities=cities)
    finally:
        client.close()


def build_events_dataset(
    start: datetime, end: datetime, cities: Optional[list[str]] = None
) -> pd.DataFrame:
    configure_logging()
    client = TdxTrafficClient()
    try:
        return client.download_events(start=start, end=end, cities=cities)
    finally:
        client.close()
