"""TDX traffic ingestion client.

This module fetches traffic datasets from Taiwan TDX (Transport Data eXchange) and normalizes them
into stable pandas DataFrames used by downstream preprocessing/analytics/API layers.

What this client does (high level):
- Auth: obtains OAuth 2.0 access tokens via `TdxTokenProvider` (client credentials flow).
- Querying: issues OData-style requests with time filters, paging, retries, and optional file cache.
- Normalization: converts raw JSON payloads into internal tables with consistent column names:
  - `segments`: static metadata keyed by `segment_id` (VDID).
  - `observations`: time-series observations keyed by (`segment_id`, `timestamp`).
  - `events`: traffic incidents keyed by `event_id`.

Why the design includes chunking/retries/caching (even for an MVP):
External APIs are unreliable by default (timeouts, transient 5xx, rate limits), so we implement
the minimal reliability primitives early to keep the pipeline reproducible and debuggable.
"""

from __future__ import annotations

# json is used to build stable cache keys for requests (endpoint + query params).
import json
# logging lets us surface rate-limit and retry behavior without spamming stdout.
import logging
# random adds small jitter to backoff so concurrent retries do not synchronize.
import random
# time provides epoch seconds for backoff sleeps (retry strategy).
import time
# dataclass gives lightweight, typed "data carriers" for queries without boilerplate.
from dataclasses import dataclass
# datetime/timedelta represent time windows and chunk boundaries for API queries.
from datetime import datetime, timedelta
# Any/Optional make type intent explicit for JSON payloads and nullable fields.
from typing import Any, Optional

# httpx is our HTTP client library for both the TDX data API and the token endpoint.
import httpx
# pandas is the tabular backbone for downstream preprocessing/analytics workflows.
import pandas as pd

# Logging is configured by helper scripts; the convenience functions in this module call it too.
from trafficpulse.logging_config import configure_logging
# AppConfig keeps this module config-driven and testable (you can pass an explicit config).
from trafficpulse.settings import AppConfig, get_config
# FileCache avoids repeated network calls during development/debugging (TTL-controlled).
from trafficpulse.utils.cache import FileCache
# parse_datetime/to_utc ensure timestamps are normalized and timezone-safe (UTC).
from trafficpulse.utils.time import parse_datetime, to_utc
# Token provider and credential loader implement OAuth client-credentials for TDX.
from trafficpulse.ingestion.tdx_auth import TdxTokenProvider, load_tdx_credentials


class TdxClientError(RuntimeError):
    """Raised when a TDX request fails after retries or returns an unexpected shape."""

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ODataQuery:
    """A single OData-style request descriptor (endpoint + query-string params)."""

    # Endpoint path under the configured `tdx.base_url` (e.g., "Traffic/VD/History/City/Taipei").
    endpoint: str
    # Query params follow OData conventions (e.g., $filter/$top/$skip/$format).
    params: dict[str, Any]
    # Which TDX Basic API version this endpoint lives under ("v1" or "v2").
    api: str = "v2"

    def cache_key(self) -> str:
        """Return a stable, deterministic cache key for this query.

        Why JSON + sort_keys:
        - Dict key order is not guaranteed across construction paths.
        - A stable key ensures identical queries map to the same cache entry.
        """

        # `sort_keys=True` keeps the serialized form stable even if params were built in a different order.
        return json.dumps({"api": self.api, "endpoint": self.endpoint, "params": self.params}, sort_keys=True)


def _isoformat_z(dt: datetime) -> str:
    """Convert a datetime to UTC and format it as an ISO-8601 string with a trailing 'Z'."""

    # Convert to UTC to avoid timezone ambiguity when sending queries to the API.
    utc = to_utc(dt)
    # Drop microseconds to reduce needless variability and keep filters consistent.
    return utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_time_filter(field: str, start: datetime, end: datetime) -> str:
    """Build an OData `$filter` expression for an inclusive start / exclusive end time window."""

    # Convert both bounds to the exact string format expected by the TDX OData filters.
    start_text = _isoformat_z(start)
    end_text = _isoformat_z(end)
    # Use `ge` (>=) for start and `lt` (<) for end to avoid duplicates across adjacent chunks.
    return f"{field} ge {start_text} and {field} lt {end_text}"


def _coerce_float(value: Any) -> Optional[float]:
    """Best-effort float conversion for external JSON values that may be missing or malformed."""

    # Preserve None to distinguish "missing" from numeric zeros.
    if value is None:
        return None
    try:
        # Many APIs encode numbers as strings; float() handles both numbers and numeric strings.
        return float(value)
    except (TypeError, ValueError):
        # If conversion fails, treat as missing rather than crashing ingestion.
        return None


def _get_by_path(record: dict[str, Any], path: str) -> Any:
    """Retrieve a nested value from a dict using a dot-path (e.g., 'Position.PositionLat')."""

    # Empty paths mean "no field configured", so we return None.
    if not path:
        return None
    # Fast path: most fields are top-level and do not require splitting.
    if "." not in path:
        return record.get(path)
    # Walk the nested dict one level at a time; if shape is unexpected, return None.
    current: Any = record
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _coerce_datetime_utc(value: Any) -> Optional[datetime]:
    """Parse a value into a timezone-aware UTC datetime, returning None on failure."""

    # Treat missing values as absent datetimes (common for optional end times).
    if value is None:
        return None
    try:
        # Parse ISO-like strings and normalize them to UTC for consistent downstream processing.
        return to_utc(parse_datetime(str(value)))
    except Exception:
        # Be permissive: ingestion should skip bad records rather than fail the entire run.
        return None


class TdxTrafficClient:
    """A small wrapper around TDX endpoints for VD observations and traffic events.

    The client provides:
    - `fetch_*_raw(...)` for raw JSON dicts (useful for debugging endpoint schemas).
    - `download_*` for normalized pandas DataFrames used by the rest of the pipeline.

    Resource lifetime:
    - This class owns HTTP clients and should be closed via `close()` to avoid connection leaks.
    """

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        # Resolve config paths so cache/output directories are absolute and consistent.
        self.config = (config or get_config()).resolve_paths()

        # The main data client targets the TDX "basic v2" base URL and returns JSON by default.
        self._http_v2 = http_client or httpx.Client(
            base_url=self.config.tdx.base_url,
            timeout=self.config.tdx.request_timeout_seconds,
            headers={"accept": "application/json"},
        )
        # Some TDX endpoints are still served under basic v1 (e.g., RoadEvent).
        self._http_v1 = httpx.Client(
            base_url=self.config.tdx.base_url_v1,
            timeout=self.config.tdx.request_timeout_seconds,
            headers={"accept": "application/json"},
        )
        # Historical datasets are served under a separate base URL (often returning NDJSON).
        self._http_historical = httpx.Client(
            base_url=self.config.tdx.historical_base_url,
            timeout=self.config.tdx.request_timeout_seconds,
            headers={"accept": "application/json"},
        )

        # Load secrets from environment variables (typically loaded from `.env` by settings).
        client_id, client_secret = load_tdx_credentials()
        # Use a separate HTTP client for auth so timeouts/lifetimes can be managed independently.
        self._auth_http = httpx.Client(timeout=self.config.tdx.request_timeout_seconds)
        # The token provider caches the access token in-memory and refreshes when near expiry.
        self._token_provider = TdxTokenProvider(
            token_url=self.config.tdx.token_url,
            client_id=client_id,
            client_secret=client_secret,
            http_client=self._auth_http,
            timeout_seconds=self.config.tdx.request_timeout_seconds,
        )

        # File cache stores API responses on disk to speed up repeated queries during development.
        self._cache = FileCache(
            directory=self.config.paths.cache_dir,
            ttl_seconds=self.config.cache.ttl_seconds,
            enabled=self.config.cache.enabled,
        )
        # Track last request time so we can enforce a client-side minimum interval (optional throttle).
        self._last_request_epoch_seconds: Optional[float] = None

    def close(self) -> None:
        """Close underlying HTTP clients to release sockets and file descriptors."""

        # Closing the data clients prevents connection pool leaks in long sessions/tests.
        self._http_v2.close()
        self._http_v1.close()
        self._http_historical.close()
        # Closing the auth client ensures token refresh requests also release resources.
        self._auth_http.close()

    def _http_for_api(self, api: str) -> httpx.Client:
        if api == "v1":
            return self._http_v1
        if api == "historical":
            return self._http_historical
        return self._http_v2

    @staticmethod
    def _parse_retry_after_seconds(value: Optional[str]) -> Optional[float]:
        """Parse Retry-After header into seconds.

        We support the most common form: an integer delay in seconds. If the server returns
        an HTTP-date form, we ignore it for now (we could parse it later if needed).
        """

        if not value:
            return None
        text = value.strip()
        try:
            seconds = float(text)
        except ValueError:
            return None
        if seconds < 0:
            return None
        return seconds

    def _sleep_throttle(self) -> None:
        """Enforce a minimum delay between requests to reduce 429 risk (optional)."""

        min_interval = float(getattr(self.config.tdx, "min_request_interval_seconds", 0.0) or 0.0)
        if min_interval <= 0:
            return
        now = time.time()
        if self._last_request_epoch_seconds is None:
            return
        elapsed = now - self._last_request_epoch_seconds
        remaining = min_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _compute_backoff_seconds(self, attempt: int, retry_after_seconds: Optional[float]) -> float:
        """Compute sleep time for a given retry attempt, honoring Retry-After when configured."""

        base = float(self.config.tdx.retry_backoff_seconds)
        multiplier = float(getattr(self.config.tdx, "backoff_multiplier", 2.0))
        max_backoff = float(getattr(self.config.tdx, "max_backoff_seconds", 60.0))
        jitter = float(getattr(self.config.tdx, "jitter_seconds", 0.0))

        # Exponential backoff: base * multiplier^attempt (attempt starts at 0 for the first retry).
        delay = base * (multiplier**attempt)
        # Cap the maximum delay so single-request stalls are bounded.
        delay = min(max_backoff, max(0.0, delay))
        # Add a small jitter so many clients do not retry in lockstep.
        if jitter > 0:
            delay += random.uniform(0.0, jitter)

        respect_retry_after = bool(getattr(self.config.tdx, "respect_retry_after", True))
        if respect_retry_after and retry_after_seconds is not None:
            delay = max(delay, retry_after_seconds)

        return delay

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        """Return True for HTTP status codes that are commonly safe to retry."""

        return status_code in {408, 429, 500, 502, 503, 504}

    def _request_json(self, query: ODataQuery) -> list[dict[str, Any]]:
        """Execute a request and return a list of record dicts (with cache + retry support)."""

        # Check disk cache first; this is especially helpful when iterating on normalization logic.
        cached = self._cache.get_json("tdx", query.cache_key())
        if isinstance(cached, list):
            return cached

        # Retry behavior is config-driven so users can tune it for their environment and dataset size.
        max_retries = max(0, int(self.config.tdx.max_retries))

        # Track the last exception so we can surface a useful error after all retries are exhausted.
        last_error: Optional[Exception] = None
        for attempt in range(max_retries + 1):
            try:
                # Optional client-side throttle to reduce the likelihood of upstream 429s.
                self._sleep_throttle()

                # Obtain a valid bearer token; the provider refreshes automatically when needed.
                token = self._token_provider.get_access_token()
                # Build the Authorization header for the TDX API requests.
                headers = {"authorization": f"Bearer {token}"}

                http_client = self._http_for_api(query.api)
                # Execute the GET with OData params; endpoint is relative to `base_url`.
                response = http_client.get(query.endpoint, params=query.params, headers=headers)
                # Record request time only when we actually hit the network (not when served from cache).
                self._last_request_epoch_seconds = time.time()
                # Convert non-2xx responses into exceptions early so we can retry consistently.
                response.raise_for_status()
                # Parse JSON payload; if it's not JSON, this will raise (useful failure signal).
                payload = response.json()
                # Normalize both "list" and OData "{value:[...]}" responses into a list of dict records.
                items = self._extract_items(payload)
                # Persist successful results so repeated local runs do not re-hit the API.
                self._cache.set_json("tdx", query.cache_key(), items)
                return items
            except httpx.HTTPStatusError as exc:
                # Decide whether this status should be retried, and optionally honor Retry-After.
                last_error = exc
                status = int(exc.response.status_code)

                # If we get a 401, the token may have expired earlier than expected; force a refresh once.
                if status == 401 and attempt < max_retries:
                    logger.warning("TDX request unauthorized (401); invalidating cached token and retrying.")
                    self._token_provider.invalidate()
                    delay = self._compute_backoff_seconds(attempt=attempt, retry_after_seconds=None)
                    time.sleep(delay)
                    continue

                if not self._is_retryable_status(status) or attempt >= max_retries:
                    break

                retry_after_seconds = self._parse_retry_after_seconds(
                    exc.response.headers.get("retry-after")
                )
                delay = self._compute_backoff_seconds(
                    attempt=attempt, retry_after_seconds=retry_after_seconds
                )
                logger.warning(
                    "TDX request failed (%s). Retrying in %.2fs (attempt %s/%s).",
                    status,
                    delay,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(delay)
            except Exception as exc:  # noqa: BLE001 - retry transient network/parse failures
                last_error = exc
                if attempt >= max_retries:
                    break
                delay = self._compute_backoff_seconds(attempt=attempt, retry_after_seconds=None)
                logger.warning(
                    "TDX request error (%s). Retrying in %.2fs (attempt %s/%s).",
                    type(exc).__name__,
                    delay,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(delay)

        # Surface a single, explicit error that includes the root cause (last_error) for debugging.
        raise TdxClientError(f"TDX request failed after retries: {last_error}") from last_error

    def _request_ndjson(self, query: ODataQuery) -> list[dict[str, Any]]:
        """Execute a request that returns NDJSON (JSONL), returning a list of dict records."""

        cached = self._cache.get_json("tdx", query.cache_key())
        if isinstance(cached, list):
            return cached

        max_retries = max(0, int(self.config.tdx.max_retries))
        last_error: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            try:
                self._sleep_throttle()

                token = self._token_provider.get_access_token()
                headers = {"authorization": f"Bearer {token}"}
                http_client = self._http_for_api(query.api)

                response = http_client.get(query.endpoint, params=query.params, headers=headers)
                self._last_request_epoch_seconds = time.time()
                response.raise_for_status()
                items: list[dict[str, Any]] = []
                for line in response.text.splitlines():
                    stripped = line.strip()
                    if not stripped:
                        continue
                    parsed = json.loads(stripped)
                    if isinstance(parsed, dict):
                        items.append(parsed)
                self._cache.set_json("tdx", query.cache_key(), items)
                return items
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status = int(exc.response.status_code)

                if status == 401 and attempt < max_retries:
                    logger.warning("TDX request unauthorized (401); invalidating cached token and retrying.")
                    self._token_provider.invalidate()
                    time.sleep(self._compute_backoff_seconds(attempt=attempt, retry_after_seconds=None))
                    continue

                if not self._is_retryable_status(status) or attempt >= max_retries:
                    break

                retry_after_seconds = self._parse_retry_after_seconds(
                    exc.response.headers.get("retry-after")
                )
                delay = self._compute_backoff_seconds(
                    attempt=attempt, retry_after_seconds=retry_after_seconds
                )
                logger.warning(
                    "TDX request failed (%s). Retrying in %.2fs (attempt %s/%s).",
                    status,
                    delay,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(delay)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= max_retries:
                    break
                delay = self._compute_backoff_seconds(attempt=attempt, retry_after_seconds=None)
                logger.warning(
                    "TDX request error (%s). Retrying in %.2fs (attempt %s/%s).",
                    type(exc).__name__,
                    delay,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(delay)

        raise TdxClientError(f"TDX request failed after retries: {last_error}") from last_error

    @staticmethod
    def _extract_items(payload: Any) -> list[dict[str, Any]]:
        """Extract record dicts from common TDX payload shapes."""

        # Some endpoints return a bare JSON array; keep only dict items for schema consistency.
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        # Other endpoints return an OData wrapper object; the records live under `value`.
        if isinstance(payload, dict) and isinstance(payload.get("value"), list):
            return [item for item in payload["value"] if isinstance(item, dict)]
        # Many TDX traffic endpoints return a wrapper with a dataset-specific list field.
        if isinstance(payload, dict):
            for key in ("VDLives", "VDs", "LiveEvents", "Events"):
                if isinstance(payload.get(key), list):
                    items = [item for item in payload[key] if isinstance(item, dict)]
                    # Preserve wrapper timestamps for snapshot-like endpoints (so downstream code can
                    # choose between DataCollectTime vs SrcUpdateTime consistently).
                    wrapper_update_time = payload.get("UpdateTime")
                    wrapper_src_update_time = payload.get("SrcUpdateTime")
                    if wrapper_update_time is not None or wrapper_src_update_time is not None:
                        for item in items:
                            if wrapper_update_time is not None and "__tdx_update_time" not in item:
                                item["__tdx_update_time"] = wrapper_update_time
                            if wrapper_src_update_time is not None and "__tdx_src_update_time" not in item:
                                item["__tdx_src_update_time"] = wrapper_src_update_time
                    return items
        # If neither shape matches, the API may have changed or returned an error object unexpectedly.
        raise TdxClientError("Unexpected TDX response shape; expected a list or an OData {value:[...]} object.")

    def fetch_vd_raw(
        self,
        start: datetime,
        end: datetime,
        cities: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        """Fetch raw VD JSON records for one or more cities in a time window."""

        # Read ingestion settings for VD; fields/endpoints are config-driven.
        config = self.config.ingestion.vd
        # Allow caller override; otherwise use configured default cities.
        selected_cities = cities or config.cities

        # Collect raw records across cities; normalization happens in `download_vd`.
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
        """Fetch raw TrafficEvent JSON records for one or more cities in a time window."""

        # Read ingestion settings for events; fields/endpoints are config-driven.
        config = self.config.ingestion.events
        # Allow caller override; otherwise use configured default cities.
        selected_cities = cities or config.cities

        # Collect raw records across cities; normalization happens in `download_events`.
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
        """Download VD records (historical by default) and return `(segments_df, observations_df)`."""

        return self.download_vd_historical(start=start, end=end, cities=cities)

    def download_vd_live(
        self,
        start: datetime,
        end: datetime,
        cities: Optional[list[str]] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Download VDLive observations for a time window, plus VD metadata for mapping."""

        config = self.config.ingestion.vd
        selected_cities = cities or config.cities

        segments_rows: list[dict[str, Any]] = []
        observation_rows: list[dict[str, Any]] = []

        for city in selected_cities:
            metadata_raw = self._fetch_vd_metadata_city_raw(city=city)
            segments_rows.extend(self._normalize_vd_metadata_records(metadata_raw, city=city))

            # Live endpoints can be inconsistent about server-side time filtering, so we fetch a
            # snapshot and filter client-side by DataCollectTime.
            obs_raw = self._fetch_vd_city_live_raw(city=city)
            observation_rows.extend(
                self._normalize_vd_observation_records(
                    obs_raw, start=start, end=end, timestamp_mode="snapshot"
                )
            )

        segments = self._finalize_segments(pd.DataFrame(segments_rows))
        observations = self._finalize_observations(pd.DataFrame(observation_rows))
        return segments, observations

    def download_vd_metadata(self, cities: Optional[list[str]] = None) -> pd.DataFrame:
        """Download static VD metadata (detector definitions) for one or more cities."""

        config = self.config.ingestion.vd
        selected_cities = cities or config.cities

        segments_rows: list[dict[str, Any]] = []
        for city in selected_cities:
            metadata_raw = self._fetch_vd_metadata_city_raw(city=city)
            segments_rows.extend(self._normalize_vd_metadata_records(metadata_raw, city=city))

        return self._finalize_segments(pd.DataFrame(segments_rows))

    def download_vd_live_snapshot(self, cities: Optional[list[str]] = None) -> pd.DataFrame:
        """Download a single VDLive snapshot for one or more cities as observations rows."""

        config = self.config.ingestion.vd
        selected_cities = cities or config.cities

        observation_rows: list[dict[str, Any]] = []
        for city in selected_cities:
            obs_raw = self._fetch_vd_city_live_raw(city=city)
            observation_rows.extend(
                self._normalize_vd_observation_records(obs_raw, timestamp_mode="snapshot")
            )

        return self._finalize_observations(pd.DataFrame(observation_rows))

    def download_vd_historical(
        self,
        start: datetime,
        end: datetime,
        cities: Optional[list[str]] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Download historical VDLive observations (JSONL by date range), plus VD metadata."""

        config = self.config.ingestion.vd
        selected_cities = cities or config.cities

        segments_rows: list[dict[str, Any]] = []
        observation_rows: list[dict[str, Any]] = []

        for city in selected_cities:
            metadata_raw = self._fetch_vd_metadata_city_raw(city=city)
            segments_rows.extend(self._normalize_vd_metadata_records(metadata_raw, city=city))

            obs_raw = self._fetch_vd_city_historical_raw(city=city, start=start, end=end)
            observation_rows.extend(self._normalize_vd_observation_records(obs_raw))

        segments = self._finalize_segments(pd.DataFrame(segments_rows))
        observations = self._finalize_observations(pd.DataFrame(observation_rows))
        return segments, observations

    @staticmethod
    def _finalize_segments(segments: pd.DataFrame) -> pd.DataFrame:
        if segments.empty:
            return segments

        def first_non_null(series: pd.Series) -> Any:
            non_null = series.dropna()
            return non_null.iloc[0] if not non_null.empty else None

        segments = segments.groupby("segment_id", as_index=False).agg(first_non_null)
        return segments.sort_values("segment_id").reset_index(drop=True)

    @staticmethod
    def _finalize_observations(observations: pd.DataFrame) -> pd.DataFrame:
        if observations.empty:
            return observations
        observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
        observations = observations.dropna(subset=["timestamp", "segment_id"]).sort_values(
            ["segment_id", "timestamp"]
        )
        return observations.reset_index(drop=True)

    def _normalize_vd_metadata_records(self, records: list[dict[str, Any]], city: str) -> list[dict[str, Any]]:
        config = self.config.ingestion.vd
        rows: list[dict[str, Any]] = []
        for record in records:
            segment_id = record.get(config.segment_id_field)
            if segment_id is None:
                continue
            rows.append(self._extract_vd_segment_metadata(record, city=city, segment_id=str(segment_id)))
        return rows

    def _normalize_vd_observation_records(
        self,
        records: list[dict[str, Any]],
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        timestamp_mode: str = "collect",
    ) -> list[dict[str, Any]]:
        config = self.config.ingestion.vd
        rows: list[dict[str, Any]] = []
        start_utc = to_utc(start) if start is not None else None
        end_utc = to_utc(end) if end is not None else None
        for record in records:
            segment_id = record.get(config.segment_id_field)
            if segment_id is None:
                continue
            if timestamp_mode == "snapshot":
                timestamp = record.get("SrcUpdateTime") or record.get("__tdx_src_update_time") or record.get(
                    "UpdateTime"
                ) or record.get("__tdx_update_time")
            else:
                timestamp = record.get(config.time_field)
            if timestamp is None:
                continue
            if start_utc is not None or end_utc is not None:
                dt = _coerce_datetime_utc(timestamp)
                if dt is None:
                    continue
                if start_utc is not None and dt < start_utc:
                    continue
                if end_utc is not None and dt >= end_utc:
                    continue
            speed_kph, volume, occupancy = self._extract_vd_observation_values(record)
            rows.append(
                {
                    "timestamp": timestamp,
                    "segment_id": str(segment_id),
                    "speed_kph": speed_kph,
                    "volume": volume,
                    "occupancy_pct": occupancy,
                }
            )
        return rows

    def download_events(
        self,
        start: datetime,
        end: datetime,
        cities: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Download TrafficEvent records and return a normalized events DataFrame."""

        # Event ingestion settings define endpoint templates and field mappings.
        config = self.config.ingestion.events
        # Allow a caller-provided city list while keeping config defaults.
        selected_cities = cities or config.cities

        # Collect normalized event dict rows across cities.
        rows: list[dict[str, Any]] = []
        for city in selected_cities:
            # Fetch raw records for the city and time window.
            raw = self._fetch_events_city_raw(city=city, start=start, end=end)
            # Normalize raw dicts into a list of stable event-row dicts.
            rows.extend(self._normalize_event_records(raw, city=city))

        # Convert to a DataFrame for sorting/deduplication and downstream compatibility.
        events = pd.DataFrame(rows)
        if events.empty:
            return events

        # Normalize datetime columns to UTC, coercing invalid values to NaT.
        events["start_time"] = pd.to_datetime(events["start_time"], errors="coerce", utc=True)
        # End time is optional, so only convert if the column exists.
        if "end_time" in events.columns:
            events["end_time"] = pd.to_datetime(events["end_time"], errors="coerce", utc=True)
        # Drop rows missing essential keys so downstream analyses do not crash.
        events = events.dropna(subset=["event_id", "start_time"])

        # Deduplicate events by keeping the first non-null value per field.
        def first_non_null(series: pd.Series) -> Any:
            # Prefer non-null values when the same event id appears multiple times.
            non_null = series.dropna()
            # If everything is null, preserve missingness explicitly.
            return non_null.iloc[0] if not non_null.empty else None

        # Group by event_id to collapse duplicates across pages/cities.
        events = events.groupby("event_id", as_index=False).agg(first_non_null)
        # Sort deterministically for reproducible exports and stable UI ordering.
        events = events.sort_values(["start_time", "event_id"]).reset_index(drop=True)
        return events

    def _fetch_vd_metadata_city_raw(self, city: str) -> list[dict[str, Any]]:
        """Fetch VD metadata (static detector definitions) for a single city."""

        config = self.config.ingestion.vd
        page_size = int(config.paging.page_size)
        if page_size <= 0:
            raise ValueError("ingestion.vd.paging.page_size must be > 0")

        base_params: dict[str, Any] = {"$format": "JSON", "$top": page_size}
        endpoint = f"Road/Traffic/VD/City/{city}"
        return self._fetch_paginated(endpoint=endpoint, base_params=base_params, page_size=page_size, api="v2")

    def _fetch_vd_city_live_raw(self, city: str) -> list[dict[str, Any]]:
        """Fetch a VDLive snapshot for a city (server-side time filtering is not relied upon)."""

        config = self.config.ingestion.vd
        page_size = int(config.paging.page_size)
        if page_size <= 0:
            raise ValueError("ingestion.vd.paging.page_size must be > 0")

        base_params: dict[str, Any] = {"$format": "JSON", "$top": page_size}
        last_error: Optional[Exception] = None
        for template in config.endpoint_templates:
            endpoint = template.format(city=city)
            try:
                return self._fetch_paginated(
                    endpoint=endpoint, base_params=base_params, page_size=page_size, api="v2"
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        raise TdxClientError(f"All VD endpoint templates failed for city={city}: {last_error}") from last_error

    def _fetch_vd_city_historical_raw(self, city: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        """Fetch VDLive observations from the historical JSONL endpoint (max 7 days per request)."""

        config = self.config.ingestion.vd
        tz = self._local_timezone()
        start_local = start.astimezone(tz)
        end_local = end.astimezone(tz)

        # Historical service only supports dates up to yesterday. If the requested window includes
        # "today" (local time), we fetch the tail from the live endpoint instead.
        today = datetime.now(tz).date()
        today_start = datetime(today.year, today.month, today.day, tzinfo=tz)
        historical_end_local = min(end_local, today_start)
        live_start_local = max(start_local, today_start)

        # Build an inclusive list of dates to query.
        start_date = start_local.date()
        end_date = (
            (historical_end_local - timedelta(seconds=1)).date()
            if historical_end_local.time() == datetime.min.time()
            else historical_end_local.date()
        )
        if end_date < start_date:
            # No historical portion; fall back to live if needed.
            return (
                self._fetch_vd_city_raw(city=city, start=live_start_local, end=end_local)
                if live_start_local < end_local
                else []
            )

        dates: list[str] = []
        current = start_date
        while current <= end_date:
            dates.append(current.strftime("%Y-%m-%d"))
            current = current + timedelta(days=1)

        results: list[dict[str, Any]] = []
        for i in range(0, len(dates), 7):
            batch = dates[i : i + 7]
            if not batch:
                continue
            dates_param = batch[0] if len(batch) == 1 else f"{batch[0]}~{batch[-1]}"

            last_error: Optional[Exception] = None
            for template in config.historical_endpoint_templates:
                endpoint = template.format(city=city)
                params: dict[str, Any] = {"Dates": dates_param, "$format": "JSONL"}
                try:
                    results.extend(
                        self._request_ndjson(ODataQuery(api="historical", endpoint=endpoint, params=params))
                    )
                    last_error = None
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
            if last_error is not None:
                raise TdxClientError(
                    f"All VD historical endpoint templates failed for city={city}: {last_error}"
                ) from last_error

        # Filter to the requested time window using the configured time field.
        filtered: list[dict[str, Any]] = []
        start_utc = to_utc(start)
        end_utc = to_utc(end)
        for item in results:
            dt = _coerce_datetime_utc(item.get(config.time_field))
            if dt is None:
                continue
            if start_utc <= dt < end_utc:
                filtered.append(item)

        if live_start_local < end_local:
            filtered.extend(self._fetch_vd_city_raw(city=city, start=live_start_local, end=end_local))

        return filtered

    def _local_timezone(self):
        from zoneinfo import ZoneInfo

        try:
            return ZoneInfo(self.config.app.timezone)
        except Exception:  # pragma: no cover
            return ZoneInfo("Asia/Taipei")

    def _fetch_vd_city_raw(self, city: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        """Fetch raw VD records for a single city, chunking long windows into smaller requests."""

        # Chunk size is config-driven so users can tune it for endpoint limits and network stability.
        chunk_minutes = int(self.config.ingestion.query_chunk_minutes)
        # A non-positive chunk size would create an infinite loop, so we validate explicitly.
        if chunk_minutes <= 0:
            raise ValueError("ingestion.query_chunk_minutes must be > 0")

        # Accumulate results across all chunks of the time window.
        results: list[dict[str, Any]] = []
        # Cursor walks from start to end, producing adjacent, non-overlapping chunks.
        cursor = start
        while cursor < end:
            # Clamp the chunk end to the overall end of the requested window.
            chunk_end = min(cursor + timedelta(minutes=chunk_minutes), end)
            # Fetch one chunk and append it to the results.
            results.extend(self._fetch_vd_city_chunk_raw(city=city, start=cursor, end=chunk_end))
            # Advance the cursor; because end is exclusive in filters, this does not duplicate rows.
            cursor = chunk_end
        return results

    def _fetch_vd_city_chunk_raw(
        self, city: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Fetch one VD time chunk for a city, using paging and endpoint-template fallback."""

        # VD config defines endpoint templates and field names for time filtering.
        config = self.config.ingestion.vd
        # Page size controls `$top` and therefore affects memory/latency trade-offs.
        page_size = int(config.paging.page_size)
        # Non-positive page sizes would loop forever or request empty pages, so we validate.
        if page_size <= 0:
            raise ValueError("ingestion.vd.paging.page_size must be > 0")

        # Build the OData time filter using configured field names (dataset-dependent).
        filter_text = _build_time_filter(config.time_field, start=start, end=end)
        # Base params apply to every page; `$skip` is added later inside `_fetch_paginated`.
        base_params: dict[str, Any] = {"$format": "JSON", "$filter": filter_text, "$top": page_size}

        # Some datasets expose multiple endpoints (history/live); try each template until one works.
        last_error: Optional[Exception] = None
        for template in config.endpoint_templates:
            # Substitute the city name into the endpoint template.
            endpoint = template.format(city=city)
            try:
                # Fetch all pages for the chosen endpoint and return immediately on success.
                return self._fetch_paginated(
                    endpoint=endpoint, base_params=base_params, page_size=page_size, api="v2"
                )
            except Exception as exc:  # noqa: BLE001 - endpoint availability can vary by city/dataset
                # Store the error so we can report it if all templates fail.
                last_error = exc
                continue

        # If no endpoint template succeeded, raise a single error with the last exception for context.
        raise TdxClientError(f"All VD endpoint templates failed for city={city}: {last_error}") from last_error

    def _fetch_events_city_raw(self, city: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        """Fetch raw TrafficEvent records for a single city, chunking long windows into smaller requests."""

        # Use the same chunking strategy as VD to avoid oversized requests.
        chunk_minutes = int(self.config.ingestion.query_chunk_minutes)
        # Validate chunk size to avoid infinite loops and meaningless requests.
        if chunk_minutes <= 0:
            raise ValueError("ingestion.query_chunk_minutes must be > 0")

        # Accumulate results across all time chunks.
        results: list[dict[str, Any]] = []
        # Cursor walks through the requested window.
        cursor = start
        while cursor < end:
            # Clamp each chunk to the requested end time.
            chunk_end = min(cursor + timedelta(minutes=chunk_minutes), end)
            # Fetch one chunk and append its records.
            results.extend(self._fetch_events_city_chunk_raw(city=city, start=cursor, end=chunk_end))
            # Advance to the next chunk boundary.
            cursor = chunk_end
        return results

    def _fetch_events_city_chunk_raw(
        self, city: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Fetch one TrafficEvent time chunk for a city, using paging and endpoint-template fallback."""

        # Events config defines endpoint templates and field mappings (which can vary across feeds).
        config = self.config.ingestion.events
        # Page size controls `$top` and therefore the number of paging requests.
        page_size = int(config.paging.page_size)
        # Validate to avoid infinite loops or invalid API calls.
        if page_size <= 0:
            raise ValueError("ingestion.events.paging.page_size must be > 0")

        # Build the OData time filter using the configured event start-time field.
        filter_text = _build_time_filter(config.start_time_field, start=start, end=end)
        # Base params for paging; `$skip` is added inside `_fetch_paginated`.
        base_params: dict[str, Any] = {"$format": "JSON", "$filter": filter_text, "$top": page_size}

        # Try endpoint templates in order because some cities may not support history endpoints, etc.
        last_error: Optional[Exception] = None
        for template in config.endpoint_templates:
            # Substitute the city into the endpoint path.
            endpoint = template.format(city=city)
            try:
                # Fetch all pages for this endpoint and return on the first success.
                return self._fetch_paginated(
                    endpoint=endpoint, base_params=base_params, page_size=page_size, api="v1"
                )
            except Exception as exc:  # noqa: BLE001 - endpoint availability can vary across feeds
                # Store the last error and try the next endpoint template.
                last_error = exc
                continue

        # If all templates failed, raise an informative error so users can adjust config.
        raise TdxClientError(
            f"All event endpoint templates failed for city={city}: {last_error}"
        ) from last_error

    def _fetch_paginated(
        self, endpoint: str, base_params: dict[str, Any], page_size: int, api: str = "v2"
    ) -> list[dict[str, Any]]:
        """Fetch all pages for an endpoint using OData `$top`/`$skip` pagination."""

        # Collect all pages into one list so normalization can operate on a complete dataset.
        items: list[dict[str, Any]] = []
        # `$skip` starts at 0 and increments by `$top` until the last page is shorter than page_size.
        skip = 0
        while True:
            # Copy base params so we do not mutate the dict shared by callers.
            params = dict(base_params)
            # Add paging offset; this is the only param that changes between pages.
            params["$skip"] = skip
            # Execute the request with caching and retry support.
            page = self._request_json(ODataQuery(api=api, endpoint=endpoint, params=params))
            # Append page records to the full result set.
            items.extend(page)
            # When we receive a short page, we have reached the end.
            if len(page) < page_size:
                break
            # Advance the offset by one page size to request the next page.
            skip += page_size
        return items

    def _normalize_event_records(self, records: list[dict[str, Any]], city: str) -> list[dict[str, Any]]:
        """Normalize raw event records into a stable list of row dicts."""

        # Config defines which raw JSON fields map to our internal event schema.
        config = self.config.ingestion.events

        # Build normalized rows as plain dicts to keep pandas conversion straightforward.
        rows: list[dict[str, Any]] = []
        for record in records:
            # Event id is required; skip records without a stable identifier.
            event_id = _get_by_path(record, config.id_field)
            if event_id is None:
                continue

            # Start time is required for time-series alignment and impact analysis.
            start_time = _coerce_datetime_utc(_get_by_path(record, config.start_time_field))
            if start_time is None:
                continue

            # End time may be missing for ongoing incidents; keep it nullable.
            end_time = _coerce_datetime_utc(_get_by_path(record, config.end_time_field))

            # Map raw fields into our internal schema, coercing types where needed.
            lat = _coerce_float(_get_by_path(record, config.lat_field)) if config.lat_field else None
            lon = _coerce_float(_get_by_path(record, config.lon_field)) if config.lon_field else None
            if lat is None and lon is None:
                positions = record.get("Positions")
                if isinstance(positions, str):
                    text = positions.strip()
                    if text.upper().startswith("POINT(") and text.endswith(")"):
                        inner = text[text.find("(") + 1 : -1].strip()
                        parts = inner.split()
                        if len(parts) == 2:
                            lon = _coerce_float(parts[0])
                            lat = _coerce_float(parts[1])

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
                    "lat": lat,
                    "lon": lon,
                    "city": city,
                    # Track provenance so we can mix sources later without ambiguity.
                    "source": "tdx",
                }
            )

        return rows

    def _normalize_vd_records(
        self, records: list[dict[str, Any]], city: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Normalize raw VD records into `(segments_df, observations_df)`."""

        # VD config defines which raw JSON fields correspond to time, ids, and lane measurements.
        config = self.config.ingestion.vd

        # Collect normalized dict rows for both outputs before building DataFrames.
        segment_rows: list[dict[str, Any]] = []
        observation_rows: list[dict[str, Any]] = []

        for record in records:
            # Segment id is required because it becomes our primary key (`segment_id`).
            segment_id = record.get(config.segment_id_field)
            if segment_id is None:
                continue
            # Normalize ids to strings so we do not lose leading zeros or mix numeric/string ids.
            segment_id = str(segment_id)

            # Extract and store segment metadata (static columns).
            segment_rows.append(self._extract_vd_segment_metadata(record, city=city, segment_id=segment_id))

            # Observations require a timestamp; skip rows that cannot be aligned in time.
            timestamp = record.get(config.time_field)
            if timestamp is None:
                continue

            # Extract observation values, including lane aggregation when lane lists are present.
            speed_kph, volume, occupancy = self._extract_vd_observation_values(record)
            # Store a single normalized observation row for downstream time-series analytics.
            observation_rows.append(
                {
                    "timestamp": timestamp,
                    "segment_id": segment_id,
                    "speed_kph": speed_kph,
                    "volume": volume,
                    "occupancy_pct": occupancy,
                }
            )

        # Convert row dicts to DataFrames; downstream steps may further clean timestamps and deduplicate.
        segments = pd.DataFrame(segment_rows)
        observations = pd.DataFrame(observation_rows)
        return segments, observations

    def _extract_vd_segment_metadata(
        self, record: dict[str, Any], city: str, segment_id: str
    ) -> dict[str, Any]:
        """Extract stable segment metadata columns from a raw VD record."""

        # Field names are config-driven because TDX schemas can vary across datasets/versions.
        fields = self.config.ingestion.vd.metadata_fields
        return {
            "segment_id": segment_id,
            "city": city,
            "name": record.get(fields.name_field),
            "direction": record.get(fields.direction_field),
            "road_name": record.get(fields.road_name_field),
            "link_id": record.get(fields.link_id_field),
            # Lat/lon are coerced to floats so mapping libraries can consume them reliably.
            "lat": _coerce_float(record.get(fields.lat_field)),
            "lon": _coerce_float(record.get(fields.lon_field)),
        }

    def _extract_vd_observation_values(
        self, record: dict[str, Any]
    ) -> tuple[Optional[float], Optional[float], Optional[float]]:
        """Extract speed/volume/occupancy values from a raw VD record (including lane aggregation)."""

        # VD config defines where lane-level measurements live and how to aggregate them.
        config = self.config.ingestion.vd

        # If the record includes a list of lane measurements, aggregate them into one station-level value.
        lane_list = record.get(config.lane_list_field)
        if isinstance(lane_list, list) and lane_list:
            return self._aggregate_lanes(lane_list)

        # Road/Traffic VDLive nests lanes under LinkFlows[].Lanes[].
        link_flows = record.get("LinkFlows")
        if isinstance(link_flows, list) and link_flows:
            lanes: list[dict[str, Any]] = []
            for flow in link_flows:
                if not isinstance(flow, dict):
                    continue
                flow_lanes = flow.get("Lanes")
                if isinstance(flow_lanes, list):
                    lanes.extend([lane for lane in flow_lanes if isinstance(lane, dict)])
            if lanes:
                return self._aggregate_lanes(lanes)

        # Otherwise fall back to top-level fields, coercing types defensively.
        speed = _coerce_float(record.get(config.lane_speed_field))
        volume = _coerce_float(record.get(config.lane_volume_field))
        occupancy = _coerce_float(record.get(config.lane_occupancy_field))
        return speed, volume, occupancy

    def _aggregate_lanes(
        self, lanes: list[dict[str, Any]]
    ) -> tuple[Optional[float], Optional[float], Optional[float]]:
        """Aggregate lane-level VD measurements into a single speed/volume/occupancy triple."""

        # Config controls aggregation strategy (e.g., volume-weighted speed vs simple mean).
        config = self.config.ingestion.vd

        # Keep raw lane measurements so we can compute mean/sum based on configured aggregation modes.
        lane_speeds: list[float] = []
        lane_volumes: list[float] = []
        lane_occupancies: list[float] = []

        # Track weighted sums separately so volume-weighted speed can be computed efficiently.
        weighted_speed_sum = 0.0
        weighted_volume_sum = 0.0

        for lane in lanes:
            # Lane lists can contain non-dict values; skip anything unexpected to stay robust.
            if not isinstance(lane, dict):
                continue
            # Coerce per-lane values defensively; external APIs may emit strings or nulls.
            speed = _coerce_float(lane.get(config.lane_speed_field))
            volume = _coerce_float(lane.get(config.lane_volume_field))
            occupancy = _coerce_float(lane.get(config.lane_occupancy_field))

            # Collect values for aggregation; we keep them as floats when present.
            if speed is not None:
                lane_speeds.append(speed)
            if volume is not None:
                lane_volumes.append(volume)
            # For weighted speed, we only use positive volumes to avoid dividing by zero or negatives.
            if speed is not None and volume is not None and volume > 0:
                weighted_speed_sum += speed * volume
                weighted_volume_sum += volume
            if occupancy is not None:
                lane_occupancies.append(occupancy)

        # Compute aggregated speed using the configured mode, returning None when no data exists.
        speed_kph = None
        if lane_speeds:
            # Prefer volume-weighted speed when configured and when we have valid volumes.
            if config.lane_speed_aggregation == "volume_weighted_mean" and weighted_volume_sum > 0:
                speed_kph = weighted_speed_sum / weighted_volume_sum
            else:
                # Fallback to a simple arithmetic mean across lanes.
                speed_kph = sum(lane_speeds) / len(lane_speeds)

        # Compute aggregated volume; this is typically a sum across lanes.
        volume_value = None
        if lane_volumes:
            if config.lane_volume_aggregation == "sum":
                volume_value = sum(v for v in lane_volumes if v is not None)
            elif config.lane_volume_aggregation == "mean":
                volume_value = sum(v for v in lane_volumes if v is not None) / len(lane_volumes)
            else:
                # Default to sum for unknown config values to avoid surprising "None" outputs.
                volume_value = sum(v for v in lane_volumes if v is not None)

        # Compute aggregated occupancy; this is typically a mean across lanes.
        occupancy_value = None
        if lane_occupancies:
            if config.lane_occupancy_aggregation == "mean":
                occupancy_value = sum(lane_occupancies) / len(lane_occupancies)
            elif config.lane_occupancy_aggregation == "sum":
                occupancy_value = sum(lane_occupancies)
            else:
                # Default to mean for unknown config values to keep the value in a reasonable range.
                occupancy_value = sum(lane_occupancies) / len(lane_occupancies)

        return speed_kph, volume_value, occupancy_value


def build_vd_dataset(
    start: datetime, end: datetime, cities: Optional[list[str]] = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convenience wrapper to build a VD dataset with default config and logging enabled."""

    # Configure logging for script usage; this keeps CLI behavior consistent across entrypoints.
    configure_logging()
    # Instantiate the client with global config; credentials must already be available in env/.env.
    client = TdxTrafficClient()
    try:
        # Delegate to `download_vd` so normalization logic stays in one place.
        return client.download_vd(start=start, end=end, cities=cities)
    finally:
        # Always close the client to prevent socket/file descriptor leaks.
        client.close()


def build_events_dataset(
    start: datetime, end: datetime, cities: Optional[list[str]] = None
) -> pd.DataFrame:
    """Convenience wrapper to build an events dataset with default config and logging enabled."""

    # Configure logging for script usage; consistent logs make debugging ingestion easier.
    configure_logging()
    # Instantiate the client; token provider will fetch/refresh tokens as needed.
    client = TdxTrafficClient()
    try:
        # Delegate to `download_events` so event normalization stays encapsulated.
        return client.download_events(start=start, end=end, cities=cities)
    finally:
        # Ensure resources are released even when network errors occur.
        client.close()
