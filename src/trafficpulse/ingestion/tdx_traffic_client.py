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


@dataclass(frozen=True)
class ODataQuery:
    """A single OData-style request descriptor (endpoint + query-string params)."""

    # Endpoint path under the configured `tdx.base_url` (e.g., "Traffic/VD/History/City/Taipei").
    endpoint: str
    # Query params follow OData conventions (e.g., $filter/$top/$skip/$format).
    params: dict[str, Any]

    def cache_key(self) -> str:
        """Return a stable, deterministic cache key for this query.

        Why JSON + sort_keys:
        - Dict key order is not guaranteed across construction paths.
        - A stable key ensures identical queries map to the same cache entry.
        """

        # `sort_keys=True` keeps the serialized form stable even if params were built in a different order.
        return json.dumps({"endpoint": self.endpoint, "params": self.params}, sort_keys=True)


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
        self._http = http_client or httpx.Client(
            base_url=self.config.tdx.base_url,
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

    def close(self) -> None:
        """Close underlying HTTP clients to release sockets and file descriptors."""

        # Closing the data client prevents connection pool leaks in long sessions/tests.
        self._http.close()
        # Closing the auth client ensures token refresh requests also release resources.
        self._auth_http.close()

    def _request_json(self, query: ODataQuery) -> list[dict[str, Any]]:
        """Execute a request and return a list of record dicts (with cache + retry support)."""

        # Check disk cache first; this is especially helpful when iterating on normalization logic.
        cached = self._cache.get_json("tdx", query.cache_key())
        if isinstance(cached, list):
            return cached

        # Obtain a valid bearer token; the provider refreshes automatically when needed.
        token = self._token_provider.get_access_token()
        # Build the Authorization header for the TDX API requests.
        headers = {"authorization": f"Bearer {token}"}

        # Retry behavior is config-driven so users can tune it for their environment and dataset size.
        max_retries = max(0, int(self.config.tdx.max_retries))
        backoff_seconds = float(self.config.tdx.retry_backoff_seconds)

        # Track the last exception so we can surface a useful error after all retries are exhausted.
        last_error: Optional[Exception] = None
        for attempt in range(max_retries + 1):
            try:
                # Execute the GET with OData params; endpoint is relative to `base_url`.
                response = self._http.get(query.endpoint, params=query.params, headers=headers)
                # Convert non-2xx responses into exceptions early so we can retry consistently.
                response.raise_for_status()
                # Parse JSON payload; if it's not JSON, this will raise (useful failure signal).
                payload = response.json()
                # Normalize both "list" and OData "{value:[...]}" responses into a list of dict records.
                items = self._extract_items(payload)
                # Persist successful results so repeated local runs do not re-hit the API.
                self._cache.set_json("tdx", query.cache_key(), items)
                return items
            except Exception as exc:  # noqa: BLE001 - we want to retry on any transient network/parse failure
                # Store the last error to report if we run out of retries.
                last_error = exc
                # Break immediately when we have no retries left.
                if attempt >= max_retries:
                    break
                # Exponential backoff reduces pressure on the upstream service and avoids thundering herds.
                time.sleep(backoff_seconds * (2**attempt))

        # Surface a single, explicit error that includes the root cause (last_error) for debugging.
        raise TdxClientError(f"TDX request failed after retries: {last_error}") from last_error

    @staticmethod
    def _extract_items(payload: Any) -> list[dict[str, Any]]:
        """Extract records from either a raw list payload or an OData `{value: [...]}` payload."""

        # Some endpoints return a bare JSON array; keep only dict items for schema consistency.
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        # Other endpoints return an OData wrapper object; the records live under `value`.
        if isinstance(payload, dict) and isinstance(payload.get("value"), list):
            return [item for item in payload["value"] if isinstance(item, dict)]
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
        """Download VD records and return `(segments_df, observations_df)` as normalized DataFrames."""

        # VD ingestion settings define which endpoints/fields we use for this dataset.
        config = self.config.ingestion.vd
        # Allow a caller-provided city list (e.g., scripts) while keeping config defaults.
        selected_cities = cities or config.cities

        # Accumulate rows as dicts first; this is flexible when raw payload schemas are inconsistent.
        segment_rows: list[dict[str, Any]] = []
        observation_rows: list[dict[str, Any]] = []

        # Fetch and normalize per city so endpoint failures or schema differences are easier to isolate.
        for city in selected_cities:
            # Fetch raw JSON dicts using chunking/pagination under the hood.
            raw = self._fetch_vd_city_raw(city=city, start=start, end=end)
            # Convert raw dicts into standardized segment/observation DataFrames.
            segments_df, observations_df = self._normalize_vd_records(raw, city=city)
            # Convert DataFrames back to dict records so we can concatenate across cities cheaply.
            if not segments_df.empty:
                segment_rows.extend(segments_df.to_dict(orient="records"))
            if not observations_df.empty:
                observation_rows.extend(observations_df.to_dict(orient="records"))

        # Build the final segments DataFrame from accumulated rows.
        segments = pd.DataFrame(segment_rows)
        if not segments.empty:

            # When the same segment appears multiple times, keep the first non-null value per column.
            def first_non_null(series: pd.Series) -> Any:
                # Drop NaNs so we do not choose missing values when a real value exists.
                non_null = series.dropna()
                # If the entire series is null, return None to keep the field explicitly missing.
                return non_null.iloc[0] if not non_null.empty else None

            # Deduplicate segments across time/cities using a stable key (`segment_id`).
            segments = segments.groupby("segment_id", as_index=False).agg(first_non_null)
            # Sort for deterministic outputs (helps reproducibility and diff-friendly exports).
            segments = segments.sort_values("segment_id")

        # Build the final observations DataFrame from accumulated rows.
        observations = pd.DataFrame(observation_rows)
        if not observations.empty:

            # Parse timestamps robustly; bad formats should become NaT instead of crashing the run.
            def normalize_timestamp(value: Any) -> Any:
                # Treat missing timestamps as NaT (pandas' missing datetime value).
                if value is None:
                    return pd.NaT
                try:
                    # Convert to UTC so all downstream grouping/aggregation happens in one timezone.
                    return to_utc(parse_datetime(str(value)))
                except Exception:
                    # Any parsing error results in NaT so we can drop invalid rows later.
                    return pd.NaT

            # Apply parsing on the raw timestamp field.
            observations["timestamp"] = observations["timestamp"].map(normalize_timestamp)
            # Ensure pandas recognizes the column as timezone-aware UTC datetimes.
            observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
            # Drop rows missing the minimum keys and sort for stable downstream processing.
            observations = observations.dropna(subset=["timestamp", "segment_id"]).sort_values(
                ["segment_id", "timestamp"]
            )

        # Return the two canonical tables used by preprocessing and analytics.
        return segments, observations

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
                return self._fetch_paginated(endpoint=endpoint, base_params=base_params, page_size=page_size)
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
                return self._fetch_paginated(endpoint=endpoint, base_params=base_params, page_size=page_size)
            except Exception as exc:  # noqa: BLE001 - endpoint availability can vary across feeds
                # Store the last error and try the next endpoint template.
                last_error = exc
                continue

        # If all templates failed, raise an informative error so users can adjust config.
        raise TdxClientError(
            f"All event endpoint templates failed for city={city}: {last_error}"
        ) from last_error

    def _fetch_paginated(
        self, endpoint: str, base_params: dict[str, Any], page_size: int
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
            page = self._request_json(ODataQuery(endpoint=endpoint, params=params))
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
