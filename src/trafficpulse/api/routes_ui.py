"""UI settings endpoints used by the static dashboard.

This module exists to keep a single source of truth for "default" dashboard parameters:
- The backend reads defaults from the server-side config (configs/config.yaml).
- The frontend calls `GET /ui/settings` to populate placeholders and reset-to-default behavior.

Why this matters:
- It prevents UI/backend drift (no duplicated magic numbers in JavaScript).
- It keeps analytics parameters explainable and configurable (no hard-coded weights/time windows).
- It avoids leaking secrets: this endpoint intentionally returns only non-sensitive settings.
"""

from __future__ import annotations

# json is used to write small status payloads without heavy dependencies.
import csv
import json
import asyncio
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict
# We use Literal to constrain certain UI-controlled strings to a small, known set of values,
# which behaves like an enum and prevents accidental typos from silently changing behavior.
from typing import Literal

# FastAPI's APIRouter lets us group related endpoints into a module and mount them in the main app.
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
# Pydantic models define the JSON schema sent to the browser, which makes the API contract explicit.
from pydantic import BaseModel, Field

# The config is the single source of truth for all tunable parameters (thresholds, weights, windows).
from trafficpulse.api.dataset_version import dataset_version_from_paths, minutes_candidates as dataset_minutes_candidates
from trafficpulse.api.schemas import EmptyReason, ItemsResponse, ReasonCode
from trafficpulse.settings import get_config
from trafficpulse.ingestion.ledger import read_latest_ledger_entry
from trafficpulse.quality.observations import clean_observations
from trafficpulse.quality.schema import SCHEMA_VERSIONS
from trafficpulse.storage.backend import duckdb_backend
from trafficpulse.storage.datasets import observations_csv_path, observations_parquet_path
from trafficpulse.utils.time import parse_datetime, to_utc


# This router is imported and included by `trafficpulse.api.app:create_app()`.
router = APIRouter()


class ReliabilitySettings(BaseModel):
    """Dashboard defaults for reliability scoring (explainable, config-driven metrics)."""

    # Speed threshold (kph) under which we consider a timestamp "congested" for frequency metrics.
    congestion_speed_threshold_kph: float
    # Minimum number of time points required to rank a segment (avoids tiny-sample noise).
    min_samples: int
    # Per-metric weights used to combine metrics into a single ranking score.
    weights: dict[str, float]


class AnomaliesSettings(BaseModel):
    """Dashboard defaults for explainable anomaly detection (rolling z-score baseline)."""

    # Method name is returned for transparency and future extensibility (e.g., other baseline methods).
    method: str
    # Rolling window size in "points" (not minutes): points = timestamps at the selected granularity.
    window_points: int
    # Z-score threshold: larger values mean fewer, more extreme anomalies are flagged.
    z_threshold: float
    # Direction controls whether we flag low-speed drops, high-speed spikes, or both.
    direction: Literal["low", "high", "both"]
    # Max gap between anomaly points (minutes) before we split them into separate "events".
    max_gap_minutes: int
    # Minimum number of points required to consider an anomaly cluster an "event".
    min_event_points: int


class EventImpactSettings(BaseModel):
    """Dashboard defaults for baseline event impact analysis (no ML; explainable windows)."""

    # Default time window (hours) used when the UI does not provide a time range explicitly.
    default_window_hours: int
    # Radius around an event point used to find nearby segments (meters).
    radius_meters: float
    # Safety cap to avoid returning too many impacted segments (UI and API performance guard).
    max_segments: int
    # Baseline window (minutes) used to estimate "normal" speed before the event.
    baseline_window_minutes: int
    # If an event has no end time, we treat it as ending after this many minutes (fallback).
    end_time_fallback_minutes: int
    # How far after the event we look for recovery (minutes).
    recovery_horizon_minutes: int
    # Recovery ratio (0..1): e.g., 0.9 means "recovered when speed is back to 90% of baseline".
    recovery_ratio: float
    # How to weight segment speeds when aggregating impacted segments (volume-weighted or equal).
    speed_weighting: Literal["volume", "equal"]
    # Minimum baseline points required to compute a stable baseline estimate.
    min_baseline_points: int
    # Minimum points inside the event window required to compute impact metrics.
    min_event_points: int


class UiSettings(BaseModel):
    """Top-level response schema consumed by the frontend Controls panel."""

    # Preprocessing settings affect the available time-series granularities (e.g., 5m -> 15m).
    preprocessing: dict[str, int]
    # Analytics defaults are grouped by feature (reliability/anomalies/event impact).
    analytics: dict[str, object] = Field(default_factory=dict)
    # Warehouse toggles help the UI explain whether DuckDB+Parquet is enabled.
    warehouse: dict[str, object] = Field(default_factory=dict)
    # Enum-like values are returned so the UI can populate dropdowns consistently.
    enums: dict[str, list[str]] = Field(default_factory=dict)


class UiStatus(BaseModel):
    """Lightweight dataset status used by the dashboard for 'freshness' indicators."""

    generated_at_utc: datetime
    observations_minutes_available: list[int] = Field(default_factory=list)
    observations_last_timestamp_utc: datetime | None = None
    dataset_version: str | None = None
    live_loop_last_snapshot_timestamp: str | None = None
    daily_backfill_last_date: str | None = None
    last_error: str | None = None
    last_error_code: str | None = None
    last_error_kind: str | None = None
    last_ingest_ok: bool | None = None
    updated_files: list[str] = Field(default_factory=list)
    ingest_ledger_latest: dict[str, object] | None = None
    ingest_quality: dict[str, int] = Field(default_factory=dict)
    ingest_consecutive_failures: int | None = None
    ingest_backoff_seconds: int | None = None
    ingest_last_success_utc: str | None = None
    ingest_rate_limit: dict[str, float | int | None] = Field(default_factory=dict)


class DatasetFileInfo(BaseModel):
    path: str
    exists: bool
    size_bytes: int | None = None
    mtime_utc: datetime | None = None


class UiDiagnostics(BaseModel):
    generated_at_utc: datetime
    processed_dir: str
    parquet_dir: str
    corridors_csv: str
    corridors_csv_exists: bool
    segments_csv: DatasetFileInfo
    observations_csv_files: list[DatasetFileInfo]
    events_csv: DatasetFileInfo
    weather_csv: DatasetFileInfo
    materialized_defaults: DatasetFileInfo
    baselines_speed_files: list[DatasetFileInfo]
    segment_quality_files: list[DatasetFileInfo]
    congestion_alerts: DatasetFileInfo
    event_hotspot_links: DatasetFileInfo
    cache_dir: str
    live_loop_state: DatasetFileInfo
    backfill_checkpoint: DatasetFileInfo


class UiAlerts(BaseModel):
    generated_at_utc: datetime
    path: str
    lines: list[str] = Field(default_factory=list)
    summary: dict[str, object] = Field(default_factory=dict)
    buckets: list[dict[str, object]] = Field(default_factory=list)


class UiTrends(BaseModel):
    generated_at_utc: datetime
    window_hours: int
    buckets: list[dict[str, object]] = Field(default_factory=list)
    summary: dict[str, object] = Field(default_factory=dict)


class UiWeatherRow(BaseModel):
    timestamp: datetime
    city: str
    rain_mm: float | None = None
    wind_mps: float | None = None
    visibility_km: float | None = None
    temperature_c: float | None = None
    humidity_pct: float | None = None
    source: str | None = None


class UiWeatherLatest(BaseModel):
    generated_at_utc: datetime
    items: list[UiWeatherRow] = Field(default_factory=list)


class QualityIssue(BaseModel):
    severity: Literal["info", "warn", "error"]
    code: str
    dataset: str
    message: str
    suggestion: str | None = None


class DatasetQuality(BaseModel):
    dataset: str
    path: str
    exists: bool
    sample_rows: int = 0
    issues: list[QualityIssue] = Field(default_factory=list)
    stats: dict[str, object] = Field(default_factory=dict)


class UiQualityReport(BaseModel):
    generated_at_utc: datetime
    sample_rows: int
    schema_versions: dict[str, int] = Field(default_factory=dict)
    datasets: list[DatasetQuality] = Field(default_factory=list)
    issues: list[QualityIssue] = Field(default_factory=list)
    fix_commands: list[str] = Field(default_factory=list)


def _file_info(path: Path) -> DatasetFileInfo:
    if not path.exists():
        return DatasetFileInfo(path=str(path), exists=False)
    stat = path.stat()
    return DatasetFileInfo(
        path=str(path),
        exists=True,
        size_bytes=int(stat.st_size),
        mtime_utc=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
    )


def _tail_csv_timestamp(path: Path) -> datetime | None:
    """Return the timestamp value from the last non-empty row of a CSV (without loading the file)."""

    if not path.exists():
        return None
    with path.open("rb") as f:
        header = f.readline()
        if not header:
            return None
        try:
            header_cols = next(csv.reader([header.decode("utf-8", errors="ignore")]))
        except Exception:
            return None
        try:
            ts_index = header_cols.index("timestamp")
        except ValueError:
            return None

        f.seek(0, 2)
        end = f.tell()
        if end <= 0:
            return None

        # Read backwards in chunks until we find at least one data line.
        chunk_size = 8192
        buffer = b""
        pos = end
        while pos > 0:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            buffer = f.read(read_size) + buffer
            if b"\n" not in buffer:
                continue
            lines = buffer.splitlines()
            # Walk from the end to find the last non-empty, non-header line.
            for raw_line in reversed(lines):
                line = raw_line.strip()
                if not line:
                    continue
                # Skip header line if it appears in the buffer.
                if line == header.strip():
                    continue
                try:
                    cols = next(csv.reader([line.decode("utf-8", errors="ignore")]))
                except Exception:
                    continue
                if ts_index >= len(cols):
                    continue
                value = cols[ts_index].strip()
                if not value:
                    continue
                try:
                    return to_utc(parse_datetime(value))
                except Exception:
                    return None
        return None


def _max_observation_timestamp(config_minutes: int) -> datetime | None:
    config = get_config()
    processed_dir = config.paths.processed_dir
    parquet_dir = config.warehouse.parquet_dir

    obs_csv = observations_csv_path(processed_dir, int(config_minutes))
    obs_parquet = observations_parquet_path(parquet_dir, int(config_minutes))

    backend = duckdb_backend(config)
    if backend is not None and obs_parquet.exists():
        ts = backend.max_observation_timestamp(minutes=int(config_minutes))
        if ts is None:
            return None
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)

    if config.warehouse.enabled and obs_parquet.exists():
        # Avoid importing pyarrow/pandas here; fall back to CSV tail if Parquet isn't queryable.
        return None

    return _tail_csv_timestamp(obs_csv)


def _read_state_value(path: Path, key: str) -> str | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    value = data.get(key)
    return str(value) if value else None


def _add_issue(
    *,
    issues: list[QualityIssue],
    dataset: str,
    severity: Literal["info", "warn", "error"],
    code: str,
    message: str,
    suggestion: str | None = None,
) -> None:
    issues.append(
        QualityIssue(
            dataset=dataset,
            severity=severity,
            code=code,
            message=message,
            suggestion=suggestion,
        )
    )


def _load_dataframe_sample(path: Path, *, usecols: list[str] | None, sample_rows: int) -> tuple[object | None, str | None]:
    """Return a small pandas DataFrame sample or an error string (kept generic for UI diagnostics)."""

    if not path.exists():
        return None, "missing"
    try:
        import pandas as pd  # local import keeps API server startup lightweight

        df = pd.read_csv(path, usecols=usecols, nrows=int(sample_rows)) if usecols else pd.read_csv(path, nrows=int(sample_rows))
        return df, None
    except Exception as exc:
        return None, str(exc)


def _quality_for_segments(path: Path, *, sample_rows: int) -> DatasetQuality:
    dataset = "segments"
    quality = DatasetQuality(dataset=dataset, path=str(path), exists=path.exists(), sample_rows=int(sample_rows))
    quality.stats["schema_version"] = int(SCHEMA_VERSIONS.get("segments", 1))
    df, err = _load_dataframe_sample(path, usecols=["segment_id", "lat", "lon"], sample_rows=sample_rows)
    if err:
        if err == "missing":
            _add_issue(
                issues=quality.issues,
                dataset=dataset,
                severity="error",
                code="missing_dataset",
                message="segments dataset not found.",
                suggestion="Run scripts/build_dataset.py to generate data/processed/segments.csv.",
            )
        else:
            _add_issue(
                issues=quality.issues,
                dataset=dataset,
                severity="error",
                code="read_failed",
                message=f"Failed to read segments sample: {err}",
                suggestion="Confirm the CSV is valid and readable by pandas.",
            )
        return quality

    import pandas as pd

    if df is None or df.empty:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="warn",
            code="empty_dataset",
            message="segments dataset is empty (sample read returned 0 rows).",
            suggestion="Confirm ingestion ran successfully and the metadata feed is enabled.",
        )
        return quality

    missing_cols = [c for c in ["segment_id", "lat", "lon"] if c not in df.columns]
    if missing_cols:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="error",
            code="missing_columns",
            message=f"segments missing required columns: {', '.join(missing_cols)}",
            suggestion="Rebuild segments with scripts/build_dataset.py.",
        )
        return quality

    df["segment_id"] = df["segment_id"].astype(str)
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    missing_ids = int(df["segment_id"].isna().sum())
    missing_geo = int(df["lat"].isna().sum() + df["lon"].isna().sum())
    quality.stats.update(
        {
            "missing_segment_id_sample": missing_ids,
            "missing_lat_lon_sample": missing_geo,
        }
    )
    if missing_geo > 0:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="warn",
            code="missing_geometry",
            message=f"{missing_geo} rows in the sample have missing lat/lon.",
            suggestion="Confirm the upstream metadata contains coordinates and that parsing is correct.",
        )
    return quality


def _quality_for_observations(path: Path, *, dataset: str, sample_rows: int, segment_ids: set[str] | None) -> DatasetQuality:
    quality = DatasetQuality(dataset=dataset, path=str(path), exists=path.exists(), sample_rows=int(sample_rows))
    quality.stats["schema_version"] = int(SCHEMA_VERSIONS.get("observations", 1))
    df, err = _load_dataframe_sample(path, usecols=["timestamp", "segment_id", "speed_kph"], sample_rows=sample_rows)
    if err:
        if err == "missing":
            _add_issue(
                issues=quality.issues,
                dataset=dataset,
                severity="error",
                code="missing_dataset",
                message=f"{dataset} dataset not found.",
                suggestion="Run ingestion + scripts/build_dataset.py to generate processed observations.",
            )
        else:
            _add_issue(
                issues=quality.issues,
                dataset=dataset,
                severity="error",
                code="read_failed",
                message=f"Failed to read observations sample: {err}",
                suggestion="Confirm the CSV is valid and readable by pandas.",
            )
        return quality

    import pandas as pd

    if df is None or df.empty:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="warn",
            code="empty_dataset",
            message=f"{dataset} is empty (sample read returned 0 rows).",
            suggestion="Run ingestion to fetch data and re-run scripts/build_dataset.py.",
        )
        return quality

    missing_cols = [c for c in ["timestamp", "segment_id", "speed_kph"] if c not in df.columns]
    if missing_cols:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="error",
            code="missing_columns",
            message=f"{dataset} missing required columns: {', '.join(missing_cols)}",
            suggestion="Rebuild observations with scripts/build_dataset.py (and scripts/aggregate_observations.py if enabled).",
        )
        return quality

    df["segment_id"] = df["segment_id"].astype(str)
    df["speed_kph"] = pd.to_numeric(df["speed_kph"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    invalid_ts = int(df["timestamp"].isna().sum())
    dupe_keys = int(df.dropna(subset=["timestamp", "segment_id"]).duplicated(subset=["timestamp", "segment_id"]).sum())
    sentinel_speed = int((df["speed_kph"] == -99).sum())
    negative_speed = int((df["speed_kph"] < 0).sum())
    extreme_speed = int((df["speed_kph"] > 200).sum())
    missing_speed = int(df["speed_kph"].isna().sum())
    cleaned, clean_stats = clean_observations(df)
    quality.stats.update(
        {
            "invalid_timestamp_sample": invalid_ts,
            "duplicate_timestamp_segment_sample": dupe_keys,
            "sentinel_speed_minus_99_sample": sentinel_speed,
            "negative_speed_sample": negative_speed,
            "extreme_speed_gt_200_sample": extreme_speed,
            "missing_speed_sample": missing_speed,
            "cleaned_output_rows_sample": int(len(cleaned)),
            "cleaned_dropped_missing_keys_sample": int(clean_stats.dropped_missing_keys),
            "cleaned_dropped_invalid_speed_sample": int(clean_stats.dropped_invalid_speed),
            "cleaned_dropped_duplicates_sample": int(clean_stats.dropped_duplicates),
        }
    )
    if invalid_ts > 0:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="warn",
            code="invalid_timestamps",
            message=f"{invalid_ts} rows in the sample have invalid timestamps.",
            suggestion="Confirm timestamps are ISO 8601 and that preprocessing preserves them.",
        )
    if sentinel_speed > 0:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="warn",
            code="sentinel_speed",
            message=f"{sentinel_speed} rows in the sample have speed_kph=-99 sentinel values.",
            suggestion="Treat sentinel values as missing (drop or convert to null) during ingestion/preprocessing.",
        )
    if dupe_keys > 0:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="warn",
            code="duplicate_keys",
            message=f"{dupe_keys} rows in the sample have duplicate (timestamp, segment_id) keys.",
            suggestion="Run scripts/compact_observations.py for this granularity to dedupe in-place.",
        )
    if negative_speed > 0 or extreme_speed > 0:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="warn",
            code="speed_out_of_range",
            message=f"Sample contains out-of-range speeds (negative={negative_speed}, >200kph={extreme_speed}).",
            suggestion="Clamp/drop invalid speeds in preprocessing to avoid misleading rankings.",
        )

    if segment_ids is not None and segment_ids:
        unknown_segments = int((~df["segment_id"].isin(segment_ids)).sum())
        quality.stats["unknown_segment_id_sample"] = unknown_segments
        if unknown_segments > 0:
            _add_issue(
                issues=quality.issues,
                dataset=dataset,
                severity="warn",
                code="unknown_segment_ids",
                message=f"{unknown_segments} rows in the sample refer to segment_id values missing from segments.csv.",
                suggestion="Ensure segments metadata and observations are from the same source/version.",
            )

    return quality


def _quality_for_events(path: Path, *, sample_rows: int) -> DatasetQuality:
    dataset = "events"
    quality = DatasetQuality(dataset=dataset, path=str(path), exists=path.exists(), sample_rows=int(sample_rows))
    quality.stats["schema_version"] = int(SCHEMA_VERSIONS.get("events", 1))
    df, err = _load_dataframe_sample(path, usecols=["event_id", "start_time", "lat", "lon"], sample_rows=sample_rows)
    if err:
        if err == "missing":
            _add_issue(
                issues=quality.issues,
                dataset=dataset,
                severity="warn",
                code="missing_dataset",
                message="events dataset not found.",
                suggestion="Run scripts/build_events.py to generate data/processed/events.csv (optional feature).",
            )
        else:
            _add_issue(
                issues=quality.issues,
                dataset=dataset,
                severity="error",
                code="read_failed",
                message=f"Failed to read events sample: {err}",
                suggestion="Confirm the CSV is valid and readable by pandas.",
            )
        return quality

    import pandas as pd

    if df is None or df.empty:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="info",
            code="empty_dataset",
            message="events dataset is empty (sample read returned 0 rows).",
            suggestion="This is expected if you haven't built events yet.",
        )
        return quality

    missing_cols = [c for c in ["event_id", "start_time"] if c not in df.columns]
    if missing_cols:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="error",
            code="missing_columns",
            message=f"events missing required columns: {', '.join(missing_cols)}",
            suggestion="Rebuild events with scripts/build_events.py.",
        )
        return quality

    df["event_id"] = df["event_id"].astype(str)
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    invalid_ts = int(df["start_time"].isna().sum())
    quality.stats["invalid_start_time_sample"] = invalid_ts
    if invalid_ts > 0:
        _add_issue(
            issues=quality.issues,
            dataset=dataset,
            severity="warn",
            code="invalid_timestamps",
            message=f"{invalid_ts} rows in the sample have invalid start_time values.",
            suggestion="Confirm event timestamps are parseable ISO 8601 strings.",
        )
    return quality


@router.get("/ui/quality", response_model=UiQualityReport)
def ui_quality(
    sample_rows: int = Query(default=20000, ge=100, le=200000),
    minutes: int | None = Query(default=None, ge=1, description="Restrict to a specific observations granularity."),
) -> UiQualityReport:
    config = get_config()
    processed_dir = config.paths.processed_dir

    minutes_candidates = dataset_minutes_candidates(config)
    if minutes is not None:
        minutes_candidates = [int(minutes)]

    segments_path = processed_dir / "segments.csv"
    segment_ids: set[str] | None = None
    seg_df, seg_err = _load_dataframe_sample(segments_path, usecols=["segment_id"], sample_rows=200000)
    if seg_err is None and seg_df is not None and not seg_df.empty and "segment_id" in seg_df.columns:
        try:
            segment_ids = set(seg_df["segment_id"].astype(str).dropna().tolist())
        except Exception:
            segment_ids = None

    datasets: list[DatasetQuality] = []
    datasets.append(_quality_for_segments(segments_path, sample_rows=sample_rows))
    for m in minutes_candidates:
        obs_path = observations_csv_path(processed_dir, int(m))
        datasets.append(
            _quality_for_observations(
                obs_path,
                dataset=f"observations_{int(m)}min",
                sample_rows=sample_rows,
                segment_ids=segment_ids,
            )
        )
    datasets.append(_quality_for_events(processed_dir / "events.csv", sample_rows=sample_rows))

    all_issues: list[QualityIssue] = []
    for d in datasets:
        all_issues.extend(d.issues)

    # Stable ordering helps the UI diff/export this report.
    all_issues = sorted(all_issues, key=lambda x: (x.severity, x.dataset, x.code))

    fix_commands: list[str] = []
    needs_observations_compact = any(
        issue.code in {"sentinel_speed", "speed_out_of_range", "duplicate_keys"} and issue.dataset.startswith("observations_")
        for issue in all_issues
    )
    if needs_observations_compact:
        existing_minutes: list[int] = []
        for d in datasets:
            if not d.dataset.startswith("observations_") or not d.exists:
                continue
            try:
                existing_minutes.append(int(d.dataset.split("_", 1)[1].replace("min", "")))
            except Exception:
                continue
        for m in sorted(set(existing_minutes)):
            fix_commands.append(f"python scripts/compact_observations.py --minutes {m}")
    fix_commands.append("python scripts/validate_data.py")
    fix_commands.append("python scripts/materialize_defaults.py")

    return UiQualityReport(
        generated_at_utc=datetime.now(timezone.utc),
        sample_rows=int(sample_rows),
        schema_versions={k: int(v) for k, v in SCHEMA_VERSIONS.items()},
        datasets=datasets,
        issues=all_issues,
        fix_commands=fix_commands,
    )

@router.get("/ui/status", response_model=UiStatus)
def ui_status() -> UiStatus:
    config = get_config()

    minutes_candidates = dataset_minutes_candidates(config)

    processed_dir = config.paths.processed_dir
    parquet_dir = config.warehouse.parquet_dir

    available: list[int] = []
    last_ts: datetime | None = None
    for minutes in minutes_candidates:
        csv_path = observations_csv_path(processed_dir, minutes)
        parquet_path = observations_parquet_path(parquet_dir, minutes)
        if not csv_path.exists() and not parquet_path.exists():
            continue
        available.append(minutes)
        ts = _max_observation_timestamp(minutes)
        if ts is not None and (last_ts is None or ts > last_ts):
            last_ts = ts

    cache_dir = config.paths.cache_dir
    live_loop_last_snapshot_timestamp = _read_state_value(cache_dir / "live_loop_state.json", "last_snapshot_timestamp")
    daily_backfill_last_date = _read_state_value(cache_dir / "daily_backfill_state.json", "last_backfill_date")
    ingest_status_path = cache_dir / "ingest_status.json"
    ingest_ledger_path = cache_dir / "ingest_ledger.jsonl"
    last_ingest_ok: bool | None = None
    updated_files: list[str] = []
    last_error = None
    ingest_rate_limit: dict[str, float | int | None] = {}
    if ingest_status_path.exists():
        try:
            ingest_data = json.loads(ingest_status_path.read_text(encoding="utf-8"))
        except Exception:
            ingest_data = None
        if isinstance(ingest_data, dict):
            ok = ingest_data.get("last_ingest_ok")
            if isinstance(ok, bool):
                last_ingest_ok = ok
            err = ingest_data.get("last_error")
            if err:
                last_error = str(err)
            err_code = ingest_data.get("last_error_code")
            last_error_code = str(err_code) if err_code else None
            err_kind = ingest_data.get("last_error_kind")
            last_error_kind = str(err_kind) if err_kind else None
            cf = ingest_data.get("consecutive_failures")
            ingest_consecutive_failures = int(cf) if isinstance(cf, (int, float)) else None
            bs = ingest_data.get("backoff_seconds")
            ingest_backoff_seconds = int(bs) if isinstance(bs, (int, float)) else None
            lsu = ingest_data.get("last_success_utc")
            ingest_last_success_utc = str(lsu) if lsu else None
            files = ingest_data.get("updated_files")
            if isinstance(files, list):
                updated_files = [str(x) for x in files if x]
            quality = ingest_data.get("quality")
            if isinstance(quality, dict):
                ingest_quality = {str(k): int(v) for k, v in quality.items() if isinstance(v, (int, float))}
            else:
                ingest_quality = {}
            rate_limit = ingest_data.get("rate_limit")
            if isinstance(rate_limit, dict):
                ingest_rate_limit = {str(k): v for k, v in rate_limit.items()}
        else:
            ingest_quality = {}
            last_error_code = None
            last_error_kind = None
            ingest_consecutive_failures = None
            ingest_backoff_seconds = None
            ingest_last_success_utc = None
            ingest_rate_limit = {}
    else:
        ingest_quality = {}
        last_error_code = None
        last_error_kind = None
        ingest_consecutive_failures = None
        ingest_backoff_seconds = None
        ingest_last_success_utc = None
        ingest_rate_limit = {}

    return UiStatus(
        generated_at_utc=datetime.now(timezone.utc),
        observations_minutes_available=available,
        observations_last_timestamp_utc=last_ts,
        dataset_version=dataset_version_from_paths(processed_dir, parquet_dir, minutes_candidates, cache_dir=config.paths.cache_dir),
        live_loop_last_snapshot_timestamp=live_loop_last_snapshot_timestamp,
        daily_backfill_last_date=daily_backfill_last_date,
        last_error=last_error,
        last_error_code=last_error_code,
        last_error_kind=last_error_kind,
        last_ingest_ok=last_ingest_ok,
        updated_files=updated_files,
        ingest_ledger_latest=read_latest_ledger_entry(ingest_ledger_path),
        ingest_quality=ingest_quality,
        ingest_consecutive_failures=ingest_consecutive_failures,
        ingest_backoff_seconds=ingest_backoff_seconds,
        ingest_last_success_utc=ingest_last_success_utc,
        ingest_rate_limit=ingest_rate_limit,
    )


@router.get("/ui/diagnostics", response_model=UiDiagnostics)
def ui_diagnostics() -> UiDiagnostics:
    config = get_config()
    processed_dir = config.paths.processed_dir
    parquet_dir = config.warehouse.parquet_dir
    cache_dir = config.paths.cache_dir

    corridors_csv = config.analytics.corridors.corridors_csv
    segments_csv = processed_dir / "segments.csv"
    events_csv = processed_dir / "events.csv"
    weather_csv = processed_dir / "weather_observations.csv"
    materialized_defaults = cache_dir / "materialized_defaults.json"
    congestion_alerts = cache_dir / "congestion_alerts.csv"
    event_hotspot_links = cache_dir / "event_hotspot_links.csv"

    obs_files: list[DatasetFileInfo] = []
    for path in sorted(processed_dir.glob("observations_*min.csv")):
        obs_files.append(_file_info(path))

    baseline_files: list[DatasetFileInfo] = []
    for path in sorted(cache_dir.glob("baselines_speed_*.csv")):
        baseline_files.append(_file_info(path))

    segment_quality_files: list[DatasetFileInfo] = []
    for path in sorted(cache_dir.glob("segment_quality_*.csv")):
        segment_quality_files.append(_file_info(path))

    return UiDiagnostics(
        generated_at_utc=datetime.now(timezone.utc),
        processed_dir=str(processed_dir),
        parquet_dir=str(parquet_dir),
        corridors_csv=str(corridors_csv),
        corridors_csv_exists=corridors_csv.exists(),
        segments_csv=_file_info(segments_csv),
        observations_csv_files=obs_files,
        events_csv=_file_info(events_csv),
        weather_csv=_file_info(weather_csv),
        materialized_defaults=_file_info(materialized_defaults),
        baselines_speed_files=baseline_files,
        segment_quality_files=segment_quality_files,
        congestion_alerts=_file_info(congestion_alerts),
        event_hotspot_links=_file_info(event_hotspot_links),
        cache_dir=str(cache_dir),
        live_loop_state=_file_info(cache_dir / "live_loop_state.json"),
        backfill_checkpoint=_file_info(cache_dir / "backfill_checkpoint.json"),
    )


def _tail_lines(path: Path, *, max_lines: int) -> list[str]:
    if not path.exists() or max_lines <= 0:
        return []
    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            end = f.tell()
            chunk = 8192
            buf = b""
            pos = end
            while pos > 0 and buf.count(b"\n") <= max_lines:
                read = min(chunk, pos)
                pos -= read
                f.seek(pos)
                buf = f.read(read) + buf
            lines = [ln.decode("utf-8", errors="ignore").rstrip("\n") for ln in buf.splitlines()]
            return lines[-int(max_lines) :]
    except Exception:
        return []

_ALERT_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T[^\s]+)\s+ok=(?P<ok>True|False)\s+code=(?P<code>[^\s]+)\s+msg=(?P<msg>.*)$"
)


def _parse_alert_line(line: str) -> dict[str, object] | None:
    m = _ALERT_RE.match(line.strip())
    if not m:
        return None
    ts_raw = m.group("ts")
    try:
        ts = to_utc(parse_datetime(ts_raw))
    except Exception:
        return None
    ok = m.group("ok") == "True"
    code = m.group("code")
    msg = m.group("msg").strip()
    return {"timestamp_utc": ts, "ok": ok, "code": code, "message": msg}


def _alert_severity(code: str, ok: bool) -> str:
    if ok:
        return "info"
    if code in {"stale", "no_data"}:
        return "warn"
    if code in {"http_429", "rate_limited"}:
        return "warn"
    if code in {"api_unreachable", "timeout", "connect_error"}:
        return "error"
    return "error"


def _alert_category(code: str) -> str:
    if code in {"api_unreachable", "timeout", "connect_error"}:
        return "network"
    if code in {"http_429", "rate_limited"}:
        return "rate_limit"
    if code in {"no_data", "stale"}:
        return "data"
    return "other"


def _hour_bucket(dt: datetime) -> datetime:
    value = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return value.replace(minute=0, second=0, microsecond=0)


def _iter_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    out: list[dict[str, object]] = []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except Exception:
                    continue
                if isinstance(parsed, dict):
                    out.append(parsed)
    except Exception:
        return []
    return out


class UiEventHotspotLink(BaseModel):
    event_id: str
    segment_id: str
    score: float | None = None
    reason: str | None = None


@router.get("/ui/alerts", response_model=UiAlerts)
def ui_alerts(
    tail: int = Query(default=200, ge=1, le=5000),
    window_hours: int = Query(default=24, ge=1, le=168),
) -> UiAlerts:
    config = get_config()
    path = config.paths.cache_dir / "alerts.log"
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=int(window_hours))

    by_code: dict[str, int] = defaultdict(int)
    by_severity: dict[str, int] = defaultdict(int)
    by_category: dict[str, int] = defaultdict(int)
    buckets: dict[datetime, dict[str, object]] = {}

    if path.exists():
        try:
            for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                rec = _parse_alert_line(raw)
                if rec is None:
                    continue
                ts = rec["timestamp_utc"]
                if not isinstance(ts, datetime):
                    continue
                if ts < since:
                    continue
                ok = bool(rec.get("ok"))
                code = str(rec.get("code") or "unknown")
                severity = _alert_severity(code, ok)
                category = _alert_category(code)

                by_code[code] += 1
                by_severity[severity] += 1
                by_category[category] += 1

                hour = _hour_bucket(ts)
                bucket = buckets.get(hour)
                if bucket is None:
                    bucket = {"hour_start_utc": hour.isoformat(), "total": 0, "errors": 0, "warn": 0, "info": 0}
                    buckets[hour] = bucket
                bucket["total"] = int(bucket.get("total", 0)) + 1
                bucket[severity] = int(bucket.get(severity, 0)) + 1
                if not ok:
                    bucket["errors"] = int(bucket.get("errors", 0)) + 1
        except Exception:
            pass

    return UiAlerts(
        generated_at_utc=datetime.now(timezone.utc),
        path=str(path),
        lines=_tail_lines(path, max_lines=int(tail)),
        summary={
            "window_hours": int(window_hours),
            "since_utc": since.isoformat(),
            "by_code": dict(sorted(by_code.items(), key=lambda kv: (-kv[1], kv[0]))),
            "by_severity": dict(sorted(by_severity.items(), key=lambda kv: (-kv[1], kv[0]))),
            "by_category": dict(sorted(by_category.items(), key=lambda kv: (-kv[1], kv[0]))),
        },
        buckets=[buckets[k] for k in sorted(buckets.keys())],
    )


@router.get("/ui/event_hotspot_links", response_model=ItemsResponse[UiEventHotspotLink])
def ui_event_hotspot_links(
    event_id: str = Query(..., description="Event identifier to lookup."),
    limit: int = Query(default=200, ge=1, le=2000),
) -> ItemsResponse[UiEventHotspotLink]:
    config = get_config()
    path = config.paths.cache_dir / "event_hotspot_links.csv"
    if not path.exists():
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.MISSING_DATASET,
                message="event_hotspot_links.csv not found.",
                suggestion="Run scripts/build_events_all.py and scripts/build_event_links_and_alerts.py (or wait for the processing timer).",
            ),
        )
    try:
        import pandas as pd  # local import keeps API startup light

        from trafficpulse.storage.datasets import load_csv
    except Exception:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_EVENT_LINKS,
                message="Server is missing optional dependencies to read CSV files.",
                suggestion="Install pandas or enable warehouse/DuckDB path.",
            ),
        )
    try:
        df = load_csv(path)
    except Exception:
        df = None
    if df is None or getattr(df, "empty", True):
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_EVENT_LINKS,
                message="No event-hotspot links available.",
                suggestion="Ensure events.csv exists and that materialized hotspots snapshot was generated.",
            ),
        )
    if "event_id" not in df.columns or "segment_id" not in df.columns:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_EVENT_LINKS,
                message="event_hotspot_links.csv missing required columns.",
                suggestion="Re-run scripts/build_event_links_and_alerts.py.",
            ),
        )
    df = df[df["event_id"].astype(str) == str(event_id)]
    if df.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_EVENT_LINKS,
                message="No linked hotspots found for this event.",
                suggestion="Try a wider time window (materialize_defaults) or confirm event coordinates are present.",
            ),
        )
    df = df.copy()
    df["event_id"] = df["event_id"].astype(str)
    df["segment_id"] = df["segment_id"].astype(str)
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df = df.sort_values(["score"], ascending=False) if "score" in df.columns else df
    df = df.head(int(limit))
    df = df.astype(object).where(pd.notnull(df), None)
    items = [UiEventHotspotLink(**rec) for rec in df.to_dict(orient="records")]
    return ItemsResponse(items=items)


@router.get("/ui/trends", response_model=UiTrends)
def ui_trends(window_hours: int = Query(default=24, ge=1, le=168)) -> UiTrends:
    config = get_config()
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=int(window_hours))

    ingest_ledger_path = config.paths.cache_dir / "ingest_ledger.jsonl"
    rate_limit_ledger_path = config.paths.cache_dir / "rate_limit_ledger.jsonl"

    buckets: dict[datetime, dict[str, object]] = {}
    error_codes: dict[str, int] = defaultdict(int)

    def get_bucket(dt: datetime) -> dict[str, object]:
        hour = _hour_bucket(dt)
        b = buckets.get(hour)
        if b is None:
            b = {
                "hour_start_utc": hour.isoformat(),
                "vd_ok": 0,
                "vd_error": 0,
                "max_backoff_seconds": 0,
                "max_consecutive_failures": 0,
                "rate_limit_429": 0,
                "avg_retry_after_seconds": None,
                "avg_adaptive_throttle_seconds": None,
            }
            buckets[hour] = b
        return b

    # Ingest ledger: ok/error counts + backoff/failures.
    for entry in _iter_jsonl(ingest_ledger_path):
        ts_raw = entry.get("generated_at_utc")
        if not ts_raw:
            continue
        try:
            ts = to_utc(parse_datetime(str(ts_raw)))
        except Exception:
            continue
        if ts < since:
            continue
        if str(entry.get("runner") or "") != "live_loop":
            continue
        if str(entry.get("source") or "") != "vd":
            continue
        b = get_bucket(ts)
        ok = bool(entry.get("ok"))
        if ok:
            b["vd_ok"] = int(b.get("vd_ok", 0)) + 1
        else:
            b["vd_error"] = int(b.get("vd_error", 0)) + 1
            code = str(entry.get("error_code") or "unknown")
            error_codes[code] += 1
            backoff = entry.get("backoff_seconds")
            if isinstance(backoff, (int, float)):
                b["max_backoff_seconds"] = max(int(b.get("max_backoff_seconds", 0)), int(backoff))
            cf = entry.get("consecutive_failures")
            if isinstance(cf, (int, float)):
                b["max_consecutive_failures"] = max(int(b.get("max_consecutive_failures", 0)), int(cf))

    # Rate-limit ledger: 429 counts per hour (+ avg retry-after).
    retry_after_sum: dict[datetime, float] = defaultdict(float)
    retry_after_n: dict[datetime, int] = defaultdict(int)
    throttle_sum: dict[datetime, float] = defaultdict(float)
    throttle_n: dict[datetime, int] = defaultdict(int)

    for entry in _iter_jsonl(rate_limit_ledger_path):
        ts_raw = entry.get("generated_at_utc")
        if not ts_raw:
            continue
        try:
            ts = to_utc(parse_datetime(str(ts_raw)))
        except Exception:
            continue
        if ts < since:
            continue
        b = get_bucket(ts)
        b["rate_limit_429"] = int(b.get("rate_limit_429", 0)) + 1
        ra = entry.get("retry_after_seconds")
        if isinstance(ra, (int, float)):
            hour = _hour_bucket(ts)
            retry_after_sum[hour] += float(ra)
            retry_after_n[hour] += 1
        thr = entry.get("adaptive_min_interval_seconds")
        if isinstance(thr, (int, float)):
            hour = _hour_bucket(ts)
            throttle_sum[hour] += float(thr)
            throttle_n[hour] += 1

    for hour, b in list(buckets.items()):
        if retry_after_n.get(hour, 0) > 0:
            b["avg_retry_after_seconds"] = retry_after_sum[hour] / float(retry_after_n[hour])
        if throttle_n.get(hour, 0) > 0:
            b["avg_adaptive_throttle_seconds"] = throttle_sum[hour] / float(throttle_n[hour])

    buckets_list = [buckets[k] for k in sorted(buckets.keys())]
    summary = {
        "window_hours": int(window_hours),
        "since_utc": since.isoformat(),
        "vd_ok_total": int(sum(int(b.get("vd_ok", 0)) for b in buckets_list)),
        "vd_error_total": int(sum(int(b.get("vd_error", 0)) for b in buckets_list)),
        "rate_limit_429_total": int(sum(int(b.get("rate_limit_429", 0)) for b in buckets_list)),
        "max_backoff_seconds_24h": int(max((int(b.get("max_backoff_seconds", 0)) for b in buckets_list), default=0)),
        "max_consecutive_failures_24h": int(max((int(b.get("max_consecutive_failures", 0)) for b in buckets_list), default=0)),
        "error_codes": dict(sorted(error_codes.items(), key=lambda kv: (-kv[1], kv[0]))),
    }
    return UiTrends(generated_at_utc=now, window_hours=int(window_hours), buckets=buckets_list, summary=summary)


@router.get("/ui/weather/latest", response_model=UiWeatherLatest)
def ui_weather_latest(
    city: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=200),
) -> UiWeatherLatest:
    config = get_config()
    path = config.paths.processed_dir / "weather_observations.csv"
    if not path.exists():
        return UiWeatherLatest(generated_at_utc=datetime.now(timezone.utc), items=[])

    try:
        import pandas as pd

        df = pd.read_csv(path)
    except Exception:
        return UiWeatherLatest(generated_at_utc=datetime.now(timezone.utc), items=[])

    if df.empty or "timestamp" not in df.columns or "city" not in df.columns:
        return UiWeatherLatest(generated_at_utc=datetime.now(timezone.utc), items=[])

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp", "city"])
    df["city"] = df["city"].astype(str)
    if city:
        df = df[df["city"] == str(city)]
    if df.empty:
        return UiWeatherLatest(generated_at_utc=datetime.now(timezone.utc), items=[])

    df = df.sort_values(["city", "timestamp"]).groupby("city", as_index=False).tail(1)
    df = df.sort_values("timestamp", ascending=False).head(int(limit))
    keep = ["timestamp", "city", "rain_mm", "wind_mps", "visibility_km", "temperature_c", "humidity_pct", "source"]
    for col in keep:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[keep].astype(object).where(pd.notnull(df), None)

    items: list[UiWeatherRow] = []
    for rec in df.to_dict(orient="records"):
        items.append(UiWeatherRow(**rec))
    return UiWeatherLatest(generated_at_utc=datetime.now(timezone.utc), items=items)


@router.get("/stream/status")
def stream_status(
    interval_seconds: int = Query(default=5, ge=1, le=60),
    max_events: int | None = Query(default=None, ge=1, le=1000),
) -> StreamingResponse:
    async def event_stream():
        sent = 0
        while True:
            status = ui_status().model_dump(mode="json")
            payload = json.dumps(status, ensure_ascii=False)
            yield f"data: {payload}\n\n".encode("utf-8")
            sent += 1
            if max_events is not None and sent >= int(max_events):
                return
            await asyncio.sleep(float(interval_seconds))

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/ui/settings", response_model=UiSettings)
def ui_settings() -> UiSettings:
    """Return the server-side default settings for the dashboard."""

    # Load the current config (configs/config.yaml, or fallback to configs/config.example.yaml).
    config = get_config()

    # Build a strongly-typed reliability settings object so the JSON shape is stable and explicit.
    reliability = ReliabilitySettings(
        # We coerce to primitives to ensure JSON serialization stays predictable across sources (YAML/env).
        congestion_speed_threshold_kph=float(config.analytics.reliability.congestion_speed_threshold_kph),
        min_samples=int(config.analytics.reliability.min_samples),
        weights={
            # Explicit keys allow the frontend to map weights to UI fields without guessing.
            "mean_speed": float(config.analytics.reliability.weights.mean_speed),
            "speed_std": float(config.analytics.reliability.weights.speed_std),
            "congestion_frequency": float(config.analytics.reliability.weights.congestion_frequency),
        },
    )

    # Build anomaly defaults; these control how the rolling z-score detector behaves.
    anomalies = AnomaliesSettings(
        method=str(config.analytics.anomalies.method),
        window_points=int(config.analytics.anomalies.window_points),
        z_threshold=float(config.analytics.anomalies.z_threshold),
        # Config stores a string; the API contract restricts it to a known Literal union for safety.
        direction=str(config.analytics.anomalies.direction),  # type: ignore[arg-type]
        max_gap_minutes=int(config.analytics.anomalies.max_gap_minutes),
        min_event_points=int(config.analytics.anomalies.min_event_points),
    )

    # Build event impact defaults; these control baseline windows and recovery definitions.
    impact = EventImpactSettings(
        default_window_hours=int(config.analytics.event_impact.default_window_hours),
        radius_meters=float(config.analytics.event_impact.radius_meters),
        max_segments=int(config.analytics.event_impact.max_segments),
        baseline_window_minutes=int(config.analytics.event_impact.baseline_window_minutes),
        end_time_fallback_minutes=int(config.analytics.event_impact.end_time_fallback_minutes),
        recovery_horizon_minutes=int(config.analytics.event_impact.recovery_horizon_minutes),
        recovery_ratio=float(config.analytics.event_impact.recovery_ratio),
        # Same idea as anomalies.direction: return a constrained enum-like string to the frontend.
        speed_weighting=str(config.analytics.event_impact.speed_weighting),  # type: ignore[arg-type]
        min_baseline_points=int(config.analytics.event_impact.min_baseline_points),
        min_event_points=int(config.analytics.event_impact.min_event_points),
    )

    # Return a single document with the defaults the UI needs to render placeholders and reset actions.
    return UiSettings(
        # Preprocessing values are needed to explain which granularities exist (5m source -> 15m target).
        preprocessing={
            "source_granularity_minutes": int(config.preprocessing.source_granularity_minutes),
            "target_granularity_minutes": int(config.preprocessing.target_granularity_minutes),
        },
        # `model_dump()` converts nested Pydantic models into plain dicts for JSON responses.
        analytics={
            "reliability": reliability.model_dump(),
            "anomalies": anomalies.model_dump(),
            "event_impact": impact.model_dump(),
        },
        # Warehouse flags let the UI display whether results come from CSV fallback or DuckDB+Parquet.
        warehouse={
            "enabled": bool(config.warehouse.enabled),
            "use_duckdb": bool(config.warehouse.use_duckdb),
        },
        # These enums let the frontend populate dropdowns without hard-coding the allowed values.
        enums={
            "anomalies_direction": ["low", "high", "both"],
            "event_impact_speed_weighting": ["volume", "equal"],
        },
    )
