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
from datetime import datetime, timezone
from pathlib import Path
# We use Literal to constrain certain UI-controlled strings to a small, known set of values,
# which behaves like an enum and prevents accidental typos from silently changing behavior.
from typing import Literal

# FastAPI's APIRouter lets us group related endpoints into a module and mount them in the main app.
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
# Pydantic models define the JSON schema sent to the browser, which makes the API contract explicit.
from pydantic import BaseModel, Field

# The config is the single source of truth for all tunable parameters (thresholds, weights, windows).
from trafficpulse.settings import get_config
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
    cache_dir: str
    live_loop_state: DatasetFileInfo
    backfill_checkpoint: DatasetFileInfo


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


@router.get("/ui/status", response_model=UiStatus)
def ui_status() -> UiStatus:
    config = get_config()

    minutes_candidates = sorted(
        {
            int(config.preprocessing.source_granularity_minutes),
            int(config.preprocessing.target_granularity_minutes),
            60,
        }
    )

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

    return UiStatus(
        generated_at_utc=datetime.now(timezone.utc),
        observations_minutes_available=available,
        observations_last_timestamp_utc=last_ts,
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

    obs_files: list[DatasetFileInfo] = []
    for path in sorted(processed_dir.glob("observations_*min.csv")):
        obs_files.append(_file_info(path))

    return UiDiagnostics(
        generated_at_utc=datetime.now(timezone.utc),
        processed_dir=str(processed_dir),
        parquet_dir=str(parquet_dir),
        corridors_csv=str(corridors_csv),
        corridors_csv_exists=corridors_csv.exists(),
        segments_csv=_file_info(segments_csv),
        observations_csv_files=obs_files,
        events_csv=_file_info(events_csv),
        cache_dir=str(cache_dir),
        live_loop_state=_file_info(cache_dir / "live_loop_state.json"),
        backfill_checkpoint=_file_info(cache_dir / "backfill_checkpoint.json"),
    )


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
