from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from trafficpulse.analytics.event_impact import (
    apply_event_impact_overrides,
    compute_event_impact,
    event_impact_spec_from_config,
    select_nearby_segments,
)
from trafficpulse.ingestion.schemas import TrafficEvent
from trafficpulse.settings import get_config
from trafficpulse.storage.backend import duckdb_backend
from trafficpulse.storage.datasets import (
    events_csv_path,
    events_parquet_path,
    load_csv,
    load_parquet,
    observations_csv_path,
    observations_parquet_path,
    segments_csv_path,
    segments_parquet_path,
)
from trafficpulse.utils.time import to_utc


router = APIRouter()


class AffectedSegment(BaseModel):
    segment_id: str
    distance_m: float
    lat: float
    lon: float


class ImpactPoint(BaseModel):
    timestamp: datetime
    speed_kph: Optional[float] = None
    volume: Optional[float] = None
    occupancy_pct: Optional[float] = None


class EventImpactSummary(BaseModel):
    event: TrafficEvent
    analysis_window_start: datetime
    analysis_window_end: datetime
    n_segments: int
    baseline_n_points: int
    event_n_points: int
    baseline_mean_speed_kph: Optional[float] = None
    baseline_std_speed_kph: Optional[float] = None
    event_mean_speed_kph: Optional[float] = None
    event_min_speed_kph: Optional[float] = None
    event_min_time: Optional[datetime] = None
    speed_delta_mean_kph: Optional[float] = None
    speed_ratio_mean: Optional[float] = None
    recovered_at: Optional[datetime] = None
    recovery_minutes: Optional[float] = None
    enough_baseline: bool
    enough_event: bool
    affected_segments: list[AffectedSegment] = Field(default_factory=list)
    timeseries: Optional[list[ImpactPoint]] = None


def _load_event_or_404(event_id: str) -> pd.Series:
    config = get_config()
    processed_dir = config.paths.processed_dir
    parquet_dir = config.warehouse.parquet_dir
    csv_path = events_csv_path(processed_dir)
    parquet_path = events_parquet_path(parquet_dir)
    backend = duckdb_backend(config)

    df: pd.DataFrame
    if backend is not None and parquet_path.exists():
        df = backend.query_event_by_id(str(event_id))
    elif config.warehouse.enabled and parquet_path.exists():
        df = load_parquet(parquet_path)
        df = df[df.get("event_id").astype(str) == str(event_id)] if "event_id" in df.columns else df.iloc[0:0]
    else:
        if not csv_path.exists():
            raise HTTPException(
                status_code=404,
                detail="events dataset not found. Run scripts/build_events.py first.",
            )
        df = load_csv(csv_path)

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="event_id not found.",
        )

    if "event_id" not in df.columns or "start_time" not in df.columns:
        raise HTTPException(status_code=500, detail="events dataset is missing required columns.")

    df["event_id"] = df["event_id"].astype(str)
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
    df = df.dropna(subset=["event_id", "start_time"])

    matched = df[df["event_id"] == str(event_id)]
    if matched.empty:
        raise HTTPException(status_code=404, detail="event_id not found.")

    return matched.iloc[0]


def _load_segments_or_404() -> pd.DataFrame:
    config = get_config()
    processed_dir = config.paths.processed_dir
    parquet_dir = config.warehouse.parquet_dir
    parquet_path = segments_parquet_path(parquet_dir)
    csv_path = segments_csv_path(processed_dir)
    backend = duckdb_backend(config)

    if backend is not None and parquet_path.exists():
        df = backend.query_segments(columns=["segment_id", "lat", "lon"])
    elif config.warehouse.enabled and parquet_path.exists():
        df = load_parquet(parquet_path)
    else:
        if not csv_path.exists():
            raise HTTPException(
                status_code=404,
                detail="segments dataset not found. Run scripts/build_dataset.py first.",
            )
        df = load_csv(csv_path)

    if df.empty:
        raise HTTPException(status_code=404, detail="segments dataset is empty.")
    return df


def _resolve_observations_paths(minutes: Optional[int]) -> tuple[int, Path, Path]:
    config = get_config()
    processed_dir = config.paths.processed_dir
    parquet_dir = config.warehouse.parquet_dir
    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)

    csv_path = observations_csv_path(processed_dir, granularity_minutes)
    parquet_path = observations_parquet_path(parquet_dir, granularity_minutes)
    if not csv_path.exists() and not parquet_path.exists():
        fallback_minutes = int(config.preprocessing.source_granularity_minutes)
        csv_path = observations_csv_path(processed_dir, fallback_minutes)
        parquet_path = observations_parquet_path(parquet_dir, fallback_minutes)
        granularity_minutes = fallback_minutes
        if csv_path.exists() or parquet_path.exists():
            return granularity_minutes, parquet_path, csv_path
        else:
            raise HTTPException(
                status_code=404,
                detail="observations dataset not found. Run scripts/build_dataset.py first.",
            )
    return granularity_minutes, parquet_path, csv_path


@router.get("/events/{event_id}/impact", response_model=EventImpactSummary)
def get_event_impact(
    event_id: str,
    minutes: Optional[int] = Query(default=None, ge=1),
    radius_meters: Optional[float] = Query(default=None, gt=0),
    max_segments: Optional[int] = Query(default=None, ge=1, le=5000),
    baseline_window_minutes: Optional[int] = Query(default=None, ge=1),
    end_time_fallback_minutes: Optional[int] = Query(default=None, ge=1),
    recovery_horizon_minutes: Optional[int] = Query(default=None, ge=1),
    recovery_ratio: Optional[float] = Query(default=None, gt=0, le=1.0),
    speed_weighting: Optional[str] = Query(default=None),
    min_baseline_points: Optional[int] = Query(default=None, ge=1),
    min_event_points: Optional[int] = Query(default=None, ge=1),
    include_timeseries: bool = Query(default=False),
) -> EventImpactSummary:
    config = get_config()
    try:
        spec = apply_event_impact_overrides(
            event_impact_spec_from_config(config),
            radius_meters=radius_meters,
            max_segments=max_segments,
            baseline_window_minutes=baseline_window_minutes,
            end_time_fallback_minutes=end_time_fallback_minutes,
            recovery_horizon_minutes=recovery_horizon_minutes,
            recovery_ratio=recovery_ratio,
            speed_weighting=speed_weighting,
            min_baseline_points=min_baseline_points,
            min_event_points=min_event_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    event = _load_event_or_404(event_id)
    segments = _load_segments_or_404()
    resolved_minutes, obs_parquet, obs_csv = _resolve_observations_paths(minutes)

    start_time = event.get("start_time")
    end_time = event.get("end_time")
    if pd.isna(start_time):
        raise HTTPException(status_code=400, detail="event.start_time is required.")
    start_dt = to_utc(start_time.to_pydatetime() if hasattr(start_time, "to_pydatetime") else start_time)

    if pd.isna(end_time) or end_time is None:
        end_dt = start_dt + timedelta(minutes=int(spec.end_time_fallback_minutes))
    else:
        end_dt = to_utc(end_time.to_pydatetime() if hasattr(end_time, "to_pydatetime") else end_time)
        if end_dt <= start_dt:
            end_dt = start_dt + timedelta(minutes=int(spec.end_time_fallback_minutes))

    baseline_start = start_dt - timedelta(minutes=int(spec.baseline_window_minutes))
    analysis_end = end_dt + timedelta(minutes=int(spec.recovery_horizon_minutes))

    event_lat = event.get("lat")
    event_lon = event.get("lon")
    if pd.isna(event_lat) or pd.isna(event_lon):
        raise HTTPException(status_code=400, detail="event.lat and event.lon are required for impact analysis.")

    nearby = select_nearby_segments(
        segments,
        lat=float(event_lat),
        lon=float(event_lon),
        radius_meters=float(spec.radius_meters),
        max_segments=int(spec.max_segments),
    )
    if nearby.empty:
        raise HTTPException(status_code=400, detail="No nearby segments found within radius.")
    segment_ids = nearby["segment_id"].astype(str).unique().tolist()

    backend = duckdb_backend(config)
    if backend is not None and obs_parquet.exists():
        observations = backend.query_observations(
            minutes=resolved_minutes,
            segment_ids=segment_ids,
            start=baseline_start,
            end=analysis_end,
        )
    elif config.warehouse.enabled and obs_parquet.exists():
        observations = load_parquet(obs_parquet)
    else:
        observations = load_csv(obs_csv)

    if observations.empty:
        raise HTTPException(status_code=404, detail="observations dataset is empty.")

    try:
        impact: dict[str, Any] = compute_event_impact(
            event,
            observations=observations,
            segments=segments,
            spec=spec,
            radius_meters=radius_meters,
            max_segments=max_segments,
            minutes=resolved_minutes,
            include_timeseries=include_timeseries,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    event_model = TrafficEvent(
        event_id=str(event.get("event_id")),
        start_time=impact["start_time"],
        end_time=impact["end_time"],
        event_type=(None if pd.isna(event.get("event_type")) else event.get("event_type")),
        description=(None if pd.isna(event.get("description")) else event.get("description")),
        road_name=(None if pd.isna(event.get("road_name")) else event.get("road_name")),
        direction=(None if pd.isna(event.get("direction")) else event.get("direction")),
        severity=(None if pd.isna(event.get("severity")) else float(event.get("severity")) if event.get("severity") is not None else None),
        lat=(None if pd.isna(event.get("lat")) else float(event.get("lat")) if event.get("lat") is not None else None),
        lon=(None if pd.isna(event.get("lon")) else float(event.get("lon")) if event.get("lon") is not None else None),
        city=(None if pd.isna(event.get("city")) else event.get("city")),
    )

    affected_segments = [AffectedSegment(**row) for row in impact["affected_segments"]]

    timeseries = None
    if include_timeseries and impact.get("timeseries"):
        ts_df = pd.DataFrame(impact["timeseries"])
        if not ts_df.empty and "timestamp" in ts_df.columns:
            ts_df["timestamp"] = pd.to_datetime(ts_df["timestamp"], errors="coerce", utc=True)
            ts_df = ts_df.dropna(subset=["timestamp"])
            ts_df["timestamp"] = ts_df["timestamp"].dt.to_pydatetime()
        ts_df = ts_df.where(pd.notnull(ts_df), None)
        timeseries = [ImpactPoint(**row) for row in ts_df.to_dict(orient="records")]

    return EventImpactSummary(
        event=event_model,
        analysis_window_start=impact["analysis_window_start"],
        analysis_window_end=impact["analysis_window_end"],
        n_segments=int(impact["n_segments"]),
        baseline_n_points=int(impact["baseline_n_points"]),
        event_n_points=int(impact["event_n_points"]),
        baseline_mean_speed_kph=impact["baseline_mean_speed_kph"],
        baseline_std_speed_kph=impact["baseline_std_speed_kph"],
        event_mean_speed_kph=impact["event_mean_speed_kph"],
        event_min_speed_kph=impact["event_min_speed_kph"],
        event_min_time=impact["event_min_time"],
        speed_delta_mean_kph=impact["speed_delta_mean_kph"],
        speed_ratio_mean=impact["speed_ratio_mean"],
        recovered_at=impact["recovered_at"],
        recovery_minutes=impact["recovery_minutes"],
        enough_baseline=bool(impact["enough_baseline"]),
        enough_event=bool(impact["enough_event"]),
        affected_segments=affected_segments,
        timeseries=timeseries,
    )
