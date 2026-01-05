from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from trafficpulse.analytics.event_impact import compute_event_impact, event_impact_spec_from_config
from trafficpulse.ingestion.schemas import TrafficEvent
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    events_csv_path,
    load_csv,
    observations_csv_path,
    segments_csv_path,
)


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
    path = events_csv_path(config.paths.processed_dir)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="events dataset not found. Run scripts/build_events.py first.",
        )
    df = load_csv(path)
    if df.empty:
        raise HTTPException(status_code=404, detail="events dataset is empty.")

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
    path = segments_csv_path(config.paths.processed_dir)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="segments dataset not found. Run scripts/build_dataset.py first.",
        )
    df = load_csv(path)
    if df.empty:
        raise HTTPException(status_code=404, detail="segments dataset is empty.")
    return df


def _load_observations_or_404(minutes: Optional[int]) -> pd.DataFrame:
    config = get_config()
    processed_dir = config.paths.processed_dir
    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)

    path = observations_csv_path(processed_dir, granularity_minutes)
    if not path.exists():
        fallback = observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes)
        if fallback.exists():
            path = fallback
        else:
            raise HTTPException(
                status_code=404,
                detail="observations dataset not found. Run scripts/build_dataset.py first.",
            )

    df = load_csv(path)
    if df.empty:
        raise HTTPException(status_code=404, detail="observations dataset is empty.")
    return df


@router.get("/events/{event_id}/impact", response_model=EventImpactSummary)
def get_event_impact(
    event_id: str,
    minutes: Optional[int] = Query(default=None, ge=1),
    radius_meters: Optional[float] = Query(default=None, gt=0),
    max_segments: Optional[int] = Query(default=None, ge=1, le=5000),
    include_timeseries: bool = Query(default=False),
) -> EventImpactSummary:
    config = get_config()
    spec = event_impact_spec_from_config(config)

    event = _load_event_or_404(event_id)
    segments = _load_segments_or_404()
    observations = _load_observations_or_404(minutes)

    try:
        impact: dict[str, Any] = compute_event_impact(
            event,
            observations=observations,
            segments=segments,
            spec=spec,
            radius_meters=radius_meters,
            max_segments=max_segments,
            minutes=minutes,
            include_timeseries=include_timeseries,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    event_model = TrafficEvent(
        event_id=str(event.get("event_id")),
        start_time=event.get("start_time").to_pydatetime(),
        end_time=(event.get("end_time").to_pydatetime() if not pd.isna(event.get("end_time")) else None),
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

