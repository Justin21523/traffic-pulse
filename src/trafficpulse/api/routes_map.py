from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from trafficpulse.analytics.reliability import compute_reliability_metrics, reliability_spec_from_config
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import load_csv, observations_csv_path, segments_csv_path
from trafficpulse.utils.time import parse_datetime, to_utc


router = APIRouter()


class SegmentSnapshot(BaseModel):
    segment_id: str
    lat: float
    lon: float
    n_samples: int
    mean_speed_kph: Optional[float] = None
    speed_std_kph: Optional[float] = None
    congestion_frequency: Optional[float] = None


def _parse_bbox(bbox: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be 'min_lon,min_lat,max_lon,max_lat'")
    min_lon, min_lat, max_lon, max_lat = map(float, parts)
    if min_lon > max_lon or min_lat > max_lat:
        raise ValueError("bbox min values must be <= max values")
    return min_lon, min_lat, max_lon, max_lat


@router.get("/map/snapshot", response_model=list[SegmentSnapshot])
def get_map_snapshot(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO 8601)."),
    end: Optional[str] = Query(default=None, description="End datetime (ISO 8601)."),
    minutes: Optional[int] = Query(
        default=None, ge=1, description="Observation granularity in minutes (default: config)."
    ),
    bbox: Optional[str] = Query(
        default=None, description="Bounding box as 'min_lon,min_lat,max_lon,max_lat'."
    ),
    city: Optional[str] = Query(default=None),
    limit: int = Query(default=5000, ge=1, le=50000),
) -> list[SegmentSnapshot]:
    config = get_config()
    processed_dir = config.paths.processed_dir

    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)
    obs_path = observations_csv_path(processed_dir, granularity_minutes)
    if not obs_path.exists():
        fallback = observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes)
        if fallback.exists():
            obs_path = fallback
        else:
            raise HTTPException(
                status_code=404,
                detail="observations dataset not found. Run scripts/build_dataset.py (and scripts/aggregate_observations.py) first.",
            )

    segments_path = segments_csv_path(processed_dir)
    if not segments_path.exists():
        raise HTTPException(
            status_code=404,
            detail="segments dataset not found. Run scripts/build_dataset.py first.",
        )

    segments = load_csv(segments_path)
    if segments.empty:
        return []

    if "segment_id" not in segments.columns or "lat" not in segments.columns or "lon" not in segments.columns:
        raise HTTPException(status_code=500, detail="segments dataset is missing required columns.")

    segments["segment_id"] = segments["segment_id"].astype(str)
    segments["lat"] = pd.to_numeric(segments["lat"], errors="coerce")
    segments["lon"] = pd.to_numeric(segments["lon"], errors="coerce")
    segments = segments.dropna(subset=["segment_id", "lat", "lon"]).drop_duplicates(
        subset=["segment_id"], keep="first"
    )

    if city:
        if "city" not in segments.columns:
            raise HTTPException(status_code=500, detail="segments dataset is missing 'city' column.")
        segments = segments[segments["city"].astype(str) == str(city)]

    if bbox:
        try:
            min_lon, min_lat, max_lon, max_lat = _parse_bbox(bbox)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        segments = segments[
            (segments["lat"] >= min_lat)
            & (segments["lat"] <= max_lat)
            & (segments["lon"] >= min_lon)
            & (segments["lon"] <= max_lon)
        ]

    if segments.empty:
        return []

    observations = load_csv(obs_path)
    if observations.empty:
        return []

    if "timestamp" not in observations.columns or "segment_id" not in observations.columns:
        raise HTTPException(status_code=500, detail="observations dataset is missing required columns.")

    observations["segment_id"] = observations["segment_id"].astype(str)
    segment_ids = set(segments["segment_id"].astype(str).tolist())
    observations = observations[observations["segment_id"].isin(segment_ids)]
    if observations.empty:
        return []

    observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
    observations = observations.dropna(subset=["timestamp"])
    if observations.empty:
        return []

    start_dt: Optional[datetime] = parse_datetime(start) if start else None
    end_dt: Optional[datetime] = parse_datetime(end) if end else None
    if (start_dt is None) != (end_dt is None):
        raise HTTPException(status_code=400, detail="Provide both 'start' and 'end', or neither.")
    if start_dt is not None and end_dt is not None and end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    if start_dt is None and end_dt is None:
        window_hours = int(config.analytics.reliability.default_window_hours)
        if not observations["timestamp"].empty:
            end_dt = observations["timestamp"].max().to_pydatetime()
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        else:
            end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=window_hours)

    spec = reliability_spec_from_config(config)
    metrics = compute_reliability_metrics(
        observations,
        spec,
        start=to_utc(start_dt) if start_dt else None,
        end=to_utc(end_dt) if end_dt else None,
    )
    if metrics.empty:
        return []

    merged = segments[["segment_id", "lat", "lon"]].merge(metrics, on="segment_id", how="inner")
    if merged.empty:
        return []

    merged = merged[merged["n_samples"] > 0].copy()
    if merged.empty:
        return []

    merged = merged.sort_values("segment_id").head(int(limit)).reset_index(drop=True)
    merged = merged.where(pd.notnull(merged), None)
    merged["n_samples"] = pd.to_numeric(merged.get("n_samples"), errors="coerce").fillna(0).astype(int)

    return [SegmentSnapshot(**record) for record in merged.to_dict(orient="records")]

