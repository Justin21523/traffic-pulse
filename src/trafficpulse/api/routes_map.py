from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from trafficpulse.analytics.reliability import compute_reliability_metrics, reliability_spec_from_config
from trafficpulse.settings import get_config
from trafficpulse.storage.backend import duckdb_backend
from trafficpulse.storage.datasets import (
    load_csv,
    load_parquet,
    observations_parquet_path,
    observations_csv_path,
    segments_csv_path,
    segments_parquet_path,
)
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
    parquet_dir = config.warehouse.parquet_dir
    obs_csv = observations_csv_path(processed_dir, granularity_minutes)
    obs_parquet = observations_parquet_path(parquet_dir, granularity_minutes)
    if not obs_csv.exists() and not obs_parquet.exists():
        fallback_minutes = int(config.preprocessing.source_granularity_minutes)
        obs_csv = observations_csv_path(processed_dir, fallback_minutes)
        obs_parquet = observations_parquet_path(parquet_dir, fallback_minutes)
        granularity_minutes = fallback_minutes
        if not obs_csv.exists() and not obs_parquet.exists():
            raise HTTPException(
                status_code=404,
                detail="observations dataset not found. Run scripts/build_dataset.py (and scripts/aggregate_observations.py) first.",
            )

    backend = duckdb_backend(config)

    bbox_tuple: Optional[tuple[float, float, float, float]] = None
    if bbox:
        try:
            bbox_tuple = _parse_bbox(bbox)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    segments: pd.DataFrame
    segments_parquet = segments_parquet_path(parquet_dir)
    segments_csv = segments_csv_path(processed_dir)
    if backend is not None and segments_parquet.exists():
        segments = backend.query_segments(
            city=city,
            bbox=bbox_tuple,
            columns=["segment_id", "lat", "lon", "city"],
        )
    elif config.warehouse.enabled and segments_parquet.exists():
        segments = load_parquet(segments_parquet)
    else:
        if not segments_csv.exists():
            raise HTTPException(
                status_code=404,
                detail="segments dataset not found. Run scripts/build_dataset.py first.",
            )
        segments = load_csv(segments_csv)
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

    if bbox_tuple:
        min_lon, min_lat, max_lon, max_lat = bbox_tuple
        segments = segments[
            (segments["lat"] >= min_lat)
            & (segments["lat"] <= max_lat)
            & (segments["lon"] >= min_lon)
            & (segments["lon"] <= max_lon)
        ]

    if segments.empty:
        return []

    start_dt: Optional[datetime] = parse_datetime(start) if start else None
    end_dt: Optional[datetime] = parse_datetime(end) if end else None
    if (start_dt is None) != (end_dt is None):
        raise HTTPException(status_code=400, detail="Provide both 'start' and 'end', or neither.")
    if start_dt is not None and end_dt is not None and end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    if start_dt is None and end_dt is None:
        window_hours = int(config.analytics.reliability.default_window_hours)
        if backend is not None and obs_parquet.exists():
            max_ts = backend.max_observation_timestamp(minutes=granularity_minutes)
            if max_ts is not None:
                end_dt = max_ts if max_ts.tzinfo is not None else max_ts.replace(tzinfo=timezone.utc)
            else:
                end_dt = datetime.now(timezone.utc)
        else:
            observations_for_max = load_parquet(obs_parquet) if config.warehouse.enabled and obs_parquet.exists() else load_csv(obs_csv)
            if not observations_for_max.empty and "timestamp" in observations_for_max.columns:
                observations_for_max["timestamp"] = pd.to_datetime(observations_for_max["timestamp"], errors="coerce", utc=True)
                observations_for_max = observations_for_max.dropna(subset=["timestamp"])
                if not observations_for_max["timestamp"].empty:
                    end_dt = observations_for_max["timestamp"].max().to_pydatetime()
                    if end_dt.tzinfo is None:
                        end_dt = end_dt.replace(tzinfo=timezone.utc)
                else:
                    end_dt = datetime.now(timezone.utc)
            else:
                end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=window_hours)

    segment_ids = set(segments["segment_id"].astype(str).tolist())
    if backend is not None and obs_parquet.exists():
        observations = backend.query_observations(
            minutes=granularity_minutes,
            segment_ids=list(segment_ids),
            start=start_dt,
            end=end_dt,
            columns=["timestamp", "segment_id", "speed_kph"],
        )
    else:
        observations = load_parquet(obs_parquet) if config.warehouse.enabled and obs_parquet.exists() else load_csv(obs_csv)

    if observations.empty:
        return []

    if "timestamp" not in observations.columns or "segment_id" not in observations.columns:
        raise HTTPException(status_code=500, detail="observations dataset is missing required columns.")

    observations["segment_id"] = observations["segment_id"].astype(str)
    observations = observations[observations["segment_id"].isin(segment_ids)]
    if observations.empty:
        return []

    observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
    observations = observations.dropna(subset=["timestamp"])
    if observations.empty:
        return []

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
