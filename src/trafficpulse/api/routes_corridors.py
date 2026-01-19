from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from trafficpulse.api.schemas import EmptyReason, ItemsResponse
from trafficpulse.analytics.corridors import (
    compute_corridor_reliability_rankings,
    corridor_metadata,
    load_corridors_csv,
)
from trafficpulse.analytics.reliability import apply_reliability_overrides, reliability_spec_from_config
from trafficpulse.settings import get_config
from trafficpulse.storage.backend import duckdb_backend
from trafficpulse.storage.datasets import (
    load_csv,
    load_parquet,
    observations_parquet_path,
    observations_csv_path,
)
from trafficpulse.utils.time import parse_datetime


router = APIRouter()


class CorridorMetadata(BaseModel):
    corridor_id: str
    corridor_name: Optional[str] = None
    segment_count: int
    # Optional geometry hints for UI map navigation (additive / backward compatible).
    min_lat: Optional[float] = None
    min_lon: Optional[float] = None
    max_lat: Optional[float] = None
    max_lon: Optional[float] = None
    center_lat: Optional[float] = None
    center_lon: Optional[float] = None


def _load_segments_for_corridors() -> pd.DataFrame:
    config = get_config()
    backend = duckdb_backend(config)
    if backend is not None:
        return backend.query_segments(columns=["segment_id", "lat", "lon"])

    from trafficpulse.storage.datasets import segments_csv_path, segments_parquet_path

    parquet_dir = config.warehouse.parquet_dir
    processed_dir = config.paths.processed_dir
    parquet_path = segments_parquet_path(parquet_dir)
    csv_path = segments_csv_path(processed_dir)
    if config.warehouse.enabled and parquet_path.exists():
        return load_parquet(parquet_path)
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="segments dataset not found. Run scripts/build_dataset.py first.")
    return load_csv(csv_path)


class CorridorReliabilityRankingRow(BaseModel):
    rank: int
    corridor_id: str
    corridor_name: Optional[str] = None
    segment_count: Optional[int] = None
    n_samples: int
    mean_speed_kph: float
    speed_std_kph: float
    congestion_frequency: float
    penalty_mean_speed: Optional[float] = None
    penalty_speed_std: Optional[float] = None
    penalty_congestion_frequency: Optional[float] = None
    reliability_score: Optional[float] = None


def _load_corridors_or_404() -> pd.DataFrame:
    config = get_config()
    path = config.analytics.corridors.corridors_csv
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                "corridors file not found. Copy configs/corridors.example.csv to configs/corridors.csv and edit it."
            ),
        )
    return load_corridors_csv(path)


@router.get("/corridors", response_model=list[CorridorMetadata])
def list_corridors() -> list[CorridorMetadata]:
    corridors = _load_corridors_or_404()
    meta = corridor_metadata(corridors)
    if meta.empty:
        return []

    # Enrich with bbox/center for map navigation when segment geometry is available.
    try:
        segments = _load_segments_for_corridors()
    except HTTPException:
        segments = pd.DataFrame()

    if not segments.empty and {"segment_id", "lat", "lon"}.issubset(set(segments.columns)):
        seg = segments[["segment_id", "lat", "lon"]].copy()
        seg["segment_id"] = seg["segment_id"].astype(str)
        seg["lat"] = pd.to_numeric(seg["lat"], errors="coerce")
        seg["lon"] = pd.to_numeric(seg["lon"], errors="coerce")
        seg = seg.dropna(subset=["segment_id", "lat", "lon"])

        corridor_segments = corridors[["corridor_id", "segment_id"]].copy()
        corridor_segments["corridor_id"] = corridor_segments["corridor_id"].astype(str)
        corridor_segments["segment_id"] = corridor_segments["segment_id"].astype(str)

        merged = corridor_segments.merge(seg, on="segment_id", how="left").dropna(subset=["lat", "lon"])
        if not merged.empty:
            bbox = merged.groupby("corridor_id", as_index=False).agg(
                min_lat=("lat", "min"),
                min_lon=("lon", "min"),
                max_lat=("lat", "max"),
                max_lon=("lon", "max"),
            )
            bbox["center_lat"] = (bbox["min_lat"] + bbox["max_lat"]) / 2
            bbox["center_lon"] = (bbox["min_lon"] + bbox["max_lon"]) / 2
            meta = meta.merge(bbox, on="corridor_id", how="left")

    meta = meta.astype(object).where(pd.notnull(meta), None)
    return [CorridorMetadata(**record) for record in meta.to_dict(orient="records")]


@router.get(
    "/rankings/reliability/corridors",
    response_model=ItemsResponse[CorridorReliabilityRankingRow],
)
def corridor_reliability_rankings(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO 8601)."),
    end: Optional[str] = Query(default=None, description="End datetime (ISO 8601)."),
    limit: int = Query(default=200, ge=1, le=5000),
    minutes: Optional[int] = Query(
        default=None, ge=1, description="Observation granularity in minutes (default: config)."
    ),
    congestion_speed_threshold_kph: Optional[float] = Query(default=None, gt=0),
    min_samples: Optional[int] = Query(default=None, ge=1),
    weight_mean_speed: Optional[float] = Query(default=None, ge=0),
    weight_speed_std: Optional[float] = Query(default=None, ge=0),
    weight_congestion_frequency: Optional[float] = Query(default=None, ge=0),
) -> ItemsResponse[CorridorReliabilityRankingRow]:
    config = get_config()
    corridors = _load_corridors_or_404()

    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)
    processed_dir = config.paths.processed_dir
    parquet_dir = config.warehouse.parquet_dir
    csv_path = observations_csv_path(processed_dir, granularity_minutes)
    parquet_path = observations_parquet_path(parquet_dir, granularity_minutes)
    if not csv_path.exists() and not parquet_path.exists():
        raise HTTPException(
            status_code=404,
            detail="observations dataset not found. Run scripts/build_dataset.py (and scripts/aggregate_observations.py) first.",
        )

    start_dt: Optional[datetime] = parse_datetime(start) if start else None
    end_dt: Optional[datetime] = parse_datetime(end) if end else None

    if (start_dt is None) != (end_dt is None):
        raise HTTPException(status_code=400, detail="Provide both 'start' and 'end', or neither.")
    if start_dt is not None and end_dt is not None and end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    if start_dt is None and end_dt is None:
        window_hours = int(config.analytics.reliability.default_window_hours)
        backend = duckdb_backend(config)
        if backend is not None and parquet_path.exists():
            max_ts = backend.max_observation_timestamp(minutes=granularity_minutes)
            if max_ts is not None:
                end_dt = max_ts if max_ts.tzinfo is not None else max_ts.replace(tzinfo=timezone.utc)
            else:
                end_dt = datetime.now(timezone.utc)
        else:
            df = load_parquet(parquet_path) if config.warehouse.enabled and parquet_path.exists() else load_csv(csv_path)
            if df.empty:
                return ItemsResponse(
                    items=[],
                    reason=EmptyReason(
                        code="no_observations",
                        message="No observations found for the default time window.",
                        suggestion="Run ingestion/build_dataset and try a wider time window.",
                    ),
                )
            if "timestamp" not in df.columns:
                raise HTTPException(status_code=500, detail="observations dataset is missing 'timestamp' column.")
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            df = df.dropna(subset=["timestamp"])
            if not df["timestamp"].empty:
                end_dt = df["timestamp"].max().to_pydatetime()
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
            else:
                end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=window_hours)

    segment_ids = corridors["segment_id"].astype(str).unique().tolist()
    backend = duckdb_backend(config)
    if backend is not None and parquet_path.exists():
        observations = backend.query_observations(
            minutes=granularity_minutes,
            segment_ids=segment_ids,
            start=start_dt,
            end=end_dt,
            columns=["timestamp", "segment_id", "speed_kph", "volume", "occupancy_pct"],
        )
    else:
        observations = load_parquet(parquet_path) if config.warehouse.enabled and parquet_path.exists() else load_csv(csv_path)
        if observations.empty:
            return ItemsResponse(
                items=[],
                reason=EmptyReason(
                    code="no_observations",
                    message="No observations found for this time window.",
                    suggestion="Try a wider time window, or confirm ingestion/build_dataset ran successfully.",
                ),
            )
        if "timestamp" not in observations.columns:
            raise HTTPException(status_code=500, detail="observations dataset is missing 'timestamp' column.")
        observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
        observations = observations.dropna(subset=["timestamp"])
        observations["segment_id"] = observations["segment_id"].astype(str)
        observations = observations[observations["segment_id"].isin(segment_ids)]

    if observations.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code="no_observations",
                message="No observations found for this time window.",
                suggestion="Try a wider time window, or confirm ingestion/build_dataset ran successfully.",
            ),
        )

    try:
        spec = apply_reliability_overrides(
            reliability_spec_from_config(config),
            congestion_speed_threshold_kph=congestion_speed_threshold_kph,
            min_samples=min_samples,
            weight_mean_speed=weight_mean_speed,
            weight_speed_std=weight_speed_std,
            weight_congestion_frequency=weight_congestion_frequency,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    ranked = compute_corridor_reliability_rankings(
        observations,
        corridors,
        spec,
        speed_weighting=config.analytics.corridors.speed_weighting,
        weight_column=config.analytics.corridors.weight_column,
        start=start_dt,
        end=end_dt,
        limit=limit,
    )
    if ranked.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code="no_rankings",
                message="No corridors met the ranking criteria for this time window.",
                suggestion="Try lowering min_samples or using a wider time window.",
            ),
        )

    meta = corridor_metadata(corridors)
    ranked = ranked.merge(meta, on="corridor_id", how="left")
    ranked = ranked.where(pd.notnull(ranked), None)
    return ItemsResponse(
        items=[CorridorReliabilityRankingRow(**record) for record in ranked.to_dict(orient="records")]
    )
