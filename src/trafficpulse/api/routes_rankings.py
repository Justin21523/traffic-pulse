from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from trafficpulse.analytics.reliability import (
    apply_reliability_overrides,
    compute_reliability_rankings,
    reliability_spec_from_config,
)
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


class ReliabilityRankingRow(BaseModel):
    rank: int
    segment_id: str
    n_samples: int
    mean_speed_kph: float
    speed_std_kph: float
    congestion_frequency: float
    penalty_mean_speed: Optional[float] = None
    penalty_speed_std: Optional[float] = None
    penalty_congestion_frequency: Optional[float] = None
    reliability_score: Optional[float] = None


@router.get("/rankings/reliability", response_model=list[ReliabilityRankingRow])
def reliability_rankings(
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
) -> list[ReliabilityRankingRow]:
    config = get_config()
    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)

    processed_dir = config.paths.processed_dir
    parquet_dir = config.warehouse.parquet_dir
    csv_path = observations_csv_path(processed_dir, granularity_minutes)
    parquet_path = observations_parquet_path(parquet_dir, granularity_minutes)
    if not csv_path.exists() and not parquet_path.exists():
        fallback_minutes = int(config.preprocessing.source_granularity_minutes)
        csv_path = observations_csv_path(processed_dir, fallback_minutes)
        parquet_path = observations_parquet_path(parquet_dir, fallback_minutes)
        granularity_minutes = fallback_minutes
        if not csv_path.exists() and not parquet_path.exists():
            raise HTTPException(
                status_code=404,
                detail="observations dataset not found. Run scripts/build_dataset.py (and optionally scripts/aggregate_observations.py) first.",
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
            # `compute_reliability_rankings` treats `end` as exclusive (< end). When we derive `end_dt`
            # from the maximum timestamp present in the data, bump it forward by one interval so the
            # latest sample is included in the default window.
            end_dt = end_dt + timedelta(minutes=granularity_minutes)
            start_dt = end_dt - timedelta(hours=window_hours)
        else:
            df = load_parquet(parquet_path) if config.warehouse.enabled and parquet_path.exists() else load_csv(csv_path)
            if df.empty:
                return []
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
            end_dt = end_dt + timedelta(minutes=granularity_minutes)
            start_dt = end_dt - timedelta(hours=window_hours)

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

    backend = duckdb_backend(config)
    if backend is not None and parquet_path.exists():
        observations = backend.query_observations(
            minutes=granularity_minutes,
            start=start_dt,
            end=end_dt,
            columns=["timestamp", "segment_id", "speed_kph"],
        )
    else:
        observations = load_parquet(parquet_path) if config.warehouse.enabled and parquet_path.exists() else load_csv(csv_path)
        if observations.empty:
            return []
        if "timestamp" not in observations.columns:
            raise HTTPException(status_code=500, detail="observations dataset is missing 'timestamp' column.")
        observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
        observations = observations.dropna(subset=["timestamp"])

    if observations.empty:
        return []

    rankings = compute_reliability_rankings(observations, spec, start=start_dt, end=end_dt, limit=limit)
    if rankings.empty:
        return []

    rankings = rankings.astype(object).where(pd.notnull(rankings), None)
    return [ReliabilityRankingRow(**record) for record in rankings.to_dict(orient="records")]
