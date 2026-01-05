from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from trafficpulse.analytics.reliability import compute_reliability_rankings, reliability_spec_from_config
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import load_csv, observations_csv_path
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
) -> list[ReliabilityRankingRow]:
    config = get_config()
    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)

    path = observations_csv_path(config.paths.processed_dir, granularity_minutes)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="observations dataset not found. Run scripts/build_dataset.py (and scripts/aggregate_observations.py) first.",
        )

    df = load_csv(path)
    if df.empty:
        return []

    if "timestamp" not in df.columns:
        raise HTTPException(status_code=500, detail="observations dataset is missing 'timestamp' column.")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])

    spec = reliability_spec_from_config(config)

    start_dt: Optional[datetime] = parse_datetime(start) if start else None
    end_dt: Optional[datetime] = parse_datetime(end) if end else None

    if (start_dt is None) != (end_dt is None):
        raise HTTPException(status_code=400, detail="Provide both 'start' and 'end', or neither.")
    if start_dt is not None and end_dt is not None and end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    if start_dt is None and end_dt is None:
        window_hours = int(config.analytics.reliability.default_window_hours)
        if not df["timestamp"].empty:
            end_dt = df["timestamp"].max().to_pydatetime()
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        else:
            end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=window_hours)

    rankings = compute_reliability_rankings(df, spec, start=start_dt, end=end_dt, limit=limit)
    if rankings.empty:
        return []

    rankings = rankings.where(pd.notnull(rankings), None)
    return [ReliabilityRankingRow(**record) for record in rankings.to_dict(orient="records")]

