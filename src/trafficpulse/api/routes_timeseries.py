from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from trafficpulse.ingestion.schemas import TrafficObservation
from trafficpulse.analytics.corridors import aggregate_observations_to_corridors, load_corridors_csv
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import load_csv, observations_csv_path
from trafficpulse.utils.time import parse_datetime, to_utc


router = APIRouter()


class CorridorObservation(BaseModel):
    timestamp: datetime
    corridor_id: str
    speed_kph: Optional[float] = None
    volume: Optional[float] = None
    occupancy_pct: Optional[float] = None


@router.get("/timeseries", response_model=list[TrafficObservation])
def get_timeseries(
    segment_id: str = Query(..., description="VD segment identifier."),
    start: str = Query(..., description="Start datetime (ISO 8601)."),
    end: str = Query(..., description="End datetime (ISO 8601)."),
    minutes: Optional[int] = Query(
        default=None, ge=1, description="Observation granularity in minutes (default: config)."
    ),
) -> list[TrafficObservation]:
    config = get_config()
    start_dt = parse_datetime(start)
    end_dt = parse_datetime(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)

    processed_dir = config.paths.processed_dir
    path = observations_csv_path(processed_dir, granularity_minutes)
    if not path.exists():
        fallback = observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes)
        if fallback.exists():
            path = fallback
        else:
            raise HTTPException(
                status_code=404,
                detail="observations dataset not found. Run scripts/build_dataset.py (and optionally scripts/aggregate_observations.py) first.",
            )

    df = load_csv(path)
    if df.empty:
        return []

    if "timestamp" not in df.columns or "segment_id" not in df.columns:
        raise HTTPException(status_code=500, detail="observations dataset is missing required columns.")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])
    df["segment_id"] = df["segment_id"].astype(str)

    start_utc = pd.Timestamp(to_utc(start_dt))
    end_utc = pd.Timestamp(to_utc(end_dt))

    df = df[df["segment_id"] == str(segment_id)]
    df = df[(df["timestamp"] >= start_utc) & (df["timestamp"] < end_utc)]
    if df.empty:
        return []

    keep_cols = [col for col in ["timestamp", "segment_id", "speed_kph", "volume", "occupancy_pct"] if col in df.columns]
    df = df[keep_cols].sort_values("timestamp").reset_index(drop=True)
    df = df.where(pd.notnull(df), None)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.to_pydatetime()

    return [TrafficObservation(**record) for record in df.to_dict(orient="records")]


@router.get("/timeseries/corridors", response_model=list[CorridorObservation])
def get_corridor_timeseries(
    corridor_id: str = Query(..., description="Corridor identifier from configs/corridors.csv."),
    start: str = Query(..., description="Start datetime (ISO 8601)."),
    end: str = Query(..., description="End datetime (ISO 8601)."),
    minutes: Optional[int] = Query(
        default=None, ge=1, description="Observation granularity in minutes (default: config)."
    ),
) -> list[CorridorObservation]:
    config = get_config()
    start_dt = parse_datetime(start)
    end_dt = parse_datetime(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)

    processed_dir = config.paths.processed_dir
    path = observations_csv_path(processed_dir, granularity_minutes)
    if not path.exists():
        fallback = observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes)
        if fallback.exists():
            path = fallback
        else:
            raise HTTPException(
                status_code=404,
                detail="observations dataset not found. Run scripts/build_dataset.py (and optionally scripts/aggregate_observations.py) first.",
            )

    corridors_path = config.analytics.corridors.corridors_csv
    if not corridors_path.exists():
        raise HTTPException(
            status_code=404,
            detail="corridors.csv not found. Copy configs/corridors.example.csv to configs/corridors.csv first.",
        )
    corridors = load_corridors_csv(corridors_path)
    corridors = corridors[corridors["corridor_id"].astype(str) == str(corridor_id)]
    if corridors.empty:
        raise HTTPException(status_code=404, detail="corridor_id not found in corridors.csv.")

    df = load_csv(path)
    if df.empty:
        return []
    if "timestamp" not in df.columns or "segment_id" not in df.columns:
        raise HTTPException(status_code=500, detail="observations dataset is missing required columns.")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])
    df["segment_id"] = df["segment_id"].astype(str)

    start_utc = pd.Timestamp(to_utc(start_dt))
    end_utc = pd.Timestamp(to_utc(end_dt))

    segment_ids = set(corridors["segment_id"].astype(str).unique().tolist())
    df = df[df["segment_id"].isin(segment_ids)]
    df = df[(df["timestamp"] >= start_utc) & (df["timestamp"] < end_utc)]
    if df.empty:
        return []

    corridor_ts = aggregate_observations_to_corridors(
        df,
        corridors,
        speed_weighting=config.analytics.corridors.speed_weighting,
        weight_column=config.analytics.corridors.weight_column,
    )
    if corridor_ts.empty:
        return []

    corridor_ts = corridor_ts[corridor_ts["corridor_id"].astype(str) == str(corridor_id)]
    corridor_ts = corridor_ts.sort_values("timestamp").reset_index(drop=True)
    corridor_ts = corridor_ts.where(pd.notnull(corridor_ts), None)
    corridor_ts["timestamp"] = pd.to_datetime(corridor_ts["timestamp"], utc=True).dt.to_pydatetime()

    keep_cols = [col for col in ["timestamp", "corridor_id", "speed_kph", "volume", "occupancy_pct"] if col in corridor_ts.columns]
    corridor_ts = corridor_ts[keep_cols]
    return [CorridorObservation(**record) for record in corridor_ts.to_dict(orient="records")]
