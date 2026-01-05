from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from trafficpulse.analytics.corridors import (
    compute_corridor_reliability_rankings,
    corridor_metadata,
    load_corridors_csv,
)
from trafficpulse.analytics.reliability import compute_reliability_rankings, reliability_spec_from_config
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import load_csv, observations_csv_path
from trafficpulse.utils.time import parse_datetime


router = APIRouter()


def _resolve_window(
    df: pd.DataFrame,
    *,
    start: Optional[str],
    end: Optional[str],
    default_window_hours: int,
) -> tuple[datetime, datetime]:
    start_dt: Optional[datetime] = parse_datetime(start) if start else None
    end_dt: Optional[datetime] = parse_datetime(end) if end else None

    if (start_dt is None) != (end_dt is None):
        raise HTTPException(status_code=400, detail="Provide both 'start' and 'end', or neither.")
    if start_dt is not None and end_dt is not None and end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    if start_dt is not None and end_dt is not None:
        return start_dt, end_dt

    if not df["timestamp"].empty:
        end_dt = df["timestamp"].max().to_pydatetime()
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
    else:
        end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=int(default_window_hours))
    return start_dt, end_dt


def _csv_response(df: pd.DataFrame, filename: str) -> Response:
    content = df.to_csv(index=False)
    return Response(
        content=content,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/exports/reliability/segments.csv")
def export_segment_reliability_csv(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO 8601)."),
    end: Optional[str] = Query(default=None, description="End datetime (ISO 8601)."),
    limit: int = Query(default=200, ge=1, le=5000),
    minutes: Optional[int] = Query(default=None, ge=1),
) -> Response:
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
        return _csv_response(pd.DataFrame(), filename="segment_rankings.csv")

    if "timestamp" not in df.columns:
        raise HTTPException(status_code=500, detail="observations dataset is missing 'timestamp' column.")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])

    start_dt, end_dt = _resolve_window(
        df, start=start, end=end, default_window_hours=int(config.analytics.reliability.default_window_hours)
    )
    spec = reliability_spec_from_config(config)
    rankings = compute_reliability_rankings(df, spec, start=start_dt, end=end_dt, limit=limit)
    return _csv_response(rankings, filename=f"segment_rankings_{granularity_minutes}min.csv")


@router.get("/exports/reliability/corridors.csv")
def export_corridor_reliability_csv(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO 8601)."),
    end: Optional[str] = Query(default=None, description="End datetime (ISO 8601)."),
    limit: int = Query(default=200, ge=1, le=5000),
    minutes: Optional[int] = Query(default=None, ge=1),
) -> Response:
    config = get_config()
    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)

    corridors_path = config.analytics.corridors.corridors_csv
    if not corridors_path.exists():
        raise HTTPException(
            status_code=404,
            detail="corridors.csv not found. Copy configs/corridors.example.csv to configs/corridors.csv first.",
        )

    path = observations_csv_path(config.paths.processed_dir, granularity_minutes)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="observations dataset not found. Run scripts/build_dataset.py (and scripts/aggregate_observations.py) first.",
        )

    df = load_csv(path)
    if df.empty:
        return _csv_response(pd.DataFrame(), filename="corridor_rankings.csv")

    if "timestamp" not in df.columns:
        raise HTTPException(status_code=500, detail="observations dataset is missing 'timestamp' column.")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])

    start_dt, end_dt = _resolve_window(
        df, start=start, end=end, default_window_hours=int(config.analytics.reliability.default_window_hours)
    )

    corridors = load_corridors_csv(corridors_path)
    spec = reliability_spec_from_config(config)
    rankings = compute_corridor_reliability_rankings(
        df,
        corridors,
        spec,
        speed_weighting=config.analytics.corridors.speed_weighting,
        weight_column=config.analytics.corridors.weight_column,
        start=start_dt,
        end=end_dt,
        limit=limit,
    )

    meta = corridor_metadata(corridors)
    if not rankings.empty:
        rankings = rankings.merge(meta, on="corridor_id", how="left")

    return _csv_response(rankings, filename=f"corridor_rankings_{granularity_minutes}min.csv")

