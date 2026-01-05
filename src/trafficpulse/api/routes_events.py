from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from trafficpulse.ingestion.schemas import TrafficEvent
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import events_csv_path, load_csv
from trafficpulse.utils.time import parse_datetime


router = APIRouter()


def _load_events_df() -> pd.DataFrame:
    config = get_config()
    path = events_csv_path(config.paths.processed_dir)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="events dataset not found. Run scripts/build_events.py first.",
        )

    df = load_csv(path)
    if df.empty:
        return df

    if "start_time" not in df.columns or "event_id" not in df.columns:
        raise HTTPException(status_code=500, detail="events dataset is missing required columns.")

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
    df["event_id"] = df["event_id"].astype(str)
    df = df.dropna(subset=["event_id", "start_time"])
    return df


def _parse_bbox(bbox: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be 'min_lon,min_lat,max_lon,max_lat'")
    min_lon, min_lat, max_lon, max_lat = map(float, parts)
    if min_lon > max_lon or min_lat > max_lat:
        raise ValueError("bbox min values must be <= max values")
    return min_lon, min_lat, max_lon, max_lat


@router.get("/events", response_model=list[TrafficEvent])
def list_events(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO 8601)."),
    end: Optional[str] = Query(default=None, description="End datetime (ISO 8601)."),
    bbox: Optional[str] = Query(
        default=None, description="Bounding box as 'min_lon,min_lat,max_lon,max_lat'."
    ),
    city: Optional[str] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> list[TrafficEvent]:
    config = get_config()
    df = _load_events_df()
    if df.empty:
        return []

    start_dt: Optional[datetime] = parse_datetime(start) if start else None
    end_dt: Optional[datetime] = parse_datetime(end) if end else None

    if (start_dt is None) != (end_dt is None):
        raise HTTPException(status_code=400, detail="Provide both 'start' and 'end', or neither.")
    if start_dt is not None and end_dt is not None and end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    if start_dt is None and end_dt is None:
        window_hours = int(config.analytics.event_impact.default_window_hours)
        if not df["start_time"].empty:
            end_dt = df["start_time"].max().to_pydatetime()
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        else:
            end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=window_hours)

    df = df[(df["start_time"] >= pd.Timestamp(start_dt)) & (df["start_time"] < pd.Timestamp(end_dt))]

    if city:
        if "city" not in df.columns:
            raise HTTPException(status_code=500, detail="events dataset is missing 'city' column.")
        df = df[df["city"].astype(str) == city]

    if bbox:
        try:
            min_lon, min_lat, max_lon, max_lat = _parse_bbox(bbox)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if "lat" not in df.columns or "lon" not in df.columns:
            raise HTTPException(status_code=500, detail="events dataset is missing lat/lon columns.")
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        df = df.dropna(subset=["lat", "lon"])
        df = df[(df["lat"] >= min_lat) & (df["lat"] <= max_lat) & (df["lon"] >= min_lon) & (df["lon"] <= max_lon)]

    df = df.sort_values("start_time", ascending=False).head(int(limit))

    keep_cols = [
        col
        for col in [
            "event_id",
            "start_time",
            "end_time",
            "event_type",
            "description",
            "road_name",
            "direction",
            "severity",
            "lat",
            "lon",
            "city",
        ]
        if col in df.columns
    ]
    df = df[keep_cols].where(pd.notnull(df), None)
    return [TrafficEvent(**record) for record in df.to_dict(orient="records")]


@router.get("/events/{event_id}", response_model=TrafficEvent)
def get_event(event_id: str) -> TrafficEvent:
    df = _load_events_df()
    df = df[df["event_id"].astype(str) == str(event_id)]
    if df.empty:
        raise HTTPException(status_code=404, detail="event_id not found.")

    row = df.iloc[0].to_dict()
    keep = {
        "event_id": row.get("event_id"),
        "start_time": row.get("start_time"),
        "end_time": row.get("end_time"),
        "event_type": row.get("event_type"),
        "description": row.get("description"),
        "road_name": row.get("road_name"),
        "direction": row.get("direction"),
        "severity": row.get("severity"),
        "lat": row.get("lat"),
        "lon": row.get("lon"),
        "city": row.get("city"),
    }
    keep = {k: (None if pd.isna(v) else v) for k, v in keep.items()}
    return TrafficEvent(**keep)

