from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from trafficpulse.api.schemas import EmptyReason, ItemsResponse, ReasonCode
from trafficpulse.ingestion.schemas import TrafficEvent
from trafficpulse.settings import get_config
from trafficpulse.storage.backend import duckdb_backend
from trafficpulse.storage.datasets import events_csv_path, events_parquet_path, load_csv, load_parquet
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


@router.get("/events", response_model=ItemsResponse[TrafficEvent])
def list_events(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO 8601)."),
    end: Optional[str] = Query(default=None, description="End datetime (ISO 8601)."),
    bbox: Optional[str] = Query(
        default=None, description="Bounding box as 'min_lon,min_lat,max_lon,max_lat'."
    ),
    city: Optional[str] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> ItemsResponse[TrafficEvent]:
    config = get_config()
    backend = duckdb_backend(config)
    parquet_path = events_parquet_path(config.warehouse.parquet_dir)
    df: pd.DataFrame

    start_dt: Optional[datetime] = parse_datetime(start) if start else None
    end_dt: Optional[datetime] = parse_datetime(end) if end else None

    if (start_dt is None) != (end_dt is None):
        raise HTTPException(status_code=400, detail="Provide both 'start' and 'end', or neither.")
    if start_dt is not None and end_dt is not None and end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    if start_dt is None and end_dt is None:
        window_hours = int(config.analytics.event_impact.default_window_hours)
        if backend is not None and parquet_path.exists():
            max_start = backend.max_event_start_time()
            if max_start is not None:
                end_dt = max_start if max_start.tzinfo is not None else max_start.replace(tzinfo=timezone.utc)
            else:
                end_dt = datetime.now(timezone.utc)
        else:
            df_for_max = _load_events_df()
            if not df_for_max.empty:
                end_dt = df_for_max["start_time"].max().to_pydatetime()
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
            else:
                end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=window_hours)

    bbox_tuple: Optional[tuple[float, float, float, float]] = None
    if bbox:
        try:
            bbox_tuple = _parse_bbox(bbox)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    if backend is not None and parquet_path.exists():
        df = backend.query_events(start=start_dt, end=end_dt, city=city, bbox=bbox_tuple, limit=limit)
    elif config.warehouse.enabled and parquet_path.exists():
        df = load_parquet(parquet_path)
    else:
        df = _load_events_df()

    if df.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_EVENTS,
                message="No events found for this time window.",
                suggestion="Try a wider time window or confirm the events dataset is built.",
            ),
        )

    if "start_time" not in df.columns or "event_id" not in df.columns:
        raise HTTPException(status_code=500, detail="events dataset is missing required columns.")
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
    df["event_id"] = df["event_id"].astype(str)
    df = df.dropna(subset=["event_id", "start_time"])

    df = df[(df["start_time"] >= pd.Timestamp(start_dt)) & (df["start_time"] < pd.Timestamp(end_dt))]

    if city:
        if "city" not in df.columns:
            raise HTTPException(status_code=500, detail="events dataset is missing 'city' column.")
        df = df[df["city"].astype(str) == city]

    if bbox_tuple:
        min_lon, min_lat, max_lon, max_lat = bbox_tuple
        if "lat" not in df.columns or "lon" not in df.columns:
            raise HTTPException(status_code=500, detail="events dataset is missing lat/lon columns.")
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        df = df.dropna(subset=["lat", "lon"])
        df = df[
            (df["lat"] >= min_lat)
            & (df["lat"] <= max_lat)
            & (df["lon"] >= min_lon)
            & (df["lon"] <= max_lon)
        ]

    if df.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_EVENTS_IN_FILTERS,
                message="No events match the current filters (time window / bbox / city).",
                suggestion="Try zooming out, removing filters, or using a wider time window.",
            ),
        )

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
    return ItemsResponse(items=[TrafficEvent(**record) for record in df.to_dict(orient="records")])


@router.get("/v1/events", response_model=list[TrafficEvent])
def list_events_v1(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO 8601)."),
    end: Optional[str] = Query(default=None, description="End datetime (ISO 8601)."),
    bbox: Optional[str] = Query(default=None, description="Bounding box as 'min_lon,min_lat,max_lon,max_lat'."),
    city: Optional[str] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> list[TrafficEvent]:
    """Legacy list-only variant of `/events`."""
    return list_events(start=start, end=end, bbox=bbox, city=city, limit=limit).items


@router.get("/events/{event_id}", response_model=TrafficEvent)
def get_event(event_id: str) -> TrafficEvent:
    config = get_config()
    backend = duckdb_backend(config)
    parquet_path = events_parquet_path(config.warehouse.parquet_dir)

    if backend is not None and parquet_path.exists():
        df = backend.query_event_by_id(str(event_id))
    elif config.warehouse.enabled and parquet_path.exists():
        df = load_parquet(parquet_path)
        df = df[df["event_id"].astype(str) == str(event_id)]
    else:
        df = _load_events_df()
        df = df[df["event_id"].astype(str) == str(event_id)]

    if df.empty:
        raise HTTPException(status_code=404, detail="event_id not found.")

    if "start_time" not in df.columns or "event_id" not in df.columns:
        raise HTTPException(status_code=500, detail="events dataset is missing required columns.")
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
    df["event_id"] = df["event_id"].astype(str)
    df = df.dropna(subset=["event_id", "start_time"])

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
