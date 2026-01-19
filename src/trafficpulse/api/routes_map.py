from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from trafficpulse.analytics.reliability import (
    apply_reliability_overrides,
    compute_reliability_metrics,
    reliability_spec_from_config,
)
from trafficpulse.api.schemas import EmptyReason, ItemsResponse
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


@router.get("/map/snapshot", response_model=ItemsResponse[SegmentSnapshot])
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
    congestion_speed_threshold_kph: Optional[float] = Query(default=None, gt=0),
    min_samples: Optional[int] = Query(default=None, ge=1),
    weight_mean_speed: Optional[float] = Query(default=None, ge=0),
    weight_speed_std: Optional[float] = Query(default=None, ge=0),
    weight_congestion_frequency: Optional[float] = Query(default=None, ge=0),
) -> ItemsResponse[SegmentSnapshot]:
    config = get_config()
    processed_dir = config.paths.processed_dir

    # Fast path: if the caller didn't provide a time range or overrides, serve a materialized snapshot.
    if (
        start is None
        and end is None
        and congestion_speed_threshold_kph is None
        and min_samples is None
        and weight_mean_speed is None
        and weight_speed_std is None
        and weight_congestion_frequency is None
    ):
        mat_minutes = int(minutes or config.preprocessing.target_granularity_minutes)
        mat_hours = int(config.analytics.reliability.default_window_hours)
        mat_path = config.paths.cache_dir / f"materialized_map_snapshot_{mat_minutes}m_{mat_hours}h.csv"
        if mat_path.exists():
            try:
                df = load_csv(mat_path)
            except Exception:
                df = pd.DataFrame()
            if df.empty:
                return ItemsResponse(
                    items=[],
                    reason=EmptyReason(
                        code="materialized_empty",
                        message="Materialized snapshot exists but is empty.",
                        suggestion="Re-run scripts/materialize_defaults.py after building datasets.",
                    ),
                )

            if city and "city" in df.columns:
                df = df[df["city"].astype(str) == str(city)]
            if bbox:
                try:
                    min_lon, min_lat, max_lon, max_lat = _parse_bbox(bbox)
                except ValueError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
                if {"lat", "lon"}.issubset(set(df.columns)):
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
                        code="materialized_no_matches",
                        message="Materialized snapshot has no rows for the current filters.",
                        suggestion="Try zooming out or removing city/bbox filters.",
                    ),
                )

            df = df.sort_values("segment_id").head(int(limit)).reset_index(drop=True)
            df = df.astype(object).where(pd.notnull(df), None)
            df["n_samples"] = pd.to_numeric(df.get("n_samples"), errors="coerce").fillna(0).astype(int)
            keep = ["segment_id", "lat", "lon", "n_samples", "mean_speed_kph", "speed_std_kph", "congestion_frequency"]
            df = df[[c for c in keep if c in df.columns]]
            return ItemsResponse(items=[SegmentSnapshot(**record) for record in df.to_dict(orient="records")])

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
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code="no_segments",
                message="No segments found in the selected area.",
                suggestion="Try zooming out, removing bbox/city filters, or checking the segments dataset.",
            ),
        )

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

        # The downstream computations treat `end` as an exclusive bound (< end). When we derive `end_dt`
        # from the maximum timestamp present in the data, bump it forward by one interval so the latest
        # sample is included in the default window.
        end_dt = end_dt + timedelta(minutes=granularity_minutes)
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
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code="no_observations",
                message="No observations found for this time window.",
                suggestion="Try a wider time window, or confirm ingestion/build_dataset ran successfully.",
            ),
        )

    if "timestamp" not in observations.columns or "segment_id" not in observations.columns:
        raise HTTPException(status_code=500, detail="observations dataset is missing required columns.")

    observations["segment_id"] = observations["segment_id"].astype(str)
    observations = observations[observations["segment_id"].isin(segment_ids)]
    if observations.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code="no_observations_for_segments",
                message="No observations remain after filtering to segments in the selected area.",
                suggestion="Try zooming out, removing bbox/city filters, or using a wider time window.",
            ),
        )

    observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
    observations = observations.dropna(subset=["timestamp"])
    if observations.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code="no_valid_timestamps",
                message="No valid timestamps found in observations for this window.",
                suggestion="Check the observations dataset timestamp column and preprocessing steps.",
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
    metrics = compute_reliability_metrics(
        observations,
        spec,
        start=to_utc(start_dt) if start_dt else None,
        end=to_utc(end_dt) if end_dt else None,
    )
    if metrics.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code="no_metrics",
                message="No hotspot metrics could be computed for this time window.",
                suggestion="Try lowering min_samples or using a wider time window.",
            ),
        )

    merged = segments[["segment_id", "lat", "lon"]].merge(metrics, on="segment_id", how="inner")
    if merged.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code="no_segments_after_merge",
                message="No segments matched the computed metrics.",
                suggestion="Try zooming out or using a wider time window.",
            ),
        )

    merged = merged[merged["n_samples"] > 0].copy()
    if merged.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code="no_samples",
                message="No segments have any samples in this time window.",
                suggestion="Try using a wider time window or confirming ingestion is running.",
            ),
        )

    merged = merged.sort_values("segment_id").head(int(limit)).reset_index(drop=True)
    merged = merged.astype(object).where(pd.notnull(merged), None)
    merged["n_samples"] = pd.to_numeric(merged.get("n_samples"), errors="coerce").fillna(0).astype(int)

    return ItemsResponse(items=[SegmentSnapshot(**record) for record in merged.to_dict(orient="records")])


@router.get("/v1/map/snapshot", response_model=list[SegmentSnapshot])
def get_map_snapshot_v1(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO 8601)."),
    end: Optional[str] = Query(default=None, description="End datetime (ISO 8601)."),
    minutes: Optional[int] = Query(default=None, ge=1, description="Observation granularity in minutes (default: config)."),
    bbox: Optional[str] = Query(default=None, description="Bounding box as 'min_lon,min_lat,max_lon,max_lat'."),
    city: Optional[str] = Query(default=None),
    limit: int = Query(default=5000, ge=1, le=50000),
    congestion_speed_threshold_kph: Optional[float] = Query(default=None, gt=0),
    min_samples: Optional[int] = Query(default=None, ge=1),
    weight_mean_speed: Optional[float] = Query(default=None, ge=0),
    weight_speed_std: Optional[float] = Query(default=None, ge=0),
    weight_congestion_frequency: Optional[float] = Query(default=None, ge=0),
) -> list[SegmentSnapshot]:
    """Legacy list-only variant of `/map/snapshot`."""
    return get_map_snapshot(
        start=start,
        end=end,
        minutes=minutes,
        bbox=bbox,
        city=city,
        limit=limit,
        congestion_speed_threshold_kph=congestion_speed_threshold_kph,
        min_samples=min_samples,
        weight_mean_speed=weight_mean_speed,
        weight_speed_std=weight_speed_std,
        weight_congestion_frequency=weight_congestion_frequency,
    ).items
