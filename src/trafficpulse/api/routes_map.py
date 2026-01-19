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
from trafficpulse.api.schemas import EmptyReason, ItemsResponse, ReasonCode
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
    baseline_median_speed_kph: Optional[float] = None
    baseline_iqr_speed_kph: Optional[float] = None
    relative_drop_pct: Optional[float] = None
    coverage_pct: Optional[float] = None
    speed_missing_pct: Optional[float] = None
    quality_expected_points: Optional[int] = None


def _load_segment_quality(cache_dir: Path, *, minutes: int, window_hours: int) -> pd.DataFrame:
    path = cache_dir / f"segment_quality_{int(minutes)}m_{int(window_hours)}h.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = load_csv(path)
    except Exception:
        return pd.DataFrame()
    if df.empty or "segment_id" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["segment_id"] = df["segment_id"].astype(str)
    for col in ["coverage_pct", "speed_missing_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "expected_points" in df.columns:
        df["expected_points"] = pd.to_numeric(df["expected_points"], errors="coerce")
    keep = [c for c in ["segment_id", "coverage_pct", "speed_missing_pct", "expected_points"] if c in df.columns]
    df = df[keep].drop_duplicates(subset=["segment_id"], keep="last")
    return df


def _load_baselines(cache_dir: Path, *, minutes: int) -> pd.DataFrame:
    preferred = cache_dir / f"baselines_speed_{int(minutes)}m_7d.csv"
    candidates: list[Path] = []
    if preferred.exists():
        candidates = [preferred]
    else:
        try:
            candidates = sorted(
                cache_dir.glob(f"baselines_speed_{int(minutes)}m_*.csv"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
        except Exception:
            candidates = []
    if not candidates:
        return pd.DataFrame()
    try:
        df = load_csv(candidates[0])
    except Exception:
        return pd.DataFrame()
    if df.empty or "segment_id" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["segment_id"] = df["segment_id"].astype(str)
    for col in ["weekday", "hour", "median_speed_kph", "iqr_speed_kph"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _baseline_for_timestamp(df: pd.DataFrame, *, ts: datetime) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    weekday = int(ts.weekday())
    hour = int(ts.hour)
    out = df
    if "weekday" in out.columns:
        out = out[out["weekday"].fillna(-1).astype(int) == weekday]
    if "hour" in out.columns:
        out = out[out["hour"].fillna(-1).astype(int) == hour]
    if out.empty:
        return pd.DataFrame()
    keep = [c for c in ["segment_id", "median_speed_kph", "iqr_speed_kph"] if c in out.columns]
    out = out[keep].drop_duplicates(subset=["segment_id"], keep="last")
    out = out.rename(
        columns={
            "median_speed_kph": "baseline_median_speed_kph",
            "iqr_speed_kph": "baseline_iqr_speed_kph",
        }
    )
    return out


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
    include_baseline: bool = Query(default=False, description="Include baseline median/IQR for the current hour."),
    include_quality: bool = Query(default=False, description="Include segment quality (coverage/missing rates)."),
    min_coverage_pct: Optional[float] = Query(default=None, ge=0, le=100, description="Drop segments below this coverage."),
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
                        code=ReasonCode.MATERIALIZED_EMPTY,
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
                        code=ReasonCode.MATERIALIZED_NO_MATCHES,
                        message="Materialized snapshot has no rows for the current filters.",
                        suggestion="Try zooming out or removing city/bbox filters.",
                    ),
                )

            df = df.sort_values("segment_id").head(int(limit)).reset_index(drop=True)
            df = df.astype(object).where(pd.notnull(df), None)
            df["n_samples"] = pd.to_numeric(df.get("n_samples"), errors="coerce").fillna(0).astype(int)
            if include_quality or min_coverage_pct is not None:
                q = _load_segment_quality(
                    config.paths.cache_dir,
                    minutes=mat_minutes,
                    window_hours=int(config.analytics.reliability.default_window_hours),
                )
                if not q.empty:
                    df = df.merge(q, on="segment_id", how="left")
                    df = df.rename(columns={"expected_points": "quality_expected_points"})
            if include_baseline:
                baselines = _load_baselines(config.paths.cache_dir, minutes=mat_minutes)
                base = _baseline_for_timestamp(baselines, ts=datetime.now(timezone.utc))
                if not base.empty:
                    df = df.merge(base, on="segment_id", how="left")
            if include_baseline and "mean_speed_kph" in df.columns and "baseline_median_speed_kph" in df.columns:
                mean = pd.to_numeric(df["mean_speed_kph"], errors="coerce")
                base = pd.to_numeric(df["baseline_median_speed_kph"], errors="coerce")
                df["relative_drop_pct"] = ((base - mean) / base.replace(0, pd.NA)) * 100.0
            if min_coverage_pct is not None and "coverage_pct" in df.columns:
                df = df[pd.to_numeric(df["coverage_pct"], errors="coerce") >= float(min_coverage_pct)]
            keep = [
                "segment_id",
                "lat",
                "lon",
                "n_samples",
                "mean_speed_kph",
                "speed_std_kph",
                "congestion_frequency",
                "baseline_median_speed_kph",
                "baseline_iqr_speed_kph",
                "relative_drop_pct",
                "coverage_pct",
                "speed_missing_pct",
                "quality_expected_points",
            ]
            df = df[[c for c in keep if c in df.columns]].astype(object).where(pd.notnull(df), None)
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
                code=ReasonCode.NO_SEGMENTS,
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
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_SEGMENTS,
                message="No segments found in the selected area.",
                suggestion="Try zooming out, removing bbox/city filters, or checking the segments dataset.",
            ),
        )

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
                code=ReasonCode.NO_OBSERVATIONS,
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
                code=ReasonCode.NO_OBSERVATIONS_FOR_SEGMENTS,
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
                code=ReasonCode.NO_VALID_TIMESTAMPS,
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
                code=ReasonCode.NO_METRICS,
                message="No hotspot metrics could be computed for this time window.",
                suggestion="Try lowering min_samples or using a wider time window.",
            ),
        )

    merged = segments[["segment_id", "lat", "lon"]].merge(metrics, on="segment_id", how="inner")
    if merged.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_SEGMENTS_AFTER_MERGE,
                message="No segments matched the computed metrics.",
                suggestion="Try zooming out or using a wider time window.",
            ),
        )

    merged = merged[merged["n_samples"] > 0].copy()
    if merged.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_SAMPLES,
                message="No segments have any samples in this time window.",
                suggestion="Try using a wider time window or confirming ingestion is running.",
            ),
        )

    merged = merged.sort_values("segment_id").head(int(limit)).reset_index(drop=True)
    merged = merged.astype(object).where(pd.notnull(merged), None)
    merged["n_samples"] = pd.to_numeric(merged.get("n_samples"), errors="coerce").fillna(0).astype(int)

    if include_quality or min_coverage_pct is not None:
        q = _load_segment_quality(
            config.paths.cache_dir,
            minutes=granularity_minutes,
            window_hours=int(config.analytics.reliability.default_window_hours),
        )
        if not q.empty:
            merged = merged.merge(q, on="segment_id", how="left")
            merged = merged.rename(columns={"expected_points": "quality_expected_points"})
    if include_baseline:
        baselines = _load_baselines(config.paths.cache_dir, minutes=granularity_minutes)
        ts_ref = end_dt if end_dt is not None else datetime.now(timezone.utc)
        base = _baseline_for_timestamp(baselines, ts=ts_ref)
        if not base.empty:
            merged = merged.merge(base, on="segment_id", how="left")
    if include_baseline and "mean_speed_kph" in merged.columns and "baseline_median_speed_kph" in merged.columns:
        mean = pd.to_numeric(merged["mean_speed_kph"], errors="coerce")
        base = pd.to_numeric(merged["baseline_median_speed_kph"], errors="coerce")
        merged["relative_drop_pct"] = ((base - mean) / base.replace(0, pd.NA)) * 100.0
    if min_coverage_pct is not None and "coverage_pct" in merged.columns:
        merged = merged[pd.to_numeric(merged["coverage_pct"], errors="coerce") >= float(min_coverage_pct)]

    merged = merged.astype(object).where(pd.notnull(merged), None)
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
