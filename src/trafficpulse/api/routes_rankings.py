from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from trafficpulse.analytics.reliability import (
    apply_reliability_overrides,
    compute_reliability_rankings,
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
    coverage_pct: Optional[float] = None
    speed_missing_pct: Optional[float] = None
    baseline_median_speed_kph: Optional[float] = None
    baseline_iqr_speed_kph: Optional[float] = None
    penalty_mean_speed: Optional[float] = None
    penalty_speed_std: Optional[float] = None
    penalty_congestion_frequency: Optional[float] = None
    reliability_score: Optional[float] = None


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
    keep = [c for c in ["segment_id", "coverage_pct", "speed_missing_pct"] if c in df.columns]
    return df[keep].drop_duplicates(subset=["segment_id"], keep="last")


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
    return out.rename(
        columns={"median_speed_kph": "baseline_median_speed_kph", "iqr_speed_kph": "baseline_iqr_speed_kph"}
    )


@router.get("/rankings/reliability", response_model=ItemsResponse[ReliabilityRankingRow])
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
    include_quality: bool = Query(default=False, description="Include segment quality (coverage/missing rates)."),
    include_baseline: bool = Query(default=False, description="Include baseline median/IQR for the current hour."),
    min_coverage_pct: Optional[float] = Query(default=None, ge=0, le=100, description="Drop segments below this coverage."),
) -> ItemsResponse[ReliabilityRankingRow]:
    config = get_config()
    granularity_minutes = int(minutes or config.preprocessing.target_granularity_minutes)

    # Fast path: if the caller didn't provide a time range or overrides, serve a materialized ranking.
    if (
        start is None
        and end is None
        and congestion_speed_threshold_kph is None
        and min_samples is None
        and weight_mean_speed is None
        and weight_speed_std is None
        and weight_congestion_frequency is None
    ):
        window_hours = int(config.analytics.reliability.default_window_hours)
        mat_path = config.paths.cache_dir / f"materialized_rankings_segments_{granularity_minutes}m_{window_hours}h.csv"
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
                        message="Materialized rankings exist but are empty.",
                        suggestion="Re-run scripts/materialize_defaults.py after building datasets.",
                    ),
                )
            df = df.sort_values("rank").head(int(limit)).reset_index(drop=True)
            if include_quality or min_coverage_pct is not None:
                q = _load_segment_quality(config.paths.cache_dir, minutes=granularity_minutes, window_hours=window_hours)
                if not q.empty:
                    df = df.merge(q, on="segment_id", how="left")
            if include_baseline:
                base = _baseline_for_timestamp(
                    _load_baselines(config.paths.cache_dir, minutes=granularity_minutes),
                    ts=datetime.now(timezone.utc),
                )
                if not base.empty:
                    df = df.merge(base, on="segment_id", how="left")
            if min_coverage_pct is not None and "coverage_pct" in df.columns:
                df = df[pd.to_numeric(df["coverage_pct"], errors="coerce") >= float(min_coverage_pct)]
            df = df.astype(object).where(pd.notnull(df), None)
            return ItemsResponse(items=[ReliabilityRankingRow(**record) for record in df.to_dict(orient="records")])

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
                return ItemsResponse(
                    items=[],
                    reason=EmptyReason(
                        code=ReasonCode.NO_OBSERVATIONS,
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
            return ItemsResponse(
                items=[],
                reason=EmptyReason(
                    code=ReasonCode.NO_OBSERVATIONS,
                    message="No observations found for this time window.",
                    suggestion="Try a wider time window, or confirm ingestion/build_dataset ran successfully.",
                ),
            )
        if "timestamp" not in observations.columns:
            raise HTTPException(status_code=500, detail="observations dataset is missing 'timestamp' column.")
        observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
        observations = observations.dropna(subset=["timestamp"])

    if observations.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_OBSERVATIONS,
                message="No observations found for this time window.",
                suggestion="Try a wider time window, or confirm ingestion/build_dataset ran successfully.",
            ),
        )

    rankings = compute_reliability_rankings(observations, spec, start=start_dt, end=end_dt, limit=limit)
    if rankings.empty:
        return ItemsResponse(
            items=[],
            reason=EmptyReason(
                code=ReasonCode.NO_RANKINGS,
                message="No segments met the ranking criteria for this time window.",
                suggestion="Try lowering min_samples or using a wider time window.",
            ),
        )

    if include_quality or min_coverage_pct is not None:
        q = _load_segment_quality(
            config.paths.cache_dir,
            minutes=granularity_minutes,
            window_hours=int(config.analytics.reliability.default_window_hours),
        )
        if not q.empty:
            rankings = rankings.merge(q, on="segment_id", how="left")
    if include_baseline:
        base = _baseline_for_timestamp(
            _load_baselines(config.paths.cache_dir, minutes=granularity_minutes),
            ts=end_dt if end_dt is not None else datetime.now(timezone.utc),
        )
        if not base.empty:
            rankings = rankings.merge(base, on="segment_id", how="left")
    if min_coverage_pct is not None and "coverage_pct" in rankings.columns:
        rankings = rankings[pd.to_numeric(rankings["coverage_pct"], errors="coerce") >= float(min_coverage_pct)]

    rankings = rankings.astype(object).where(pd.notnull(rankings), None)
    return ItemsResponse(items=[ReliabilityRankingRow(**record) for record in rankings.to_dict(orient="records")])


@router.get("/v1/rankings/reliability", response_model=list[ReliabilityRankingRow])
def reliability_rankings_v1(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO 8601)."),
    end: Optional[str] = Query(default=None, description="End datetime (ISO 8601)."),
    limit: int = Query(default=200, ge=1, le=5000),
    minutes: Optional[int] = Query(default=None, ge=1, description="Observation granularity in minutes (default: config)."),
    congestion_speed_threshold_kph: Optional[float] = Query(default=None, gt=0),
    min_samples: Optional[int] = Query(default=None, ge=1),
    weight_mean_speed: Optional[float] = Query(default=None, ge=0),
    weight_speed_std: Optional[float] = Query(default=None, ge=0),
    weight_congestion_frequency: Optional[float] = Query(default=None, ge=0),
) -> list[ReliabilityRankingRow]:
    """Legacy list-only variant of `/rankings/reliability`."""
    return reliability_rankings(
        start=start,
        end=end,
        limit=limit,
        minutes=minutes,
        congestion_speed_threshold_kph=congestion_speed_threshold_kph,
        min_samples=min_samples,
        weight_mean_speed=weight_mean_speed,
        weight_speed_std=weight_speed_std,
        weight_congestion_frequency=weight_congestion_frequency,
    ).items
