from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from trafficpulse.analytics.anomalies import (
    apply_anomaly_overrides,
    anomaly_spec_from_config,
    compute_anomaly_timeseries,
    spec_for_entity,
    summarize_anomaly_events,
)
from trafficpulse.analytics.corridors import aggregate_observations_to_corridors, load_corridors_csv
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


class AnomalyPoint(BaseModel):
    timestamp: datetime
    entity_id: str
    speed_kph: float
    baseline_mean_kph: Optional[float] = None
    baseline_std_kph: Optional[float] = None
    z_score: Optional[float] = None
    is_anomaly: bool


class AnomalyEvent(BaseModel):
    entity_id: str
    event_id: int
    start_time: datetime
    end_time: datetime
    n_points: int
    min_speed_kph: Optional[float] = None
    mean_speed_kph: Optional[float] = None
    min_z_score: Optional[float] = None
    mean_z_score: Optional[float] = None


def _load_observations(
    minutes: Optional[int],
    *,
    segment_ids: Optional[list[str]] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> pd.DataFrame:
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

    backend = duckdb_backend(config)
    if backend is not None and parquet_path.exists():
        return backend.query_observations(
            minutes=granularity_minutes,
            segment_ids=segment_ids,
            start=start,
            end=end,
        )

    if config.warehouse.enabled and parquet_path.exists():
        return load_parquet(parquet_path)
    return load_csv(csv_path)


@router.get("/anomalies", response_model=list[AnomalyPoint])
def segment_anomalies(
    segment_id: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    minutes: Optional[int] = Query(default=None, ge=1),
    window_points: Optional[int] = Query(default=None, ge=2),
    z_threshold: Optional[float] = Query(default=None, gt=0),
    direction: Optional[str] = Query(default=None),
    max_gap_minutes: Optional[int] = Query(default=None, ge=0),
    min_event_points: Optional[int] = Query(default=None, ge=1),
) -> list[AnomalyPoint]:
    config = get_config()
    try:
        spec = apply_anomaly_overrides(
            anomaly_spec_from_config(config),
            window_points=window_points,
            z_threshold=z_threshold,
            direction=direction,
            max_gap_minutes=max_gap_minutes,
            min_event_points=min_event_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    start_dt = parse_datetime(start)
    end_dt = parse_datetime(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    observations = _load_observations(minutes, segment_ids=[str(segment_id)], start=start_dt, end=end_dt)
    if observations.empty:
        return []

    enriched = compute_anomaly_timeseries(
        observations,
        spec,
        entity_id=str(segment_id),
        start=start_dt,
        end=end_dt,
    )
    if enriched.empty:
        return []

    enriched = enriched.rename(
        columns={
            "segment_id": "entity_id",
            "baseline_mean": "baseline_mean_kph",
            "baseline_std": "baseline_std_kph",
        }
    )
    enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], utc=True).dt.to_pydatetime()
    enriched = enriched.where(pd.notnull(enriched), None)
    return [AnomalyPoint(**record) for record in enriched.to_dict(orient="records")]


@router.get("/anomalies/events", response_model=list[AnomalyEvent])
def segment_anomaly_events(
    segment_id: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    minutes: Optional[int] = Query(default=None, ge=1),
    window_points: Optional[int] = Query(default=None, ge=2),
    z_threshold: Optional[float] = Query(default=None, gt=0),
    direction: Optional[str] = Query(default=None),
    max_gap_minutes: Optional[int] = Query(default=None, ge=0),
    min_event_points: Optional[int] = Query(default=None, ge=1),
) -> list[AnomalyEvent]:
    config = get_config()
    try:
        spec = apply_anomaly_overrides(
            anomaly_spec_from_config(config),
            window_points=window_points,
            z_threshold=z_threshold,
            direction=direction,
            max_gap_minutes=max_gap_minutes,
            min_event_points=min_event_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    start_dt = parse_datetime(start)
    end_dt = parse_datetime(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    observations = _load_observations(minutes, segment_ids=[str(segment_id)], start=start_dt, end=end_dt)
    if observations.empty:
        return []

    enriched = compute_anomaly_timeseries(
        observations,
        spec,
        entity_id=str(segment_id),
        start=start_dt,
        end=end_dt,
    )
    events = summarize_anomaly_events(enriched, spec)
    if events.empty:
        return []

    events = events.rename(
        columns={
            "segment_id": "entity_id",
            "min_value": "min_speed_kph",
            "mean_value": "mean_speed_kph",
        }
    )
    events["start_time"] = pd.to_datetime(events["start_time"], utc=True).dt.to_pydatetime()
    events["end_time"] = pd.to_datetime(events["end_time"], utc=True).dt.to_pydatetime()
    events = events.where(pd.notnull(events), None)
    return [AnomalyEvent(**record) for record in events.to_dict(orient="records")]


@router.get("/anomalies/corridors", response_model=list[AnomalyPoint])
def corridor_anomalies(
    corridor_id: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    minutes: Optional[int] = Query(default=None, ge=1),
    window_points: Optional[int] = Query(default=None, ge=2),
    z_threshold: Optional[float] = Query(default=None, gt=0),
    direction: Optional[str] = Query(default=None),
    max_gap_minutes: Optional[int] = Query(default=None, ge=0),
    min_event_points: Optional[int] = Query(default=None, ge=1),
) -> list[AnomalyPoint]:
    config = get_config()
    try:
        spec = apply_anomaly_overrides(
            anomaly_spec_from_config(config),
            window_points=window_points,
            z_threshold=z_threshold,
            direction=direction,
            max_gap_minutes=max_gap_minutes,
            min_event_points=min_event_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    start_dt = parse_datetime(start)
    end_dt = parse_datetime(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    corridors = load_corridors_csv(config.analytics.corridors.corridors_csv)
    corridors = corridors[corridors["corridor_id"].astype(str) == str(corridor_id)]
    if corridors.empty:
        raise HTTPException(status_code=404, detail="corridor_id not found in corridors.csv.")

    segment_ids = corridors["segment_id"].astype(str).unique().tolist()
    observations = _load_observations(minutes, segment_ids=segment_ids, start=start_dt, end=end_dt)
    if observations.empty:
        return []

    corridor_ts = aggregate_observations_to_corridors(
        observations,
        corridors,
        speed_weighting=config.analytics.corridors.speed_weighting,
        weight_column=config.analytics.corridors.weight_column,
    )
    if corridor_ts.empty:
        return []

    corridor_spec = spec_for_entity(spec, entity_id_column="corridor_id")
    enriched = compute_anomaly_timeseries(
        corridor_ts,
        corridor_spec,
        entity_id=str(corridor_id),
        start=start_dt,
        end=end_dt,
    )
    if enriched.empty:
        return []

    enriched = enriched.rename(
        columns={
            "corridor_id": "entity_id",
            "baseline_mean": "baseline_mean_kph",
            "baseline_std": "baseline_std_kph",
        }
    )
    enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], utc=True).dt.to_pydatetime()
    enriched = enriched.where(pd.notnull(enriched), None)
    return [AnomalyPoint(**record) for record in enriched.to_dict(orient="records")]


@router.get("/anomalies/corridors/events", response_model=list[AnomalyEvent])
def corridor_anomaly_events(
    corridor_id: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    minutes: Optional[int] = Query(default=None, ge=1),
    window_points: Optional[int] = Query(default=None, ge=2),
    z_threshold: Optional[float] = Query(default=None, gt=0),
    direction: Optional[str] = Query(default=None),
    max_gap_minutes: Optional[int] = Query(default=None, ge=0),
    min_event_points: Optional[int] = Query(default=None, ge=1),
) -> list[AnomalyEvent]:
    config = get_config()
    try:
        spec = apply_anomaly_overrides(
            anomaly_spec_from_config(config),
            window_points=window_points,
            z_threshold=z_threshold,
            direction=direction,
            max_gap_minutes=max_gap_minutes,
            min_event_points=min_event_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    start_dt = parse_datetime(start)
    end_dt = parse_datetime(end)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="'end' must be greater than 'start'.")

    corridors = load_corridors_csv(config.analytics.corridors.corridors_csv)
    corridors = corridors[corridors["corridor_id"].astype(str) == str(corridor_id)]
    if corridors.empty:
        raise HTTPException(status_code=404, detail="corridor_id not found in corridors.csv.")

    segment_ids = corridors["segment_id"].astype(str).unique().tolist()
    observations = _load_observations(minutes, segment_ids=segment_ids, start=start_dt, end=end_dt)
    if observations.empty:
        return []

    corridor_ts = aggregate_observations_to_corridors(
        observations,
        corridors,
        speed_weighting=config.analytics.corridors.speed_weighting,
        weight_column=config.analytics.corridors.weight_column,
    )
    if corridor_ts.empty:
        return []

    corridor_spec = spec_for_entity(spec, entity_id_column="corridor_id")
    enriched = compute_anomaly_timeseries(
        corridor_ts,
        corridor_spec,
        entity_id=str(corridor_id),
        start=start_dt,
        end=end_dt,
    )
    events = summarize_anomaly_events(enriched, corridor_spec)
    if events.empty:
        return []

    events = events.rename(
        columns={
            "corridor_id": "entity_id",
            "min_value": "min_speed_kph",
            "mean_value": "mean_speed_kph",
        }
    )
    events["start_time"] = pd.to_datetime(events["start_time"], utc=True).dt.to_pydatetime()
    events["end_time"] = pd.to_datetime(events["end_time"], utc=True).dt.to_pydatetime()
    events = events.where(pd.notnull(events), None)
    return [AnomalyEvent(**record) for record in events.to_dict(orient="records")]
