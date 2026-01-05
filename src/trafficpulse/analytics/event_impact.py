from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd

from trafficpulse.analytics.corridors import aggregate_observations_to_corridors
from trafficpulse.settings import AppConfig, get_config
from trafficpulse.utils.time import to_utc


EARTH_RADIUS_M = 6_371_000.0


@dataclass(frozen=True)
class EventImpactSpec:
    default_window_hours: int
    radius_meters: float
    max_segments: int
    baseline_window_minutes: int
    end_time_fallback_minutes: int
    recovery_horizon_minutes: int
    recovery_ratio: float
    speed_weighting: str
    min_baseline_points: int
    min_event_points: int

    timestamp_column: str = "timestamp"
    segment_id_column: str = "segment_id"
    speed_column: str = "speed_kph"
    volume_column: str = "volume"

    def normalized(self) -> "EventImpactSpec":
        weighting = (self.speed_weighting or "volume").strip().lower()
        if weighting not in {"volume", "equal"}:
            raise ValueError("analytics.event_impact.speed_weighting must be one of: volume, equal")

        if self.default_window_hours <= 0:
            raise ValueError("analytics.event_impact.default_window_hours must be > 0")
        if self.radius_meters <= 0:
            raise ValueError("analytics.event_impact.radius_meters must be > 0")
        if self.max_segments <= 0:
            raise ValueError("analytics.event_impact.max_segments must be > 0")
        if self.baseline_window_minutes <= 0:
            raise ValueError("analytics.event_impact.baseline_window_minutes must be > 0")
        if self.end_time_fallback_minutes <= 0:
            raise ValueError("analytics.event_impact.end_time_fallback_minutes must be > 0")
        if self.recovery_horizon_minutes <= 0:
            raise ValueError("analytics.event_impact.recovery_horizon_minutes must be > 0")
        if not (0 < self.recovery_ratio <= 1.0):
            raise ValueError("analytics.event_impact.recovery_ratio must be within (0, 1]")
        if self.min_baseline_points <= 0:
            raise ValueError("analytics.event_impact.min_baseline_points must be > 0")
        if self.min_event_points <= 0:
            raise ValueError("analytics.event_impact.min_event_points must be > 0")

        return replace(self, speed_weighting=weighting)


def event_impact_spec_from_config(config: Optional[AppConfig] = None) -> EventImpactSpec:
    resolved = config or get_config()
    section = resolved.analytics.event_impact
    return EventImpactSpec(
        default_window_hours=int(section.default_window_hours),
        radius_meters=float(section.radius_meters),
        max_segments=int(section.max_segments),
        baseline_window_minutes=int(section.baseline_window_minutes),
        end_time_fallback_minutes=int(section.end_time_fallback_minutes),
        recovery_horizon_minutes=int(section.recovery_horizon_minutes),
        recovery_ratio=float(section.recovery_ratio),
        speed_weighting=str(section.speed_weighting),
        min_baseline_points=int(section.min_baseline_points),
        min_event_points=int(section.min_event_points),
    ).normalized()


def haversine_distance_meters(
    lat_deg: float, lon_deg: float, other_lat_deg: np.ndarray, other_lon_deg: np.ndarray
) -> np.ndarray:
    lat1 = np.radians(lat_deg)
    lon1 = np.radians(lon_deg)
    lat2 = np.radians(other_lat_deg)
    lon2 = np.radians(other_lon_deg)

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return EARTH_RADIUS_M * c


def select_nearby_segments(
    segments: pd.DataFrame,
    *,
    lat: float,
    lon: float,
    radius_meters: float,
    max_segments: int,
) -> pd.DataFrame:
    if segments.empty:
        return pd.DataFrame(columns=["segment_id", "distance_m", "lat", "lon"])

    df = segments.copy()
    required = {"segment_id", "lat", "lon"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Segments dataset is missing required columns: {missing}")

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["segment_id"] = df["segment_id"].astype(str)
    df = df.dropna(subset=["segment_id", "lat", "lon"])
    if df.empty:
        return pd.DataFrame(columns=["segment_id", "distance_m", "lat", "lon"])

    distances = haversine_distance_meters(
        float(lat), float(lon), df["lat"].to_numpy(), df["lon"].to_numpy()
    )
    df = df.assign(distance_m=distances)
    df = df[df["distance_m"] <= float(radius_meters)]
    df = df.sort_values("distance_m").head(int(max_segments)).reset_index(drop=True)
    return df[["segment_id", "distance_m", "lat", "lon"]]


def compute_event_impact(
    event: pd.Series,
    *,
    observations: pd.DataFrame,
    segments: pd.DataFrame,
    spec: EventImpactSpec,
    radius_meters: Optional[float] = None,
    max_segments: Optional[int] = None,
    minutes: Optional[int] = None,
    include_timeseries: bool = False,
) -> dict[str, Any]:
    spec = spec.normalized()

    event_id = str(event.get("event_id"))
    start_time = event.get("start_time")
    end_time = event.get("end_time")

    if pd.isna(start_time):
        raise ValueError("event.start_time is required.")
    start_dt = to_utc(start_time.to_pydatetime() if hasattr(start_time, "to_pydatetime") else start_time)

    if pd.isna(end_time) or end_time is None:
        end_dt = start_dt + timedelta(minutes=int(spec.end_time_fallback_minutes))
    else:
        end_dt = to_utc(end_time.to_pydatetime() if hasattr(end_time, "to_pydatetime") else end_time)
        if end_dt <= start_dt:
            end_dt = start_dt + timedelta(minutes=int(spec.end_time_fallback_minutes))

    event_lat = event.get("lat")
    event_lon = event.get("lon")
    if pd.isna(event_lat) or pd.isna(event_lon):
        raise ValueError("event.lat and event.lon are required for impact analysis.")

    nearby = select_nearby_segments(
        segments,
        lat=float(event_lat),
        lon=float(event_lon),
        radius_meters=float(radius_meters or spec.radius_meters),
        max_segments=int(max_segments or spec.max_segments),
    )
    if nearby.empty:
        raise ValueError("No nearby segments found within radius.")

    baseline_start = start_dt - timedelta(minutes=int(spec.baseline_window_minutes))
    analysis_end = end_dt + timedelta(minutes=int(spec.recovery_horizon_minutes))

    obs = observations.copy()
    ts_col = spec.timestamp_column
    seg_col = spec.segment_id_column
    speed_col = spec.speed_column

    missing = sorted({ts_col, seg_col, speed_col} - set(obs.columns))
    if missing:
        raise ValueError(f"Observations dataset is missing required columns: {missing}")

    obs[ts_col] = pd.to_datetime(obs[ts_col], errors="coerce", utc=True)
    obs[seg_col] = obs[seg_col].astype(str)
    obs[speed_col] = pd.to_numeric(obs[speed_col], errors="coerce")
    obs = obs.dropna(subset=[ts_col, seg_col, speed_col])

    segment_ids = set(nearby["segment_id"].astype(str).tolist())
    obs = obs[obs[seg_col].isin(segment_ids)]
    obs = obs[(obs[ts_col] >= pd.Timestamp(baseline_start)) & (obs[ts_col] < pd.Timestamp(analysis_end))]
    if obs.empty:
        raise ValueError("No observations found for nearby segments in the analysis window.")

    corridor_membership = pd.DataFrame(
        {
            "corridor_id": [event_id] * len(segment_ids),
            "corridor_name": [None] * len(segment_ids),
            "segment_id": list(segment_ids),
            "weight": [1.0] * len(segment_ids),
        }
    )

    corridor_ts = aggregate_observations_to_corridors(
        obs,
        corridor_membership,
        speed_weighting=spec.speed_weighting,
        weight_column="weight",
        timestamp_column=ts_col,
        segment_id_column=seg_col,
        speed_column=speed_col,
        volume_column=spec.volume_column,
    )
    if corridor_ts.empty:
        raise ValueError("Failed to aggregate corridor time series for the event.")

    corridor_ts = corridor_ts.sort_values(ts_col).reset_index(drop=True)
    corridor_ts = corridor_ts[(corridor_ts[ts_col] >= pd.Timestamp(baseline_start)) & (corridor_ts[ts_col] < pd.Timestamp(analysis_end))]
    corridor_ts = corridor_ts.dropna(subset=[speed_col])

    baseline_mask = (corridor_ts[ts_col] >= pd.Timestamp(baseline_start)) & (corridor_ts[ts_col] < pd.Timestamp(start_dt))
    event_mask = (corridor_ts[ts_col] >= pd.Timestamp(start_dt)) & (corridor_ts[ts_col] < pd.Timestamp(end_dt))
    post_mask = (corridor_ts[ts_col] >= pd.Timestamp(end_dt)) & (corridor_ts[ts_col] < pd.Timestamp(analysis_end))

    baseline = corridor_ts[baseline_mask]
    during = corridor_ts[event_mask]
    post = corridor_ts[post_mask]

    baseline_n = int(len(baseline))
    event_n = int(len(during))

    baseline_mean = float(baseline[speed_col].mean()) if baseline_n else np.nan
    baseline_std = float(baseline[speed_col].std(ddof=0)) if baseline_n else np.nan
    event_mean = float(during[speed_col].mean()) if event_n else np.nan
    event_min = float(during[speed_col].min()) if event_n else np.nan

    event_min_time: Optional[datetime] = None
    if event_n:
        idx = during[speed_col].idxmin()
        if idx is not None and idx in corridor_ts.index:
            event_min_time = corridor_ts.loc[idx, ts_col].to_pydatetime()

    speed_delta_mean = event_mean - baseline_mean if np.isfinite(event_mean) and np.isfinite(baseline_mean) else np.nan
    speed_ratio_mean = event_mean / baseline_mean if np.isfinite(event_mean) and np.isfinite(baseline_mean) and baseline_mean > 0 else np.nan

    recovered_at: Optional[datetime] = None
    recovery_minutes: Optional[float] = None
    if post_mask.any() and np.isfinite(baseline_mean) and baseline_mean > 0:
        threshold_speed = baseline_mean * float(spec.recovery_ratio)
        recovered = post[post[speed_col] >= threshold_speed]
        if not recovered.empty:
            recovered_at = recovered.iloc[0][ts_col].to_pydatetime()
            recovery_minutes = (recovered_at - end_dt).total_seconds() / 60.0

    enough_baseline = baseline_n >= int(spec.min_baseline_points)
    enough_event = event_n >= int(spec.min_event_points)

    timeseries = None
    if include_timeseries:
        keep_cols = [c for c in [ts_col, speed_col, spec.volume_column, "occupancy_pct"] if c in corridor_ts.columns]
        series_df = corridor_ts[keep_cols].rename(columns={ts_col: "timestamp"})
        timeseries = series_df.to_dict(orient="records")

    return {
        "event_id": event_id,
        "start_time": start_dt,
        "end_time": end_dt,
        "analysis_window_start": baseline_start,
        "analysis_window_end": analysis_end,
        "n_segments": int(len(segment_ids)),
        "baseline_n_points": baseline_n,
        "event_n_points": event_n,
        "baseline_mean_speed_kph": baseline_mean if np.isfinite(baseline_mean) else None,
        "baseline_std_speed_kph": baseline_std if np.isfinite(baseline_std) else None,
        "event_mean_speed_kph": event_mean if np.isfinite(event_mean) else None,
        "event_min_speed_kph": event_min if np.isfinite(event_min) else None,
        "event_min_time": event_min_time,
        "speed_delta_mean_kph": speed_delta_mean if np.isfinite(speed_delta_mean) else None,
        "speed_ratio_mean": speed_ratio_mean if np.isfinite(speed_ratio_mean) else None,
        "recovered_at": recovered_at,
        "recovery_minutes": recovery_minutes,
        "enough_baseline": enough_baseline,
        "enough_event": enough_event,
        "affected_segments": nearby.to_dict(orient="records"),
        "timeseries": timeseries,
        "minutes": minutes,
    }


def compute_event_impacts(
    events: pd.DataFrame,
    *,
    observations: pd.DataFrame,
    segments: pd.DataFrame,
    spec: EventImpactSpec,
    limit_events: Optional[int] = None,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()

    df = events.copy()
    if "event_id" not in df.columns or "start_time" not in df.columns:
        raise ValueError("events dataset must contain event_id and start_time columns.")

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
    df = df.dropna(subset=["event_id", "start_time"]).sort_values("start_time")

    if limit_events is not None:
        df = df.tail(int(limit_events))

    rows: list[dict[str, Any]] = []
    for _, event in df.iterrows():
        try:
            impact = compute_event_impact(
                event,
                observations=observations,
                segments=segments,
                spec=spec,
                include_timeseries=False,
            )
        except Exception:
            continue
        rows.append(
            {
                "event_id": impact["event_id"],
                "start_time": impact["start_time"],
                "end_time": impact["end_time"],
                "n_segments": impact["n_segments"],
                "baseline_mean_speed_kph": impact["baseline_mean_speed_kph"],
                "event_mean_speed_kph": impact["event_mean_speed_kph"],
                "event_min_speed_kph": impact["event_min_speed_kph"],
                "speed_delta_mean_kph": impact["speed_delta_mean_kph"],
                "speed_ratio_mean": impact["speed_ratio_mean"],
                "recovery_minutes": impact["recovery_minutes"],
                "enough_baseline": impact["enough_baseline"],
                "enough_event": impact["enough_event"],
            }
        )

    return pd.DataFrame(rows)
