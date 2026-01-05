from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Optional

import pandas as pd

from trafficpulse.settings import AppConfig, get_config
from trafficpulse.utils.time import to_utc


@dataclass(frozen=True)
class AnomalySpec:
    method: str
    window_points: int
    z_threshold: float
    direction: str
    max_gap_minutes: int
    min_event_points: int
    timestamp_column: str = "timestamp"
    entity_id_column: str = "segment_id"
    value_column: str = "speed_kph"

    def normalized(self) -> "AnomalySpec":
        method = (self.method or "").strip().lower()
        direction = (self.direction or "low").strip().lower()
        if method not in {"rolling_zscore"}:
            raise ValueError("analytics.anomalies.method must be 'rolling_zscore'")
        if direction not in {"low", "high", "both"}:
            raise ValueError("analytics.anomalies.direction must be one of: low, high, both")
        if int(self.window_points) <= 1:
            raise ValueError("analytics.anomalies.window_points must be > 1")
        if float(self.z_threshold) <= 0:
            raise ValueError("analytics.anomalies.z_threshold must be > 0")
        if int(self.max_gap_minutes) < 0:
            raise ValueError("analytics.anomalies.max_gap_minutes must be >= 0")
        if int(self.min_event_points) <= 0:
            raise ValueError("analytics.anomalies.min_event_points must be > 0")

        return replace(
            self,
            method=method,
            window_points=int(self.window_points),
            z_threshold=float(self.z_threshold),
            direction=direction,
            max_gap_minutes=int(self.max_gap_minutes),
            min_event_points=int(self.min_event_points),
        )


def anomaly_spec_from_config(config: Optional[AppConfig] = None) -> AnomalySpec:
    resolved = config or get_config()
    section = resolved.analytics.anomalies
    return AnomalySpec(
        method=section.method,
        window_points=section.window_points,
        z_threshold=section.z_threshold,
        direction=section.direction,
        max_gap_minutes=section.max_gap_minutes,
        min_event_points=section.min_event_points,
    ).normalized()


def compute_anomaly_timeseries(
    observations: pd.DataFrame,
    spec: AnomalySpec,
    *,
    entity_id: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> pd.DataFrame:
    spec = spec.normalized()

    ts_col = spec.timestamp_column
    id_col = spec.entity_id_column
    val_col = spec.value_column

    if observations.empty:
        return pd.DataFrame(
            columns=[id_col, ts_col, val_col, "baseline_mean", "baseline_std", "z_score", "is_anomaly"]
        )

    missing = sorted({ts_col, id_col, val_col} - set(observations.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = observations[[id_col, ts_col, val_col]].copy()
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df[id_col] = df[id_col].astype(str)
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
    df = df.dropna(subset=[id_col, ts_col, val_col])

    if entity_id is not None:
        df = df[df[id_col] == str(entity_id)]

    if start is not None:
        df = df[df[ts_col] >= pd.Timestamp(to_utc(start))]
    if end is not None:
        df = df[df[ts_col] < pd.Timestamp(to_utc(end))]

    if df.empty:
        return pd.DataFrame(
            columns=[id_col, ts_col, val_col, "baseline_mean", "baseline_std", "z_score", "is_anomaly"]
        )

    if entity_id is not None:
        df = df.sort_values(ts_col).reset_index(drop=True)
        return _rolling_zscore_single(df, spec)

    df = df.sort_values([id_col, ts_col]).reset_index(drop=True)
    return df.groupby(id_col, group_keys=False).apply(lambda g: _rolling_zscore_single(g, spec))


def summarize_anomaly_events(anomaly_timeseries: pd.DataFrame, spec: AnomalySpec) -> pd.DataFrame:
    spec = spec.normalized()

    id_col = spec.entity_id_column
    ts_col = spec.timestamp_column
    val_col = spec.value_column

    if anomaly_timeseries.empty:
        return pd.DataFrame(
            columns=[
                id_col,
                "event_id",
                "start_time",
                "end_time",
                "n_points",
                "min_value",
                "mean_value",
                "min_z_score",
                "mean_z_score",
            ]
        )

    required = {id_col, ts_col, val_col, "z_score", "is_anomaly"}
    missing = sorted(required - set(anomaly_timeseries.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = anomaly_timeseries[[id_col, ts_col, val_col, "z_score", "is_anomaly"]].copy()
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df[id_col] = df[id_col].astype(str)
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
    df["z_score"] = pd.to_numeric(df["z_score"], errors="coerce")
    df["is_anomaly"] = df["is_anomaly"].fillna(False).astype(bool)
    df = df.dropna(subset=[id_col, ts_col])
    df = df[df["is_anomaly"]]

    if df.empty:
        return pd.DataFrame(
            columns=[
                id_col,
                "event_id",
                "start_time",
                "end_time",
                "n_points",
                "min_value",
                "mean_value",
                "min_z_score",
                "mean_z_score",
            ]
        )

    df = df.sort_values([id_col, ts_col]).reset_index(drop=True)
    max_gap = pd.Timedelta(minutes=int(spec.max_gap_minutes))

    df["_prev_ts"] = df.groupby(id_col)[ts_col].shift(1)
    df["_new_event"] = df["_prev_ts"].isna() | ((df[ts_col] - df["_prev_ts"]) > max_gap)
    df["event_id"] = df.groupby(id_col)["_new_event"].cumsum().astype(int)

    grouped = df.groupby([id_col, "event_id"], as_index=False)
    events = grouped.agg(
        start_time=(ts_col, "min"),
        end_time=(ts_col, "max"),
        n_points=(ts_col, "count"),
        min_value=(val_col, "min"),
        mean_value=(val_col, "mean"),
        min_z_score=("z_score", "min"),
        mean_z_score=("z_score", "mean"),
    )

    events = events[events["n_points"] >= int(spec.min_event_points)]
    events = events.sort_values([id_col, "start_time"]).reset_index(drop=True)
    return events


def spec_for_entity(
    spec: AnomalySpec,
    *,
    entity_id_column: str,
    value_column: str = "speed_kph",
    timestamp_column: str = "timestamp",
) -> AnomalySpec:
    spec = spec.normalized()
    return replace(
        spec,
        entity_id_column=entity_id_column,
        value_column=value_column,
        timestamp_column=timestamp_column,
    )


def _rolling_zscore_single(df: pd.DataFrame, spec: AnomalySpec) -> pd.DataFrame:
    ts_col = spec.timestamp_column
    val_col = spec.value_column

    df = df.sort_values(ts_col).copy()
    rolling = df[val_col].rolling(window=spec.window_points, min_periods=spec.window_points)
    baseline_mean = rolling.mean().shift(1)
    baseline_std = rolling.std(ddof=0).shift(1)

    z = (df[val_col] - baseline_mean) / baseline_std

    df["baseline_mean"] = baseline_mean
    df["baseline_std"] = baseline_std
    df["z_score"] = z

    threshold = float(spec.z_threshold)
    if spec.direction == "low":
        is_anomaly = df["z_score"] <= -threshold
    elif spec.direction == "high":
        is_anomaly = df["z_score"] >= threshold
    else:
        is_anomaly = df["z_score"].abs() >= threshold

    is_anomaly = is_anomaly.fillna(False)
    is_anomaly.loc[df["baseline_std"].isna() | (df["baseline_std"] <= 0)] = False
    df["is_anomaly"] = is_anomaly.astype(bool)
    return df


def apply_anomaly_overrides(
    spec: AnomalySpec,
    *,
    window_points: Optional[int] = None,
    z_threshold: Optional[float] = None,
    direction: Optional[str] = None,
    max_gap_minutes: Optional[int] = None,
    min_event_points: Optional[int] = None,
) -> AnomalySpec:
    updated = spec
    if window_points is not None:
        updated = replace(updated, window_points=int(window_points))
    if z_threshold is not None:
        updated = replace(updated, z_threshold=float(z_threshold))
    if direction is not None:
        updated = replace(updated, direction=str(direction))
    if max_gap_minutes is not None:
        updated = replace(updated, max_gap_minutes=int(max_gap_minutes))
    if min_event_points is not None:
        updated = replace(updated, min_event_points=int(min_event_points))
    return updated.normalized()
