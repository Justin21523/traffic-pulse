from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class AlertSpec:
    # Trigger when speed is below (baseline_median - k * iqr)
    k_iqr: float = 1.5
    # Require at least N consecutive points for an alert.
    min_consecutive_points: int = 3


def detect_congestion_alerts(
    observations: pd.DataFrame,
    baselines: pd.DataFrame,
    *,
    spec: AlertSpec = AlertSpec(),
) -> pd.DataFrame:
    """Detect simple explainable congestion alerts per segment using baseline median/IQR."""

    if observations.empty or baselines.empty:
        return pd.DataFrame(columns=["segment_id", "start_time", "end_time", "points", "threshold_kph"])

    obs = observations.copy()
    base = baselines.copy()

    required_obs = {"timestamp", "segment_id", "speed_kph"}
    required_base = {"segment_id", "median_speed_kph", "iqr_speed_kph"}
    if not required_obs.issubset(set(obs.columns)) or not required_base.issubset(set(base.columns)):
        return pd.DataFrame(columns=["segment_id", "start_time", "end_time", "points", "threshold_kph"])

    obs["timestamp"] = pd.to_datetime(obs["timestamp"], errors="coerce", utc=True)
    obs["segment_id"] = obs["segment_id"].astype(str)
    obs["speed_kph"] = pd.to_numeric(obs["speed_kph"], errors="coerce")
    obs = obs.dropna(subset=["timestamp", "segment_id", "speed_kph"])

    base["segment_id"] = base["segment_id"].astype(str)
    base["median_speed_kph"] = pd.to_numeric(base["median_speed_kph"], errors="coerce")
    base["iqr_speed_kph"] = pd.to_numeric(base["iqr_speed_kph"], errors="coerce").fillna(0.0)
    base = base.dropna(subset=["segment_id", "median_speed_kph"])

    # Use the simplest baseline (no weekday/hour stratification): take max n_samples row per segment if present.
    if "weekday" in base.columns or "hour" in base.columns:
        if "n_samples" in base.columns:
            base = base.sort_values(["segment_id", "n_samples"], ascending=[True, False]).drop_duplicates(subset=["segment_id"])
        else:
            base = base.drop_duplicates(subset=["segment_id"])

    merged = obs.merge(base[["segment_id", "median_speed_kph", "iqr_speed_kph"]], on="segment_id", how="left")
    merged = merged.dropna(subset=["median_speed_kph"])
    if merged.empty:
        return pd.DataFrame(columns=["segment_id", "start_time", "end_time", "points", "threshold_kph"])

    merged["threshold_kph"] = merged["median_speed_kph"] - float(spec.k_iqr) * merged["iqr_speed_kph"]
    merged["is_congested"] = merged["speed_kph"] < merged["threshold_kph"]
    merged = merged.sort_values(["segment_id", "timestamp"]).reset_index(drop=True)

    alerts: list[dict[str, object]] = []
    for seg_id, g in merged.groupby("segment_id"):
        current_start = None
        current_points = 0
        current_threshold = None
        last_ts = None
        for _, row in g.iterrows():
            is_cong = bool(row["is_congested"])
            ts = row["timestamp"]
            if is_cong:
                if current_start is None:
                    current_start = ts
                    current_points = 1
                    current_threshold = row["threshold_kph"]
                else:
                    current_points += 1
                last_ts = ts
            else:
                if current_start is not None and current_points >= int(spec.min_consecutive_points):
                    alerts.append(
                        {
                            "segment_id": str(seg_id),
                            "start_time": current_start,
                            "end_time": last_ts,
                            "points": int(current_points),
                            "threshold_kph": float(current_threshold) if current_threshold is not None else None,
                        }
                    )
                current_start = None
                current_points = 0
                current_threshold = None
                last_ts = None

        if current_start is not None and current_points >= int(spec.min_consecutive_points):
            alerts.append(
                {
                    "segment_id": str(seg_id),
                    "start_time": current_start,
                    "end_time": last_ts,
                    "points": int(current_points),
                    "threshold_kph": float(current_threshold) if current_threshold is not None else None,
                }
            )

    if not alerts:
        return pd.DataFrame(columns=["segment_id", "start_time", "end_time", "points", "threshold_kph"])
    out = pd.DataFrame(alerts).sort_values(["start_time", "segment_id"]).reset_index(drop=True)
    out = out.astype(object).where(pd.notnull(out), None)
    return out

