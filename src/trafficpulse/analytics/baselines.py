from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class BaselineSpec:
    timestamp_column: str = "timestamp"
    segment_id_column: str = "segment_id"
    speed_column: str = "speed_kph"
    # Stratification controls
    include_weekday: bool = True
    include_hour: bool = True


def compute_segment_speed_baselines(
    observations: pd.DataFrame,
    *,
    spec: BaselineSpec = BaselineSpec(),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> pd.DataFrame:
    """Compute explainable baselines (median/IQR) for each segment (optionally stratified)."""

    if observations.empty:
        return pd.DataFrame(
            columns=[
                "segment_id",
                "weekday",
                "hour",
                "median_speed_kph",
                "p25_speed_kph",
                "p75_speed_kph",
                "iqr_speed_kph",
                "n_samples",
            ]
        )

    df = observations.copy()
    ts = spec.timestamp_column
    seg = spec.segment_id_column
    spd = spec.speed_column

    if ts not in df.columns or seg not in df.columns or spd not in df.columns:
        raise ValueError("observations missing required columns for baseline computation")

    df[ts] = pd.to_datetime(df[ts], errors="coerce", utc=True)
    df[seg] = df[seg].astype(str)
    df[spd] = pd.to_numeric(df[spd], errors="coerce")
    df = df.dropna(subset=[ts, seg, spd])

    if start is not None:
        df = df[df[ts] >= pd.Timestamp(start)]
    if end is not None:
        df = df[df[ts] < pd.Timestamp(end)]

    if df.empty:
        return pd.DataFrame(
            columns=[
                "segment_id",
                "weekday",
                "hour",
                "median_speed_kph",
                "p25_speed_kph",
                "p75_speed_kph",
                "iqr_speed_kph",
                "n_samples",
            ]
        )

    group_cols = [seg]
    if spec.include_weekday:
        df["weekday"] = df[ts].dt.weekday
        group_cols.append("weekday")
    else:
        df["weekday"] = pd.NA
    if spec.include_hour:
        df["hour"] = df[ts].dt.hour
        group_cols.append("hour")
    else:
        df["hour"] = pd.NA

    grouped = df.groupby(group_cols, as_index=False)
    out = grouped.agg(
        n_samples=(spd, "count"),
        median_speed_kph=(spd, "median"),
        p25_speed_kph=(spd, lambda s: float(s.quantile(0.25)) if not s.empty else float("nan")),
        p75_speed_kph=(spd, lambda s: float(s.quantile(0.75)) if not s.empty else float("nan")),
    )
    out["iqr_speed_kph"] = pd.to_numeric(out["p75_speed_kph"], errors="coerce") - pd.to_numeric(out["p25_speed_kph"], errors="coerce")
    out = out.rename(columns={seg: "segment_id"})

    # Keep a stable set of columns even when stratification is off.
    if "weekday" not in out.columns:
        out["weekday"] = pd.NA
    if "hour" not in out.columns:
        out["hour"] = pd.NA

    keep = [
        "segment_id",
        "weekday",
        "hour",
        "median_speed_kph",
        "p25_speed_kph",
        "p75_speed_kph",
        "iqr_speed_kph",
        "n_samples",
    ]
    out = out[keep].sort_values(["segment_id", "weekday", "hour"]).reset_index(drop=True)
    out = out.astype(object).where(pd.notnull(out), None)
    return out

