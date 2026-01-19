from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class SegmentQualitySpec:
    timestamp_column: str = "timestamp"
    segment_id_column: str = "segment_id"
    speed_column: str = "speed_kph"
    volume_column: str = "volume"
    occupancy_column: str = "occupancy_pct"


def compute_segment_quality(
    observations: pd.DataFrame,
    *,
    spec: SegmentQualitySpec = SegmentQualitySpec(),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    expected_interval_minutes: int | None = None,
) -> pd.DataFrame:
    """Compute per-segment quality/coverage metrics for a window."""

    if observations.empty:
        return pd.DataFrame(
            columns=[
                "segment_id",
                "n_samples",
                "coverage_pct",
                "speed_missing_pct",
                "volume_missing_pct",
                "occupancy_missing_pct",
                "speed_std_kph",
            ]
        )

    df = observations.copy()
    ts = spec.timestamp_column
    seg = spec.segment_id_column

    if ts not in df.columns or seg not in df.columns:
        raise ValueError("observations missing required columns (timestamp, segment_id)")

    df[ts] = pd.to_datetime(df[ts], errors="coerce", utc=True)
    df[seg] = df[seg].astype(str)
    df = df.dropna(subset=[ts, seg])

    if start is not None:
        df = df[df[ts] >= pd.Timestamp(start)]
    if end is not None:
        df = df[df[ts] < pd.Timestamp(end)]

    if df.empty:
        return pd.DataFrame(
            columns=[
                "segment_id",
                "n_samples",
                "coverage_pct",
                "speed_missing_pct",
                "volume_missing_pct",
                "occupancy_missing_pct",
                "speed_std_kph",
            ]
        )

    # Normalize numeric columns for missingness stats.
    for col in [spec.speed_column, spec.volume_column, spec.occupancy_column]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA

    grouped = df.groupby(seg, as_index=False)
    out = grouped.agg(
        n_samples=(ts, "count"),
        speed_missing=(spec.speed_column, lambda x: int(pd.to_numeric(x, errors="coerce").isna().sum())),
        volume_missing=(spec.volume_column, lambda x: int(pd.to_numeric(x, errors="coerce").isna().sum())),
        occupancy_missing=(spec.occupancy_column, lambda x: int(pd.to_numeric(x, errors="coerce").isna().sum())),
        speed_std_kph=(spec.speed_column, "std"),
        ts_min=(ts, "min"),
        ts_max=(ts, "max"),
    )
    out["speed_std_kph"] = pd.to_numeric(out["speed_std_kph"], errors="coerce").fillna(0.0)

    out["speed_missing_pct"] = out["speed_missing"] / out["n_samples"].where(out["n_samples"] > 0, 1) * 100.0
    out["volume_missing_pct"] = out["volume_missing"] / out["n_samples"].where(out["n_samples"] > 0, 1) * 100.0
    out["occupancy_missing_pct"] = out["occupancy_missing"] / out["n_samples"].where(out["n_samples"] > 0, 1) * 100.0

    if expected_interval_minutes is not None and start is not None and end is not None:
        total_minutes = (pd.Timestamp(end) - pd.Timestamp(start)).total_seconds() / 60.0
        expected_points = max(1, int(total_minutes // float(expected_interval_minutes)))
        out["expected_points"] = expected_points
        out["coverage_pct"] = out["n_samples"] / expected_points * 100.0
    else:
        out["coverage_pct"] = pd.NA
        out["expected_points"] = pd.NA

    keep = [
        "segment_id",
        "n_samples",
        "expected_points",
        "coverage_pct",
        "speed_missing_pct",
        "volume_missing_pct",
        "occupancy_missing_pct",
        "speed_std_kph",
    ]
    out = out.rename(columns={seg: "segment_id"})
    out = out[keep].sort_values("segment_id").reset_index(drop=True)
    out = out.astype(object).where(pd.notnull(out), None)
    return out

