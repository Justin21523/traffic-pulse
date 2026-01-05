from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Optional

import pandas as pd

from trafficpulse.analytics.reliability import ReliabilitySpec, compute_reliability_rankings


REQUIRED_CORRIDOR_COLUMNS = {"corridor_id", "segment_id"}
OPTIONAL_CORRIDOR_COLUMNS = {"corridor_name", "weight"}


def load_corridors_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = sorted(REQUIRED_CORRIDOR_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"Corridors CSV is missing required columns: {missing}")

    df = df.copy()
    df["corridor_id"] = df["corridor_id"].astype(str)
    df["segment_id"] = df["segment_id"].astype(str)

    if "corridor_name" not in df.columns:
        df["corridor_name"] = pd.NA
    if "weight" not in df.columns:
        df["weight"] = 1.0

    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(1.0)
    return df


def corridor_metadata(corridors: pd.DataFrame) -> pd.DataFrame:
    if corridors.empty:
        return pd.DataFrame(columns=["corridor_id", "corridor_name", "segment_count"])

    def first_non_null(series: pd.Series) -> Optional[str]:
        non_null = series.dropna().astype(str)
        return non_null.iloc[0] if not non_null.empty else None

    grouped = corridors.groupby("corridor_id", as_index=False)
    meta = grouped.agg(
        corridor_name=("corridor_name", first_non_null),
        segment_count=("segment_id", "nunique"),
    )
    meta = meta.sort_values("corridor_id").reset_index(drop=True)
    return meta


def aggregate_observations_to_corridors(
    observations: pd.DataFrame,
    corridors: pd.DataFrame,
    *,
    speed_weighting: str = "volume",
    weight_column: str = "weight",
    timestamp_column: str = "timestamp",
    segment_id_column: str = "segment_id",
    speed_column: str = "speed_kph",
    volume_column: str = "volume",
    occupancy_column: str = "occupancy_pct",
) -> pd.DataFrame:
    if observations.empty or corridors.empty:
        return pd.DataFrame(
            columns=["corridor_id", timestamp_column, speed_column, volume_column, occupancy_column]
        )

    obs = observations.copy()
    if timestamp_column not in obs.columns or segment_id_column not in obs.columns:
        raise ValueError(f"Observations must include columns: {timestamp_column}, {segment_id_column}")

    obs[timestamp_column] = pd.to_datetime(obs[timestamp_column], errors="coerce", utc=True)
    obs[segment_id_column] = obs[segment_id_column].astype(str)
    obs = obs.dropna(subset=[timestamp_column, segment_id_column])

    if speed_column in obs.columns:
        obs[speed_column] = pd.to_numeric(obs[speed_column], errors="coerce")
    if volume_column in obs.columns:
        obs[volume_column] = pd.to_numeric(obs[volume_column], errors="coerce")
    if occupancy_column in obs.columns:
        obs[occupancy_column] = pd.to_numeric(obs[occupancy_column], errors="coerce")

    corr = corridors.copy()
    corr["corridor_id"] = corr["corridor_id"].astype(str)
    corr["segment_id"] = corr["segment_id"].astype(str)
    if "corridor_name" not in corr.columns:
        corr["corridor_name"] = pd.NA
    if weight_column not in corr.columns:
        corr[weight_column] = 1.0
    corr[weight_column] = pd.to_numeric(corr[weight_column], errors="coerce").fillna(1.0)

    joined = obs.merge(
        corr[["corridor_id", "corridor_name", "segment_id", weight_column]],
        left_on=segment_id_column,
        right_on="segment_id",
        how="inner",
    )
    if joined.empty:
        return pd.DataFrame(
            columns=["corridor_id", timestamp_column, speed_column, volume_column, occupancy_column]
        )

    group_cols = ["corridor_id", timestamp_column]

    speed_series = _aggregate_speed(
        joined,
        group_cols=group_cols,
        speed_column=speed_column,
        weighting=speed_weighting,
        volume_column=volume_column,
        static_weight_column=weight_column,
    )
    result = speed_series.reset_index().rename(columns={0: speed_column})

    if volume_column in joined.columns:
        volume = joined.groupby(group_cols, as_index=False)[volume_column].sum(min_count=1)
        result = result.merge(volume, on=group_cols, how="left")
    else:
        result[volume_column] = pd.NA

    if occupancy_column in joined.columns:
        occupancy = joined.groupby(group_cols, as_index=False)[occupancy_column].mean()
        result = result.merge(occupancy, on=group_cols, how="left")
    else:
        result[occupancy_column] = pd.NA

    result = result.sort_values(["corridor_id", timestamp_column]).reset_index(drop=True)
    return result


def compute_corridor_reliability_rankings(
    observations: pd.DataFrame,
    corridors: pd.DataFrame,
    spec: ReliabilitySpec,
    *,
    speed_weighting: str = "volume",
    weight_column: str = "weight",
    start=None,
    end=None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    corridor_ts = aggregate_observations_to_corridors(
        observations,
        corridors,
        speed_weighting=speed_weighting,
        weight_column=weight_column,
    )

    corridor_spec = replace(spec, segment_id_column="corridor_id")
    return compute_reliability_rankings(
        corridor_ts, corridor_spec, start=start, end=end, limit=limit
    )


def _aggregate_speed(
    df: pd.DataFrame,
    *,
    group_cols: list[str],
    speed_column: str,
    weighting: str,
    volume_column: str,
    static_weight_column: str,
) -> pd.Series:
    weighting = (weighting or "volume").lower()

    if speed_column not in df.columns:
        raise ValueError(f"Missing speed column '{speed_column}' in observations.")

    if weighting == "equal":
        return df.groupby(group_cols)[speed_column].mean()

    if weighting == "static":
        return _weighted_mean(
            df,
            group_cols=group_cols,
            value_column=speed_column,
            weight_column=static_weight_column,
        )

    if weighting == "volume":
        if volume_column not in df.columns:
            return df.groupby(group_cols)[speed_column].mean()
        weighted = _weighted_mean(
            df,
            group_cols=group_cols,
            value_column=speed_column,
            weight_column=volume_column,
        )
        fallback = df.groupby(group_cols)[speed_column].mean()
        return weighted.combine_first(fallback)

    raise ValueError("speed_weighting must be one of: volume, equal, static")


def _weighted_mean(
    df: pd.DataFrame,
    *,
    group_cols: list[str],
    value_column: str,
    weight_column: str,
) -> pd.Series:
    work = df[group_cols + [value_column, weight_column]].copy()
    work[value_column] = pd.to_numeric(work[value_column], errors="coerce")
    work[weight_column] = pd.to_numeric(work[weight_column], errors="coerce")
    work = work.dropna(subset=[value_column, weight_column])
    work = work[work[weight_column] > 0]
    if work.empty:
        return pd.Series(dtype="float64")

    work["_weighted"] = work[value_column] * work[weight_column]
    grouped = work.groupby(group_cols).agg(weight_sum=(weight_column, "sum"), value_sum=("_weighted", "sum"))
    return grouped["value_sum"] / grouped["weight_sum"]

