from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd


SUPPORTED_AGGREGATIONS = {"mean", "sum", "min", "max", "median", "volume_weighted_mean"}


@dataclass(frozen=True)
class AggregationSpec:
    target_granularity_minutes: int
    aggregations: dict[str, str]
    timestamp_column: str = "timestamp"
    segment_id_column: str = "segment_id"
    volume_column: str = "volume"


def aggregate_observations(
    observations: pd.DataFrame,
    spec: AggregationSpec,
) -> pd.DataFrame:
    if observations.empty:
        return observations.copy()

    if spec.target_granularity_minutes <= 0:
        raise ValueError("target_granularity_minutes must be > 0")

    unknown = sorted({value for value in spec.aggregations.values()} - SUPPORTED_AGGREGATIONS)
    if unknown:
        raise ValueError(f"Unsupported aggregations: {unknown}. Supported: {sorted(SUPPORTED_AGGREGATIONS)}")

    df = observations.copy()

    ts_col = spec.timestamp_column
    seg_col = spec.segment_id_column
    if ts_col not in df.columns or seg_col not in df.columns:
        raise ValueError(f"Missing required columns: {ts_col}, {seg_col}")

    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df[seg_col] = df[seg_col].astype(str)
    df = df.dropna(subset=[ts_col, seg_col])

    for column in spec.aggregations.keys():
        if column in df.columns and column not in {ts_col, seg_col}:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    bucket_col = "_bucket"
    df[bucket_col] = df[ts_col].dt.floor(f"{spec.target_granularity_minutes}min")

    group_cols = [seg_col, bucket_col]

    base_agg_map: dict[str, str] = {}
    weighted_cols: list[str] = []

    for column, aggregation in spec.aggregations.items():
        if column not in df.columns or column in {ts_col, seg_col}:
            continue
        if aggregation == "volume_weighted_mean":
            weighted_cols.append(column)
            continue
        base_agg_map[column] = aggregation

    if base_agg_map:
        aggregated = df.groupby(group_cols, as_index=False).agg(base_agg_map)
    else:
        aggregated = df[group_cols].drop_duplicates()

    weighted = _aggregate_volume_weighted_means(
        df=df,
        group_cols=group_cols,
        value_columns=weighted_cols,
        volume_column=spec.volume_column,
    )
    if weighted is not None:
        aggregated = aggregated.merge(weighted, on=group_cols, how="left")

    aggregated = aggregated.rename(columns={bucket_col: ts_col})
    aggregated = aggregated.sort_values([seg_col, ts_col]).reset_index(drop=True)
    return aggregated


def _aggregate_volume_weighted_means(
    df: pd.DataFrame,
    group_cols: list[str],
    value_columns: list[str],
    volume_column: str,
) -> Optional[pd.DataFrame]:
    if not value_columns:
        return None
    if volume_column not in df.columns:
        raise ValueError(
            f"Requested volume_weighted_mean but missing volume column '{volume_column}'."
        )

    work = df[group_cols + [volume_column] + value_columns].copy()
    work[volume_column] = pd.to_numeric(work[volume_column], errors="coerce")

    weighted_sum_cols: dict[str, str] = {}
    for col in value_columns:
        work[col] = pd.to_numeric(work[col], errors="coerce")
        wcol = f"_w_{col}"
        work[wcol] = work[col] * work[volume_column]
        weighted_sum_cols[wcol] = "sum"

    agg_map: dict[str, str] = {volume_column: "sum", **weighted_sum_cols}
    grouped = work.groupby(group_cols, as_index=False).agg(agg_map)

    volume_sum = grouped[volume_column]
    for col in value_columns:
        wcol = f"_w_{col}"
        grouped[col] = grouped[wcol] / volume_sum
        grouped.loc[volume_sum <= 0, col] = pd.NA

    return grouped[group_cols + value_columns]


def build_aggregation_spec(
    target_granularity_minutes: int,
    aggregations: dict[str, str],
    *,
    timestamp_column: str = "timestamp",
    segment_id_column: str = "segment_id",
    volume_column: str = "volume",
) -> AggregationSpec:
    return AggregationSpec(
        target_granularity_minutes=target_granularity_minutes,
        aggregations=dict(aggregations),
        timestamp_column=timestamp_column,
        segment_id_column=segment_id_column,
        volume_column=volume_column,
    )

