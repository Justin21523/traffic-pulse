"""Time-series aggregation utilities for traffic observations.

This module turns fine-grained time-series observations (e.g., 5-min) into coarser aggregates
(e.g., 15-min or hourly) using a config-driven aggregation spec.

Why this exists:
- Downstream analytics and dashboards are easier and faster on coarser, consistent time buckets.
- Aggregation rules (mean/sum/volume-weighted mean) must be reproducible, so we keep them in config.
- We implement defensive type coercion because external CSV/Parquet data can contain strings/nulls.
"""

from __future__ import annotations

# dataclass provides a simple, typed container for aggregation settings without boilerplate.
from dataclasses import dataclass
# Any/Optional are used for flexible typing when working with pandas and nullable values.
from typing import Any, Optional

# pandas provides datetime bucketing (`dt.floor`) and groupby aggregations for tabular time series.
import pandas as pd


# Supported aggregation keywords; we validate config values against this set to fail fast.
SUPPORTED_AGGREGATIONS = {"mean", "sum", "min", "max", "median", "volume_weighted_mean"}


@dataclass(frozen=True)
class AggregationSpec:
    """Configuration for aggregating observations into time buckets."""

    # Target time bucket size in minutes (e.g., 15 for 15-min, 60 for hourly).
    target_granularity_minutes: int
    # Mapping from column name -> aggregation keyword (e.g., {"speed_kph": "mean"}).
    aggregations: dict[str, str]
    # Name of the timestamp column used to form buckets (default matches our internal schema).
    timestamp_column: str = "timestamp"
    # Name of the segment id column used to group per road segment (default matches our schema).
    segment_id_column: str = "segment_id"
    # Name of the volume column used for `volume_weighted_mean` (default matches our schema).
    volume_column: str = "volume"


def aggregate_observations(
    observations: pd.DataFrame,
    spec: AggregationSpec,
) -> pd.DataFrame:
    """Aggregate observation rows into time buckets per segment.

    Inputs:
    - `observations`: a DataFrame with at least timestamp and segment id columns.
    - `spec`: defines the target granularity and per-column aggregation methods.

    Output:
    - A DataFrame keyed by (`segment_id`, bucketed `timestamp`) containing aggregated columns.
    """

    # If there is no data, return a copy to preserve the "don't mutate inputs" contract.
    if observations.empty:
        return observations.copy()

    # Target granularity must be positive; otherwise bucketing would be undefined.
    if spec.target_granularity_minutes <= 0:
        raise ValueError("target_granularity_minutes must be > 0")

    # Validate aggregation keywords early so misconfigured YAML fails with a clear error.
    unknown = sorted({value for value in spec.aggregations.values()} - SUPPORTED_AGGREGATIONS)
    if unknown:
        raise ValueError(f"Unsupported aggregations: {unknown}. Supported: {sorted(SUPPORTED_AGGREGATIONS)}")

    # Work on a copy to avoid surprising side effects for callers that reuse the DataFrame.
    df = observations.copy()

    # Read the configured key column names so this function can work with schema variants.
    ts_col = spec.timestamp_column
    seg_col = spec.segment_id_column
    # These two columns are required to group the data by segment and time bucket.
    if ts_col not in df.columns or seg_col not in df.columns:
        raise ValueError(f"Missing required columns: {ts_col}, {seg_col}")

    # Parse timestamps into timezone-aware UTC datetimes so dt.floor behaves consistently.
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    # Normalize segment ids to strings so ids are stable across CSV/JSON numeric parsing.
    df[seg_col] = df[seg_col].astype(str)
    # Drop rows missing essential keys; aggregation cannot place them into a bucket or segment group.
    df = df.dropna(subset=[ts_col, seg_col])

    # Coerce requested value columns to numeric so groupby aggregations behave predictably.
    for column in spec.aggregations.keys():
        # Skip missing columns and avoid coercing the key columns.
        if column in df.columns and column not in {ts_col, seg_col}:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    # Create a bucket column using floor-to-interval semantics (e.g., 12:07 -> 12:00 for 15-min buckets).
    bucket_col = "_bucket"
    # Pandas expects a frequency string like "15min"; we build it from the spec for reproducibility.
    df[bucket_col] = df[ts_col].dt.floor(f"{spec.target_granularity_minutes}min")

    # Grouping keys are segment id and the computed time bucket.
    group_cols = [seg_col, bucket_col]

    # Base aggregations are those that can be expressed directly in DataFrame.groupby().agg().
    base_agg_map: dict[str, str] = {}
    # Volume-weighted means are computed in a separate pass because they need both value and volume sums.
    weighted_cols: list[str] = []

    # Split configured aggregations into "simple" vs "volume_weighted_mean".
    for column, aggregation in spec.aggregations.items():
        # Ignore columns that are not present, and do not aggregate the key columns themselves.
        if column not in df.columns or column in {ts_col, seg_col}:
            continue
        # Weighted mean needs a custom formula, so we defer it to the helper.
        if aggregation == "volume_weighted_mean":
            weighted_cols.append(column)
            continue
        # Everything else is a supported pandas aggregation keyword (mean/sum/min/max/median).
        base_agg_map[column] = aggregation

    # Compute simple aggregations with one groupby call when possible.
    if base_agg_map:
        aggregated = df.groupby(group_cols, as_index=False).agg(base_agg_map)
    else:
        # If there are no base aggregations, still return one row per group so weighted means can be merged.
        aggregated = df[group_cols].drop_duplicates()

    # Compute any requested volume-weighted means and merge them back into the aggregated frame.
    weighted = _aggregate_volume_weighted_means(
        df=df,
        group_cols=group_cols,
        value_columns=weighted_cols,
        volume_column=spec.volume_column,
    )
    # The helper returns None when no weighted columns were requested.
    if weighted is not None:
        aggregated = aggregated.merge(weighted, on=group_cols, how="left")

    # Replace the internal bucket column with the public timestamp column name expected by downstream code.
    aggregated = aggregated.rename(columns={bucket_col: ts_col})
    # Sort deterministically so exports and downstream processing produce stable, diff-friendly outputs.
    aggregated = aggregated.sort_values([seg_col, ts_col]).reset_index(drop=True)
    return aggregated


def _aggregate_volume_weighted_means(
    df: pd.DataFrame,
    group_cols: list[str],
    value_columns: list[str],
    volume_column: str,
) -> Optional[pd.DataFrame]:
    """Compute per-group volume-weighted means for one or more value columns.

    Formula:
    - weighted_mean = sum(value * volume) / sum(volume)
    """

    # If no weighted columns were requested, signal "nothing to merge".
    if not value_columns:
        return None
    # Weighted mean requires the volume column to exist; fail fast with a clear message.
    if volume_column not in df.columns:
        raise ValueError(
            f"Requested volume_weighted_mean but missing volume column '{volume_column}'."
        )

    # Work on a compact subset to reduce memory overhead when the full DataFrame is wide.
    work = df[group_cols + [volume_column] + value_columns].copy()
    # Ensure volume is numeric; non-numeric values become NaN so they do not corrupt sums.
    work[volume_column] = pd.to_numeric(work[volume_column], errors="coerce")

    # We build intermediate columns that store (value * volume) so we can sum them per group.
    weighted_sum_cols: dict[str, str] = {}
    for col in value_columns:
        # Ensure the value column is numeric so multiplication behaves correctly.
        work[col] = pd.to_numeric(work[col], errors="coerce")
        # Use a prefixed internal column name to avoid colliding with real dataset columns.
        wcol = f"_w_{col}"
        # Multiply value by volume to prepare the numerator of the weighted mean.
        work[wcol] = work[col] * work[volume_column]
        # We want sums for numerators, so the aggregation keyword is "sum".
        weighted_sum_cols[wcol] = "sum"

    # Groupby aggregation map includes volume sum (denominator) plus the weighted numerators.
    agg_map: dict[str, str] = {volume_column: "sum", **weighted_sum_cols}
    # Aggregate one row per group containing volume_sum and each weighted_sum.
    grouped = work.groupby(group_cols, as_index=False).agg(agg_map)

    # Pull the per-group volume sum for reuse when computing multiple columns.
    volume_sum = grouped[volume_column]
    for col in value_columns:
        # Retrieve the per-group numerator column name that we created above.
        wcol = f"_w_{col}"
        # Divide numerator by denominator to compute the weighted mean.
        grouped[col] = grouped[wcol] / volume_sum
        # Guard against divide-by-zero and invalid denominators by returning NA in those groups.
        grouped.loc[volume_sum <= 0, col] = pd.NA

    # Return only the group keys plus the computed weighted-mean columns for a clean merge.
    return grouped[group_cols + value_columns]


def build_aggregation_spec(
    target_granularity_minutes: int,
    aggregations: dict[str, str],
    *,
    timestamp_column: str = "timestamp",
    segment_id_column: str = "segment_id",
    volume_column: str = "volume",
) -> AggregationSpec:
    """Build an immutable AggregationSpec from user/config inputs.

    Why we copy `aggregations`:
    - It prevents accidental mutation of the original dict (e.g., a config object reused elsewhere).
    """

    # Construct the spec with explicit column names so callers can adapt to schema variations.
    return AggregationSpec(
        target_granularity_minutes=target_granularity_minutes,
        aggregations=dict(aggregations),
        timestamp_column=timestamp_column,
        segment_id_column=segment_id_column,
        volume_column=volume_column,
    )
