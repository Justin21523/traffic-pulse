from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ObservationCleanStats:
    input_rows: int
    output_rows: int
    dropped_missing_keys: int
    dropped_invalid_timestamp: int
    dropped_invalid_speed: int
    dropped_duplicates: int


def clean_observations(
    observations: pd.DataFrame,
    *,
    timestamp_column: str = "timestamp",
    segment_id_column: str = "segment_id",
    speed_column: str = "speed_kph",
    min_speed_kph: float = 0.0,
    max_speed_kph: float = 200.0,
    drop_missing_speed: bool = True,
    dedupe: bool = True,
) -> tuple[pd.DataFrame, ObservationCleanStats]:
    """Normalize observation rows for downstream analytics.

    Guarantees (when output is non-empty):
    - `timestamp_column` is UTC-aware datetime64
    - `segment_id_column` is string
    - `speed_column` is numeric (float) and within [min_speed_kph, max_speed_kph]
    - Optional dedup by (segment_id, timestamp) keeping last
    """

    if observations.empty:
        stats = ObservationCleanStats(
            input_rows=0,
            output_rows=0,
            dropped_missing_keys=0,
            dropped_invalid_timestamp=0,
            dropped_invalid_speed=0,
            dropped_duplicates=0,
        )
        return observations.copy(), stats

    df = observations.copy()
    input_rows = int(len(df))

    # Normalize key columns.
    if segment_id_column in df.columns:
        df[segment_id_column] = df[segment_id_column].astype(str)

    if timestamp_column in df.columns:
        df[timestamp_column] = pd.to_datetime(df[timestamp_column], errors="coerce", utc=True)

    # Drop missing keys after coercion (includes invalid timestamps converted to NaT).
    before_drop_keys = int(len(df))
    keep_cols = [c for c in [timestamp_column, segment_id_column] if c in df.columns]
    df = df.dropna(subset=keep_cols)
    dropped_missing_keys = before_drop_keys - int(len(df))

    # Count invalid timestamps within the original input (best-effort).
    invalid_timestamp = 0
    if timestamp_column in observations.columns:
        ts = pd.to_datetime(observations[timestamp_column], errors="coerce", utc=True)
        invalid_timestamp = int(ts.isna().sum())

    # Coerce speed and drop invalids.
    dropped_invalid_speed = 0
    if speed_column in df.columns:
        df[speed_column] = pd.to_numeric(df[speed_column], errors="coerce")
        before_speed = int(len(df))
        # Drop sentinel-like values and out-of-range values.
        df = df[(df[speed_column].isna()) | ((df[speed_column] >= float(min_speed_kph)) & (df[speed_column] <= float(max_speed_kph)))]
        if drop_missing_speed:
            df = df.dropna(subset=[speed_column])
        dropped_invalid_speed = before_speed - int(len(df))

    dropped_duplicates = 0
    if dedupe and not df.empty and timestamp_column in df.columns and segment_id_column in df.columns:
        before_dupe = int(len(df))
        df = df.drop_duplicates(subset=[segment_id_column, timestamp_column], keep="last")
        dropped_duplicates = before_dupe - int(len(df))

    # Deterministic order is important for stable exports and diff-friendly CSVs.
    sort_cols = [c for c in [segment_id_column, timestamp_column] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    stats = ObservationCleanStats(
        input_rows=input_rows,
        output_rows=int(len(df)),
        dropped_missing_keys=int(dropped_missing_keys),
        dropped_invalid_timestamp=int(invalid_timestamp),
        dropped_invalid_speed=int(dropped_invalid_speed),
        dropped_duplicates=int(dropped_duplicates),
    )
    return df, stats
