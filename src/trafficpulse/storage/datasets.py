from __future__ import annotations

from pathlib import Path

import pandas as pd


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def segments_csv_path(processed_dir: Path) -> Path:
    return processed_dir / "segments.csv"


def segments_parquet_path(parquet_dir: Path) -> Path:
    return parquet_dir / "segments.parquet"


def observations_csv_path(processed_dir: Path, granularity_minutes: int) -> Path:
    return processed_dir / f"observations_{granularity_minutes}min.csv"


def observations_parquet_path(parquet_dir: Path, granularity_minutes: int) -> Path:
    return parquet_dir / f"observations_{granularity_minutes}min.parquet"


def reliability_rankings_csv_path(processed_dir: Path, granularity_minutes: int) -> Path:
    return processed_dir / f"reliability_rankings_{granularity_minutes}min.csv"


def reliability_rankings_parquet_path(parquet_dir: Path, granularity_minutes: int) -> Path:
    return parquet_dir / f"reliability_rankings_{granularity_minutes}min.parquet"


def events_csv_path(processed_dir: Path) -> Path:
    return processed_dir / "events.csv"


def events_parquet_path(parquet_dir: Path) -> Path:
    return parquet_dir / "events.parquet"


def save_csv(df: pd.DataFrame, path: Path) -> Path:
    ensure_parent_dir(path)
    df.to_csv(path, index=False)
    return path


def append_csv(df: pd.DataFrame, path: Path) -> Path:
    """Append a DataFrame to a CSV, creating the file if it does not exist.

    Notes:
    - When appending to an existing file, this function aligns columns to the existing header:
      extra columns are dropped and missing columns are filled with NA.
    - This is intentionally simple and optimized for ingestion backfills; downstream code can
      re-sort/deduplicate if needed.
    """

    ensure_parent_dir(path)
    if df.empty:
        return path

    if not path.exists():
        df.to_csv(path, index=False)
        return path

    existing_cols = list(pd.read_csv(path, nrows=0).columns)
    aligned = df.copy()
    for col in existing_cols:
        if col not in aligned.columns:
            aligned[col] = pd.NA
    aligned = aligned[existing_cols]
    aligned.to_csv(path, mode="a", header=False, index=False)
    return path


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def save_parquet(df: pd.DataFrame, path: Path) -> Path:
    ensure_parent_dir(path)
    try:
        df.to_parquet(path, index=False)
    except ImportError as exc:
        raise RuntimeError("pyarrow is required to write Parquet files. Install requirements.txt.") from exc
    return path


def load_parquet(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except ImportError as exc:
        raise RuntimeError("pyarrow is required to read Parquet files. Install requirements.txt.") from exc


def load_dataset(csv_path: Path, parquet_path: Path) -> pd.DataFrame:
    if parquet_path.exists():
        return load_parquet(parquet_path)
    if csv_path.exists():
        return load_csv(csv_path)
    raise FileNotFoundError(f"Dataset not found: {csv_path} or {parquet_path}")
