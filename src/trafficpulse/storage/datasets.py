from __future__ import annotations

from pathlib import Path

import pandas as pd


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def segments_csv_path(processed_dir: Path) -> Path:
    return processed_dir / "segments.csv"


def observations_csv_path(processed_dir: Path, granularity_minutes: int) -> Path:
    return processed_dir / f"observations_{granularity_minutes}min.csv"


def save_csv(df: pd.DataFrame, path: Path) -> Path:
    ensure_parent_dir(path)
    df.to_csv(path, index=False)
    return path


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

