from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trafficpulse.analytics.segment_quality import compute_segment_quality
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.backend import duckdb_backend
from trafficpulse.storage.datasets import (
    load_csv,
    load_parquet,
    observations_csv_path,
    observations_parquet_path,
    save_csv,
)
from trafficpulse.utils.time import parse_datetime


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute per-segment data quality/coverage metrics.")
    p.add_argument("--processed-dir", default=None, help="Override processed dir (default: config.paths.processed_dir).")
    p.add_argument("--minutes", type=int, default=None, help="Granularity minutes (default: config target).")
    p.add_argument("--window-hours", type=int, default=24, help="Window hours (default: 24).")
    p.add_argument("--start", default=None, help="Explicit start datetime (ISO 8601).")
    p.add_argument("--end", default=None, help="Explicit end datetime (ISO 8601).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()

    config = get_config()
    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    minutes = int(args.minutes or config.preprocessing.target_granularity_minutes)
    window_hours = int(args.window_hours)

    start_dt = parse_datetime(args.start) if args.start else None
    end_dt = parse_datetime(args.end) if args.end else None

    backend = duckdb_backend(config)
    parquet_dir = config.warehouse.parquet_dir
    csv_path = observations_csv_path(processed_dir, minutes)
    parquet_path = observations_parquet_path(parquet_dir, minutes)
    if backend is not None and parquet_path.exists():
        if start_dt is None and end_dt is None:
            max_ts = backend.max_observation_timestamp(minutes=minutes)
            if max_ts is not None:
                end_dt = max_ts if max_ts.tzinfo is not None else max_ts.replace(tzinfo=timezone.utc)
        if end_dt is None:
            end_dt = datetime.now(timezone.utc)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        if start_dt is None:
            start_dt = end_dt - timedelta(hours=window_hours)
        df = backend.query_observations(minutes=minutes, start=start_dt, end=end_dt)
    else:
        df = load_parquet(parquet_path) if config.warehouse.enabled and parquet_path.exists() else load_csv(csv_path)
        if df.empty:
            raise SystemExit("observations dataset empty; cannot compute quality.")
        if start_dt is None and end_dt is None:
            ts = df["timestamp"] if "timestamp" in df.columns else None
            if ts is not None:
                ts = ts.astype(str)
            end_dt = datetime.now(timezone.utc)
        if end_dt is None:
            end_dt = datetime.now(timezone.utc)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        if start_dt is None:
            start_dt = end_dt - timedelta(hours=window_hours)

    out = compute_segment_quality(
        df,
        start=start_dt,
        end=end_dt,
        expected_interval_minutes=minutes,
    )
    out_path = config.paths.cache_dir / f"segment_quality_{minutes}m_{window_hours}h.csv"
    save_csv(out, out_path)
    print(f"[segment-quality] wrote {out_path} rows={len(out):,}")


if __name__ == "__main__":
    main()

