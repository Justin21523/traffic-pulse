from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

from trafficpulse.analytics.reliability import compute_reliability_rankings, reliability_spec_from_config
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    load_dataset,
    observations_parquet_path,
    observations_csv_path,
    reliability_rankings_parquet_path,
    reliability_rankings_csv_path,
    save_parquet,
    save_csv,
)
from trafficpulse.utils.time import parse_datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute segment reliability rankings.")
    parser.add_argument(
        "--processed-dir",
        default=None,
        help="Processed directory containing observations CSVs (default: config.paths.processed_dir).",
    )
    parser.add_argument(
        "--parquet-dir",
        default=None,
        help="Parquet directory containing observations Parquet files (default: config.warehouse.parquet_dir).",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=None,
        help="Observation granularity minutes (default: config.preprocessing.target_granularity_minutes).",
    )
    parser.add_argument("--start", default=None, help="Start datetime (ISO 8601).")
    parser.add_argument("--end", default=None, help="End datetime (ISO 8601).")
    parser.add_argument("--limit", type=int, default=200, help="Max ranking rows to write.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()
    config = get_config()

    processed_dir = (
        Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    )
    parquet_dir = (
        Path(args.parquet_dir)
        if args.parquet_dir
        else (processed_dir / "parquet" if args.processed_dir else config.warehouse.parquet_dir)
    )
    minutes = (
        int(args.minutes)
        if args.minutes is not None
        else int(config.preprocessing.target_granularity_minutes)
    )

    start: Optional[datetime] = parse_datetime(args.start) if args.start else None
    end: Optional[datetime] = parse_datetime(args.end) if args.end else None

    observations = load_dataset(
        observations_csv_path(processed_dir, minutes),
        observations_parquet_path(parquet_dir, minutes),
    )
    spec = reliability_spec_from_config(config)
    rankings = compute_reliability_rankings(
        observations, spec, start=start, end=end, limit=args.limit
    )

    output_path = reliability_rankings_csv_path(processed_dir, minutes)
    save_csv(rankings, output_path)
    if config.warehouse.enabled:
        parquet_path = save_parquet(
            rankings, reliability_rankings_parquet_path(parquet_dir, minutes)
        )
        print(f"Saved reliability rankings (Parquet): {parquet_path}")
    print(f"Saved reliability rankings: {output_path}")
    print(f"Rows: {len(rankings):,}")


if __name__ == "__main__":
    main()
