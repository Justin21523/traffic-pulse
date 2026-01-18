from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

from trafficpulse.analytics.corridors import (
    compute_corridor_reliability_rankings,
    corridor_metadata,
    load_corridors_csv,
)
from trafficpulse.analytics.reliability import reliability_spec_from_config
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    load_dataset,
    observations_parquet_path,
    observations_csv_path,
    save_csv,
)
from trafficpulse.utils.time import parse_datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute corridor reliability rankings.")
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
    parser.add_argument(
        "--corridors-csv",
        default=None,
        help="Corridors CSV path (default: config.analytics.corridors.corridors_csv).",
    )
    parser.add_argument("--start", default=None, help="Start datetime (ISO 8601).")
    parser.add_argument("--end", default=None, help="End datetime (ISO 8601).")
    parser.add_argument("--limit", type=int, default=200, help="Max ranking rows to write.")
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default: data/processed/corridor_rankings_{minutes}min.csv).",
    )
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
    corridors_path = (
        Path(args.corridors_csv)
        if args.corridors_csv
        else config.analytics.corridors.corridors_csv
    )

    start: Optional[datetime] = parse_datetime(args.start) if args.start else None
    end: Optional[datetime] = parse_datetime(args.end) if args.end else None

    corridors = load_corridors_csv(corridors_path)
    observations = load_dataset(
        observations_csv_path(processed_dir, minutes),
        observations_parquet_path(parquet_dir, minutes),
    )

    spec = reliability_spec_from_config(config)
    rankings = compute_corridor_reliability_rankings(
        observations,
        corridors,
        spec,
        speed_weighting=config.analytics.corridors.speed_weighting,
        weight_column=config.analytics.corridors.weight_column,
        start=start,
        end=end,
        limit=args.limit,
    )

    output_path = Path(args.output) if args.output else (processed_dir / f"corridor_rankings_{minutes}min.csv")
    meta = corridor_metadata(corridors)
    if not rankings.empty:
        rankings = rankings.merge(meta, on="corridor_id", how="left")
    save_csv(rankings, output_path)
    print(f"Saved corridor rankings: {output_path}")
    print(f"Rows: {len(rankings):,}")


if __name__ == "__main__":
    main()
