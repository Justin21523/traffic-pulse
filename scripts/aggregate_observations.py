from __future__ import annotations

import argparse
from pathlib import Path

from trafficpulse.logging_config import configure_logging
from trafficpulse.preprocessing.aggregation import aggregate_observations, build_aggregation_spec
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    load_dataset,
    observations_parquet_path,
    observations_csv_path,
    save_parquet,
    save_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate observations from a finer time granularity to a coarser one."
    )
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
        "--source-minutes",
        type=int,
        default=None,
        help="Source granularity minutes (default: config.preprocessing.source_granularity_minutes).",
    )
    parser.add_argument(
        "--target-minutes",
        type=int,
        default=None,
        help="Target granularity minutes (default: config.preprocessing.target_granularity_minutes).",
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
    source_minutes = (
        int(args.source_minutes)
        if args.source_minutes is not None
        else int(config.preprocessing.source_granularity_minutes)
    )
    target_minutes = (
        int(args.target_minutes)
        if args.target_minutes is not None
        else int(config.preprocessing.target_granularity_minutes)
    )

    input_path = observations_csv_path(processed_dir, source_minutes)
    output_path = observations_csv_path(processed_dir, target_minutes)
    input_parquet = observations_parquet_path(parquet_dir, source_minutes)
    output_parquet = observations_parquet_path(parquet_dir, target_minutes)

    df = load_dataset(input_path, input_parquet)

    spec = build_aggregation_spec(
        target_granularity_minutes=target_minutes,
        aggregations=config.preprocessing.aggregation,
    )
    aggregated = aggregate_observations(df, spec)

    save_csv(aggregated, output_path)
    if config.warehouse.enabled:
        parquet_path = save_parquet(aggregated, output_parquet)
        print(f"Saved aggregated observations (Parquet): {parquet_path}")
    print(f"Saved aggregated observations: {output_path}")
    print(f"Rows: {len(aggregated):,}")


if __name__ == "__main__":
    main()
