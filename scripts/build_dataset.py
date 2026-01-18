from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
from datetime import datetime
from pathlib import Path

from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    observations_parquet_path,
    observations_csv_path,
    save_parquet,
    save_csv,
    segments_parquet_path,
    segments_csv_path,
)
from trafficpulse.utils.time import parse_datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a local VD dataset from TDX.")
    parser.add_argument("--start", required=True, help="Start datetime (ISO 8601).")
    parser.add_argument("--end", required=True, help="End datetime (ISO 8601).")
    parser.add_argument(
        "--source",
        choices=["historical", "live"],
        default="historical",
        help="Data source: 'historical' uses JSONL-by-date backfill; 'live' uses live VDLive endpoint.",
    )
    parser.add_argument(
        "--cities",
        nargs="*",
        default=None,
        help="One or more city names used in TDX endpoint templates (default: config ingestion.vd.cities).",
    )
    parser.add_argument(
        "--processed-dir",
        default=None,
        help="Override output directory (default: config.paths.processed_dir).",
    )
    parser.add_argument(
        "--parquet-dir",
        default=None,
        help="Override Parquet output directory (default: config.warehouse.parquet_dir).",
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

    start: datetime = parse_datetime(args.start)
    end: datetime = parse_datetime(args.end)

    client = TdxTrafficClient(config=config)
    try:
        if args.source == "live":
            segments, observations = client.download_vd_live(start=start, end=end, cities=args.cities)
        else:
            segments, observations = client.download_vd_historical(start=start, end=end, cities=args.cities)
    finally:
        client.close()

    segments_path = save_csv(segments, segments_csv_path(processed_dir))
    observations_path = save_csv(
        observations,
        observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes),
    )

    print(f"Saved segments: {segments_path}")
    print(f"Saved observations: {observations_path}")
    print(f"Segments rows: {len(segments):,}")
    print(f"Observation rows: {len(observations):,}")

    if config.warehouse.enabled:
        segments_parquet = save_parquet(segments, segments_parquet_path(parquet_dir))
        observations_parquet = save_parquet(
            observations,
            observations_parquet_path(parquet_dir, config.preprocessing.source_granularity_minutes),
        )
        print(f"Saved segments (Parquet): {segments_parquet}")
        print(f"Saved observations (Parquet): {observations_parquet}")


if __name__ == "__main__":
    main()
