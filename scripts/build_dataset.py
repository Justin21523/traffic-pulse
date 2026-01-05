from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    observations_csv_path,
    save_csv,
    segments_csv_path,
)
from trafficpulse.utils.time import parse_datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a local VD dataset from TDX.")
    parser.add_argument("--start", required=True, help="Start datetime (ISO 8601).")
    parser.add_argument("--end", required=True, help="End datetime (ISO 8601).")
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()

    config = get_config()
    processed_dir = (
        Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    )

    start: datetime = parse_datetime(args.start)
    end: datetime = parse_datetime(args.end)

    client = TdxTrafficClient(config=config)
    try:
        segments, observations = client.download_vd(start=start, end=end, cities=args.cities)
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


if __name__ == "__main__":
    main()

