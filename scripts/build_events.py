from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import events_parquet_path, events_csv_path, save_parquet, save_csv
from trafficpulse.utils.time import parse_datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a local traffic events dataset from TDX.")
    parser.add_argument("--start", required=True, help="Start datetime (ISO 8601).")
    parser.add_argument("--end", required=True, help="End datetime (ISO 8601).")
    parser.add_argument(
        "--cities",
        nargs="*",
        default=None,
        help="One or more city names used in TDX endpoint templates (default: config ingestion.events.cities).",
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
        events = client.download_events(start=start, end=end, cities=args.cities)
    finally:
        client.close()

    output_path = save_csv(events, events_csv_path(processed_dir))
    print(f"Saved events: {output_path}")
    print(f"Rows: {len(events):,}")

    if config.warehouse.enabled:
        parquet_path = save_parquet(events, events_parquet_path(parquet_dir))
        print(f"Saved events (Parquet): {parquet_path}")


if __name__ == "__main__":
    main()
