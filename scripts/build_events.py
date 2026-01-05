from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import events_csv_path, save_csv
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
        events = client.download_events(start=start, end=end, cities=args.cities)
    finally:
        client.close()

    output_path = save_csv(events, events_csv_path(processed_dir))
    print(f"Saved events: {output_path}")
    print(f"Rows: {len(events):,}")


if __name__ == "__main__":
    main()

