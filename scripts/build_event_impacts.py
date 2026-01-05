from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from trafficpulse.analytics.event_impact import compute_event_impacts, event_impact_spec_from_config
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    events_csv_path,
    events_parquet_path,
    load_dataset,
    observations_parquet_path,
    observations_csv_path,
    save_parquet,
    save_csv,
    segments_csv_path,
    segments_parquet_path,
)
from trafficpulse.utils.time import parse_datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute event impact summaries.")
    parser.add_argument(
        "--processed-dir",
        default=None,
        help="Processed directory containing datasets (default: config.paths.processed_dir).",
    )
    parser.add_argument(
        "--parquet-dir",
        default=None,
        help="Parquet directory containing datasets (default: config.warehouse.parquet_dir).",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=None,
        help="Observation granularity minutes (default: config.preprocessing.target_granularity_minutes).",
    )
    parser.add_argument("--start", default=None, help="Start datetime (ISO 8601).")
    parser.add_argument("--end", default=None, help="End datetime (ISO 8601).")
    parser.add_argument(
        "--window-hours",
        type=int,
        default=None,
        help="Default window if start/end not provided (default: config.analytics.event_impact.default_window_hours).",
    )
    parser.add_argument(
        "--limit-events",
        type=int,
        default=200,
        help="Max number of events (most recent) to evaluate.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default: data/processed/event_impacts_{minutes}min.csv).",
    )
    return parser.parse_args()


def resolve_time_window(
    events: pd.DataFrame,
    *,
    start_text: Optional[str],
    end_text: Optional[str],
    default_window_hours: int,
) -> tuple[datetime, datetime]:
    start_dt: Optional[datetime] = parse_datetime(start_text) if start_text else None
    end_dt: Optional[datetime] = parse_datetime(end_text) if end_text else None

    if (start_dt is None) != (end_dt is None):
        raise SystemExit("Provide both --start and --end, or neither.")
    if start_dt is not None and end_dt is not None and end_dt <= start_dt:
        raise SystemExit("--end must be greater than --start.")

    if start_dt is not None and end_dt is not None:
        return start_dt, end_dt

    df = events.copy()
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    df = df.dropna(subset=["start_time"])
    if not df["start_time"].empty:
        end_dt = df["start_time"].max().to_pydatetime()
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
    else:
        end_dt = datetime.now(timezone.utc)

    start_dt = end_dt - timedelta(hours=int(default_window_hours))
    return start_dt, end_dt


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

    events_csv = events_csv_path(processed_dir)
    events_parquet = events_parquet_path(parquet_dir)
    if not events_csv.exists() and not events_parquet.exists():
        raise SystemExit("events dataset not found. Run scripts/build_events.py first.")

    segments_csv = segments_csv_path(processed_dir)
    segments_parquet = segments_parquet_path(parquet_dir)
    if not segments_csv.exists() and not segments_parquet.exists():
        raise SystemExit("segments dataset not found. Run scripts/build_dataset.py first.")

    obs_csv = observations_csv_path(processed_dir, minutes)
    obs_parquet = observations_parquet_path(parquet_dir, minutes)
    if not obs_csv.exists() and not obs_parquet.exists():
        fallback_csv = observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes)
        fallback_parquet = observations_parquet_path(parquet_dir, config.preprocessing.source_granularity_minutes)
        if fallback_csv.exists() or fallback_parquet.exists():
            obs_csv = fallback_csv
            obs_parquet = fallback_parquet
        else:
            raise SystemExit("observations dataset not found. Run scripts/build_dataset.py first.")

    events = load_dataset(events_csv, events_parquet)
    segments = load_dataset(segments_csv, segments_parquet)
    observations = load_dataset(obs_csv, obs_parquet)

    spec = event_impact_spec_from_config(config)
    window_hours = int(args.window_hours) if args.window_hours is not None else spec.default_window_hours
    start_dt, end_dt = resolve_time_window(
        events, start_text=args.start, end_text=args.end, default_window_hours=window_hours
    )

    events["start_time"] = pd.to_datetime(events["start_time"], errors="coerce", utc=True)
    events = events.dropna(subset=["event_id", "start_time"])
    events = events[(events["start_time"] >= pd.Timestamp(start_dt)) & (events["start_time"] < pd.Timestamp(end_dt))]

    impacts = compute_event_impacts(
        events,
        observations=observations,
        segments=segments,
        spec=spec,
        limit_events=int(args.limit_events) if args.limit_events is not None else None,
    )

    output_path = Path(args.output) if args.output else (processed_dir / f"event_impacts_{minutes}min.csv")
    save_csv(impacts, output_path)
    if config.warehouse.enabled:
        parquet_out = parquet_dir / f"event_impacts_{minutes}min.parquet"
        parquet_path = save_parquet(impacts, parquet_out)
        print(f"Saved event impacts (Parquet): {parquet_path}")
    print(f"Saved event impacts: {output_path}")
    print(f"Rows: {len(impacts):,}")


if __name__ == "__main__":
    main()
