from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

from trafficpulse.analytics.anomalies import (
    anomaly_spec_from_config,
    compute_anomaly_timeseries,
    spec_for_entity,
    summarize_anomaly_events,
)
from trafficpulse.analytics.corridors import aggregate_observations_to_corridors, load_corridors_csv
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
    parser = argparse.ArgumentParser(description="Detect speed anomalies (rolling z-score baseline).")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--segment-id", default=None, help="Segment (VD) identifier.")
    group.add_argument("--corridor-id", default=None, help="Corridor identifier from corridors.csv.")

    parser.add_argument("--start", required=True, help="Start datetime (ISO 8601).")
    parser.add_argument("--end", required=True, help="End datetime (ISO 8601).")
    parser.add_argument(
        "--minutes",
        type=int,
        default=None,
        help="Observation granularity minutes (default: config.preprocessing.target_granularity_minutes).",
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
        "--output-dir",
        default=None,
        help="Output directory (default: processed_dir).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()
    config = get_config()

    start: datetime = parse_datetime(args.start)
    end: datetime = parse_datetime(args.end)
    if end <= start:
        raise SystemExit("'end' must be greater than 'start'.")

    minutes = (
        int(args.minutes)
        if args.minutes is not None
        else int(config.preprocessing.target_granularity_minutes)
    )

    processed_dir = (
        Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    )
    parquet_dir = (
        Path(args.parquet_dir)
        if args.parquet_dir
        else (processed_dir / "parquet" if args.processed_dir else config.warehouse.parquet_dir)
    )
    output_dir = Path(args.output_dir) if args.output_dir else processed_dir

    observations = load_dataset(
        observations_csv_path(processed_dir, minutes),
        observations_parquet_path(parquet_dir, minutes),
    )
    if observations.empty:
        raise SystemExit("observations dataset is empty.")

    spec = anomaly_spec_from_config(config)

    if args.segment_id:
        entity_id = str(args.segment_id)
        enriched = compute_anomaly_timeseries(
            observations, spec, entity_id=entity_id, start=start, end=end
        )
        events = summarize_anomaly_events(enriched, spec)
        id_label = entity_id
    else:
        entity_id = str(args.corridor_id)
        corridors = load_corridors_csv(config.analytics.corridors.corridors_csv)
        corridors = corridors[corridors["corridor_id"].astype(str) == entity_id]
        if corridors.empty:
            raise SystemExit("corridor_id not found in corridors.csv.")

        corridor_ts = aggregate_observations_to_corridors(
            observations,
            corridors,
            speed_weighting=config.analytics.corridors.speed_weighting,
            weight_column=config.analytics.corridors.weight_column,
        )

        corridor_spec = spec_for_entity(spec, entity_id_column="corridor_id")
        enriched = compute_anomaly_timeseries(
            corridor_ts, corridor_spec, entity_id=entity_id, start=start, end=end
        )
        events = summarize_anomaly_events(enriched, corridor_spec)
        id_label = f"corridor_{entity_id}"

    points_path = output_dir / f"anomalies_points_{id_label}_{minutes}min.csv"
    events_path = output_dir / f"anomalies_events_{id_label}_{minutes}min.csv"
    save_csv(enriched, points_path)
    save_csv(events, events_path)

    print(f"Saved anomaly points: {points_path}")
    print(f"Saved anomaly events: {events_path}")
    print(f"Points rows: {len(enriched):,}")
    print(f"Events rows: {len(events):,}")


if __name__ == "__main__":
    main()
