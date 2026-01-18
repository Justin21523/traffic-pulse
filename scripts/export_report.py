from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from trafficpulse.analytics.anomalies import (
    anomaly_spec_from_config,
    compute_anomaly_timeseries,
    spec_for_entity,
    summarize_anomaly_events,
)
from trafficpulse.analytics.corridors import (
    aggregate_observations_to_corridors,
    compute_corridor_reliability_rankings,
    corridor_metadata,
    load_corridors_csv,
)
from trafficpulse.analytics.reliability import compute_reliability_rankings, reliability_spec_from_config
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    load_csv,
    observations_csv_path,
    save_csv,
    segments_csv_path,
)
from trafficpulse.utils.time import parse_datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a report snapshot (CSV + JSON metadata).")
    parser.add_argument(
        "--processed-dir",
        default=None,
        help="Processed directory containing datasets (default: config.paths.processed_dir).",
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
        help="Default window if start/end not provided (default: config.analytics.reliability.default_window_hours).",
    )
    parser.add_argument("--limit", type=int, default=200, help="Max rows for ranking outputs.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: outputs/reports/<timestamp>/).",
    )
    parser.add_argument(
        "--include-corridors",
        action="store_true",
        help="Include corridor rankings (requires configs/corridors.csv).",
    )
    parser.add_argument(
        "--anomaly-segment-id",
        default=None,
        help="Optional segment ID to export anomaly points/events for.",
    )
    parser.add_argument(
        "--anomaly-corridor-id",
        default=None,
        help="Optional corridor ID to export anomaly points/events for.",
    )
    return parser.parse_args()


def resolve_time_window(
    observations: pd.DataFrame,
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

    obs = observations.copy()
    if "timestamp" not in obs.columns:
        raise SystemExit("observations dataset is missing 'timestamp' column.")
    obs["timestamp"] = pd.to_datetime(obs["timestamp"], errors="coerce", utc=True)
    obs = obs.dropna(subset=["timestamp"])

    if not obs["timestamp"].empty:
        end_dt = obs["timestamp"].max().to_pydatetime()
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
    minutes = (
        int(args.minutes)
        if args.minutes is not None
        else int(config.preprocessing.target_granularity_minutes)
    )

    observations_path = observations_csv_path(processed_dir, minutes)
    if not observations_path.exists():
        fallback = observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes)
        if fallback.exists():
            observations_path = fallback
        else:
            raise SystemExit("observations dataset not found. Run scripts/build_dataset.py first.")

    observations = load_csv(observations_path)
    if observations.empty:
        raise SystemExit("observations dataset is empty.")

    window_hours = (
        int(args.window_hours)
        if args.window_hours is not None
        else int(config.analytics.reliability.default_window_hours)
    )
    start_dt, end_dt = resolve_time_window(
        observations, start_text=args.start, end_text=args.end, default_window_hours=window_hours
    )

    timestamp_tag = end_dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else (config.paths.outputs_dir / "reports" / timestamp_tag)
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    artifacts: dict[str, str] = {}

    segments_path = segments_csv_path(processed_dir)
    if segments_path.exists():
        artifacts["segments_csv"] = str(segments_path)

    rel_spec = reliability_spec_from_config(config)
    segment_rankings = compute_reliability_rankings(
        observations, rel_spec, start=start_dt, end=end_dt, limit=int(args.limit)
    )
    segment_rankings_path = output_dir / f"segment_rankings_{minutes}min.csv"
    save_csv(segment_rankings, segment_rankings_path)
    artifacts["segment_rankings_csv"] = str(segment_rankings_path)

    if args.include_corridors:
        corridors_path = config.analytics.corridors.corridors_csv
        if not corridors_path.exists():
            raise SystemExit(
                "corridors.csv not found. Copy configs/corridors.example.csv to configs/corridors.csv first."
            )
        corridors = load_corridors_csv(corridors_path)
        corridor_rankings = compute_corridor_reliability_rankings(
            observations,
            corridors,
            rel_spec,
            speed_weighting=config.analytics.corridors.speed_weighting,
            weight_column=config.analytics.corridors.weight_column,
            start=start_dt,
            end=end_dt,
            limit=int(args.limit),
        )
        meta = corridor_metadata(corridors)
        if not corridor_rankings.empty:
            corridor_rankings = corridor_rankings.merge(meta, on="corridor_id", how="left")
        corridor_rankings_path = output_dir / f"corridor_rankings_{minutes}min.csv"
        save_csv(corridor_rankings, corridor_rankings_path)
        artifacts["corridor_rankings_csv"] = str(corridor_rankings_path)

    anomaly_spec = anomaly_spec_from_config(config)

    if args.anomaly_segment_id:
        segment_id = str(args.anomaly_segment_id)
        anomaly_points = compute_anomaly_timeseries(
            observations, anomaly_spec, entity_id=segment_id, start=start_dt, end=end_dt
        )
        anomaly_events = summarize_anomaly_events(anomaly_points, anomaly_spec)
        points_path = output_dir / f"anomalies_points_segment_{segment_id}_{minutes}min.csv"
        events_path = output_dir / f"anomalies_events_segment_{segment_id}_{minutes}min.csv"
        save_csv(anomaly_points, points_path)
        save_csv(anomaly_events, events_path)
        artifacts["segment_anomaly_points_csv"] = str(points_path)
        artifacts["segment_anomaly_events_csv"] = str(events_path)

    if args.anomaly_corridor_id:
        corridor_id = str(args.anomaly_corridor_id)
        corridors_path = config.analytics.corridors.corridors_csv
        if not corridors_path.exists():
            raise SystemExit(
                "corridors.csv not found. Copy configs/corridors.example.csv to configs/corridors.csv first."
            )
        corridors = load_corridors_csv(corridors_path)
        corridors = corridors[corridors["corridor_id"].astype(str) == corridor_id]
        if corridors.empty:
            raise SystemExit("anomaly corridor_id not found in corridors.csv.")

        corridor_ts = aggregate_observations_to_corridors(
            observations,
            corridors,
            speed_weighting=config.analytics.corridors.speed_weighting,
            weight_column=config.analytics.corridors.weight_column,
        )
        corridor_spec = spec_for_entity(anomaly_spec, entity_id_column="corridor_id")
        anomaly_points = compute_anomaly_timeseries(
            corridor_ts, corridor_spec, entity_id=corridor_id, start=start_dt, end=end_dt
        )
        anomaly_events = summarize_anomaly_events(anomaly_points, corridor_spec)
        points_path = output_dir / f"anomalies_points_corridor_{corridor_id}_{minutes}min.csv"
        events_path = output_dir / f"anomalies_events_corridor_{corridor_id}_{minutes}min.csv"
        save_csv(anomaly_points, points_path)
        save_csv(anomaly_events, events_path)
        artifacts["corridor_anomaly_points_csv"] = str(points_path)
        artifacts["corridor_anomaly_events_csv"] = str(events_path)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
        "minutes": minutes,
        "limit": int(args.limit),
        "inputs": {"observations_csv": str(observations_path)},
        "artifacts": artifacts,
        "config": {
            "analytics": {
                "reliability": config.analytics.reliability.model_dump(mode="json"),
                "corridors": config.analytics.corridors.model_dump(mode="json"),
                "anomalies": config.analytics.anomalies.model_dump(mode="json"),
            }
        },
    }

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    artifacts["summary_json"] = str(summary_path)

    print(f"Report directory: {output_dir}")
    for key, value in artifacts.items():
        print(f"- {key}: {value}")


if __name__ == "__main__":
    main()
