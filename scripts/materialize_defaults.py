from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from trafficpulse.analytics.corridors import corridor_metadata, load_corridors_csv
from trafficpulse.analytics.corridors import compute_corridor_reliability_rankings
from trafficpulse.analytics.reliability import (
    compute_reliability_metrics,
    compute_reliability_rankings,
    reliability_spec_from_config,
)
from trafficpulse.settings import get_config
from trafficpulse.storage.backend import duckdb_backend
from trafficpulse.storage.datasets import (
    load_csv,
    load_parquet,
    observations_csv_path,
    observations_parquet_path,
    save_csv,
    segments_csv_path,
    segments_parquet_path,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Materialize default analytics outputs (map snapshot + rankings) for fast API responses."
    )
    p.add_argument("--minutes", type=int, default=None, help="Granularity minutes (default: config target).")
    p.add_argument("--window-hours", type=int, default=24, help="Window hours (default: 24).")
    p.add_argument("--limit-rankings", type=int, default=5000, help="Max rows to store for rankings (default: 5000).")
    return p.parse_args()


def _max_observation_timestamp(df: pd.DataFrame) -> datetime | None:
    if df.empty or "timestamp" not in df.columns:
        return None
    ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    ts = ts.dropna()
    if ts.empty:
        return None
    out = ts.max().to_pydatetime()
    if out.tzinfo is None:
        out = out.replace(tzinfo=timezone.utc)
    return out


def main() -> None:
    args = parse_args()
    config = get_config()
    cache_dir = config.paths.cache_dir
    cache_dir.mkdir(parents=True, exist_ok=True)

    minutes = int(args.minutes or config.preprocessing.target_granularity_minutes)
    window_hours = int(args.window_hours)
    limit_rankings = int(args.limit_rankings)

    processed_dir = config.paths.processed_dir
    parquet_dir = config.warehouse.parquet_dir
    backend = duckdb_backend(config)

    seg_parquet = segments_parquet_path(parquet_dir)
    seg_csv = segments_csv_path(processed_dir)
    if backend is not None and seg_parquet.exists():
        segments = backend.query_segments(columns=["segment_id", "lat", "lon", "city"])
    elif config.warehouse.enabled and seg_parquet.exists():
        segments = load_parquet(seg_parquet)
    else:
        if not seg_csv.exists():
            raise SystemExit("segments dataset not found. Run scripts/build_dataset.py first.")
        segments = load_csv(seg_csv)

    if segments.empty or "segment_id" not in segments.columns:
        raise SystemExit("segments dataset is empty or missing segment_id.")

    segments = segments.copy()
    segments["segment_id"] = segments["segment_id"].astype(str)
    if "lat" in segments.columns:
        segments["lat"] = pd.to_numeric(segments["lat"], errors="coerce")
    if "lon" in segments.columns:
        segments["lon"] = pd.to_numeric(segments["lon"], errors="coerce")
    if "city" not in segments.columns:
        segments["city"] = pd.NA
    segments = segments.dropna(subset=["segment_id"])

    obs_parquet = observations_parquet_path(parquet_dir, minutes)
    obs_csv = observations_csv_path(processed_dir, minutes)
    if not obs_parquet.exists() and not obs_csv.exists():
        fallback_minutes = int(config.preprocessing.source_granularity_minutes)
        minutes = fallback_minutes
        obs_parquet = observations_parquet_path(parquet_dir, minutes)
        obs_csv = observations_csv_path(processed_dir, minutes)
        if not obs_parquet.exists() and not obs_csv.exists():
            raise SystemExit("observations dataset not found. Run ingestion + scripts/build_dataset.py first.")

    max_ts: datetime | None = None
    if backend is not None and obs_parquet.exists():
        max_ts = backend.max_observation_timestamp(minutes=minutes)
        if max_ts is not None and max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=timezone.utc)
    else:
        df_max = load_parquet(obs_parquet) if config.warehouse.enabled and obs_parquet.exists() else load_csv(obs_csv)
        max_ts = _max_observation_timestamp(df_max)

    if max_ts is None:
        raise SystemExit("Could not determine max observation timestamp; dataset may be empty.")

    end_dt = max_ts + timedelta(minutes=minutes)
    start_dt = end_dt - timedelta(hours=window_hours)

    segment_ids = segments["segment_id"].astype(str).unique().tolist()

    if backend is not None and obs_parquet.exists():
        observations = backend.query_observations(
            minutes=minutes,
            segment_ids=segment_ids,
            start=start_dt,
            end=end_dt,
            columns=["timestamp", "segment_id", "speed_kph", "volume", "occupancy_pct"],
        )
    else:
        observations = load_parquet(obs_parquet) if config.warehouse.enabled and obs_parquet.exists() else load_csv(obs_csv)

    if observations.empty:
        raise SystemExit("observations query returned empty; cannot materialize.")

    if "timestamp" not in observations.columns or "segment_id" not in observations.columns:
        raise SystemExit("observations dataset missing timestamp/segment_id columns.")

    observations = observations.copy()
    observations["segment_id"] = observations["segment_id"].astype(str)
    observations["timestamp"] = pd.to_datetime(observations["timestamp"], errors="coerce", utc=True)
    observations = observations.dropna(subset=["timestamp", "segment_id"])
    observations = observations[(observations["timestamp"] >= pd.Timestamp(start_dt)) & (observations["timestamp"] < pd.Timestamp(end_dt))]
    observations = observations[observations["segment_id"].isin(set(segment_ids))]

    if observations.empty:
        raise SystemExit("observations empty after filtering to time window/segments.")

    spec = reliability_spec_from_config(config)

    metrics = compute_reliability_metrics(observations, spec, start=start_dt, end=end_dt)
    if metrics.empty:
        raise SystemExit("metrics computation returned empty.")

    snapshot = segments[["segment_id", "lat", "lon", "city"]].merge(metrics, on="segment_id", how="inner")
    snapshot = snapshot[snapshot["n_samples"] > 0].copy()
    snapshot = snapshot.sort_values("segment_id").reset_index(drop=True)
    snapshot = snapshot.astype(object).where(pd.notnull(snapshot), None)

    rankings = compute_reliability_rankings(observations, spec, start=start_dt, end=end_dt, limit=limit_rankings)
    rankings = rankings.astype(object).where(pd.notnull(rankings), None)

    corridors_csv_path = config.analytics.corridors.corridors_csv
    corridor_rankings = pd.DataFrame()
    if corridors_csv_path.exists():
        try:
            corridors = load_corridors_csv(corridors_csv_path)
            corridor_rankings = compute_corridor_reliability_rankings(
                observations,
                corridors,
                spec,
                speed_weighting=config.analytics.corridors.speed_weighting,
                weight_column=config.analytics.corridors.weight_column,
                start=start_dt,
                end=end_dt,
                limit=limit_rankings,
            )
            if not corridor_rankings.empty:
                meta = corridor_metadata(corridors)
                corridor_rankings = corridor_rankings.merge(meta, on="corridor_id", how="left")
                corridor_rankings = corridor_rankings.astype(object).where(pd.notnull(corridor_rankings), None)
        except Exception:
            corridor_rankings = pd.DataFrame()

    snapshot_out = cache_dir / f"materialized_map_snapshot_{minutes}m_{window_hours}h.csv"
    rankings_out = cache_dir / f"materialized_rankings_segments_{minutes}m_{window_hours}h.csv"
    corridor_rankings_out = cache_dir / f"materialized_rankings_corridors_{minutes}m_{window_hours}h.csv"

    save_csv(snapshot, snapshot_out)
    save_csv(rankings, rankings_out)
    if not corridor_rankings.empty:
        save_csv(corridor_rankings, corridor_rankings_out)

    meta_out = cache_dir / "materialized_defaults.json"
    meta_out.write_text(
        json.dumps(
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "minutes": minutes,
                "window_hours": window_hours,
                "start_utc": start_dt.isoformat(),
                "end_utc": end_dt.isoformat(),
                "snapshot_path": str(snapshot_out),
                "rankings_path": str(rankings_out),
                "corridor_rankings_path": str(corridor_rankings_out) if corridor_rankings_out.exists() else None,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"[materialize] wrote {snapshot_out}")
    print(f"[materialize] wrote {rankings_out}")
    if corridor_rankings_out.exists():
        print(f"[materialize] wrote {corridor_rankings_out}")
    print(f"[materialize] wrote {meta_out}")


if __name__ == "__main__":
    main()
