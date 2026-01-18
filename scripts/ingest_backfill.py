from __future__ import annotations

import argparse
import json
import _bootstrap  # noqa: F401
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import AppConfig, get_config
from trafficpulse.storage.datasets import (
    append_csv,
    events_csv_path,
    events_parquet_path,
    observations_csv_path,
    observations_parquet_path,
    save_csv,
    save_parquet,
    segments_csv_path,
    segments_parquet_path,
)
from trafficpulse.utils.time import parse_datetime


@dataclass(frozen=True)
class Checkpoint:
    dataset: str
    next_start: str

    @classmethod
    def load(cls, path: Path) -> Optional["Checkpoint"]:
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        dataset = str(data.get("dataset", "")).strip()
        next_start = str(data.get("next_start", "")).strip()
        if not dataset or not next_start:
            return None
        return cls(dataset=dataset, next_start=next_start)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"dataset": self.dataset, "next_start": self.next_start}, indent=2) + "\n",
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Slow, resumable TDX backfill under rate limits (VD observations or TrafficEvents)."
    )
    parser.add_argument("--dataset", choices=["vd", "events"], required=True)
    parser.add_argument(
        "--source",
        choices=["historical", "live"],
        default="historical",
        help="For vd: historical uses JSONL-by-date backfill; live uses live VDLive endpoint.",
    )
    parser.add_argument("--start", required=True, help="Start datetime (ISO 8601).")
    parser.add_argument("--end", required=True, help="End datetime (ISO 8601).")
    parser.add_argument(
        "--cities",
        nargs="*",
        default=None,
        help="Override city list (default: config ingestion.{dataset}.cities).",
    )
    parser.add_argument(
        "--chunk-minutes",
        type=int,
        default=None,
        help="Chunk size for resumable backfill (default: config.ingestion.query_chunk_minutes).",
    )
    parser.add_argument(
        "--min-request-interval",
        type=float,
        default=None,
        help="Client-side throttle seconds between requests (override config.tdx.min_request_interval_seconds).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable disk cache for this run (useful for live smoke tests).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help="Override config.tdx.max_retries.",
    )
    parser.add_argument(
        "--checkpoint",
        default="data/cache/backfill_checkpoint.json",
        help="Checkpoint file path (default: data/cache/backfill_checkpoint.json).",
    )
    parser.add_argument(
        "--processed-dir",
        default=None,
        help="Override output directory (default: config.paths.processed_dir).",
    )
    parser.add_argument(
        "--parquet-dir",
        default=None,
        help="Override Parquet directory (default: config.warehouse.parquet_dir).",
    )
    parser.add_argument(
        "--write-parquet",
        action="store_true",
        help="Also write canonical Parquet files at the end of this run (can be slow for large backfills).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write outputs/checkpoints.",
    )
    return parser.parse_args()


def _first_non_null(series: pd.Series) -> Any:
    non_null = series.dropna()
    return non_null.iloc[0] if not non_null.empty else None


def _merge_by_key(existing: pd.DataFrame, incoming: pd.DataFrame, key: str) -> pd.DataFrame:
    if existing.empty:
        return incoming
    if incoming.empty:
        return existing
    merged = pd.concat([existing, incoming], ignore_index=True, copy=False)
    merged = merged.groupby(key, as_index=False).agg(_first_non_null)
    return merged.sort_values(key).reset_index(drop=True)


def _override_config(config: AppConfig, args: argparse.Namespace) -> AppConfig:
    updated = config
    if getattr(args, "no_cache", False):
        updated = updated.model_copy(update={"cache": updated.cache.model_copy(update={"enabled": False})})
    if args.min_request_interval is not None:
        updated = updated.model_copy(
            update={
                "tdx": updated.tdx.model_copy(
                    update={"min_request_interval_seconds": float(args.min_request_interval)}
                )
            }
        )
    if args.max_retries is not None:
        updated = updated.model_copy(
            update={"tdx": updated.tdx.model_copy(update={"max_retries": int(args.max_retries)})}
        )
    if args.chunk_minutes is not None:
        updated = updated.model_copy(
            update={
                "ingestion": updated.ingestion.model_copy(
                    update={"query_chunk_minutes": int(args.chunk_minutes)}
                )
            }
        )
    return updated.resolve_paths()


def main() -> None:
    args = parse_args()
    configure_logging()

    base_config = get_config()
    config = _override_config(base_config, args)

    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    parquet_dir = (
        Path(args.parquet_dir)
        if args.parquet_dir
        else (processed_dir / "parquet" if args.processed_dir else config.warehouse.parquet_dir)
    )

    requested_start: datetime = parse_datetime(args.start)
    requested_end: datetime = parse_datetime(args.end)
    if requested_end <= requested_start:
        raise SystemExit("--end must be greater than --start")

    checkpoint_path = Path(args.checkpoint)
    checkpoint = Checkpoint.load(checkpoint_path)
    start = requested_start
    if checkpoint is not None and checkpoint.dataset == args.dataset:
        try:
            checkpoint_start = parse_datetime(checkpoint.next_start)
        except Exception:
            checkpoint_start = None
        if checkpoint_start is not None and requested_start <= checkpoint_start < requested_end:
            start = checkpoint_start

    chunk_minutes = int(args.chunk_minutes or config.ingestion.query_chunk_minutes)
    if chunk_minutes <= 0:
        raise SystemExit("--chunk-minutes must be > 0")

    segments_out = segments_csv_path(processed_dir)
    observations_out = observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes)
    events_out = events_csv_path(processed_dir)

    segments_df = pd.DataFrame()
    events_df = pd.DataFrame()
    if not args.dry_run:
        if segments_out.exists():
            segments_df = pd.read_csv(segments_out)
        if args.dataset == "events" and events_out.exists():
            events_df = pd.read_csv(events_out)

    client = TdxTrafficClient(config=config)
    try:
        cursor = start
        while cursor < requested_end:
            if args.dataset == "vd" and args.source == "live":
                # Live VD is a snapshot endpoint; chunking doesn't apply. Fetch once per loop.
                chunk_end = requested_end
            else:
                chunk_end = min(cursor + timedelta(minutes=chunk_minutes), requested_end)

            if args.dataset == "vd":
                if args.source == "live":
                    chunk_segments, chunk_observations = client.download_vd_live(
                        start=cursor, end=chunk_end, cities=args.cities
                    )
                else:
                    chunk_segments, chunk_observations = client.download_vd_historical(
                        start=cursor, end=chunk_end, cities=args.cities
                    )
                if not args.dry_run:
                    segments_df = _merge_by_key(segments_df, chunk_segments, key="segment_id")
                    save_csv(segments_df, segments_out)
                    append_csv(chunk_observations, observations_out)
            else:
                chunk_events = client.download_events(start=cursor, end=chunk_end, cities=args.cities)
                if not args.dry_run:
                    events_df = _merge_by_key(events_df, chunk_events, key="event_id")
                    save_csv(events_df, events_out)

            cursor = chunk_end
            if not args.dry_run:
                Checkpoint(dataset=args.dataset, next_start=cursor.isoformat()).save(checkpoint_path)

            print(
                f"[{args.dataset}] progress: {cursor.isoformat()} / {requested_end.isoformat()} "
                f"(chunk={chunk_minutes}m, throttle={config.tdx.min_request_interval_seconds}s)"
            )

            if args.dataset == "vd" and args.source == "live":
                break
    finally:
        client.close()

    if args.dry_run or not args.write_parquet or not config.warehouse.enabled:
        return

    if args.dataset == "vd":
        segments_parquet = segments_parquet_path(parquet_dir)
        observations_parquet = observations_parquet_path(
            parquet_dir, config.preprocessing.source_granularity_minutes
        )
        save_parquet(pd.read_csv(segments_out), segments_parquet)
        save_parquet(pd.read_csv(observations_out), observations_parquet)
        print(f"Wrote Parquet: {segments_parquet}")
        print(f"Wrote Parquet: {observations_parquet}")
    else:
        events_parquet = events_parquet_path(parquet_dir)
        save_parquet(pd.read_csv(events_out), events_parquet)
        print(f"Wrote Parquet: {events_parquet}")


if __name__ == "__main__":
    main()
