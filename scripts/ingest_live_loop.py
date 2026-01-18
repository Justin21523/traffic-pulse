from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import AppConfig, get_config
from trafficpulse.storage.datasets import (
    append_csv,
    observations_csv_path,
    save_csv,
    segments_csv_path,
)


@dataclass(frozen=True)
class LiveLoopState:
    last_snapshot_timestamp: Optional[str] = None

    @classmethod
    def load(cls, path: Path) -> "LiveLoopState":
        if not path.exists():
            return cls()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return cls()
        if not isinstance(data, dict):
            return cls()
        value = data.get("last_snapshot_timestamp")
        return cls(last_snapshot_timestamp=str(value) if value else None)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"last_snapshot_timestamp": self.last_snapshot_timestamp}, indent=2) + "\n",
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously ingest TDX VDLive snapshots under rate limits (deduped by snapshot timestamp)."
    )
    parser.add_argument(
        "--cities",
        nargs="*",
        default=None,
        help="Override cities (default: config ingestion.vd.cities).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Loop interval seconds (default: 60).",
    )
    parser.add_argument(
        "--min-request-interval",
        type=float,
        default=1.0,
        help="Client-side throttle seconds between requests (default: 1.0).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable disk cache for this run (recommended for live loops).",
    )
    parser.add_argument(
        "--state-path",
        default="data/cache/live_loop_state.json",
        help="State file path (default: data/cache/live_loop_state.json).",
    )
    parser.add_argument(
        "--processed-dir",
        default=None,
        help="Override output directory (default: config.paths.processed_dir).",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Stop after N iterations (default: run forever).",
    )
    return parser.parse_args()


def _override_config(config: AppConfig, args: argparse.Namespace) -> AppConfig:
    updated = config
    updated = updated.model_copy(
        update={
            "tdx": updated.tdx.model_copy(
                update={"min_request_interval_seconds": float(args.min_request_interval)}
            )
        }
    )
    if args.no_cache:
        updated = updated.model_copy(update={"cache": updated.cache.model_copy(update={"enabled": False})})
    return updated.resolve_paths()


def _parse_timestamp(value: str) -> datetime:
    # Stored values come from pandas UTC conversion: "YYYY-MM-DD HH:MM:SS+00:00"
    return pd.to_datetime(value, utc=True).to_pydatetime()


def main() -> None:
    args = parse_args()
    configure_logging()

    base_config = get_config()
    config = _override_config(base_config, args)

    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    segments_out = segments_csv_path(processed_dir)
    observations_out = observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes)

    state_path = Path(args.state_path)
    state = LiveLoopState.load(state_path)

    iterations = 0
    client = TdxTrafficClient(config=config)
    try:
        # Refresh segments at startup so the UI has metadata even if we only ingest snapshots.
        segments = client.download_vd_metadata(cities=args.cities)
        if not segments.empty:
            save_csv(segments, segments_out)
            print(f"[vd-live] segments refreshed: {len(segments):,} rows -> {segments_out}")

        while True:
            iterations += 1
            if args.max_iterations is not None and iterations > int(args.max_iterations):
                break

            snapshot = client.download_vd_live_snapshot(cities=args.cities)
            if snapshot.empty:
                print("[vd-live] empty snapshot; sleeping")
                time.sleep(args.interval_seconds)
                continue

            snapshot_ts = str(snapshot["timestamp"].max())
            if state.last_snapshot_timestamp == snapshot_ts:
                print(f"[vd-live] unchanged snapshot {snapshot_ts}; skipping append")
            else:
                # Avoid appending older snapshots in case clocks/config mismatch.
                if state.last_snapshot_timestamp is not None:
                    if _parse_timestamp(snapshot_ts) <= _parse_timestamp(state.last_snapshot_timestamp):
                        print(
                            f"[vd-live] non-increasing snapshot {snapshot_ts} (last={state.last_snapshot_timestamp}); skipping"
                        )
                    else:
                        append_csv(snapshot, observations_out)
                        state = LiveLoopState(last_snapshot_timestamp=snapshot_ts)
                        state.save(state_path)
                        print(
                            f"[vd-live] appended {len(snapshot):,} rows at {snapshot_ts} -> {observations_out}"
                        )
                else:
                    append_csv(snapshot, observations_out)
                    state = LiveLoopState(last_snapshot_timestamp=snapshot_ts)
                    state.save(state_path)
                    print(f"[vd-live] appended {len(snapshot):,} rows at {snapshot_ts} -> {observations_out}")

            time.sleep(args.interval_seconds)
    finally:
        client.close()


if __name__ == "__main__":
    main()
