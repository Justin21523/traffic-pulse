from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

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
class DailyBackfillState:
    last_backfill_date: Optional[str] = None  # YYYY-MM-DD (Asia/Taipei)

    @classmethod
    def load(cls, path: Path) -> "DailyBackfillState":
        if not path.exists():
            return cls()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return cls()
        if not isinstance(data, dict):
            return cls()
        value = data.get("last_backfill_date")
        return cls(last_backfill_date=str(value) if value else None)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"last_backfill_date": self.last_backfill_date}, indent=2) + "\n",
            encoding="utf-8",
        )


def _write_ingest_status(path: Path, *, ok: bool, updated_files: list[str] | None = None, error: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "last_ingest_ok": bool(ok),
        "updated_files": updated_files or [],
        "last_error": error,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill yesterday's VD observations (historical feed) and write to canonical outputs."
    )
    parser.add_argument(
        "--cities",
        nargs="*",
        default=None,
        help="Override cities (default: config ingestion.vd.cities).",
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
        help="Disable disk cache for this run.",
    )
    parser.add_argument(
        "--processed-dir",
        default=None,
        help="Override output directory (default: config.paths.processed_dir).",
    )
    parser.add_argument(
        "--state-path",
        default="data/cache/daily_backfill_state.json",
        help="State file path (default: data/cache/daily_backfill_state.json).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even if the state file says yesterday is already backfilled.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and compute but do not write outputs/state.",
    )
    return parser.parse_args()


def _override_config(config: AppConfig, args: argparse.Namespace) -> AppConfig:
    updated = config.model_copy(
        update={
            "tdx": config.tdx.model_copy(update={"min_request_interval_seconds": float(args.min_request_interval)})
        }
    )
    if args.no_cache:
        updated = updated.model_copy(update={"cache": updated.cache.model_copy(update={"enabled": False})})
    return updated.resolve_paths()


def main() -> None:
    args = parse_args()
    configure_logging()

    base_config = get_config()
    config = _override_config(base_config, args)

    tz = datetime.now().astimezone().tzinfo
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(config.app.timezone)
    except Exception:
        pass

    now = datetime.now(tz).replace(second=0, microsecond=0)
    today = now.date()
    yesterday = today - timedelta(days=1)
    yesterday_start = datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=tz)
    today_start = datetime(today.year, today.month, today.day, tzinfo=tz)

    state_path = Path(args.state_path)
    ingest_status_path = state_path.parent / "ingest_status.json"
    state = DailyBackfillState.load(state_path)
    if not args.force and state.last_backfill_date == yesterday.strftime("%Y-%m-%d"):
        print(f"[vd-backfill] already backfilled {state.last_backfill_date}; skipping")
        return

    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    segments_out = segments_csv_path(processed_dir)
    observations_out = observations_csv_path(processed_dir, config.preprocessing.source_granularity_minutes)

    client = TdxTrafficClient(config=config)
    try:
        segments, observations = client.download_vd_historical(
            start=yesterday_start, end=today_start, cities=args.cities
        )
    except Exception as exc:
        _write_ingest_status(ingest_status_path, ok=False, updated_files=[], error=str(exc))
        raise
    finally:
        client.close()

    print(f"[vd-backfill] segments rows: {len(segments):,}")
    print(f"[vd-backfill] observations rows: {len(observations):,}")
    if observations.empty:
        print("[vd-backfill] observations empty; not writing state")
        _write_ingest_status(ingest_status_path, ok=False, updated_files=[], error="empty observations")
        return

    if args.dry_run:
        return

    if not segments.empty:
        save_csv(segments, segments_out)
    append_csv(observations, observations_out)

    state = DailyBackfillState(last_backfill_date=yesterday.strftime("%Y-%m-%d"))
    state.save(state_path)
    _write_ingest_status(
        ingest_status_path,
        ok=True,
        updated_files=[str(segments_out), str(observations_out), str(state_path)],
        error=None,
    )
    print(f"[vd-backfill] done: {state.last_backfill_date} -> {observations_out}")


if __name__ == "__main__":
    main()
