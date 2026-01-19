from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient
from trafficpulse.ingestion.errors import classify_ingest_error
from trafficpulse.ingestion.ledger import safe_append_ledger_entry
from trafficpulse.logging_config import configure_logging
from trafficpulse.quality.observations import clean_observations
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
    last_segments_refresh_utc: Optional[str] = None

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
        seg_refresh = data.get("last_segments_refresh_utc")
        return cls(
            last_snapshot_timestamp=str(value) if value else None,
            last_segments_refresh_utc=str(seg_refresh) if seg_refresh else None,
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "last_snapshot_timestamp": self.last_snapshot_timestamp,
                    "last_segments_refresh_utc": self.last_segments_refresh_utc,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


def _write_ingest_status(
    path: Path,
    *,
    ok: bool,
    updated_files: list[str] | None = None,
    error: str | None = None,
    quality: dict[str, int] | None = None,
    error_code: str | None = None,
    error_kind: str | None = None,
    consecutive_failures: int | None = None,
    backoff_seconds: int | None = None,
    last_success_utc: str | None = None,
    rate_limit: dict[str, float | int | None] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "last_ingest_ok": bool(ok),
        "updated_files": updated_files or [],
        "last_error": error,
        "last_error_code": error_code,
        "last_error_kind": error_kind,
        "consecutive_failures": consecutive_failures,
        "backoff_seconds": backoff_seconds,
        "last_success_utc": last_success_utc,
        "quality": quality or {},
        "rate_limit": rate_limit or {},
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


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
    parser.add_argument(
        "--segments-refresh-hours",
        type=int,
        default=24,
        help="Refresh segments metadata at most once per N hours (default: 24).",
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
    ingest_status_path = state_path.parent / "ingest_status.json"
    ledger_path = state_path.parent / "ingest_ledger.jsonl"
    state = LiveLoopState.load(state_path)

    iterations = 0
    client = TdxTrafficClient(config=config)
    consecutive_failures = 0
    backoff_seconds = 0
    last_success_utc: str | None = None
    try:
        # Refresh segments at startup (and occasionally) so the UI has metadata even if snapshots ingest slowly.
        try:
            do_refresh = True
            if state.last_segments_refresh_utc:
                try:
                    last_refresh = pd.to_datetime(state.last_segments_refresh_utc, utc=True).to_pydatetime()
                    hours = (datetime.now(timezone.utc) - last_refresh).total_seconds() / 3600.0
                    do_refresh = hours >= float(args.segments_refresh_hours)
                except Exception:
                    do_refresh = True
            if do_refresh:
                segments = client.download_vd_metadata(cities=args.cities)
                if not segments.empty:
                    save_csv(segments, segments_out)
                    state = LiveLoopState(
                        last_snapshot_timestamp=state.last_snapshot_timestamp,
                        last_segments_refresh_utc=datetime.now(timezone.utc).isoformat(),
                    )
                    state.save(state_path)
                    _write_ingest_status(
                        ingest_status_path,
                        ok=True,
                        updated_files=[str(segments_out)],
                        error=None,
                        rate_limit=client.rate_limit_summary(),
                    )
                    safe_append_ledger_entry(
                        ledger_path,
                        {
                            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                            "source": "vd",
                            "runner": "live_loop",
                            "event": "segments_refreshed",
                            "ok": True,
                            "cities": args.cities or [],
                            "rows": int(len(segments)),
                            "updated_files": [str(segments_out)],
                        },
                    )
                    print(f"[vd-live] segments refreshed: {len(segments):,} rows -> {segments_out}")
        except Exception as exc:
            # Metadata refresh should not prevent long-running snapshot ingestion.
            _write_ingest_status(
                ingest_status_path,
                ok=False,
                updated_files=[],
                error=f"segments refresh failed: {exc}",
                rate_limit=client.rate_limit_summary(),
            )
            safe_append_ledger_entry(
                ledger_path,
                {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "source": "vd",
                    "runner": "live_loop",
                    "event": "segments_refresh_failed",
                    "ok": False,
                    "cities": args.cities or [],
                    "error": str(exc),
                    "updated_files": [],
                },
            )
            print(f"[vd-live] segments refresh error: {exc}; continuing to snapshot loop")

        while True:
            iterations += 1
            if args.max_iterations is not None and iterations > int(args.max_iterations):
                break

            try:
                snapshot = client.download_vd_live_snapshot(cities=args.cities)
                if snapshot.empty:
                    print("[vd-live] empty snapshot; sleeping")
                    time.sleep(args.interval_seconds)
                    continue

                cleaned, stats = clean_observations(snapshot)
                if cleaned.empty:
                    safe_append_ledger_entry(
                        ledger_path,
                        {
                            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                            "source": "vd",
                            "runner": "live_loop",
                            "event": "snapshot_discarded",
                            "ok": True,
                            "cities": args.cities or [],
                            "snapshot_timestamp": str(snapshot["timestamp"].max()) if "timestamp" in snapshot.columns and not snapshot.empty else None,
                            "input_rows": stats.input_rows,
                            "output_rows": stats.output_rows,
                            "dropped_missing_keys": stats.dropped_missing_keys,
                            "dropped_invalid_timestamp": stats.dropped_invalid_timestamp,
                            "dropped_invalid_speed": stats.dropped_invalid_speed,
                            "dropped_duplicates": stats.dropped_duplicates,
                            "updated_files": [],
                        },
                    )
                    print("[vd-live] snapshot contained no valid rows after cleaning; sleeping")
                    time.sleep(args.interval_seconds)
                    continue

                snapshot_ts = str(cleaned["timestamp"].max()) if "timestamp" in cleaned.columns else None
                if not snapshot_ts:
                    print("[vd-live] could not determine snapshot timestamp after cleaning; sleeping")
                    time.sleep(args.interval_seconds)
                    continue
                if state.last_snapshot_timestamp == snapshot_ts:
                    print(f"[vd-live] unchanged snapshot {snapshot_ts}; skipping append")
                    _write_ingest_status(
                        ingest_status_path,
                        ok=True,
                        updated_files=[],
                        error=None,
                        quality={
                            "input_rows": stats.input_rows,
                            "output_rows": stats.output_rows,
                            "dropped_missing_keys": stats.dropped_missing_keys,
                            "dropped_invalid_timestamp": stats.dropped_invalid_timestamp,
                            "dropped_invalid_speed": stats.dropped_invalid_speed,
                            "dropped_duplicates": stats.dropped_duplicates,
                        },
                        error_code=None,
                        error_kind=None,
                        consecutive_failures=0,
                        backoff_seconds=0,
                        last_success_utc=datetime.now(timezone.utc).isoformat(),
                        rate_limit=client.rate_limit_summary(),
                    )
                    last_success_utc = datetime.now(timezone.utc).isoformat()
                else:
                    # Avoid appending older snapshots in case clocks/config mismatch.
                    if state.last_snapshot_timestamp is not None:
                        if _parse_timestamp(snapshot_ts) <= _parse_timestamp(state.last_snapshot_timestamp):
                            print(
                                f"[vd-live] non-increasing snapshot {snapshot_ts} (last={state.last_snapshot_timestamp}); skipping"
                            )
                            _write_ingest_status(
                                ingest_status_path,
                                ok=True,
                                updated_files=[],
                                error=None,
                                quality={
                                    "input_rows": stats.input_rows,
                                    "output_rows": stats.output_rows,
                                    "dropped_missing_keys": stats.dropped_missing_keys,
                                    "dropped_invalid_timestamp": stats.dropped_invalid_timestamp,
                                    "dropped_invalid_speed": stats.dropped_invalid_speed,
                                    "dropped_duplicates": stats.dropped_duplicates,
                                },
                                error_code=None,
                                error_kind=None,
                                consecutive_failures=0,
                                backoff_seconds=0,
                                last_success_utc=datetime.now(timezone.utc).isoformat(),
                                rate_limit=client.rate_limit_summary(),
                            )
                            last_success_utc = datetime.now(timezone.utc).isoformat()
                        else:
                            append_csv(cleaned, observations_out)
                            state = LiveLoopState(last_snapshot_timestamp=snapshot_ts)
                            state.save(state_path)
                            _write_ingest_status(
                                ingest_status_path,
                                ok=True,
                                updated_files=[str(observations_out), str(state_path)],
                                error=None,
                                quality={
                                    "input_rows": stats.input_rows,
                                    "output_rows": stats.output_rows,
                                    "dropped_missing_keys": stats.dropped_missing_keys,
                                    "dropped_invalid_timestamp": stats.dropped_invalid_timestamp,
                                    "dropped_invalid_speed": stats.dropped_invalid_speed,
                                    "dropped_duplicates": stats.dropped_duplicates,
                                },
                                error_code=None,
                                error_kind=None,
                                consecutive_failures=0,
                                backoff_seconds=0,
                                last_success_utc=datetime.now(timezone.utc).isoformat(),
                                rate_limit=client.rate_limit_summary(),
                            )
                            safe_append_ledger_entry(
                                ledger_path,
                                {
                                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                                    "source": "vd",
                                    "runner": "live_loop",
                                    "event": "snapshot_appended",
                                    "ok": True,
                                    "cities": args.cities or [],
                                    "snapshot_timestamp": snapshot_ts,
                                    "input_rows": stats.input_rows,
                                    "output_rows": stats.output_rows,
                                    "dropped_missing_keys": stats.dropped_missing_keys,
                                    "dropped_invalid_timestamp": stats.dropped_invalid_timestamp,
                                    "dropped_invalid_speed": stats.dropped_invalid_speed,
                                    "dropped_duplicates": stats.dropped_duplicates,
                                    "updated_files": [str(observations_out), str(state_path)],
                                },
                            )
                            print(
                                f"[vd-live] appended {len(cleaned):,} rows at {snapshot_ts} -> {observations_out}"
                            )
                            consecutive_failures = 0
                            backoff_seconds = 0
                            last_success_utc = datetime.now(timezone.utc).isoformat()
                    else:
                        append_csv(cleaned, observations_out)
                        state = LiveLoopState(last_snapshot_timestamp=snapshot_ts)
                        state.save(state_path)
                        _write_ingest_status(
                            ingest_status_path,
                            ok=True,
                            updated_files=[str(observations_out), str(state_path)],
                            error=None,
                            quality={
                                "input_rows": stats.input_rows,
                                "output_rows": stats.output_rows,
                                "dropped_missing_keys": stats.dropped_missing_keys,
                                "dropped_invalid_timestamp": stats.dropped_invalid_timestamp,
                                "dropped_invalid_speed": stats.dropped_invalid_speed,
                                "dropped_duplicates": stats.dropped_duplicates,
                            },
                            error_code=None,
                            error_kind=None,
                            consecutive_failures=0,
                            backoff_seconds=0,
                            last_success_utc=datetime.now(timezone.utc).isoformat(),
                            rate_limit=client.rate_limit_summary(),
                        )
                        safe_append_ledger_entry(
                            ledger_path,
                            {
                                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                                "source": "vd",
                                "runner": "live_loop",
                                "event": "snapshot_appended",
                                "ok": True,
                                "cities": args.cities or [],
                                "snapshot_timestamp": snapshot_ts,
                                "input_rows": stats.input_rows,
                                "output_rows": stats.output_rows,
                                "dropped_missing_keys": stats.dropped_missing_keys,
                                "dropped_invalid_timestamp": stats.dropped_invalid_timestamp,
                                "dropped_invalid_speed": stats.dropped_invalid_speed,
                                "dropped_duplicates": stats.dropped_duplicates,
                                "updated_files": [str(observations_out), str(state_path)],
                            },
                        )
                        print(f"[vd-live] appended {len(cleaned):,} rows at {snapshot_ts} -> {observations_out}")
                        consecutive_failures = 0
                        backoff_seconds = 0
                        last_success_utc = datetime.now(timezone.utc).isoformat()
            except Exception as exc:
                info = classify_ingest_error(exc)
                consecutive_failures += 1
                # Exponential backoff (cap at 10 minutes). We still wake up and try again.
                backoff_seconds = int(min(600, max(max(30, backoff_seconds * 2), int(args.interval_seconds))))
                _write_ingest_status(
                    ingest_status_path,
                    ok=False,
                    updated_files=[],
                    error=info.message,
                    quality={},
                    error_code=info.code,
                    error_kind=info.kind,
                    consecutive_failures=consecutive_failures,
                    backoff_seconds=backoff_seconds,
                    last_success_utc=last_success_utc,
                    rate_limit=client.rate_limit_summary(),
                )
                safe_append_ledger_entry(
                    ledger_path,
                    {
                        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                        "source": "vd",
                        "runner": "live_loop",
                        "event": "error",
                        "ok": False,
                        "cities": args.cities or [],
                        "error": info.message,
                        "error_code": info.code,
                        "error_kind": info.kind,
                        "consecutive_failures": consecutive_failures,
                        "backoff_seconds": backoff_seconds,
                        "updated_files": [],
                    },
                )
                print(f"[vd-live] error ({info.code}): {info.message}; sleeping {backoff_seconds}s")
                time.sleep(max(float(args.interval_seconds), float(backoff_seconds)))
                continue

            time.sleep(args.interval_seconds)
    finally:
        client.close()


if __name__ == "__main__":
    main()
