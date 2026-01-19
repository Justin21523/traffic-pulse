from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import subprocess
import sys
from pathlib import Path

from trafficpulse.settings import get_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TrafficPulse ingestion runner (daily backfill + live loop).")
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
        "--interval-seconds",
        type=int,
        default=60,
        help="Live loop interval seconds (default: 60).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable disk cache for this run (recommended for live loop).",
    )
    parser.add_argument(
        "--processed-dir",
        default=None,
        help="Override output directory (default: config.paths.processed_dir).",
    )
    parser.add_argument(
        "--state-dir",
        default=None,
        help="Override state directory (default: data/cache).",
    )
    parser.add_argument(
        "--skip-backfill",
        action="store_true",
        help="Skip the daily backfill step.",
    )
    parser.add_argument(
        "--backfill-only",
        action="store_true",
        help="Run daily backfill and exit.",
    )
    parser.add_argument(
        "--live-only",
        action="store_true",
        help="Run live loop only (skip backfill).",
    )
    parser.add_argument(
        "--live-max-iterations",
        type=int,
        default=None,
        help="Stop live loop after N iterations (default: run forever).",
    )
    parser.add_argument(
        "--segments-refresh-hours",
        type=int,
        default=24,
        help="Refresh segments metadata at most once per N hours (default: 24).",
    )
    parser.add_argument(
        "--no-materialize-after",
        action="store_true",
        help="Skip scripts/materialize_defaults.py after the runner completes.",
    )
    parser.add_argument(
        "--no-aggregate-after",
        action="store_true",
        help="Skip scripts/aggregate_observations.py after the runner completes.",
    )
    return parser.parse_args()


def _run(cmd: list[str]) -> None:
    print("[runner] exec:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _run_optional(cmd: list[str]) -> None:
    print("[runner] exec (optional):", " ".join(cmd))
    subprocess.run(cmd, check=False)


def main() -> None:
    args = parse_args()

    state_dir = args.state_dir or "data/cache"
    backfill_state = f"{state_dir.rstrip('/')}/daily_backfill_state.json"
    live_state = f"{state_dir.rstrip('/')}/live_loop_state.json"

    cities = args.cities or []
    cities_args = ["--cities", *cities] if cities else []
    processed_args = ["--processed-dir", args.processed_dir] if args.processed_dir else []
    cache_args = ["--no-cache"] if args.no_cache else []

    do_backfill = not args.skip_backfill and not args.live_only
    do_live = not args.backfill_only
    if args.live_only:
        do_backfill = False
        do_live = True

    if do_backfill:
        _run(
            [
                sys.executable,
                "scripts/ingest_daily_backfill.py",
                *cities_args,
                "--min-request-interval",
                str(args.min_request_interval),
                *cache_args,
                *processed_args,
                "--state-path",
                backfill_state,
            ]
        )

    if do_live:
        _run(
            [
                sys.executable,
                "scripts/ingest_live_loop.py",
                *cities_args,
                "--interval-seconds",
                str(args.interval_seconds),
                "--min-request-interval",
                str(args.min_request_interval),
                "--segments-refresh-hours",
                str(args.segments_refresh_hours),
                *cache_args,
                *processed_args,
                "--state-path",
                live_state,
                *(["--max-iterations", str(args.live_max_iterations)] if args.live_max_iterations else []),
            ]
        )

    if not args.no_aggregate_after:
        config = get_config()
        processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
        source_minutes = int(config.preprocessing.source_granularity_minutes)
        target_minutes = int(config.preprocessing.target_granularity_minutes)
        source_path = processed_dir / f"observations_{source_minutes}min.csv"
        if source_path.exists():
            if target_minutes != source_minutes:
                _run_optional([sys.executable, "scripts/aggregate_observations.py"])
            if 60 not in {source_minutes, target_minutes}:
                _run_optional([sys.executable, "scripts/aggregate_observations.py", "--target-minutes", "60"])

    if not args.no_materialize_after:
        _run([sys.executable, "scripts/materialize_defaults.py"])


if __name__ == "__main__":
    main()
