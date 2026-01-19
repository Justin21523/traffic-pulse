from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
from pathlib import Path

from trafficpulse.analytics.weather_features import WeatherJoinSpec, join_weather_to_observations
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    load_csv,
    observations_csv_path,
    save_csv,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Join weather observations onto processed traffic observations.")
    p.add_argument("--processed-dir", default=None, help="Override processed dir (default: config.paths.processed_dir).")
    p.add_argument("--minutes", type=int, default=None, help="Observations granularity minutes (default: config target).")
    p.add_argument("--tolerance-minutes", type=int, default=60, help="Nearest-weather tolerance (default: 60).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()

    config = get_config()
    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    minutes = int(args.minutes or config.preprocessing.target_granularity_minutes)

    obs_path = observations_csv_path(processed_dir, minutes)
    weather_path = processed_dir / "weather_observations.csv"
    out_path = processed_dir / f"observations_{minutes}min_with_weather.csv"

    if not obs_path.exists():
        raise SystemExit(f"observations not found: {obs_path}")
    if not weather_path.exists():
        raise SystemExit(f"weather observations not found: {weather_path}. Run scripts/ingest_weather.py first.")

    obs = load_csv(obs_path)
    weather = load_csv(weather_path)

    # Require a city column in observations for now; if missing, this join is a no-op.
    joined = join_weather_to_observations(
        obs,
        weather,
        spec=WeatherJoinSpec(tolerance_minutes=int(args.tolerance_minutes)),
    )
    save_csv(joined, out_path)
    print(f"[weather-features] wrote {out_path} rows={len(joined):,}")


if __name__ == "__main__":
    main()

