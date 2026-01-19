from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
from pathlib import Path

from trafficpulse.analytics.alerts import AlertSpec, detect_congestion_alerts
from trafficpulse.analytics.event_linking import EventLinkSpec, link_events_to_hotspots
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import load_csv, save_csv


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build event-hotspot links and simple congestion alerts.")
    p.add_argument("--processed-dir", default=None, help="Override processed dir (default: config.paths.processed_dir).")
    p.add_argument("--cache-dir", default=None, help="Override cache dir (default: config.paths.cache_dir).")
    p.add_argument("--minutes", type=int, default=None, help="Minutes for hotspots materialization (default config target).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()

    config = get_config()
    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    cache_dir = Path(args.cache_dir) if args.cache_dir else config.paths.cache_dir
    minutes = int(args.minutes or config.preprocessing.target_granularity_minutes)

    # Inputs: use materialized snapshot if present to avoid expensive recompute.
    window_hours = int(config.analytics.reliability.default_window_hours)
    hotspots_path = cache_dir / f"materialized_map_snapshot_{minutes}m_{window_hours}h.csv"
    events_path = processed_dir / "events.csv"
    baselines_path = cache_dir / f"baselines_speed_{minutes}m_7d.csv"
    observations_path = processed_dir / f"observations_{minutes}min.csv"

    if hotspots_path.exists() and events_path.exists():
        links = link_events_to_hotspots(
            events=load_csv(events_path),
            hotspots=load_csv(hotspots_path),
            spec=EventLinkSpec(),
        )
        links_out = cache_dir / "event_hotspot_links.csv"
        save_csv(links, links_out)
        print(f"[event-links] wrote {links_out} rows={len(links):,}")
    else:
        print("[event-links] skipped (missing events.csv or materialized hotspots snapshot)")

    if baselines_path.exists() and observations_path.exists():
        alerts = detect_congestion_alerts(
            load_csv(observations_path),
            load_csv(baselines_path),
            spec=AlertSpec(),
        )
        alerts_out = cache_dir / "congestion_alerts.csv"
        save_csv(alerts, alerts_out)
        print(f"[alerts] wrote {alerts_out} rows={len(alerts):,}")
    else:
        print("[alerts] skipped (missing baselines or observations)")


if __name__ == "__main__":
    main()

