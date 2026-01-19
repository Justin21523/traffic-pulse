from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
from datetime import datetime, timezone
from pathlib import Path

from trafficpulse.ingestion.ledger import safe_append_ledger_entry
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.sources.csv_sources import normalize_events_csv, read_csv
from trafficpulse.storage.datasets import save_csv


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest roadworks/closures events from a configured CSV source.")
    p.add_argument("--processed-dir", default=None, help="Override processed dir (default: config.paths.processed_dir).")
    p.add_argument("--state-dir", default=None, help="Override state dir (default: config.paths.cache_dir).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()

    config = get_config()
    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    state_dir = Path(args.state_dir) if args.state_dir else config.paths.cache_dir
    ledger_path = state_dir / "ingest_ledger.jsonl"

    if not config.sources.roadworks.enabled:
        print("[roadworks] source disabled (config.sources.roadworks.enabled=false)")
        return

    input_path = config.sources.roadworks.csv_path
    df = read_csv(input_path)
    normalized = normalize_events_csv(
        df,
        source_name=config.sources.roadworks.source_name,
        default_event_type="roadworks",
    )
    out_path = processed_dir / "events_roadworks.csv"
    save_csv(normalized, out_path)

    safe_append_ledger_entry(
        ledger_path,
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "roadworks",
            "runner": "ingest_roadworks",
            "event": "events_written",
            "ok": True,
            "input_path": str(input_path),
            "output_path": str(out_path),
            "input_rows": int(len(df)),
            "output_rows": int(len(normalized)),
            "updated_files": [str(out_path)],
        },
    )
    print(f"[roadworks] wrote {out_path} rows={len(normalized):,}")


if __name__ == "__main__":
    main()

