from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from trafficpulse.ingestion.ledger import safe_append_ledger_entry
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import load_csv, save_csv


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrich segments with optional road network attributes (CSV join).")
    p.add_argument("--processed-dir", default=None, help="Override processed dir (default: config.paths.processed_dir).")
    p.add_argument("--state-dir", default=None, help="Override state dir (default: config.paths.cache_dir).")
    p.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite segments.csv (default writes segments_enriched.csv).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()

    config = get_config()
    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    state_dir = Path(args.state_dir) if args.state_dir else config.paths.cache_dir
    ledger_path = state_dir / "ingest_ledger.jsonl"

    if not config.sources.road_network.enabled:
        print("[road-network] source disabled (config.sources.road_network.enabled=false)")
        return

    segments_path = processed_dir / "segments.csv"
    if not segments_path.exists():
        raise SystemExit(f"segments not found: {segments_path}")

    enrich_path = config.sources.road_network.csv_path
    if not enrich_path.exists():
        raise SystemExit(f"segments enrichment CSV not found: {enrich_path}")

    segments = load_csv(segments_path)
    enrich = pd.read_csv(enrich_path)
    if segments.empty or "segment_id" not in segments.columns:
        raise SystemExit("segments.csv is empty or missing segment_id.")
    if enrich.empty or "segment_id" not in enrich.columns:
        raise SystemExit("segments_enrichment.csv is empty or missing segment_id.")

    segments["segment_id"] = segments["segment_id"].astype(str)
    enrich["segment_id"] = enrich["segment_id"].astype(str)

    # Avoid duplicate columns collisions by suffixing enrichment fields.
    overlap = sorted(set(segments.columns) & set(enrich.columns) - {"segment_id"})
    if overlap:
        enrich = enrich.rename(columns={c: f"{c}_enrich" for c in overlap})

    merged = segments.merge(enrich, on="segment_id", how="left")
    out_path = segments_path if args.in_place else (processed_dir / "segments_enriched.csv")
    save_csv(merged, out_path)

    safe_append_ledger_entry(
        ledger_path,
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "road_network",
            "runner": "enrich_segments",
            "event": "segments_enriched",
            "ok": True,
            "input_segments": str(segments_path),
            "input_enrichment": str(enrich_path),
            "output_path": str(out_path),
            "segments_rows": int(len(segments)),
            "enrichment_rows": int(len(enrich)),
            "updated_files": [str(out_path)],
        },
    )
    print(f"[road-network] wrote {out_path} rows={len(merged):,}")


if __name__ == "__main__":
    main()

