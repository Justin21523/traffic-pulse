from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from trafficpulse.ingestion.ledger import safe_append_ledger_entry
from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import events_csv_path, events_parquet_path, load_csv, save_csv, save_parquet


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge all available event sources into the canonical events.csv.")
    p.add_argument("--processed-dir", default=None, help="Override processed dir (default: config.paths.processed_dir).")
    p.add_argument("--parquet-dir", default=None, help="Override parquet dir (default: config.warehouse.parquet_dir).")
    p.add_argument("--state-dir", default=None, help="Override state dir (default: config.paths.cache_dir).")
    p.add_argument(
        "--fetch-tdx-window-hours",
        type=int,
        default=24,
        help="If no event sources exist, try fetching TDX events for the last N hours (default: 24).",
    )
    return p.parse_args()


def _load_optional(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return load_csv(path)
    except Exception:
        return pd.DataFrame()


def main() -> None:
    args = parse_args()
    configure_logging()

    config = get_config()
    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    parquet_dir = (
        Path(args.parquet_dir)
        if args.parquet_dir
        else (processed_dir / "parquet" if args.processed_dir else config.warehouse.parquet_dir)
    )
    state_dir = Path(args.state_dir) if args.state_dir else config.paths.cache_dir
    ledger_path = state_dir / "ingest_ledger.jsonl"

    sources: list[tuple[str, Path]] = [
        ("tdx", processed_dir / "events.csv"),
        ("roadworks", processed_dir / "events_roadworks.csv"),
        ("incidents", processed_dir / "events_incidents_extra.csv"),
        ("calendar", processed_dir / "events_calendar.csv"),
    ]

    frames: list[pd.DataFrame] = []
    counts: dict[str, int] = {}
    for name, path in sources:
        df = _load_optional(path)
        counts[name] = int(len(df))
        if df.empty:
            continue
        df = df.copy()
        df["source"] = df.get("source", name)
        frames.append(df)

    if not frames:
        # Best-effort: fetch the default TDX events window so downstream linking/impact can run.
        window_hours = int(args.fetch_tdx_window_hours)
        if window_hours > 0:
            end = datetime.now(timezone.utc)
            start = end - timedelta(hours=window_hours)
            client = TdxTrafficClient(config=config)
            try:
                tdx_events = client.download_events(start=start, end=end, cities=None)
            except Exception as exc:
                print(f"[events-all] no event sources found and TDX fetch failed: {exc}")
                return
            finally:
                client.close()
            if not tdx_events.empty:
                tdx_events = tdx_events.copy()
                if "source" not in tdx_events.columns:
                    tdx_events["source"] = "tdx"
                out_csv = events_csv_path(processed_dir)
                save_csv(tdx_events, out_csv)
                frames.append(tdx_events)
                counts["tdx_fetch"] = int(len(tdx_events))
                print(f"[events-all] fetched TDX events rows={len(tdx_events):,}")
            else:
                print("[events-all] no event sources found; TDX fetch returned empty; skipping")
                return
        else:
            print("[events-all] no event sources found; skipping")
            return

    merged = pd.concat(frames, ignore_index=True, sort=False)
    # Canonicalize types.
    if "start_time" in merged.columns:
        merged["start_time"] = pd.to_datetime(merged["start_time"], errors="coerce", utc=True)
    if "end_time" in merged.columns:
        merged["end_time"] = pd.to_datetime(merged["end_time"], errors="coerce", utc=True)
    merged = merged.dropna(subset=["event_id", "start_time"])
    merged["event_id"] = merged["event_id"].astype(str)
    merged = merged.sort_values(["start_time", "event_id"]).drop_duplicates(subset=["event_id"], keep="last")

    out_csv = events_csv_path(processed_dir)
    save_csv(merged, out_csv)
    updated = [str(out_csv)]
    if config.warehouse.enabled:
        out_parquet = events_parquet_path(parquet_dir)
        save_parquet(merged, out_parquet)
        updated.append(str(out_parquet))

    safe_append_ledger_entry(
        ledger_path,
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "events",
            "runner": "build_events_all",
            "event": "events_merged",
            "ok": True,
            "input_counts": counts,
            "output_rows": int(len(merged)),
            "updated_files": updated,
        },
    )
    print(f"[events-all] wrote {out_csv} rows={len(merged):,}")


if __name__ == "__main__":
    main()
