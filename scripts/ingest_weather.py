from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

from trafficpulse.ingestion.ledger import safe_append_ledger_entry
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config
from trafficpulse.sources.csv_sources import normalize_weather_csv, read_csv
from trafficpulse.storage.datasets import save_csv


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest weather observations from CSV or a lightweight API provider.")
    p.add_argument("--processed-dir", default=None, help="Override processed dir (default: config.paths.processed_dir).")
    p.add_argument("--state-dir", default=None, help="Override state dir (default: config.paths.cache_dir).")
    return p.parse_args()

def _fetch_open_meteo_current(*, lat: float, lon: float) -> dict[str, object] | None:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,relative_humidity_2m,visibility,wind_speed_10m,precipitation"
        "&timezone=UTC"
    )
    req = Request(url, headers={"accept": "application/json"})
    with urlopen(req, timeout=10) as resp:  # noqa: S310
        payload = resp.read().decode("utf-8", errors="ignore")
    data = json.loads(payload)
    return data if isinstance(data, dict) else None


def main() -> None:
    args = parse_args()
    configure_logging()

    config = get_config()
    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    state_dir = Path(args.state_dir) if args.state_dir else config.paths.cache_dir
    ledger_path = state_dir / "ingest_ledger.jsonl"

    if not config.sources.weather.enabled:
        print("[weather] source disabled (config.sources.weather.enabled=false)")
        return

    out_path = processed_dir / "weather_observations.csv"
    provider = str(getattr(config.sources.weather, "provider", "csv") or "csv").strip().lower()

    if provider == "open_meteo":
        city = str(getattr(config.sources.weather, "open_meteo_city", "Taipei") or "Taipei")
        lat = float(getattr(config.sources.weather, "open_meteo_lat", 25.033) or 25.033)
        lon = float(getattr(config.sources.weather, "open_meteo_lon", 121.5654) or 121.5654)
        data = _fetch_open_meteo_current(lat=lat, lon=lon)
        current = data.get("current") if isinstance(data, dict) else None
        if not isinstance(current, dict):
            raise SystemExit("[weather] open_meteo response missing current")
        ts = current.get("time")
        row = {
            "timestamp": str(ts) if ts else datetime.now(timezone.utc).isoformat(),
            "city": city,
            "rain_mm": current.get("precipitation"),
            "wind_mps": current.get("wind_speed_10m"),
            "visibility_km": (float(current.get("visibility")) / 1000.0) if current.get("visibility") is not None else None,
            "temperature_c": current.get("temperature_2m"),
            "humidity_pct": current.get("relative_humidity_2m"),
            "source": "open_meteo",
        }
        import pandas as pd

        normalized = pd.DataFrame([row])
        normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], errors="coerce", utc=True)
        normalized = normalized.dropna(subset=["timestamp", "city"])
        normalized["city"] = normalized["city"].astype(str)
        # Append to existing file (dedupe by timestamp+city).
        if out_path.exists():
            try:
                existing = pd.read_csv(out_path)
                if not existing.empty and "timestamp" in existing.columns and "city" in existing.columns:
                    existing["timestamp"] = pd.to_datetime(existing["timestamp"], errors="coerce", utc=True)
                    existing["city"] = existing["city"].astype(str)
                    merged = pd.concat([existing, normalized], ignore_index=True, sort=False)
                    merged = merged.dropna(subset=["timestamp", "city"])
                    merged = merged.drop_duplicates(subset=["timestamp", "city"], keep="last")
                    merged = merged.sort_values(["city", "timestamp"]).reset_index(drop=True)
                    normalized = merged
            except Exception:
                pass
        save_csv(normalized, out_path)

        safe_append_ledger_entry(
            ledger_path,
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "source": "weather",
                "runner": "ingest_weather",
                "event": "weather_written",
                "ok": True,
                "provider": "open_meteo",
                "city": city,
                "lat": lat,
                "lon": lon,
                "output_path": str(out_path),
                "output_rows": int(len(normalized)),
                "updated_files": [str(out_path)],
            },
        )
        print(f"[weather] wrote {out_path} rows={len(normalized):,} (provider=open_meteo)")
        return

    input_path = config.sources.weather.csv_path
    df = read_csv(input_path)
    normalized = normalize_weather_csv(df, source_name=config.sources.weather.source_name)
    save_csv(normalized, out_path)
    safe_append_ledger_entry(
        ledger_path,
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "weather",
            "runner": "ingest_weather",
            "event": "weather_written",
            "ok": True,
            "provider": "csv",
            "input_path": str(input_path),
            "output_path": str(out_path),
            "input_rows": int(len(df)),
            "output_rows": int(len(normalized)),
            "updated_files": [str(out_path)],
        },
    )
    print(f"[weather] wrote {out_path} rows={len(normalized):,} (provider=csv)")


if __name__ == "__main__":
    main()
