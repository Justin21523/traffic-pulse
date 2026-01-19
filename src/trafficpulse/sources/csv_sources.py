from __future__ import annotations

import hashlib
from datetime import timezone
from pathlib import Path

import pandas as pd


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def ensure_utc_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df
    out = df.copy()
    out[column] = pd.to_datetime(out[column], errors="coerce", utc=True)
    return out


def stable_event_id(*parts: str) -> str:
    text = "|".join([p.strip() for p in parts if p is not None])
    digest = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()  # noqa: S324
    return f"ext_{digest[:16]}"


def normalize_weather_csv(df: pd.DataFrame, *, source_name: str) -> pd.DataFrame:
    """Normalize a CSV into canonical weather observations.

    Expected columns (case-insensitive best-effort):
    - timestamp, city
    - rain_mm, wind_mps, visibility_km, temperature_c, humidity_pct (optional)
    """

    if df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    # Common aliases
    aliases = {
        "time": "timestamp",
        "datetime": "timestamp",
        "obs_time": "timestamp",
        "city_name": "city",
        "rain": "rain_mm",
        "rainfall_mm": "rain_mm",
        "wind": "wind_mps",
        "wind_speed": "wind_mps",
        "visibility": "visibility_km",
        "temp_c": "temperature_c",
        "temperature": "temperature_c",
        "humidity": "humidity_pct",
    }
    rename: dict[str, str] = {}
    for c in out.columns:
        key = c.lower()
        if key in aliases:
            rename[c] = aliases[key]
    if rename:
        out = out.rename(columns=rename)

    if "timestamp" not in out.columns or "city" not in out.columns:
        return pd.DataFrame()

    out = ensure_utc_datetime(out, "timestamp")
    out = out.dropna(subset=["timestamp", "city"])
    out["city"] = out["city"].astype(str)
    for col in ["rain_mm", "wind_mps", "visibility_km", "temperature_c", "humidity_pct"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        else:
            out[col] = pd.NA

    out["source"] = str(source_name)
    keep = ["timestamp", "city", "rain_mm", "wind_mps", "visibility_km", "temperature_c", "humidity_pct", "source"]
    out = out[keep].sort_values(["city", "timestamp"]).reset_index(drop=True)
    return out


def normalize_events_csv(df: pd.DataFrame, *, source_name: str, default_event_type: str) -> pd.DataFrame:
    """Normalize a CSV into canonical events rows used by TrafficPulse.

    Expected columns (case-insensitive best-effort):
    - event_id (optional), start_time (required), end_time (optional)
    - event_type, description, road_name, direction, severity, lat, lon, city
    """

    if df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    aliases = {
        "id": "event_id",
        "eventid": "event_id",
        "start": "start_time",
        "starttime": "start_time",
        "end": "end_time",
        "endtime": "end_time",
        "type": "event_type",
        "desc": "description",
        "location": "road_name",
        "road": "road_name",
        "dir": "direction",
        "lng": "lon",
        "longitude": "lon",
        "lat": "lat",
        "latitude": "lat",
        "city_name": "city",
    }
    rename: dict[str, str] = {}
    for c in out.columns:
        key = c.lower().replace("_", "")
        if key in aliases:
            rename[c] = aliases[key]
    if rename:
        out = out.rename(columns=rename)

    if "start_time" not in out.columns:
        return pd.DataFrame()

    out["start_time"] = pd.to_datetime(out["start_time"], errors="coerce", utc=True)
    if "end_time" in out.columns:
        out["end_time"] = pd.to_datetime(out["end_time"], errors="coerce", utc=True)
    else:
        out["end_time"] = pd.NaT

    out = out.dropna(subset=["start_time"])

    if "event_type" not in out.columns:
        out["event_type"] = default_event_type
    out["event_type"] = out["event_type"].astype(str)

    for col in ["description", "road_name", "direction", "city"]:
        if col in out.columns:
            out[col] = out[col].astype(str)
        else:
            out[col] = pd.NA

    if "severity" in out.columns:
        out["severity"] = pd.to_numeric(out["severity"], errors="coerce")
    else:
        out["severity"] = pd.NA

    for col in ["lat", "lon"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        else:
            out[col] = pd.NA

    if "event_id" in out.columns:
        out["event_id"] = out["event_id"].astype(str)
    else:
        out["event_id"] = pd.NA

    # Fill missing event_ids with a stable hash so we can dedupe/merge.
    missing_id = out["event_id"].isna() | (out["event_id"].astype(str).str.strip() == "")
    if missing_id.any():
        ids: list[str] = []
        for _, row in out.loc[missing_id].iterrows():
            ids.append(
                stable_event_id(
                    str(source_name),
                    str(row.get("start_time")),
                    str(row.get("end_time")),
                    str(row.get("event_type")),
                    str(row.get("description")),
                    str(row.get("road_name")),
                    str(row.get("lat")),
                    str(row.get("lon")),
                    str(row.get("city")),
                )
            )
        out.loc[missing_id, "event_id"] = ids

    out["source"] = str(source_name)
    keep = [
        "event_id",
        "start_time",
        "end_time",
        "event_type",
        "description",
        "road_name",
        "direction",
        "severity",
        "lat",
        "lon",
        "city",
        "source",
    ]
    out = out[[c for c in keep if c in out.columns]]
    out = out.sort_values(["start_time", "event_id"]).reset_index(drop=True)
    return out

