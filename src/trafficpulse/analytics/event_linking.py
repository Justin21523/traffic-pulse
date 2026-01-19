from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class EventLinkSpec:
    # Time window around events to consider a hotspot relevant.
    pre_minutes: int = 30
    post_minutes: int = 60
    # Maximum distance in degrees (rough, assumes small area) if only lat/lon are available.
    # For MVP we use a simple bbox filter; later we can upgrade to Haversine meters.
    max_abs_lat_delta: float = 0.02
    max_abs_lon_delta: float = 0.02


def link_events_to_hotspots(
    *,
    events: pd.DataFrame,
    hotspots: pd.DataFrame,
    spec: EventLinkSpec = EventLinkSpec(),
) -> pd.DataFrame:
    """Link events to nearby hotspots in time+space (explainable join).

    Required columns:
    - events: event_id, start_time, (optional) end_time, lat, lon
    - hotspots: segment_id, timestamp, lat, lon, mean_speed_kph
    """

    if events.empty or hotspots.empty:
        return pd.DataFrame(columns=["event_id", "segment_id", "score", "reason"])

    ev = events.copy()
    hs = hotspots.copy()

    if "event_id" not in ev.columns or "start_time" not in ev.columns:
        return pd.DataFrame(columns=["event_id", "segment_id", "score", "reason"])
    if "segment_id" not in hs.columns:
        return pd.DataFrame(columns=["event_id", "segment_id", "score", "reason"])

    ev["start_time"] = pd.to_datetime(ev["start_time"], errors="coerce", utc=True)
    if "end_time" in ev.columns:
        ev["end_time"] = pd.to_datetime(ev["end_time"], errors="coerce", utc=True)
    hs["timestamp"] = pd.to_datetime(hs["timestamp"], errors="coerce", utc=True) if "timestamp" in hs.columns else pd.NaT

    ev = ev.dropna(subset=["event_id", "start_time"])
    hs = hs.dropna(subset=["segment_id"])
    if ev.empty or hs.empty:
        return pd.DataFrame(columns=["event_id", "segment_id", "score", "reason"])

    for col in ["lat", "lon"]:
        if col in ev.columns:
            ev[col] = pd.to_numeric(ev[col], errors="coerce")
        if col in hs.columns:
            hs[col] = pd.to_numeric(hs[col], errors="coerce")

    links: list[dict[str, object]] = []
    for _, e in ev.iterrows():
        start = e["start_time"]
        end = e.get("end_time")
        if pd.isna(end):
            end = start
        window_start = pd.Timestamp(start) - pd.Timedelta(minutes=int(spec.pre_minutes))
        window_end = pd.Timestamp(end) + pd.Timedelta(minutes=int(spec.post_minutes))

        subset = hs
        if "timestamp" in hs.columns:
            subset = subset[(subset["timestamp"] >= window_start) & (subset["timestamp"] < window_end)]

        if "lat" in ev.columns and "lon" in ev.columns and "lat" in hs.columns and "lon" in hs.columns:
            if pd.notna(e.get("lat")) and pd.notna(e.get("lon")):
                subset = subset[
                    ((subset["lat"] - float(e["lat"])).abs() <= float(spec.max_abs_lat_delta))
                    & ((subset["lon"] - float(e["lon"])).abs() <= float(spec.max_abs_lon_delta))
                ]

        if subset.empty:
            continue

        # Score: prefer lower mean_speed_kph hotspots (more congested).
        if "mean_speed_kph" in subset.columns:
            subset = subset.copy()
            subset["mean_speed_kph"] = pd.to_numeric(subset["mean_speed_kph"], errors="coerce")
            subset["score"] = -subset["mean_speed_kph"]
            best = subset.sort_values("score", ascending=False).head(20)
            for _, row in best.iterrows():
                links.append(
                    {
                        "event_id": str(e["event_id"]),
                        "segment_id": str(row["segment_id"]),
                        "score": float(row.get("score")) if row.get("score") is not None else None,
                        "reason": "nearby_in_time_space",
                    }
                )
        else:
            best = subset.drop_duplicates(subset=["segment_id"]).head(20)
            for _, row in best.iterrows():
                links.append(
                    {
                        "event_id": str(e["event_id"]),
                        "segment_id": str(row["segment_id"]),
                        "score": None,
                        "reason": "nearby_in_time_space",
                    }
                )

    if not links:
        return pd.DataFrame(columns=["event_id", "segment_id", "score", "reason"])
    out = pd.DataFrame(links).drop_duplicates(subset=["event_id", "segment_id"]).reset_index(drop=True)
    return out
