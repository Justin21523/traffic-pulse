from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class WeatherJoinSpec:
    timestamp_column: str = "timestamp"
    city_column: str = "city"
    weather_timestamp_column: str = "timestamp"
    weather_city_column: str = "city"
    # Nearest join tolerance: we pick the closest weather observation within this many minutes.
    tolerance_minutes: int = 60


def join_weather_to_observations(
    observations: pd.DataFrame,
    weather: pd.DataFrame,
    *,
    spec: WeatherJoinSpec = WeatherJoinSpec(),
) -> pd.DataFrame:
    """As-of join weather data onto observations by city and timestamp.

    This returns an augmented observations DataFrame with weather columns:
    - rain_mm, wind_mps, visibility_km, temperature_c, humidity_pct
    """

    if observations.empty or weather.empty:
        return observations.copy()

    obs = observations.copy()
    met = weather.copy()

    ts = spec.timestamp_column
    city = spec.city_column
    wts = spec.weather_timestamp_column
    wcity = spec.weather_city_column

    if ts not in obs.columns:
        return observations.copy()
    if city not in obs.columns:
        # If city is missing from observations, we cannot safely join. Keep as-is.
        return observations.copy()

    if wts not in met.columns or wcity not in met.columns:
        return observations.copy()

    obs[ts] = pd.to_datetime(obs[ts], errors="coerce", utc=True)
    met[wts] = pd.to_datetime(met[wts], errors="coerce", utc=True)
    obs = obs.dropna(subset=[ts, city])
    met = met.dropna(subset=[wts, wcity])
    if obs.empty or met.empty:
        return observations.copy()

    obs[city] = obs[city].astype(str)
    met[wcity] = met[wcity].astype(str)

    # Ensure weather numeric columns exist.
    for col in ["rain_mm", "wind_mps", "visibility_km", "temperature_c", "humidity_pct"]:
        if col not in met.columns:
            met[col] = pd.NA
        met[col] = pd.to_numeric(met[col], errors="coerce")

    obs = obs.sort_values([city, ts]).reset_index(drop=True)
    met = met.sort_values([wcity, wts]).reset_index(drop=True)

    # pandas merge_asof requires sorted keys and supports tolerance.
    tolerance = pd.Timedelta(minutes=int(spec.tolerance_minutes))
    joined = pd.merge_asof(
        obs,
        met.rename(columns={wts: ts, wcity: city}),
        on=ts,
        by=city,
        direction="nearest",
        tolerance=tolerance,
        suffixes=("", "_weather"),
    )
    return joined

