from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Optional

import pandas as pd

from trafficpulse.settings import AppConfig, get_config
from trafficpulse.utils.time import to_utc


@dataclass(frozen=True)
class ReliabilitySpec:
    congestion_speed_threshold_kph: float
    min_samples: int
    weight_mean_speed: float
    weight_speed_std: float
    weight_congestion_frequency: float
    timestamp_column: str = "timestamp"
    segment_id_column: str = "segment_id"
    speed_column: str = "speed_kph"

    def normalized_weights(self) -> "ReliabilitySpec":
        total = self.weight_mean_speed + self.weight_speed_std + self.weight_congestion_frequency
        if total <= 0:
            return ReliabilitySpec(
                congestion_speed_threshold_kph=self.congestion_speed_threshold_kph,
                min_samples=self.min_samples,
                weight_mean_speed=1 / 3,
                weight_speed_std=1 / 3,
                weight_congestion_frequency=1 / 3,
                timestamp_column=self.timestamp_column,
                segment_id_column=self.segment_id_column,
                speed_column=self.speed_column,
            )
        return ReliabilitySpec(
            congestion_speed_threshold_kph=self.congestion_speed_threshold_kph,
            min_samples=self.min_samples,
            weight_mean_speed=self.weight_mean_speed / total,
            weight_speed_std=self.weight_speed_std / total,
            weight_congestion_frequency=self.weight_congestion_frequency / total,
            timestamp_column=self.timestamp_column,
            segment_id_column=self.segment_id_column,
            speed_column=self.speed_column,
        )


def reliability_spec_from_config(config: Optional[AppConfig] = None) -> ReliabilitySpec:
    resolved = config or get_config()
    section = resolved.analytics.reliability
    return ReliabilitySpec(
        congestion_speed_threshold_kph=float(section.congestion_speed_threshold_kph),
        min_samples=int(section.min_samples),
        weight_mean_speed=float(section.weights.mean_speed),
        weight_speed_std=float(section.weights.speed_std),
        weight_congestion_frequency=float(section.weights.congestion_frequency),
    ).normalized_weights()


def compute_reliability_metrics(
    observations: pd.DataFrame,
    spec: ReliabilitySpec,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame(
            columns=[
                spec.segment_id_column,
                "n_samples",
                "mean_speed_kph",
                "speed_std_kph",
                "congestion_frequency",
            ]
        )

    df = observations.copy()
    ts_col = spec.timestamp_column
    seg_col = spec.segment_id_column
    speed_col = spec.speed_column

    if ts_col not in df.columns or seg_col not in df.columns:
        raise ValueError(f"Missing required columns: {ts_col}, {seg_col}")

    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df[seg_col] = df[seg_col].astype(str)
    df = df.dropna(subset=[ts_col, seg_col])

    if start is not None:
        start_utc = to_utc(start)
        df = df[df[ts_col] >= pd.Timestamp(start_utc)]
    if end is not None:
        end_utc = to_utc(end)
        df = df[df[ts_col] < pd.Timestamp(end_utc)]

    if df.empty:
        return pd.DataFrame(
            columns=[
                seg_col,
                "n_samples",
                "mean_speed_kph",
                "speed_std_kph",
                "congestion_frequency",
            ]
        )

    df[speed_col] = pd.to_numeric(df.get(speed_col), errors="coerce")
    df = df.dropna(subset=[speed_col])
    if df.empty:
        return pd.DataFrame(
            columns=[
                seg_col,
                "n_samples",
                "mean_speed_kph",
                "speed_std_kph",
                "congestion_frequency",
            ]
        )

    df["_is_congested"] = df[speed_col] < float(spec.congestion_speed_threshold_kph)

    grouped = df.groupby(seg_col, as_index=False)
    base = grouped[speed_col].agg(n_samples="count", mean_speed_kph="mean", speed_std_kph="std")
    congestion = grouped["_is_congested"].mean().rename(columns={"_is_congested": "congestion_frequency"})
    metrics = base.merge(congestion, on=seg_col, how="left")

    metrics["speed_std_kph"] = metrics["speed_std_kph"].fillna(0.0)
    metrics["congestion_frequency"] = metrics["congestion_frequency"].fillna(0.0)
    metrics = metrics.sort_values(seg_col).reset_index(drop=True)
    return metrics


def add_reliability_score(metrics: pd.DataFrame, spec: ReliabilitySpec) -> pd.DataFrame:
    if metrics.empty:
        return metrics.copy()

    seg_col = spec.segment_id_column
    if seg_col not in metrics.columns:
        raise ValueError(f"Missing required column: {seg_col}")

    df = metrics.copy()
    df["n_samples"] = pd.to_numeric(df.get("n_samples"), errors="coerce")
    df["mean_speed_kph"] = pd.to_numeric(df.get("mean_speed_kph"), errors="coerce")
    df["speed_std_kph"] = pd.to_numeric(df.get("speed_std_kph"), errors="coerce")
    df["congestion_frequency"] = pd.to_numeric(df.get("congestion_frequency"), errors="coerce")

    eligible = df[df["n_samples"] >= int(spec.min_samples)].copy()
    if eligible.empty:
        df["reliability_score"] = pd.NA
        df["penalty_mean_speed"] = pd.NA
        df["penalty_speed_std"] = pd.NA
        df["penalty_congestion_frequency"] = pd.NA
        return df

    eligible["penalty_mean_speed"] = 1.0 - eligible["mean_speed_kph"].rank(
        pct=True, ascending=True, method="average"
    )
    eligible["penalty_speed_std"] = eligible["speed_std_kph"].rank(
        pct=True, ascending=True, method="average"
    )
    eligible["penalty_congestion_frequency"] = eligible["congestion_frequency"].rank(
        pct=True, ascending=True, method="average"
    )

    spec = spec.normalized_weights()
    eligible["reliability_score"] = (
        spec.weight_mean_speed * eligible["penalty_mean_speed"]
        + spec.weight_speed_std * eligible["penalty_speed_std"]
        + spec.weight_congestion_frequency * eligible["penalty_congestion_frequency"]
    )

    score_cols = [
        seg_col,
        "penalty_mean_speed",
        "penalty_speed_std",
        "penalty_congestion_frequency",
        "reliability_score",
    ]
    df = df.merge(eligible[score_cols], on=seg_col, how="left")
    return df


def compute_reliability_rankings(
    observations: pd.DataFrame,
    spec: ReliabilitySpec,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    metrics = compute_reliability_metrics(observations, spec, start=start, end=end)
    scored = add_reliability_score(metrics, spec)
    ranked = scored.dropna(subset=["reliability_score"]).sort_values(
        "reliability_score", ascending=False
    )
    ranked = ranked.reset_index(drop=True)
    ranked["rank"] = ranked.index + 1
    if limit is not None:
        ranked = ranked.head(int(limit))
    return ranked


def apply_reliability_overrides(
    spec: ReliabilitySpec,
    *,
    congestion_speed_threshold_kph: Optional[float] = None,
    min_samples: Optional[int] = None,
    weight_mean_speed: Optional[float] = None,
    weight_speed_std: Optional[float] = None,
    weight_congestion_frequency: Optional[float] = None,
) -> ReliabilitySpec:
    updated = spec
    if congestion_speed_threshold_kph is not None:
        if float(congestion_speed_threshold_kph) <= 0:
            raise ValueError("congestion_speed_threshold_kph must be > 0")
        updated = replace(updated, congestion_speed_threshold_kph=float(congestion_speed_threshold_kph))
    if min_samples is not None:
        if int(min_samples) <= 0:
            raise ValueError("min_samples must be > 0")
        updated = replace(updated, min_samples=int(min_samples))

    weights = {
        "weight_mean_speed": weight_mean_speed,
        "weight_speed_std": weight_speed_std,
        "weight_congestion_frequency": weight_congestion_frequency,
    }
    if any(value is not None for value in weights.values()):
        mean = float(weight_mean_speed) if weight_mean_speed is not None else updated.weight_mean_speed
        std = float(weight_speed_std) if weight_speed_std is not None else updated.weight_speed_std
        cong = (
            float(weight_congestion_frequency)
            if weight_congestion_frequency is not None
            else updated.weight_congestion_frequency
        )
        if mean < 0 or std < 0 or cong < 0:
            raise ValueError("reliability weights must be >= 0")
        updated = replace(
            updated,
            weight_mean_speed=mean,
            weight_speed_std=std,
            weight_congestion_frequency=cong,
        ).normalized_weights()

    return updated
