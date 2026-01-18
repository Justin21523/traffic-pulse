"""Explainable reliability metrics and rankings for traffic speed time series.

This module intentionally avoids complex ML for the MVP and focuses on transparent statistics:
- Mean speed (higher is generally better).
- Speed variability (standard deviation; lower is generally better).
- Congestion frequency (share of time below a speed threshold; lower is better).

The output is used by:
- Offline scripts (e.g., `scripts/build_reliability_rankings.py`) to export CSV reports.
- FastAPI endpoints (e.g., `/rankings/reliability`) to power the dashboard.

All tunable parameters are config-driven (thresholds, minimum samples, weights) to keep results
reproducible and to allow UI overrides without changing code.
"""

from __future__ import annotations

# dataclass provides lightweight, immutable specs used to parameterize computations.
from dataclasses import dataclass, replace
# datetime is used for optional start/end filtering of time-series windows.
from datetime import datetime
# Optional expresses nullable parameters and fields in a type-safe way.
from typing import Optional

# pandas provides groupby statistics, percent ranks, and datetime filtering.
import pandas as pd

# AppConfig is used to build a spec from YAML config; get_config loads the singleton config.
from trafficpulse.settings import AppConfig, get_config
# to_utc normalizes boundary datetimes so filtering is timezone-consistent.
from trafficpulse.utils.time import to_utc


@dataclass(frozen=True)
class ReliabilitySpec:
    """Parameters that define how reliability metrics and scores are computed."""

    # Speed threshold (kph) below which a timestamp is considered "congested".
    congestion_speed_threshold_kph: float
    # Minimum number of samples required for a segment to receive a reliability score.
    min_samples: int
    # Weight for the mean-speed penalty in the final score.
    weight_mean_speed: float
    # Weight for the speed-variability penalty in the final score.
    weight_speed_std: float
    # Weight for the congestion-frequency penalty in the final score.
    weight_congestion_frequency: float
    # Column names are configurable so the same logic can be applied to schema variants.
    timestamp_column: str = "timestamp"
    segment_id_column: str = "segment_id"
    speed_column: str = "speed_kph"

    def normalized_weights(self) -> "ReliabilitySpec":
        """Return a new spec with weights normalized to sum to 1.0.

        Why normalize:
        - Users might provide weights that do not sum to 1 (e.g., 40/30/30).
        - Only relative proportions matter for a weighted sum, so normalization preserves intent.
        """

        # Compute the total weight so we can scale each weight into a 0..1 proportion.
        total = self.weight_mean_speed + self.weight_speed_std + self.weight_congestion_frequency
        # If all weights are zero or negative, fall back to equal weights to keep scoring defined.
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
        # Return a new spec with each weight divided by the total so the sum becomes 1.0.
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
    """Build a ReliabilitySpec from the loaded YAML config (with normalized weights)."""

    # Allow dependency injection (tests) while providing a convenient default for app usage.
    resolved = config or get_config()
    # Read the reliability section from config; it is the single source of truth for defaults.
    section = resolved.analytics.reliability
    # Convert config values to primitives to avoid surprises from YAML typing.
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
    """Compute per-segment reliability metrics from time-series observations.

    Metrics:
    - n_samples: count of valid speed observations.
    - mean_speed_kph: mean of speed (kph).
    - speed_std_kph: standard deviation of speed (kph), filled with 0 when undefined (single sample).
    - congestion_frequency: fraction of samples below congestion threshold (0..1).
    """

    # If there is no input data, return an empty DataFrame with the expected columns.
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

    # Work on a copy to keep the function pure (callers may reuse the input frame elsewhere).
    df = observations.copy()
    # Resolve the configured column names so this function can handle schema variants.
    ts_col = spec.timestamp_column
    seg_col = spec.segment_id_column
    speed_col = spec.speed_column

    # Segment id and timestamp are required keys for time filtering and grouping.
    if ts_col not in df.columns or seg_col not in df.columns:
        raise ValueError(f"Missing required columns: {ts_col}, {seg_col}")

    # Parse timestamps to UTC-aware datetimes so comparisons and window filters are consistent.
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    # Normalize ids to strings to avoid numeric-vs-string mismatches across data sources.
    df[seg_col] = df[seg_col].astype(str)
    # Drop rows missing keys; they cannot be grouped reliably.
    df = df.dropna(subset=[ts_col, seg_col])

    # Apply start bound as inclusive (>=) so the window includes the start timestamp.
    if start is not None:
        start_utc = to_utc(start)
        df = df[df[ts_col] >= pd.Timestamp(start_utc)]
    # Apply end bound as exclusive (<) to avoid double-counting across adjacent windows.
    if end is not None:
        end_utc = to_utc(end)
        df = df[df[ts_col] < pd.Timestamp(end_utc)]

    # If filtering removed all rows, return an empty metrics DataFrame with stable columns.
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

    # Coerce speed column to numeric so comparisons and mean/std computations are correct.
    df[speed_col] = pd.to_numeric(df.get(speed_col), errors="coerce")
    # Drop rows without valid speed values; they should not affect metrics.
    df = df.dropna(subset=[speed_col])
    # If speed is entirely missing, return an empty metrics DataFrame rather than crashing.
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

    # Mark each sample as congested when speed is below the configured threshold.
    df["_is_congested"] = df[speed_col] < float(spec.congestion_speed_threshold_kph)

    # Group by segment id to compute per-segment statistics.
    grouped = df.groupby(seg_col, as_index=False)
    metrics = grouped.agg(
        n_samples=(speed_col, "count"),
        mean_speed_kph=(speed_col, "mean"),
        speed_std_kph=(speed_col, "std"),
        congestion_frequency=("_is_congested", "mean"),
    )

    # Pandas std is NaN for a single sample; treat it as 0 variability for MVP readability.
    metrics["speed_std_kph"] = metrics["speed_std_kph"].fillna(0.0)
    # Missing congestion values should be rare, but fill with 0 to keep downstream code simple.
    metrics["congestion_frequency"] = metrics["congestion_frequency"].fillna(0.0)
    # Sort deterministically to keep outputs stable for exports and testing.
    metrics = metrics.sort_values(seg_col).reset_index(drop=True)
    return metrics


def add_reliability_score(metrics: pd.DataFrame, spec: ReliabilitySpec) -> pd.DataFrame:
    """Add percentile-based penalties and a weighted reliability score to a metrics DataFrame.

    We use percentile ranks so different metrics (kph vs ratios) become comparable on a 0..1 scale.
    """

    # If there is no data, return a copy to preserve the "don't mutate inputs" contract.
    if metrics.empty:
        return metrics.copy()

    # Resolve the key column name from the spec for schema flexibility.
    seg_col = spec.segment_id_column
    # We require the key column to merge computed scores back to the original metrics.
    if seg_col not in metrics.columns:
        raise ValueError(f"Missing required column: {seg_col}")

    # Work on a copy so callers can reuse the original metrics without side effects.
    df = metrics.copy()
    # Coerce numeric columns; CSV imports can turn numbers into strings.
    df["n_samples"] = pd.to_numeric(df.get("n_samples"), errors="coerce")
    df["mean_speed_kph"] = pd.to_numeric(df.get("mean_speed_kph"), errors="coerce")
    df["speed_std_kph"] = pd.to_numeric(df.get("speed_std_kph"), errors="coerce")
    df["congestion_frequency"] = pd.to_numeric(df.get("congestion_frequency"), errors="coerce")

    # Only score segments with enough samples so rankings are not dominated by tiny windows.
    eligible = df[df["n_samples"] >= int(spec.min_samples)].copy()
    # If nothing is eligible, return NA scores so the caller can distinguish "not enough data".
    if eligible.empty:
        df["reliability_score"] = pd.NA
        df["penalty_mean_speed"] = pd.NA
        df["penalty_speed_std"] = pd.NA
        df["penalty_congestion_frequency"] = pd.NA
        return df

    # Mean speed: lower mean is worse, so we rank ascending and invert (1 - pct_rank).
    eligible["penalty_mean_speed"] = 1.0 - eligible["mean_speed_kph"].rank(
        pct=True, ascending=True, method="average"
    )
    # Speed std: higher variability is worse, so we rank ascending directly as penalty.
    eligible["penalty_speed_std"] = eligible["speed_std_kph"].rank(
        pct=True, ascending=True, method="average"
    )
    # Congestion frequency: higher share of congested samples is worse, so rank ascending as penalty.
    eligible["penalty_congestion_frequency"] = eligible["congestion_frequency"].rank(
        pct=True, ascending=True, method="average"
    )

    # Normalize weights (again) so overrides or custom specs always produce a proper convex combination.
    spec = spec.normalized_weights()
    # Weighted sum of penalties yields a final score where higher means "more unreliable" for ranking.
    eligible["reliability_score"] = (
        spec.weight_mean_speed * eligible["penalty_mean_speed"]
        + spec.weight_speed_std * eligible["penalty_speed_std"]
        + spec.weight_congestion_frequency * eligible["penalty_congestion_frequency"]
    )

    # Select only the computed columns to keep the merge small and explicit.
    score_cols = [
        seg_col,
        "penalty_mean_speed",
        "penalty_speed_std",
        "penalty_congestion_frequency",
        "reliability_score",
    ]
    # Merge computed scores back into the full metrics table (non-eligible rows get NA).
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
    """Compute reliability metrics + scores and return a ranked DataFrame."""

    # Compute base metrics for the requested time window (or all data when no window is provided).
    metrics = compute_reliability_metrics(observations, spec, start=start, end=end)
    # Add penalties and the combined score using the config-driven weights.
    scored = add_reliability_score(metrics, spec)
    # Rank segments by score descending so rank=1 is the "most unreliable" (highest penalty score).
    ranked = scored.dropna(subset=["reliability_score"]).sort_values(
        "reliability_score", ascending=False
    )
    # Reset index to produce a clean 0..n-1 range used to assign ranks deterministically.
    ranked = ranked.reset_index(drop=True)
    # Ranks are 1-based for human readability (dashboards/reports).
    ranked["rank"] = ranked.index + 1
    # Apply an optional limit to reduce payload size for UI/API usage.
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
    """Return a new spec with optional overrides applied (validated + normalized)."""

    # Start from the existing spec and apply overrides immutably via dataclasses.replace().
    updated = spec
    # Validate threshold because non-positive speeds make the congestion definition meaningless.
    if congestion_speed_threshold_kph is not None:
        if float(congestion_speed_threshold_kph) <= 0:
            raise ValueError("congestion_speed_threshold_kph must be > 0")
        updated = replace(updated, congestion_speed_threshold_kph=float(congestion_speed_threshold_kph))
    # Validate minimum sample size because zero or negative sample counts do not make sense.
    if min_samples is not None:
        if int(min_samples) <= 0:
            raise ValueError("min_samples must be > 0")
        updated = replace(updated, min_samples=int(min_samples))

    # Collect possible weight overrides so we can update them together when any is provided.
    weights = {
        "weight_mean_speed": weight_mean_speed,
        "weight_speed_std": weight_speed_std,
        "weight_congestion_frequency": weight_congestion_frequency,
    }
    # Only update weights when at least one override is present to preserve the original spec otherwise.
    if any(value is not None for value in weights.values()):
        # Use provided overrides when present; otherwise keep the existing values.
        mean = float(weight_mean_speed) if weight_mean_speed is not None else updated.weight_mean_speed
        std = float(weight_speed_std) if weight_speed_std is not None else updated.weight_speed_std
        cong = (
            float(weight_congestion_frequency)
            if weight_congestion_frequency is not None
            else updated.weight_congestion_frequency
        )
        # Negative weights would invert meaning and break interpretation, so we forbid them.
        if mean < 0 or std < 0 or cong < 0:
            raise ValueError("reliability weights must be >= 0")
        # Apply weight overrides and normalize so the sum becomes 1.0.
        updated = replace(
            updated,
            weight_mean_speed=mean,
            weight_speed_std=std,
            weight_congestion_frequency=cong,
        ).normalized_weights()

    return updated
