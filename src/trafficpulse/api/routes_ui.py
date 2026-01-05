"""UI settings endpoints used by the static dashboard.

This module exists to keep a single source of truth for "default" dashboard parameters:
- The backend reads defaults from the server-side config (configs/config.yaml).
- The frontend calls `GET /ui/settings` to populate placeholders and reset-to-default behavior.

Why this matters:
- It prevents UI/backend drift (no duplicated magic numbers in JavaScript).
- It keeps analytics parameters explainable and configurable (no hard-coded weights/time windows).
- It avoids leaking secrets: this endpoint intentionally returns only non-sensitive settings.
"""

from __future__ import annotations

# We use Literal to constrain certain UI-controlled strings to a small, known set of values,
# which behaves like an enum and prevents accidental typos from silently changing behavior.
from typing import Literal

# FastAPI's APIRouter lets us group related endpoints into a module and mount them in the main app.
from fastapi import APIRouter
# Pydantic models define the JSON schema sent to the browser, which makes the API contract explicit.
from pydantic import BaseModel, Field

# The config is the single source of truth for all tunable parameters (thresholds, weights, windows).
from trafficpulse.settings import get_config


# This router is imported and included by `trafficpulse.api.app:create_app()`.
router = APIRouter()


class ReliabilitySettings(BaseModel):
    """Dashboard defaults for reliability scoring (explainable, config-driven metrics)."""

    # Speed threshold (kph) under which we consider a timestamp "congested" for frequency metrics.
    congestion_speed_threshold_kph: float
    # Minimum number of time points required to rank a segment (avoids tiny-sample noise).
    min_samples: int
    # Per-metric weights used to combine metrics into a single ranking score.
    weights: dict[str, float]


class AnomaliesSettings(BaseModel):
    """Dashboard defaults for explainable anomaly detection (rolling z-score baseline)."""

    # Method name is returned for transparency and future extensibility (e.g., other baseline methods).
    method: str
    # Rolling window size in "points" (not minutes): points = timestamps at the selected granularity.
    window_points: int
    # Z-score threshold: larger values mean fewer, more extreme anomalies are flagged.
    z_threshold: float
    # Direction controls whether we flag low-speed drops, high-speed spikes, or both.
    direction: Literal["low", "high", "both"]
    # Max gap between anomaly points (minutes) before we split them into separate "events".
    max_gap_minutes: int
    # Minimum number of points required to consider an anomaly cluster an "event".
    min_event_points: int


class EventImpactSettings(BaseModel):
    """Dashboard defaults for baseline event impact analysis (no ML; explainable windows)."""

    # Default time window (hours) used when the UI does not provide a time range explicitly.
    default_window_hours: int
    # Radius around an event point used to find nearby segments (meters).
    radius_meters: float
    # Safety cap to avoid returning too many impacted segments (UI and API performance guard).
    max_segments: int
    # Baseline window (minutes) used to estimate "normal" speed before the event.
    baseline_window_minutes: int
    # If an event has no end time, we treat it as ending after this many minutes (fallback).
    end_time_fallback_minutes: int
    # How far after the event we look for recovery (minutes).
    recovery_horizon_minutes: int
    # Recovery ratio (0..1): e.g., 0.9 means "recovered when speed is back to 90% of baseline".
    recovery_ratio: float
    # How to weight segment speeds when aggregating impacted segments (volume-weighted or equal).
    speed_weighting: Literal["volume", "equal"]
    # Minimum baseline points required to compute a stable baseline estimate.
    min_baseline_points: int
    # Minimum points inside the event window required to compute impact metrics.
    min_event_points: int


class UiSettings(BaseModel):
    """Top-level response schema consumed by the frontend Controls panel."""

    # Preprocessing settings affect the available time-series granularities (e.g., 5m -> 15m).
    preprocessing: dict[str, int]
    # Analytics defaults are grouped by feature (reliability/anomalies/event impact).
    analytics: dict[str, object] = Field(default_factory=dict)
    # Warehouse toggles help the UI explain whether DuckDB+Parquet is enabled.
    warehouse: dict[str, object] = Field(default_factory=dict)
    # Enum-like values are returned so the UI can populate dropdowns consistently.
    enums: dict[str, list[str]] = Field(default_factory=dict)


@router.get("/ui/settings", response_model=UiSettings)
def ui_settings() -> UiSettings:
    """Return the server-side default settings for the dashboard."""

    # Load the current config (configs/config.yaml, or fallback to configs/config.example.yaml).
    config = get_config()

    # Build a strongly-typed reliability settings object so the JSON shape is stable and explicit.
    reliability = ReliabilitySettings(
        # We coerce to primitives to ensure JSON serialization stays predictable across sources (YAML/env).
        congestion_speed_threshold_kph=float(config.analytics.reliability.congestion_speed_threshold_kph),
        min_samples=int(config.analytics.reliability.min_samples),
        weights={
            # Explicit keys allow the frontend to map weights to UI fields without guessing.
            "mean_speed": float(config.analytics.reliability.weights.mean_speed),
            "speed_std": float(config.analytics.reliability.weights.speed_std),
            "congestion_frequency": float(config.analytics.reliability.weights.congestion_frequency),
        },
    )

    # Build anomaly defaults; these control how the rolling z-score detector behaves.
    anomalies = AnomaliesSettings(
        method=str(config.analytics.anomalies.method),
        window_points=int(config.analytics.anomalies.window_points),
        z_threshold=float(config.analytics.anomalies.z_threshold),
        # Config stores a string; the API contract restricts it to a known Literal union for safety.
        direction=str(config.analytics.anomalies.direction),  # type: ignore[arg-type]
        max_gap_minutes=int(config.analytics.anomalies.max_gap_minutes),
        min_event_points=int(config.analytics.anomalies.min_event_points),
    )

    # Build event impact defaults; these control baseline windows and recovery definitions.
    impact = EventImpactSettings(
        default_window_hours=int(config.analytics.event_impact.default_window_hours),
        radius_meters=float(config.analytics.event_impact.radius_meters),
        max_segments=int(config.analytics.event_impact.max_segments),
        baseline_window_minutes=int(config.analytics.event_impact.baseline_window_minutes),
        end_time_fallback_minutes=int(config.analytics.event_impact.end_time_fallback_minutes),
        recovery_horizon_minutes=int(config.analytics.event_impact.recovery_horizon_minutes),
        recovery_ratio=float(config.analytics.event_impact.recovery_ratio),
        # Same idea as anomalies.direction: return a constrained enum-like string to the frontend.
        speed_weighting=str(config.analytics.event_impact.speed_weighting),  # type: ignore[arg-type]
        min_baseline_points=int(config.analytics.event_impact.min_baseline_points),
        min_event_points=int(config.analytics.event_impact.min_event_points),
    )

    # Return a single document with the defaults the UI needs to render placeholders and reset actions.
    return UiSettings(
        # Preprocessing values are needed to explain which granularities exist (5m source -> 15m target).
        preprocessing={
            "source_granularity_minutes": int(config.preprocessing.source_granularity_minutes),
            "target_granularity_minutes": int(config.preprocessing.target_granularity_minutes),
        },
        # `model_dump()` converts nested Pydantic models into plain dicts for JSON responses.
        analytics={
            "reliability": reliability.model_dump(),
            "anomalies": anomalies.model_dump(),
            "event_impact": impact.model_dump(),
        },
        # Warehouse flags let the UI display whether results come from CSV fallback or DuckDB+Parquet.
        warehouse={
            "enabled": bool(config.warehouse.enabled),
            "use_duckdb": bool(config.warehouse.use_duckdb),
        },
        # These enums let the frontend populate dropdowns without hard-coding the allowed values.
        enums={
            "anomalies_direction": ["low", "high", "both"],
            "event_impact_speed_weighting": ["volume", "equal"],
        },
    )
