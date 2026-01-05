from __future__ import annotations

from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel, Field

from trafficpulse.settings import get_config


router = APIRouter()


class ReliabilitySettings(BaseModel):
    congestion_speed_threshold_kph: float
    min_samples: int
    weights: dict[str, float]


class AnomaliesSettings(BaseModel):
    method: str
    window_points: int
    z_threshold: float
    direction: Literal["low", "high", "both"]
    max_gap_minutes: int
    min_event_points: int


class EventImpactSettings(BaseModel):
    default_window_hours: int
    radius_meters: float
    max_segments: int
    baseline_window_minutes: int
    end_time_fallback_minutes: int
    recovery_horizon_minutes: int
    recovery_ratio: float
    speed_weighting: Literal["volume", "equal"]
    min_baseline_points: int
    min_event_points: int


class UiSettings(BaseModel):
    preprocessing: dict[str, int]
    analytics: dict[str, object] = Field(default_factory=dict)
    warehouse: dict[str, object] = Field(default_factory=dict)
    enums: dict[str, list[str]] = Field(default_factory=dict)


@router.get("/ui/settings", response_model=UiSettings)
def ui_settings() -> UiSettings:
    config = get_config()

    reliability = ReliabilitySettings(
        congestion_speed_threshold_kph=float(config.analytics.reliability.congestion_speed_threshold_kph),
        min_samples=int(config.analytics.reliability.min_samples),
        weights={
            "mean_speed": float(config.analytics.reliability.weights.mean_speed),
            "speed_std": float(config.analytics.reliability.weights.speed_std),
            "congestion_frequency": float(config.analytics.reliability.weights.congestion_frequency),
        },
    )

    anomalies = AnomaliesSettings(
        method=str(config.analytics.anomalies.method),
        window_points=int(config.analytics.anomalies.window_points),
        z_threshold=float(config.analytics.anomalies.z_threshold),
        direction=str(config.analytics.anomalies.direction),  # type: ignore[arg-type]
        max_gap_minutes=int(config.analytics.anomalies.max_gap_minutes),
        min_event_points=int(config.analytics.anomalies.min_event_points),
    )

    impact = EventImpactSettings(
        default_window_hours=int(config.analytics.event_impact.default_window_hours),
        radius_meters=float(config.analytics.event_impact.radius_meters),
        max_segments=int(config.analytics.event_impact.max_segments),
        baseline_window_minutes=int(config.analytics.event_impact.baseline_window_minutes),
        end_time_fallback_minutes=int(config.analytics.event_impact.end_time_fallback_minutes),
        recovery_horizon_minutes=int(config.analytics.event_impact.recovery_horizon_minutes),
        recovery_ratio=float(config.analytics.event_impact.recovery_ratio),
        speed_weighting=str(config.analytics.event_impact.speed_weighting),  # type: ignore[arg-type]
        min_baseline_points=int(config.analytics.event_impact.min_baseline_points),
        min_event_points=int(config.analytics.event_impact.min_event_points),
    )

    return UiSettings(
        preprocessing={
            "source_granularity_minutes": int(config.preprocessing.source_granularity_minutes),
            "target_granularity_minutes": int(config.preprocessing.target_granularity_minutes),
        },
        analytics={
            "reliability": reliability.model_dump(),
            "anomalies": anomalies.model_dump(),
            "event_impact": impact.model_dump(),
        },
        warehouse={
            "enabled": bool(config.warehouse.enabled),
            "use_duckdb": bool(config.warehouse.use_duckdb),
        },
        enums={
            "anomalies_direction": ["low", "high", "both"],
            "event_impact_speed_weighting": ["volume", "equal"],
        },
    )

