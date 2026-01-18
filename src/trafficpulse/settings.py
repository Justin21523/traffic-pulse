from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_path(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (root / path)


class AppSection(BaseModel):
    name: str = "trafficpulse"
    timezone: str = "Asia/Taipei"


class PathsSection(BaseModel):
    raw_dir: Path = Path("data/raw")
    processed_dir: Path = Path("data/processed")
    cache_dir: Path = Path("data/cache")
    outputs_dir: Path = Path("outputs")


class WarehouseSection(BaseModel):
    enabled: bool = False
    parquet_dir: Path = Path("data/processed/parquet")
    duckdb_path: Path = Path("data/processed/trafficpulse.duckdb")
    use_duckdb: bool = True


class CacheSection(BaseModel):
    enabled: bool = True
    ttl_seconds: int = 3600


class TdxSection(BaseModel):
    # Most TrafficPulse datasets use TDX Basic v2.
    base_url: str = "https://tdx.transportdata.tw/api/basic/v2"
    # Some endpoints (e.g., RoadEvent) are still served under TDX Basic v1.
    base_url_v1: str = "https://tdx.transportdata.tw/api/basic/v1"
    # Historical datasets are served by a separate base URL.
    historical_base_url: str = "https://tdx.transportdata.tw/api/historical"
    token_url: str = (
        "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
    )
    request_timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 60.0
    jitter_seconds: float = 0.25
    respect_retry_after: bool = True
    min_request_interval_seconds: float = 0.0


class VdMetadataFields(BaseModel):
    # Note: V2 Road/Traffic VD metadata uses RoadName/RoadSection and may not have VDName/Direction.
    name_field: str = "RoadSection"
    direction_field: str = "Direction"
    road_name_field: str = "RoadName"
    link_id_field: str = "RoadID"
    lat_field: str = "PositionLat"
    lon_field: str = "PositionLon"


class VdPagingSection(BaseModel):
    page_size: int = 1000


class VdIngestionSection(BaseModel):
    endpoint_templates: list[str] = Field(
        default_factory=lambda: [
            "Road/Traffic/Live/VD/City/{city}",
        ]
    )
    historical_endpoint_templates: list[str] = Field(
        default_factory=lambda: [
            "v2/Historical/Road/Traffic/Live/VD/City/{city}",
        ]
    )
    cities: list[str] = Field(default_factory=lambda: ["Taipei"])

    time_field: str = "DataCollectTime"
    segment_id_field: str = "VDID"

    # For Road/Traffic VDLive, lanes are nested under LinkFlows[].Lanes[].
    lane_list_field: str = "VDLives"
    lane_speed_field: str = "Speed"
    lane_volume_field: str = "Volume"
    lane_occupancy_field: str = "Occupancy"

    lane_speed_aggregation: str = "volume_weighted_mean"
    lane_volume_aggregation: str = "sum"
    lane_occupancy_aggregation: str = "mean"

    metadata_fields: VdMetadataFields = Field(default_factory=VdMetadataFields)
    paging: VdPagingSection = Field(default_factory=VdPagingSection)


class EventsPagingSection(BaseModel):
    page_size: int = 1000


class EventsIngestionSection(BaseModel):
    endpoint_templates: list[str] = Field(
        default_factory=lambda: [
            "Traffic/RoadEvent/LiveEvent/City/{city}",
        ]
    )
    historical_endpoint_templates: list[str] = Field(
        default_factory=lambda: [
            "Traffic/RoadEvent/Event/City/{city}",
        ]
    )
    cities: list[str] = Field(default_factory=lambda: ["Taipei"])

    start_time_field: str = "EffectiveTime"
    end_time_field: str = "ExpireTime"
    id_field: str = "EventID"
    type_field: str = "EventType"
    description_field: str = "Description"
    road_name_field: str = "Location"
    direction_field: str = "Direction"
    severity_field: str = "Severity"
    lat_field: str = ""
    lon_field: str = ""

    paging: EventsPagingSection = Field(default_factory=EventsPagingSection)


class IngestionSection(BaseModel):
    dataset: str = "vd"
    query_chunk_minutes: int = 60
    vd: VdIngestionSection = Field(default_factory=VdIngestionSection)
    events: EventsIngestionSection = Field(default_factory=EventsIngestionSection)


class PreprocessingSection(BaseModel):
    source_granularity_minutes: int = 5
    target_granularity_minutes: int = 15
    aggregation: dict[str, str] = Field(
        default_factory=lambda: {
            "speed_kph": "mean",
            "volume": "sum",
            "occupancy_pct": "mean",
        }
    )


class ReliabilityWeights(BaseModel):
    mean_speed: float = 0.4
    speed_std: float = 0.3
    congestion_frequency: float = 0.3


class ReliabilitySection(BaseModel):
    congestion_speed_threshold_kph: float = 30
    min_samples: int = 12
    default_window_hours: int = 24
    weights: ReliabilityWeights = Field(default_factory=ReliabilityWeights)


class CorridorsSection(BaseModel):
    corridors_csv: Path = Path("configs/corridors.csv")
    speed_weighting: str = "volume"  # volume | equal | static
    weight_column: str = "weight"


class AnomaliesSection(BaseModel):
    method: str = "rolling_zscore"
    window_points: int = 12
    z_threshold: float = 3.0
    direction: str = "low"  # low | high | both
    max_gap_minutes: int = 30
    min_event_points: int = 2


class EventImpactSection(BaseModel):
    default_window_hours: int = 24
    radius_meters: float = 1000
    max_segments: int = 50
    baseline_window_minutes: int = 60
    end_time_fallback_minutes: int = 60
    recovery_horizon_minutes: int = 180
    recovery_ratio: float = 0.9
    speed_weighting: str = "volume"  # volume | equal
    min_baseline_points: int = 4
    min_event_points: int = 2


class AnalyticsSection(BaseModel):
    reliability: ReliabilitySection = Field(default_factory=ReliabilitySection)
    corridors: CorridorsSection = Field(default_factory=CorridorsSection)
    anomalies: AnomaliesSection = Field(default_factory=AnomaliesSection)
    event_impact: EventImpactSection = Field(default_factory=EventImpactSection)


class CorsSection(BaseModel):
    allow_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:8000", "http://localhost:5173"]
    )


class ApiSection(BaseModel):
    class Cache(BaseModel):
        enabled: bool = True
        ttl_seconds: int = 60
        include_paths: list[str] = Field(
            default_factory=lambda: [
                "/map/snapshot",
                "/rankings/reliability",
                "/rankings/reliability/corridors",
                "/events",
            ]
        )

    class RateLimit(BaseModel):
        enabled: bool = True
        window_seconds: int = 60
        max_requests: int = 60
        include_paths: list[str] = Field(
            default_factory=lambda: [
                "/map/snapshot",
                "/rankings/reliability",
                "/rankings/reliability/corridors",
                "/events",
            ]
        )

    host: str = "0.0.0.0"
    port: int = 8000
    cors: CorsSection = Field(default_factory=CorsSection)
    cache: Cache = Field(default_factory=Cache)
    rate_limit: RateLimit = Field(default_factory=RateLimit)


class AppConfig(BaseModel):
    app: AppSection = Field(default_factory=AppSection)
    paths: PathsSection = Field(default_factory=PathsSection)
    warehouse: WarehouseSection = Field(default_factory=WarehouseSection)
    cache: CacheSection = Field(default_factory=CacheSection)
    tdx: TdxSection = Field(default_factory=TdxSection)
    ingestion: IngestionSection = Field(default_factory=IngestionSection)
    preprocessing: PreprocessingSection = Field(default_factory=PreprocessingSection)
    analytics: AnalyticsSection = Field(default_factory=AnalyticsSection)
    api: ApiSection = Field(default_factory=ApiSection)

    def resolve_paths(self, root: Optional[Path] = None) -> "AppConfig":
        repo_root = project_root() if root is None else root
        updated_paths = self.paths.model_copy(
            update={
                "raw_dir": _resolve_path(repo_root, self.paths.raw_dir),
                "processed_dir": _resolve_path(repo_root, self.paths.processed_dir),
                "cache_dir": _resolve_path(repo_root, self.paths.cache_dir),
                "outputs_dir": _resolve_path(repo_root, self.paths.outputs_dir),
            }
        )
        updated_corridors = self.analytics.corridors.model_copy(
            update={
                "corridors_csv": _resolve_path(repo_root, self.analytics.corridors.corridors_csv)
            }
        )
        updated_analytics = self.analytics.model_copy(update={"corridors": updated_corridors})
        updated_warehouse = self.warehouse.model_copy(
            update={
                "parquet_dir": _resolve_path(repo_root, self.warehouse.parquet_dir),
                "duckdb_path": _resolve_path(repo_root, self.warehouse.duckdb_path),
            }
        )
        return self.model_copy(
            update={
                "paths": updated_paths,
                "warehouse": updated_warehouse,
                "analytics": updated_analytics,
            }
        )


def _maybe_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return

    load_dotenv()


def load_config(config_path: str | Path | None = None) -> AppConfig:
    _maybe_load_dotenv()

    root = project_root()
    candidate = config_path or os.getenv("TRAFFICPULSE_CONFIG", "configs/config.yaml")
    path = _resolve_path(root, candidate)
    if not path.exists():
        path = root / "configs/config.example.yaml"

    data: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return AppConfig.model_validate(data).resolve_paths(root)


_CONFIG: AppConfig | None = None


def get_config() -> AppConfig:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = load_config()
    return _CONFIG
