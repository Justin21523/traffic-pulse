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


class CacheSection(BaseModel):
    enabled: bool = True
    ttl_seconds: int = 3600


class TdxSection(BaseModel):
    base_url: str = "https://tdx.transportdata.tw/api/basic/v2"
    token_url: str = (
        "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
    )
    request_timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0


class VdMetadataFields(BaseModel):
    name_field: str = "VDName"
    direction_field: str = "Direction"
    road_name_field: str = "RoadName"
    link_id_field: str = "LinkID"
    lat_field: str = "PositionLat"
    lon_field: str = "PositionLon"


class VdPagingSection(BaseModel):
    page_size: int = 1000


class VdIngestionSection(BaseModel):
    endpoint_templates: list[str] = Field(
        default_factory=lambda: [
            "Traffic/VD/History/City/{city}",
            "Traffic/VD/Live/City/{city}",
        ]
    )
    cities: list[str] = Field(default_factory=lambda: ["Taipei"])

    time_field: str = "DataCollectTime"
    segment_id_field: str = "VDID"

    lane_list_field: str = "VDLives"
    lane_speed_field: str = "Speed"
    lane_volume_field: str = "Volume"
    lane_occupancy_field: str = "Occupancy"

    lane_speed_aggregation: str = "volume_weighted_mean"
    lane_volume_aggregation: str = "sum"
    lane_occupancy_aggregation: str = "mean"

    metadata_fields: VdMetadataFields = Field(default_factory=VdMetadataFields)
    paging: VdPagingSection = Field(default_factory=VdPagingSection)


class IngestionSection(BaseModel):
    dataset: str = "vd"
    query_chunk_minutes: int = 60
    vd: VdIngestionSection = Field(default_factory=VdIngestionSection)


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


class AnalyticsSection(BaseModel):
    reliability: ReliabilitySection = Field(default_factory=ReliabilitySection)
    corridors: CorridorsSection = Field(default_factory=CorridorsSection)


class CorsSection(BaseModel):
    allow_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:8000", "http://localhost:5173"]
    )


class ApiSection(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    cors: CorsSection = Field(default_factory=CorsSection)


class AppConfig(BaseModel):
    app: AppSection = Field(default_factory=AppSection)
    paths: PathsSection = Field(default_factory=PathsSection)
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
            }
        )
        updated_corridors = self.analytics.corridors.model_copy(
            update={
                "corridors_csv": _resolve_path(repo_root, self.analytics.corridors.corridors_csv)
            }
        )
        updated_analytics = self.analytics.model_copy(update={"corridors": updated_corridors})
        return self.model_copy(update={"paths": updated_paths, "analytics": updated_analytics})


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
