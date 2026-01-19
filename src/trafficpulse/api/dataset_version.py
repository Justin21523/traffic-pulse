from __future__ import annotations

from pathlib import Path

from trafficpulse.settings import AppConfig
from trafficpulse.storage.datasets import observations_csv_path, observations_parquet_path


def minutes_candidates(config: AppConfig) -> list[int]:
    return sorted(
        {
            int(config.preprocessing.source_granularity_minutes),
            int(config.preprocessing.target_granularity_minutes),
            60,
        }
    )


def dataset_version_from_paths(
    processed_dir: Path,
    parquet_dir: Path,
    minutes: list[int],
    *,
    cache_dir: Path | None = None,
) -> str | None:
    candidates: list[Path] = []
    for m in minutes:
        candidates.append(observations_csv_path(processed_dir, int(m)))
        candidates.append(observations_parquet_path(parquet_dir, int(m)))
    if cache_dir is not None:
        candidates.append(cache_dir / "materialized_defaults.json")
        candidates.append(cache_dir / "congestion_alerts.csv")
        candidates.append(cache_dir / "event_hotspot_links.csv")
        candidates.extend(sorted(cache_dir.glob("baselines_speed_*")))
        candidates.extend(sorted(cache_dir.glob("segment_quality_*")))
    candidates.append(processed_dir / "weather_observations.csv")
    candidates.append(processed_dir / "events_roadworks.csv")
    candidates.append(processed_dir / "events_incidents_extra.csv")
    candidates.append(processed_dir / "events_calendar.csv")
    candidates.append(processed_dir / "segments_enriched.csv")
    existing = [p for p in candidates if p.exists()]
    if not existing:
        return None
    latest = max(existing, key=lambda p: p.stat().st_mtime)
    stat = latest.stat()
    return f"{latest.name}:{int(stat.st_mtime)}:{int(stat.st_size)}"


def dataset_version(config: AppConfig) -> str | None:
    mins = minutes_candidates(config)
    return dataset_version_from_paths(
        config.paths.processed_dir,
        config.warehouse.parquet_dir,
        mins,
        cache_dir=config.paths.cache_dir,
    )
