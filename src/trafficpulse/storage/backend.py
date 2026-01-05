from __future__ import annotations

from pathlib import Path
from typing import Optional

import importlib.util

from trafficpulse.settings import AppConfig, get_config
from trafficpulse.storage.duckdb_backend import DuckdbParquetBackend


def duckdb_available() -> bool:
    return importlib.util.find_spec("duckdb") is not None


def warehouse_enabled(config: Optional[AppConfig] = None) -> bool:
    resolved = config or get_config()
    return bool(getattr(resolved, "warehouse", None) and resolved.warehouse.enabled)


def parquet_dir(config: Optional[AppConfig] = None) -> Path:
    resolved = config or get_config()
    return resolved.warehouse.parquet_dir


def duckdb_backend(config: Optional[AppConfig] = None) -> Optional[DuckdbParquetBackend]:
    resolved = config or get_config()
    if not resolved.warehouse.enabled or not resolved.warehouse.use_duckdb:
        return None
    if not duckdb_available():
        return None
    return DuckdbParquetBackend(parquet_dir=resolved.warehouse.parquet_dir)
