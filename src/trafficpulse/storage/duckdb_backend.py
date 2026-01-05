from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

from trafficpulse.storage.datasets import (
    events_parquet_path,
    observations_parquet_path,
    segments_parquet_path,
)
from trafficpulse.utils.time import to_utc


def _import_duckdb():
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required. Install requirements.txt.") from exc
    return duckdb


def _sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return to_utc(dt)


@dataclass(frozen=True)
class DuckdbParquetBackend:
    parquet_dir: Path

    def max_observation_timestamp(self, *, minutes: int) -> Optional[datetime]:
        path = observations_parquet_path(self.parquet_dir, int(minutes))
        if not path.exists():
            return None

        sql = f"SELECT max(timestamp) AS max_ts FROM read_parquet({_sql_literal(str(path))})"
        duckdb = _import_duckdb()
        con = duckdb.connect(database=":memory:")
        try:
            df = con.execute(sql).fetchdf()
        finally:
            con.close()

        if df.empty:
            return None
        value = df.loc[0, "max_ts"]
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, datetime):
            return value
        return None

    def max_event_start_time(self) -> Optional[datetime]:
        path = events_parquet_path(self.parquet_dir)
        if not path.exists():
            return None

        sql = f"SELECT max(start_time) AS max_start FROM read_parquet({_sql_literal(str(path))})"
        duckdb = _import_duckdb()
        con = duckdb.connect(database=":memory:")
        try:
            df = con.execute(sql).fetchdf()
        finally:
            con.close()

        if df.empty:
            return None
        value = df.loc[0, "max_start"]
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, datetime):
            return value
        return None

    def query_segments(
        self,
        *,
        city: Optional[str] = None,
        bbox: Optional[tuple[float, float, float, float]] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        path = segments_parquet_path(self.parquet_dir)
        if not path.exists():
            return pd.DataFrame()

        cols = list(columns) if columns else ["segment_id", "name", "city", "direction", "lat", "lon", "road_name", "link_id"]
        select_cols = ", ".join(cols)

        sql = f"SELECT {select_cols} FROM read_parquet({_sql_literal(str(path))}) WHERE 1=1"
        params: list[object] = []

        if city:
            sql += " AND city = ?"
            params.append(str(city))
        if bbox:
            min_lon, min_lat, max_lon, max_lat = bbox
            sql += " AND lon >= ? AND lon <= ? AND lat >= ? AND lat <= ?"
            params.extend([float(min_lon), float(max_lon), float(min_lat), float(max_lat)])

        duckdb = _import_duckdb()
        con = duckdb.connect(database=":memory:")
        try:
            return con.execute(sql, params).fetchdf()
        finally:
            con.close()

    def query_observations(
        self,
        *,
        minutes: int,
        segment_ids: Optional[Sequence[str]] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        path = observations_parquet_path(self.parquet_dir, int(minutes))
        if not path.exists():
            return pd.DataFrame()

        cols = list(columns) if columns else ["timestamp", "segment_id", "speed_kph", "volume", "occupancy_pct"]
        select_cols = ", ".join(cols)

        sql = f"SELECT {select_cols} FROM read_parquet({_sql_literal(str(path))}) WHERE 1=1"
        params: list[object] = []

        if segment_ids:
            placeholders = ", ".join(["?"] * len(segment_ids))
            sql += f" AND segment_id IN ({placeholders})"
            params.extend([str(sid) for sid in segment_ids])

        start_utc = _as_utc(start)
        end_utc = _as_utc(end)
        if start_utc is not None:
            sql += " AND timestamp >= ?"
            params.append(start_utc)
        if end_utc is not None:
            sql += " AND timestamp < ?"
            params.append(end_utc)

        duckdb = _import_duckdb()
        con = duckdb.connect(database=":memory:")
        try:
            return con.execute(sql, params).fetchdf()
        finally:
            con.close()

    def query_events(
        self,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        city: Optional[str] = None,
        bbox: Optional[tuple[float, float, float, float]] = None,
        limit: int = 500,
        columns: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        path = events_parquet_path(self.parquet_dir)
        if not path.exists():
            return pd.DataFrame()

        cols = list(columns) if columns else [
            "event_id",
            "start_time",
            "end_time",
            "event_type",
            "description",
            "road_name",
            "direction",
            "severity",
            "lat",
            "lon",
            "city",
        ]
        select_cols = ", ".join(cols)

        sql = f"SELECT {select_cols} FROM read_parquet({_sql_literal(str(path))}) WHERE 1=1"
        params: list[object] = []

        start_utc = _as_utc(start)
        end_utc = _as_utc(end)
        if start_utc is not None:
            sql += " AND start_time >= ?"
            params.append(start_utc)
        if end_utc is not None:
            sql += " AND start_time < ?"
            params.append(end_utc)

        if city:
            sql += " AND city = ?"
            params.append(str(city))

        if bbox:
            min_lon, min_lat, max_lon, max_lat = bbox
            sql += " AND lon >= ? AND lon <= ? AND lat >= ? AND lat <= ?"
            params.extend([float(min_lon), float(max_lon), float(min_lat), float(max_lat)])

        sql += " ORDER BY start_time DESC LIMIT ?"
        params.append(int(limit))

        duckdb = _import_duckdb()
        con = duckdb.connect(database=":memory:")
        try:
            return con.execute(sql, params).fetchdf()
        finally:
            con.close()

    def query_event_by_id(self, event_id: str, *, columns: Optional[Sequence[str]] = None) -> pd.DataFrame:
        path = events_parquet_path(self.parquet_dir)
        if not path.exists():
            return pd.DataFrame()

        cols = list(columns) if columns else [
            "event_id",
            "start_time",
            "end_time",
            "event_type",
            "description",
            "road_name",
            "direction",
            "severity",
            "lat",
            "lon",
            "city",
        ]
        select_cols = ", ".join(cols)

        sql = f"SELECT {select_cols} FROM read_parquet({_sql_literal(str(path))}) WHERE event_id = ? LIMIT 1"
        duckdb = _import_duckdb()
        con = duckdb.connect(database=":memory:")
        try:
            return con.execute(sql, [str(event_id)]).fetchdf()
        finally:
            con.close()
