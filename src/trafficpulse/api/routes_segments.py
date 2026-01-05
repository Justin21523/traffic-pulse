from __future__ import annotations

from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from trafficpulse.ingestion.schemas import SegmentMetadata
from trafficpulse.settings import get_config
from trafficpulse.storage.backend import duckdb_backend
from trafficpulse.storage.datasets import load_csv, load_parquet, segments_csv_path, segments_parquet_path


router = APIRouter()


@router.get("/segments", response_model=list[SegmentMetadata])
def list_segments(city: Optional[str] = Query(default=None)) -> list[SegmentMetadata]:
    config = get_config()
    backend = duckdb_backend(config)
    df: pd.DataFrame
    if backend is not None:
        df = backend.query_segments(
            city=city,
            columns=["segment_id", "name", "city", "direction", "lat", "lon", "road_name", "link_id"],
        )
    else:
        parquet_path = segments_parquet_path(config.warehouse.parquet_dir)
        csv_path = segments_csv_path(config.paths.processed_dir)
        if config.warehouse.enabled and parquet_path.exists():
            df = load_parquet(parquet_path)
        else:
            if not csv_path.exists():
                raise HTTPException(
                    status_code=404,
                    detail="segments dataset not found. Run scripts/build_dataset.py first.",
                )
            df = load_csv(csv_path)

    if df.empty:
        return []

    if city:
        if "city" not in df.columns:
            raise HTTPException(status_code=500, detail="segments dataset is missing 'city' column.")
        df = df[df["city"].astype(str) == city]

    df = df.where(pd.notnull(df), None)
    return [SegmentMetadata(**record) for record in df.to_dict(orient="records")]
