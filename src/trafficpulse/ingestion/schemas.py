from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class SegmentMetadata(BaseModel):
    segment_id: str
    name: Optional[str] = None
    city: Optional[str] = None
    direction: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    road_name: Optional[str] = None
    link_id: Optional[str] = None


class TrafficObservation(BaseModel):
    timestamp: datetime
    segment_id: str
    speed_kph: Optional[float] = None
    volume: Optional[float] = None
    occupancy_pct: Optional[float] = None


class TrafficEvent(BaseModel):
    event_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    event_type: Optional[str] = None
    description: Optional[str] = None
    road_name: Optional[str] = None
    direction: Optional[str] = None
    severity: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    city: Optional[str] = None
