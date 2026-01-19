from __future__ import annotations

from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class ReasonCode(str, Enum):
    MISSING_DATASET = "missing_dataset"
    MATERIALIZED_EMPTY = "materialized_empty"
    MATERIALIZED_NO_MATCHES = "materialized_no_matches"
    NO_EVENT_LINKS = "no_event_links"
    NO_EVENTS = "no_events"
    NO_EVENTS_IN_FILTERS = "no_events_in_filters"
    NO_METRICS = "no_metrics"
    NO_OBSERVATIONS = "no_observations"
    NO_OBSERVATIONS_FOR_SEGMENTS = "no_observations_for_segments"
    NO_RANKINGS = "no_rankings"
    NO_SAMPLES = "no_samples"
    NO_SEGMENTS = "no_segments"
    NO_SEGMENTS_AFTER_MERGE = "no_segments_after_merge"
    NO_VALID_TIMESTAMPS = "no_valid_timestamps"


class EmptyReason(BaseModel):
    code: ReasonCode
    message: str
    suggestion: str | None = None


class ItemsResponse(BaseModel, Generic[T]):
    items: list[T] = Field(default_factory=list)
    reason: EmptyReason | None = None
