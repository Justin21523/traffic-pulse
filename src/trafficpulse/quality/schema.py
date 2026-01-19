from __future__ import annotations

# Schema versions for processed datasets. Bump when on-disk column names/semantics change.
SCHEMA_VERSIONS: dict[str, int] = {
    "segments": 1,
    "observations": 1,
    "events": 1,
}

# Required columns (minimum contract) for processed datasets.
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "segments": ("segment_id", "lat", "lon"),
    "observations": ("timestamp", "segment_id", "speed_kph"),
    "events": ("event_id", "start_time"),
}

