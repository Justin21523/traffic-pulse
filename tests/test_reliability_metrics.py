from __future__ import annotations

import pandas as pd

from trafficpulse.analytics.reliability import ReliabilitySpec, compute_reliability_metrics


def test_compute_reliability_metrics_basic() -> None:
    df = pd.DataFrame(
        [
            {"timestamp": "2026-01-01T00:00:00Z", "segment_id": "A", "speed_kph": 10},
            {"timestamp": "2026-01-01T00:05:00Z", "segment_id": "A", "speed_kph": 30},
            {"timestamp": "2026-01-01T00:00:00Z", "segment_id": "B", "speed_kph": 50},
        ]
    )
    spec = ReliabilitySpec(
        congestion_speed_threshold_kph=15.0,
        min_samples=1,
        weight_mean_speed=0.4,
        weight_speed_std=0.3,
        weight_congestion_frequency=0.3,
    )

    metrics = compute_reliability_metrics(df, spec)
    rows = {row["segment_id"]: row for row in metrics.to_dict(orient="records")}

    assert rows["A"]["n_samples"] == 2
    assert rows["A"]["mean_speed_kph"] == 20.0
    assert rows["A"]["congestion_frequency"] == 0.5
    assert rows["B"]["n_samples"] == 1
    assert rows["B"]["speed_std_kph"] == 0.0

