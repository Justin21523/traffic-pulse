from __future__ import annotations

from fastapi.testclient import TestClient


def test_timeseries_endpoint_replaces_nan_with_none(monkeypatch, tmp_path) -> None:
    processed = tmp_path / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    (processed / "observations_5min.csv").write_text(
        "timestamp,segment_id,speed_kph,volume,occupancy_pct\n"
        "2026-01-01T00:00:00Z,V1,50.0,,10.0\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  processed_dir: {processed}\n", encoding="utf-8")

    monkeypatch.setenv("TRAFFICPULSE_CONFIG", str(config_path))
    monkeypatch.setattr("trafficpulse.settings._CONFIG", None)

    from trafficpulse.api.app import create_app

    client = TestClient(create_app())
    resp = client.get(
        "/timeseries",
        params={
            "segment_id": "V1",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-01T01:00:00Z",
            "minutes": 5,
        },
    )
    assert resp.status_code == 200, resp.text
    items = resp.json()
    assert items and items[0]["segment_id"] == "V1"
    assert items[0]["volume"] is None

