from __future__ import annotations

from fastapi.testclient import TestClient


def test_segments_endpoint_coerces_nan_to_none(monkeypatch, tmp_path) -> None:
    processed = tmp_path / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    # Empty string cells become NaN when pandas reads CSV; the API should return None instead of raising.
    (processed / "segments.csv").write_text(
        "segment_id,city,name,direction,lat,lon,road_name,link_id\n"
        "V1,Taipei,,,25.0,121.5,Road,123\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  processed_dir: {processed}\n", encoding="utf-8")

    monkeypatch.setenv("TRAFFICPULSE_CONFIG", str(config_path))
    monkeypatch.setattr("trafficpulse.settings._CONFIG", None)

    from trafficpulse.api.app import create_app

    client = TestClient(create_app())
    resp = client.get("/segments")
    assert resp.status_code == 200, resp.text
    items = resp.json()
    assert items and items[0]["segment_id"] == "V1"
    assert items[0]["name"] is None
    assert items[0]["direction"] is None

