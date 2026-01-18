from __future__ import annotations

from fastapi.testclient import TestClient


def test_corridors_endpoint_includes_bbox(monkeypatch, tmp_path) -> None:
    processed = tmp_path / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    (processed / "segments.csv").write_text(
        "segment_id,lat,lon,city\nS1,25.0,121.5,Taipei\nS2,25.1,121.6,Taipei\n",
        encoding="utf-8",
    )

    corridors_csv = tmp_path / "corridors.csv"
    corridors_csv.write_text(
        "corridor_id,corridor_name,segment_id,weight\nC1,Test,S1,1\nC1,Test,S2,1\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"paths:\n  processed_dir: {processed}\nanalytics:\n  corridors:\n    corridors_csv: {corridors_csv}\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("TRAFFICPULSE_CONFIG", str(config_path))
    monkeypatch.setattr("trafficpulse.settings._CONFIG", None)

    from trafficpulse.api.app import create_app

    client = TestClient(create_app())
    resp = client.get("/corridors")
    assert resp.status_code == 200, resp.text
    items = resp.json()
    assert items and items[0]["corridor_id"] == "C1"
    assert items[0]["min_lat"] == 25.0
    assert items[0]["max_lon"] == 121.6

