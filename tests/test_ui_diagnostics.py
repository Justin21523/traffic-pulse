from __future__ import annotations

from fastapi.testclient import TestClient


def test_ui_diagnostics_endpoint(monkeypatch, tmp_path) -> None:
    processed = tmp_path / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    (processed / "segments.csv").write_text("segment_id,lat,lon\nS1,25.0,121.5\n", encoding="utf-8")
    (processed / "observations_5min.csv").write_text(
        "timestamp,segment_id,speed_kph\n2026-01-01T00:00:00Z,S1,50\n",
        encoding="utf-8",
    )

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "live_loop_state.json").write_text("{}", encoding="utf-8")

    corridors_csv = tmp_path / "corridors.csv"
    corridors_csv.write_text("corridor_id,corridor_name,segment_id,weight\nC1,Test,S1,1\n", encoding="utf-8")

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"paths:\n  processed_dir: {processed}\n  cache_dir: {cache_dir}\nanalytics:\n  corridors:\n    corridors_csv: {corridors_csv}\napi:\n  cache:\n    enabled: false\n  rate_limit:\n    enabled: false\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("TRAFFICPULSE_CONFIG", str(config_path))
    monkeypatch.setattr("trafficpulse.settings._CONFIG", None)

    from trafficpulse.api.app import create_app

    client = TestClient(create_app())
    resp = client.get("/ui/diagnostics")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["segments_csv"]["exists"] is True
    assert body["corridors_csv_exists"] is True
    assert body["live_loop_state"]["exists"] is True

