from __future__ import annotations

from fastapi.testclient import TestClient


def test_stream_status_endpoint_returns_event_stream(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "api:\n  cache:\n    enabled: false\n  rate_limit:\n    enabled: false\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("TRAFFICPULSE_CONFIG", str(config_path))
    monkeypatch.setattr("trafficpulse.settings._CONFIG", None)

    from trafficpulse.api.app import create_app

    client = TestClient(create_app())
    resp = client.get("/stream/status?interval_seconds=1&max_events=1")
    assert resp.status_code == 200
    assert resp.headers.get("content-type", "").startswith("text/event-stream")
    assert "data: " in resp.text
