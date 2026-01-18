from __future__ import annotations

import httpx

from trafficpulse.ingestion.tdx_traffic_client import ODataQuery, TdxTrafficClient
from trafficpulse.settings import AppConfig


class _FakeTokenProvider:
    def get_access_token(self) -> str:  # pragma: no cover - trivial
        return "token"

    def invalidate(self) -> None:  # pragma: no cover - trivial
        return None


def test_request_json_honors_retry_after(monkeypatch, tmp_path) -> None:
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("trafficpulse.ingestion.tdx_traffic_client.time.sleep", fake_sleep)
    monkeypatch.setenv("TDX_CLIENT_ID", "dummy")
    monkeypatch.setenv("TDX_CLIENT_SECRET", "dummy")

    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(status_code=429, headers={"Retry-After": "1"})
        return httpx.Response(status_code=200, json=[{"ok": True}])

    config = AppConfig().model_copy(
        update={
            "cache": AppConfig().cache.model_copy(update={"enabled": False}),
            "tdx": AppConfig()
            .tdx.model_copy(
                update={
                    "base_url": "https://example.test",
                    "retry_backoff_seconds": 0.0,
                    "jitter_seconds": 0.0,
                    "respect_retry_after": True,
                    "max_retries": 1,
                }
            ),
        }
    ).resolve_paths(root=tmp_path)

    http_client = httpx.Client(
        base_url=config.tdx.base_url,
        transport=httpx.MockTransport(handler),
        timeout=config.tdx.request_timeout_seconds,
        headers={"accept": "application/json"},
    )

    client = TdxTrafficClient(config=config, http_client=http_client)
    client._token_provider = _FakeTokenProvider()  # type: ignore[assignment]
    try:
        items = client._request_json(ODataQuery(endpoint="/anything", params={}))
    finally:
        client.close()

    assert items == [{"ok": True}]
    assert calls["count"] == 2
    assert sleeps and sleeps[0] >= 1.0


def test_sleep_throttle_respects_min_interval(monkeypatch, tmp_path) -> None:
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("trafficpulse.ingestion.tdx_traffic_client.time.sleep", fake_sleep)
    monkeypatch.setattr("trafficpulse.ingestion.tdx_traffic_client.time.time", lambda: 100.0)
    monkeypatch.setenv("TDX_CLIENT_ID", "dummy")
    monkeypatch.setenv("TDX_CLIENT_SECRET", "dummy")

    config = AppConfig().model_copy(
        update={
            "cache": AppConfig().cache.model_copy(update={"enabled": False}),
            "tdx": AppConfig()
            .tdx.model_copy(
                update={
                    "base_url": "https://example.test",
                    "min_request_interval_seconds": 1.0,
                }
            ),
        }
    ).resolve_paths(root=tmp_path)

    http_client = httpx.Client(
        base_url=config.tdx.base_url,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[])),
        timeout=config.tdx.request_timeout_seconds,
        headers={"accept": "application/json"},
    )

    client = TdxTrafficClient(config=config, http_client=http_client)
    client._token_provider = _FakeTokenProvider()  # type: ignore[assignment]
    try:
        client._last_request_epoch_seconds = 99.5
        client._sleep_throttle()
    finally:
        client.close()

    assert sleeps == [0.5]
