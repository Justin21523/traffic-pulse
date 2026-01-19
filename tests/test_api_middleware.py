from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from trafficpulse.api.middleware import CacheConfig, RateLimitConfig, SimpleRateLimitMiddleware, TtlResponseCacheMiddleware


def test_ttl_cache_middleware_caches_get(monkeypatch) -> None:
    # Control time deterministically.
    now = {"t": 1000.0}
    monkeypatch.setattr("trafficpulse.api.middleware.time.time", lambda: now["t"])

    calls = {"n": 0}

    app = FastAPI()
    app.add_middleware(
        TtlResponseCacheMiddleware,
        config=CacheConfig(enabled=True, ttl_seconds=10.0, include_paths=("/expensive",)),
    )

    @app.get("/expensive")
    def expensive() -> dict[str, int]:
        calls["n"] += 1
        return {"calls": calls["n"]}

    client = TestClient(app)
    first = client.get("/expensive?b=1&a=2")
    assert first.headers.get("X-Cache") == "MISS"
    assert first.json() == {"calls": 1}

    second = client.get("/expensive?a=2&b=1")
    assert second.headers.get("X-Cache") == "HIT"
    assert second.json() == {"calls": 1}
    assert calls["n"] == 1

    now["t"] += 11.0
    third = client.get("/expensive?a=2&b=1")
    assert third.headers.get("X-Cache") == "MISS"
    assert third.json() == {"calls": 2}


def test_rate_limit_middleware_blocks_after_threshold(monkeypatch) -> None:
    now = {"t": 1000.0}
    monkeypatch.setattr("trafficpulse.api.middleware.time.time", lambda: now["t"])

    app = FastAPI()
    app.add_middleware(
        SimpleRateLimitMiddleware,
        config=RateLimitConfig(enabled=True, window_seconds=60.0, max_requests=1, include_paths=("/limited",)),
    )

    @app.get("/limited")
    def limited() -> dict[str, bool]:
        return {"ok": True}

    client = TestClient(app)
    assert client.get("/limited").status_code == 200
    resp = client.get("/limited")
    assert resp.status_code == 429
    assert "Retry-After" in resp.headers
