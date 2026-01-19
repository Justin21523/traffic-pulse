from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional
from urllib.parse import urlencode

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response


@dataclass(frozen=True)
class CacheConfig:
    enabled: bool
    ttl_seconds: float
    include_paths: tuple[str, ...]
    max_body_bytes: int = 5_000_000


@dataclass(frozen=True)
class RateLimitConfig:
    enabled: bool
    window_seconds: float
    max_requests: int
    include_paths: tuple[str, ...]


def _matches_prefix(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(path.startswith(prefix) for prefix in prefixes)


def _cache_key(request: Request) -> str:
    # Canonicalize query ordering so semantically-identical requests hit the same cache key.
    base = request.url.path
    items = sorted(request.query_params.multi_items())
    if not items:
        return base
    return f"{base}?{urlencode(items, doseq=True)}"


class TtlResponseCacheMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, config: CacheConfig) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        self._config = config
        self._store: dict[str, tuple[float, bytes, int, list[tuple[str, str]]]] = {}

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        cfg = self._config
        if not cfg.enabled:
            return await call_next(request)

        if request.method != "GET":
            return await call_next(request)

        if request.query_params.get("no_cache") in {"1", "true", "yes"}:
            return await call_next(request)

        path = request.url.path
        if not _matches_prefix(path, cfg.include_paths):
            return await call_next(request)

        key = _cache_key(request)
        now = time.time()

        hit = self._store.get(key)
        if hit is not None:
            expires_at, body, status_code, headers = hit
            if now < expires_at:
                remaining = max(0.0, expires_at - now)
                out_headers = dict(headers)
                out_headers["X-Cache"] = "HIT"
                out_headers["X-Cache-TTL"] = str(int(remaining))
                return Response(content=body, status_code=status_code, headers=out_headers, media_type=None)
            self._store.pop(key, None)

        response = await call_next(request)
        if response.status_code != 200:
            return response

        body_bytes: Optional[bytes] = None
        body_iterator = getattr(response, "body_iterator", None)
        if body_iterator is not None:
            chunks: list[bytes] = []
            async for chunk in body_iterator:
                chunks.append(chunk)
            body_bytes = b"".join(chunks)
        else:
            body = getattr(response, "body", None)
            if isinstance(body, (bytes, bytearray)):
                body_bytes = bytes(body)

        if body_bytes is None:
            return response
        if len(body_bytes) > cfg.max_body_bytes:
            return response

        headers = [(k, v) for k, v in response.headers.items() if k.lower() != "set-cookie"]
        self._store[key] = (now + cfg.ttl_seconds, body_bytes, response.status_code, headers)
        out_headers = dict(headers)
        out_headers["X-Cache"] = "MISS"
        out_headers["X-Cache-TTL"] = str(int(cfg.ttl_seconds))
        return Response(
            content=body_bytes,
            status_code=response.status_code,
            headers=out_headers,
            media_type=response.media_type,
        )


class SimpleRateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, config: RateLimitConfig) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        self._config = config
        self._hits: dict[tuple[str, str], Deque[float]] = {}

    def _client_ip(self, request: Request) -> str:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        if request.client:
            return request.client.host
        return "unknown"

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        cfg = self._config
        if not cfg.enabled:
            return await call_next(request)

        if request.method != "GET":
            return await call_next(request)

        path = request.url.path
        if not _matches_prefix(path, cfg.include_paths):
            return await call_next(request)

        ip = self._client_ip(request)
        key = (ip, path)
        now = time.time()

        bucket = self._hits.get(key)
        if bucket is None:
            bucket = deque()
            self._hits[key] = bucket

        window_start = now - cfg.window_seconds
        while bucket and bucket[0] < window_start:
            bucket.popleft()

        if len(bucket) >= cfg.max_requests:
            retry_after = max(1, int(bucket[0] + cfg.window_seconds - now)) if bucket else int(cfg.window_seconds)
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        bucket.append(now)
        return await call_next(request)
