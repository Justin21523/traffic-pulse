from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class IngestErrorInfo:
    code: str
    kind: str
    message: str


def classify_ingest_error(exc: Exception) -> IngestErrorInfo:
    """Classify common ingestion failures into stable codes for UI/monitoring."""

    # Import lazily so non-ingestion code paths don't require httpx types.
    try:
        import httpx  # type: ignore
    except Exception:  # pragma: no cover
        httpx = None  # type: ignore

    text = str(exc)
    lower = text.lower()

    if httpx is not None and isinstance(exc, httpx.HTTPStatusError):
        status = int(exc.response.status_code)
        if status == 429:
            return IngestErrorInfo(code="rate_limited", kind="http", message=f"HTTP 429 rate limited: {text}")
        if status in {401, 403}:
            return IngestErrorInfo(code="auth", kind="http", message=f"HTTP {status} auth error: {text}")
        return IngestErrorInfo(code=f"http_{status}", kind="http", message=f"HTTP {status}: {text}")

    if httpx is not None and isinstance(exc, (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout)):
        return IngestErrorInfo(code="timeout", kind="network", message=text)

    if httpx is not None and isinstance(exc, httpx.ConnectError):
        # Often wraps OSError with errno.
        if "no route to host" in lower or "errno 113" in lower:
            return IngestErrorInfo(code="no_route", kind="network", message=text)
        if "name or service not known" in lower or "temporary failure in name resolution" in lower:
            return IngestErrorInfo(code="dns", kind="network", message=text)
        return IngestErrorInfo(code="connect_error", kind="network", message=text)

    if "no route to host" in lower or "errno 113" in lower:
        return IngestErrorInfo(code="no_route", kind="network", message=text)

    return IngestErrorInfo(code="unknown", kind="unknown", message=text)

