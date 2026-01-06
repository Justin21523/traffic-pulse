"""TDX authentication helpers (OAuth 2.0 client credentials).

This module is a small, focused layer in the ingestion data flow:
- `.env` provides `TDX_CLIENT_ID` / `TDX_CLIENT_SECRET` (never commit these).
- We exchange those credentials for a short-lived access token via the TDX token endpoint.
- We cache the token in-memory and refresh it when it is (almost) expired.

Key beginner-friendly idea:
Even if the rest of the ingestion pipeline is correct, every data request will fail without
an `Authorization: Bearer <token>` header, so token handling must be reliable and explainable.
"""

from __future__ import annotations

# We use os.getenv to read credentials from the environment (often loaded from a local `.env` file).
import os
# We use time.time() for epoch-seconds comparisons (simple and timezone-independent).
import time
# Dataclasses are a lightweight way to bundle a few related fields without boilerplate.
from dataclasses import dataclass
# Optional expresses "this value may be None", which is common for cached state.
from typing import Optional

# httpx is our HTTP client for both the token endpoint and data endpoints (in other modules).
import httpx

# AppConfig typing keeps this module usable with explicit configs (tests) and the global config.
from trafficpulse.settings import AppConfig, get_config


@dataclass
class OAuthToken:
    """A cached OAuth access token with an absolute expiry time (epoch seconds)."""

    # The bearer token string that will be placed in `Authorization: Bearer ...`.
    access_token: str
    # When the token expires, expressed as epoch seconds (time.time()) for easy comparison.
    expires_at_epoch_seconds: float

    def is_expired(self, buffer_seconds: int = 30) -> bool:
        """Return True when the token is expired (or close enough to expiry to be unsafe).

        Why we use a buffer:
        - Client/server clocks can drift (clock skew).
        - A token can expire mid-request (race condition).
        - Treating "almost expired" as expired reduces intermittent 401/403 errors.
        """

        # Compare "now" to an expiry time reduced by a safety buffer.
        return time.time() >= (self.expires_at_epoch_seconds - buffer_seconds)


def load_tdx_credentials() -> tuple[str, str]:
    """Load TDX credentials from environment variables.

    We keep secrets out of YAML configs and out of git by using `.env` and environment variables.
    """

    # Read and strip values to avoid "invisible" whitespace bugs from copy/paste.
    client_id = os.getenv("TDX_CLIENT_ID", "").strip()
    client_secret = os.getenv("TDX_CLIENT_SECRET", "").strip()
    # Fail fast with an actionable error message so the user can fix setup before downloading data.
    if not client_id or not client_secret:
        raise ValueError(
            "Missing TDX credentials. Set TDX_CLIENT_ID and TDX_CLIENT_SECRET in .env or environment variables."
        )
    # Return as a pair so callers can pass credentials explicitly into other helpers.
    return client_id, client_secret


class TdxTokenProvider:
    """Fetch and cache a TDX access token for authenticated API requests."""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        http_client: httpx.Client,
        timeout_seconds: int = 30,
    ) -> None:
        # Token endpoint URL is provided by config so it can be changed without code edits.
        self.token_url = token_url
        # Client id/secret are secrets: never log them, and keep them out of repo files.
        self.client_id = client_id
        self.client_secret = client_secret
        # We accept an injected http client so the caller can manage lifetime (and close it properly).
        self.http_client = http_client
        # Timeout is configurable because network conditions differ across environments.
        self.timeout_seconds = timeout_seconds

        # Cached token state (None until we successfully fetch one).
        self._token: Optional[OAuthToken] = None

    @classmethod
    def from_config(
        cls, config: Optional[AppConfig] = None, http_client: Optional[httpx.Client] = None
    ) -> "TdxTokenProvider":
        """Convenience constructor that reads endpoints/timeouts from config.

        Pitfall:
        If this method creates an `httpx.Client`, the caller should ensure it is eventually closed.
        For long-running apps, prefer passing a shared client owned by a higher-level component.
        """

        # Use an explicit config if provided (useful for tests); otherwise load the global config.
        resolved_config = config or get_config()
        # Read secrets from environment so they are not stored in versioned files.
        client_id, client_secret = load_tdx_credentials()
        # Allow injection of a client (tests); otherwise create one with the configured timeout.
        client = http_client or httpx.Client(timeout=resolved_config.tdx.request_timeout_seconds)
        # Instantiate the provider with config-driven settings so behavior is reproducible.
        return cls(
            token_url=resolved_config.tdx.token_url,
            client_id=client_id,
            client_secret=client_secret,
            http_client=client,
            timeout_seconds=resolved_config.tdx.request_timeout_seconds,
        )

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing it when needed (lazy refresh)."""

        # Refresh on first use or when the token is (nearly) expired.
        if self._token is None or self._token.is_expired():
            self._token = self._refresh_token()
        # At this point `_token` must exist, so we can return the bearer string.
        return self._token.access_token

    def _refresh_token(self) -> OAuthToken:
        """Request a new token from TDX using the client credentials flow."""

        # Use form-encoded body fields per the OAuth 2.0 token endpoint convention.
        response = self.http_client.post(
            self.token_url,
            data={
                # Client credentials flow: exchange id/secret for a short-lived access token.
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            # Keep a hard timeout so auth failures don't stall the whole ingestion pipeline.
            timeout=self.timeout_seconds,
        )
        # Raise an exception on non-2xx so callers see the real HTTP failure (401/403/5xx, etc.).
        response.raise_for_status()
        # Parse JSON; if the server returns a non-JSON error, this will raise, which is desirable.
        payload = response.json()

        # Validate required fields to avoid propagating a broken token through the pipeline.
        access_token = payload.get("access_token")
        if not access_token:
            raise ValueError("TDX token response is missing 'access_token'.")

        # TDX typically returns `expires_in` seconds; default to 30 minutes if missing.
        expires_in = int(payload.get("expires_in", 1800))
        # Convert "expires in N seconds" into an absolute epoch timestamp for easy expiry checks.
        return OAuthToken(
            access_token=access_token, expires_at_epoch_seconds=time.time() + expires_in
        )
