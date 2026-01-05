from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional

import httpx

from trafficpulse.settings import AppConfig, get_config


@dataclass
class OAuthToken:
    access_token: str
    expires_at_epoch_seconds: float

    def is_expired(self, buffer_seconds: int = 30) -> bool:
        return time.time() >= (self.expires_at_epoch_seconds - buffer_seconds)


def load_tdx_credentials() -> tuple[str, str]:
    client_id = os.getenv("TDX_CLIENT_ID", "").strip()
    client_secret = os.getenv("TDX_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise ValueError(
            "Missing TDX credentials. Set TDX_CLIENT_ID and TDX_CLIENT_SECRET in .env or environment variables."
        )
    return client_id, client_secret


class TdxTokenProvider:
    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        http_client: httpx.Client,
        timeout_seconds: int = 30,
    ) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.http_client = http_client
        self.timeout_seconds = timeout_seconds

        self._token: Optional[OAuthToken] = None

    @classmethod
    def from_config(
        cls, config: Optional[AppConfig] = None, http_client: Optional[httpx.Client] = None
    ) -> "TdxTokenProvider":
        resolved_config = config or get_config()
        client_id, client_secret = load_tdx_credentials()
        client = http_client or httpx.Client(timeout=resolved_config.tdx.request_timeout_seconds)
        return cls(
            token_url=resolved_config.tdx.token_url,
            client_id=client_id,
            client_secret=client_secret,
            http_client=client,
            timeout_seconds=resolved_config.tdx.request_timeout_seconds,
        )

    def get_access_token(self) -> str:
        if self._token is None or self._token.is_expired():
            self._token = self._refresh_token()
        return self._token.access_token

    def _refresh_token(self) -> OAuthToken:
        response = self.http_client.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()

        access_token = payload.get("access_token")
        if not access_token:
            raise ValueError("TDX token response is missing 'access_token'.")

        expires_in = int(payload.get("expires_in", 1800))
        return OAuthToken(
            access_token=access_token, expires_at_epoch_seconds=time.time() + expires_in
        )

