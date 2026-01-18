from __future__ import annotations

import httpx

from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient
from trafficpulse.settings import AppConfig


class _FakeTokenProvider:
    def get_access_token(self) -> str:  # pragma: no cover - trivial
        return "token"

    def invalidate(self) -> None:  # pragma: no cover - trivial
        return None


def test_vd_lane_speed_sentinel_is_sanitized(tmp_path) -> None:
    config = AppConfig().model_copy(
        update={
            "cache": AppConfig().cache.model_copy(update={"enabled": False}),
            "tdx": AppConfig().tdx.model_copy(update={"base_url": "https://example.test"}),
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
        record = {
            "LinkFlows": [
                {
                    "Lanes": [
                        {"Speed": -99, "Volume": 10, "Occupancy": 10},
                        {"Speed": 50, "Volume": 10, "Occupancy": 10},
                    ]
                }
            ]
        }
        speed, volume, occupancy = client._extract_vd_observation_values(record)
    finally:
        client.close()

    assert speed == 50.0
    assert volume == 20.0
    assert occupancy == 10.0

