from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _read_env_value(path: Path, key: str) -> str | None:
    if not path.exists():
        return None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip() == key:
            return v.strip()
    return None


def _fetch_json(url: str, timeout_seconds: float = 5.0) -> dict[str, Any]:
    req = Request(url, headers={"accept": "application/json"})
    with urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
        payload = resp.read().decode("utf-8", errors="ignore")
    data = json.loads(payload)
    return data if isinstance(data, dict) else {}


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        # Handles "2026-01-19T10:01:00Z" and ISO with offset.
        text = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


@dataclass(frozen=True)
class HealthAssessment:
    ok: bool
    code: str
    message: str


def assess(status: dict[str, Any], *, stale_minutes: int = 20) -> HealthAssessment:
    last_ok = status.get("last_ingest_ok")
    if last_ok is False:
        code = str(status.get("last_error_code") or "ingest_error")
        msg = str(status.get("last_error") or "ingestion error")
        return HealthAssessment(ok=False, code=code, message=msg)

    last_ts = _parse_dt(status.get("observations_last_timestamp_utc"))
    if last_ts is None:
        return HealthAssessment(ok=False, code="no_data", message="observations_last_timestamp_utc missing")

    age_minutes = int((_now_utc() - last_ts).total_seconds() / 60.0)
    if age_minutes > int(stale_minutes):
        return HealthAssessment(ok=False, code="stale", message=f"observations stale: {age_minutes} minutes old")

    return HealthAssessment(ok=True, code="ok", message="healthy")


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env_path = repo_root / ".env"
    in_docker = Path("/.dockerenv").exists()
    if in_docker:
        # When executed via `docker compose run ...`, localhost points to the ephemeral container itself.
        # Use the compose service DNS name instead.
        api_base = "http://api:8000"
    else:
        port = os.getenv("TRAFFICPULSE_API_PORT") or _read_env_value(env_path, "TRAFFICPULSE_API_PORT") or "8003"
        api_base = f"http://localhost:{port}"

    cache_dir = repo_root / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    state_path = cache_dir / "monitor_state.json"
    alerts_log = cache_dir / "alerts.log"

    status_url = f"{api_base}/ui/status"
    try:
        status = _fetch_json(status_url, timeout_seconds=5.0)
    except Exception as exc:
        assessment = HealthAssessment(ok=False, code="api_unreachable", message=str(exc))
    else:
        assessment = assess(status, stale_minutes=20)

    prev: dict[str, Any] = {}
    if state_path.exists():
        try:
            prev = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            prev = {}

    prev_code = prev.get("code")
    prev_ok = prev.get("ok")
    changed = (prev_code != assessment.code) or (prev_ok != assessment.ok)

    state_path.write_text(
        json.dumps(
            {
                "generated_at_utc": _now_utc().isoformat(),
                "ok": assessment.ok,
                "code": assessment.code,
                "message": assessment.message,
                "api_base": api_base,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    if changed or not assessment.ok:
        msg = str(assessment.message).replace("\n", " | ").strip()
        line = f"{_now_utc().isoformat()} ok={assessment.ok} code={assessment.code} msg={msg}\n"
        alerts_log.open("a", encoding="utf-8").write(line)


if __name__ == "__main__":
    main()
