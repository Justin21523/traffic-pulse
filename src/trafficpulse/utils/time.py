from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


DEFAULT_TZ = ZoneInfo("Asia/Taipei")


def parse_datetime(value: str, default_tz: ZoneInfo = DEFAULT_TZ) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)
    return dt


def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=DEFAULT_TZ)
    return dt.astimezone(timezone.utc)


def floor_to_minutes(dt: datetime, minutes: int) -> datetime:
    if minutes <= 0:
        raise ValueError("minutes must be > 0")
    discard = timedelta(
        minutes=dt.minute % minutes,
        seconds=dt.second,
        microseconds=dt.microsecond,
    )
    return dt - discard

