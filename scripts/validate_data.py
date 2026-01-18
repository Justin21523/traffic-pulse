from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from trafficpulse.settings import get_config


@dataclass(frozen=True)
class DatasetReport:
    name: str
    path: str
    rows: int
    columns: list[str]
    timestamp_min_utc: str | None = None
    timestamp_max_utc: str | None = None
    unique_segments: int | None = None
    null_speed_rows: int | None = None
    sentinel_speed_rows: int | None = None
    outlier_speed_rows: int | None = None


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _ts_bounds(df: pd.DataFrame, col: str) -> tuple[str | None, str | None]:
    if col not in df.columns or df.empty:
        return None, None
    ts = pd.to_datetime(df[col], errors="coerce", utc=True)
    ts = ts.dropna()
    if ts.empty:
        return None, None
    return ts.min().to_pydatetime().isoformat().replace("+00:00", "Z"), ts.max().to_pydatetime().isoformat().replace("+00:00", "Z")


def report_segments(path: Path) -> DatasetReport:
    df = _read_csv(path)
    return DatasetReport(
        name="segments",
        path=str(path),
        rows=int(len(df)),
        columns=list(df.columns),
    )


def report_observations(path: Path) -> DatasetReport:
    df = _read_csv(path)
    ts_min, ts_max = _ts_bounds(df, "timestamp")
    unique_segments = int(df["segment_id"].astype(str).nunique()) if "segment_id" in df.columns and not df.empty else None

    null_speed = None
    sentinel_speed = None
    outlier_speed = None
    if "speed_kph" in df.columns and not df.empty:
        speed = pd.to_numeric(df["speed_kph"], errors="coerce")
        null_speed = int(speed.isna().sum())
        sentinel_speed = int((speed <= -90).sum())
        outlier_speed = int(((speed < 0) | (speed > 200)).sum())

    return DatasetReport(
        name=path.stem,
        path=str(path),
        rows=int(len(df)),
        columns=list(df.columns),
        timestamp_min_utc=ts_min,
        timestamp_max_utc=ts_max,
        unique_segments=unique_segments,
        null_speed_rows=null_speed,
        sentinel_speed_rows=sentinel_speed,
        outlier_speed_rows=outlier_speed,
    )


def report_events(path: Path) -> DatasetReport:
    df = _read_csv(path)
    ts_min, ts_max = _ts_bounds(df, "start_time")
    return DatasetReport(
        name="events",
        path=str(path),
        rows=int(len(df)),
        columns=list(df.columns),
        timestamp_min_utc=ts_min,
        timestamp_max_utc=ts_max,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate processed datasets (CSV) for basic sanity checks.")
    parser.add_argument("--processed-dir", default=None, help="Override processed dir (default: config.paths.processed_dir).")
    parser.add_argument("--json", dest="json_path", default=None, help="Write JSON report to this path.")
    args = parser.parse_args()

    config = get_config()
    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir

    reports: list[DatasetReport] = []

    segments = processed_dir / "segments.csv"
    if segments.exists():
        reports.append(report_segments(segments))
    else:
        reports.append(DatasetReport(name="segments", path=str(segments), rows=0, columns=[]))

    for obs_path in sorted(processed_dir.glob("observations_*min.csv")):
        reports.append(report_observations(obs_path))

    events = processed_dir / "events.csv"
    if events.exists():
        reports.append(report_events(events))

    for rep in reports:
        print(f"[{rep.name}] {rep.path}")
        print(f"  rows={rep.rows:,}")
        if rep.columns:
            print(f"  columns={', '.join(rep.columns)}")
        if rep.timestamp_min_utc or rep.timestamp_max_utc:
            print(f"  timestamp_utc={rep.timestamp_min_utc} .. {rep.timestamp_max_utc}")
        if rep.unique_segments is not None:
            print(f"  unique_segments={rep.unique_segments:,}")
        if rep.null_speed_rows is not None:
            print(f"  speed_null_rows={rep.null_speed_rows:,}")
        if rep.sentinel_speed_rows is not None:
            print(f"  speed_sentinel_rows={rep.sentinel_speed_rows:,}")
        if rep.outlier_speed_rows is not None:
            print(f"  speed_outlier_rows={rep.outlier_speed_rows:,}")

    if args.json_path:
        out = Path(args.json_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        payload = [asdict(r) for r in reports]
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote JSON: {out}")


if __name__ == "__main__":
    main()

