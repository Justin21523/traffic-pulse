from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest all optional external CSV sources (best-effort).")
    p.add_argument("--processed-dir", default=None)
    p.add_argument("--state-dir", default=None)
    return p.parse_args()


def _run_optional(argv: list[str]) -> None:
    subprocess.run(argv, check=False)


def main() -> None:
    args = parse_args()
    common: list[str] = []
    if args.processed_dir:
        common += ["--processed-dir", str(args.processed_dir)]
    if args.state_dir:
        common += ["--state-dir", str(args.state_dir)]

    _run_optional([sys.executable, "scripts/ingest_weather.py", *common])
    _run_optional([sys.executable, "scripts/ingest_roadworks.py", *common])
    _run_optional([sys.executable, "scripts/ingest_incidents_extra.py", *common])
    _run_optional([sys.executable, "scripts/ingest_event_calendar.py", *common])
    _run_optional([sys.executable, "scripts/enrich_segments.py", *common])


if __name__ == "__main__":
    main()

