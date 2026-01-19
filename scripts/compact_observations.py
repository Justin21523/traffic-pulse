from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import os
from pathlib import Path

from trafficpulse.logging_config import configure_logging
from trafficpulse.quality.observations import clean_observations
from trafficpulse.settings import get_config
from trafficpulse.storage.datasets import (
    load_dataset,
    observations_csv_path,
    observations_parquet_path,
    save_parquet,
    save_csv,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Compact (clean + dedupe) a processed observations dataset in-place."
    )
    p.add_argument("--processed-dir", default=None, help="Override processed dir (default: config.paths.processed_dir).")
    p.add_argument("--parquet-dir", default=None, help="Override parquet dir (default: config.warehouse.parquet_dir).")
    p.add_argument("--minutes", type=int, required=True, help="Observations granularity minutes (e.g., 5, 15, 60).")
    p.add_argument("--no-parquet", action="store_true", help="Do not write Parquet output even if warehouse enabled.")
    return p.parse_args()


def _atomic_replace_csv(df, path: Path) -> None:  # type: ignore[no-untyped-def]
    tmp = path.with_suffix(path.suffix + ".tmp")
    save_csv(df, tmp)
    os.replace(tmp, path)


def main() -> None:
    args = parse_args()
    configure_logging()

    config = get_config()
    processed_dir = Path(args.processed_dir) if args.processed_dir else config.paths.processed_dir
    parquet_dir = (
        Path(args.parquet_dir)
        if args.parquet_dir
        else (processed_dir / "parquet" if args.processed_dir else config.warehouse.parquet_dir)
    )
    minutes = int(args.minutes)

    csv_path = observations_csv_path(processed_dir, minutes)
    parquet_path = observations_parquet_path(parquet_dir, minutes)

    df = load_dataset(csv_path, parquet_path)
    cleaned, stats = clean_observations(df)

    _atomic_replace_csv(cleaned, csv_path)
    print(f"[compact] wrote {csv_path} rows={len(cleaned):,}")
    print(
        f"[compact] stats input={stats.input_rows:,} output={stats.output_rows:,} "
        f"dropped_invalid_speed={stats.dropped_invalid_speed:,} dropped_invalid_timestamp={stats.dropped_invalid_timestamp:,} "
        f"dropped_duplicates={stats.dropped_duplicates:,}"
    )

    if config.warehouse.enabled and not args.no_parquet:
        save_parquet(cleaned, parquet_path)
        print(f"[compact] wrote {parquet_path}")


if __name__ == "__main__":
    main()

