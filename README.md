# TrafficPulse

TrafficPulse is a road congestion analytics and visualization web app for Taiwan, powered by TDX (Transport Data eXchange).

This repository is being developed in phases. **Phase 1 (MVP)** focuses on:
- Reproducible ingestion → preprocessing → analytics pipeline (Python)
- A FastAPI backend for segment metadata, time series, and reliability rankings
- A minimal map-based frontend for interactive exploration

## Repository Layout (MVP)

```text
configs/          # YAML configs (copy examples into real configs)
data/             # Local data (raw/processed/cache)
scripts/          # CLI helpers (build dataset, run API)
src/trafficpulse/ # Python package (ingestion/preprocessing/analytics/api)
web/              # Minimal static frontend (map + chart)
```

## Quickstart (Phase 1)

1) Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Create local config files:

```bash
cp configs/config.example.yaml configs/config.yaml
cp .env.example .env
```

3) Fill in `.env` with your TDX credentials (`TDX_CLIENT_ID`, `TDX_CLIENT_SECRET`).

## Build a VD Dataset (Phase 1)

Install the package in editable mode so scripts can import `trafficpulse`:

```bash
pip install -e .
python scripts/build_dataset.py --start 2026-01-01T00:00:00+08:00 --end 2026-01-01T03:00:00+08:00 --cities Taipei
```

## Aggregate Observations (Phase 1)

Convert `5-min` observations into `15-min` or `hourly` series (config-driven):

```bash
python scripts/aggregate_observations.py
```

## Notes

- All implementation, docstrings, and documentation in this repo are written in English.
- Local data under `data/` is ignored by default (except `.gitkeep` placeholders).
