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
cp configs/corridors.example.csv configs/corridors.csv
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

## Reliability Rankings (Phase 1)

Compute basic reliability metrics (mean speed, speed variability, congestion frequency) and a weighted ranking score:

```bash
python scripts/build_reliability_rankings.py --limit 200
```

## Corridors (Phase 2)

Corridors are defined as curated lists of segments (VD IDs).

1) Edit `configs/corridors.csv` (copied from `configs/corridors.example.csv`).
2) Compute corridor rankings (optional offline output):

```bash
python scripts/build_corridor_rankings.py --limit 200
```

API endpoints:

- `GET /corridors`
- `GET /rankings/reliability/corridors`

## Anomalies (Phase 2)

Explainable anomaly detection uses a rolling z-score baseline on speed (config-driven).

Endpoints:

- `GET /anomalies?segment_id=...&start=...&end=...&minutes=...`
- `GET /anomalies/events?segment_id=...&start=...&end=...&minutes=...`
- `GET /anomalies/corridors?corridor_id=...&start=...&end=...&minutes=...`
- `GET /anomalies/corridors/events?corridor_id=...&start=...&end=...&minutes=...`

Offline helper:

```bash
python scripts/detect_anomalies.py --segment-id <VDID> --start 2026-01-01T00:00:00+08:00 --end 2026-01-01T06:00:00+08:00
```

## Reports and Exports (Phase 2)

Export a reproducible snapshot (CSV + `summary.json`) to `outputs/reports/`:

```bash
python scripts/export_report.py --include-corridors --limit 200
```

CSV export endpoints:

- `GET /exports/reliability/segments.csv`
- `GET /exports/reliability/corridors.csv`

## Run the API (Phase 1)

```bash
python scripts/run_api.py
```

## Open the MVP Dashboard (Phase 1)

If you run the API, the static dashboard is served automatically:

- `http://localhost:8000/`

Alternatively, serve `web/` separately and point it to your API:

```bash
python -m http.server 5173 --directory web
```

Then open:

- `http://localhost:5173/?api=http://localhost:8000`

## Notes

- All implementation, docstrings, and documentation in this repo are written in English.
- Local data under `data/` is ignored by default (except `.gitkeep` placeholders).
