# TrafficPulse: Road Congestion & Incident Analytics (Taiwan)

## 1. Project Overview

TrafficPulse is a road-network analytics platform that monitors and analyzes road congestion patterns and incident signals using Taiwan transportation open data (TDX).

The system provides:
- Congestion and speed heatmaps over time
- Incident/event tracking and impact analysis (when available)
- Travel time reliability metrics for key corridors
- Exploratory and predictive insights (baseline first, optional ML later)

Outputs are delivered as:
1) reproducible data pipelines,
2) analytical dashboards,
3) a web app for interactive map-based exploration.

---

## 2. Core Use Cases

1. "Where are today's congestion hotspots, and how are they evolving hourly?"
2. "Which road segments have the worst reliability during peak hours?"
3. "After an incident, how long does congestion persist and how far does it spread?"
4. "What are recurring weekly congestion patterns per corridor?"

---

## 3. Data Sources

### 3.1 TDX Road & Traffic Data (Primary)
Potential relevant datasets:
- Vehicle Detector (VD) observations (speed, flow, occupancy per road segment)
- Road network / link metadata (segment identifiers, geometry if available)
- Traffic events / incidents signals (if available)
- Optional: CCTV / CMS / AVI-related metadata (availability varies by region)

### 3.2 Supporting Data (Optional)
- Administrative boundaries (GIS)
- Weather data (for correlation studies)
- Public holidays calendar

---

## 4. Analytical Framework

### 4.1 Spatiotemporal Modeling
- Segment-level time series aggregation (e.g., 5-min to 15-min to hourly)
- Weekday/weekend and peak/off-peak decomposition
- Corridor grouping and segment clustering

### 4.2 Reliability Metrics
Compute reliability indicators per segment/corridor:
- Average speed and variance
- Buffer Index / Planning Time Index (or simplified reliability proxies)
- Congestion frequency and duration

### 4.3 Incident Impact (If available)
- Impact radius (affected segment neighborhood)
- Recovery time (time to return to baseline speed)
- Event-driven anomaly detection (baseline z-score or rolling stats)

---

## 5. Web App Features

### 5.1 Map Dashboard
- Heatmap layer (speed/congestion)
- Segment selection: time series view + reliability summary
- Time slider: current vs last 24h vs weekly pattern

### 5.2 Ranking & Reports
- Top unreliable corridors
- Peak-hour worst segments
- Exportable report snapshots (CSV + charts)

---

## 6. Architecture

trafficpulse/
├─ data/
│ ├─ raw/
│ ├─ processed/
│ └─ external/ # GIS boundaries, corridor definitions
├─ src/
│ ├─ ingestion/
│ │ └─ tdx_traffic_client.py
│ ├─ preprocessing/
│ │ ├─ normalize.py
│ │ └─ aggregation.py
│ ├─ analytics/
│ │ ├─ reliability.py
│ │ ├─ anomalies.py
│ │ └─ incidents.py
│ ├─ visualization/
│ │ ├─ charts.py
│ │ └─ map_layers.py
│ ├─ api/
│ │ └─ app.py # FastAPI
│ └─ utils/
│ ├─ cache.py
│ └─ logging.py
├─ web/ # React (optional) or a simple frontend
├─ configs/
│ ├─ config.yaml
│ └─ secrets.env.example
├─ outputs/
│ ├─ figures/
│ └─ reports/
└─ PROJECT_DESCRIPTION.md


---

## 7. Tech Stack

Backend/Analysis:
- Python 3.10+
- pandas, numpy
- geopandas, shapely (optional)
- scikit-learn (optional)
- httpx/requests
- FastAPI + pydantic
- sqlite (cache) or duckdb

Frontend:
- A simple React map UI (Leaflet / MapLibre) OR a minimal HTML dashboard
- Plotly or ECharts for charts

---

## 8. Deliverables

- Segment-level dataset pipeline (reproducible)
- Reliability metrics module
- Interactive heatmap dashboard
- Basic anomaly detection
- Optional: baseline forecasting for recurring patterns

---

## 9. Roadmap

Phase 1 (MVP):
- TDX traffic ingestion + segment metadata
- Aggregation + basic heatmap
- Segment time series view
- Basic reliability metrics

Phase 2:
- Corridor-level reliability rankings
- Incident/anomaly detection and impact analysis
- Exportable reports

Phase 3:
- Predictive features (baseline patterns, holiday effects)
- Performance optimizations (caching, incremental updates)
