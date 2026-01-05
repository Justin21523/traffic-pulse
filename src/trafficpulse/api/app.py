from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from trafficpulse.api.routes_rankings import router as rankings_router
from trafficpulse.api.routes_corridors import router as corridors_router
from trafficpulse.api.routes_anomalies import router as anomalies_router
from trafficpulse.api.routes_exports import router as exports_router
from trafficpulse.api.routes_events import router as events_router
from trafficpulse.api.routes_event_impact import router as event_impact_router
from trafficpulse.api.routes_map import router as map_router
from trafficpulse.api.routes_segments import router as segments_router
from trafficpulse.api.routes_timeseries import router as timeseries_router
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config, project_root


def create_app() -> FastAPI:
    configure_logging()
    config = get_config()

    app = FastAPI(title="TrafficPulse API", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.api.cors.allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(segments_router, tags=["segments"])
    app.include_router(timeseries_router, tags=["timeseries"])
    app.include_router(rankings_router, tags=["rankings"])
    app.include_router(corridors_router, tags=["corridors"])
    app.include_router(anomalies_router, tags=["anomalies"])
    app.include_router(exports_router, tags=["exports"])
    app.include_router(events_router, tags=["events"])
    app.include_router(event_impact_router, tags=["events"])
    app.include_router(map_router, tags=["map"])

    web_dir = project_root() / "web"
    if web_dir.exists():
        app.mount("/", StaticFiles(directory=str(web_dir), html=True), name="web")

    return app


app = create_app()
