from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from trafficpulse.api.routes_rankings import router as rankings_router
from trafficpulse.api.routes_segments import router as segments_router
from trafficpulse.api.routes_timeseries import router as timeseries_router
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config


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

    return app


app = create_app()

