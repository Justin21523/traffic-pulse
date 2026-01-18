from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
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
from trafficpulse.api.routes_ui import router as ui_router
from trafficpulse.api.middleware import (
    CacheConfig,
    RateLimitConfig,
    SimpleRateLimitMiddleware,
    TtlResponseCacheMiddleware,
)
from trafficpulse.logging_config import configure_logging
from trafficpulse.settings import get_config, project_root


def create_app() -> FastAPI:
    configure_logging()
    config = get_config()

    app = FastAPI(title="TrafficPulse API", version="0.1.0")

    @app.get("/healthz")
    def healthz() -> dict[str, bool]:
        return {"ok": True}

    # Guard expensive endpoints and smooth UI-driven reload spikes.
    if config.api.rate_limit.enabled:
        app.add_middleware(
            SimpleRateLimitMiddleware,
            config=RateLimitConfig(
                enabled=True,
                window_seconds=float(config.api.rate_limit.window_seconds),
                max_requests=int(config.api.rate_limit.max_requests),
                include_paths=tuple(config.api.rate_limit.include_paths),
            ),
        )
    if config.api.cache.enabled:
        app.add_middleware(
            TtlResponseCacheMiddleware,
            config=CacheConfig(
                enabled=True,
                ttl_seconds=float(config.api.cache.ttl_seconds),
                include_paths=tuple(config.api.cache.include_paths),
            ),
        )

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
    app.include_router(ui_router, tags=["ui"])

    @app.get("/web")
    def web_root_redirect() -> RedirectResponse:
        return RedirectResponse(url="/web/", status_code=307)

    web_dir = project_root() / "web"
    if web_dir.exists():
        # Serve the UI from `/` and also from `/web` to match common expectations
        # when the on-disk folder is named `web/`.
        app.mount("/web", StaticFiles(directory=str(web_dir), html=True), name="web")
        app.mount("/", StaticFiles(directory=str(web_dir), html=True), name="root")

    return app


app = create_app()
