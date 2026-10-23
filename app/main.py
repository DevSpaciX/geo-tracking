"""FastAPI application factory and lifespan wiring."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import orjson
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api import geozones, ingest, system, ws
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.services.broker import CHANNEL_ALERTS, CHANNEL_LOCATIONS, make_broker
from app.services.connections import ConnectionManager
from app.services.pipeline import IngestPipeline

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
log = logging.getLogger(__name__)


def _configure_logging(level: str) -> None:
    logging.basicConfig(level=level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    _configure_logging(settings.log_level)

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    connections = ConnectionManager(
        alert_backlog=settings.ws_alert_backlog, location_backlog=settings.ws_location_backlog
    )
    broker = make_broker(settings.redis_url)
    pipeline = IngestPipeline(settings, session_factory, broker)

    async def dispatch(channel: str, payload: bytes) -> None:
        # Runs on every process for every published batch (local or via Redis).
        if channel == CHANNEL_LOCATIONS:
            connections.broadcast_locations(orjson.loads(payload))
        elif channel == CHANNEL_ALERTS:
            msg = orjson.loads(payload)
            connections.send_alerts(msg["user_id"], msg["alerts"])

    await broker.start(dispatch)
    await pipeline.start()

    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.connections = connections
    app.state.broker = broker
    app.state.pipeline = pipeline
    log.info("instance %s ready (broker=%s)", settings.instance_id, type(broker).__name__)
    try:
        yield
    finally:
        await pipeline.stop()
        await connections.close_all()
        await broker.stop()
        await engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(
        title="Geo-Tracking & Alerting Service",
        version="1.0.0",
        description="Real-time device tracking with per-user circular geozones and WebSocket alerts.",
        lifespan=lifespan,
    )
    app.state.settings = settings or get_settings()

    app.include_router(system.router)
    app.include_router(geozones.router)
    app.include_router(ingest.router)
    app.include_router(ws.router)

    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

        @app.get("/", include_in_schema=False)
        async def index() -> FileResponse:
            return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
