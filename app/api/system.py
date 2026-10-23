"""Health, stats and read-only device listing."""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.models import Device
from app.schemas import DeviceOut

router = APIRouter(tags=["system"])
SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.get("/health")
async def health(request: Request, session: SessionDep) -> dict[str, Any]:
    db_ok = True
    try:
        await asyncio.wait_for(session.execute(text("SELECT 1")), timeout=2.0)
    except Exception:  # noqa: BLE001
        db_ok = False
    return {"status": "ok" if db_ok else "degraded", "database": db_ok, "instance": request.app.state.settings.instance_id}


@router.get("/api/stats")
async def stats(request: Request) -> dict[str, Any]:
    return {
        "instance": request.app.state.settings.instance_id,
        "ingest": request.app.state.pipeline.snapshot(),
        "websocket": request.app.state.connections.stats(),
    }


@router.get("/api/devices", response_model=list[DeviceOut])
async def list_devices(
    session: SessionDep,
    limit: int = Query(20_000, ge=1, le=50_000),
    bbox: str | None = Query(None, description="min_lon,min_lat,max_lon,max_lat"),
) -> list[Device]:
    """Last known position of every device (used by the UI to pre-populate the map)."""
    stmt = select(Device).order_by(Device.last_seen_at.desc()).limit(limit)
    if bbox:
        min_lon, min_lat, max_lon, max_lat = (float(v) for v in bbox.split(","))
        stmt = stmt.where(
            text("last_position && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)::geography")
        ).params(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat)
    return list((await session.scalars(stmt)).all())
