"""Geozone CRUD. Every query is scoped to the authenticated user."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user_id
from app.db import get_session
from app.models import Geozone
from app.schemas import GeozoneCreate, GeozoneOut, GeozoneUpdate

router = APIRouter(prefix="/api/geozones", tags=["geozones"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]
UserDep = Annotated[str, Depends(get_current_user_id)]


async def _get_owned(session: AsyncSession, user_id: str, zone_id: uuid.UUID) -> Geozone:
    zone = await session.get(Geozone, zone_id)
    # A zone that belongs to someone else is indistinguishable from a missing one.
    if zone is None or zone.user_id != user_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Geozone not found")
    return zone


@router.post("", response_model=GeozoneOut, status_code=status.HTTP_201_CREATED)
async def create_geozone(payload: GeozoneCreate, session: SessionDep, user_id: UserDep) -> Geozone:
    zone = Geozone(user_id=user_id, **payload.model_dump())
    session.add(zone)
    await session.commit()
    await session.refresh(zone)
    return zone


@router.get("", response_model=list[GeozoneOut])
async def list_geozones(
    session: SessionDep,
    user_id: UserDep,
    active_only: bool = Query(False, description="Return only active zones"),
) -> list[Geozone]:
    stmt = select(Geozone).where(Geozone.user_id == user_id).order_by(Geozone.created_at)
    if active_only:
        stmt = stmt.where(Geozone.is_active.is_(True))
    return list((await session.scalars(stmt)).all())


@router.get("/{zone_id}", response_model=GeozoneOut)
async def get_geozone(zone_id: uuid.UUID, session: SessionDep, user_id: UserDep) -> Geozone:
    return await _get_owned(session, user_id, zone_id)


@router.patch("/{zone_id}", response_model=GeozoneOut)
@router.put("/{zone_id}", response_model=GeozoneOut)
async def update_geozone(
    zone_id: uuid.UUID, payload: GeozoneUpdate, session: SessionDep, user_id: UserDep
) -> Geozone:
    zone = await _get_owned(session, user_id, zone_id)
    changes = payload.model_dump(exclude_unset=True)
    if not changes:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail="No fields to update")
    for key, value in changes.items():
        setattr(zone, key, value)
    await session.commit()
    await session.refresh(zone)
    return zone


@router.delete("/{zone_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_geozone(zone_id: uuid.UUID, session: SessionDep, user_id: UserDep) -> Response:
    zone = await _get_owned(session, user_id, zone_id)
    await session.delete(zone)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
