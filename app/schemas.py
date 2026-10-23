"""Pydantic request/response models."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

DeviceId = Field(pattern=r"^[A-Za-z0-9_.:@-]{1,64}$")
Latitude = Field(ge=-90, le=90)
Longitude = Field(ge=-180, le=180)


class LocationIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    device_id: str = DeviceId
    latitude: float = Latitude
    longitude: float = Longitude
    timestamp: datetime | None = None


class IngestAccepted(BaseModel):
    accepted: int


class GeozoneBase(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    latitude: float = Latitude
    longitude: float = Longitude
    radius_m: float = Field(gt=0, le=1_000_000)
    is_active: bool = True


class GeozoneCreate(GeozoneBase):
    pass


class GeozoneUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    radius_m: float | None = Field(default=None, gt=0, le=1_000_000)
    is_active: bool | None = None


class GeozoneOut(GeozoneBase):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: str
    created_at: datetime
    updated_at: datetime


class DeviceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    device_id: str
    latitude: float
    longitude: float
    last_seen_at: datetime


class AlertOut(BaseModel):
    event: Literal["enter", "inside", "exit"]
    zone_id: uuid.UUID
    zone_name: str
    device_id: str
    latitude: float
    longitude: float
    distance_m: float
    timestamp: datetime
