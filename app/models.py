"""SQLAlchemy ORM models.

Design note: latitude/longitude are stored as plain floats (the API
representation) and the PostGIS ``geography`` columns are *generated* from them.
That keeps the ORM free of WKB handling while every spatial query still runs on
indexed geography types.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from geoalchemy2 import Geography
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Computed,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    Index,
    String,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Expression used by generated geography columns.
POINT_EXPR = "ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography"


class Base(DeclarativeBase):
    pass


class Geozone(Base):
    __tablename__ = "geozones"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    radius_m: Mapped[float] = mapped_column(Float, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Exact centre used by ST_DWithin.
    center = mapped_column(Geography("POINT", srid=4326, spatial_index=False), Computed(POINT_EXPR, persisted=True))
    # Pre-computed circle footprint: lets the GiST index pre-filter candidates even
    # though every zone has its own radius (ST_DWithin with a per-row radius cannot
    # use an index on its own).
    bounds = mapped_column(
        Geography(srid=4326, spatial_index=False), Computed(f"ST_Buffer({POINT_EXPR}, radius_m)", persisted=True)
    )

    __table_args__ = (
        CheckConstraint("radius_m > 0", name="ck_geozones_radius_positive"),
        CheckConstraint("latitude BETWEEN -90 AND 90", name="ck_geozones_latitude"),
        CheckConstraint("longitude BETWEEN -180 AND 180", name="ck_geozones_longitude"),
        Index("ix_geozones_user_id_is_active", "user_id", "is_active"),
        Index("ix_geozones_bounds", "bounds", postgresql_using="gist"),
    )


class Device(Base):
    """Last known position of every device (one row per device)."""

    __tablename__ = "devices"

    device_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    last_position = mapped_column(
        Geography("POINT", srid=4326, spatial_index=False), Computed(POINT_EXPR, persisted=True)
    )

    __table_args__ = (Index("ix_devices_last_position", "last_position", postgresql_using="gist"),)


class DeviceLocation(Base):
    """Append-only location history (high write volume, no spatial index on purpose)."""

    __tablename__ = "device_locations"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    device_id: Mapped[str] = mapped_column(String(64), nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    position = mapped_column(Geography("POINT", srid=4326, spatial_index=False), Computed(POINT_EXPR, persisted=True))

    __table_args__ = (Index("ix_device_locations_device_id_recorded_at", "device_id", "recorded_at"),)


class ZonePresence(Base):
    """Which devices are currently inside which zones (drives enter/exit events)."""

    __tablename__ = "zone_presence"

    zone_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("geozones.id", ondelete="CASCADE"), primary_key=True
    )
    device_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_zone_presence_device_id", "device_id"),)
