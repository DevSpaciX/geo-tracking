"""Set-based PostGIS statements used by the ingestion pipeline.

Every statement takes a whole batch as PostgreSQL arrays (``unnest``), so a batch
of 2 000 updates costs three round-trips, not 6 000. Spatial math (distance,
containment) happens entirely inside PostGIS.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime

from sqlalchemy import ARRAY, DateTime, Float, Text, bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.types import Alert, LocationUpdate

_ARRAY_PARAMS = (
    bindparam("device_ids", type_=ARRAY(Text)),
    bindparam("lons", type_=ARRAY(Float)),
    bindparam("lats", type_=ARRAY(Float)),
    bindparam("tss", type_=ARRAY(DateTime(timezone=True))),
)

# unnest() of the four parallel arrays -> one row per update.
_POINTS_SRC = """
    unnest(
        CAST(:device_ids AS text[]),
        CAST(:lons AS float8[]),
        CAST(:lats AS float8[]),
        CAST(:tss AS timestamptz[])
    ) AS t(device_id, lon, lat, ts)
"""

INSERT_HISTORY = text(
    f"""
    INSERT INTO device_locations (device_id, latitude, longitude, recorded_at)
    SELECT device_id, lat, lon, ts FROM {_POINTS_SRC}
    """
).bindparams(*_ARRAY_PARAMS)

UPSERT_DEVICES = text(
    f"""
    INSERT INTO devices (device_id, latitude, longitude, last_seen_at)
    SELECT device_id, lat, lon, ts FROM {_POINTS_SRC}
    ON CONFLICT (device_id) DO UPDATE
        SET latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            last_seen_at = EXCLUDED.last_seen_at,
            updated_at = now()
        WHERE devices.last_seen_at <= EXCLUDED.last_seen_at
    """
).bindparams(*_ARRAY_PARAMS)

# One statement that (1) finds every (zone, device) pair where the device's latest
# point is inside the zone, (2) records presence so we know whether it just
# entered, (3) removes stale presence rows for devices that are no longer inside,
# and (4) returns enter / inside / exit events with the owning user.
#
# Index usage: ``g.bounds && p.pt`` is a cheap bounding-box probe on the GiST index
# over the pre-buffered zone footprint; ``ST_DWithin`` on the geography centre then
# gives the exact spheroidal distance check for the surviving candidates.
EVALUATE_ZONES = text(
    f"""
    WITH pts AS (
        SELECT device_id, lon, lat, ts,
               ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS pt
        FROM {_POINTS_SRC}
    ),
    hits AS (
        SELECT g.id AS zone_id, g.user_id, g.name AS zone_name,
               p.device_id, p.lat, p.lon, p.ts, p.pt,
               ST_Distance(g.center, p.pt) AS distance_m
        FROM pts p
        JOIN geozones g
          ON g.is_active
         AND g.bounds && p.pt
         AND ST_DWithin(g.center, p.pt, g.radius_m)
    ),
    upserted AS (
        INSERT INTO zone_presence (zone_id, device_id, entered_at, last_seen_at)
        SELECT zone_id, device_id, ts, ts FROM hits
        ON CONFLICT (zone_id, device_id) DO UPDATE
            SET last_seen_at = EXCLUDED.last_seen_at
        RETURNING zone_id, device_id, (xmax = 0) AS entered
    ),
    exited AS (
        DELETE FROM zone_presence zp
        USING pts p
        WHERE zp.device_id = p.device_id
          AND NOT EXISTS (
              SELECT 1 FROM hits h WHERE h.zone_id = zp.zone_id AND h.device_id = zp.device_id
          )
        RETURNING zp.zone_id, zp.device_id
    )
    SELECT CASE WHEN u.entered THEN 'enter' ELSE 'inside' END AS event,
           h.zone_id, h.user_id, h.zone_name, h.device_id,
           h.lat, h.lon, h.distance_m, h.ts
    FROM hits h
    JOIN upserted u ON u.zone_id = h.zone_id AND u.device_id = h.device_id
    UNION ALL
    SELECT 'exit', e.zone_id, g.user_id, g.name, e.device_id,
           p.lat, p.lon, ST_Distance(g.center, p.pt), p.ts
    FROM exited e
    JOIN geozones g ON g.id = e.zone_id
    JOIN pts p ON p.device_id = e.device_id
    """
).bindparams(*_ARRAY_PARAMS)


def dedupe_latest(points: Iterable[LocationUpdate]) -> list[LocationUpdate]:
    """Keep only the newest update per device (zone state is evaluated on the latest fix)."""
    latest: dict[str, LocationUpdate] = {}
    for p in points:
        cur = latest.get(p.device_id)
        if cur is None or p.timestamp >= cur.timestamp:
            latest[p.device_id] = p
    return list(latest.values())


def _arrays(points: Sequence[LocationUpdate]) -> dict[str, list[str] | list[float] | list[datetime]]:
    return {
        "device_ids": [p.device_id for p in points],
        "lons": [p.longitude for p in points],
        "lats": [p.latitude for p in points],
        "tss": [p.timestamp for p in points],
    }


async def insert_history(session: AsyncSession, points: Sequence[LocationUpdate]) -> None:
    if points:
        await session.execute(INSERT_HISTORY, _arrays(points))


async def upsert_devices(session: AsyncSession, latest: Sequence[LocationUpdate]) -> None:
    if latest:
        await session.execute(UPSERT_DEVICES, _arrays(latest))


async def evaluate_zones(session: AsyncSession, latest: Sequence[LocationUpdate]) -> list[Alert]:
    """Run the zone evaluation for the latest point of each device and return alerts."""
    if not latest:
        return []
    result = await session.execute(EVALUATE_ZONES, _arrays(latest))
    return [
        Alert(
            event=row.event,
            zone_id=row.zone_id,
            user_id=row.user_id,
            zone_name=row.zone_name,
            device_id=row.device_id,
            latitude=row.lat,
            longitude=row.lon,
            distance_m=row.distance_m,
            timestamp=row.ts,
        )
        for row in result
    ]
