"""Core spatial logic against a real PostGIS database."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.models import Device, Geozone, ZonePresence
from app.services import spatial
from app.services.broker import CHANNEL_ALERTS, CHANNEL_LOCATIONS, LocalBroker
from app.services.pipeline import IngestPipeline
from app.services.types import LocationUpdate

pytestmark = pytest.mark.db

KYIV = (50.4501, 30.5234)
T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def _pt(device: str, lat: float, lon: float, offset_s: int = 0) -> LocationUpdate:
    return LocationUpdate(device, lat, lon, T0 + timedelta(seconds=offset_s))


async def _zone(session: AsyncSession, user_id: str, radius_m: float, name: str = "z", **kw: object) -> Geozone:
    zone = Geozone(user_id=user_id, name=name, latitude=KYIV[0], longitude=KYIV[1], radius_m=radius_m, **kw)
    session.add(zone)
    await session.commit()
    return zone


async def test_enter_inside_exit_sequence(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as s:
        zone = await _zone(s, "alice", 500)

    async with session_factory() as s, s.begin():
        alerts = await spatial.evaluate_zones(s, [_pt("d1", KYIV[0] + 0.001, KYIV[1])])  # ~111 m north
    assert [(a.event, a.user_id, a.zone_id) for a in alerts] == [("enter", "alice", zone.id)]
    assert 100 < alerts[0].distance_m < 120

    async with session_factory() as s, s.begin():
        alerts = await spatial.evaluate_zones(s, [_pt("d1", KYIV[0] + 0.002, KYIV[1], 3)])
    assert [a.event for a in alerts] == ["inside"]

    async with session_factory() as s, s.begin():
        alerts = await spatial.evaluate_zones(s, [_pt("d1", KYIV[0] + 0.02, KYIV[1], 6)])  # ~2.2 km: outside
    assert [a.event for a in alerts] == ["exit"]

    async with session_factory() as s, s.begin():
        alerts = await spatial.evaluate_zones(s, [_pt("d1", KYIV[0] + 0.02, KYIV[1], 9)])
    assert alerts == []  # still outside, nothing to report

    async with session_factory() as s:
        assert (await s.scalars(select(ZonePresence))).all() == []


async def test_radius_boundary_uses_spheroidal_distance(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as s:
        await _zone(s, "alice", 1000)
    # 1000 m is ~0.00898 deg of latitude; 0.0089 deg is just inside, 0.0091 deg just outside.
    async with session_factory() as s, s.begin():
        inside = await spatial.evaluate_zones(s, [_pt("near", KYIV[0] + 0.0089, KYIV[1])])
        outside = await spatial.evaluate_zones(s, [_pt("far", KYIV[0] + 0.0091, KYIV[1])])
    assert [a.device_id for a in inside] == ["near"]
    assert outside == []


async def test_zones_are_isolated_per_user(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as s:
        za = await _zone(s, "alice", 300, "alice-zone")
        zb = await _zone(s, "bob", 300, "bob-zone")
        await _zone(s, "carol", 300, "carol-inactive", is_active=False)

    async with session_factory() as s, s.begin():
        alerts = await spatial.evaluate_zones(s, [_pt("d1", *KYIV)])

    by_user = {a.user_id: a for a in alerts}
    assert set(by_user) == {"alice", "bob"}  # carol's zone is inactive
    assert by_user["alice"].zone_id == za.id and by_user["bob"].zone_id == zb.id


async def test_only_latest_point_per_device_is_evaluated(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as s:
        await _zone(s, "alice", 200)
    batch = [_pt("d1", *KYIV, 0), _pt("d1", KYIV[0] + 0.05, KYIV[1], 5)]  # inside then far away
    async with session_factory() as s, s.begin():
        alerts = await spatial.evaluate_zones(s, spatial.dedupe_latest(batch))
    assert alerts == []


async def test_bounds_index_is_used(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as s:
        for i in range(50):
            await _zone(s, f"u{i}", 100 + i)
        await s.execute(text("SET enable_seqscan = off"))  # table is tiny; force the planner to show the index
        plan = "\n".join(r[0] for r in await s.execute(text(
            "EXPLAIN SELECT id FROM geozones g "
            "WHERE g.bounds && ST_SetSRID(ST_MakePoint(30.52, 50.45), 4326)::geography"
        )))
    assert "ix_geozones_bounds" in plan


async def test_pipeline_persists_and_publishes(session_factory: async_sessionmaker[AsyncSession]) -> None:
    published: list[tuple[str, bytes]] = []

    class CapturingBroker(LocalBroker):
        async def publish(self, channel: str, payload: bytes) -> None:
            published.append((channel, payload))

    settings = Settings(ingest_workers=1, alert_repeat_inside=False)
    pipeline = IngestPipeline(settings, session_factory, CapturingBroker())

    async with session_factory() as s:
        await _zone(s, "alice", 500)

    alerts = await pipeline.process_batch([_pt("d1", *KYIV), _pt("d2", 0.0, 0.0)])
    assert [(a.event, a.device_id) for a in alerts] == [("enter", "d1")]
    assert await pipeline.process_batch([_pt("d1", *KYIV, 3)]) == []  # "inside" suppressed by settings

    channels = [c for c, _ in published]
    assert channels == [CHANNEL_LOCATIONS, CHANNEL_ALERTS, CHANNEL_LOCATIONS]

    async with session_factory() as s:
        devices = {d.device_id: d for d in (await s.scalars(select(Device))).all()}
        history = await s.scalar(text("SELECT count(*) FROM device_locations"))
        assert set(devices) == {"d1", "d2"}
        assert devices["d1"].last_seen_at == T0 + timedelta(seconds=3)
        assert history == 3
        # Spatial lookup on the devices table works on the generated geography column.
        near = await s.scalar(
            text("SELECT count(*) FROM devices WHERE ST_DWithin(last_position, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography, 50)"),
            {"lon": KYIV[1], "lat": KYIV[0]},
        )
        assert near == 1


async def test_deleting_zone_cascades_presence(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as s:
        zone = await _zone(s, "alice", 500)
    async with session_factory() as s, s.begin():
        await spatial.evaluate_zones(s, [_pt("d1", *KYIV)])
    async with session_factory() as s:
        zone = await s.get(Geozone, zone.id)
        assert zone is not None
        await s.delete(zone)
        await s.commit()
        assert (await s.scalars(select(ZonePresence))).all() == []
    # Fresh uuid with no zone must not blow up either.
    async with session_factory() as s, s.begin():
        assert await spatial.evaluate_zones(s, [_pt(uuid.uuid4().hex[:8], *KYIV)]) == []
