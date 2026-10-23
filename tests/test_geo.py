"""Pure helpers: distance, viewport filter, batch de-duplication, generator motion."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from app.services.geo import BBox, haversine_m
from app.services.spatial import dedupe_latest
from app.services.types import LocationUpdate
from generator import Device, make_devices


def test_haversine_known_distance() -> None:
    # Kyiv centre -> Boryspil airport is roughly 29 km.
    d = haversine_m(50.4501, 30.5234, 50.3450, 30.8947)
    assert 28_000 < d < 30_000


def test_haversine_zero() -> None:
    assert haversine_m(10, 20, 10, 20) == 0


def test_bbox_contains_and_rejects() -> None:
    box = BBox.parse([30.0, 50.0, 31.0, 51.0])
    assert box.contains(50.5, 30.5)
    assert not box.contains(49.9, 30.5)
    assert not box.contains(50.5, 31.1)


def test_bbox_crossing_antimeridian() -> None:
    box = BBox.parse([179.0, -10.0, -179.0, 10.0])
    assert box.contains(0.0, 179.5)
    assert box.contains(0.0, -179.5)
    assert not box.contains(0.0, 0.0)


@pytest.mark.parametrize("raw", [None, [1, 2, 3], [0, 95, 1, 96], "x"])
def test_bbox_invalid(raw: object) -> None:
    with pytest.raises(ValueError):
        BBox.parse(raw)


def test_dedupe_latest_keeps_newest_per_device() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    pts = [
        LocationUpdate("a", 1, 1, t0),
        LocationUpdate("b", 2, 2, t0),
        LocationUpdate("a", 3, 3, t0 + timedelta(seconds=5)),
        LocationUpdate("a", 4, 4, t0 + timedelta(seconds=1)),  # older than the previous "a"
    ]
    latest = {p.device_id: p for p in dedupe_latest(pts)}
    assert set(latest) == {"a", "b"}
    assert latest["a"].latitude == 3


def test_generator_devices_drift_realistically() -> None:
    rng = random.Random(1)
    devices = make_devices(50, 50.45, 30.52, 0.1, rng)
    before = [(d.lat, d.lon) for d in devices]
    for d in devices:
        d.step(3.0, rng)
    moved = [haversine_m(b[0], b[1], d.lat, d.lon) for b, d in zip(before, devices, strict=True)]
    assert all(0 < m < 150 for m in moved), "a 3 s tick at <= 40 m/s must move each device a little, not teleport it"
    assert len({(d.lat, d.lon) for d in devices}) == 50


def test_generator_device_stays_in_bounds() -> None:
    rng = random.Random(2)
    d = Device("x", 84.9, 179.99, heading=0.0, speed_mps=40.0)
    for _ in range(100):
        d.step(10.0, rng)
        assert -85 <= d.lat <= 85 and -180 <= d.lon <= 180
