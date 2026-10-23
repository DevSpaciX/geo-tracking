"""HTTP/WebSocket API tests against a real PostGIS database (end-to-end through the app)."""

from __future__ import annotations

import time

import orjson
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

pytestmark = pytest.mark.db

KYIV = {"latitude": 50.4501, "longitude": 30.5234}


@pytest.fixture
def client(migrated_database: str, engine) -> TestClient:  # noqa: ANN001 - engine fixture truncates tables
    settings = Settings(database_url=migrated_database, redis_url=None, ingest_flush_interval_ms=50, alert_repeat_inside=True)
    with TestClient(create_app(settings)) as client:
        yield client


def test_geozone_crud_is_scoped_to_user(client: TestClient) -> None:
    alice = {"X-User-Id": "alice"}
    bob = {"X-User-Id": "bob"}

    assert client.post("/api/geozones", json={"name": "x", "radius_m": 10, **KYIV}).status_code == 401
    assert client.post("/api/geozones", headers=alice, json={"name": "x", "radius_m": -1, **KYIV}).status_code == 422

    created = client.post("/api/geozones", headers=alice, json={"name": "Office", "radius_m": 250, **KYIV})
    assert created.status_code == 201
    zone = created.json()
    assert zone["user_id"] == "alice" and zone["is_active"] is True

    assert [z["id"] for z in client.get("/api/geozones", headers=alice).json()] == [zone["id"]]
    assert client.get("/api/geozones", headers=bob).json() == []
    assert client.get(f"/api/geozones/{zone['id']}", headers=bob).status_code == 404
    assert client.patch(f"/api/geozones/{zone['id']}", headers=bob, json={"name": "hack"}).status_code == 404

    updated = client.patch(f"/api/geozones/{zone['id']}", headers=alice, json={"radius_m": 300, "is_active": False})
    assert updated.status_code == 200 and updated.json()["radius_m"] == 300 and updated.json()["is_active"] is False
    assert client.get("/api/geozones?active_only=true", headers=alice).json() == []

    assert client.delete(f"/api/geozones/{zone['id']}", headers=bob).status_code == 404
    assert client.delete(f"/api/geozones/{zone['id']}", headers=alice).status_code == 204
    assert client.get("/api/geozones", headers=alice).json() == []


def test_ingest_validation(client: TestClient) -> None:
    assert client.post("/api/locations", json={"device_id": "d1", "latitude": 95, "longitude": 0}).status_code == 422
    assert client.post("/api/locations", json={"device_id": "bad id!", "latitude": 0, "longitude": 0}).status_code == 422
    ok = client.post("/api/locations", json=[{"device_id": "d1", "latitude": 1, "longitude": 2}, {"device_id": "d2", "latitude": 3, "longitude": 4}])
    assert ok.status_code == 202 and ok.json() == {"accepted": 2}


def _collect(ws, kinds: set[str], max_messages: int = 50) -> dict[str, dict]:  # noqa: ANN001
    """Read frames until one of each requested kind has been seen (alerts are prioritised
    over locations on the wire, so the order is not fixed)."""
    seen: dict[str, dict] = {}
    for _ in range(max_messages):
        msg = ws.receive_json()
        if msg["type"] in kinds and msg["type"] not in seen:
            seen[msg["type"]] = msg
        if kinds <= set(seen):
            return seen
    raise AssertionError(f"did not receive {kinds - set(seen)} within {max_messages} frames")


def _recv_until(ws, kind: str) -> dict:  # noqa: ANN001
    return _collect(ws, {kind})[kind]


def test_live_stream_and_alerts_to_all_sessions(client: TestClient) -> None:
    alice = {"X-User-Id": "alice"}
    client.post("/api/geozones", headers=alice, json={"name": "Office", "radius_m": 300, **KYIV})

    with client.websocket_connect("/ws/live?user_id=alice") as laptop, client.websocket_connect(
        "/ws/live", headers=alice
    ) as phone, client.websocket_connect("/ws/live?user_id=bob") as bob, client.websocket_connect("/ws/ingest") as device:
        for ws in (laptop, phone, bob):
            assert ws.receive_json()["type"] == "hello"

        device.send_text(orjson.dumps({"device_id": "truck-1", **KYIV}).decode())
        assert device.receive_json() == {"type": "ack", "accepted": 1}

        assert _recv_until(bob, "locations")["items"][0]["device_id"] == "truck-1"
        for ws in (laptop, phone):
            frames = _collect(ws, {"locations", "alerts"})
            assert frames["locations"]["items"][0]["device_id"] == "truck-1"
            assert frames["alerts"]["items"][0]["event"] == "enter"
            assert frames["alerts"]["items"][0]["zone_name"] == "Office"

        # Bob has no zones: he must never receive alice's alert.
        device.send_text(orjson.dumps([{"device_id": "truck-1", "latitude": 50.4505, "longitude": 30.5234}]).decode())
        assert device.receive_json()["type"] == "ack"
        assert _recv_until(bob, "locations")["items"][0]["device_id"] == "truck-1"
        assert _recv_until(laptop, "alerts")["items"][0]["event"] == "inside"
        assert _recv_until(phone, "alerts")["items"][0]["event"] == "inside"

        # Viewport filter: devices outside the bbox are not streamed.
        bob.send_json({"type": "set_viewport", "bbox": [0, 0, 1, 1]})
        assert _recv_until(bob, "viewport")["bbox"] == [0, 0, 1, 1]
        device.send_text(orjson.dumps({"device_id": "truck-1", "latitude": 50.4506, "longitude": 30.5234}).decode())
        device.receive_json()
        device.send_text(orjson.dumps({"device_id": "boat-1", "latitude": 0.5, "longitude": 0.5}).decode())
        device.receive_json()
        seen = _recv_until(bob, "locations")
        assert {i["device_id"] for i in seen["items"]} == {"boat-1"}

    stats = client.get("/api/stats").json()
    assert stats["ingest"]["received"] >= 4
    assert stats["websocket"]["connections"] == 0


def test_ws_live_requires_user(client: TestClient) -> None:
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws/live") as ws:
            ws.receive_json()


def test_health_and_devices(client: TestClient) -> None:
    assert client.get("/health").json()["status"] == "ok"
    client.post("/api/locations", json={"device_id": "d9", "latitude": 50.45, "longitude": 30.52})
    for _ in range(50):
        if client.get("/api/devices").json():
            break
        time.sleep(0.05)
    devices = client.get("/api/devices?bbox=30,50,31,51").json()
    assert [d["device_id"] for d in devices] == ["d9"]
    assert client.get("/api/devices?bbox=0,0,1,1").json() == []
