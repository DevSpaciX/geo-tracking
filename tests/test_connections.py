"""ConnectionManager: multi-session routing, per-user isolation, back-pressure."""

from __future__ import annotations

import asyncio

import orjson
import pytest

from app.services.connections import ConnectionManager
from app.services.geo import BBox


class FakeWebSocket:
    def __init__(self, *, slow: bool = False) -> None:
        self.sent: list[dict] = []
        self.slow = slow
        self.accepted = False

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, text: str) -> None:
        if self.slow:
            await asyncio.sleep(0.05)
        self.sent.append(orjson.loads(text))

    async def close(self) -> None:
        pass

    def messages(self, kind: str) -> list[dict]:
        return [m for m in self.sent if m["type"] == kind]


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def test_alerts_reach_every_session_of_the_user_only() -> None:
    manager = ConnectionManager()
    laptop, phone, other = FakeWebSocket(), FakeWebSocket(), FakeWebSocket()
    c1 = await manager.connect(laptop, "alice")  # type: ignore[arg-type]
    c2 = await manager.connect(phone, "alice")  # type: ignore[arg-type]
    c3 = await manager.connect(other, "bob")  # type: ignore[arg-type]

    reached = manager.send_alerts("alice", [{"event": "enter", "device_id": "d1"}])
    await _settle()

    assert reached == 2
    assert laptop.messages("alerts") == phone.messages("alerts") == [{"type": "alerts", "items": [{"event": "enter", "device_id": "d1"}]}]
    assert other.messages("alerts") == []
    assert manager.stats()["connections"] == 3 and manager.stats()["users"] == 2

    await manager.disconnect(c1)
    assert manager.send_alerts("alice", [{"event": "exit"}]) == 1
    await manager.disconnect(c2)
    await manager.disconnect(c3)
    assert manager.stats() == {"connections": 0, "users": 0, "dropped_location_batches": 0, "dropped_alert_batches": 0, "sent_messages": 0}


async def test_locations_broadcast_respects_viewport() -> None:
    manager = ConnectionManager()
    everything, kyiv = FakeWebSocket(), FakeWebSocket()
    c1 = await manager.connect(everything, "u1")  # type: ignore[arg-type]
    c2 = await manager.connect(kyiv, "u2")  # type: ignore[arg-type]
    c2.bbox = BBox.parse([30.0, 50.0, 31.0, 51.0])

    items = [{"device_id": "in", "lat": 50.5, "lon": 30.5}, {"device_id": "out", "lat": 10.0, "lon": 10.0}]
    manager.broadcast_locations(items)
    await _settle()

    assert everything.messages("locations")[0]["items"] == items
    assert [i["device_id"] for i in kyiv.messages("locations")[0]["items"]] == ["in"]
    await manager.close_all()


async def test_slow_client_drops_old_location_batches_but_keeps_alerts() -> None:
    manager = ConnectionManager(alert_backlog=100, location_backlog=2)
    slow = FakeWebSocket(slow=True)
    conn = await manager.connect(slow, "u")  # type: ignore[arg-type]
    await asyncio.sleep(0.06)  # let the hello frame drain

    for i in range(10):
        manager.broadcast_locations([{"device_id": f"d{i}", "lat": 0, "lon": 0}])
    manager.send_alerts("u", [{"event": "enter"}])
    await asyncio.sleep(0.4)

    assert conn.dropped_locations > 0
    assert conn.dropped_alerts == 0
    assert len(slow.messages("alerts")) == 1
    assert len(slow.messages("locations")) <= 3  # at most the backlog plus one in flight
    await manager.close_all()


@pytest.mark.parametrize("user", ["alice", "bob"])
async def test_hello_frame(user: str) -> None:
    manager = ConnectionManager()
    ws = FakeWebSocket()
    conn = await manager.connect(ws, user)  # type: ignore[arg-type]
    await _settle()
    assert ws.accepted
    assert ws.sent[0] == {"type": "hello", "session_id": conn.session_id, "user_id": user}
    await manager.close_all()
