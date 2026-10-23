"""WebSocket connection registry and non-blocking fan-out.

Each live client gets:

* its own sender task - the broadcaster never awaits a socket write, so one slow
  client cannot stall the others or the ingestion pipeline;
* two bounded buffers - alerts (large, never silently reordered) and location
  batches (small: when a client falls behind, the *oldest* batches are dropped
  because a newer position supersedes them anyway).

Connections are grouped by ``user_id`` so a user with several open sessions
(laptop + phone) receives every alert on every session.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import deque
from collections.abc import Iterable
from typing import Any

import orjson
from fastapi import WebSocket

from app.services.geo import BBox

log = logging.getLogger(__name__)


def _dumps(obj: Any) -> str:
    return orjson.dumps(obj).decode()


class ClientConnection:
    __slots__ = (
        "session_id",
        "user_id",
        "websocket",
        "bbox",
        "_alerts",
        "_locations",
        "_wake",
        "_sender",
        "closed",
        "dropped_locations",
        "dropped_alerts",
        "sent_messages",
    )

    def __init__(self, websocket: WebSocket, user_id: str, *, alert_backlog: int, location_backlog: int) -> None:
        self.session_id = uuid.uuid4().hex
        self.user_id = user_id
        self.websocket = websocket
        self.bbox: BBox | None = None
        self._alerts: deque[str] = deque(maxlen=alert_backlog)
        self._locations: deque[str] = deque(maxlen=location_backlog)
        self._wake = asyncio.Event()
        self._sender: asyncio.Task[None] | None = None
        self.closed = False
        self.dropped_locations = 0
        self.dropped_alerts = 0
        self.sent_messages = 0

    # -- producer side (never blocks) ----------------------------------------
    def push_locations(self, payload: str) -> None:
        if len(self._locations) == self._locations.maxlen:
            self.dropped_locations += 1
        self._locations.append(payload)
        self._wake.set()

    def push_alerts(self, payload: str) -> None:
        if len(self._alerts) == self._alerts.maxlen:
            self.dropped_alerts += 1
        self._alerts.append(payload)
        self._wake.set()

    def push_control(self, payload: str) -> None:
        """Low-volume messages (hello, errors, acks) share the alert lane."""
        self.push_alerts(payload)

    # -- consumer side -------------------------------------------------------
    def start(self) -> None:
        self._sender = asyncio.create_task(self._run_sender(), name=f"ws-sender-{self.session_id}")

    async def stop(self) -> None:
        self.closed = True
        if self._sender and not self._sender.done():
            self._sender.cancel()
            try:
                await self._sender
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

    async def _run_sender(self) -> None:
        try:
            while not self.closed:
                await self._wake.wait()
                self._wake.clear()
                while True:
                    if self._alerts:
                        payload = self._alerts.popleft()
                    elif self._locations:
                        payload = self._locations.popleft()
                    else:
                        break
                    await self.websocket.send_text(payload)
                    self.sent_messages += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - socket gone; reader loop will clean up
            log.debug("sender for %s stopped: %r", self.session_id, exc)
        finally:
            self.closed = True


class ConnectionManager:
    def __init__(self, *, alert_backlog: int = 1_000, location_backlog: int = 8) -> None:
        self._alert_backlog = alert_backlog
        self._location_backlog = location_backlog
        self._by_user: dict[str, dict[str, ClientConnection]] = {}
        self._all: dict[str, ClientConnection] = {}

    # -- lifecycle -----------------------------------------------------------
    async def connect(self, websocket: WebSocket, user_id: str) -> ClientConnection:
        await websocket.accept()
        conn = ClientConnection(
            websocket, user_id, alert_backlog=self._alert_backlog, location_backlog=self._location_backlog
        )
        self._all[conn.session_id] = conn
        self._by_user.setdefault(user_id, {})[conn.session_id] = conn
        conn.start()
        conn.push_control(_dumps({"type": "hello", "session_id": conn.session_id, "user_id": user_id}))
        log.info("ws connected user=%s session=%s sessions_for_user=%d", user_id, conn.session_id, len(self._by_user[user_id]))
        return conn

    async def disconnect(self, conn: ClientConnection) -> None:
        self._all.pop(conn.session_id, None)
        sessions = self._by_user.get(conn.user_id)
        if sessions is not None:
            sessions.pop(conn.session_id, None)
            if not sessions:
                self._by_user.pop(conn.user_id, None)
        await conn.stop()
        log.info("ws disconnected user=%s session=%s", conn.user_id, conn.session_id)

    async def close_all(self) -> None:
        for conn in list(self._all.values()):
            await self.disconnect(conn)
            try:
                await conn.websocket.close()
            except Exception:  # noqa: BLE001
                pass

    # -- fan-out -------------------------------------------------------------
    def broadcast_locations(self, items: list[dict[str, Any]]) -> None:
        """Send a batch of positions to every client (respecting viewport filters)."""
        if not self._all or not items:
            return
        full_payload: str | None = None
        for conn in self._all.values():
            if conn.closed:
                continue
            if conn.bbox is None:
                if full_payload is None:
                    full_payload = _dumps({"type": "locations", "items": items})
                conn.push_locations(full_payload)
            else:
                bbox = conn.bbox
                visible = [it for it in items if bbox.contains(it["lat"], it["lon"])]
                if visible:
                    conn.push_locations(_dumps({"type": "locations", "items": visible}))

    def send_alerts(self, user_id: str, alerts: list[dict[str, Any]]) -> int:
        """Deliver alerts to *every* active session of ``user_id``. Returns sessions reached."""
        sessions = self._by_user.get(user_id)
        if not sessions or not alerts:
            return 0
        payload = _dumps({"type": "alerts", "items": alerts})
        reached = 0
        for conn in sessions.values():
            if not conn.closed:
                conn.push_alerts(payload)
                reached += 1
        return reached

    # -- introspection -------------------------------------------------------
    def sessions_for(self, user_id: str) -> Iterable[ClientConnection]:
        return list(self._by_user.get(user_id, {}).values())

    def stats(self) -> dict[str, Any]:
        return {
            "connections": len(self._all),
            "users": len(self._by_user),
            "dropped_location_batches": sum(c.dropped_locations for c in self._all.values()),
            "dropped_alert_batches": sum(c.dropped_alerts for c in self._all.values()),
            "sent_messages": sum(c.sent_messages for c in self._all.values()),
        }
