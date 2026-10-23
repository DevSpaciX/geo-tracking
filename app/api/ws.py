"""WebSocket endpoints: live client stream and device ingestion."""

from __future__ import annotations

import logging

import orjson
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from pydantic import ValidationError

from app.api.ingest import MAX_BATCH, location_payload_adapter, to_updates
from app.auth import user_id_from_websocket
from app.services.connections import ConnectionManager
from app.services.geo import BBox
from app.services.pipeline import IngestPipeline

log = logging.getLogger(__name__)
router = APIRouter(tags=["websocket"])


@router.websocket("/ws/live")
async def ws_live(websocket: WebSocket) -> None:
    """Client stream: receives ``locations`` batches and the user's ``alerts``.

    Identify with ``X-User-Id`` header or ``?user_id=`` query parameter.
    Client -> server messages:
      {"type": "set_viewport", "bbox": [min_lon, min_lat, max_lon, max_lat]}  - only stream positions inside bbox
      {"type": "clear_viewport"}
      {"type": "ping"}
    """
    user_id = user_id_from_websocket(websocket)
    if user_id is None:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="user id required")
        return
    manager: ConnectionManager = websocket.app.state.connections
    conn = await manager.connect(websocket, user_id)
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = orjson.loads(raw)
                kind = msg.get("type")
                if kind == "set_viewport":
                    conn.bbox = BBox.parse(msg.get("bbox"))
                    conn.push_control(orjson.dumps({"type": "viewport", "bbox": list(conn.bbox)}).decode())
                elif kind == "clear_viewport":
                    conn.bbox = None
                    conn.push_control(orjson.dumps({"type": "viewport", "bbox": None}).decode())
                elif kind == "ping":
                    conn.push_control('{"type":"pong"}')
                else:
                    conn.push_control(orjson.dumps({"type": "error", "detail": f"unknown message type {kind!r}"}).decode())
            except (ValueError, AttributeError) as exc:
                conn.push_control(orjson.dumps({"type": "error", "detail": str(exc)}).decode())
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(conn)


@router.websocket("/ws/ingest")
async def ws_ingest(websocket: WebSocket) -> None:
    """Device stream: each text frame is one location object or an array of them.

    The server answers every frame with ``{"type":"ack","accepted":n}`` (or an
    ``error``). The ack is sent only after the batch is queued, so a producer that
    waits for acks is naturally throttled when the server is saturated.
    """
    await websocket.accept()
    pipeline: IngestPipeline = websocket.app.state.pipeline
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                payload = location_payload_adapter.validate_json(raw)
            except ValidationError as exc:
                await websocket.send_text(orjson.dumps({"type": "error", "detail": exc.errors()[:3]}).decode())
                continue
            updates = to_updates(payload)
            if len(updates) > MAX_BATCH:
                await websocket.send_text(orjson.dumps({"type": "error", "detail": f"max {MAX_BATCH} per frame"}).decode())
                continue
            accepted = await pipeline.submit(updates, timeout=None)
            await websocket.send_text(f'{{"type":"ack","accepted":{accepted}}}')
    except WebSocketDisconnect:
        pass
