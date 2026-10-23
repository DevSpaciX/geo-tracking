"""Device location ingestion over HTTP (a WebSocket variant lives in ``ws.py``)."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import TypeAdapter

from app.schemas import IngestAccepted, LocationIn
from app.services.pipeline import IngestOverloaded, IngestPipeline
from app.services.types import LocationUpdate

router = APIRouter(prefix="/api/locations", tags=["ingest"])

LocationPayload = LocationIn | list[LocationIn]
location_payload_adapter: TypeAdapter[LocationPayload] = TypeAdapter(LocationPayload)

MAX_BATCH = 5_000


def to_updates(payload: LocationPayload) -> list[LocationUpdate]:
    items = payload if isinstance(payload, list) else [payload]
    now = datetime.now(UTC)
    return [
        LocationUpdate(
            device_id=it.device_id,
            latitude=it.latitude,
            longitude=it.longitude,
            timestamp=(it.timestamp.astimezone(UTC) if it.timestamp and it.timestamp.tzinfo else (it.timestamp.replace(tzinfo=UTC) if it.timestamp else now)),
        )
        for it in items
    ]


@router.post(
    "",
    response_model=IngestAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    responses={503: {"description": "Ingestion queue is full - retry after the indicated delay"}},
)
async def ingest_locations(payload: LocationPayload, request: Request, response: Response) -> IngestAccepted:
    """Accept one location or a batch (max 5 000) and return as soon as it is queued."""
    updates = to_updates(payload)
    if len(updates) > MAX_BATCH:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail=f"Max {MAX_BATCH} locations per request")
    pipeline: IngestPipeline = request.app.state.pipeline
    try:
        accepted = await pipeline.submit(updates, timeout=request.app.state.settings.ingest_submit_timeout_s)
    except IngestOverloaded as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc), headers={"Retry-After": "1"}
        ) from None
    return IngestAccepted(accepted=accepted)
