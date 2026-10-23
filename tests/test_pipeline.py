"""Ingestion queue back-pressure (no database: the workers are never started)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.config import Settings
from app.services.broker import LocalBroker
from app.services.pipeline import IngestOverloaded, IngestPipeline
from app.services.types import LocationUpdate


def _updates(n: int) -> list[LocationUpdate]:
    now = datetime.now(UTC)
    return [LocationUpdate(f"d{i}", 0.0, 0.0, now) for i in range(n)]


async def test_submit_rejects_when_queue_stays_full() -> None:
    pipeline = IngestPipeline(Settings(ingest_queue_size=5), session_factory=None, broker=LocalBroker())  # type: ignore[arg-type]

    assert await pipeline.submit(_updates(5), timeout=0.05) == 5
    assert pipeline.queue_depth == 5

    with pytest.raises(IngestOverloaded):
        await pipeline.submit(_updates(3), timeout=0.05)

    snap = pipeline.snapshot()
    assert snap["received"] == 5 and snap["rejected"] == 3 and snap["queue_depth"] == 5


async def test_submit_waits_for_room_when_consumer_catches_up() -> None:
    pipeline = IngestPipeline(Settings(ingest_queue_size=2), session_factory=None, broker=LocalBroker())  # type: ignore[arg-type]
    await pipeline.submit(_updates(2))

    async def drain_later() -> None:
        await asyncio.sleep(0.05)
        pipeline._queue.get_nowait()
        pipeline._queue.get_nowait()

    asyncio.create_task(drain_later())
    assert await pipeline.submit(_updates(2), timeout=1.0) == 2
    assert pipeline.snapshot()["received"] == 4


async def test_collect_batch_respects_size_and_flush_interval() -> None:
    pipeline = IngestPipeline(
        Settings(ingest_batch_size=3, ingest_flush_interval_ms=50), session_factory=None, broker=LocalBroker()  # type: ignore[arg-type]
    )
    await pipeline.submit(_updates(7))
    assert len(await pipeline._collect_batch()) == 3
    assert len(await pipeline._collect_batch()) == 3
    started = asyncio.get_running_loop().time()
    assert len(await pipeline._collect_batch()) == 1  # last one flushes after the interval, not when full
    assert asyncio.get_running_loop().time() - started >= 0.04
