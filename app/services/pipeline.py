"""Ingestion pipeline: bounded queue -> micro-batches -> PostGIS -> broker.

Flow for every location update:

1. ``submit`` validates nothing (already done by the API layer) and puts the
   update on a bounded ``asyncio.Queue``. If the queue is full the producer
   waits up to ``submit_timeout`` and then gets ``IngestOverloaded`` - this is
   the back-pressure signal (HTTP 503 + Retry-After for REST producers; for
   WebSocket producers the await simply slows the reader, which propagates as
   TCP back-pressure to the device).
2. A worker drains the queue into a batch of at most ``batch_size`` updates or
   whatever arrived within ``flush_interval``.
3. The batch is written and evaluated with three set-based statements on a
   single pooled connection (history append, last-position upsert, zone
   evaluation). Only ``ingest_workers`` connections are ever used by ingestion,
   so 10 000 devices cannot exhaust the pool.
4. Positions and per-user alerts are published to the broker, which fans them
   out to WebSocket clients (possibly on other processes).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import orjson
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.services import spatial
from app.services.broker import CHANNEL_ALERTS, CHANNEL_LOCATIONS, Broker
from app.services.types import Alert, LocationUpdate

log = logging.getLogger(__name__)


class IngestOverloaded(Exception):
    """Raised when the ingestion queue stayed full for longer than the submit timeout."""


@dataclass
class PipelineStats:
    received: int = 0
    processed: int = 0
    batches: int = 0
    alerts: int = 0
    rejected: int = 0
    db_errors: int = 0
    last_batch_size: int = 0
    last_batch_ms: float = 0.0
    started_at: float = field(default_factory=time.monotonic)

    def snapshot(self, queue_depth: int, queue_size: int) -> dict[str, Any]:
        return {
            "received": self.received,
            "processed": self.processed,
            "batches": self.batches,
            "alerts": self.alerts,
            "rejected": self.rejected,
            "db_errors": self.db_errors,
            "queue_depth": queue_depth,
            "queue_size": queue_size,
            "last_batch_size": self.last_batch_size,
            "last_batch_ms": round(self.last_batch_ms, 1),
            "uptime_s": round(time.monotonic() - self.started_at, 1),
        }


class IngestPipeline:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        broker: Broker,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._broker = broker
        self._queue: asyncio.Queue[LocationUpdate] = asyncio.Queue(maxsize=settings.ingest_queue_size)
        self._workers: list[asyncio.Task[None]] = []
        self._stopping = False
        self.stats = PipelineStats()

    # -- producer API ----------------------------------------------------------
    async def submit(self, updates: Sequence[LocationUpdate], timeout: float | None = None) -> int:
        """Enqueue updates. ``timeout=None`` waits indefinitely (WebSocket producers);
        a number bounds the wait and raises ``IngestOverloaded`` afterwards."""
        if self._stopping:
            raise IngestOverloaded("shutting down")
        deadline = None if timeout is None else time.monotonic() + timeout
        for i, update in enumerate(updates):
            try:
                self._queue.put_nowait(update)
            except asyncio.QueueFull:
                remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                try:
                    await asyncio.wait_for(self._queue.put(update), timeout=remaining)
                except TimeoutError:
                    self.stats.rejected += len(updates) - i
                    raise IngestOverloaded("ingestion queue is full") from None
        self.stats.received += len(updates)
        return len(updates)

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    def snapshot(self) -> dict[str, Any]:
        return self.stats.snapshot(self._queue.qsize(), self._settings.ingest_queue_size)

    # -- lifecycle -------------------------------------------------------------
    async def start(self) -> None:
        self._stopping = False
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"ingest-worker-{i}") for i in range(self._settings.ingest_workers)
        ]
        log.info(
            "ingest pipeline started workers=%d batch=%d flush=%dms queue=%d",
            self._settings.ingest_workers,
            self._settings.ingest_batch_size,
            self._settings.ingest_flush_interval_ms,
            self._settings.ingest_queue_size,
        )

    async def stop(self) -> None:
        self._stopping = True
        for task in self._workers:
            task.cancel()
        for task in self._workers:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._workers.clear()

    # -- worker ----------------------------------------------------------------
    async def _collect_batch(self) -> list[LocationUpdate]:
        """Block for the first item, then gather more until the batch is full or the flush interval passes."""
        batch = [await self._queue.get()]
        max_size = self._settings.ingest_batch_size
        deadline = time.monotonic() + self._settings.ingest_flush_interval_ms / 1000
        while len(batch) < max_size:
            try:
                batch.append(self._queue.get_nowait())
                continue
            except asyncio.QueueEmpty:
                pass
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                batch.append(await asyncio.wait_for(self._queue.get(), timeout=remaining))
            except TimeoutError:
                break
        return batch

    async def _worker(self, index: int) -> None:
        log.debug("ingest worker %d running", index)
        while True:
            batch = await self._collect_batch()
            started = time.perf_counter()
            try:
                alerts = await self.process_batch(batch)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - never let one bad batch kill the worker
                self.stats.db_errors += 1
                log.exception("batch of %d updates failed; dropping it", len(batch))
                await asyncio.sleep(0.5)
                continue
            self.stats.batches += 1
            self.stats.processed += len(batch)
            self.stats.alerts += len(alerts)
            self.stats.last_batch_size = len(batch)
            self.stats.last_batch_ms = (time.perf_counter() - started) * 1000

    async def process_batch(self, batch: Sequence[LocationUpdate]) -> list[Alert]:
        """Persist a batch, evaluate zones and publish the results. Public for tests."""
        latest = spatial.dedupe_latest(batch)
        async with self._session_factory() as session, session.begin():
            if self._settings.store_location_history:
                await spatial.insert_history(session, batch)
            await spatial.upsert_devices(session, latest)
            alerts = await spatial.evaluate_zones(session, latest)

        if not self._settings.alert_repeat_inside:
            alerts = [a for a in alerts if a.event != "inside"]

        await self._broker.publish(CHANNEL_LOCATIONS, orjson.dumps([p.to_wire() for p in latest]))
        if alerts:
            by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for alert in alerts:
                by_user[alert.user_id].append(alert.to_wire())
            for user_id, items in by_user.items():
                await self._broker.publish(CHANNEL_ALERTS, orjson.dumps({"user_id": user_id, "alerts": items}))
        return alerts
