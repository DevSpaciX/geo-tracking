"""Pub/sub broker used to fan events out from the ingestion pipeline to WebSocket clients.

Two implementations share one tiny interface:

* ``LocalBroker``  - in-process; fine for a single uvicorn worker.
* ``RedisBroker``  - Redis pub/sub; every app process publishes the batches it
  ingested and subscribes to everything, so clients connected to *any* process
  receive *all* events. This is what makes horizontal scaling possible.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol

import redis.asyncio as redis_async

log = logging.getLogger(__name__)

CHANNEL_LOCATIONS = "geo:locations"
CHANNEL_ALERTS = "geo:alerts"
CHANNELS = (CHANNEL_LOCATIONS, CHANNEL_ALERTS)

MessageHandler = Callable[[str, bytes], Awaitable[None]]


class Broker(Protocol):
    async def start(self, handler: MessageHandler) -> None: ...
    async def stop(self) -> None: ...
    async def publish(self, channel: str, payload: bytes) -> None: ...


class LocalBroker:
    def __init__(self) -> None:
        self._handler: MessageHandler | None = None

    async def start(self, handler: MessageHandler) -> None:
        self._handler = handler

    async def stop(self) -> None:
        self._handler = None

    async def publish(self, channel: str, payload: bytes) -> None:
        if self._handler is not None:
            await self._handler(channel, payload)


class RedisBroker:
    def __init__(self, url: str) -> None:
        self._url = url
        self._redis: redis_async.Redis | None = None
        self._pubsub: redis_async.client.PubSub | None = None
        self._task: asyncio.Task[None] | None = None
        self._handler: MessageHandler | None = None

    async def start(self, handler: MessageHandler) -> None:
        self._handler = handler
        self._redis = redis_async.from_url(self._url)
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        await self._pubsub.subscribe(*CHANNELS)
        self._task = asyncio.create_task(self._reader(), name="redis-broker-reader")
        log.info("Redis broker subscribed to %s", CHANNELS)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 - shutting down
                pass
        if self._pubsub:
            await self._pubsub.aclose()
        if self._redis:
            await self._redis.aclose()

    async def publish(self, channel: str, payload: bytes) -> None:
        assert self._redis is not None, "broker not started"
        await self._redis.publish(channel, payload)

    async def _reader(self) -> None:
        assert self._pubsub is not None and self._handler is not None
        while True:
            try:
                async for message in self._pubsub.listen():
                    if message.get("type") != "message":
                        continue
                    channel = message["channel"]
                    if isinstance(channel, bytes):
                        channel = channel.decode()
                    await self._handler(channel, message["data"])
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep subscribing after transient Redis errors
                log.exception("Redis subscriber failed; reconnecting in 1s")
                await asyncio.sleep(1)
                try:
                    await self._pubsub.subscribe(*CHANNELS)
                except Exception:  # noqa: BLE001
                    log.exception("Resubscribe failed")


def make_broker(redis_url: str | None) -> Broker:
    return RedisBroker(redis_url) if redis_url else LocalBroker()
