#!/usr/bin/env python3
"""Asynchronous load generator: simulates thousands of moving devices.

Each simulated device performs a smooth random walk (heading persistence plus a
random step length) and reports its position every ``--interval`` seconds.
Devices are sharded across ``--connections`` WebSocket connections (or HTTP
requests) and the sends are spread evenly over the interval so the server sees a
steady stream instead of a burst every few seconds.

Examples
--------
    python generator.py                                   # 10 000 devices over WS to localhost:8080
    python generator.py --devices 2000 --interval 2
    python generator.py --transport http --url http://localhost:8080
    python generator.py --duration 60                     # stop after a minute

Only ``aiohttp`` is required.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import math
import random
import signal
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import aiohttp

METRES_PER_DEG_LAT = 111_320.0


@dataclass(slots=True)
class Device:
    device_id: str
    lat: float
    lon: float
    heading: float  # radians
    speed_mps: float

    def step(self, dt_s: float, rng: random.Random) -> None:
        """Advance along the current heading, then jitter heading and speed slightly."""
        distance = self.speed_mps * dt_s
        dlat = distance * math.cos(self.heading) / METRES_PER_DEG_LAT
        dlon = distance * math.sin(self.heading) / (METRES_PER_DEG_LAT * max(0.01, math.cos(math.radians(self.lat))))
        self.lat = max(-85.0, min(85.0, self.lat + dlat))
        self.lon = ((self.lon + dlon + 180.0) % 360.0) - 180.0
        self.heading += rng.gauss(0.0, 0.35)
        self.speed_mps = max(0.5, min(40.0, self.speed_mps + rng.gauss(0.0, 1.0)))

    def payload(self) -> dict[str, object]:
        return {
            "device_id": self.device_id,
            "latitude": round(self.lat, 6),
            "longitude": round(self.lon, 6),
            "timestamp": datetime.now(UTC).isoformat(),
        }


def make_devices(n: int, center_lat: float, center_lon: float, spread_deg: float, rng: random.Random) -> list[Device]:
    return [
        Device(
            device_id=f"dev-{i:05d}",
            lat=center_lat + rng.uniform(-spread_deg, spread_deg),
            lon=center_lon + rng.uniform(-spread_deg, spread_deg),
            heading=rng.uniform(0, 2 * math.pi),
            speed_mps=rng.uniform(1.0, 25.0),
        )
        for i in range(n)
    ]


@dataclass
class Stats:
    points_sent: int = 0
    batches_sent: int = 0
    errors: int = 0
    reconnects: int = 0
    throttled: int = 0
    rtts_ms: list[float] = field(default_factory=list)
    _last_points: int = 0
    _last_time: float = field(default_factory=time.monotonic)

    def record_rtt(self, ms: float) -> None:
        if len(self.rtts_ms) < 5_000:
            self.rtts_ms.append(ms)

    def report(self) -> str:
        now = time.monotonic()
        rate = (self.points_sent - self._last_points) / max(1e-6, now - self._last_time)
        self._last_points, self._last_time = self.points_sent, now
        if self.rtts_ms:
            rtts = sorted(self.rtts_ms)
            p50 = statistics.median(rtts)
            p95 = rtts[min(len(rtts) - 1, int(len(rtts) * 0.95))]
            self.rtts_ms.clear()
            rtt = f"rtt p50={p50:.0f}ms p95={p95:.0f}ms"
        else:
            rtt = "rtt n/a"
        return (
            f"sent={self.points_sent} ({rate:,.0f} pts/s) batches={self.batches_sent} "
            f"{rtt} errors={self.errors} throttled={self.throttled} reconnects={self.reconnects}"
        )


class Shard:
    """A group of devices driven by one connection."""

    def __init__(self, index: int, devices: list[Device], args: argparse.Namespace, stats: Stats, stop: asyncio.Event) -> None:
        self.index = index
        self.devices = devices
        self.args = args
        self.stats = stats
        self.stop = stop
        self.rng = random.Random(args.seed + index)
        self.batches = max(1, math.ceil(len(devices) / args.batch))

    def tick_batches(self) -> list[list[dict[str, object]]]:
        """Move every device one tick and return the payloads chunked into batches."""
        for d in self.devices:
            d.step(self.args.interval, self.rng)
        payloads = [d.payload() for d in self.devices]
        return [payloads[i : i + self.args.batch] for i in range(0, len(payloads), self.args.batch)]

    async def run(self, session: aiohttp.ClientSession) -> None:
        # Stagger shards so they do not all start on the same instant.
        await asyncio.sleep(self.rng.uniform(0, self.args.interval))
        while not self.stop.is_set():
            try:
                if self.args.transport == "ws":
                    await self._run_ws(session)
                else:
                    await self._run_http(session)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.stats.errors += 1
                self.stats.reconnects += 1
                if self.args.verbose:
                    print(f"[shard {self.index}] {type(exc).__name__}: {exc}; reconnecting", file=sys.stderr)
                await asyncio.sleep(min(10.0, 0.5 * (1 + self.stats.reconnects % 8)))

    async def _run_ws(self, session: aiohttp.ClientSession) -> None:
        url = self.args.url.replace("http://", "ws://").replace("https://", "wss://").rstrip("/") + "/ws/ingest"
        async with session.ws_connect(url, heartbeat=30, max_msg_size=4 * 1024 * 1024) as ws:
            gap = self.args.interval / self.batches
            while not self.stop.is_set():
                tick_start = time.monotonic()
                for batch in self.tick_batches():
                    if self.stop.is_set():
                        return
                    t0 = time.perf_counter()
                    await ws.send_json(batch)
                    msg = await ws.receive(timeout=30)
                    if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        raise ConnectionError(f"websocket closed: {msg.type}")
                    reply = msg.json()
                    self.stats.record_rtt((time.perf_counter() - t0) * 1000)
                    if reply.get("type") == "ack":
                        self.stats.points_sent += reply.get("accepted", len(batch))
                        self.stats.batches_sent += 1
                    else:
                        self.stats.errors += 1
                        if self.args.verbose:
                            print(f"[shard {self.index}] server error: {reply}", file=sys.stderr)
                    await asyncio.sleep(gap)
                # Keep the tick cadence stable even if sends were slow.
                await asyncio.sleep(max(0.0, self.args.interval - (time.monotonic() - tick_start)))

    async def _run_http(self, session: aiohttp.ClientSession) -> None:
        url = self.args.url.rstrip("/") + "/api/locations"
        gap = self.args.interval / self.batches
        while not self.stop.is_set():
            tick_start = time.monotonic()
            for batch in self.tick_batches():
                if self.stop.is_set():
                    return
                for attempt in range(5):
                    t0 = time.perf_counter()
                    async with session.post(url, json=batch) as resp:
                        self.stats.record_rtt((time.perf_counter() - t0) * 1000)
                        if resp.status == 202:
                            self.stats.points_sent += len(batch)
                            self.stats.batches_sent += 1
                            break
                        if resp.status == 503:
                            # Back-pressure signal from the server: honour Retry-After.
                            self.stats.throttled += 1
                            await asyncio.sleep(float(resp.headers.get("Retry-After", "1")) * (attempt + 1))
                            continue
                        self.stats.errors += 1
                        if self.args.verbose:
                            print(f"[shard {self.index}] HTTP {resp.status}: {await resp.text()}", file=sys.stderr)
                        break
                await asyncio.sleep(gap)
            await asyncio.sleep(max(0.0, self.args.interval - (time.monotonic() - tick_start)))


async def main(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    devices = make_devices(args.devices, args.center_lat, args.center_lon, args.spread, rng)
    stats = Stats()
    stop = asyncio.Event()
    shards = [Shard(i, devices[i :: args.connections], args, stats, stop) for i in range(args.connections)]
    print(
        f"simulating {args.devices} devices over {args.connections} {args.transport} connection(s) -> {args.url}; "
        f"interval={args.interval}s batch={args.batch} (~{args.devices / args.interval:,.0f} points/s)"
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    async def reporter() -> None:
        while not stop.is_set():
            await asyncio.sleep(args.report)
            print(f"[{datetime.now():%H:%M:%S}] {stats.report()}", flush=True)

    async def deadline() -> None:
        if args.duration:
            await asyncio.sleep(args.duration)
            stop.set()

    connector = aiohttp.TCPConnector(limit=max(args.connections, args.concurrency))
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [asyncio.create_task(s.run(session)) for s in shards]
        tasks.append(asyncio.create_task(reporter()))
        tasks.append(asyncio.create_task(deadline()))
        await stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    print(f"final: {stats.report()}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default="http://localhost:8080", help="service base URL (http(s)://host:port)")
    p.add_argument("--transport", choices=("ws", "http"), default="ws")
    p.add_argument("--devices", type=int, default=10_000)
    p.add_argument("--interval", type=float, default=3.0, help="seconds between reports of one device")
    p.add_argument("--connections", type=int, default=20, help="parallel connections (device shards)")
    p.add_argument("--batch", type=int, default=250, help="locations per frame/request")
    p.add_argument("--concurrency", type=int, default=50, help="max in-flight HTTP requests")
    p.add_argument("--center-lat", type=float, default=50.4501, help="Kyiv by default")
    p.add_argument("--center-lon", type=float, default=30.5234)
    p.add_argument("--spread", type=float, default=0.25, help="initial +/- degrees around the centre")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--report", type=float, default=5.0, help="seconds between stats lines")
    p.add_argument("--duration", type=float, default=0.0, help="stop after N seconds (0 = run until Ctrl+C)")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    if args.devices < 1 or args.connections < 1 or args.batch < 1:
        p.error("--devices, --connections and --batch must be >= 1")
    args.connections = min(args.connections, args.devices)
    return args


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        pass
