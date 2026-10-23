# Real-Time Geo-Tracking & Alerting Service

A FastAPI + PostGIS backend that ingests live coordinates from thousands of
devices, streams them to web clients over WebSockets and raises alerts when a
device enters / stays in / leaves a user's circular geozone.

![Architecture](docs/architecture.svg)

## Demo

Leaflet UI with 10 000 simulated devices streaming in, a 1.5 km geozone around
the city centre and the live alert feed (ENTER / EXIT, with the distance to the
zone centre at the moment of the event):

![Live map with devices and alerts](docs/screenshots/live-map.png)

The same user opened on a phone-sized screen at the same time. Both sessions
receive every alert; `GET /api/stats` confirms two connections for one user and
no dropped frames while the generator is running:

<img src="docs/screenshots/mobile-session.png" alt="Second session of the same user on a phone" width="320">

```json
{
  "ingest":    { "received": 441500, "processed": 440750, "batches": 441,
                 "alerts": 1819, "rejected": 0, "db_errors": 0,
                 "queue_depth": 0, "queue_size": 100000,
                 "last_batch_size": 500, "last_batch_ms": 34.6 },
  "websocket": { "connections": 2, "users": 1,
                 "dropped_location_batches": 0, "dropped_alert_batches": 0,
                 "sent_messages": 1034 }
}
```

Interactive API docs (Swagger UI) at `/docs`:

![Swagger UI](docs/screenshots/swagger.png)

## Running

Requirements: Docker + Docker Compose v2.

```bash
cp .env.example .env            # optional - defaults work out of the box
docker compose up --build -d    # PostGIS, Redis, migrations, app
open http://localhost:8080      # Leaflet demo UI
```

Default host ports: app `8080`, PostgreSQL `5433`. Change them in `.env`
(`APP_PORT`, `DB_PORT`). Migrations run in the one-shot `migrate` service
before `app` starts; `docker compose logs -f app` tails the server.

Interactive API docs: <http://localhost:8080/docs>.

### Quick manual check

```bash
# identify as "alice" and create a 300 m zone around Kyiv centre
curl -s -X POST localhost:8080/api/geozones -H 'X-User-Id: alice' -H 'Content-Type: application/json' \
  -d '{"name":"Office","latitude":50.4501,"longitude":30.5234,"radius_m":300}'

# report a device inside it
curl -s -X POST localhost:8080/api/locations -H 'Content-Type: application/json' \
  -d '{"device_id":"truck-1","latitude":50.4502,"longitude":30.5234}'
```

Open <http://localhost:8080>, connect as `alice` (twice, in two tabs, to see
multi-session delivery), and the `ENTER` alert appears in both. Click the map
to draw more zones.

### Running the load generator

```bash
# inside the compose network (10 000 devices, every 3 s, 20 WebSocket connections)
docker compose --profile load up --build generator

# or from the host (needs Python 3.10+ and aiohttp)
pip install aiohttp
python generator.py --url http://localhost:8080 --devices 10000 --interval 3
python generator.py --transport http --devices 2000      # HTTP ingestion with 503 back-off
python generator.py --duration 60 --connections 50 -v    # run for a minute, more verbose
```

The generator prints throughput and round-trip latency every 5 s:

```
simulating 10000 devices over 20 ws connection(s) -> http://localhost:8080; interval=3.0s batch=250 (~3,333 points/s)
[17:07:19] sent=15000 (2,998 pts/s) batches=60 rtt p50=3ms p95=18ms errors=0 throttled=0 reconnects=0
[17:07:24] sent=31000 (3,198 pts/s) batches=124 rtt p50=3ms p95=29ms errors=0 throttled=0 reconnects=0
[17:07:29] sent=47250 (3,246 pts/s) batches=189 rtt p50=5ms p95=17ms errors=0 throttled=0 reconnects=0
...
final: sent=196500 (3,363 pts/s) batches=786 rtt p50=5ms p95=28ms errors=0 throttled=0 reconnects=0
```

Measured on a laptop (Docker Desktop, single uvicorn worker) with the default
settings: ~3 300 points/s sustained, the ingestion queue stays empty, a batch of
1 250 points takes ~130 ms in PostGIS, the app container uses ~8 % CPU and
~80 MB RAM, PostGIS ~20 % CPU.

`GET /api/stats` shows the server-side view: queue depth, batch size and
latency of the last batch, alerts produced, WebSocket sessions and how many
batches had to be dropped for slow clients.

### Running the tests

```bash
docker compose --profile test run --build --rm test    # unit + PostGIS integration + API tests
```

Locally: `pip install -r requirements-dev.txt && pytest -q -m "not db"` runs the
DB-free tests; export `TEST_DATABASE_URL=postgresql+asyncpg://geo:geo@localhost:5433/geo_test`
(the compose database) to run everything.

## API

All geozone endpoints require the `X-User-Id` header (mock auth - any
`[A-Za-z0-9_.@-]{1,64}` string). WebSocket clients pass it as a header or
`?user_id=` query parameter.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/locations` | Ingest one location or an array (≤ 5 000). `202 Accepted`, or `503 + Retry-After` when overloaded. |
| `WS` | `/ws/ingest` | Same payloads as frames; each frame is acknowledged with `{"type":"ack","accepted":n}`. |
| `POST/GET` | `/api/geozones` | Create / list the caller's zones (`?active_only=true`). |
| `GET/PATCH/PUT/DELETE` | `/api/geozones/{id}` | Read / update / delete one zone (404 for zones of other users). |
| `WS` | `/ws/live` | Client stream, see below. |
| `GET` | `/api/devices?bbox=w,s,e,n` | Last known positions (UI bootstrap). |
| `GET` | `/api/stats`, `/health` | Metrics / liveness. |

Location payload: `{"device_id": "truck-1", "latitude": 50.45, "longitude": 30.52, "timestamp": "2026-10-23T10:00:00Z"}`
(`timestamp` optional, defaults to server time).

### `/ws/live` protocol

Server → client:

```jsonc
{"type": "hello", "session_id": "…", "user_id": "alice"}
{"type": "locations", "items": [{"device_id": "dev-00001", "lat": 50.45, "lon": 30.52, "ts": "…"}, …]}   // one frame per ingested batch
{"type": "alerts", "items": [{"event": "enter" | "inside" | "exit", "zone_id": "…", "zone_name": "Office",
                             "device_id": "dev-00001", "lat": 50.45, "lon": 30.52, "distance_m": 87.2, "ts": "…"}]}
```

Client → server:

```jsonc
{"type": "set_viewport", "bbox": [min_lon, min_lat, max_lon, max_lat]}   // only stream devices inside the map viewport
{"type": "clear_viewport"}
{"type": "ping"}
```

`enter` fires once when a device first reports inside a zone, `inside` on every
subsequent report inside it (disable with `ALERT_REPEAT_INSIDE=false`), `exit`
once the device next reports outside.

## Architecture

### Ingestion & high throughput

* **Bounded queue + micro-batching.** Every HTTP/WS handler only validates the
  payload and pushes it onto an `asyncio.Queue` (default capacity 100 000).
  A worker drains it into batches of ≤ 2 000 updates or whatever arrived within
  200 ms, so 10 000 devices reporting every 3 s (~3 300 points/s) cost roughly
  two database round-trips per second instead of thousands.
* **Back-pressure instead of unbounded buffering.** When the queue is full, HTTP
  producers wait up to 2 s and then receive `503 Service Unavailable` with
  `Retry-After` (the generator honours it); WebSocket producers are simply not
  read any further until there is room, which the OS turns into TCP
  back-pressure on the device. The ack per frame on `/ws/ingest` gives
  well-behaved producers a natural rate limiter.
* **Connection pool safety.** Request handlers use one pooled connection for the
  duration of a request; ingestion uses exactly `INGEST_WORKERS` (default 1)
  connections regardless of how many devices are connected. The pool is
  explicitly sized (`DB_POOL_SIZE` / `DB_MAX_OVERFLOW`) with a `pool_timeout`, so
  exhaustion fails fast instead of hanging the event loop.
* **No blocking work on the event loop.** The hot path is JSON (orjson) +
  queue operations; all persistence and *all* spatial math happen inside
  PostgreSQL via asyncpg. Serialisation of a broadcast frame happens once per
  batch, not once per client. uvicorn runs with `uvloop` + `httptools`.
* **Per-batch set-based SQL.** A batch is sent as parallel PostgreSQL arrays
  and `unnest`-ed: one `INSERT … SELECT` for history, one `INSERT … ON CONFLICT`
  upsert for last positions and one CTE statement for zone evaluation
  (`app/services/spatial.py`). Each device's *latest* fix in a batch is the one
  evaluated. History is append-only with `synchronous_commit=off` in the
  compose Postgres; for real deployments it would be partitioned by time or
  moved to a time-series store, and can be switched off with
  `STORE_LOCATION_HISTORY=false`.

### Spatial queries (PostGIS)

* Coordinates are stored as `geography(Point, 4326)` so `ST_DWithin` /
  `ST_Distance` work in metres on the spheroid - the natural fit for "within
  *N* metres of a point" with real-world radii (a planar `geometry` would need
  per-location reprojection to be accurate).
* The lat/lon floats are the API representation; the geography columns are
  **generated columns** (`ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography`)
  so the ORM never has to handle WKB and the two can't drift apart.
* Each zone also stores a generated `bounds = ST_Buffer(center, radius_m)` with
  a GiST index. The evaluation query is
  `g.bounds && point AND ST_DWithin(g.center, point, g.radius_m)`: the `&&`
  bounding-box probe uses the index, `ST_DWithin` makes the exact check.
  This matters because `ST_DWithin` with a per-row radius cannot use an index
  on its own (the planner can't expand the probe by a value that lives in the
  indexed row). A test asserts the index is used.
* Enter/exit detection is a `zone_presence (zone_id, device_id)` table maintained
  in the same statement: `INSERT … ON CONFLICT … RETURNING (xmax = 0) AS entered`
  tells apart "just entered" from "still inside", and a `DELETE … WHERE NOT EXISTS`
  on the devices in the batch yields exits. Deleting a zone cascades.
* Zones are strictly per user: every CRUD query filters by `user_id` (other
  users' zones are reported as 404), and alerts carry the zone owner's id so
  they are routed only to that user's sessions.

### WebSocket state management

* `ConnectionManager` (`app/services/connections.py`) keeps
  `user_id → {session_id → connection}`. Multiple simultaneous sessions per
  user are first-class: an alert for a user is pushed to every session; a
  session can set its own viewport filter.
* **Non-blocking fan-out.** Broadcasting never awaits a socket write. Each
  connection has a dedicated sender task and two bounded buffers: alerts
  (1 000 frames) and location batches (8 frames). A slow consumer therefore
  cannot stall the pipeline or other clients; when it falls behind, the
  *oldest* location batches are discarded (a newer position supersedes them),
  while alerts are kept. Drops are counted in `/api/stats`.
* Clients can restrict the stream to their map viewport (`set_viewport`), which
  is applied server-side so a browser looking at one district doesn't receive
  10 000 positions every batch.
* **Horizontal scaling.** Pipeline output goes through a tiny pub/sub broker
  abstraction. With `REDIS_URL` set (the compose default) every app process
  publishes the batches it ingested to Redis and subscribes to all of them, so
  clients connected to *any* uvicorn worker / replica receive all positions and
  their alerts. Set `WEB_CONCURRENCY=4` to run several workers; the in-process
  broker is used when `REDIS_URL` is empty.

### Project layout

```
app/
  main.py            app factory, lifespan wiring (engine, broker, pipeline, connection manager)
  config.py          settings (env / .env)
  models.py          SQLAlchemy models with generated PostGIS columns
  schemas.py         Pydantic request/response models
  auth.py            X-User-Id mock authentication
  api/               geozones.py (CRUD), ingest.py (HTTP), ws.py (live + ingest sockets), system.py
  services/
    pipeline.py      bounded queue -> micro-batches -> DB -> broker (back-pressure lives here)
    spatial.py       set-based PostGIS statements (history, upsert, zone evaluation)
    connections.py   WebSocket registry + per-connection sender tasks
    broker.py        LocalBroker / RedisBroker
    geo.py           pure helpers (haversine, viewport bbox)
alembic/             migrations (async env)
static/index.html    Leaflet demo UI
generator.py         asyncio load generator
tests/               unit tests (no services) + PostGIS/API integration tests (marker "db")
```

### Trade-offs & next steps

* Devices are global (any user sees all positions on the map) while zones are
  private - matching the task; scoping devices to tenants would be a
  `devices.owner_id` filter in the broadcast.
* Device ingestion is unauthenticated for the exercise; a per-device token
  checked in `ws_ingest` / `ingest_locations` is the obvious addition.
* "Exit" is only detected when a device next reports *outside*; a device that
  goes silent stays "inside" until it reports again (a periodic sweep on
  `zone_presence.last_seen_at` would close that gap).
* With `INGEST_WORKERS > 1` batches are processed concurrently, which raises
  throughput but can reorder enter/exit events of a single device across
  batches.
