"""Application settings (environment driven, see .env.example)."""

from __future__ import annotations

import socket
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- database -----------------------------------------------------------
    database_url: str = "postgresql+asyncpg://geo:geo@localhost:5433/geo"
    db_pool_size: int = 10
    db_max_overflow: int = 10
    db_pool_timeout_s: float = 10.0
    db_echo: bool = False

    # --- messaging ----------------------------------------------------------
    # When set, location/alert fan-out goes through Redis pub/sub so several
    # app processes (uvicorn workers / replicas) share one event stream.
    # When unset an in-process broker is used (single process only).
    redis_url: str | None = None

    # --- ingestion pipeline -------------------------------------------------
    ingest_queue_size: int = 100_000      # max buffered location updates
    ingest_batch_size: int = 2_000        # max updates per DB round-trip
    ingest_flush_interval_ms: int = 200   # max time an update waits in a batch
    ingest_submit_timeout_s: float = 2.0  # HTTP producers wait this long when the queue is full
    ingest_workers: int = 1               # concurrent batch processors (>1 relaxes per-device event ordering)
    store_location_history: bool = True   # append every point to device_locations
    alert_repeat_inside: bool = True      # also emit "inside" alerts on every report inside a zone

    # --- websocket fan-out --------------------------------------------------
    ws_alert_backlog: int = 1_000         # alert messages buffered per slow client
    ws_location_backlog: int = 8          # location batches buffered per slow client (oldest dropped)

    # --- misc ---------------------------------------------------------------
    instance_id: str = Field(default_factory=socket.gethostname)
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
