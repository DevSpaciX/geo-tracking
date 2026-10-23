"""Shared fixtures.

Unit tests run without any services. Tests marked ``db`` need a PostGIS database
reachable via ``TEST_DATABASE_URL`` (``docker compose --profile test run --rm test``
sets it up); they are skipped when the variable is absent.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings

ROOT = Path(__file__).resolve().parent.parent
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "db: requires a PostGIS database (TEST_DATABASE_URL)")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if TEST_DATABASE_URL:
        return
    skip = pytest.mark.skip(reason="TEST_DATABASE_URL not set")
    for item in items:
        if "db" in item.keywords:
            item.add_marker(skip)


def _ensure_database(url: str) -> None:
    """Create the test database if missing (connect to the maintenance DB)."""
    import asyncpg  # local import: only needed for db tests

    dsn = url.replace("postgresql+asyncpg://", "postgresql://")
    base, _, dbname = dsn.rpartition("/")

    async def _run() -> None:
        conn = await asyncpg.connect(f"{base}/postgres")
        try:
            exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", dbname)
            if not exists:
                await conn.execute(f'CREATE DATABASE "{dbname}"')
        finally:
            await conn.close()

    asyncio.run(_run())


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    assert TEST_DATABASE_URL
    return Settings(database_url=TEST_DATABASE_URL, redis_url=None, ingest_workers=1)


@pytest.fixture(scope="session")
def migrated_database(test_settings: Settings) -> str:
    _ensure_database(test_settings.database_url)
    env = {**os.environ, "DATABASE_URL": test_settings.database_url}
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], cwd=ROOT, env=env, check=True)
    return test_settings.database_url


@pytest.fixture
async def engine(migrated_database: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(migrated_database, pool_size=2, max_overflow=2)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE zone_presence, device_locations, devices, geozones"))
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
