from __future__ import annotations

import os
import time

import psycopg
import pytest

from txstream import streams


@pytest.fixture(scope="session")
def bootstrap() -> str:
    value = os.environ.get("TXS_BOOTSTRAP")
    if not value:
        pytest.skip("TXS_BOOTSTRAP is not set (a Kafka broker for integration tests)")
    return value


@pytest.fixture(scope="session")
def dsn() -> str:
    value = os.environ.get("TXS_DSN")
    if not value:
        pytest.skip("TXS_DSN is not set (a PostgreSQL database for integration tests)")
    return value


@pytest.fixture
def topics(bootstrap: str, request: pytest.FixtureRequest) -> streams.Topics:
    t = streams.Topics(f"test-{request.node.name[:30].replace('_', '-')}-{time.time_ns()}")
    streams.create(bootstrap, t)
    return t


@pytest.fixture
def schema(dsn: str, topics: streams.Topics) -> str:
    from txstream.chaos import schema_dsn

    return schema_dsn(dsn, topics.prefix.replace("-", "_"))


def rows(dsn: str, sql: str) -> list[tuple[object, ...]]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(sql.encode()).fetchall()
