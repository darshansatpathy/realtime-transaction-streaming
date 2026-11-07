"""Snowflake ledger sink: the same offsets-with-the-data pattern as the PostgreSQL sink.

Each batch is staged into a temporary table and merged into LEDGER_POSTINGS, and the next
offsets are merged into SINK_OFFSETS, inside one explicit transaction. On start the sink seeks
to the offsets Snowflake holds.

This module is exercised against a recording connection in the tests, which checks the
statements and their order. It has not been run against a Snowflake account here.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

DDL = [
    """CREATE TABLE IF NOT EXISTS LEDGER_POSTINGS (
        POSTING_ID STRING PRIMARY KEY, EVENT_ID STRING NOT NULL, ACCOUNT STRING NOT NULL, COUNTERPARTY STRING NOT NULL,
        AMOUNT_MINOR NUMBER(38, 0) NOT NULL, TYPE STRING NOT NULL, KAFKA_PARTITION NUMBER(10, 0) NOT NULL,
        KAFKA_OFFSET NUMBER(38, 0) NOT NULL, LOADED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS SINK_OFFSETS (
        TOPIC STRING NOT NULL, PARTITION NUMBER(10, 0) NOT NULL, NEXT_OFFSET NUMBER(38, 0) NOT NULL, PRIMARY KEY (TOPIC, PARTITION))""",
]
STAGE = "CREATE TEMPORARY TABLE IF NOT EXISTS STAGE_POSTINGS LIKE LEDGER_POSTINGS"
INSERT_STAGE = (
    "INSERT INTO STAGE_POSTINGS (POSTING_ID, EVENT_ID, ACCOUNT, COUNTERPARTY, AMOUNT_MINOR, TYPE, KAFKA_PARTITION, KAFKA_OFFSET)"
    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
)
# Snowflake does not enforce primary keys, so idempotence comes from the MERGE, not the constraint.
MERGE_POSTINGS = """MERGE INTO LEDGER_POSTINGS t USING STAGE_POSTINGS s ON t.POSTING_ID = s.POSTING_ID
    WHEN NOT MATCHED THEN INSERT (POSTING_ID, EVENT_ID, ACCOUNT, COUNTERPARTY, AMOUNT_MINOR, TYPE, KAFKA_PARTITION, KAFKA_OFFSET)
    VALUES (s.POSTING_ID, s.EVENT_ID, s.ACCOUNT, s.COUNTERPARTY, s.AMOUNT_MINOR, s.TYPE, s.KAFKA_PARTITION, s.KAFKA_OFFSET)"""
MERGE_OFFSET = """MERGE INTO SINK_OFFSETS t USING (SELECT %s AS TOPIC, %s AS PARTITION, %s AS NEXT_OFFSET) s
    ON t.TOPIC = s.TOPIC AND t.PARTITION = s.PARTITION
    WHEN MATCHED THEN UPDATE SET NEXT_OFFSET = s.NEXT_OFFSET
    WHEN NOT MATCHED THEN INSERT (TOPIC, PARTITION, NEXT_OFFSET) VALUES (s.TOPIC, s.PARTITION, s.NEXT_OFFSET)"""


class Cursor(Protocol):
    def execute(self, sql: str, params: Sequence[Any] | None = ...) -> Any: ...
    def executemany(self, sql: str, params: Sequence[Sequence[Any]]) -> Any: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...


def setup(conn: Connection) -> None:
    cur = conn.cursor()
    for statement in DDL:
        cur.execute(statement)


def stored_offsets(conn: Connection, topic: str) -> dict[int, int]:
    cur = conn.cursor()
    cur.execute("SELECT PARTITION, NEXT_OFFSET FROM SINK_OFFSETS WHERE TOPIC = %s", [topic])
    return {int(p): int(o) for p, o in cur.fetchall()}


def write_batch(conn: Connection, topic: str, rows: list[tuple[Any, ...]], next_offsets: dict[int, int]) -> None:
    """rows: (posting_id, event_id, account, counterparty, amount_minor, type, partition, offset)."""
    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        cur.execute(STAGE)
        cur.execute("TRUNCATE TABLE STAGE_POSTINGS")
        cur.executemany(INSERT_STAGE, rows)
        cur.execute(MERGE_POSTINGS)
        for partition, offset in sorted(next_offsets.items()):
            cur.execute(MERGE_OFFSET, [topic, partition, offset])
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise


def connect(**kwargs: Any) -> Connection:  # pragma: no cover - needs an account
    import snowflake.connector

    conn: Connection = snowflake.connector.connect(**kwargs)
    return conn
