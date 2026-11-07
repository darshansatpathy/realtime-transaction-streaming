"""Ledger sinks that are exactly-once by storing their Kafka offsets in the target database.

Each batch of postings and the offsets after it are written in one database transaction, and
on start the sink seeks to the offsets the database holds. Kafka's own committed offsets are
not used for correctness. A crash between the database commit and anything else replays
nothing; a crash before it rolls back both the rows and the offsets together.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import psycopg
from confluent_kafka import Consumer, KafkaError, Message, TopicPartition

from txstream.model import Posting
from txstream.processor import caught_up
from txstream.streams import Topics

SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger_postings (
    posting_id   text PRIMARY KEY,
    event_id     text NOT NULL,
    account      text NOT NULL,
    counterparty text NOT NULL,
    amount_minor bigint NOT NULL,
    type         text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ledger_postings_account ON ledger_postings (account);
CREATE TABLE IF NOT EXISTS sink_offsets (
    topic        text NOT NULL,
    partition    integer NOT NULL,
    next_offset  bigint NOT NULL,
    PRIMARY KEY (topic, partition)
);
"""


@dataclass
class SinkStats:
    batches: int = 0
    rows: int = 0
    conflicts: int = 0  # rows whose posting_id was already loaded: always 0 when the processor is exactly-once


class PostgresSink:
    def __init__(self, bootstrap: str, topics: Topics, dsn: str, batch_size: int = 1000, batch_wait: float = 0.2) -> None:
        self.topics = topics
        self.dsn = dsn
        self.batch_size = batch_size
        self.batch_wait = batch_wait
        self.stats = SinkStats()
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(SCHEMA)
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": topics.sink_group,
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
                "auto.offset.reset": "earliest",
                "group.instance.id": f"{topics.sink_group}-0",
                "session.timeout.ms": 10000,
            }
        )

    def _offsets(self) -> dict[int, int]:
        with psycopg.connect(self.dsn) as conn:
            rows = conn.execute("SELECT partition, next_offset FROM sink_offsets WHERE topic = %s", [self.topics.ledger]).fetchall()
        return {int(p): int(o) for p, o in rows}

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        stored = self._offsets()
        for tp in partitions:
            tp.offset = stored.get(tp.partition, 0)
        consumer.assign(partitions)

    def run(self, idle_exit: float | None = None, max_seconds: float | None = None) -> SinkStats:
        self.consumer.subscribe([self.topics.ledger], on_assign=self._on_assign)
        started = idle_since = time.monotonic()
        try:
            with psycopg.connect(self.dsn) as conn:
                while True:
                    msgs = [m for m in self.consumer.consume(num_messages=self.batch_size, timeout=self.batch_wait) if not _eof(m)]
                    now = time.monotonic()
                    if max_seconds is not None and now - started > max_seconds:
                        break
                    if not msgs:
                        if idle_exit is not None and now - idle_since > idle_exit and self._caught_up():
                            break
                        continue
                    idle_since = now
                    self._write(conn, msgs)
        finally:
            self.consumer.close()
        return self.stats

    def _caught_up(self) -> bool:
        return caught_up(self.consumer, self._offsets())

    def _write(self, conn: psycopg.Connection[tuple[object, ...]], msgs: list[Message]) -> None:
        rows: list[tuple[object, ...]] = []
        next_offsets: dict[int, int] = {}
        for m in msgs:
            if m.error() is not None:
                raise RuntimeError(str(m.error()))
            p = Posting.from_json(m.value() or b"")
            part, off = int(m.partition() or 0), int(m.offset() or 0)
            rows.append((p.posting_id, p.event_id, p.account, p.counterparty, p.amount_minor, p.type, part, off))
            next_offsets[part] = max(next_offsets.get(part, 0), off + 1)
        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO ledger_postings (posting_id, event_id, account, counterparty, amount_minor, type, kafka_partition, kafka_offset)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (posting_id) DO NOTHING",
                    rows,
                )
                inserted = cur.rowcount
            conn.execute(
                "INSERT INTO sink_offsets (topic, partition, next_offset) SELECT %s, unnest(%s::int[]), unnest(%s::bigint[])"
                " ON CONFLICT (topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset",
                [self.topics.ledger, list(next_offsets), list(next_offsets.values())],
            )
        self.stats.batches += 1
        self.stats.rows += len(rows)
        self.stats.conflicts += len(rows) - max(inserted, 0)


def _eof(msg: Message) -> bool:
    err = msg.error()
    return err is not None and err.code() == KafkaError._PARTITION_EOF
