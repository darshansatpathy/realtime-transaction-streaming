"""Kafka plumbing: topics, the gateway producer, and reading a topic back as committed data."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from txstream.model import Event


@dataclass(frozen=True)
class Topics:
    """Every name derives from a prefix, so runs never share topics or consumer groups."""

    prefix: str
    partitions: int = 6

    @property
    def payments(self) -> str:
        return f"{self.prefix}.payments"

    @property
    def ledger(self) -> str:
        return f"{self.prefix}.ledger"

    @property
    def rejected(self) -> str:
        return f"{self.prefix}.payments-rejected"

    @property
    def state(self) -> str:
        return f"{self.prefix}.processor-state"

    @property
    def processor_group(self) -> str:
        return f"{self.prefix}.processor"

    @property
    def sink_group(self) -> str:
        return f"{self.prefix}.sink"


def create(bootstrap: str, topics: Topics) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    wanted = [
        NewTopic(topics.payments, topics.partitions, 1),
        NewTopic(topics.ledger, topics.partitions, 1),
        NewTopic(topics.rejected, topics.partitions, 1),
        # The processor's state: compacted, one partition per input partition (co-partitioned).
        NewTopic(topics.state, topics.partitions, 1, config={"cleanup.policy": "compact", "min.compaction.lag.ms": "60000"}),
    ]
    for future in admin.create_topics(wanted).values():
        try:
            future.result()
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def publish(bootstrap: str, topic: str, events: list[Event]) -> None:
    """The gateway: an idempotent producer, keyed by account so each account's events stay in order."""
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 5, "compression.type": "lz4"})
    for e in events:
        while True:
            try:
                producer.produce(topic, key=e.account.encode(), value=e.to_json())
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    if producer.flush(30) != 0:
        raise RuntimeError("gateway could not deliver every event")


def read_all(bootstrap: str, topic: str, committed_only: bool = True, timeout: float = 30.0) -> list[Any]:
    """Every record currently in a topic: (partition, offset, key, value)."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"reader-{time.monotonic_ns()}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed" if committed_only else "read_uncommitted",
        }
    )
    meta = consumer.list_topics(topic, timeout=10).topics[topic]
    parts = []
    for p in meta.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if high > low:
            parts.append((p, low, high))
    consumer.assign([TopicPartition(topic, p, low) for p, low, _ in parts])
    ends = {p: high for p, _, high in parts}
    out: list[Any] = []
    deadline = time.monotonic() + timeout
    while ends and time.monotonic() < deadline:
        for msg in consumer.consume(num_messages=1000, timeout=0.5):
            if msg.error():
                continue
            out.append((msg.partition(), msg.offset(), msg.key(), msg.value()))
        for p in list(ends):
            position = consumer.position([TopicPartition(topic, p)])[0].offset
            # Transaction markers occupy offsets, so the position can reach the end without a record there.
            if position >= ends[p] or (position < 0 and _drained(consumer, topic, p, ends[p])):
                del ends[p]
    consumer.close()
    return out


def _drained(consumer: Consumer, topic: str, partition: int, end: int) -> bool:
    committed = consumer.position([TopicPartition(topic, partition)])[0].offset
    return committed >= end


def lag(bootstrap: str, topic: str, group: str) -> int:
    """Records in a topic not yet committed by a consumer group."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    meta = consumer.list_topics(topic, timeout=10).topics[topic]
    tps = [TopicPartition(topic, p) for p in meta.partitions]
    committed = {tp.partition: tp.offset for tp in consumer.committed(tps, timeout=10)}
    total = 0
    for tp in tps:
        _, high = consumer.get_watermark_offsets(tp, timeout=10)
        pos = committed[tp.partition]
        total += high - (pos if pos >= 0 else 0)
    consumer.close()
    return total
