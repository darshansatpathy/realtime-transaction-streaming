"""The payment processor: consume, decide, produce, commit, as one Kafka transaction.

Exactly-once mode: each batch's postings, rejections, state changes and input offsets are
committed atomically. A crash at any point leaves either all of a batch or none of it visible
to read_committed consumers, and the restarted processor resumes from the committed offsets
with the state that matches them, restored from its changelog topic.

At-least-once mode, for comparison: the same logic with an idempotent producer and offsets
committed after the batch is flushed, which is how many pipelines are built. A crash between
writing outputs and committing offsets replays the batch.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Literal

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition

from txstream.model import AccountState, Event, apply
from txstream.streams import Topics

Mode = Literal["exactly-once", "at-least-once"]


def caught_up(consumer: Consumer, stored: dict[int, int] | None = None) -> bool:
    """True when every assigned partition has been read to its end.

    librdkafka reports no position for a partition until a message has been consumed from it,
    so a restarted consumer with nothing left to read falls back to its committed (or stored)
    offset.
    """
    assignment = consumer.assignment()
    if not assignment:
        return False
    committed = {tp.partition: tp.offset for tp in consumer.committed(assignment, timeout=10)} if stored is None else stored
    for tp in consumer.position(assignment):
        _, high = consumer.get_watermark_offsets(tp, timeout=5)
        offset = tp.offset if tp.offset >= 0 else committed.get(tp.partition, -1)
        if offset < high and not (offset < 0 and high == 0):
            return False
    return True


@dataclass
class Stats:
    batches: int = 0
    events: int = 0
    duplicates_skipped: int = 0
    aborted: int = 0
    restored_keys: int = 0
    started: float = field(default_factory=time.monotonic)


class Processor:
    def __init__(
        self, bootstrap: str, topics: Topics, mode: Mode = "exactly-once", instance: str = "0", batch_size: int = 500, batch_wait: float = 0.1
    ) -> None:
        self.bootstrap = bootstrap
        self.topics = topics
        self.mode = mode
        self.batch_size = batch_size
        self.batch_wait = batch_wait
        self.stats = Stats()
        # Fault-injection hook for tests: called with the stage name inside each transaction.
        self.fault: Callable[[str], None] | None = None
        self.accounts: dict[str, AccountState] = {}
        self.seen: set[str] = set()
        common = {"bootstrap.servers": bootstrap}
        producer_conf = common | {"enable.idempotence": True, "linger.ms": 5, "compression.type": "lz4"}
        if mode == "exactly-once":
            producer_conf |= {"transactional.id": f"{topics.prefix}.processor-{instance}", "transaction.timeout.ms": 30000}
        self.producer = Producer(producer_conf)
        self.consumer = Consumer(
            common
            | {
                "group.id": topics.processor_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
                # Static membership: a restarted instance reclaims its partitions at once instead of
                # waiting for the dead member's session to time out.
                "group.instance.id": f"{topics.processor_group}-{instance}",
                "session.timeout.ms": 10000,
            }
        )
        if mode == "exactly-once":
            # Fences any earlier instance with the same transactional id and aborts its open transaction.
            self.producer.init_transactions(30)

    # ---- state ------------------------------------------------------------------------------

    def _restore(self, partitions: list[int]) -> None:
        """Rebuild state for these partitions from the changelog (committed records only)."""
        reader = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": f"{self.topics.prefix}.restore-{time.monotonic_ns()}",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            }
        )
        assignments, ends = [], {}
        for p in partitions:
            low, high = reader.get_watermark_offsets(TopicPartition(self.topics.state, p), timeout=10)
            if high > low:
                assignments.append(TopicPartition(self.topics.state, p, low))
                ends[p] = high
        if assignments:
            reader.assign(assignments)
            while ends:
                for msg in reader.consume(num_messages=1000, timeout=0.5):
                    if msg.error():
                        continue
                    self._load(msg.key(), msg.value())
                for p in list(ends):
                    if reader.position([TopicPartition(self.topics.state, p)])[0].offset >= ends[p]:
                        del ends[p]
        reader.close()

    def _load(self, key: bytes | None, value: bytes | None) -> None:
        if key is None or value is None:
            return
        k = key.decode()
        self.stats.restored_keys += 1
        if k.startswith("acct:"):
            self.accounts[k[5:]] = AccountState.from_json(value)
        elif k.startswith("seen:"):
            self.seen.add(k[5:])

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        self._restore([tp.partition for tp in partitions])

    # ---- the loop -------------------------------------------------------------------------------

    def run(self, idle_exit: float | None = None, max_seconds: float | None = None, on_batch: Callable[[Stats], None] | None = None) -> Stats:
        self.consumer.subscribe([self.topics.payments], on_assign=self._on_assign)
        idle_since = time.monotonic()
        try:
            while True:
                msgs = [m for m in self.consumer.consume(num_messages=self.batch_size, timeout=self.batch_wait) if not self._skip(m)]
                now = time.monotonic()
                if max_seconds is not None and now - self.stats.started > max_seconds:
                    break
                if not msgs:
                    if idle_exit is not None and now - idle_since > idle_exit and self._caught_up():
                        break
                    continue
                idle_since = now
                if self.mode == "exactly-once":
                    self._transactional_batch(msgs)
                else:
                    self._at_least_once_batch(msgs)
                if on_batch:
                    on_batch(self.stats)
        finally:
            self.consumer.close()
        return self.stats

    def _skip(self, msg: Message) -> bool:
        err = msg.error()
        if err is None:
            return False
        if err.code() == KafkaError._PARTITION_EOF:
            return True
        raise KafkaException(err)

    def _caught_up(self) -> bool:
        return caught_up(self.consumer)

    def _decide(self, msgs: list[Message]) -> tuple[dict[str, AccountState], set[str]]:
        """Decide a batch against a copy of the state, producing every output. The copy replaces
        the state only once the batch is committed."""
        accounts: dict[str, AccountState] = {}
        seen: set[str] = set()
        for msg in msgs:
            try:
                event = Event.from_json(msg.value() or b"")
            except (ValueError, TypeError, KeyError):
                self.producer.produce(self.topics.rejected, key=msg.key(), value=b'{"reason":"malformed event"}')
                continue
            if event.event_id in self.seen or event.event_id in seen:
                self.stats.duplicates_skipped += 1
                continue
            seen.add(event.event_id)
            state = accounts.get(event.account) or deepcopy(self.accounts.get(event.account, AccountState()))
            accounts[event.account] = state
            postings, rejection = apply(state, event)
            for posting in postings:
                self.producer.produce(self.topics.ledger, key=posting.account.encode(), value=posting.to_json())
            if rejection:
                self.producer.produce(self.topics.rejected, key=event.account.encode(), value=rejection.to_json())
            partition = int(msg.partition() or 0)
            self.producer.produce(self.topics.state, key=f"seen:{event.event_id}".encode(), value=b"1", partition=partition)
            self.producer.produce(self.topics.state, key=f"acct:{event.account}".encode(), value=state.to_json(), partition=partition)
            self.stats.events += 1
            self.producer.poll(0)
        return accounts, seen

    def _positions(self, msgs: list[Message]) -> list[TopicPartition]:
        last: dict[int, int] = {}
        for m in msgs:
            part, off = int(m.partition() or 0), int(m.offset() or 0)
            last[part] = max(last.get(part, -1), off)
        return [TopicPartition(self.topics.payments, p, o + 1) for p, o in last.items()]

    def _rewind(self, msgs: list[Message]) -> None:
        first: dict[int, int] = {}
        for m in msgs:
            part, off = int(m.partition() or 0), int(m.offset() or 0)
            first[part] = min(first.get(part, off), off)
        for p, o in first.items():
            self.consumer.seek(TopicPartition(self.topics.payments, p, o))

    def _transactional_batch(self, msgs: list[Message]) -> None:
        self.producer.begin_transaction()
        try:
            accounts, seen = self._decide(msgs)
            self.producer.send_offsets_to_transaction(self._positions(msgs), self.consumer.consumer_group_metadata(), 30)
            if self.fault:
                self.fault("before-commit")
            self.producer.commit_transaction(30)
        except KafkaException as exc:
            error = exc.args[0]
            if error.txn_requires_abort():
                self.producer.abort_transaction(30)
                self.stats.aborted += 1
                self._rewind(msgs)
                return
            raise  # fatal, including being fenced by a newer instance
        self.accounts.update(accounts)
        self.seen |= seen
        self.stats.batches += 1

    def _at_least_once_batch(self, msgs: list[Message]) -> None:
        accounts, seen = self._decide(msgs)
        if self.producer.flush(30) != 0:
            raise RuntimeError("outputs not delivered")
        self.consumer.commit(offsets=self._positions(msgs), asynchronous=False)
        self.accounts.update(accounts)
        self.seen |= seen
        self.stats.batches += 1
