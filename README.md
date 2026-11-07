# Real-time Transaction Streaming

[![CI](https://github.com/darshansatpathy/realtime-transaction-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/darshansatpathy/realtime-transaction-streaming/actions/workflows/ci.yml)
[![CodeQL](https://github.com/darshansatpathy/realtime-transaction-streaming/actions/workflows/codeql.yml/badge.svg)](https://github.com/darshansatpathy/realtime-transaction-streaming/actions/workflows/codeql.yml)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
![Kafka](https://img.shields.io/badge/Kafka-4.3-231F20)
![Fault injection](https://img.shields.io/badge/SIGKILL%20x12-0%20discrepancies-success)
![License](https://img.shields.io/badge/license-MIT-green)

**A payment processor on Kafka that stays exactly-once when it is killed mid-flight, a ledger
sink that stays exactly-once when it is killed mid-write, and a reconciliation that proves it
against an answer computed outside the pipeline.**

```
gateway ──▶ payments ──▶ processor ──────────────▶ ledger ─────────▶ sink ──▶ PostgreSQL (or Snowflake)
 (retries,   (keyed by    one Kafka transaction:    (double-entry     one DB transaction:
  duplicates) account)    postings, rejections,     postings)         rows + Kafka offsets
                          state changelog, input
                          offsets
                                  │                                          │
                                  └──────────── reconciliation ◀──────────────┘
                                   (expected ledger from the gateway's events, applied once each)
```

## The result that matters

[Fault injection](docs/fault-injection.md): 40,799 gateway events, of which 40,000 are unique
because about 2% are retries. The processor and the sink run as separate processes and are
killed with SIGKILL at random moments, 12 times in all (8 processor, 4 sink). Each is
restarted, and when the stream drains everything is reconciled:

| | Exactly-once (this design) | At-least-once (idempotent producer, commit after flush) |
|---|---|---|
| Ledger topic | 65,222 postings, all expected, **0 duplicates, 0 missing** | **68 postings missing** |
| Rejections | all 7,389 exactly once | **4 missing** |
| Database | exactly the expected postings | **68 missing** |
| Double entry | every event sums to zero | **12 events with one leg only**, ledger off by 1,164.48 |
| Account balances | all match the reference | **49 accounts wrong** |

The at-least-once version did not produce duplicates. It lost data, which is worse. Its
"seen this event" marker reached the state topic while the postings it guarded were still in
the client's send buffer when the process died. The restarted processor trusted the marker and
skipped the event. "Commit offsets after the outputs are flushed" doesn't save you when the
state and the outputs aren't written atomically. Kafka transactions make them atomic.

## How exactly-once is achieved

**Processor** (consume, decide, produce, commit, as one transaction):

- Each batch's ledger postings, rejections, state changes (account balances, refundable
  amounts, processed event ids) and the input offsets are written in one Kafka transaction
  (`send_offsets_to_transaction`). Consumers read with `read_committed`, so a batch is visible
  completely or not at all.
- The state lives in a compacted changelog topic, co-partitioned with the input. Records are
  written to the same partition number as the event that changed them. On (re)assignment the
  processor rebuilds its state from the committed changelog, so state and offsets can never
  disagree.
- A batch is decided against a copy of the state. The copy replaces the state only after the
  commit succeeds, so an aborted transaction leaves nothing behind and the batch is re-read.
- A restarted instance with the same `transactional.id` fences the old one: a zombie's late
  commit fails, and its open transaction is aborted.
- Gateway retries are deduplicated by event id, which is part of the transactional state.

**Sink** (offsets live with the data):

- Each batch of postings and the next offset per partition are written in one PostgreSQL
  transaction. On start the sink seeks to the offsets the database holds; Kafka's committed
  offsets aren't used for correctness. The [Snowflake sink](src/txstream/snowflake_sink.py)
  does the same with `MERGE` in an explicit transaction.

**Business rules** ([`model.py`](src/txstream/model.py)): top-ups, card payments against a
balance with a 50.00 overdraft, refunds up to the original amount, currency and amount
validation. Every accepted event becomes two postings that sum to zero.

## Throughput

[One processor, one broker, six partitions, 2 CPUs](docs/throughput.md), 100,000 events,
including group join and state restore:

| Guarantee | Events per transaction | Events per second |
|---|---|---|
| At-least-once | up to 500 | 12,284 |
| Exactly-once | up to 100 | 5,542 |
| Exactly-once | up to 500 | 12,127 |
| Exactly-once | up to 2,000 | 13,779 |

At 500 events per transaction, exactly-once costs about 1% of throughput. With small
transactions the per-commit round trips dominate. The batch size is the lever, and the price is
latency.

## Run it

```bash
docker compose up -d
pip install -e ".[dev]"
export TXS_BOOTSTRAP=localhost:9092 TXS_DSN=postgresql://postgres:txs@localhost:5432/txs

txs publish --events 20000                  # the gateway
txs process --idle-exit 5                   # exactly-once processor
txs sink --idle-exit 5                      # PostgreSQL sink
txs reconcile --events 20000 --check        # Kafka and the database against the expected ledger

txs chaos --events 40000 --kills 12 --mode both --report fault-injection.md   # both guarantees, under SIGKILL
txs bench --events 100000 --report throughput.md
```

## Tests

15 tests, 95% line coverage, against a real Kafka 4.3 broker and PostgreSQL:

- end-to-end reconciliation; a processor stopped mid-stream and restarted (state restored from
  the changelog);
- an injected abortable failure after outputs were produced: the transaction aborts, the batch
  is retried, and there are no duplicates;
- a zombie producer fenced by a newer instance, its write never visible;
- a sink stopped mid-stream resuming from database offsets with no conflicts;
- a 15,000-event fault-injection run that must reconcile exactly;
- the Snowflake sink's statement order and rollback, against a recording connection.

CI runs all of it with Kafka and PostgreSQL service containers, plus a fault-injection run in
both modes with its report in the job summary, CodeQL, pip-audit and Dependabot. Design notes
are in [architecture](docs/architecture.md) and the [decision records](docs/adr).

## Known limits

- One processor instance. The design supports more (each with its own `transactional.id`,
  partitions spread by the consumer group), but only one is exercised here.
- The processed-event-id set grows without bound in the changelog. In production it would
  expire after the gateway's retry window.
- The Snowflake sink has not been run against a Snowflake account. Its statements and
  transaction handling are tested against a recording connection.
- At-least-once results vary from run to run. Which failure you see depends on where the kill
  lands. The exactly-once result has never varied.

## License

MIT. See [LICENSE](LICENSE).
