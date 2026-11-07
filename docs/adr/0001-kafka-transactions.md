# ADR-0001: Kafka transactions for consume-process-produce

**Status:** accepted

## Context

The processor writes outputs, changes its own state and advances its input offsets. Done as
three separate writes, a crash between them either replays an event (duplicates) or, if the
state change landed and the outputs did not, loses it. The fault-injection run shows the second
case with an idempotent producer and commit-after-flush.

## Decision

Write outputs, state changes and input offsets in one Kafka transaction, keep the state in a
compacted changelog topic co-partitioned with the input, and read everything with
`read_committed`.

## Consequences

- Exactly-once inside Kafka, demonstrated under repeated SIGKILL.
- Throughput depends on events per transaction: about 1% overhead at 500, more than half at 100.
- Downstream consumers must use `read_committed`.
