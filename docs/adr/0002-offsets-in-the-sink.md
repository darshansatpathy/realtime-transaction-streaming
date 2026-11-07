# ADR-0002: The sink stores its offsets in the target database

**Status:** accepted

## Context

A Kafka transaction cannot include a database write. Committing Kafka offsets after the
database commit replays a batch if the process dies in between; committing them before loses
it.

## Decision

Write each batch's rows and the next offsets in one database transaction, and seek to the
stored offsets on start. The primary key on the posting id is a second line of defence, and the
sink counts conflicts so that one would be visible.

## Consequences

- Exactly-once into PostgreSQL, and by the same pattern into Snowflake (MERGE in an explicit
  transaction).
- Kafka's committed offsets for the sink group are informational only.
