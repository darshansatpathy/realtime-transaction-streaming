# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [0.1.1] - 2026-11-08

### Fixed

- The processor aborts and replays a batch on any non-fatal transaction error, not only on errors flagged as
  abortable. After an abort that purged in-flight sends, the next transaction can be refused with a sequence gap
  (librdkafka repairs it with an epoch bump); that refusal was raised as if fatal. Fatal errors, such as being
  fenced by a newer instance, still stop the processor.

## [0.1.0] - 2026-11-08

### Added

- Payment rules (top-ups, payments with an overdraft, refunds) with double-entry postings.
- A gateway generator with retries, an exactly-once transactional processor with a compacted,
  co-partitioned state changelog, and an at-least-once mode for comparison.
- A PostgreSQL sink and a Snowflake sink that store their offsets with the data.
- Independent reconciliation of topics, database, double entry and balances.
- A SIGKILL fault-injection harness, a throughput benchmark, and the `txs` command.
