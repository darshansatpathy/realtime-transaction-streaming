# ADR-0003: Prove exactly-once with SIGKILL and an independent reconciliation

**Status:** accepted

## Context

Exactly-once claims are easy to make and hard to observe: a pipeline that never crashes in
testing never shows the difference.

## Decision

Run the processor and sink as processes and kill them with SIGKILL at random moments, then
reconcile against the ledger computed outside the pipeline. Run the same harness against an
at-least-once build for comparison, and gate CI on the exactly-once result.

## Consequences

- The comparison is concrete: in the recorded run the at-least-once build lost 68 postings and
  left 12 events half-booked.
- Runs are randomized; the exactly-once result must hold for every run, while the at-least-once
  result varies with where the kills land.
