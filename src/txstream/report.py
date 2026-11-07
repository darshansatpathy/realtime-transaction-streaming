"""Markdown for the chaos and benchmark results."""

from __future__ import annotations

from txstream.chaos import ChaosResult


def chaos_markdown(results: list[ChaosResult]) -> str:
    lines = ["# Fault injection", ""]
    for r in results:
        rec = r.reconciliation
        if rec is None:
            continue
        verdict = "reconciles exactly" if rec.passed else "does not reconcile"
        lines += [
            f"## {r.mode}: {verdict}",
            "",
            f"{rec.counts['events_sent']:,} gateway events ({rec.counts['unique_events']:,} unique), processor killed "
            f"{r.kills['processor']} times and sink killed {r.kills['sink']} times with SIGKILL, drained in {r.seconds:.0f} s.",
            "",
            "| Check | Result | Detail |",
            "|---|---|---|",
        ]
        lines += [f"| {c.name} | {'pass' if c.passed else '**FAIL**'} | {c.detail} |" for c in rec.checks]
        lines += [
            "",
            f"Ledger topic: {rec.counts['ledger_records']:,} records for {rec.counts['expected_postings']:,} expected postings "
            f"({rec.counts['ledger_duplicates']:,} duplicates). Database: {rec.counts['database_rows']:,} rows.",
            "",
        ]
    return "\n".join(lines)
