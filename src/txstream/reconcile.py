"""Independent reconciliation: what the ledger must contain, against what Kafka and the database hold.

The expected outcome comes from applying the gateway's events once each, in order per account,
with the same business rules, outside the pipeline. Then:

- the ledger topic (committed records) must hold every expected posting exactly once;
- the rejections topic must hold every expected rejection exactly once;
- the database must hold exactly the expected postings, balance per account and per event
  (double entry), and match the reference balances.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field

import psycopg

from txstream import streams
from txstream.model import Event, Ledger, Posting
from txstream.streams import Topics


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


@dataclass
class Reconciliation:
    checks: list[Check] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def add(self, name: str, passed: bool, detail: str) -> None:
        self.checks.append(Check(name, passed, detail))


def expected(events: list[Event]) -> Ledger:
    ledger = Ledger()
    for e in events:
        ledger.process(e)
    return ledger


def reconcile(bootstrap: str, topics: Topics, dsn: str, events: list[Event]) -> Reconciliation:
    ref = expected(events)
    r = Reconciliation()
    r.counts = {
        "events_sent": len(events),
        "unique_events": len(ref.seen),
        "expected_postings": len(ref.postings),
        "expected_rejections": len(ref.rejections),
    }

    postings = [Posting.from_json(v) for _, _, _, v in streams.read_all(bootstrap, topics.ledger)]
    ids = Counter(p.posting_id for p in postings)
    dup = sum(n - 1 for n in ids.values() if n > 1)
    missing = set(ref.postings) - set(ids)
    unexpected = set(ids) - set(ref.postings)
    wrong = sum(1 for p in postings if p.posting_id in ref.postings and p != ref.postings[p.posting_id])
    r.counts |= {"ledger_records": len(postings), "ledger_duplicates": dup}
    r.add("ledger topic: no posting twice", dup == 0, f"{dup} duplicate records")
    r.add(
        "ledger topic: every expected posting, nothing else", not missing and not unexpected, f"{len(missing)} missing, {len(unexpected)} unexpected"
    )
    r.add("ledger topic: amounts and accounts as expected", wrong == 0, f"{wrong} postings differ from the reference")

    rejected = [json.loads(v)["event_id"] for _, _, _, v in streams.read_all(bootstrap, topics.rejected) if b"event_id" in v]
    rej_ids = Counter(rejected)
    rej_dup = sum(n - 1 for n in rej_ids.values() if n > 1)
    r.counts |= {"rejection_records": len(rejected), "rejection_duplicates": rej_dup}
    r.add(
        "rejections topic: every expected rejection exactly once",
        rej_dup == 0 and set(rej_ids) == set(ref.rejections),
        f"{rej_dup} duplicates, {len(set(ref.rejections) - set(rej_ids))} missing, {len(set(rej_ids) - set(ref.rejections))} unexpected",
    )

    with psycopg.connect(dsn) as conn:
        rows = conn.execute("SELECT posting_id, event_id, account, counterparty, amount_minor, type FROM ledger_postings").fetchall()
        unbalanced = conn.execute("SELECT count(*) FROM (SELECT event_id FROM ledger_postings GROUP BY 1 HAVING sum(amount_minor) <> 0) e").fetchone()
        total = conn.execute("SELECT coalesce(sum(amount_minor), 0) FROM ledger_postings").fetchone()
        balances: dict[str, int] = {
            str(a): int(v) for a, v in conn.execute("SELECT account, sum(amount_minor) FROM ledger_postings GROUP BY 1").fetchall()
        }
    db = {row[0]: Posting(row[0], row[1], row[2], row[3], int(row[4]), row[5]) for row in rows}
    r.counts |= {"database_rows": len(db)}
    r.add(
        "database: exactly the expected postings",
        db == ref.postings,
        f"{len(set(ref.postings) - set(db))} missing, {len(set(db) - set(ref.postings))} unexpected, "
        f"{sum(1 for k in db if k in ref.postings and db[k] != ref.postings[k])} different",
    )
    r.add(
        "database: double entry (every event and the whole ledger sum to zero)",
        bool(unbalanced and unbalanced[0] == 0 and total and int(total[0]) == 0),
        f"{unbalanced[0] if unbalanced else '?'} unbalanced events, ledger total {total[0] if total else '?'}",
    )
    expected_balances = ref.balances()
    off = {a for a in set(expected_balances) | set(balances) if int(balances.get(a, 0)) != expected_balances.get(a, 0)}
    r.add("database: every account balance matches the reference", not off, f"{len(off)} accounts differ")
    return r
