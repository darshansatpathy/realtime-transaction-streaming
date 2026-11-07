"""A payment gateway's output: top-ups, card payments and refunds, with some sent twice.

Events for one account are produced in order (they share a key, so they share a partition),
which is what a gateway keyed by account gives you. About 2% of events are re-sent with the
same event id, as a gateway does when it retries after a timeout.
"""

from __future__ import annotations

import datetime as dt
import random

from txstream.model import Event

MERCHANTS = [f"merchant-{i:03d}" for i in range(1, 41)]


def generate(n: int, accounts: int = 500, seed: int = 5, duplicate_rate: float = 0.02) -> list[Event]:
    rng = random.Random(seed)
    clock = dt.datetime(2026, 1, 1, 9, 0, 0)
    payments: dict[str, list[tuple[str, int, str]]] = {}
    events: list[Event] = []
    for i in range(n):
        account = f"acct-{rng.randrange(accounts):05d}"
        clock += dt.timedelta(milliseconds=rng.randint(1, 200))
        roll = rng.random()
        event_id = f"evt-{seed}-{i:08d}"
        if roll < 0.20:
            event = Event(event_id, "topup", account, rng.choice([5_000, 10_000, 20_000, 50_000]), ts=clock.isoformat())
        elif roll < 0.93 or not payments.get(account):
            merchant = rng.choice(MERCHANTS)
            amount = rng.choice([150, 499, 1_250, 2_999, 7_500, 15_000, 40_000])
            event = Event(event_id, "payment", account, amount, merchant=merchant, ts=clock.isoformat())
            payments.setdefault(account, []).append((event_id, amount, merchant))
        else:
            original, amount, merchant = rng.choice(payments[account])
            event = Event(
                event_id,
                "refund",
                account,
                rng.choice([amount, amount // 2, amount + 100]),
                merchant=merchant,
                refund_of=original,
                ts=clock.isoformat(),
            )
        if rng.random() < 0.002:
            event = Event(event.event_id, event.type, event.account, event.amount_minor, "USD", event.merchant, event.refund_of, event.ts)
        events.append(event)
        if rng.random() < duplicate_rate:
            events.append(event)  # a gateway retry: same event, sent again
    return events
