from __future__ import annotations

from collections import Counter

from txstream.generator import generate
from txstream.model import OVERDRAFT_MINOR, AccountState, Event, Ledger, Posting, apply


def test_payment_rules_and_double_entry() -> None:
    s = AccountState()
    legs, rej = apply(s, Event("t1", "topup", "a", 10_000))
    assert rej is None and [p.amount_minor for p in legs] == [10_000, -10_000] and s.balance_minor == 10_000
    legs, rej = apply(s, Event("p1", "payment", "a", 12_000, merchant="m"))
    assert rej is None and legs[0].account == "a" and legs[0].amount_minor == -12_000 and legs[1].account == "m"
    assert s.balance_minor == -2_000
    _, rej = apply(s, Event("p2", "payment", "a", 3_001 + OVERDRAFT_MINOR - 5_000 + 5_000 - 2_000 + 1, merchant="m"))
    assert rej is not None and rej.reason == "insufficient funds" and s.balance_minor == -2_000
    _, rej = apply(s, Event("r0", "refund", "a", 100, merchant="m", refund_of="p2"))
    assert rej is not None and "unknown or rejected" in rej.reason
    _, rej = apply(s, Event("r1", "refund", "a", 12_001, merchant="m", refund_of="p1"))
    assert rej is not None and "exceeds" in rej.reason
    legs, rej = apply(s, Event("r2", "refund", "a", 12_000, merchant="m", refund_of="p1"))
    assert rej is None and s.balance_minor == 10_000 and s.refundable["p1"] == 0
    for bad, reason in [
        (Event("x", "payment", "a", 0, merchant="m"), "positive"),
        (Event("y", "payment", "a", 5, "USD", "m"), "currency"),
        (Event("z", "payment", "a", 5), "merchant"),
        (Event("w", "chargeback", "a", 5), "unknown event type"),
    ]:  # type: ignore[arg-type]
        _, rej = apply(s, bad)
        assert rej is not None and reason in rej.reason


def test_ledger_reference_applies_each_event_once() -> None:
    led = Ledger()
    e = Event("t1", "topup", "a", 500)
    led.process(e)
    led.process(e)
    assert len(led.postings) == 2 and led.balances() == {"a": 500, "funding": -500}


def test_serialisation_round_trips() -> None:
    e = Event("p", "payment", "a", 5, merchant="m", ts="2026-01-01T00:00:00")
    assert Event.from_json(e.to_json()) == e
    p = Posting("p:a", "p", "a", "m", -5, "payment")
    assert Posting.from_json(p.to_json()) == p
    st = AccountState(7, {"p": 5})
    assert AccountState.from_json(st.to_json()) == st


def test_generator_shape() -> None:
    events = generate(5000, seed=3)
    assert events == generate(5000, seed=3)
    ids = Counter(e.event_id for e in events)
    assert len(ids) == 5000 and 50 < sum(n - 1 for n in ids.values()) < 160
    assert {e.type for e in events} == {"topup", "payment", "refund"}
    assert any(e.currency != "EUR" for e in events)
    led = Ledger()
    for e in events:
        led.process(e)
    assert sum(led.balances().values()) == 0 and led.rejections
