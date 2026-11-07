"""Events, postings and the business rules, with no Kafka in sight.

The same `Ledger` rules run inside the streaming processor and in the reconciliation reference,
so the reference can say what the ledger must contain without trusting the pipeline.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

EventType = Literal["topup", "payment", "refund"]
CURRENCY = "EUR"
OVERDRAFT_MINOR = 5_000  # each customer may go 50.00 below zero
FUNDING_ACCOUNT = "funding"


@dataclass(frozen=True)
class Event:
    event_id: str
    type: EventType
    account: str
    amount_minor: int
    currency: str = CURRENCY
    merchant: str = ""
    refund_of: str = ""
    ts: str = ""

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes) -> Event:
        return cls(**json.loads(raw))


@dataclass(frozen=True)
class Posting:
    """One leg of a double-entry booking. The legs of an event sum to zero."""

    posting_id: str
    event_id: str
    account: str
    counterparty: str
    amount_minor: int  # positive credits the account, negative debits it
    type: str

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes) -> Posting:
        return cls(**json.loads(raw))


@dataclass(frozen=True)
class Rejection:
    event_id: str
    account: str
    reason: str

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()


@dataclass
class AccountState:
    balance_minor: int = 0
    refundable: dict[str, int] = field(default_factory=dict)  # accepted payment id -> amount still refundable

    def to_json(self) -> bytes:
        return json.dumps({"balance_minor": self.balance_minor, "refundable": self.refundable}, separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes) -> AccountState:
        d = json.loads(raw)
        return cls(int(d["balance_minor"]), {k: int(v) for k, v in d["refundable"].items()})


def _legs(event: Event, account_delta: int, counterparty: str) -> list[Posting]:
    return [
        Posting(f"{event.event_id}:a", event.event_id, event.account, counterparty, account_delta, event.type),
        Posting(f"{event.event_id}:b", event.event_id, counterparty, event.account, -account_delta, event.type),
    ]


def apply(state: AccountState, event: Event) -> tuple[list[Posting], Rejection | None]:
    """Decide one event against the account's state, mutating the state if accepted."""
    if event.currency != CURRENCY:
        return [], Rejection(event.event_id, event.account, f"unsupported currency {event.currency}")
    if event.amount_minor <= 0:
        return [], Rejection(event.event_id, event.account, "amount must be positive")
    if event.type == "topup":
        state.balance_minor += event.amount_minor
        return _legs(event, event.amount_minor, FUNDING_ACCOUNT), None
    if event.type == "payment":
        if not event.merchant:
            return [], Rejection(event.event_id, event.account, "payment without a merchant")
        if state.balance_minor + OVERDRAFT_MINOR < event.amount_minor:
            return [], Rejection(event.event_id, event.account, "insufficient funds")
        state.balance_minor -= event.amount_minor
        state.refundable[event.event_id] = event.amount_minor
        return _legs(event, -event.amount_minor, event.merchant), None
    if event.type == "refund":
        left = state.refundable.get(event.refund_of)
        if left is None:
            return [], Rejection(event.event_id, event.account, "refund of an unknown or rejected payment")
        if event.amount_minor > left:
            return [], Rejection(event.event_id, event.account, "refund exceeds the refundable amount")
        state.refundable[event.refund_of] = left - event.amount_minor
        state.balance_minor += event.amount_minor
        return _legs(event, event.amount_minor, event.merchant), None
    return [], Rejection(event.event_id, event.account, f"unknown event type {event.type}")


@dataclass
class Ledger:
    """The reference: the expected outcome of a stream of events, applied once each in order per account."""

    accounts: dict[str, AccountState] = field(default_factory=dict)
    seen: set[str] = field(default_factory=set)
    postings: dict[str, Posting] = field(default_factory=dict)
    rejections: dict[str, Rejection] = field(default_factory=dict)

    def process(self, event: Event) -> None:
        if event.event_id in self.seen:
            return
        self.seen.add(event.event_id)
        state = self.accounts.setdefault(event.account, AccountState())
        legs, rejection = apply(state, event)
        for p in legs:
            self.postings[p.posting_id] = p
        if rejection:
            self.rejections[rejection.event_id] = rejection

    def balances(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for p in self.postings.values():
            out[p.account] = out.get(p.account, 0) + p.amount_minor
        return out


def as_dict(obj: Any) -> dict[str, Any]:
    return asdict(obj)
