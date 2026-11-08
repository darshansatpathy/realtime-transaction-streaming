"""Against a real broker and database."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from confluent_kafka import KafkaError, KafkaException, Producer

from tests.conftest import rows
from txstream import chaos, streams
from txstream.bench import markdown
from txstream.bench import run as bench
from txstream.cli import main
from txstream.generator import generate
from txstream.processor import Processor
from txstream.reconcile import reconcile
from txstream.report import chaos_markdown
from txstream.sink import PostgresSink


def test_end_to_end_reconciles(bootstrap: str, topics: streams.Topics, schema: str) -> None:
    events = generate(4000, seed=11)
    streams.publish(bootstrap, topics.payments, events)
    stats = Processor(bootstrap, topics).run(idle_exit=2)
    assert stats.events == 4000 and stats.duplicates_skipped == len(events) - 4000
    sink = PostgresSink(bootstrap, topics, schema).run(idle_exit=2)
    assert sink.conflicts == 0
    r = reconcile(bootstrap, topics, schema, events)
    assert r.passed, [c for c in r.checks if not c.passed]
    assert streams.lag(bootstrap, topics.payments, topics.processor_group) == 0


def test_restart_restores_state_and_resumes(bootstrap: str, topics: streams.Topics, schema: str) -> None:
    events = generate(6000, seed=12)
    streams.publish(bootstrap, topics.payments, events)
    first = Processor(bootstrap, topics, batch_size=200)
    first.fault = lambda stage: None
    done = {"n": 0}

    def stop_early(stats: object) -> None:
        done["n"] += 1
        if done["n"] == 5:
            raise KeyboardInterrupt  # stop after five committed batches, as if the process were stopped

    with pytest.raises(KeyboardInterrupt):
        first.run(on_batch=stop_early)  # type: ignore[arg-type]
    assert 0 < streams.lag(bootstrap, topics.payments, topics.processor_group) < len(events)
    second = Processor(bootstrap, topics, batch_size=200)
    stats = second.run(idle_exit=2)
    assert stats.restored_keys > 0
    PostgresSink(bootstrap, topics, schema).run(idle_exit=2)
    assert reconcile(bootstrap, topics, schema, events).passed


def test_aborted_transaction_is_retried_without_duplicates(bootstrap: str, topics: streams.Topics, schema: str) -> None:
    events = generate(1500, seed=13)
    streams.publish(bootstrap, topics.payments, events)
    p = Processor(bootstrap, topics, batch_size=300)
    calls = {"n": 0}

    def flaky(stage: str) -> None:
        calls["n"] += 1
        if calls["n"] == 2:  # the second transaction fails after its outputs were produced
            raise KafkaException(KafkaError(KafkaError._TIMED_OUT, "injected", txn_requires_abort=True))

    p.fault = flaky
    stats = p.run(idle_exit=2)
    assert stats.aborted >= 1  # the injected abort, and any follow-on abort while the producer recovers
    PostgresSink(bootstrap, topics, schema).run(idle_exit=2)
    assert reconcile(bootstrap, topics, schema, events).passed


def test_a_newer_instance_fences_the_old_one(bootstrap: str, topics: streams.Topics) -> None:
    old = Producer({"bootstrap.servers": bootstrap, "transactional.id": f"{topics.prefix}.fence"})
    old.init_transactions(30)
    old.begin_transaction()
    new = Producer({"bootstrap.servers": bootstrap, "transactional.id": f"{topics.prefix}.fence"})
    new.init_transactions(30)
    old.produce(topics.ledger, value=b"zombie")
    with pytest.raises(KafkaException) as err:
        old.commit_transaction(10)
    assert err.value.args[0].fatal() or "fenced" in str(err.value).lower()
    assert not [v for *_, v in streams.read_all(bootstrap, topics.ledger) if v == b"zombie"]


def test_sink_resumes_from_database_offsets(bootstrap: str, topics: streams.Topics, schema: str) -> None:
    events = generate(3000, seed=14)
    streams.publish(bootstrap, topics.payments, events)
    Processor(bootstrap, topics).run(idle_exit=2)
    PostgresSink(bootstrap, topics, schema, batch_size=200).run(max_seconds=3)
    first_rows = rows(schema, "SELECT count(*) FROM ledger_postings")[0][0]
    again = PostgresSink(bootstrap, topics, schema).run(idle_exit=2)
    total = rows(schema, "SELECT count(*) FROM ledger_postings")[0][0]
    assert again.conflicts == 0 and again.rows == total - first_rows, "the second sink read only what the first had not loaded"
    assert reconcile(bootstrap, topics, schema, events).passed


def test_chaos_exactly_once_reconciles(bootstrap: str, dsn: str, tmp_path: Path) -> None:
    result = chaos.run(bootstrap, dsn, events=15_000, mode="exactly-once", kills=8, seed=4, workdir=tmp_path)
    assert result.reconciliation is not None and result.reconciliation.passed, chaos_markdown([result])
    assert sum(result.kills.values()) >= 4


def test_cli(bootstrap: str, dsn: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    prefix = f"cli-{time.time_ns()}"
    target = chaos.schema_dsn(dsn, prefix.replace("-", "_"))
    base = ["--bootstrap", bootstrap, "--prefix", prefix]
    assert main([*base, "topics"]) == 0
    assert main([*base, "publish", "--events", "2000", "--seed", "21"]) == 0
    assert main([*base, "process", "--idle-exit", "2"]) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["events"] == 2000
    assert main([*base, "sink", "--dsn", target, "--idle-exit", "2"]) == 0
    assert main([*base, "reconcile", "--dsn", target, "--events", "2000", "--seed", "21", "--check"]) == 0
    report = tmp_path / "chaos.md"
    assert main([*base, "chaos", "--dsn", dsn, "--events", "3000", "--kills", "2", "--mode", "exactly-once", "--report", str(report), "--check"]) == 0
    assert "reconciles exactly" in report.read_text()


def test_bench(bootstrap: str) -> None:
    out = bench(bootstrap, 3000, [("exactly-once", 500), ("at-least-once", 500)])
    assert [r.events for r in out] == [3000, 3000] and all(r.per_second > 0 for r in out)
    assert "Events per second" in markdown(out, 3000)
