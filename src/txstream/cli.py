"""Command line: topics, publish, process, sink, reconcile, chaos, bench."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from txstream import __version__


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="txs", description="Exactly-once payment streaming on Kafka.")
    parser.add_argument("--version", action="version", version=f"txs {__version__}")
    parser.add_argument("--bootstrap", default=os.environ.get("TXS_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--prefix", default=os.environ.get("TXS_PREFIX", "txs"))
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("topics", help="create the topics")
    p = sub.add_parser("publish", help="publish generated gateway events")
    p.add_argument("--events", type=int, default=10_000)
    p.add_argument("--seed", type=int, default=5)

    p = sub.add_parser("process", help="run the processor")
    p.add_argument("--mode", choices=["exactly-once", "at-least-once"], default="exactly-once")
    p.add_argument("--instance", default="0")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--idle-exit", type=float, help="exit after this many idle seconds once caught up")

    p = sub.add_parser("sink", help="run the PostgreSQL ledger sink")
    p.add_argument("--dsn", default=os.environ.get("TXS_DSN"))
    p.add_argument("--idle-exit", type=float)

    p = sub.add_parser("reconcile", help="compare Kafka and the database with the expected ledger")
    p.add_argument("--dsn", default=os.environ.get("TXS_DSN"))
    p.add_argument("--events", type=int, default=10_000)
    p.add_argument("--seed", type=int, default=5)
    p.add_argument("--check", action="store_true")

    p = sub.add_parser("chaos", help="kill the processor and sink at random, then reconcile")
    p.add_argument("--dsn", default=os.environ.get("TXS_DSN"))
    p.add_argument("--events", type=int, default=20_000)
    p.add_argument("--kills", type=int, default=12)
    p.add_argument("--mode", choices=["exactly-once", "at-least-once", "both"], default="both")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--report", type=Path)
    p.add_argument("--check", action="store_true", help="exit 1 unless exactly-once reconciles cleanly")

    p = sub.add_parser("bench", help="processor throughput")
    p.add_argument("--events", type=int, default=100_000)
    p.add_argument("--report", type=Path)

    args = parser.parse_args(argv)
    return int(COMMANDS[args.command](args))


def _topics(args: argparse.Namespace) -> int:
    from txstream import streams

    streams.create(args.bootstrap, streams.Topics(args.prefix))
    return 0


def _publish(args: argparse.Namespace) -> int:
    from txstream import streams
    from txstream.generator import generate

    topics = streams.Topics(args.prefix)
    streams.create(args.bootstrap, topics)
    events = generate(args.events, seed=args.seed)
    streams.publish(args.bootstrap, topics.payments, events)
    print(f"published {len(events)} events to {topics.payments}")
    return 0


def _process(args: argparse.Namespace) -> int:
    from txstream.processor import Processor
    from txstream.streams import Topics

    stats = Processor(args.bootstrap, Topics(args.prefix), args.mode, args.instance, args.batch_size).run(idle_exit=args.idle_exit)
    print(json.dumps(stats.__dict__), flush=True)
    return 0


def _sink(args: argparse.Namespace) -> int:
    from txstream.sink import PostgresSink
    from txstream.streams import Topics

    if not args.dsn:
        print("set --dsn or TXS_DSN", file=sys.stderr)
        return 2
    stats = PostgresSink(args.bootstrap, Topics(args.prefix), args.dsn).run(idle_exit=args.idle_exit)
    print(json.dumps(stats.__dict__), flush=True)
    return 0


def _reconcile(args: argparse.Namespace) -> int:
    from txstream.generator import generate
    from txstream.reconcile import reconcile
    from txstream.streams import Topics

    r = reconcile(args.bootstrap, Topics(args.prefix), args.dsn, generate(args.events, seed=args.seed))
    for c in r.checks:
        print(f"{'PASS' if c.passed else 'FAIL'}  {c.name}: {c.detail}")
    return 1 if args.check and not r.passed else 0


def _chaos(args: argparse.Namespace) -> int:
    from txstream import chaos
    from txstream.report import chaos_markdown

    if not args.dsn:
        print("set --dsn or TXS_DSN", file=sys.stderr)
        return 2
    modes = ["exactly-once", "at-least-once"] if args.mode == "both" else [args.mode]
    results = [chaos.run(args.bootstrap, args.dsn, args.events, m, args.kills, args.seed) for m in modes]
    text = chaos_markdown(results)
    print(text)
    if args.report:
        args.report.write_text(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as fh:
            fh.write(text)
    eos = [r for r in results if r.mode == "exactly-once"]
    return 1 if args.check and eos and not all(r.reconciliation and r.reconciliation.passed for r in eos) else 0


def _bench(args: argparse.Namespace) -> int:
    from txstream.bench import markdown, run

    rows = run(args.bootstrap, args.events)
    text = markdown(rows, args.events)
    print(text)
    if args.report:
        args.report.write_text(text)
    return 0


COMMANDS = {"topics": _topics, "publish": _publish, "process": _process, "sink": _sink, "reconcile": _reconcile, "chaos": _chaos, "bench": _bench}

if __name__ == "__main__":
    sys.exit(main())
