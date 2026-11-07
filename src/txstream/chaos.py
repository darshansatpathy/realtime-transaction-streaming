"""Fault injection: run the processor and the sink as separate processes, kill them with SIGKILL
at random moments, restart them, and reconcile when the stream has drained.

SIGKILL gives no chance to flush, abort or commit: whatever the client had buffered is lost and
whatever transaction was open stays open until the next instance fences it.
"""

from __future__ import annotations

import random
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

from txstream import streams
from txstream.generator import generate
from txstream.reconcile import Reconciliation, reconcile
from txstream.streams import Topics


@dataclass
class ChaosResult:
    mode: str
    prefix: str
    kills: dict[str, int] = field(default_factory=lambda: {"processor": 0, "sink": 0})
    seconds: float = 0.0
    reconciliation: Reconciliation | None = None


class Worker:
    def __init__(self, role: str, args: list[str], log: Path) -> None:
        self.role, self.args, self.log = role, args, log
        self.proc: subprocess.Popen[bytes] | None = None
        self.done = False

    def ensure(self) -> None:
        if self.done:
            return
        if self.proc is not None and self.proc.poll() is not None:
            code = self.proc.returncode
            self.proc = None
            if code == 0:
                self.done = True
                return
        if self.proc is None:
            with self.log.open("ab") as fh:
                self.proc = subprocess.Popen(self.args, stdout=fh, stderr=subprocess.STDOUT)  # noqa: S603 - our own CLI

    def kill(self) -> bool:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
            self.proc = None
            return True
        return False


def schema_dsn(dsn: str, schema: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    sep = "&" if "?" in dsn else "?"
    return f"{dsn}{sep}options=-csearch_path%3D{schema}"


def run(
    bootstrap: str,
    dsn: str,
    events: int = 20_000,
    mode: str = "exactly-once",
    kills: int = 12,
    seed: int = 1,
    workdir: Path = Path("chaos-logs"),
    timeout: float = 600.0,
) -> ChaosResult:
    rng = random.Random(seed)
    prefix = f"chaos-{mode.replace('-', '')}-{int(time.time() * 1000)}"
    topics = Topics(prefix)
    streams.create(bootstrap, topics)
    stream = generate(events, seed=seed)
    streams.publish(bootstrap, topics.payments, stream)
    target = schema_dsn(dsn, prefix.replace("-", "_"))
    workdir.mkdir(parents=True, exist_ok=True)
    base = [sys.executable, "-m", "txstream.cli", "--bootstrap", bootstrap, "--prefix", prefix]
    processor = Worker("processor", [*base, "process", "--mode", mode, "--idle-exit", "4"], workdir / f"{prefix}-processor.log")
    sink = Worker("sink", [*base, "sink", "--dsn", target, "--idle-exit", "4"], workdir / f"{prefix}-sink.log")
    result = ChaosResult(mode, prefix)
    started = time.monotonic()
    processor.ensure()
    sink.ensure()
    remaining = kills
    while time.monotonic() - started < timeout:
        time.sleep(rng.uniform(0.4, 1.6))
        if remaining > 0:
            victim = processor if rng.random() < 0.65 else sink
            if victim.kill():
                result.kills[victim.role] += 1
                remaining -= 1
        processor.ensure()
        if processor.done:
            # The sink may only finish once nothing more can arrive.
            sink.ensure()
            if sink.done:
                break
        elif sink.proc is None and not sink.done:
            sink.ensure()
        elif sink.done:
            sink.done = False
            sink.ensure()
    else:
        raise TimeoutError(f"chaos run {prefix} did not drain in {timeout}s")
    result.seconds = time.monotonic() - started
    result.reconciliation = reconcile(bootstrap, topics, target, stream)
    return result
