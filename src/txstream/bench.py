"""Processor throughput: exactly-once against at-least-once, at several batch sizes.

Events are published first; the clock runs from the processor's start to its last committed
batch, so group join and state restore are included and the final idle wait is not.
"""

from __future__ import annotations

import os
import platform
import time
from dataclasses import dataclass

from txstream import streams
from txstream.generator import generate
from txstream.processor import Mode, Processor, Stats
from txstream.streams import Topics

CONFIGS: list[tuple[Mode, int]] = [("at-least-once", 500), ("exactly-once", 100), ("exactly-once", 500), ("exactly-once", 2000)]


@dataclass
class Row:
    mode: str
    batch_size: int
    events: int
    seconds: float
    batches: int

    @property
    def per_second(self) -> float:
        return self.events / self.seconds if self.seconds else 0.0


def run(bootstrap: str, events: int = 100_000, configs: list[tuple[Mode, int]] = CONFIGS) -> list[Row]:
    stream = generate(events, seed=9)
    rows = []
    for mode, batch in configs:
        topics = Topics(f"bench-{mode.replace('-', '')}-{batch}-{time.time_ns()}")
        streams.create(bootstrap, topics)
        streams.publish(bootstrap, topics.payments, stream)
        processor = Processor(bootstrap, topics, mode, batch_size=batch)
        last = [time.monotonic()]

        def mark(_: Stats, last: list[float] = last) -> None:
            last[0] = time.monotonic()

        stats = processor.run(idle_exit=2.0, on_batch=mark)
        rows.append(Row(mode, batch, stats.events, last[0] - stats.started, stats.batches))
    return rows


def markdown(rows: list[Row], events: int) -> str:
    lines = [
        "# Processor throughput",
        "",
        f"{events:,} gateway events per run, one processor on one broker (6 partitions), {os.cpu_count()} CPUs, "
        f"{platform.system()} {platform.machine()}. Includes group join and state restore.",
        "",
        "| Guarantee | Events per transaction or commit | Events | Batches | Seconds | Events per second |",
        "|---|---|---|---|---|---|",
    ]
    lines += [f"| {r.mode} | up to {r.batch_size:,} | {r.events:,} | {r.batches:,} | {r.seconds:.1f} | {r.per_second:,.0f} |" for r in rows]
    return "\n".join(lines) + "\n"
