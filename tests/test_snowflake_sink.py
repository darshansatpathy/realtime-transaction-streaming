from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from txstream import snowflake_sink as sf


class Recorder:
    def __init__(self, fail_on: str | None = None) -> None:
        self.statements: list[str] = []
        self.fail_on = fail_on
        self.result: list[tuple[Any, ...]] = [(0, 10), (3, 7)]

    def cursor(self) -> Recorder:
        return self

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        self.statements.append(sql.split()[0] + (" " + sql.split()[2] if sql.startswith("MERGE") else ""))
        if self.fail_on and sql.startswith(self.fail_on):
            raise RuntimeError("boom")

    def executemany(self, sql: str, params: Sequence[Sequence[Any]]) -> None:
        self.statements.append(f"INSERT x{len(params)}")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.result


def test_batch_and_offsets_commit_together() -> None:
    conn = Recorder()
    sf.write_batch(conn, "ledger", [("p:a", "p", "a", "m", -5, "payment", 0, 9)], {0: 10, 3: 7})
    assert conn.statements == [
        "BEGIN",
        "CREATE",
        "TRUNCATE",
        "INSERT x1",
        "MERGE LEDGER_POSTINGS",
        "MERGE SINK_OFFSETS",
        "MERGE SINK_OFFSETS",
        "COMMIT",
    ]


def test_failure_rolls_back_rows_and_offsets() -> None:
    conn = Recorder(fail_on="MERGE INTO SINK_OFFSETS")
    with pytest.raises(RuntimeError):
        sf.write_batch(conn, "ledger", [], {0: 1})
    assert conn.statements[-1] == "ROLLBACK" and "COMMIT" not in conn.statements


def test_setup_and_stored_offsets() -> None:
    conn = Recorder()
    sf.setup(conn)
    assert conn.statements == ["CREATE", "CREATE"]
    assert sf.stored_offsets(conn, "ledger") == {0: 10, 3: 7}
