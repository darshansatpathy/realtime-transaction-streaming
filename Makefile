.PHONY: install check lint test chaos bench

install:
	pip install -e ".[dev]"

lint:
	ruff check .
	ruff format --check .
	mypy

# Needs TXS_BOOTSTRAP and TXS_DSN for the integration tests.
test:
	pytest --cov --cov-report=term-missing

check: lint test

chaos:
	txs chaos --events 40000 --kills 12 --mode both --report docs/fault-injection.md

bench:
	txs bench --events 100000 --report docs/throughput.md
