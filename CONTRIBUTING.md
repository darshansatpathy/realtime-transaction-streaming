# Contributing

```bash
docker compose up -d
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
export TXS_BOOTSTRAP=localhost:9092 TXS_DSN=postgresql://postgres:txs@localhost:5432/txs
make check
make chaos
```

A change to the processor or sinks must keep `txs chaos --mode exactly-once --check` passing,
and should say in the pull request how it was killed and what reconciliation showed.
