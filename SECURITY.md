# Security policy

## Reporting a vulnerability

Please do not open a public issue for security problems. Use GitHub's
[private vulnerability reporting](https://github.com/darshansatpathy/realtime-transaction-streaming/security/advisories/new)
for this repository. You should get an acknowledgement within three working days.

## Operating it safely

- The compose file runs Kafka without TLS or authentication and PostgreSQL with a fixed
  password; both are for local development only.
- In production, give the processor ACLs for its own topics and transactional id only, and the
  sink a database role limited to its two tables.
- Event payloads are parsed as JSON into fixed fields; malformed events are rejected, not
  executed or stored raw.
