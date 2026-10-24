# Supported modules

The `d-core-*` family. Every module carries the same skeleton (`.version`, `build.clj`,
`Makefile`, `README.md`, `GEMINI.md`, `docs/`, `:test`/`:lint`/`:build` aliases) and is
published to Clojars as `org.clojars.cinguilherme/<module>`.

Run `make release-check` from this directory for the live matrix (it reads every module's
`.version` and reports uncommitted work).

## Platform

| Module | Owns |
| :-- | :-- |
| `d-core-std` | shared protocols (cache, storage, queue, circuit breaker, health), clocks, schemas |
| `d-core-serialization` | serialization codecs |
| `d-core-metrics` | Prometheus registry, collectors, scrape server |
| `d-core-health` | health registry + `/live` `/ready` `/health` handlers |
| `d-core-circuit-breaker` | breaker state machine, shared open signal |
| `d-core-resilience` | one guard: timeout → bulkhead → breaker → retry → fallback |
| `d-core-rate-limit` | token buckets |
| `d-core-cache` | cache implementations |
| `d-core-store` | blob/object storage |
| `d-core-db` | SQL databases (Postgres, SQLite) |
| `d-core-redis` / `d-core-leader-election` | Valkey/Redis client, leases and ownership |
| `d-core-cron` | cron scheduling |
| `d-core-http` | HTTP server/client plumbing |
| `d-core-server` | service bootstrap |
| `d-core-grpc` | gRPC transport |
| `d-core-actor-system` | actors, clustering, partition ownership |
| `d-core-async-messaging` / `d-core-async-proxy` | async messaging and the proxy tier |
| `d-core-workers` | worker pools |
| `d-core-auth` / `d-core-cripto` | auth and cryptography |
| `d-core-ai` | model clients |
| `d-core-chaos` | fault controller, journal, instrumentation, `/_chaos` admin |
| `d-core-chaos-engine` | scenario runner, target discovery, invariants, reports |
| `d-core-stateflow` | state-flow steps, causal journal assertions, test tiers |
| `d-core-http`, `d-core-cache`, … | see the table above |

## Test tooling

| Module | Tier |
| :-- | :-- |
| `d-core-stateflow` | `make test-flow` (in-process), `--tier stack` (compose), `--tier chaos` |
| `d-core-chaos-engine` | scenario runs, JSON/JUnit reports, `scripts/chaos_run.py` |
