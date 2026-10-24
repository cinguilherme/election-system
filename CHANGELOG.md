# Changelog

## Unreleased

### Chaos that is trustworthy (items 1–6)

- `d-core-chaos`: closed rule schema, string→keyword coercion, `:max-total-hits` budget, TTL
  expiry, audit trail, kill switch, `:duplicate` delivery fault, sampled/`meta` journal modes,
  `?dry-run`, auth on `/_chaos`, query-string parsing, experiment/trace correlation.
- `d-core-chaos-engine`: compose service discovery with per-replica targets, self-registration,
  scenarios as EDN with baseline/invariants, Toxiproxy and Docker-socket faults, merged journal
  timeline with clock skew, JSON/JUnit reports, `chaos_run.py` CLI.
- `election-system`: chaos wired behind `CHAOS_ENABLED` (default off) on the admin port `:8082`
  (internal only), compose override, six scenarios, admin token.

### Simplifying the application (items 7–9)

- `d-core-resilience`: one guard — `fallback ← retry ← breaker ← bulkhead ← timeout` — with a
  closed policy schema, metrics and health. Drop-in for `CircuitBreakerProtocol`.
- `d-core-circuit-breaker` v2: pure state machine, rate-based windows (`:failure-rate` +
  `:min-calls`), non-mutating reads, bounded half-open admission, jitter, shared Valkey signal.
- `d-core-health` v2: cached evaluation, bounded pool under one deadline, lifecycle
  (`:starting`/`:running`/`:draining`), flap damping, `/health?verbose`, `/health/system`.
- `election-system`: API guards are policy maps; actors serve `/live` `/ready` `/health` on
  `:8081`; compose healthchecks probe `/ready` instead of a TCP connect.

### Developer superpowers (items 10–12)

- Test tiers (`^:tier :flow|:stack|:chaos`), `with-stack` compose fixture, portable journal,
  seeded chaos runs (`:seed` recorded and replayed), `make test-flow|test-stack|test-chaos`.
- State-flow ergonomics: timeline diff on a failed causal assertion, `eventually` /
  `await-status` / `retry-step` / `poll-until`, validated `world/->world`.
- Release parity: `build.clj`/`Makefile`/`.version` for health, circuit-breaker and async-proxy;
  `scripts/release.clj` (`--check`, `--bump`, `--apply`, `--tag`); `scripts/new-module.sh` that
  scaffolds a compliant module; `d-core.stateflow.e2e` moved to the `:dev` alias so it is not
  published; shared breaker/health protocols committed to `d-core-std`.
