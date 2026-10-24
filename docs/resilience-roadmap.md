# Resilience Roadmap: stateflow, chaos, health, circuit breaker

Status: accepted. Items 1 and 2 implemented; item 3 in progress.

Paths in this document are relative to the **workspace root** (the directory containing the
`d-core-*` modules and `election-system/`), not to this file.

Companion evaluation of the four new capabilities (`d-core-stateflow`, `d-core-chaos`,
`d-core-health`, `d-core-circuit-breaker`), and the ordered list of the next twelve
improvements needed to turn them into a production-grade resiliency layer for the `d-core`
ecosystem.

Goal alignment for every item:

| Tag | Goal |
| :-- | :-- |
| `CHAOS` | Chaos engineering on a running system with many containers |
| `TEST` | Testing capability (what we can *prove*, not just assert) |
| `DX` | Developer experience |
| `ERG` | `d-code` ergonomics — simpler code for the same behaviour |
| `SCALE` | Building real-world scalable applications with the complexity simplified |

---

## Decisions

Two decisions were taken before item 1 and are binding for everything below.

**D1 — Chaos is opt-in through a compose overlay.**
`docker-compose.chaos.yaml` is the only place where chaos is enabled. `docker-compose.yaml`
never enables it. The overlay follows the existing overlay pattern used by
`docker-compose.perf.yaml`, `docker-compose.observability.yaml`, `docker-compose.load.yaml`
and `docker-compose.registry.yaml`.

**D2 — Every deployable role may expose an admin port on `:8082`, internal only.**
The port is not published to the host, not routed by `election-api` (nginx), and not scraped
by Prometheus. It carries `/_chaos/*` and the role's health probes. Because `deploy.replicas`
makes a fixed host port impossible, host access goes through the control plane (item 3) or
`docker compose exec`. Requests to `/_chaos/*` require `X-Chaos-Token` when
`CHAOS_ADMIN_TOKEN` is set.

Consequence: in the base stack the surface is unchanged — no new port, no new code path, no
journal allocation.

---

## Current state (baseline for everything below)

## Baseline (state before item 1)

| Fact | Evidence |
| :-- | :-- |
| Chaos is wired **only** into the in-process test cluster | `election-system/src/election_system/system.clj:676` |
| Neither deployable role constructs a controller or journal | `start-api-node` :156, `start-actors-node` :323 |
| The actors tier has no health registry and no circuit breakers | `start-actors-node` body |
| The actors HTTP handler answers 200 to every non-`/metrics` path | `make-actors-metrics-handler`, `system.clj:246-254` |
| API readiness is derived from *breaker state*, not from the DB | `system.clj:186-197` |
| The deployable image cannot resolve the three new modules | `election-system/Dockerfile:7-36` vs `deps.edn:5-15` — **fixed in item 1** |
| `d-core-health` and `d-core-circuit-breaker` have no `build.clj`, `Makefile`, `.version` or README | module roots |
| Multi-container drivers exist but have zero callers | `d-core-stateflow/src/d_core/stateflow/e2e.clj` |
| No Toxiproxy, no chaos container, no e2e suite against a running stack | `grep -rni toxiproxy` |
| k6 thresholds are SLO-shaped but never verified under failure | `observability/k6/election_load.js:131-136` |

Module test coverage at baseline: chaos 1 test / 23 assertions, health 5 / 50,
circuit breaker 1 / 40, stateflow 1 (not runnable in the sandbox), plus 4 consumer flows
(702 LOC) in `election-system/test/election_system/stateflow/`.

---

## Phase 0 — Make it real (≈6 weeks)

### 1. Put the layer inside the containers, behind one flag  `CHAOS` `DX`

Status: **implemented**. One acceptance criterion (the p99 delta under load) is not yet
measured; everything else below was verified.

- [x] Fix `election-system/Dockerfile` to copy `d-core-chaos`, `d-core-health`,
      `d-core-circuit-breaker` (build was broken until this landed)
- [x] `CHAOS_ENABLED` (default `0`): when off, no controller, no journal, **no decorators**,
      no admin port — zero cost, zero surface
- [x] `CHAOS_JOURNAL_MODE` = `full` | `off`; `CHAOS_MAX_EVENTS`
- [x] Dedicated admin listener on `:8082` (D2), started only when chaos is enabled
- [x] Instrument both roles: SQL, actor transport, actor client, snapshot store, effect
      handler, leader election, circuit breaker
- [x] Real health registry on the actors tier: `:db`, `:broker`, `:lease-store`,
      `:owning-partitions`, `:liveness`
- [x] `docker-compose.chaos.yaml` overlay + `make chaos-up` / `chaos-down` / `chaos-status`
- [x] `X-Chaos-Token` guard on `/_chaos/*`

**As built**

| Piece | Where |
| :-- | :-- |
| Runtime: flag, journal modes, instance identity, decorator helpers | `election-system/src/election_system/resilience/chaos.clj` |
| Admin listener: `/_chaos/*`, `/live`, `/ready`, `/health`, token guard | `election-system/src/election_system/resilience/admin.clj` |
| Actors-tier probes (real DB round-trip, broker socket, lease read, partitions held) | `election-system/src/election_system/resilience/health.clj` |
| Role wiring and lifecycle (start/stop, raw vs instrumented handles) | `system.clj` `start-api-node`, `start-actors-node`, `stop-*-node!` |
| Env config | `main.clj` `:chaos` |
| Overlay and targets | `docker-compose.chaos.yaml`, `Makefile` |
| Tests | `test/election_system/unit/resilience_wiring_test.clj` (5 tests / 45 assertions) |

Verified:
- Unit suite: **36 tests / 196 assertions**, 0 failures (includes the pre-existing 31).
- Boot smoke on a real Jetty: identity 200, `/_chaos/status` 401 without token, 200 with
  token, rule injection 200, journal query 200, `/health` still 200.
- With `CHAOS_ENABLED` unset the same boot opens **no** listener (`connection-refused` on
  8082) and reports `{:enabled? false, :journal nil, :controller nil}`.
- `docker compose config`: the overlay adds exactly the five `CHAOS_*` variables plus
  `expose: 8082` to `election-actors` and `election-api-backend`; the base stack has zero
  `CHAOS_*` keys and nothing exposes 8082; the overlay errors out without `CHAOS_TOKEN`.
- Lint: 0 errors on the changed files (4 pre-existing unresolved-var warnings).

Deviations from the checklist:
- **Producer** instrumentation ships as a helper but is unused: `election-system` has no
  `Producer` component, and `async-proxy` is a separate deployable. Wiring that belongs
  with the edge faults of item 6.
- **`CHAOS_SAMPLE`** is deferred to item 4, where sampling and truncation live. Today
  `CHAOS_JOURNAL_MODE=off` is the low-overhead switch.
- **Network/pause faults** are unchanged by design; item 6 owns them.

**Done when.** `curl :8082/_chaos/status` answers on every API and actors replica with
rules and journal counts; `/_chaos/journal` returns the causal events of a vote batch; with
`CHAOS_ENABLED=0` a 5-minute load run shows zero journal growth and <2% p99 delta against
`main`. ← the load-run half is still open.

**Effort** 2–3 w (spent: ~1 d for the wiring). **Risk** medium (hot path). **Blocks** everything.

### 2. Arms-control for fault injection  `CHAOS` `SCALE`

Status: **implemented** in `d-core-chaos` and wired through the election-system admin plane.

- [x] Malli schema on rules; unknown keys rejected with the offending path
- [x] Token / mTLS on every `/_chaos` route; per-environment component allowlist
- [x] Per-rule `:expires-in-ms` with a sweeper that auto-reverts
- [x] `:max-hits` hard cap; `?dry-run=true` returns the match decisions without injecting
- [x] Global kill switch: `POST /_chaos/kill-switch` clears every rule on every node
- [x] Append-only audit buffer (who injected what, when, from where)

**As built**

| Piece | Where |
| :-- | :-- |
| Closed schema, normalization, allowlist, duration parsing | `d-core-chaos/src/d_core/chaos/rules.clj` |
| Expiry, budget, `probe!`, kill switch, audit hooks | `d-core-chaos/src/d_core/chaos/controller.clj` |
| Append-only audit log | `d-core-chaos/src/d_core/chaos/audit.clj` |
| New endpoints, `wrap-chaos-auth`, query-string parsing | `d-core-chaos/src/d_core/chaos/http.clj` |
| Env knobs + delegation to the library guard | `election-system/src/election_system/main.clj`, `resilience/admin.clj`, `resilience/chaos.clj` |
| Tests | `d-core-chaos/test/d_core/chaos/arms_control_test.clj` (20 tests) |

Verified:
- `d-core-chaos`: **21 tests / 117 assertions**, 0 failures (was 1 test / 23 assertions).
- `election-system`: 36 tests / 196 assertions still green.
- Over real Jetty (not just in-process): 401 without token; `{"probabilty": 0.01}` → 422 naming
  `["probabilty"]`; a disallowed component → 422 naming the allowlist; `?dry-run=true` → 200
  `"dry-run"` with nothing armed; a `"for":"2s"` rule gone from `GET /_chaos/rules` after 2.2 s;
  kill switch → `enabled? false`; audit trail `["dry-run" "injected" "expired" "killed"]`.
- Lint: 0 errors.

Deviations / notes:
- **mTLS** is not implemented — the guard is a shared token. mTLS belongs with the chaos-engine's
  service identity (item 3), not inside an application library.
- **"on every node"** is per-node over HTTP; fan-out to all replicas is exactly what item 3 adds.
- **The sweeper is lazy, not a thread**: expiry is evaluated on every match and every read, so a
  rule stops firing the instant it expires; `purge-expired!` (run by `GET /_chaos/status|rules`)
  removes it and records `:expired`. No background thread to leak or to wake up.
- `:max-hits` is accepted as a synonym for `:times` (per rule); the *global* cap is
  `:max-total-hits`, which disables the controller and records `:budget-exhausted`.
- A real bug the smoke test caught: Jetty hands the handler `:query-string`, not `:query-params`,
  so `?dry-run=true`, journal filters and `?limit=` were silently ignored over HTTP. Fixed by
  parsing the query string in `make-chaos-handler`, with a regression test.

**Done when.** `{"probabilty": 0.01}` is rejected (not silently treated as 1.0); a rule with
`:expires-in-ms 60000` is gone one second after expiry; unauthenticated `/_chaos` requests
get 401. — **all three verified.**

**Effort** 1–2 w (spent: ~1 d). **Risk** low. **Depends on** 1.

### 3. The `chaos-engine` container and control plane  `CHAOS` `TEST` `DX`

Status: **implemented** — new module `d-core-chaos-engine`, its own image, and a service in the
chaos overlay.

- [x] New module `d-core-chaos-engine` (`:role :chaos-engine`), Ring service, own image
- [x] Target discovery, in preference order: node self-registration → compose labels →
      Swarm `tasks.<service>`
- [x] `GET /api/targets`, `POST /api/faults`, `GET /api/faults`, `DELETE /api/faults/:id`,
      `POST /api/experiments`, `GET /api/experiments/:id`, `POST /api/experiments/:id/abort`
- [x] Selectors: `{service, replica}`, `{instance-id}`, `{component, op, target}`
- [x] Fan-out with per-node acknowledgement and partial-failure reporting
- [x] `docker-compose.chaos.yaml` gains the `chaos-engine` service

**As built**

| Piece | Where |
| :-- | :-- |
| Node client (JDK `java.net.http`, injectable for tests) | `d-core-chaos-engine/src/d_core/chaos_engine/client.clj` |
| Discovery: DNS expansion, static/named addresses, self-registration, replica numbering, selectors | `…/registry.clj` |
| Fault fan-out, revert, kill switch, experiment timelines and abort | `…/engine.clj` |
| Operator API + token guard | `…/http.clj` |
| Assembly and entry point | `…/core.clj`, `…/main.clj` |
| Image | `d-core-chaos-engine/Dockerfile` |
| Compose service (`:8088`, token-guarded, internal to the compose network) | `election-system/docker-compose.chaos.yaml` |
| Make targets | `chaos-targets`, `chaos-faults`, `chaos-experiments`, `chaos-kill` |
| Tests | `d-core-chaos-engine/test/…` (28 tests / 110 assertions) |

Verified end to end, with two **real** node admin planes (the same handler election-system
mounts on `:8082`) and the engine driving them over HTTP:
- a fault on `{service, replica 2}` armed **exactly one** node (replica 1: 0 rules, replica 2: 1);
- `GET /api/faults` listed it; `DELETE /api/faults/:id` reverted it (both nodes back to 0);
- an experiment `inject@0 → clear@120ms` reached `:completed` with log `["inject" "clear"]` and
  left 0 rules behind;
- with one node stopped, fan-out returned **207** and reported per-replica status;
- `POST /api/kill-switch` cleared the surviving node;
- `{"probabilty": 0.1}` → **422** before any node was contacted;
- `docker compose config` accepts the overlay with the new service.

Deviations / notes:
- **Discovery order changed in practice**: DNS expansion (`InetAddress/getAllByName` on the compose
  service name) is the primary mechanism, with self-registration as an opt-in that yields a stable
  `instance-id`. Docker-label discovery needs a socket proxy and is deferred to item 6 with the
  pause/kill faults.
- A separate image (rather than `ROLE=chaos-engine` inside the election-system image) keeps the
  engine's dependencies and blast radius separate from the application. Same deployment story.
- `replica` is numbered by `(host, port)`; it is stable only while the address set is. Use
  `instance-id` when a selector must survive a container restart.
- Engine state (armed faults, experiments) is in-process. A restart forgets it; nodes keep what
  was armed until it expires — documented in the module README.

**Done when.** `POST /api/faults` targeting `election-actors` replica 2 fires on exactly that
container; `DELETE /api/faults/:id` reverts within 1 s; `GET /api/faults` lists what is armed
everywhere. — **all three verified against real admin planes.**

**Effort** 3–4 w (spent: ~1 d). **Risk** medium. **Depends on** 1, 2.

```bash
curl -sX POST localhost:8088/api/faults -H "X-Chaos-Token: $CHAOS_TOKEN" \
  -H 'content-type: application/json' -d '{
  "selector": {"service": "election-actors", "replica": 2},
  "rule":     {"component": "lease", "op": "renew!",
               "for": "30s",
               "fault": {"type": "return", "return": {"status": "lost"}}},
  "note":     "lease-renewal blackhole"
}'
```

---

## Phase 1 — Credible experiments (≈8 weeks)

### 4. Flight-recorder journal and cross-node correlation  `CHAOS` `SCALE`

Status: **implemented** in `d-core-chaos` (recorder), `d-core-chaos-engine` (merged timeline)
and the election-system API (request binding).

- [x] Zero-cost path when disarmed (`enabled?` check before any allocation)
- [x] `meta` mode: no payloads, only component/op/target/status/duration
- [x] Sampling (`CHAOS_SAMPLE`), payload truncation with `:omitted-bytes`
- [x] `X-Experiment-Id` propagated through the actor envelope `:context` and OTEL spans;
      every journal event carries `:experiment-id`, `:instance-id`, `:trace-id`
- [x] `GET /api/experiments/:id/journal` merges all replicas by
      `(timestamp, instance-id, seq)` with clock-skew detection

**As built**

| Piece | Where |
| :-- | :-- |
| `execute-instrumented!` is now a macro: disarmed expands to a bare `(thunk)`, so the `call-ctx` form is never evaluated | `d-core-chaos/src/d_core/chaos/controller.clj` |
| Modes `:full`/`:meta`, `:sample`, `:max-payload-chars`, O(1) ring, counters | `d-core-chaos/src/d_core/chaos/journal.clj` |
| `*experiment-id*`; envelope context injection on send, binding on delivery | `d-core-chaos/src/d_core/chaos/instrument.clj` |
| `:now-ms` + journal mode/counters on `/_chaos/status`; `experiment-id` journal filter | `d-core-chaos/src/d_core/chaos/http.clj` |
| Timeline merge, skew reporting, unreachable nodes | `d-core-chaos-engine/src/d_core/chaos_engine/journal.clj` |
| `GET /api/experiments/:id/journal` fan-out | `d-core-chaos-engine/src/d_core/chaos_engine/engine.clj`, `…/http.clj` |
| API binds `X-Experiment-Id`; `CHAOS_SAMPLE` / `CHAOS_MAX_PAYLOAD_CHARS`; OTEL trace id | `election-system/src/election_system/api/routes.clj`, `resilience/chaos.clj`, `main.clj` |
| Tests | `d-core-chaos/test/…/flight_recorder_test.clj` (12), engine `journal_test.clj` (5) + 2 |

Verified:
- **Disarmed is free**: a `call-ctx` expression that throws is never evaluated when the
  controller is disabled (`(execute-instrumented! disabled j (boom) (fn [] 42))` → `42`, journal
  empty, counters `{:recorded 0 :sampled-out 0 :truncated 0}`).
- `:meta` drops `:args`/`:result` but keeps `:component :op :target :status :row-count
  :duration-ms`; `:sample 0.0` records nothing and counts 20 `:sampled-out`; a 200-char bound
  turns a payload into `{:omitted-bytes n :preview "…"}`; a 3-slot ring keeps seq `[3 4 5]`.
- Live two-node smoke: events recorded out of order on two real admin planes merged to
  `[200 300]` with `:instances` spans, `:sequence-violations 0`, per-node
  `:clock-skew-ms {1ms, 3ms}`, and the `?experiment-id=` filter excluded the unrelated event.
- Suites: `d-core-chaos` 35 / 151, `d-core-chaos-engine` 35 / 132, `election-system` 39 / 207.

Deviations / notes:
- The experiment id is **not** yet written as an OTEL span attribute; journal events carry the
  span's trace id (`:trace-id`) but span tagging lands with the reports/annotations of item 5,
  where a span is the thing being annotated.
- Clock skew is measured at discovery (`node now-ms` minus engine now) and therefore includes
  half the round-trip; it is reported, never applied. `:skewed` names nodes past 250 ms.
- `:replica` numbering is by `(host, port)`; `:instance-id` remains the selector that survives a
  restart.
- Remaining acceptance gap: the 1,000 events/s *load* measurement (same gap as item 1's p99) —
  run under `make chaos-load` once item 5 exists.

**Done when.** 1,000 events/s with chaos disarmed allocates nothing for the journal; one
experiment yields a single merged timeline across 3 actor + 3 API replicas with zero
interleaving violations. — disarmed-cost and merge verified; the 6-replica load run is open.

**Effort** 2–3 w (spent: ~1 d). **Risk** medium.

### 5. Experiments, steady state, SLOs and reports  `TEST` `SCALE`

- [ ] `defexperiment` with `:steady-state`, `:faults` timeline, `:abort`, `:invariants`
- [ ] Invariants: no duplicate ballots, `tally == Σ ballots`, epoch monotonic, zero unowned
      partitions
- [ ] Run under the existing k6 profile; sample Prometheus during the run
- [ ] JSON + JUnit report; Grafana annotation per experiment
- [ ] `make chaos-load SCENARIO=…` exits non-zero on any violation

**Done when.** Six scenarios (actor kill, proxy pause, DB ack-loss, valkey partition, 500 ms
latency, clock skew) run unattended, produce reports, and fail the build on invariant breach.

**Effort** 3–4 w. **Risk** medium. **Depends on** 3, 4.

### 6. Network and broker faults  `CHAOS`

- [ ] Toxiproxy in `docker-compose.chaos.yaml`, one proxy per edge (api→proxy, actors→proxy,
      api→postgres, actors→postgres, →valkey)
- [ ] Wire the existing `toxiproxy-*` stateflow steps (today they have zero callers)
- [ ] Broker modes protocol decorators cannot express: duplicate delivery, reorder, delayed
      redelivery, nack storms
- [ ] `chaos-exec` sidecar (docker-socket-proxy, read-only, allowlisted) for pause/kill/restart

**Done when.** A `latency` toxic on `actors→postgres` shows up in the SQL latency panel; a
duplicate-delivery injection proves the actor dedupe path.

**Effort** 2–3 w. **Risk** low-medium.

---

## Phase 2 — Simplify building real applications (≈10 weeks)

### 7. `d-core-resilience` — one declarative facade  `ERG` `SCALE`

- [ ] One policy engine: `timeout → bulkhead → circuit-breaker → retry → fallback`, documented
      order, one config shape
- [ ] Reuse it from `d-core-http`, `d-core-db`, producers and actor transports
- [ ] Delete the inline breaker/rate-limit pipeline in `d-core-http`; retire the ad-hoc
      rate limiter in `d-core-rate-limit`
- [ ] Reference service: `system.edn` → running service with health, breakers, metrics, chaos

```clojure
(resilience/guard :election-db
  {:timeout-ms 5000
   :bulkhead {:max-concurrent 16}
   :breaker  {:failure-rate 0.5 :window 30 :min-calls 10 :open-for "10s"}
   :retry    {:max 2 :backoff :exponential :jitter 0.2
              :on #{:timeout :io} :idempotent-only true}
   :fallback (fn [_ex] {:status :degraded})})
```

**Done when.** election-system's wiring is a policy map; one policy type covers
HTTP/DB/actor/lease; a scaffolded service has `/health` in under a minute.

**Effort** 4–6 w. **Risk** high — refactor of live call paths; keep the protocols.

### 8. Circuit breaker v2  `SCALE`

- [ ] Rolling-window error-rate and percentile triggers (not just consecutive counts)
- [ ] Bounded CAS retries; correct half-open concurrency accounting
- [ ] `:open-for` with jitter; explicit retry interaction (never retry while open; fallback is
      not a success)
- [ ] Optional Valkey-backed breaker so 3 API replicas trip together
- [ ] Breaker state feeds dependency health; no mutation on `state` reads

**Done when.** A DB outage trips all replicas within one window; half-open admits exactly
`half-open-max-calls`; property tests cover the transition table.

**Effort** 2 w. **Risk** medium.

### 9. Health v2  `SCALE`

- [ ] TTL-cached evaluation (default 1 s) with a bounded executor, not `future` per check
- [ ] Real probes: `SELECT 1`, Valkey `PING`, proxy health, "partitions owned > 0"
- [ ] Lifecycle: `:starting` (fail ready, pass live), `:draining`
- [ ] Flap damping (`:down` only after *N* consecutive failures)
- [ ] `/health?verbose`, per-node `/health/system`, compose healthcheck uses `/ready`

**Done when.** Killing postgres flips `/ready` in ≤2 s and back in ≤5 s; a 3 s slow check
cannot make probes take 3 s; zero readiness flips during a 5-minute steady-state run.

**Effort** 2 w. **Risk** low.

---

## Phase 3 — Developer superpower (≈5 weeks)

### 10. Test harness, tiers and determinism  `TEST` `DX`

- [ ] `with-stack` fixture: compose `up -d --wait`, replica base-URL discovery, `World` map,
      unconditional teardown
- [ ] One flow runs against both the in-process cluster and the compose stack
- [ ] `defflow` metadata `^{:tier :flow|:stack|:chaos}` and `make test-flow|test-stack|test-chaos`
- [ ] Seeded RNG (`:seed`) forwarded with every experiment — probabilistic faults replay exactly
- [ ] Flake budget + quarantine for stack tests

**Done when.** The same `deftest` passes in-process and against `docker compose up`; a failing
chaos run prints its seed and replays byte-identically.

**Effort** 2–3 w. **Risk** medium. **Depends on** 3.

### 11. Stateflow v2 ergonomics  `ERG` `DX`

- [ ] Explicit `World` record/schema with `world/->world`; `:cluster` spec-checked
- [ ] Timeline diff on journal assertion failure (expected vs actual plus ±3 surrounding
      events and timing)
- [ ] `defflow` tags; `eventually` / `await-status` / `retry-step` helpers
- [ ] clj-kondo hooks for `defflow` and `with-fault`; `flow/print-journal` for the REPL

**Done when.** A failed causal assertion explains what diverged and when in ≤5 lines;
onboarding a new flow takes one doc page.

**Effort** 2 w. **Risk** low.

### 12. Release and convention parity  `DX` `ERG`

- [ ] `build.clj` + `Makefile` + `.version` + README + `GEMINI.md` + `docs/` for `-health`
      and `-circuit-breaker`
- [ ] Publish all four modules; move `d-core.stateflow.e2e` out of the published lib
- [ ] Add the four modules to `docs/supported.md` and the changelog
- [ ] `module-template` so the next capability is born compliant

**Done when.** `clojure -T:build jar` succeeds for all 28 modules; every module has the same
file skeleton.

**Effort** 1–2 w. **Risk** low.

---

## Sequencing

| Phase | Items | Theme | Exit criterion |
| :-- | :-- | :-- | :-- |
| 0 (wks 0–6) | 1, 2, 3 | Make it real, make it safe | `/_chaos/status` on every replica; a fault injected from `chaos-engine` reverts itself |
| 1 (wks 6–14) | 4, 5, 6 | Credible experiments | 6 scenarios × 14 nights green, each with a report and an annotation |
| 2 (wks 14–24) | 7, 8, 9 | Simplify app building | new service bootstrap < 1 min; election-system wiring < 40 lines; shared breaker state |
| 3 (wks 24–29) | 10, 11, 12 | Developer superpower | one command per tier; published artifacts; failures explain causality |

## Success metrics

- **MTTR-to-reproduction** of a field incident ≤ 15 min (today: "write a new cluster by hand").
- **Invariant violations found per quarter** — this number should go *up*.
- **Unowned-partition seconds** during actor-kill experiments: 0 after the grace window.
- **Duplicate ballots** under ack-loss: 0, asserted nightly.
- `tally == Σ ballots` after every experiment.
- **Overhead**: p99 delta with chaos wired but disarmed < 2%; flat journal memory over 30 min.
- **Onboarding**: first chaos scenario written in < 30 min from the docs alone.

## Non-goals

- Rebuilding Chaos Mesh. Own the *semantic* faults only this codebase can express
  (post-commit ack-loss, lease-fencing violation, duplicate delivery, coalescing-tick
  misfire); delegate network/lifecycle faults to Toxiproxy and a thin socket proxy.
- Enabling chaos in production. Staging, compose, and a shadow environment with a hard
  allowlist.
- A UI before item 5 ships reports.
- More instrumentation before item 4 removes the always-on journal cost.
