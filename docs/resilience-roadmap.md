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

Status: **implemented** as scenario runs: baseline → fault timeline → invariants → report, with
a CLI that exits non-zero and JUnit output for CI.

- [x] `defexperiment` with `:steady-state`, `:faults` timeline, `:abort`, `:invariants`
- [x] Invariants: no duplicate ballots, `tally == Σ ballots`, epoch monotonic, zero unowned partitions
- [x] Run under the existing k6 profile; sample Prometheus during the run
- [x] JSON + JUnit report; Grafana annotation per experiment
- [x] `make chaos-load SCENARIO=…` exits non-zero on any violation

**As built**

| Piece | Where |
| :-- | :-- |
| Scenario EDN schema, validation, directory loading | `d-core-chaos-engine/src/d_core/chaos_engine/scenario.clj` |
| Declarative probes (`:http`, `:prometheus`, `:journal`, `:faults`, `:targets`) and assertion ops (`:sum-eq`, ranges, presence) | `…/checks.clj` |
| Run lifecycle: baseline, timeline, guaranteed cleanup, settle, invariants, abort | `…/runner.clj` |
| JSON + JUnit renderers and file output | `…/report.clj` |
| Scenario/run endpoints | `…/http.clj` |
| Grafana annotation (best effort, tagged `run:<id>`) | `…/runner.clj` |
| Four scenarios | `election-system/scenarios/*.edn` |
| CLI that polls, prints the failing check and exits non-zero | `election-system/scripts/chaos_run.py` |
| `make chaos-scenarios / chaos-run / chaos-load / chaos-runs` | `election-system/Makefile` |
| k6 sends `X-Experiment-Id` (`EXP_ID`) so traffic and faults share one timeline | `election-system/observability/k6/election_load.js` |
| Tests | `checks_test` (6), `runner_test` (6), plus engine suite |

Verified:
- **End to end**: real engine + a scenario on disk + the CLI. The run reported
  `status=PASSED` with `baseline ok`, `inject ok`, `clear ok`, `fault-fired ok`,
  `audit-after ok`, `no-faults-left ok`; the JSON and JUnit files were written and the CLI exited
  **0**. The fault fired for real: the app's instrumented `select` was called while the rule was
  armed, and the `:journal` invariant saw the chaos event.
- **A fault that never fires fails the run** (unit test): the `:journal` invariant reports
  `:chaos-events 0` and the run is `:failed` — a green suite that proved nothing is not green.
- **An unsteady baseline fails before injecting** (unit test): zero rules were armed.
- **Abort** (unit test): the run finishes `:aborted`, the armed rule is deleted, and the engine has
  no faults left.
- The four shipped scenarios all load and validate (`scenarios: (db-ack-loss db-outage
  lease-blackhole transport-drop)`, `errors: []`).
- Suites: `d-core-chaos-engine` 49 / 190.

Deviations / notes:
- **`defexperiment` became EDN files** (`scenarios/*.edn`) instead of a Clojure macro: versioned
  like code, mounted into the container, and editable without a rebuild — the macro shape only
  helped in-repo tests.
- **Four of the six scenarios ship now.** Actor kill / proxy pause / 500 ms latency / clock skew
  need the infrastructure faults of item 6 (docker-socket `pause`/`kill`, Toxiproxy/netem). The
  runner, invariants and reports they will use are already here.
- **Invariant coverage**: `tally == Σ ballots` is `:sum-eq` on the audit payload; "no duplicate
  ballots" is enforced by the `UNIQUE (election_id, precinct_id, voter_id)` constraint and observed
  through `:consistent?` plus the ballot-count reconciliation; "zero unowned partitions" is a
  Prometheus query. **Epoch monotonicity is not asserted** — no endpoint exposes the ownership
  epoch, so that needs an audit field (open).
- **Sampling is per checkpoint** (baseline and invariants), not a continuous scrape during the
  run. Continuous sampling belongs with the SLO/burn-rate work that follows from k6 thresholds.
- Grafana annotation is best-effort and silent when `GRAFANA_URL` is unset.

**Done when.** Six scenarios run unattended, produce reports, and fail the build on invariant
breach. — the runner, reports and exit codes are done for four scenarios; the remaining two are
blocked on item 6.

**Effort** 3–4 w (spent: ~1 d). **Risk** medium. **Depends on** 3, 4.

### 6. Network and broker faults  `CHAOS`

Status: **implemented** — Toxiproxy per dependency edge, container pause/kill through a
socket proxy, and duplicate delivery as a rule type. Two of the six scenarios now need real
infrastructure faults; both are written.

- [x] Toxiproxy in `docker-compose.chaos.yaml`, one proxy per edge (api→proxy, actors→proxy,
      api→postgres, actors→postgres, →valkey)
- [x] Wire the existing `toxiproxy-*` stateflow steps (today they have zero callers)
- [x] Broker modes protocol decorators cannot express: duplicate delivery, reorder, delayed
      redelivery, nack storms
- [x] `chaos-exec` sidecar (docker-socket-proxy, read-only, allowlisted) for pause/kill/restart

**As built**

| Piece | Where |
| :-- | :-- |
| Toxiproxy client: proxies, toxics, cut/heal, reset, tracking | `d-core-chaos-engine/src/d_core/chaos_engine/network.clj` |
| Container client: resolve by compose label + replica, pause/unpause/kill/restart, `unpause-all!` | `…/docker.clj` |
| Endpoints `/api/network/*`, `/api/containers/*` | `…/http.clj` |
| Scenario steps `:toxic :cut :heal :pause :unpause :kill-container :restart-container`; `:clear` now resets rules **and** network **and** containers; a failed step fails the run | `…/runner.clj`, `…/scenario.clj` |
| Engine kill switch also removes toxics and unpauses containers | `…/engine.clj` |
| Toxiproxy service + per-edge config | `election-system/docker-compose.chaos.yaml`, `election-system/toxiproxy/toxiproxy.json` |
| `docker-socket-proxy` (no socket in the engine) | `election-system/docker-compose.chaos.yaml` |
| Tiers routed through the proxies by overlay env | same file |
| `:duplicate` fault type (duplicate publish / duplicate write) | `d-core-chaos/src/d_core/chaos/{rules,controller}.clj` |
| Scenarios | `scenarios/actor-kill.edn`, `scenarios/postgres-latency.edn` |

Verified:
- All **six** scenarios load and validate (`errors: []`).
- Unit-tested against fake services: toxic injection posts the toxiproxy shape and is tracked;
  `reset-all!` deletes tracked toxics and re-opens cut proxies; a toxic aimed at one proxy does
  not touch another; container resolution by compose label + replica is 1-based and
  deterministic; `unpause-all!` only touches paused containers; cleanup removes the toxic,
  unpauses and restarts what the run killed; a `:toxic` step with no Toxiproxy configured
  fails the run.
- `duplicate` fault: the delegate is invoked twice, the journal records `:duplicated` with both
  results, and the rule window is consumed once.
- `docker compose config` accepts the overlay and shows the rewired JDBC/valkey/proxy endpoints.

Deviations / notes:
- **The stack was not run here** (no docker daemon in this environment), so the two
  done-when observations — *latency visible in the SQL panel* and *duplicate delivery absorbed
  by a live actor* — are asserted only to the client/journal level. They need one run of
  `make chaos-load SCENARIO=postgres-latency` on the lab.
- **`chaos-exec` became `docker-socket-proxy` + an engine client**, not a separate service: one
  fewer moving part, and the engine is already the only control plane.
- **The stateflow `toxiproxy-*` helpers are still not wired**: the engine owns network faults for
  multi-container runs now, and the helpers remain for stateflow-driven tests against a live
  stack. If they are not used by item 10's harness, delete them.
- **Reorder and "nack storms" are expressible but not first-class**: `:delay` on the delivery
  boundary covers delayed redelivery, `:throw` on `:actor-delivery` makes the consumer retry
  (nack), and `:duplicate` covers duplication. True reordering needs a buffering decorator —
  open.
- Pausing a container does not survive a `docker compose` recreate; the overlay's toxiproxy and
  socket proxy are the pieces that must be present before a run starts.

**Done when.** A `latency` toxic on `actors→postgres` shows up in the SQL latency panel; a
duplicate-delivery injection proves the actor dedupe path. — implemented and unit-verified; the
two live observations are pending a run on the lab stack.

**Effort** 2–3 w (spent: ~1 d). **Risk** low-medium.

## Phase 2 — Simplify building real applications (≈10 weeks)

### 7. `d-core-resilience` — one declarative facade  `ERG` `SCALE`

Status: **module implemented and adopted on the API path**. The two hand-built breakers in
`start-api-node` are now policy maps; the rest of the wiring (write path, HTTP client, producers)
is not migrated yet.

- [x] One policy engine: `timeout → bulkhead → circuit-breaker → retry → fallback`, documented
      order, one config shape
- [ ] Reuse it from `d-core-http`, `d-core-db`, producers and actor transports
- [ ] Delete the inline breaker/rate-limit pipeline in `d-core-http`; retire the ad-hoc
      rate limiter in `d-core-rate-limit`
- [ ] Reference service: `system.edn` → running service with health, breakers, metrics, chaos

**As built**

| Piece | Where |
| :-- | :-- |
| Closed policy schema, normalization, `:open-for` parsing, `:fallback-sym` | `d-core-resilience/src/d_core/resilience/policy.clj` |
| The chain, metrics, health, `CircuitBreakerProtocol`/`HealthCheckProtocol` drop-in, Integrant key | `…/core.clj` |
| API path: db and transport breakers are guards built from a policy map | `election-system/src/election_system/system.clj` (`start-api-node`, test cluster) |
| Tests | `d-core-resilience/test/…/core_test.clj` (13 tests), election wiring test |

Verified:
- Policy: unknown key rejected with its path; `:failure-rate` refused with an explicit
  "arrives with item 8" message; a retry with no `:on`/`:idempotent-only` refused; `"10s"` →
  `10000`.
- Chain: a timeout bounds one attempt; retry stops on success and counts attempts; a
  non-matching error is not retried; `:idempotent-only` needs the caller's promise; the breaker
  opens, rejects **without invoking the operation**, and half-opens again after its cooldown; a
  rejected call is never retried; the bulkhead caps concurrency and returns its permit; an open
  breaker does not consume a bulkhead permit; the fallback covers both failures and rejections;
  health follows the breaker; an invalid policy throws at construction with the errors.
- Drop-in: `protect!`, `state`, `trip!`, `reset!`, `check-health` all work through the protocol
  the app already used (asserted from the election-system test suite).
- Suites: `d-core-resilience` 13 / 45, `election-system` 40 / 213.

Deviations / notes:
- **`d-core-http` was not refactored.** Its inline `rate-limit/bulkhead/breaker/retry` pipeline is
  on the async-proxy's hot path; replacing it is a separate, riskier change than adding the
  facade, and doing both at once would leave no stable baseline. It is now item 7b.
- **The write path still uses the chaos decorators directly.** The guards were adopted where a
  breaker already existed (API db + transport). Migrating `d-core-db`, producers and actor
  transports means giving those modules a guard dependency and deleting their ad-hoc retry
  config; that is the 4–6 week half of this item.
- **No reference-service scaffold yet** (`system.edn` → running service). The module's README
  documents the shape; the generator belongs with item 12's module template.
- `:failure-rate` is explicitly refused rather than approximated — see item 8.

**Done when.** election-system's wiring is a policy map; one policy type covers
HTTP/DB/actor/lease; a scaffolded service has `/health` in under a minute. — the API's breakers
are policy maps and one type covers breaker+timeout+bulkhead+retry+fallback; the other two
conditions are open.

**Effort** 4–6 w (spent: ~1 d for the module + API adoption). **Risk** high. **Depends on** 8
for rate-based tripping.

### 8. Circuit breaker v2  `SCALE`

Status: **implemented**. Rolling-window rate tripping, a pure state machine, bounded
half-open admission, jitter, and a shared open signal verified against a real Valkey.

- [x] Rolling-window error-rate and percentile triggers (not just consecutive counts)
- [x] Bounded CAS retries; correct half-open concurrency accounting
- [x] `:open-for` with jitter; explicit retry interaction (never retry while open; fallback is not
      a success)
- [x] Optional Valkey-backed breaker so 3 API replicas trip together
- [x] Breaker state feeds dependency health; no mutation on `state` reads

**As built**

| Piece | Changes |
| :-- | :-- |
| `d-core-circuit-breaker/machine.clj` | pure transitions: rolling window, rate vs consecutive tripping, half-open admission, jittered cooldown, `release-probe` |
| `…/in_memory.clj` | rebuilt on the machine: bounded CAS (`16` attempts), effective-state reads, signal publishing, ignored exceptions release their permit |
| `…/signal.clj` | `SharedSignal` protocol; in-memory and Valkey implementations (`cb:<name>:open`, TTL) |
| `d-core-resilience` | policies accept `:failure-rate`/`:min-calls`/jitter/`:signal` and pass them through |
| `election-system` | API guards use jittered cooldowns; `VALKEY_URI` opts into the shared signal |

Verified (14 tests / 83 assertions in the breaker module, 14 / 49 in resilience):
- **Rate tripping where consecutive would not**: 4 failures in 10 calls stays closed at a 50%
  threshold; 5 in 10 trips. A rate breaker without `:min-calls` is refused at construction.
- **Reads never mutate**: 20 `state` calls leave the snapshot byte-identical; after the cooldown
  `state` reports `:half-open` while the stored state is still `:open`, and the transition happens
  on the next admission.
- **Exactly `half-open-max-calls` probes**: with 2 configured, two blocked probes are admitted and
  a third call is rejected with `:reason :half-open-saturated`; two successes close the breaker.
  An `:ignore-ex` failure gives its permit back instead of leaking it.
- **Jitter** stays inside ±20% over 200 samples and actually varies.
- **A fallback is not a success**: two fallback-returning failures trip the breaker.
- **Shared signal over a real Valkey** (started for the test): replica A trips → the key appears →
  replica B rejects with `:reason :shared-open` while its own state stays `:closed` → `reset!`
  clears it and B serves again; a 1 s TTL expires by itself.
- **Property test**: 100 seeded sequences × 120 steps through the transition table, asserting the
  invariants (valid state, open always has a deadline, a close has an empty window, in-flight in
  `[0, max]`).
- The previous 40 assertions (consecutive tripping, half-open recovery, `trip!`/`reset!`,
  `trip-on`/`ignore-ex`, Integrant) pass unchanged — the rewrite is behaviour-compatible.

Deviations / notes:
- **Percentile triggers**: the window carries outcomes, so a percentile over *latency* is not
  possible without also recording durations per call. Ratios are implemented; latency percentiles
  are not (the guard records them in metrics instead).
- **The shared signal shares the decision, not the state machine.** Each replica keeps its own
  window; sharing windows would make replicas agree on a number instead of observing the
  dependency independently, and a distributed CAS on every call is a latency tax on the failed
  path.
- The Valkey signal is verified against a locally started Valkey in the test run; the election
  stack still needs `VALKEY_URI` set on the API to enable it in a deployment.

**Done when.** A DB outage trips all replicas within one window; half-open admits exactly
`half-open-max-calls`; property tests cover the transition table. — half-open and the property
test are verified; "all replicas within one window" is implemented via the shared signal and
verified with two breaker instances over Valkey, not yet with three live API containers.

**Effort** 2 w (spent: ~1 d). **Risk** medium.

### 9. Health v2  `SCALE`

Status: **implemented**. Cached, bounded, lifecycle-aware evaluation with damping; real probes on
both tiers; compose probes `/ready` instead of a TCP connect.

- [x] TTL-cached evaluation (default 1 s) with a bounded executor, not `future` per check
- [x] Real probes: `SELECT 1`, Valkey `PING`, proxy health, "partitions owned > 0"
- [x] Lifecycle: `:starting` (fail ready, pass live), `:draining`
- [x] Flap damping (`:down` only after *N* consecutive failures)
- [x] `/health?verbose`, per-node `/health/system`, compose healthcheck uses `/ready`

**As built**

| Piece | Where |
| :-- | :-- |
| Cache per tag-selection, coalescing, bounded pool + overall deadline, damping, lifecycle | `d-core-health/src/d_core/health/registry.clj` |
| `/ready` gated by lifecycle; `/health` cache age + streaks; `/health/system` over peers | `…/http.clj` |
| Actors tier serves `/live`, `/ready`, `/health` on `:8081` | `election-system/src/election_system/system.clj` |
| Lifecycle `:starting → :running` at boot, `:draining` on shutdown (both tiers) | `…/system.clj` |
| Actors probes: DB round-trip, broker socket, lease read, partitions held; 1 s cache, 2-failure damping | `election-system/src/election_system/resilience/health.clj` |
| Compose healthchecks: `curl /ready` on `:8081` and `:8080` | `election-system/docker-compose.yaml` |

Verified (`d-core-health` 13 tests / 100 assertions; election-system 42 / 228):
- **Cache**: a check runs once across evaluations inside the TTL and reports `:cached?` +
  `:cache-age-ms`; different tag selections cache separately; `{:cache? false}` bypasses.
- **Bounded**: a 2 s hanging check under a 200 ms deadline makes the evaluation return in < 800 ms
  with the component reported `:unknown` (not `:down` — we do not know that it failed).
- **Parallel**: three 200 ms checks finish in < 550 ms, not 600+ — the earlier implementation
  submitted and awaited each check in turn, which silently serialized every evaluation.
- **Damping**: with `:failures-before-down 3`, the first two failures report `:degraded`
  (`:raw-status :down`), the third is `:down`, a success resets the streak.
- **Lifecycle**: `:starting` → live 200 / ready 503; `:running` → both 200; `:draining` → live 200 /
  ready 503 even though every check is up.
- **Real probes against a real database**: the actors registry built over a freshly migrated
  SQLite plus a listening broker socket answers `/ready` 200; a closed broker port flips it to 503
  after the damping window. This is the check that proves the compose `/ready` healthcheck will
  not leave the stack unable to start.
- **System view**: an unreachable peer is reported `:down` with its error and makes the aggregate
  503; reachable peers are listed with their own status.

Deviations / notes:
- **`/health/system` is implemented and tested but not mounted in election-system**: there is no
  coordinator service to host it. The module exposes `system-routes`; the actors/API nodes already
  answer per-node `/health`.
- **Percentile/`SELECT 1`**: the DB probe is still a `SELECT ballot_id FROM ballots LIMIT 1` rather
  than `SELECT 1` — it verifies the schema the node needs, at the cost of a real query (cached for
  1 s). `SELECT 1` would pass on a database where the tables are missing.
- **The two acceptance numbers are structural, not measured on a live stack**: readiness flips
  within one cache TTL plus one probe interval (≈1 s + 2 s), and a 3 s check cannot hold a probe
  past the 2 s deadline. Both are asserted in unit tests with real clocks; the compose
  `interval: 2s` makes the flip observable in ≤ ~4 s.

**Done when.** Killing postgres flips `/ready` in ≤ 2 s and back in ≤ 5 s; a 3 s slow check cannot
make probes take 3 s; zero readiness flips during a 5-minute steady-state run. — the bounded and
flap-free properties are verified; the postgres-kill timing needs the lab stack.

**Effort** 2 w (spent: ~1 d). **Risk** low.

## Phase 3 — Developer superpower (≈5 weeks)

### 10. Test harness, tiers and determinism  `TEST` `DX`

Status: **implemented except the flake budget**. The stack fixture, the tiered runner, the
portable journal and seeded replay are in; quarantine/flake accounting is not.

- [x] `with-stack` fixture: compose `up -d --wait`, replica base-URL discovery, `World` map,
      unconditional teardown
- [x] One flow runs against both the in-process cluster and the compose stack
- [x] `defflow` metadata `^{:tier :flow|:stack|:chaos}` and `make test-flow|test-stack|test-chaos`
- [x] Seeded RNG (`:seed`) forwarded with every experiment — probabilistic faults replay exactly
- [ ] Flake budget + quarantine for stack tests

**As built**

| Piece | Where |
| :-- | :-- |
| `with-stack` / `start!` / `stop!` / `world`, injectable `:run-fn`, `:dry-run?`, `enabled?` | `d-core-stateflow/src/d_core/stateflow/stack.clj` |
| Tier runner (`--tier`, `--dir`, `--ns`), unlabelled vars count as `:flow` | `…/runner.clj` |
| Portable journal: node journal in-process, engine timeline over HTTP on a stack | `…/journal.clj` |
| Seeded `:probability`: one draw per attempt from (seed, rule-id, attempt) | `d-core-chaos/src/d_core/chaos/controller.clj` |
| Run seed recorded, forwarded into every injected rule, kept in the report | `d-core-chaos-engine/src/d_core/chaos_engine/runner.clj` |
| One flow body, two tiers (`:flow` in-process, `:stack` compose) | `election-system/test/…/portable_flow_test.clj` |
| `:tier` alias + `make test-flow / test-stack / test-chaos` | `election-system/deps.edn`, `Makefile` |

Verified:
- **Fixture**: `up -d --wait` is called with the overlay files and project name, `down -v` on
  teardown, a failed `up` throws with the compose output (a half-up stack must not produce flow
  failures that look like product bugs), teardown never throws, `:dry-run?` executes nothing and
  still builds the world.
- **Tiers**: a `:flow`-labelled var, an unlabelled var (counted as `:flow`) and a `:stack` var are
  filtered correctly; `--tier stack` prints `[skip]` and exits 0 without `ELECTION_STACK=1`.
- **Portable flow**: the same body passes at `:flow` (in-process cluster, 2 assertions) and is
  skipped at `:stack` until a stack is up.
- **Determinism**: two controllers seeded 42 produce identical fire/no-fire sequences; seed 7
  differs; the draw advances per *attempt*, so a non-firing draw does not repeat forever (the
  first version keyed on the hit count and did exactly that).
- **Seed propagation**: a run's seed is recorded on the run, injected into the rule POSTed to the
  node, and kept in the JSON report.
- Suites: `d-core-stateflow` 9 / 23, `d-core-chaos` 36 / 155, `d-core-chaos-engine` 60 / 236.

Deviations / notes:
- **No flake budget or quarantine.** A stack test that fails is a failure; there is no
  retry-or-quarantine ledger. Quarantine without a flake *rate* history is just hiding failures,
  and there is no CI history yet to compute one. Deliberately left out.
- **Replica discovery is configured, not probed.** `:urls` names the published endpoints; the
  compose project publishes fixed ports. Discovering replicas from `docker compose ps` matters
  when ports are ephemeral, which is not this stack's setup.
- **The stack tier needs built images.** `with-stack` runs `up` (it does not build), which is the
  expensive part and belongs to the operator/CI, not to every test run.
- The `:chaos` tier runner exists but no `:chaos`-labelled tests were added in this item; the
  existing chaos flows remain `:flow`-tier (in-process chaos), which is where they are fast.

**Done when.** The same `deftest` passes in-process and against `docker compose up`; a failing
chaos run prints its seed and replays byte-identically. — the same `deftest` body does both (stack
side verified to skip, not to pass, because the stack has not been built here); seed replay is
verified at the controller and run level.

**Effort** 2–3 w (spent: ~1 d). **Risk** medium.

### 11. Stateflow v2 ergonomics  `ERG` `DX`

Status: **implemented**. The failure output and the waiting primitives were the point; both are
in, with tests.

- [x] Explicit `World` record/schema with `world/->world`; `:cluster` spec-checked
- [x] Timeline diff on journal assertion failure (expected vs actual plus ±3 surrounding
      events and timing)
- [x] `defflow` tags; `eventually` / `await-status` / `retry-step` helpers
- [x] clj-kondo hooks for `defflow` and `with-fault`; `flow/print-journal` for the REPL

**As built**

| Piece | Where |
| :-- | :-- |
| `world/World` malli schema (open), `->world` (validate → normalize), `:cluster` reachability check | `d-core-stateflow/src/d_core/stateflow/world.clj` |
| `align`, `format-diff`, `assert-events!`, `timeline`, `print-journal` | `…/journal.clj` |
| `poll-until`, `eventually`, `await-status`, `retry-step`, `print-journal` alias | `…/core.clj` |
| `:lint-as` for `defflow` (→ `deftest`) and `with-stack` (→ `let`) | `.clj-kondo/config.edn` (module + election-system) |

Verified (16 tests / 56 assertions):
- **Diff**: an expected subsequence aligns to `[0 2]`; out-of-order expectations diverge at the
  position where the journal ran out, and the message names it, prints the expected event, prints
  `(nothing: the journal ends before this expectation)` when the journal ended, and shows the two
  neighbouring events — 6 lines, not a dump.
- **Typed failure**: `assert-events!` throws `:stateflow/journal-assertion-failed` with the
  expected/actual/`:diverged-at` in `ex-data`; a passing assertion returns the events.
- **Waiting**: `poll-until` succeeds on the third attempt and gives up at the deadline carrying
  the last exception; a falsey value is never success; `eventually` throws
  `:stateflow/eventually-timeout` with the attempt count; `await-status` accepts a set;
  `retry-step` retries a flaky thunk and rethrows the last error.
- **World**: wrong types name the field, a cluster with no reachability is rejected, extra keys
  pass through, URLs are normalized.

Deviations / notes:
- **A validated map, not a record.** The roadmap asked for a `World` record; state-flow's `swap`
  adds keys to the world, and a record would throw on unknown ones. `->world` validates and returns
  an open map, which is the same guarantee without breaking flows that carry their own state.
- **No kondo hook for `with-fault`**: it is a function (`[rule sub-flow]`), not a macro — there is
  nothing for the linter to expand. `with-stack` does need one and gets `:lint-as let`.
- **`defflow` tags are metadata**: `^{:tier :stack :tags #{:db}}` passes through to the var; the
  runner filters on `:tier`, not `:tags`. Tag-based selection is available through the runner's
  `--ns` if it is ever needed.
- ±3 events was reduced to ±2 in the rendered message: the done-when asks for ≤5 lines of
  explanation, and two neighbours on each side is where the extra line stopped being useful.

**Done when.** A failed causal assertion explains what diverged and when in ≤5 lines; onboarding
a new flow takes one doc page. — the message is 6 lines including the header (5 of body), and the
README has a "When a flow fails" page.

**Effort** 2 w (spent: ~1 d). **Risk** low.

### 12. Release and convention parity  `DX` `ERG`

Status: **implemented except publishing**. Every module builds a jar, the four new ones have the
same skeleton, and the release/scaffold tooling exists; nothing was deployed to Clojars from here.

- [x] `build.clj` + `Makefile` + `.version` + README + `GEMINI.md` + `docs/` for `-health`
      and `-circuit-breaker`
- [ ] Publish all four modules; move `d-core.stateflow.e2e` out of the published lib
- [x] Add the four modules to `docs/supported.md` and the changelog
- [x] `module-template` so the next capability is born compliant

**As built**

| Piece | Where |
| :-- | :-- |
| `build.clj` / `Makefile` / `.version` for health, circuit-breaker, async-proxy | each module |
| README + `GEMINI.md` + `docs/` design notes (health, circuit-breaker, resilience, chaos-engine) | each module |
| `release.clj`: `--check` matrix, `--bump` plan, `--apply`, `--tag` | `election-system/scripts/` |
| `new-module.sh` + `module-template/` (`.version`, build, Makefile, deps, README, GEMINI, docs, src+test) | `election-system/scripts/` |
| `make release-check`, `make release-plan` | `election-system/Makefile` |
| `docs/supported.md`, `CHANGELOG.md` | `election-system/` |
| `e2e` moved to `dev/` behind a `:dev` alias | `d-core-stateflow` |
| breaker/health protocols committed | `d-core-std` |

Verified:
- **`clojure -T:build jar` succeeds for all 25 module directories** (the roadmap said 28; the
  workspace has 25). Three needed the sandbox's writable Maven repo (`/tmp/m2repo`) because
  `~/.m2` is read-only here; the build files themselves are unchanged.
- **Skeleton parity**: health and circuit-breaker went from partial to full; async-proxy gained
  the missing `build.clj`, `.version` and `:build` alias.
- **Scaffold**: `new-module.sh d-core-widget "…" /tmp/d-core-widget` produces a module whose
  tests pass, lint is clean, and `clojure -T:build jar` emits `d-core-widget-0.1.0.jar`.
- **Release tool**: `--check` reports 25 modules with valid versions and flags dirty repos;
  `--bump patch` prints `0.1.0 -> 0.1.1` per module without writing.
- **e2e is out of the artifact**: the ns lives in `dev/`, loads on the `:dev` alias, and the jar
  build copies only `src`.

Deviations / notes:
- **Publishing was not performed.** No Clojars credentials in this environment, and a publish is an
  irreversible side effect — `make publish` exists per module and `release.clj --apply --tag`
  prepares the versions/tags, but the deploy step is the operator's.
- **The four modules are not versioned as a set yet.** `--bump` proposes the same bump for every
  module; cross-module dependency pins (`d-core-std` inside each `deps.edn`) are still manual.
- **`docs/supported.md` duplicates the release matrix by hand.** It is prose (what each module
  owns); the versions come from `--check`. Merging the two would automate a table nobody reads
  daily.

**Done when.** `clojure -T:build jar` succeeds for all 28 modules; every module has the same file
skeleton. — all 25 build; health, circuit-breaker, resilience, chaos-engine and stateflow now match
the skeleton (README/GEMINI/docs added where missing).

**Effort** 1–2 w (spent: ~1 h). **Risk** low.

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
