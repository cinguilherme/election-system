# d-core-chaos-cockpit — plan

A control room for the chaos stack: a web application where a human can see the
system, inject a failure, watch what happens, and put it back — and where an RFC
that claims "the system fails over safely" can be **proven** instead of argued.

Status: **plan, no code**. Every decision below was taken in the interview of
2026-10-24; §17 lists what is still unsettled.

---

## 1. What we gain

The scenario runner is an excellent *batch* tool and a poor *conversation* tool. Today a
hypothesis goes: edit an EDN file → `make chaos-run` → wait for a run → read a JSON report.
That loop is minutes long and produces no memory of *why* someone tried it.

The cockpit changes the loop and, more importantly, changes who can participate:

| Before | After |
| :-- | :-- |
| Write a scenario to test one fault on one replica (~5–10 min) | Pick a replica, pick a fault, apply — ~20 s |
| Faults are invisible until you read a report | Blast radius is on screen before you confirm, and a countdown after |
| A failed experiment is a JSON file | A failed experiment is a timeline you can scrub, with the exact rule highlighted |
| "What did we do to staging yesterday?" needs container logs | Audit rows: who, what, target, result, and the resulting fault id |
| Demos are screen-shares of a terminal | A demo is a URL |
| "Does this RFC fail over safely?" is an argument in review | A drill report: RTO 8.2 s, 0.12% errors, zero data loss, per claim |
| Scenario authoring is the only entry point | Manual exploration, then **save as scenario** — the UI becomes the authoring tool for the batch tool |

Four gains justify the build:

1. **Failover RFCs become evidence.** A design review can attach a drill report — claim,
   measured value, pass/fail, timeline — instead of a paragraph asserting the load balancer
   will notice. This is a new capability, not a UI for an existing one (§2).
2. **Incident reproduction in seconds.** Find the action in history, hit *replay*, and the same
   fault set lands on the same selectors. This is the MTTR-to-reproduction metric from the
   roadmap, finally with a button behind it.
3. **The inject → observe → revert loop closes.** A control room is the only place where a human
   can watch a dependency fail and recover on one screen. That loop is what makes the invariant
   library trustworthy — you find the missing invariant by watching, then encode it.
4. **The stack becomes demonstrable.** The scenario library stops being folklore and becomes
   something you can show a new engineer on their first day.

What we do **not** gain: a chaos platform, a replacement for Grafana, a production tool, or a
second source of truth. See §15.

Cost, honestly: a new service to secure, a UI that can go stale and lie if it caches badly, a
second language in the ecosystem (TypeScript), and — for drills — a small, careful extension of
the experiment runner (§5). The mitigation for the last one is design: the runner learns to
*measure* time-series, it never learns the word "failover".

---

## 2. Failover drills — proving an RFC

This is the capability the rest of the document exists to serve, and it deserves to be
specified before the UI.

The system under test has two failover mechanisms today — a load balancer in front of the API
replicas, and consumer distribution across the actor nodes — and no others. This document does
**not** audit them or propose new ones. It specifies how the tooling proves that *whatever*
mechanism an RFC describes actually behaves as claimed.

### 2.1 Why a scenario is not enough

A scenario asserts an invariant at a point in time: *after* the fault, the audit still reconciles.
That answers "was the data correct at the end". An RFC claims something stronger and temporal:

> RFC-142: if an API replica dies, the load balancer routes to the healthy replicas; users see
> at most 0.5% errors; the system is fully serving again within 15 s; no vote is lost or duplicated.

Four of those five claims are about the **transition**, not the end state. A scenario can inject
the fault and check the last clause. It cannot answer: how long was the gap, how many requests
failed while the balancer decided, or whether the remaining replicas actually absorbed the load.

What is missing is measurement, not injection.

### 2.2 The failover contract

A drill is a scenario plus a **contract**: named claims with measurable thresholds. The vocabulary
is deliberately small.

| Claim | Measured by | Example threshold |
| :-- | :-- | :-- |
| `rto-ms` | time from the action to the first *sustained* clean window | ≤ 15 000 |
| `error-budget` | failed probe requests / total during the drill | ≤ 0.5% |
| `max-error-burst` | longest run of consecutive failures | ≤ 3 s |
| `capacity` | p99 of successful probes after the transition | ≤ 250 ms |
| `traffic-shift` | the failed replica's request counter stops; survivors' counters rise | exact |
| `rebalance` | every partition owned again within RTO; unowned window ≤ grace | ≤ 8 000 ms |
| `data-integrity` | the existing invariants (tally = Σ ballots, no duplicates) | exact |
| `failback` | after the node returns, traffic resumes and errors stay in budget | ≤ 30 000 ms |

A claim is a tuple of *(name, measurement, threshold)*. The report maps one to one: **RFC §3.2
"recovery under 15 s" → 8 200 ms → PASS**. That mapping is the product.

### 2.3 The anatomy of a drill

```
baseline (30 s) → arm → act (kill / pause / fault) → observe → settle
                → verify claims → restore → failback → report
```

Same runner, same scenario file format, same seeds, same reports — with temporal assertions
instead of point-in-time ones. A drill that fails is a **valid result**: it reports "RFC not
proven", with the numbers that disprove it. The tool must never retry until green; a retry is
recorded as a retry and reported as such.

### 2.4 What exists, what is missing

| Capability | Today | Verdict |
| :-- | :-- | :-- |
| Cause the failure (kill/pause a container, inject a rule) | engine `/api/containers/:op`, `/api/faults` | exists |
| Point-in-time invariants and journal assertions | scenario `:checks`, `:journal` | exists |
| Seeds, replay, JSON/JUnit reports | engine runner | exists |
| Continuous request traffic during the drill | — | **missing** |
| Time-series of a metric or a JSON field (per-replica counters, owned partitions) | `:checks` are single samples | **missing** |
| RTO / error-budget arithmetic and thresholds | — | **missing** |
| Parameterised drills (which replica, which RTO) | scenarios are static files | **missing** |
| A deliberate pause for a human runbook step | — | **missing** |
| Evidence artifact tied to RFC claims | JUnit/JSON reports | **missing** |

Everything missing is a *measurement* primitive. None of it knows what a load balancer is, which
is what keeps it out of `d-core-chaos` and generic enough to prove an RFC nobody has written yet.

### 2.5 The two drills that exist today

These are the acceptance tests for the tooling. If the cockpit can produce trustworthy evidence
for them, the capability is real.

**D1 — `failover-api-replica`.** Kill one API replica behind the balancer.

- probe the public entry (through the balancer) at ~10 rps for the whole drill;
- watch each replica's request counter (from its metrics endpoint) at 1 Hz;
- claims: `rto-ms`, `error-budget`, `traffic-shift` (the dead replica's counter freezes, the
  survivors' counters rise), `capacity`, `data-integrity`, `failback` on restart.

**D2 — `rebalance-actors-replica`.** Kill one actors replica.

- probe the write path (through the balancer) at ~10 rps;
- watch each actor replica's `/health` `owning-partitions` field at 1 Hz;
- claims: `rebalance` (all partitions owned again within RTO, unowned window under the grace),
  `error-budget`, `data-integrity` (tally = Σ ballots, no duplicates — a killed node may lose
  its buffered batch, and *that is exactly the finding the drill exists to surface*), `failback`.

D2 is the more interesting one: an RFC that claims "no data loss on node failure" can be proven or
refuted in one run, and the difference between "we think the flush policy is fine" and a number.

### 2.6 Beyond today: stateful tiers and the manual gate

Promoting a database primary, or switching a single-writer component to a standby, is a runbook
action with real data risk. The tool must not automate it and must not pretend to.

The drill encodes the phases and stops at a **`gate`** — a step that waits for the operator to
confirm the runbook step (`POST /api/runs/:id/gate/promote`) with a timeout and an abort path — and
then measures the rest: reconnect time, error burst, RTO, and RPO by comparing a pre-drill
watermark with post-failover data (counts, max ids, checksums), not by trusting the tooling.

This is how a future RFC for a stateful tier gets proven *without* the cockpit becoming a database
operator. The gate is also honest in the report: "promotion performed by a human at 09:14:02".

### 2.7 Evidence

Every drill produces a markdown + JSON artifact:

```markdown
# Failover drill — API replica 2
2026-10-25T09:12:04Z · scenario failover-api-replica · seed 1234 · operator sam

| Claim | RFC target | Measured | Result |
| :-- | :-- | :-- | :-- |
| RTO | ≤ 15 s | 8.2 s | PASS |
| Error budget | ≤ 0.5% | 0.12% (37 / 30 412) | PASS |
| Traffic shift | dead replica at 0 | 0 after 1.1 s | PASS |
| Capacity p99 | ≤ 250 ms | 118 ms | PASS |
| Data integrity | tally = Σ ballots | 1 204 = 1 204 | PASS |
| Failback | ≤ 30 s | 11.4 s | PASS |

Timeline: chaos-rule 3f2a… → container kill api-2 → LB marked down 1.1 s → …
```

Attach it to the RFC's pull request. Keep every run: "RTO was 8.2 s in March and 22 s after the
ingress change" is the regression signal a failover plan needs, and it is a query, not a meeting.

### 2.8 What a drill is not

- Not a load test. The built-in probe is a **canary** (low rate) that measures availability and
  latency; it says nothing about capacity under production load. Capacity claims need a concurrent
  load test, and the drill can read its numbers from Prometheus (`:load` mode, §4.6) instead of
  pretending the canary is traffic.
- Not a failover mechanism. It proves the mechanism the RFC describes; it does not create,
  configure or promote anything.
- Not a guarantee. A green drill is evidence for the tested scenario on the tested topology; the
  report says exactly what was tested, on which replicas, at which time.

---

## 3. Decisions locked

| Decision | Choice |
| :-- | :-- |
| Audience | Shared staging control room, a few engineers, concurrent sessions |
| Scope | Chaos control **+ read-only** observability (health, breakers, partitions, journal) |
| Blast radius | Two-tier: presets for everyone, free-form builder for operators |
| Auto-revert | Every manual fault expires by default; visible countdown; capped maximum |
| Liveness | Polling every 1–2 s for v1; SSE is a v2 option |
| Stack | React + TypeScript SPA (`web/`), Clojure BFF, one container |
| Identity | Shared chaos token + typed name; operators from an env allowlist |
| Observability source | BFF fans out to each replica's admin port directly |
| History | Cockpit-owned SQLite: sessions, actions (audit), run cache, presets, drills |
| Placement | `d-core-chaos-cockpit` module, its own compose service, same-origin |
| Failover proof | A drill is a scenario + claims; measurement primitives live in the engine (§4.4) |

---

## 4. Architecture

```
                      browser (React SPA, same origin)
                              │  session cookie + CSRF header
                              ▼
   ┌──────────────────────────────────────────────────────────┐
   │  d-core-chaos-cockpit  (:8090)                            │
   │                                                           │
   │  BFF (Clojure)                                            │
   │   ├── session + roles      ── SQLite (sessions, actions,  │
   │   ├── /api/* (SPA-facing)      drills, runs, presets)     │
   │   ├── fan-out poller       ── cache 1 s, bounded, tolerant│
   │   └── static resources/public (built SPA)                 │
   └───────────┬───────────────────────────┬──────────────────┘
               │ X-Chaos-Token             │ X-Chaos-Token (per node)
               ▼                           ▼
      chaos-engine :8088          node admin :8082 (api, actors, …)
      /api/faults, /api/runs      /_chaos/status, /_chaos/rules,
      /api/scenarios, /api/…      /health, /ready
               │
               │ probe traffic + metric watches (drills only)
               ▼
      nginx :8080, per-replica /metrics, node /health
```

**Responsibilities, deliberately split:**

| Concern | Owner | Why |
| :-- | :-- | :-- |
| Fault rules and their lifecycle | engine + nodes (unchanged) | it already works and is tested |
| Measurement during a drill | **engine** (new, generic) | a drill must run in CI without a UI |
| Drill evidence and history | cockpit | human-facing artifact, per-team retention |
| Who did what | cockpit SQLite | the engine has no user concept |
| Per-replica health/breaker view | cockpit fan-out | the engine is a fault engine |
| Session, roles, CSRF, rate limits | cockpit | browser concerns |
| TTL enforcement | **nodes** (existing `:for`) | safety must not depend on the cockpit being up |

The last row is the strongest design constraint: **the cockpit is never required for safety**. If
it dies mid-drill, every fault still expires and the container kill still has its restart policy.

### 4.1 Data flow for one injection

1. SPA posts `POST /api/faults` with `{preset | rule, selector, ttl-ms, note}` and the CSRF header.
2. BFF checks the session, role, TTL bounds, and that the selector only names registered targets.
   It writes a *pending* action row.
3. BFF posts to the engine `POST /api/faults` with the rule including `:for` (TTL) and a `:note`
   carrying the actor.
4. Engine fans the rule out to the matching replicas; returns per-target status.
5. BFF completes the action row with the fault id and results, returns the fault view.
6. SPA shows the countdown from **server-provided `expires-at-ms`**, never a client timer.

### 4.2 Data flow for one drill

1. SPA posts `POST /api/drills/failover-api-replica/run` with `{params: {replica: 2}, seed?}`.
2. BFF validates params against the drill's declared schema, enforces "one drill at a time", and
   calls the engine `POST /api/scenarios/failover-api-replica/run` with the params.
3. The engine runs the drill: baseline, probe, act, watch, claims, restore, failback.
4. The SPA polls `GET /api/drills/:id` (1 s). The BFF proxies the engine's live run and adds the
   cockpit's own audit row.
5. On completion the engine writes the drill report; the cockpit stores the claim summary in
   SQLite and offers markdown/JSON export.

The cockpit adds no semantics here: a drill that runs in CI is byte-identical to one started from
the button, because it *is* the same engine run.

### 4.3 Why the probe lives in the engine, not the cockpit

A nightly RFC regression must run without someone opening a browser. If the traffic generator lived
in the cockpit, "prove the RFC" would only work while a UI was up. So the engine gains the
measurement primitives, and the cockpit becomes a good client of them — the same boundary rule as
before: *if a requirement can be met by calling the existing API, it belongs to the cockpit; if it
cannot, it belongs in the component that must run unattended.*

### 4.4 The engine additions (all additive, all generic)

| Addition | What it does | Why generic |
| :-- | :-- | :-- |
| `:probe` step | sends HTTP requests at a fixed rate for a duration; records per-second success, failures by status, p50/p99, and an optional JSON field extracted from each response | it measures availability, not failover |
| `:watch` step | polls a URL at 1 Hz and records a value: a JSON field, or a single metric from a Prometheus text endpoint | it measures a series, not a concept |
| `:claims` step | evaluates the collected series against thresholds: `rto-ms`, `error-budget`, `max-error-burst`, `converged`, `stable`, `monotonic` | arithmetic over series |
| `:gate` step | pauses until `POST /api/runs/:id/gate/:name` or a timeout | a human checkpoint |
| `:params` | typed, defaulted parameters on a scenario, overridable in `POST /api/scenarios/:name/run` | every scenario can be parameterised |
| drill report section | claims table + the series + the phase timeline in the JSON/JUnit report | reporting |

The engine never learns what a load balancer is. The *scenario file* says "watch this counter and
claim RTO ≤ 15 s"; the engine just measures.

### 4.5 RTO, defined once

RTO is the time from the recorded act (the kill, the rule firing) to the **first of a sustained
clean window**: no failed probe for 5 consecutive seconds, and at least 3 successful requests in
that window. The definition is part of the report, because "recovery" without a definition is how
two people argue for an hour about the same run.

### 4.6 Canary vs load

The built-in probe is deliberately light (default 10 rps, configurable per drill). It answers
"were users being served" and nothing about capacity under load. For capacity claims the drill
declares `:load {:source :prometheus :query …}` and reads the numbers from a concurrent load test.
Prometheus is optional; without it, drills still produce RTO, error budget and integrity evidence,
and the report says `capacity: not measured` rather than inventing a number.

---

## 5. API surface — what exists, what we add

### 5.1 What the containers already expose (verified in the code)

| Need | Endpoint | Exists |
| :-- | :-- | :-- |
| Engine liveness + counts | `GET /api/health` | yes |
| Targets with service/replica/base-url/admin port | `GET /api/targets` | yes |
| Inject / list / get / revert a fault | `POST/GET /api/faults`, `GET/DELETE /api/faults/:id` | yes |
| Kill switch | `POST /api/kill-switch` | yes |
| Experiments + merged journal | `GET/POST /api/experiments`, `GET /api/experiments/:id/journal` | yes |
| Scenarios, run with seed, abort | `GET /api/scenarios`, `POST /api/scenarios/:n/run`, `POST /api/runs/:id/abort` | yes |
| Runs + reports (JSON/JUnit) | `GET /api/runs`, `GET /api/runs/:id`, `GET /api/runs/:id/report` | yes |
| Network faults (Toxiproxy) | `/api/network/*` | yes |
| Container pause/kill/restart | `GET /api/containers`, `POST /api/containers/:op` | yes |
| Per-replica chaos state | `GET /_chaos/status`, `/_chaos/rules`, `/_chaos/audit` | yes, per node |
| Per-replica health | `GET /health`, `/ready`, `/live` on `:8082` | yes, per node |

**Cockpit v1 (fault injection) requires zero changes to `d-core-chaos` and `d-core-chaos-engine`.**
The gap list from the interview is closed in the BFF:

| Gap | Closed where |
| :-- | :-- |
| No CORS / no session | same-origin BFF; browser never talks to the engine |
| No live feed | BFF poller (1 s cache) + SPA poll 1–2 s |
| No aggregated per-replica view | BFF fan-out to `:8082`, cached, partial failures tolerated |
| No engine-side audit | cockpit SQLite |
| Runs lost on engine restart | cockpit caches run summaries; reports stay on disk |
| No presence | BFF session table |
| No breaker reset | **genuine gap** — see §5.3 |

### 5.2 What drills add

| Change | Component | Breaking? |
| :-- | :-- | :-- |
| `:probe`, `:watch`, `:claims`, `:gate` steps | `d-core-chaos-engine` (new step types) | no — existing scenarios are untouched |
| `:params` on scenarios + run payload | engine | no — absent params keep the old behaviour |
| `POST /api/runs/:id/gate/:name` | engine | no |
| drill section in the report | engine | no — additive key |
| `drills/` discovery (or `:kind :drill` in `scenarios/`) | engine | no |

None of this touches `d-core-chaos` (the library). The node-side rule schema already supports
everything the drills inject; the new work is measurement in the orchestrator.

### 5.3 The one genuine gap, unchanged

**`POST /admin/breakers/:name/reset`** does not exist anywhere. The breaker self-heals after
`:reset-timeout-ms`, so it is a convenience, not a blocker, and it belongs to the *application*
admin — not to chaos. Deferred.

---

## 6. Safety model

The cockpit is a machine for causing incidents. Its safety design is the product.

**Bounds, enforced server-side (never only in the UI):**

| Knob | Default | Cap |
| :-- | :-- | :-- |
| Fault TTL | 5 min | 30 min |
| Faults active at once | 10 | 25 |
| Concurrent experiments | 1 | 1 |
| Concurrent drills | 1 | 1 |
| Drill probe rate | 10 rps | 50 rps |
| Raw-rule mutations per session | 30/min | — |
| Login attempts per IP | 5/min | — |

**Invariants:**

1. Every injected fault has a TTL. The API rejects `ttl-ms` outside `[5s, 30min]`.
2. The kill switch is visible on every screen and never disabled — including when the engine is
   unreachable, where it degrades to calling each node's `/_chaos/kill-switch` directly.
3. Destructive actions (kill switch, panic, container kill, TTL > 10 min, **any drill**) require
   typing the target's name to confirm.
4. Blast-radius preview: before confirm, the UI shows which replicas the selector matches and how
   many faults are already active on them.
5. One experiment at a time; manual faults during an experiment are attributed to it in a banner.
6. **Drills have a pre-flight**: the target replica must be healthy, no fault may be armed against
   the drill's selectors, and the drill aborts rather than starting from a dirty state.
7. A drill's probe traffic is bounded and tagged (`X-Chaos-Drill: <id>`) so metrics can exclude it
   and an operator can see what the cockpit is doing.
8. **No automatic promotion of a stateful tier, ever.** The `:gate` stops and waits for a human.
9. Panic: disable controllers → kill switch on all nodes → remove all faults → reset toxics →
   unpause/restart paused containers → **abort any running drill** → report what it could not reach.

---

## 7. Screens

| Screen | Content | Endpoints |
| :-- | :-- | :-- |
| **Login** | token + name; error feedback; rate limited | `POST /api/session` |
| **Control Room** (default) | engine + node health, service×replica grid with breaker badges, active faults with countdowns, journal tail, presence, kill switch | `GET /api/overview` |
| **Fault Composer** (drawer) | preset picker, or operator raw form; selector preview; TTL slider; note; confirm-by-typing for destructive | `GET /api/presets`, `POST /api/faults` |
| **Active Faults** | rule, selector, hits, remaining TTL, extend (+5 min, capped), revert, revert-all | `GET /api/faults`, `POST /api/faults/:id/extend`, `DELETE /api/faults/:id` |
| **Drills** | available drills with last result and last RTO; run dialog (params, seed, RFC reference); history column | `GET /api/drills`, `POST /api/drills/:name/run` |
| **Drill Run** | phase timeline, live RTO timer, error sparkline, claim checklist, evidence export | `GET /api/drills/:id` |
| **Drill History** | runs over time, RTO trend per drill, compare two runs, evidence download | `GET /api/drills?name=` |
| **Scenarios** | list with last-run status; run dialog; live progress; report link | `/api/scenarios`, `/api/runs/:id` |
| **Timeline** | journal viewer: experiment selector, filters, causal order | `GET /api/journal` |
| **History** | audit rows, replay an action set, save as preset, export as scenario | `GET /api/history` |
| **Panic** | confirm-by-typing, step-by-step result list | `POST /api/panic` |

**Drill Run is the screen this document is about.** It shows, live: which phase is running, a clock
counting toward (or past) the RTO target, a sparkline of probe results with failures in red, and a
claim list that fills in as measurements land — `RTO: 8.2s / ≤15s PASS`, `errors: 0.12% PASS`,
`traffic shift: pending`. When the drill fails, the failing claim is expanded with its series, not
hidden behind a green summary.

UX rules that keep the UI honest: every panel shows *last updated*; a stale panel is visibly stale;
no optimistic state for fault or drill lists (the engine is the truth); the kill switch is never
hidden behind a menu; a drill's report is downloadable as-is, never summarised into a screenshot.

---

## 8. Storage (SQLite)

```sql
CREATE TABLE sessions (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL,   -- 'viewer' | 'operator'
  created_at INTEGER NOT NULL, last_seen_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL, ip TEXT
);

CREATE TABLE actions (            -- the audit trail
  id TEXT PRIMARY KEY, session_id TEXT, actor TEXT NOT NULL, role TEXT NOT NULL,
  action TEXT NOT NULL,           -- inject | revert | extend | drill | run | abort | panic | login
  target_kind TEXT, target_id TEXT,
  payload_json TEXT NOT NULL,
  result TEXT NOT NULL,           -- ok | partial | rejected | error
  detail_json TEXT,
  fault_id TEXT, experiment_id TEXT, run_id TEXT,
  created_at INTEGER NOT NULL
);

CREATE TABLE runs (               -- cache of engine runs, survives engine restarts
  run_id TEXT PRIMARY KEY, scenario TEXT, kind TEXT,        -- 'scenario' | 'drill'
  status TEXT, seed INTEGER, started_at INTEGER, duration_ms INTEGER, passed INTEGER,
  summary_json TEXT, cached_at INTEGER
);

CREATE TABLE drills (             -- one row per drill run: the RFC evidence index
  run_id TEXT PRIMARY KEY, drill TEXT NOT NULL, params_json TEXT,
  rfc_ref TEXT,                   -- e.g. "RFC-142 §3.2"
  started_at INTEGER, finished_at INTEGER,
  passed INTEGER, rto_ms INTEGER, error_rate REAL, claims_json TEXT,
  evidence_md TEXT, evidence_json TEXT
);

CREATE TABLE presets (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT,
  payload_json TEXT NOT NULL, created_by TEXT NOT NULL, created_at INTEGER NOT NULL
);
```

Retention: actions 90 days, runs 30 days, **drills kept indefinitely** (they are evidence),
sessions 12 h idle.

---

## 9. BFF contract (SPA-facing)

All JSON, all under `/api`, all require a session except `POST /api/session` and `GET /api/healthz`.

| Method | Path | Notes |
| :-- | :-- | :-- |
| `POST` | `/api/session` | `{token, name}` → sets `cockpit_session` (HttpOnly, SameSite=Strict, Secure) |
| `GET`/`DELETE` | `/api/session` | who am I / logout |
| `GET` | `/api/overview` | Control Room payload: engine, targets, node health (cached), faults, active run, presence |
| `GET` | `/api/targets` | passthrough with admin URLs |
| `GET`/`POST` | `/api/faults` | list / inject (role-checked, TTL-bounded) |
| `POST` | `/api/faults/:id/extend` | `{ms}` capped |
| `DELETE` | `/api/faults/:id` | revert |
| `GET` | `/api/presets`, `POST /api/presets` | list / save |
| `GET` | `/api/drills` | available drills + last result |
| `POST` | `/api/drills/:name/run` | `{params, seed?, rfc-ref?}`; pre-flight, one at a time |
| `GET` | `/api/drills/:id` | live run + claims + series (proxied, cached 1 s) |
| `GET` | `/api/drills/:id/evidence?format=md|json` | the artifact |
| `GET` | `/api/drills?name=` | history, paginated |
| `POST` | `/api/drills/:id/gate/:name` | confirm a manual drill step |
| `GET` | `/api/scenarios`, `POST /api/scenarios/:name/run`, `POST /api/runs/:id/abort` | |
| `GET` | `/api/runs`, `/api/runs/:id` | cached summaries + live detail |
| `GET` | `/api/journal?experiment=&since=` | normalised timeline |
| `GET` | `/api/history` | audit, paginated, filter by actor/action |
| `POST` | `/api/kill-switch`, `POST /api/panic` | |
| `GET` | `/api/healthz` | for compose; no session |

Error model: `{error: {code, message, details}}` with stable codes (`forbidden`, `ttl-out-of-range`,
`selector-not-found`, `drill-preflight-failed`, `drill-in-progress`, `engine-unreachable`,
`partial`). The SPA switches on `code`, never on message text.

Mutations require `X-CSRF-Token` matching the session's token (double submit).

---

## 10. Front end

- **Stack**: Vite + React + TypeScript, MUI, react-router, TanStack Query for polling
  (`refetchInterval` 1000–2000 ms per panel, 1 s on a running drill), zod for response validation.
- **Layout**: app bar (engine state, experiment banner, kill switch, presence) + left nav + content.
  Control Room is a CSS grid of service columns, replica cards inside. Drill Run is a full-width
  timeline with a claim sidebar.
- **State**: server state in TanStack Query; the only client state is form state and filters.
- **Reuse**: copy the POC's Vite/MUI/eslint setup from `js-fe-client-poc/my-app` (house style),
  not its code.
- **Build**: `vite build` → `resources/public`; hashed assets long-cached, `index.html` no-store.
- **Accessibility**: keyboard-only path to the kill switch; the RTO clock is `aria-live="polite"`
  only when a claim crosses its threshold, not every second.

---

## 11. Deployment

```yaml
# docker-compose.chaos.yaml (addition)
  chaos-cockpit:
    build: ../d-core-chaos-cockpit
    ports: ["127.0.0.1:8090:8090"]
    environment:
      CHAOS_ENGINE_URL: http://chaos-engine:8088
      CHAOS_TOKEN: ${CHAOS_TOKEN:?}
      COCKPIT_OPERATORS: ${COCKPIT_OPERATORS:-}
      COCKPIT_SESSION_SECRET: ${COCKPIT_SESSION_SECRET:?}
      COCKPIT_DB: /data/cockpit.db
      COCKPIT_TTL_DEFAULT_MS: "300000"
      COCKPIT_TTL_MAX_MS: "1800000"
      COCKPIT_PROBE_MAX_RPS: "50"
    volumes: [cockpit-data:/data]
    depends_on: {chaos-engine: {condition: service_healthy}}
    healthcheck: {test: ["CMD", "curl", "-fsS", "http://127.0.0.1:8090/api/healthz"], interval: 5s}
```

Engine-side additions for drills:

```yaml
  chaos-engine:
    environment:
      CHAOS_PROBE_MAX_RPS: "50"                 # hard cap for :probe
      CHAOS_PROBE_ALLOWED_HOSTS: "nginx,election-api-backend,election-actors"  # SSRF guard
      CHAOS_PROMETHEUS_URL: ""                  # optional, for :load mode
```

- Published to **127.0.0.1 only**, same posture as the rest of the chaos overlay.
- If `CHAOS_TOKEN` is unset, the container refuses to start (fail closed).
- Multi-stage Dockerfile: `node:22` builds `web/`, `clojure` builds the BFF jar, final image runs it.

---

## 12. Milestones

Two tracks that converge. Track A is the cockpit; Track B is the engine's measurement primitives.

| # | Deliverable | Acceptance | Effort |
| :-- | :-- | :-- | --: |
| **A0** | Repo, BFF skeleton, session, compose service, CI | login → `/api/overview` returns engine health | 3 d |
| **A1** | Read-only Control Room: fan-out, cache, degraded mode, grid, journal tail | kill a node → badge red within 3 s; stop the engine → banner, UI keeps serving cached data, no crash | 4 d |
| **A2** | Injection with TTL: presets, composer, active faults, countdown, revert, audit, **kill switch** | inject 30 s TTL on one replica → auto-reverts; audit names the actor; over-cap TTL rejected; the kill switch stops injection from every screen | 5 d |
| **B0** | Engine `:probe` + `:watch` | a scenario can generate 10 rps and record success/latency and a watched field series | 3 d |
| **B1** | Engine `:claims` + RTO definition + report section | a claim `rto-ms ≤ 15000` evaluated from a series, in the JSON report | 2 d |
| **B2** | Engine `:params` + `:gate` + `POST /api/runs/:id/gate/:name` | a scenario runs with `{replica: 2}`; a gate blocks until confirmed or times out | 2 d |
| **A3** | **Drill Run screen + evidence + history** | D1 and D2 run from the UI, claims fill in live, markdown evidence downloads | 4 d |
| **A4** | Scenarios, runs, bulk panic | from a dirty state, panic leaves zero faults, zero toxics, zero paused containers, zero running drills | 4 d |
| **A5** | Roles, free-form builder, presence, save-as-scenario | viewer gets 403 on raw inject (audited); exported EDN validates and runs | 5 d |
| **A6** | Polish, docs, Playwright e2e | an RFC can be proven end-to-end from a clean checkout | 3 d |

**~37 dev-days** for one engineer, with Track A and B partially parallel. Two useful cuts:

- **Fault injection MVP**: A0–A2 = **12 days** (no engine changes at all).
- **RFC proof**: A0–A3 + B0–B2 = **25 days**, and that is the milestone that delivers the capability
  the rest of this document is about.

---

## 13. Testing

| Layer | What |
| :-- | :-- |
| BFF unit | session lifecycle, role enforcement, TTL bounds, selector allowlist, CSRF, rate limits, audit rows — with a fake engine |
| Engine unit | `:probe` rate/timing accuracy, `:watch` series extraction, `:claims` arithmetic (RTO/burst/convergence), `:gate` timeout |
| Contract | run the BFF against a real engine on a random port; fail the build if a response shape changes |
| Degraded mode | engine down, node down, node slow, partial fan-out, engine restart mid-drill |
| SPA | vitest for composer validation and claim rendering; Playwright for login → inject → revert → drill |
| Drill acceptance | D1 and D2 against the compose stack, nightly, with the evidence artifact attached to the run |
| Safety | kill the cockpit container mid-experiment → every fault still expires; abort a drill mid-probe → no orphaned fault, no orphaned probe |

The drill acceptance tests are the ones that matter: if D1 and D2 cannot produce trustworthy
evidence, no amount of UI polish makes the capability real.

---

## 14. Security and threat model

| Threat | Mitigation |
| :-- | :-- |
| Token theft from the browser | token never leaves the server; server-side session |
| CSRF | SameSite=Strict + double-submit `X-CSRF-Token` |
| SSRF via "target URL" | the BFF only builds URLs from engine targets; ports restricted to 8082 |
| **SSRF via drill probe/watch URLs** | the engine's `:probe`/`:watch` accept only registered targets or an explicit `CHAOS_PROBE_ALLOWED_HOSTS` allowlist; no redirects, no userinfo, no arbitrary ports |
| Probe used as a load generator | `CHAOS_PROBE_MAX_RPS` hard cap, 50 rps |
| Probe contaminating metrics | requests tagged `X-Chaos-Drill: <id>`; dashboards exclude them |
| Viewer escalating | role checked server-side per route; every rejected attempt audited |
| Brute force | 5 logins/min/IP, constant-time token compare |
| Session fixation | new session id on login; 12 h idle expiry |
| Cockpit reachable from an untrusted network | published on 127.0.0.1 only; TLS via reverse proxy when it leaves the box |
| Secrets in logs | redact `token`/`cookie`/`authorization` |

---

## 15. Non-goals

- User management, SSO, teams, permissions beyond viewer/operator.
- Multi-cluster or multi-tenant views.
- Charts and dashboards — link to Grafana; do not rebuild it.
- **Building a failover mechanism.** The tool proves the RFC; it does not create replicas, configure
  the balancer, or promote a database. A drill whose RFC is unimplemented fails, correctly.
- Enabling chaos in production.
- Replacing the scenario format: the cockpit exports scenarios and drills; it does not define a
  second one.
- A generic fault DSL beyond what `d-core-chaos` validates.
- Load testing — the probe is a canary; capacity claims come from a concurrent load test.

---

## 16. Risks

| Risk | Impact | Mitigation |
| :-- | :-- | :-- |
| Requirements leak into `d-core-chaos` | the coupling the module exists to avoid | the library is untouched; the engine gains only measurement primitives |
| The UI becomes a second source of truth | divergence between UI and reality | engine authoritative; no optimistic lists; "last updated" everywhere |
| **A drill is trusted as proof of more than it tested** | false confidence in an RFC | the evidence names the topology, replicas, time and seed; "not measured" is a valid cell, never a guess |
| Probe too light to trip the balancer | a green drill that proves nothing | drill pre-flight asserts the probe is flowing to the target; D1/D2 calibrate the rate; report includes request counts |
| Probe destabilises staging | an experiment becomes an outage | 50 rps cap, explicit allowlist, `X-Chaos-Drill` tag, panic aborts it |
| RTO definition disputes | the same run argued about forever | one definition, in §4.5, printed in every report |
| Fan-out cost at scale | poller load grows with replicas | 1 s cache, bounded concurrency, per-node timeout, circuit-broken fan-out |
| Stale UI hides a dangerous state | someone believes a fault is reverted | server-computed countdown; stale panels marked; panic re-queries before acting |
| Build complexity (node + clojure) | slower pipeline | multi-stage image; separate front-end build |
| TypeScript drift from the BFF | runtime surprises | zod validation in development; contract tests |

---

## 17. Open questions (with proposed defaults)

| # | Question | Proposed default |
| :-- | :-- | :-- |
| 1 | Default and maximum fault TTL | 5 min default, 30 min cap |
| 2 | Cockpit reachable at `127.0.0.1:8090` or through nginx? | 127.0.0.1 only for v1 |
| 3 | Breaker reset endpoint in the app admin — now or later? | later; read-only breaker state in the cockpit |
| 4 | "Save as scenario" in A5 (recommended) or earlier? | A5 |
| 5 | Seed presets with the module or only in the team DB? | four seeded + team presets |
| 6 | Election business actions in the cockpit? | no — non-goal |
| 7 | UI copy language | English |
| 8 | **RTO definition** | first of a 5 s clean window with ≥ 3 successes (§4.5) |
| 9 | **Probe rate default** | 10 rps, cap 50 |
| 10 | **Drills live in `scenarios/` with `:kind :drill` or a separate `drills/` dir?** | separate `drills/` dir, same schema, discovered by the runner |
| 11 | **Capacity claims in v1?** | no: canary only; `:load`/Prometheus mode in a follow-up, report says "not measured" |
| 12 | **May a drill kill a stateful singleton (async-proxy) before a secondary exists?** | no — drills declare `:requires`, and the runner refuses if the topology lacks redundancy |

---

## 18. Why a separate module is the right call

`d-core-chaos` is a library that injects faults into a process; `d-core-chaos-engine` is a service
that orchestrates experiments; `d-core-chaos-cockpit` is an application that a human operates. The
three have different lifecycles, different threat models and different reasons to change. Sharing a
repository would mean a UI tweak can force a library release, and a session-cookie bug could live in
the same artifact a service embeds.

The rule for the boundary: **if a requirement can be met by calling the existing API, it belongs to
the cockpit.** The first requirement that could not — measuring a drill — went to the engine,
because a nightly RFC regression must run without a browser. It went there as *measurement*
(`probe`, `watch`, `claims`, `gate`), not as failover semantics, so the engine stays reusable for
any RFC anyone writes next.
