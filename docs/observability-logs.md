# Log aggregation (Loki) — plan

## Motivation

Metrics answer *what* and *how much*; traces answer *where the time went* for the requests
that were sampled. Neither answers **why**, and in this lab the gap has already cost us
time:

- The API returned `503 {"reason":"circuit-breaker-open"}`. The trace shows the span
  failing; the **cause** — which dependency timed out, when the breaker tripped, what the
  exception was — exists only in process logs, and today the process logs it nowhere.
- The API crash loop (`Invalid resilience policy "election-db"`) was visible only through
  `docker logs`. Jaeger's traces are **in memory** (`MEMORY_MAX_TRACES=10000`), so a crash
  loses the evidence; logs are the durable record.
- A chaos drill's decisions (which container a `:replica 2` resolved to, why a step failed)
  are engine stdout. Reconstructing a drill's timeline means reading ten containers in
  parallel.
- One request crosses nginx → API → async-proxy → actors → Postgres. Correlating a single
  bad request across those by hand is exactly the work an aggregator removes.

So the goal is not "collect logs" but **answer "why" without `docker logs` on five
containers**, and keep the answer after the container that produced it is gone.

## What we have today (measured)

| Piece | State |
| :-- | :-- |
| Jaeger | all-in-one, **in-memory** traces (10k), sampled at 5% |
| Prometheus | app `/metrics` + cAdvisor; dashboards provisioned |
| Grafana | Prometheus + Jaeger datasources, **baked into the image** |
| Logs | only `docker logs` (json-file). nginx access log → stdout, with `rt`/`urt`/`upstream` |
| App logging | **8 `println`s total** (startup only). No request, error, level or trace ids. `slf4j-nop` on the classpath |

Under the 100-VU soak, nginx produced ~2.2k lines/min (~450 KB/min, **~0.65 GB/day** at
full load); the API, actors, proxies and Postgres logged nothing but startup. Idle traffic
is ~30 lines/min (health checks).

That last row is the important finding: **Loki alone would collect startup lines and nginx
access logs**. The aggregator becomes genuinely useful only when the applications log
events — especially with a trace id, so a log line and a trace are two views of the same
request.

## Correlation: the id that must survive every hop

### The CID is a path, not a single id

The originator sets the root as `<app>.<use-case>.REQ` — e.g. `APPX.SIGNIN.REQ` names the
application and the use case, not a user. Services **adopt** it and append; they never
invent it. Each hop appends its span:

```
APPX.SIGNIN.REQ                       ← business context (set by the client / entry app)
APPX.SIGNIN.REQ.3znzifu93mze          ← + request id (the trace id, at ingress)
APPX.SIGNIN.REQ.3znzifu93mze.ysqgm97g ← + this hop's span
…                                     ← + every downstream hop
```

That gives three zoom levels from the same header:

| Prefix query | Returns |
| :-- | :-- |
| `APPX.SIGNIN.REQ` | every sign-in request of that app |
| `APPX.SIGNIN.REQ.<request-id>` | one request, every service |
| `APPX.SIGNIN.REQ.<request-id>.<hop>` | one hop |

The CID is capped at **256 characters** (`d-core.tracing/max-cid-length`): at the cap a hop
keeps the parent CIDs instead of growing it, so deep retry chains stay queryable. The
business prefix is capped at 200, leaving room for the request id and a hop.

It travels inside the existing header as an extra key, so nothing new is threaded through
gRPC: `t=…;s=…;p=…;c=APPX.SIGNIN.REQ.3znzifu93mze.ysqgm97g`. Logs carry `cid`, `traceId` and
`otelTraceId` side by side. The CID is a **log-level** device (not sampled, unlike traces):
it is what makes "this error, then that alternative flow" readable across services and
requests.

Do not put user identifiers in the prefix — logs are centralized and retained. Use the app
and the use case, or a pseudonymous id.

### The motivating case: a timeout

The response is gone, but the CID was minted on the **request**, so every log line of that
request is still queryable. The API now logs one `:downstream-call` line per dependency
(`call`, `durationMs`, `outcome`, `error`) on the same cid:

```
APPX.SIGNIN.REQ.1g8qn6qt6gro.c3vzgoez  :downstream-call  election-tally  351ms  ok
APPX.SIGNIN.REQ.1g8qn6qt6gro.c3vzgoez  :downstream-call  db-audit        247ms  ok
APPX.SIGNIN.REQ.1g8qn6qt6gro.c3vzgoez  :http-request     598ms            200
```

Under a timeout the same line reads `outcome: error, durationMs: 10001`, which names the
dependency that hung instead of "the request timed out". A hop that never logs is also a
signal — once the downstream services log their inbound calls, absence tells you where the
request stopped.

One known gap: a handler that **throws** (e.g. the 10 s actor timeout) produces Jetty's 500
page without the header, because the middleware can only add it to a returned response. The
CID is still in the error log line, which is what a support engineer needs; returning a
500 body with the CID is a follow-up.

### How it is carried

The ecosystem already has the pieces:

- `d-core.tracing` — `*ctx*` = `{trace-id span-id parent-span-id}`, encoded as
  `t=<trace-id>;s=<span-id>;p=<parent-span-id>` in `x-correlation-id`.
- gRPC **client** interceptor — propagates `d-core.tracing/*ctx*` as `x-correlation-id`.
- gRPC **server** interceptor — decodes it, creates a child span, binds it in the call
  context and echoes it in response headers.
- `d-core-grpc` tests already prove service A and B share one trace id across gRPC.

**The gap found:** the HTTP layer never bound `d-core.tracing/*ctx*`. `wrap-http-tracing`
binds only the OpenTelemetry context, so on an HTTP request the client interceptor saw
`nil` and called `ensure-ctx`, starting a **new trace per actor call** — the request, its
traces and its downstream calls had no shared id. This is exactly the "losing correlation
between services" risk.

**Fix (implemented):** `wrap-correlation-id` in `d-core-metrics` (next to the HTTP tracing
wrapper) decodes an inbound `x-correlation-id` or creates one, makes this hop a child span,
binds `d-core.tracing/*ctx*` for the whole handler and echoes the id in the response. Wired
around the API handler in `routes.clj`.

Verified live:

```
$ curl -H 'x-correlation-id: t=abc123456789;s=11112222;p=-' .../audit -D -
x-correlation-id: t=abc123456789;s=ioid56om;p=11112222
```

Same trace id, a new span per hop, the parent span preserved; six requests, six child
spans, all 200. The actor hop is covered by the existing gRPC propagation test (A and B
share the trace id). Remaining for L0: bind the decoded context for **actor-side** work so
actor logs carry it too (the server interceptor attaches it to the gRPC context; the
handler must bind it or read it explicitly).

## Architecture

```
containers (stdout/stderr) ──docker API──▶ Alloy ──loki.write──▶ Loki ──▶ Grafana
                                                                          (Explore + dashboards)
```

- **Loki** `grafana/loki:3.5` single binary, filesystem + TSDB, no auth, `127.0.0.1:3100`
  published for debugging only. Volume `loki-data`. Retention **72 h or 5 GB** (whichever
  first; see decisions).
- **Alloy** `grafana/alloy:1.9` as the collector (Promtail is EOL). `discovery.docker`
  filtered by `com.docker.compose.project=election-system`, `loki.source.docker` with
  relabeling, `loki.write` to `http://loki:3100`.
- **Grafana**: a `Loki` datasource (uid `loki`) added to the provisioning files — the
  Grafana image bakes them, so it must be **rebuilt** (the F9 lesson: baked config drifts).

### Labels (cardinality discipline)

| Use as a label | Never a label |
| :-- | :-- |
| `compose_project`, `compose_service`, `container`, `stream` (stdout/stderr), `level` (parsed) | `trace_id`, `run_id`, `path`, `voter_id`, any UUID |

High-cardinality values are **filtered in queries**, not indexed as labels:
`{compose_service="election-api-backend"} |= "Circuit breaker"`.

### Resources and guardrails

- Loki: 0.5 CPU / 512 MB, `restart: unless-stopped`, volume + retention/compaction.
- Alloy: 0.25 CPU / 256 MB.
- `/var/run/docker.sock` mounted **read-only** into Alloy (standard for log collection).
  This is a lab trade-off: the chaos plane deliberately uses a socket proxy, and the same
  proxy could serve log reads later. Documented here so it is a decision, not an accident.
- No authentication. Loopback only; never expose Loki/Alloy beyond 127.0.0.1.

## Isolating the log: slow paths without a CID

To find "where it got slow" before knowing any CID, the request log elevates:

- `:http-request` with `slow: true` when the response takes ≥ `SLOW_REQUEST_MS`
  (default 500 ms);
- `:downstream-call` with `slow: true` when a dependency call takes ≥ `SLOW_CALL_MS`
  (default 300 ms).

Query (no cid needed): `{compose_service="election-api-backend"} | json | slow="true"`.
The lines carry the cid, so a slow request leads to its chain with one more filter:

```
{compose_service="election-api-backend"} | json | slow="true"
  :downstream-call db-audit 4055ms slow=true  cid=…lnzihzac
  :http-request               4162ms 200 slow=true  cid=…lnzihzac
```

### Chaos telemetry

Three signals, because "is chaos on?" and "is it firing?" are different questions:

| Signal | Where | Notes |
| :-- | :-- | :-- |
| `fault-armed` / `fault-cleared` | engine log | id, selector, target count, status |
| `:chaos-engaged` | app log, `:warn` | first hit **per rule per process**, with rule/component/fault and the request's cid |
| `:chaos-engaged-summary` | app log, `:warn` | every 10 s, counts per `component/fault` — a per-hit line would flood (~18k msg/min) |
| `chaos_hits_total` | Prometheus | labels `component`, `fault` (not the rule id: cardinality) |
| `chaos` health component | `/health` | `{enabled?, rules, hits}` on every node |

The counter is the thing to alert on; the engaged line is the thing to **read** (it carries
the cid, so a firing fault leads to the affected request). Announcements are per process:
with two API replicas you will see the first hit twice, once per node. That is correct —
"this node saw chaos fire" is a per-node fact.

## Status

**L1 — built and verified.** Loki 3.4.2 (pinned: 3.5.0 rejected Alloy's pushes with
`negative structured metadata bytes received`), single binary with the config baked into
`election-system-loki`; Alloy 1.9.2 (`election-system-alloy`) reads container stdout/stderr
via the read-only Docker socket and pushes to Loki; Grafana has the `Loki` datasource. 11
Prometheus targets unaffected.

Verified:

```
GET /loki/api/v1/labels
  → compose_project, compose_service, container, stream
{compose_service="election-api"}            → nginx access lines (rt/urt/upstream)
{compose_service="election-api-backend"}    → JSON request lines
  {"service":"election-api","traceId":"cafe12345678","event":":http-request",
   "status":200,"durationMs":621,"uri":"/v1/elections/national-2026/audit", …}
```

A client-supplied `x-correlation-id: t=cafe12345678;…` appears as `traceId` in Loki — the
id survives HTTP → handler → log. Under the soak the same stream shows the real failure:
`status:503` and a `http-request-failed` with `durationMs:10001, error:"timeout"`, next to
the nginx line with `urt=10.002`. That is the "why" this was built for.

**L0 — built (first slice).** `election-system.log/make-logger` is a Duct `Logger` that
writes one JSON line per event with `service`, `instance`, `traceId`/`spanId` and data; the
node creates one and passes it to ownership and the API. `wrap-request-log` (in
`d-core-metrics`) logs every request with status, duration and correlation id — 5xx at
`:error`, 4xx at `:warn`. Remaining L0 events: breaker transitions, vote rejections,
chaos step verdicts.

**L2 — built.** Every request line carries two ids: `traceId` (d-core, the id that survives
HTTP → gRPC → actors) and `otelTraceId` (hex, sampled requests only). The Loki datasource
has a derived field matching `otelTraceId` and linking to the Jaeger datasource; a **Logs**
dashboard (uid `logs`) has a service variable, a logs panel and a log-volume panel; a drill
run's page links to it with the run's time window.

Verified end to end: a log line with `otelTraceId=3f2e7016…` resolves in Jaeger to a trace
with 4 spans across 2 services. Click-through works; the correlation id on the same line
still ties the request across services.

## Phases

### L0 — use the Duct logger (the ecosystem's), not a new one

`org.duct-framework/logger` and `module.logging` are already dependencies, and two call
sites already speak the protocol (`ownership.clj`, `db/writer.clj` accept "a duct logger or
a plain fn"). What is missing is a **logger component**: today the node passes a plain fn
(`ownership-log`) because no logger is instantiated, and the `Logger` protocol is never
used end-to-end.

Plan: construct exactly one logger in the node bootstrap (`duct.logger.simple/logger`, or a
small `Logger` implementation that prints one JSON line per event and adds the current
correlation id), then pass it to ownership, the DB writer, the API handlers, resilience and
chaos. No second logging abstraction.

The logger reads `d-core.tracing/*ctx*` (see "Correlation" below) so every line carries the
same trace id as the request and its traces. Events to add:

- HTTP 5xx with `reason`/`exception` (including `:circuit-breaker/open`);
- breaker transitions open/closed, with the failing dependency;
- vote rejected (`409 duplicate-vote`) and actor transport failures;
- chaos engine: run started, step executed (action, target/container, ok/error), claim
  verdicts, gate waits — enough to reconstruct a drill timeline from logs alone.

### L0 (second half) — make the applications say something
The aggregator is only as useful as the events we emit. A dependency-free JSON-lines
logger (`d-core-std.log` or a tiny namespace per app): `ts`, `level`, `service`,
`instance`, `trace_id`, `span_id`, `event`, `fields`. Replace the 8 `println`s and add the
events that answer "why":

- HTTP 5xx with `reason`/`exception` (including the breaker's `:circuit-breaker/open`);
- breaker transitions open/closed, with the failing dependency;
- vote rejected (`409 duplicate-vote`) and actor transport failures;
- chaos engine: run started, step executed (action, target/container, ok/error), claim
  verdicts, gate waits — enough to reconstruct a drill timeline from logs alone.

Trace ids come from the existing OTel tracer context. That single field is what turns
"logs" into "correlated logs".

### L1 — Loki + Alloy + Grafana datasource
This is the aggregator itself. Delivers immediately: nginx access logs (per-request
status/upstream/timing), JVM stack traces, Postgres errors, chaos-engine output, container
restarts. Verify in Explore and with one dashboard panel.

### L2 — correlation
- Grafana **derived field** `trace_id` → Jaeger (datasource uid `jaeger`): click a log line,
  open the trace.
- A "Logs" row on the election-system dashboard and a per-service panel.
- Cockpit: a link from a drill run to Grafana Explore with the run's time range and
  `{compose_service=~"election-.*"}` — one click from claims to the logs that explain them.

### L3 — alerts and retention tuning
Log-based alerts (breaker-open bursts, OOM, crash loops) and retention/volume sizing based
on real usage.

## Verification checklist (L1)

1. `{compose_service="election-api"}` returns nginx access lines with `upstream` and `rt`.
2. Restart a container; the startup lines appear within seconds.
3. Force a 503 (`db-outage` preset) and find the API's error line by filter.
4. `sum(count_over_time({compose_service="election-api"}[1m]))` ≈ 2.2k under load.
5. Loki volume growth after an hour under load is within the cap.
6. Grafana Explore queries Loki; a dashboard panel renders.

## Decisions to make

1. **Retention**: 72 h (simple, ~2 GB at load) vs 7 d with a 5 GB cap (better for a
   week-long investigation). Recommendation: 7 d + 5 GB cap; compaction makes the real
   usage much smaller than the raw 0.65 GB/day.
2. **When to do L0**: it is the difference between "Loki collects logs" and "Loki answers
   why". Recommendation: L0 + L1 in the same change; L2 after.
