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

## Phases

### L0 — make the applications say something (prerequisite, small)
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
