# Observability & Profiling

Everything needed to answer "where is the time going?" for the election stack:
Prometheus metrics, Grafana dashboards, Jaeger traces, and async-profiler
flamegraphs from inside the running containers.

---

## 1. What is collected

| Source | Endpoint | Prometheus job | `tier` label |
| :--- | :--- | :--- | :--- |
| election-api-1 / -2 | `:8080/metrics` | `election-api` | `api` |
| election-actors-1 / -2 | `:8081`, `:8082` `/metrics` | `election-actors` | `actors` |
| async-proxy-ingress | `:4000/metrics` | `async-proxy-ingress` | `proxy-ingress` |
| async-proxy-egress | `:4001/metrics` | `async-proxy-egress` | `proxy-egress` |
| **cadvisor** | `:8089/metrics` | `cadvisor` | *(per-container `service`)* |
| Traces (OTLP HTTP) | `jaeger:4318/v1/traces` | — | Jaeger UI `:16686` |

Prometheus scrapes every **2 s**; Grafana auto-provisions every dashboard JSON in
`observability/grafana/provisioning/dashboards/` within 5 s of a file change.

---

## 2. Container resources — why cAdvisor is here

Application metrics alone **cannot** tell you that a container is CPU-starved.
The async-proxy was pinned at its `0.60` CPU quota and throttled in **22–26 % of
CFS periods** while every application-level dashboard looked healthy. Nothing in
the app process knows it was descheduled.

cAdvisor closes that gap. The dashboards to look at: **Container Resources &
Throttling**.

Key series (all carry a `service` label promoted from the compose service name):

| Metric | Meaning |
| :--- | :--- |
| `container_cpu_usage_seconds_total` | cores actually burned (`rate()` it) |
| `container_spec_cpu_quota / container_spec_cpu_period` | the CPU quota, in cores |
| `container_cpu_cfs_throttled_periods_total` / `container_cpu_cfs_periods_total` | **throttling ratio** — the money metric |
| `container_cpu_cfs_throttled_seconds_total` | wall-clock seconds spent throttled |
| `container_pressure_cpu_stalled_seconds_total` | PSI: stall time from *any* CPU cause |
| `container_memory_working_set_bytes` | RSS-ish, what the OOM killer scores |
| `container_spec_memory_limit_bytes` | the memory limit |
| `container_pressure_memory_stalled_seconds_total` | PSI: memory reclaim stalls |
| `container_oom_events_total` | OOM kills — any non-zero value is a bug |

Quick CLI view (no Grafana needed):

```bash
make throttling
```

```
  service                    throttled    cpu / quota (cores)   mem MiB   OOM
  async-proxy-egress              0.1%      0.068 / 0.60        402.3     0
  ...
```

### Gotchas discovered while wiring this up

- **Use `ghcr.io/google/cadvisor`, not `gcr.io/cadvisor/cadvisor`.** The GCR
  mirror is stale (stops at `v0.55.1`). Anything before `v0.60.x` cannot resolve
  container layers when Docker uses the containerd image store (`Storage Driver:
  overlayfs`, the default on Docker Desktop 28). It fails *silently*: you get one
  root-cgroup series and **no throttling metrics at all**, with only
  `Failed to create existing container: ... mount-id` in its logs.
- **`--store_container_labels=false` is required** for
  `--whitelisted_container_labels` to have any effect. With it `true`, every
  compose and image label is exported instead of just the ones you asked for.
- `--docker_only=true` keeps cardinality down by ignoring host cgroups.

---

## 3. Profiling with async-profiler

`asprof` (v3.0) and a JDK (`jcmd`) are baked into every app image via
`Dockerfile.base`. All app containers run with `SYS_PTRACE`, so thread dumps and
profiling work out of the box.

### Choosing the right event

| Event | What it measures | Needs extra caps? | Use it for |
| :--- | :--- | :--- | :--- |
| `ctimer` | CPU time (signal-based) | no | **default** CPU profile |
| `cpu` | CPU time via `perf_events` | **yes** (see below) | highest-fidelity CPU profile |
| `alloc` | allocations | no | garbage-churn / GC hunting |
| `wall` | wall-clock, all threads | no | finding *waits*; read the `GC_active` line |
| `lock` | lock contention | no | contention on monitors/queues |

> **Do not use `itimer` for a CPU profile.** It under-samples worker threads: a
> 30 s `itimer` run on PID 1 returned **112 samples** and made the proxy look
> ~4 % busy, while `docker stats` showed it pinned at its CPU quota. `ctimer`
> returned ~50 samples/s on the same workload. `make profile-cpu` defaults to
> `ctimer`.

### Recipes

```bash
# CPU profile of a service (default event: ctimer)
make profile-cpu   SERVICE=async-proxy-egress DURATION=30

# Allocation profile -- finds the garbage churn that drives GC
make profile-alloc SERVICE=async-proxy-egress DURATION=30

# Wall-clock + GC share
make profile-wall  SERVICE=async-proxy-egress DURATION=30   # read GC_active

# Lock contention
make profile-lock  SERVICE=async-proxy-egress DURATION=30

# Any event, explicit
make profile SERVICE=async-proxy-ingress DURATION=60 EVENT=alloc
```

Output lands in `profiles/<service>-<event>-<timestamp>.html`; open it in a
browser. Direct invocation, if you prefer:

```bash
CID=$(docker compose ps -q async-proxy-egress)
docker exec "$CID" asprof -d 30 -e ctimer -f /tmp/p.html 1
docker cp "$CID":/tmp/p.html ./egress.html
```

Text output is often easier to grep than the HTML:

```bash
docker exec "$CID" asprof -d 30 -e alloc -o flat 1        # top allocators
docker exec "$CID" asprof -d 30 -e alloc -o collapsed 1   # full stacks + counts
```

### `-e cpu` and capabilities

`perf_event_open` is governed by both capabilities and the host kernel.

- The base `docker-compose.yaml` grants **`CAP_PERFMON`**, which is the correct
  and sufficient grant on a standard Linux host.
- **On Docker Desktop (macOS/Windows) that is not enough**: its Linux VM ships
  `kernel.perf_event_paranoid=4`, where `perf_event_open` for CPU events is only
  permitted with `CAP_SYS_ADMIN`. Measured in this environment:

  | Capabilities | Result |
  | :--- | :--- |
  | `CAP_PERFMON` | `EACCES` on every TID |
  | `CAP_PERFMON` + `seccomp=unconfined` | `EACCES` — so it is *not* seccomp |
  | `CAP_SYS_ADMIN` + `CAP_PERFMON` | 0 failures, samples collected |

  `CAP_SYS_ADMIN` is close to root-equivalent, so it is deliberately **not**
  granted by default. Opt in only while profiling:

  ```bash
  make up-perf        # docker compose -f docker-compose.yaml -f docker-compose.perf.yaml up -d
  make profile-perf SERVICE=async-proxy-egress DURATION=30
  docker compose up -d   # back to the restricted default
  ```

  Note the VM also exposes **no hardware PMU** (`/sys/bus/event_source/devices`
  has only `software`, `kprobe`, `uprobe`, `tracepoint`, `breakpoint`), so even
  with the capability you are sampling software CPU-clock events.

**Rule of thumb:** use `ctimer` for day-to-day work — it needs no privileges and
answers the same question. Reach for `-e cpu` on a real Linux host where
`perf_event_paranoid <= 2`.

### Thread dumps

```bash
make threaddump SERVICE=async-proxy-egress     # -> profiles/<service>-threaddump-<ts>.txt
```

Fast triage of a dump:

```bash
grep -c '"' profiles/*threaddump*.txt                 # thread count
grep -oE '^\s+at [a-zA-Z0-9_.$]+' file.txt | sort | uniq -c | sort -rn | head
```

Useful signals: many `grpc-default-executor-*` threads means the shared gRPC
cached pool has grown; many `async-mixed-*` parked in `WAITING` means blocked
core.async stream pumps.

---

## 4. Traces

Jaeger is at <http://localhost:16686>. Services: `election-api-1`,
`election-api-2`, `election-actors`, `async-proxy`.

Proxy-side spans:

| Span | Kind | Meaning |
| :--- | :--- | :--- |
| `proxy.ingress.publish` | server | proxy work to accept a publish |
| `jetstream.publish_async` | producer | enqueue to JetStream |
| `proxy.egress.dispatch_stream` | producer | proxy work to hand a message to a subscriber |
| `proxy.client.publish` | — | root span for the whole chain |

Sampling is controlled by `OTEL_SAMPLING_RATIO` (currently `0.10`). Raise it to
`1.0` for short debugging runs, and remember Jaeger keeps only
`MEMORY_MAX_TRACES=5000`.

---

## 5. Metric bucket sizing

Histograms must have a bucket above the slowest observation, otherwise
`histogram_quantile` clamps at the top bucket and the tail becomes invisible.
This already bit us once: `actor_envelope_transit_lag_seconds` topped out at
`2.5` — exactly where the p95 sat under load — so the metric could not express
the real distribution.

Current ceilings (`d-core-metrics/src/d_core/core/metrics/wrappers.clj`):

| Metric | Ceiling |
| :--- | :--- |
| `actor_envelope_transit_lag_seconds` | 10 s |
| `actor_step_duration_seconds` | 5 s |
| `http_request_duration_seconds` | 5 s |
| `sql_operation_duration_seconds` | 2.5 s |
| `async_proxy_delivery_duration_seconds` | 2.5 s |

When you raise a ceiling, rebuild the emitting service — bucket changes only take
effect after the image is rebuilt, and historical series are not comparable
across the change.

---

## 6. Make targets

```
make throttling                              # container CPU/quota/throttle/mem/OOM
make profile-cpu   SERVICE=<svc> DURATION=30 # CPU flamegraph (ctimer)
make profile-perf  SERVICE=<svc> DURATION=30 # CPU flamegraph (perf_events)
make profile-alloc SERVICE=<svc> DURATION=30 # allocation flamegraph
make profile-wall  SERVICE=<svc> DURATION=30 # wall-clock flamegraph
make profile-lock  SERVICE=<svc> DURATION=30 # lock contention flamegraph
make threaddump    SERVICE=<svc>             # thread dump
make up-perf                                 # bring stack up with SYS_ADMIN
```

`SERVICE` defaults to `async-proxy-ingress`; any compose service name works
(`async-proxy-egress`, `election-actors-1`, `election-api-1`, ...).

---

## 7. Troubleshooting

| Symptom | Cause |
| :--- | :--- |
| `make throttling` prints "No container metrics found" | `cadvisor` not running, or the `cadvisor` target is down — check <http://localhost:9090/targets> |
| cAdvisor up but only one series, no throttling | Old cAdvisor image and/or `--store_container_labels=true` alongside the whitelist. See §2 gotchas. |
| `perf_event_open ... Permission denied` | `kernel.perf_event_paranoid` too high — use `ctimer`, or `make up-perf` on Docker Desktop. |
| Grafana panel is empty after editing a dashboard JSON | Provisioning reloads within 5 s; check the JSON parses and the datasource `uid` is `prometheus`. |
| Traces missing for a window | `OTEL_SAMPLING_RATIO` is 0.10, and Jaeger retains only the last 5000 traces. |
