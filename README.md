# Election System

Distributed election tallying benchmark built on `d-core-actor-system` and `d-core-async-proxy` (NATS JetStream + Valkey partition leases + PostgreSQL).

> 📐 **Architecture & DB Write Compression**: See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the visual architecture layout, layer-by-layer breakdown, and how the actor system achieved **over 20x DB write compression** during load testing.

---

## 1. Running Locally (Docker Compose)

By default, the stack runs **3 replicas** each of `async-proxy`, `election-actors`, and `election-api-backend` behind Nginx load balancers (`async-proxy-gateway` for gRPC/TCP and `election-api` on `:8080` for HTTP).

```bash
# Build base + service images and start stack + observability (3x3x3 default)
make up-build

# Override replica counts inline or in .env
PROXY_REPLICAS=3 ACTORS_REPLICAS=3 API_REPLICAS=3 make up-build

# Reset NATS streams, Valkey leases, and PostgreSQL state between runs
make reset-data

# Tear down stack and volumes
make down
```

### Local Endpoints
* **Election API (Nginx LB):** <http://localhost:8080> (`/health`)
* **Grafana:** <http://localhost:3000> (`admin` / `admin`)
* **Prometheus:** <http://localhost:9090>
* **Jaeger UI:** <http://localhost:16686>
* **cAdvisor:** <http://localhost:8089>

For profiling (`async-profiler`), cAdvisor throttling queries, and metric/trace details, see [observability/README.md](observability/README.md).

### Chaos / Fault Injection (opt-in)

The resiliency layer (`d-core-chaos`, `d-core-health`, `d-core-circuit-breaker`) ships in every
image but stays inert. It is enabled only by the `docker-compose.chaos.yaml` overlay:

```bash
export CHAOS_TOKEN=$(openssl rand -hex 16)
make chaos-up          # stack + observability + admin planes + chaos-engine
make chaos-targets     # what the engine can reach, per replica
make chaos-faults      # what is armed right now
make chaos-kill        # clear and disable everywhere
make chaos-down
```

Every API and actors replica then exposes an **internal** admin plane on `:8082`
(`/_chaos/status`, `/_chaos/rules`, `/_chaos/audit`, plus `/live`, `/ready`, `/health` with real
dependency probes). The port is not published to the host and is not routed by the API load
balancer; the `chaos-engine` on `:8088` is the only thing that talks to it.

```bash
# Lose lease renewals on exactly replica 2 of the actors tier, for 30 s.
curl -s -X POST localhost:8088/api/faults -H "X-Chaos-Token: $CHAOS_TOKEN" \
  -H 'content-type: application/json' -d '{
    "selector": {"service": "election-actors", "replica": 2},
    "rule": {"id": "lease-blackhole", "component": "lease", "op": "renew!",
             "for": "30s", "max-hits": 3,
             "fault": {"type": "return", "return": {"status": "lost"}}}
  }'

# Revert it everywhere.
curl -s -X DELETE localhost:8088/api/faults/lease-blackhole -H "X-Chaos-Token: $CHAOS_TOKEN"
```

`make chaos-status` still bypasses the engine and asks every container directly — useful when the
engine is the thing that is broken:

```bash
docker compose exec election-system-election-actors-1 sh -c \
  'curl -s -H "X-Chaos-Token: $CHAOS_ADMIN_TOKEN" http://127.0.0.1:8082/_chaos/status'
```

Each node keeps a flight recorder (`CHAOS_JOURNAL_MODE=full|meta|off`, `CHAOS_SAMPLE`) and
attributes its events to an experiment. Send `X-Experiment-Id` with the traffic you want to
correlate and the API binds it for the request; the actor client copies it into the envelope
context, so every step it touches records it. Ask the engine for the merged view:

```bash
curl -s "localhost:8088/api/experiments/$EXP_ID/journal" -H "X-Chaos-Token: $CHAOS_TOKEN" \
  | jq '{instances, "violations": .["sequence-violations"], "skew": .["clock-skew-ms"]}'
```

The reply is one timeline ordered by `(timestamp, node, seq)` with per-replica spans, clock
skew and unreachable replicas named.

### Test tiers

The state-flow suite is tiered: `make test-flow` runs the in-process clusters, `make test-stack`
the same flows against `docker-compose.yaml` + `docker-compose.chaos.yaml` (needs
`ELECTION_STACK=1` and a running stack), and `make test-chaos` the chaos scenarios. A flow body
is shared between tiers — only the world changes — and probabilistic faults carry a `:seed` that
is recorded in the run report, so a failing experiment replays.

### Health and lifecycle

Every tier answers three different questions:

| Endpoint | Question | Fails when |
| :-- | :-- | :-- |
| `/live` | should I be restarted? | a `:liveness` check fails |
| `/ready` | should I receive traffic? | the node is `:starting`/`:draining`, or a `:readiness` check is `:down` |
| `/health` | what exactly is wrong? | — (diagnostic; `?verbose=true` for cache age and failure streaks) |

The actors tier now serves these on its metrics port (`:8081`) and has real probes — a
`SELECT ballot_id FROM ballots LIMIT 1`, a TCP connect to the async-proxy, a lease-store read, and
"do I hold partitions". The compose healthcheck uses `/ready` for both tiers instead of a TCP
connect, so a container that cannot reach its database is unhealthy rather than merely listening.

Evaluation is cached (1 s) and bounded: checks run in parallel on a small pool under one
deadline, so a hanging database makes `/ready` answer `:unknown` in about the deadline instead of
hanging the probe. A check reporting `:down` must fail twice in a row before readiness flips —
one blip does not move traffic. On shutdown the node switches to `:draining` first, so it leaves
rotation while it is still answering.

### Circuit breakers and the shared signal

The API's db and transport guards come from one policy map (`d-core-resilience`): timeout,
breaker, and — when the policy asks — bulkhead, retry and fallback, in a fixed order. Tripping is
either consecutive (`:failure-threshold`) or rate-based (`:failure-rate` + `:min-calls`) over a
rolling window, and the cooldown is jittered so replicas do not all probe in the same
millisecond.

Breakers are per-process, so by default three API replicas hold three independent circuits. Set
`VALKEY_URI` on the API and a replica that trips publishes a shared open signal (`cb:<name>:open`,
TTL = cooldown): the other replicas fail fast on their next call instead of each rediscovering
the outage, and the flag expires by itself if the replica that set it dies.

### Scenario runs (item 5)

A scenario is a hypothesis with a baseline, a fault timeline and invariants, stored as EDN in
[`scenarios/`](scenarios) and mounted into the engine:

```bash
make chaos-scenarios                    # what the engine found on disk
make chaos-run SCENARIO=db-ack-loss     # against a stack that already has traffic
make chaos-load SCENARIO=db-ack-loss    # starts k6 too, sharing one experiment id
make chaos-runs                         # history
```

`chaos-run` exits non-zero when the baseline is not steady, when the fault never fired, or when an
invariant breaks; the report (`/api/runs/:id/report`, or the JSON/JUnit files in the engine's
`CHAOS_REPORTS_DIR`) says which check failed and what it saw.

| Scenario | Hypothesis |
| :-- | :-- |
| `db-ack-loss` | a ballot batch commits but the caller sees an error → retries stay idempotent |
| `db-outage` | reads fail → the API breaker trips, then recovers on its own |
| `lease-blackhole` | renewals are lost → partitions move, then ownership is whole again |
| `transport-drop` | precinct→tally tells are dropped → the projection must converge |
| `actor-kill` | SIGKILL one actor replica → survivors take over, ledger survives |
| `postgres-latency` | 500 ms on actors→postgres only → writes slow, data reconciles |

`actor-kill` and `postgres-latency` need the infrastructure half of the overlay: Toxiproxy
(one proxy per dependency edge) and a `docker-socket-proxy` for pause/kill. When the overlay is
in use, the tiers reach their dependencies **through** the proxies (`toxiproxy:15432` for the
API's postgres, `:15433` for the actors', `:15051/:15052` for the async-proxy, `:16379` for
valkey); without a toxic they are a plain TCP relay. The engine never mounts the docker socket —
the proxy exposes a narrow TCP view of it.

```bash
make chaos-load SCENARIO=postgres-latency   # watch SQL latency in Grafana while it runs
make chaos-load SCENARIO=actor-kill         # a replica dies for real, then is restarted
```

With `CHAOS_ENABLED` unset (the default stack) no decorator, journal or admin port exists.
Roadmap and trade-offs: [docs/resilience-roadmap.md](docs/resilience-roadmap.md).

---

## 2. Running on a Remote Windows Worker (Lab Load Testing)

In the two-machine lab setup:
* **Mac (Build & Registry Host):** Builds `linux/amd64` images, serves a local Docker Registry on port `5001`, and generates a stripped compose bundle (`dist/worker/`).
* **Windows Machine (Worker):** Pulls pre-built images from the Mac's LAN registry and runs the stack (or load generators) without needing the monorepo or Clojure toolchain on disk.

### Step 1 — On the Mac: Start Registry, Build & Push Images, Generate Bundle

1. **Keep `.env` pointed at `localhost:5001` on the Mac:**
   ```dotenv
   REGISTRY=localhost:5001
   TAG=latest
   ```
   *(Docker Desktop allows plain HTTP pushes to `localhost` without extra configuration, while still serving the registry on `0.0.0.0:5001` for LAN machines.)*

2. **Start the local Docker registry (`:5001`):**
   ```bash
   make registry
   ```

3. **Build and push `linux/amd64` images to the registry:**
   ```bash
   make images push
   ```
   *(Alternatively, to build and push directly via `buildx` with attestations stripped: `PLATFORMS=linux/amd64 make images-multi`.)*

4. **Regenerate the worker bundle (`dist/worker/`):**
   ```bash
   make worker-bundle
   ```
   This runs `./scripts/make-worker-bundle.sh dist/worker`, which copies `docker-compose.yaml`, `docker-compose.observability.yaml`, and `docker-compose.load.yaml` into `dist/worker/` with **all `build:` blocks removed**.

5. **Verify the registry is reachable on your LAN IP:**
   ```bash
   ipconfig getifaddr en0   # e.g. 192.168.15.29
   REGISTRY=192.168.15.29:5001 make registry-check
   ```

---

### Step 2 — One-Time Setup on the Windows Worker

1. **Allow the Mac's insecure HTTP registry in Docker Desktop (Windows):**
   Open **Docker Desktop → Settings → Docker Engine** and add your Mac's LAN IP under `insecure-registries`:
   ```json
   {
     "insecure-registries": [
       "192.168.15.29:5001"
     ]
   }
   ```
   Click **Apply & restart**.

2. **Copy `election-system/dist/worker/` to the Windows machine** (e.g. `C:\election-worker\`).
   It contains only four files:
   * `docker-compose.yaml`
   * `docker-compose.observability.yaml`
   * `docker-compose.load.yaml`
   * `.env.example`

3. **Create `.env` on the Windows machine** (`copy .env.example .env`) and set the Mac's LAN IP:
   ```dotenv
   REGISTRY=192.168.15.29:5001
   TAG=latest
   PROXY_REPLICAS=3
   ACTORS_REPLICAS=3
   API_REPLICAS=3
   ```

---

### Step 3 — Start the Stack on the Windows Worker

From PowerShell or terminal inside the `worker` directory on Windows:

```powershell
# Pull pre-built application + config images from the Mac registry
docker compose -f docker-compose.yaml -f docker-compose.observability.yaml pull

# Start the full stack (3x3x3 replicas by default) + observability
docker compose -f docker-compose.yaml -f docker-compose.observability.yaml up -d
```

Verify the stack is healthy:
* **API Health:** `http://<windows-lan-ip>:8080/health`
* **Grafana:** `http://<windows-lan-ip>:3000`
* **Prometheus:** `http://<windows-lan-ip>:9090`
* **Jaeger UI:** `http://<windows-lan-ip>:16686`

---

### Step 4 — Running the Load Tests

#### Option A: Stack on Windows Worker, k6 Driven Remotely from Mac over LAN
Run k6 on the Mac targeting the Windows worker's API load balancer (`:8080`) so load generation does not compete with the containers for CPU:

```bash
# Default progressive 5-stage load test (50 -> 150 -> 300 -> 500 VUs)
API_URL=http://<windows-lan-ip>:8080 make load-test

# Breaking-point stress test (100 -> 300 -> 600 -> 1000 VUs, zero think time)
API_URL=http://<windows-lan-ip>:8080 make stress-test

# Constant-arrival-rate test (2000 -> 5000 -> 8000 -> 12000 req/s)
API_URL=http://<windows-lan-ip>:8080 make rate-test
```

#### Option B: Running Containerized k6 on the Windows Worker (via `docker-compose.load.yaml`)
If running k6 directly inside Docker on the worker (or using the Windows machine as a remote load generator against another host):

```powershell
# 1. Pull the k6 image from the registry
docker compose -f docker-compose.yaml -f docker-compose.load.yaml --profile load pull

# 2a. Run default load test against the local worker stack
docker compose -f docker-compose.yaml -f docker-compose.load.yaml --profile load run --rm k6

# 2b. Run distributed 2-container stress test (k6-1 + k6-2 split 50/50)
$env:STAGE_1_VUS="100"; $env:STAGE_2_VUS="300"; $env:STAGE_3_VUS="600"; $env:STAGE_4_VUS="1000"; $env:SLEEP_S="0"
docker compose -f docker-compose.yaml -f docker-compose.load.yaml --profile load up --force-recreate k6-1 k6-2

# 2c. Or use the Windows machine purely as a load generator targeting the Mac stack:
docker compose -f docker-compose.load.yaml --profile load run --rm --no-deps -e API_URL=http://<mac-lan-ip>:8080 k6
```

---

### Step 5 — Hard-Won Gotchas (Why Things Are Set Up This Way)

| Symptom / Error | Root Cause & Fix |
| :--- | :--- |
| **`GetFileAttributeEx ... The system cannot find the file specified`** on Windows | Running the root `docker-compose.yaml` on Windows makes Compose resolve `../d-core-async-proxy/Dockerfile` build contexts even when pulling. Always use the stripped files in `dist/worker/` generated by `make worker-bundle`. |
| **`short read: expected 32bytes but got 0: unexpected EOF`** on Windows pull | Buildx provenance/SBOM attestations add an `unknown/unknown` entry to the manifest index that containerd 2.x mishandles over plain HTTP registries. All Dockerfiles use `provenance: false` (and `images-multi` passes `--provenance=false --sbom=false`). |
| **`HTTP 403 Forbidden`** when pushing/pulling on port `5000` | macOS ControlCenter (AirPlay Receiver) listens on port `5000`. The registry in `docker-compose.registry.yaml` binds host port **`5001:5000`**. |
| **`no match for platform in manifest`** on Windows worker | An image was built on Apple Silicon without `PLATFORM=linux/amd64`, or `d-core-base:latest` was cached as `linux/arm64`. Always build via `make images` (which exports `DOCKER_DEFAULT_PLATFORM=linux/amd64` and runs `ensure-base`). |
| **`image with reference ... was found but does not provide the specified platform (linux/amd64)`** on Mac | A public image (`jaeger`, `cadvisor`, `nats`, `postgres`, `valkey`) was previously pulled as `linux/arm64` outside `make`. `make up` / `make up-build` now runs `ensure-base` to auto-pull `--platform $(PLATFORM)` when a cached tag mismatches. |
| **Registry disk usage grows unboundedly** | Run `make tags` to inspect stored tags and `make prune` (`KEEP=10`) to delete old manifests and garbage-collect registry blobs. |
