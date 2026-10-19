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
