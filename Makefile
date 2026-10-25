.PHONY: tests unit integration benchmark lint clean up up-build up-perf chaos-up chaos-down chaos-status chaos-targets chaos-faults chaos-experiments chaos-kill chaos-scenarios chaos-run chaos-runs chaos-load down logs load-soak load-test load-test-docker stress-test stress-test-docker stress-test-single rate-test rate-test-docker profile profile-cpu profile-perf profile-alloc profile-wall profile-lock profile-mem threaddump throttling purge-streams reset-data build-base ensure-base

# Prebuild shared base image containing diagnostic utilities and async-profiler
build-base:
	@echo "Building shared base image d-core-base:latest..."
	docker build -t d-core-base:latest -f Dockerfile.base .

PUBLIC_IMAGES = nats:2.10-alpine valkey/valkey:8-alpine postgres:16-alpine \
                jaegertracing/all-in-one:1.60 ghcr.io/google/cadvisor:v0.60.6

# The base image and any pre-cached public images must match $(PLATFORM):
# an arm64 base silently produces arm64 children, and a locally cached arm64
# public image causes `docker compose up` to fail with "image with reference ...
# was found but does not provide the specified platform (linux/amd64)".
ensure-base:
	@arch=$$(docker image inspect d-core-base:latest --format '{{.Os}}/{{.Architecture}}' 2>/dev/null || echo none); \
	if [ "$$arch" != "$(PLATFORM)" ]; then \
	  echo "Base image is $$arch, need $(PLATFORM). Rebuilding..."; \
	  $(MAKE) build-base; \
	fi
	@for img in $(PUBLIC_IMAGES); do \
	  arch=$$(docker image inspect "$$img" --format '{{.Os}}/{{.Architecture}}' 2>/dev/null || echo none); \
	  if [ "$$arch" != "none" ] && [ "$$arch" != "$(PLATFORM)" ]; then \
	    echo "Image $$img is cached as $$arch, pulling $(PLATFORM)..."; \
	    docker pull --platform "$(PLATFORM)" "$$img"; \
	  fi; \
	done

## Tiered tests (item 10): a flow must pass in-process before it is trusted on a
## stack, and chaos runs against a stack that is already up.
test-flow:
	clojure -M:tier --tier flow

test-stack:
	ELECTION_STACK=1 clojure -M:tier --tier stack

test-chaos:
	ELECTION_STACK=1 clojure -M:tier --tier chaos

## Release: validate every module's .version and show the bump plan.
release-check:
	clojure -Sdeps '{:paths ["scripts"]}' -M -m release --root .. --check

release-plan:
	clojure -Sdeps '{:paths ["scripts"]}' -M -m release --root .. --bump $(BUMP)

tests:
	clojure -M:test

unit:
	clojure -M:test -d test/election_system/unit

integration:
	clojure -M:test -d test/election_system/integration

benchmark:
	clojure -M:test -n election-system.integration.scalability-benchmark-test

lint:
	clojure -M:lint --lint src test

# The compose files are split by concern:
#   docker-compose.yaml                broker, lease store, DB, proxies, actors, API
#   docker-compose.observability.yaml  tracing, metrics, dashboards, container stats
#   docker-compose.load.yaml           k6 generators (run from another machine)
#   docker-compose.perf.yaml           extra capabilities for profiling
#   docker-compose.chaos.yaml          the internal /_chaos admin plane (opt-in)
#   docker-compose.registry.yaml       the image registry
up: ensure-base
	docker compose -f docker-compose.yaml -f docker-compose.observability.yaml up -d

up-build: ensure-base
	docker compose -f docker-compose.yaml -f docker-compose.observability.yaml up --build -d

# Bring the stack up with CAP_SYS_ADMIN so `asprof -e cpu` (perf_events) works.
# Only needed on Docker Desktop, where the VM sets kernel.perf_event_paranoid=4;
# on a normal Linux host CAP_PERFMON in the base compose file is enough.
# ctimer/alloc/wall/lock profiling works without this.
up-perf:
	docker compose -f docker-compose.yaml -f docker-compose.perf.yaml up -d

# Chaos overlay: the full stack plus the internal /_chaos admin plane on :8082.
# Fail-closed on CHAOS_TOKEN; the port is never published to the host.
chaos-up:
	@test -n "$(CHAOS_TOKEN)" || { echo "CHAOS_TOKEN is required:"; echo "  export CHAOS_TOKEN=\$$(openssl rand -hex 16)"; exit 1; }
	docker compose -f docker-compose.yaml -f docker-compose.observability.yaml -f docker-compose.chaos.yaml up -d

chaos-down:
	docker compose -f docker-compose.yaml -f docker-compose.observability.yaml -f docker-compose.chaos.yaml down

# Asks every chaos-enabled replica for its active rules and journal size.
# Bypasses the engine, so it still works when the engine itself is the problem.
chaos-status:
	@for c in $$(docker ps --format '{{.Names}}' | grep -E '^election-system-(election-actors|election-api-backend)-[0-9]+$$'); do \
		printf '== %s\n   ' "$$c"; \
		docker exec "$$c" sh -c 'curl -fsS -H "X-Chaos-Token: $$CHAOS_ADMIN_TOKEN" http://127.0.0.1:8082/_chaos/status' \
			|| echo "(no admin plane -- CHAOS_ENABLED=1?)"; \
		echo; \
	done

# Operator-facing surface: the chaos-engine on :8088, which addresses replicas for you.
CHAOS_ENGINE_URL ?= http://127.0.0.1:8088
CHAOS_CURL = curl -fsS -H "X-Chaos-Token: $(CHAOS_TOKEN)" -H 'content-type: application/json'

chaos-targets:
	@$(CHAOS_CURL) "$(CHAOS_ENGINE_URL)/api/targets?refresh=true" | python3 -m json.tool

chaos-faults:
	@$(CHAOS_CURL) "$(CHAOS_ENGINE_URL)/api/faults" | python3 -m json.tool

chaos-experiments:
	@$(CHAOS_CURL) "$(CHAOS_ENGINE_URL)/api/experiments" | python3 -m json.tool

# Emergency stop: clears and disables injection on every target.
# Scenario runs: the engine holds the scenario, the script polls it and exits non-zero
# when a check fails. `make chaos-run SCENARIO=db-ack-loss`.
chaos-scenarios:
	@$(CHAOS_CURL) "$(CHAOS_ENGINE_URL)/api/scenarios" | python3 -m json.tool

chaos-runs:
	@$(CHAOS_CURL) "$(CHAOS_ENGINE_URL)/api/runs" | python3 -m json.tool

chaos-run:
	@test -n "$(SCENARIO)" || { echo "usage: make chaos-run SCENARIO=<name>"; exit 1; }
	@python3 scripts/chaos_run.py "$(SCENARIO)"

# Same, with load. The experiment id is generated here and shared with k6 (EXP_ID),
# so the run's timeline correlates the traffic it caused with the faults it armed.
chaos-load:
	@test -n "$(SCENARIO)" || { echo "usage: make chaos-load SCENARIO=<name>"; exit 1; }
	@test -n "$(CHAOS_TOKEN)" || { echo "CHAOS_TOKEN is required"; exit 1; }
	@EXP=$$(python3 -c 'import uuid;print(uuid.uuid4())'); \
	  echo "experiment: $$EXP"; \
	  if command -v k6 >/dev/null 2>&1; then \
	    EXP_ID=$$EXP k6 run observability/k6/election_load.js >/tmp/chaos-k6.log 2>&1 & K6_PID=$$!; \
	  else \
	    docker compose -f docker-compose.yaml -f docker-compose.observability.yaml -f docker-compose.load.yaml --profile load run -d --rm --name chaos-k6 -e EXP_ID=$$EXP k6 >/dev/null; K6_PID=""; \
	  fi; \
	  python3 scripts/chaos_run.py "$(SCENARIO)" --experiment-id "$$EXP"; RC=$$?; \
	  if [ -n "$$K6_PID" ]; then kill $$K6_PID 2>/dev/null || true; else docker stop chaos-k6 >/dev/null 2>&1 || true; fi; \
	  exit $$RC

chaos-kill:
	@$(CHAOS_CURL) -X POST "$(CHAOS_ENGINE_URL)/api/kill-switch" | python3 -m json.tool

down:
	docker compose -f docker-compose.yaml -f docker-compose.observability.yaml down -v

logs:
	docker compose logs -f

K6_HOST ?= $(shell command -v k6 2>/dev/null)
SOAK_DURATION ?= 30m
EXP_ID ?=

# Long, steady load for chaos experiments: 100 constant VUs, no ramps.
# Usage: make load-soak SOAK_DURATION=45m [EXP_ID=exp-123]
load-soak:
	@if [ -n "$(K6_HOST)" ] && [ -z "$(USE_DOCKER)" ]; then \
		echo "Running 100-VU soak against $${API_URL:-http://127.0.0.1:8080} for $(SOAK_DURATION)..."; \
		API_URL=$${API_URL:-http://127.0.0.1:8080} DURATION=$(SOAK_DURATION) EXP_ID=$(EXP_ID) \
			$(K6_HOST) run observability/k6/chaos_soak.js; \
	else \
		$(MAKE) load-soak-docker; \
	fi

load-soak-docker:
	docker compose -f docker-compose.yaml -f docker-compose.observability.yaml -f docker-compose.load.yaml \
		--profile load run --rm -e API_URL=http://election-api -e DURATION=$(SOAK_DURATION) -e EXP_ID=$(EXP_ID) k6 \
		run /scripts/chaos_soak.js

# Default progressive 5-stage stress staircase (50 -> 150 -> 300 -> 500 VUs over 2m45s)
load-test:
	@if [ -n "$(K6_HOST)" ] && [ -z "$(USE_DOCKER)" ]; then \
		echo "Running load-test natively on host using $(K6_HOST) -> http://127.0.0.1:8080..."; \
		API_URL=$${API_URL:-http://127.0.0.1:8080} k6 run observability/k6/election_load.js; \
	else \
		$(MAKE) load-test-docker; \
	fi

load-test-docker:
	docker compose run --rm k6

# Extreme breaking-point concurrency test (100 -> 300 -> 600 -> 1000 VUs with zero think-time)
stress-test:
	@if [ -n "$(K6_HOST)" ] && [ -z "$(USE_DOCKER)" ]; then \
		echo "Running stress-test natively on host using $(K6_HOST) -> http://127.0.0.1:8080..."; \
		STAGE_1_VUS=100 STAGE_1_DURATION=20s \
		STAGE_2_VUS=300 STAGE_2_DURATION=45s \
		STAGE_3_VUS=600 STAGE_3_DURATION=60s \
		STAGE_4_VUS=1000 STAGE_4_DURATION=60s \
		STAGE_5_DURATION=20s \
		SLEEP_S=0 \
		API_URL=$${API_URL:-http://127.0.0.1:8080} \
		k6 run observability/k6/election_load.js; \
	else \
		$(MAKE) stress-test-docker; \
	fi

# Distributed multi-instance Docker stress-test (split across k6-1 and k6-2)
stress-test-docker:
	@docker compose rm -f k6-1 k6-2 2>/dev/null || true
	STAGE_1_VUS=100 STAGE_1_DURATION=20s \
	STAGE_2_VUS=300 STAGE_2_DURATION=45s \
	STAGE_3_VUS=600 STAGE_3_DURATION=60s \
	STAGE_4_VUS=1000 STAGE_4_DURATION=60s \
	STAGE_5_DURATION=20s \
	SLEEP_S=0 \
	docker compose up --force-recreate k6-1 k6-2

# Single-instance Docker stress-test for baseline comparison
stress-test-single:
	docker compose run --rm \
		-e STAGE_1_VUS=100 -e STAGE_1_DURATION=20s \
		-e STAGE_2_VUS=300 -e STAGE_2_DURATION=45s \
		-e STAGE_3_VUS=600 -e STAGE_3_DURATION=60s \
		-e STAGE_4_VUS=1000 -e STAGE_4_DURATION=60s \
		-e STAGE_5_DURATION=20s \
		-e SLEEP_S=0 \
		k6

# Open-model arrival-rate stress test (2,000 -> 5,000 -> 8,000 -> 12,000 req/s)
# Directly forces target request volume regardless of response latency to identify true breaking points
rate-test:
	@if [ -n "$(K6_HOST)" ] && [ -z "$(USE_DOCKER)" ]; then \
		echo "Running open-model rate-test natively on host using $(K6_HOST) -> http://127.0.0.1:8080..."; \
		LOAD_TYPE=rate \
		STAGE_1_RATE=2000 STAGE_1_DURATION=20s \
		STAGE_2_RATE=5000 STAGE_2_DURATION=40s \
		STAGE_3_RATE=8000 STAGE_3_DURATION=60s \
		STAGE_4_RATE=12000 STAGE_4_DURATION=60s \
		STAGE_5_DURATION=15s \
		MAX_VUS=3500 \
		API_URL=$${API_URL:-http://127.0.0.1:8080} \
		k6 run observability/k6/election_load.js; \
	else \
		$(MAKE) rate-test-docker; \
	fi

rate-test-docker:
	docker compose run --rm \
		-e LOAD_TYPE=rate \
		-e STAGE_1_RATE=2000 -e STAGE_1_DURATION=20s \
		-e STAGE_2_RATE=5000 -e STAGE_2_DURATION=40s \
		-e STAGE_3_RATE=8000 -e STAGE_3_DURATION=60s \
		-e STAGE_4_RATE=12000 -e STAGE_4_DURATION=60s \
		-e STAGE_5_DURATION=15s \
		-e MAX_VUS=2500 \
		k6

# JVM Diagnostics & Flamegraphs using async-profiler
# Usage: make profile SERVICE=async-proxy-ingress DURATION=30 EVENT=ctimer
#
# Event selection matters -- the wrong event gives actively misleading results:
#   ctimer  CPU time, signal-based. No perf_events required. DEFAULT.
#   cpu     CPU time via perf_events. Most accurate. Needs CAP_PERFMON
#           (docker-compose adds it); some hosts also need perf_event_paranoid<=1.
#   alloc   Allocation profiling -- use this to find garbage-churn hotspots.
#   wall    Wall-clock across all threads -- read the GC_active line for GC share.
#   lock    Lock contention.
#
# Do NOT use `itimer` for a CPU profile on PID 1: it under-samples worker threads.
# A 30s itimer run returned 112 samples and made the proxy look ~4% busy, while
# `docker stats` showed it pinned at its 0.60 CPU quota and throttled ~25% of CFS
# periods. ctimer is the correct fallback when perf_events is unavailable.
SERVICE ?= async-proxy
DURATION ?= 30
EVENT ?= ctimer

profile:
	@mkdir -p profiles
	@CID=$$(docker compose ps -q $(SERVICE) | head -n 1); \
	if [ -z "$$CID" ]; then \
		echo "Error: Service $(SERVICE) is not running."; exit 1; \
	fi; \
	OUT_FILE="profiles/$(SERVICE)-$(EVENT)-$$(date +%Y%m%d-%H%M%S).html"; \
	echo "Profiling $(SERVICE) ($$CID) for $(DURATION)s (event: $(EVENT))..."; \
	docker exec $$CID /opt/async-profiler/bin/asprof -d $(DURATION) -e $(EVENT) -f /tmp/profile.html 1 && \
	docker cp $$CID:/tmp/profile.html $$OUT_FILE && \
	echo "Flamegraph saved to: $$OUT_FILE" && \
	echo "To view: open $$OUT_FILE"

profile-cpu:
	@$(MAKE) profile SERVICE=$(SERVICE) DURATION=$(DURATION) EVENT=ctimer

# Hardware-sampled CPU profile (requires CAP_PERFMON + host perf_event access).
profile-perf:
	@$(MAKE) profile SERVICE=$(SERVICE) DURATION=$(DURATION) EVENT=cpu

profile-alloc:
	@$(MAKE) profile SERVICE=$(SERVICE) DURATION=$(DURATION) EVENT=alloc

profile-wall:
	@$(MAKE) profile SERVICE=$(SERVICE) DURATION=$(DURATION) EVENT=wall

profile-lock:
	@$(MAKE) profile SERVICE=$(SERVICE) DURATION=$(DURATION) EVENT=lock

# Kept for backwards compatibility; prefer profile-alloc.
profile-mem:
	@$(MAKE) profile SERVICE=$(SERVICE) DURATION=$(DURATION) EVENT=alloc

threaddump:
	@mkdir -p profiles
	@CID=$$(docker compose ps -q $(SERVICE) | head -n 1); \
	if [ -z "$$CID" ]; then \
		echo "Error: Service $(SERVICE) is not running."; exit 1; \
	fi; \
	OUT_FILE="profiles/$(SERVICE)-threaddump-$$(date +%Y%m%d-%H%M%S).txt"; \
	echo "Capturing thread dump for $(SERVICE) ($$CID)..."; \
	docker exec $$CID jcmd 1 Thread.print > $$OUT_FILE && \
	echo "Thread dump saved to: $$OUT_FILE"

# Container CPU throttling / quota / memory / OOM for every service (needs cadvisor).
# This is the check that catches "every application dashboard looks healthy but the
# container is being throttled" -- the blind spot that hid the async-proxy bottleneck.
# Grafana equivalent: "Container Resources & Throttling" dashboard.
throttling:
	@./observability/scripts/container-resources.sh

# Purge every proxy_* JetStream stream.
#
# Two bugs are fixed here. The old target ran `nats stream purge` inside the
# `nats` container, which has no nats CLI at all -- every call failed into
# `|| true`, invisibly. And the names it tried (proxy_votes,
# proxy_actor_election) do not match the streams actually in use, which are
# partitioned (proxy_actor_precinct_p0..p7, proxy_actor_election-tally_p0..p7).
# Between them, the backlog silently grew to 12.6 GB / 13.9M messages.
NATS_INTERNAL ?= nats://nats:4222
COMPOSE_NETWORK ?= election-system_default

purge-streams:
	@echo "Purging all proxy_* JetStream streams on $(NATS_INTERNAL)..."
	@docker run --rm --network $(COMPOSE_NETWORK) natsio/nats-box:latest sh -c '\
		nats -s $(NATS_INTERNAL) stream ls -n | grep "^proxy_" | while read s; do \
			printf "  purging %-34s" "$$s"; \
			if nats -s $(NATS_INTERNAL) stream purge -f "$$s" >/dev/null 2>&1; then echo " ok"; else echo " FAILED"; fi; \
		done'
	@docker run --rm --network $(COMPOSE_NETWORK) natsio/nats-box:latest \
		nats -s $(NATS_INTERNAL) stream report 2>/dev/null | tail -4 || true

# Fast benchmark data reset
reset-data:
	@echo "Stopping actors and API nodes to flush in-flight state..."
	@docker compose stop election-actors election-api-backend
	@echo "Resetting PostgreSQL tables..."
	@docker compose exec -T postgres psql -U election -d election_db -c "TRUNCATE TABLE ballots, precinct_tallies, election_tallies, actor_snapshots;"
	@$(MAKE) purge-streams
	@echo "Starting actors and API nodes with clean state..."
	@docker compose start election-actors election-api-backend
	@echo "Restarting reverse proxy..."
	@docker compose restart election-api
	@echo "Waiting for cluster to become healthy..."
	@until [ "$$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/health 2>/dev/null)" = "200" ]; do sleep 1; done
	@echo "Data reset complete and cluster is ready."

clean:
	rm -rf target .cpcache .nrepl-port profiles


# ---------------------------------------------------------------------------
# Lab: image registry so worker machines pull instead of build
# ---------------------------------------------------------------------------
# .env holds REGISTRY (the LAN address of whichever machine serves the registry)
# and optionally TAG. Read it here as well as in compose, so `make` and
# `docker compose` can never disagree about the image names.
-include .env
export

# Default to native host architecture (linux/arm64 on Apple Silicon, linux/amd64
# on x86_64) for local `make up` / `make up-build` so Colima does not run 9 JVM
# containers under QEMU x86_64 emulation (which takes ~150s to boot Clojure and
# crashes amd64 cAdvisor on ARM64 /proc/cpuinfo).
# When building images for the Windows lab worker (`make images`), target
# WORKER_PLATFORM (linux/amd64).
HOST_ARCH := $(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
PLATFORM ?= linux/$(HOST_ARCH)
WORKER_PLATFORM ?= linux/amd64
PLATFORMS ?= linux/amd64,linux/arm64
export DOCKER_DEFAULT_PLATFORM = $(PLATFORM)

COMPOSE = docker compose -f docker-compose.yaml -f docker-compose.observability.yaml
# Images are built and pushed from a compose set that includes the load file too:
# k6 is a service in docker-compose.load.yaml, and `docker compose build k6`
# against the core+observability set fails with "no such service: k6".
COMPOSE_IMAGES = docker compose -f docker-compose.yaml -f docker-compose.observability.yaml -f docker-compose.load.yaml
REGISTRY ?= 192.168.15.29:5001
KEEP ?= 10
# Tag by commit so every image is traceable back to the source that produced it.
LAB_TAG ?= $(shell git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d-%H%M%S)
export REGISTRY
export TAG = $(LAB_TAG)

.PHONY: registry images push pull prune tags
registry:
	docker compose -f docker-compose.registry.yaml up -d

# Only the services we build. `docker compose push` with no arguments pushes
# EVERY service that has an `image:`, which includes nginx, nats, postgres,
# jaeger, prometheus, grafana and cadvisor -- public images that must never be
# pushed (and that fail anyway, since we cannot write to their registries).
# ONE image per build, shared by every container that runs it. Two services own
# a build and the rest just reference the image:
#   async-proxy      builds election-system-async-proxy
#   election-actors  builds election-system            (ROLE selects api/actors)
# The four config services are also images now: a worker machine needs nothing
# but the compose files and the registry -- no observability/ tree on disk.
BUILT  = async-proxy election-actors election-api async-proxy-gateway \
         prometheus grafana k6
IMAGES = election-system-async-proxy election-system election-system-api-lb \
         election-system-proxy-gateway election-system-prometheus \
         election-system-grafana election-system-k6

images:
	$(MAKE) PLATFORM=$(WORKER_PLATFORM) ensure-base
	DOCKER_DEFAULT_PLATFORM=$(WORKER_PLATFORM) $(COMPOSE_IMAGES) build $(BUILT)
	@printf '\ntagged $(REGISTRY)/<image>:$(TAG) for: $(IMAGES) ($(WORKER_PLATFORM))\n'

# Re-point images built under an older registry prefix (e.g. localhost:5001) at
# the current one. The registry host is part of the image NAME, so changing the
# address in .env orphans every image built before the change -- retagging is
# local metadata only, no rebuild.
retag:
	@for i in $(IMAGES); do \
	  src=$$(docker images --format '{{.Repository}}:{{.Tag}}' | grep -E "/$$i:$(TAG)$$" | grep -v "^$(REGISTRY)/" | head -1); \
	  if [ -n "$$src" ]; then \
	    docker tag "$$src" "$(REGISTRY)/$$i:$(TAG)" && echo "  $$src -> $(REGISTRY)/$$i:$(TAG)"; \
	  fi; \
	done

push:
	@for i in $(IMAGES); do \
	  docker image inspect "$(REGISTRY)/$$i:$(TAG)" >/dev/null 2>&1 || { \
	    echo "missing $(REGISTRY)/$$i:$(TAG) -- run 'make images' first"; exit 1; }; \
	done
	$(COMPOSE_IMAGES) push $(BUILT)
	@# Also move `latest` to this build. Without a moving tag, a worker that does
	@# not know the commit asks for the default tag and gets "manifest unknown" --
	@# which looks like a registry/network failure but is just a missing tag.
	@for i in $(IMAGES); do \
	  docker tag "$(REGISTRY)/$$i:$(TAG)" "$(REGISTRY)/$$i:latest"; \
	  docker push -q "$(REGISTRY)/$$i:latest" >/dev/null; \
	done
	@printf 'pushed %s and latest for %d image(s)\n' "$(TAG)" $(words $(IMAGES))

# On a worker: pull only what we built, then start. Public base images are pulled
# by `up` itself.
pull:
	$(COMPOSE_IMAGES) pull $(BUILT)

prune:
	KEEP=$(KEEP) ./scripts/registry-prune.sh --apply

tags:
	./scripts/registry-prune.sh

# Prove the registry is reachable from this machine before blaming the build.
registry-check:
	@printf 'registry %s: ' "$(REGISTRY)"; \
	  curl -sS -o /dev/null -w 'HTTP %{http_code}\n' "http://$(REGISTRY)/v2/" || \
	  echo "unreachable -- check the IP, the firewall, and that the registry is running"
	@curl -fsS "http://$(REGISTRY)/v2/_catalog" 2>/dev/null | head -c 300; echo

# Publish images with buildx, without attestations.
#
# Two reasons this is the publish path rather than `compose build && push`:
#   1. a multi-platform result cannot be loaded into the local daemon, so it has
#      to push anyway;
#   2. compose's `build.provenance: false` does not reach buildx here, and the
#      attestation it adds shows up as an `unknown/unknown` entry in the index --
#      which containerd 2.x can choke on over an insecure HTTP registry
#      ("short read: expected 32bytes but got 0: unexpected EOF").
# Use PLATFORMS=linux/amd64 for a single-arch push (default: amd64 + arm64).
images-multi: ensure-base
	@for spec in \
	  "election-system/docker/nginx/Dockerfile election-system-api-lb" \
	  "election-system/observability/nginx/Dockerfile election-system-proxy-gateway" \
	  "election-system/observability/prometheus/Dockerfile election-system-prometheus" \
	  "election-system/observability/grafana/Dockerfile election-system-grafana" \
	  "election-system/observability/k6/Dockerfile election-system-k6" \
	  "d-core-async-proxy/Dockerfile election-system-async-proxy" \
	  "election-system/Dockerfile election-system"; do \
	  df=$${spec%% *}; name=$${spec##* }; \
	  echo "buildx $$name for $(PLATFORMS)"; \
	  ( cd .. && docker buildx build --platform $(PLATFORMS) --provenance=false --sbom=false -f $$df \
	    --build-arg BASE_IMAGE=d-core-base:latest \
	    -t "$(REGISTRY)/$$name:$(TAG)" -t "$(REGISTRY)/$$name:latest" \
	    --push . ) || exit 1; \
	done
	@echo "pushed multi-arch $(TAG) and latest"

# Generate the thing you copy to a worker: the compose files with every `build:`
# removed, so the machine only pulls. Leaving build contexts in place makes
# compose resolve repo paths that do not exist there, which on Windows surfaces
# as "GetFileAttributeEx: The system cannot find the file specified".
worker-bundle:
	./scripts/make-worker-bundle.sh dist/worker
	@printf '\ncopy dist/worker/ to the worker, set REGISTRY in .env, then:\n'
	@printf '  docker compose -f docker-compose.yaml -f docker-compose.observability.yaml pull\n'
	@printf '  docker compose -f docker-compose.yaml -f docker-compose.observability.yaml up -d\n'

# Print exactly what a worker machine needs.
worker-env:
	@printf 'REGISTRY=%s\nTAG=%s\n\n' "$(REGISTRY)" "$(LAB_TAG)"
	@printf 'On the worker:\n'
	@printf '  REGISTRY=%s TAG=%s make pull up\n' "$(REGISTRY)" "$(LAB_TAG)"
	@printf '  API base URL: http://<this-host>:8080\n'
	@printf '  Grafana: http://<this-host>:3000   Jaeger: http://<this-host>:16686\n'
