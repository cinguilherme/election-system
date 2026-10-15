.PHONY: tests unit integration benchmark lint clean up up-build up-perf down logs load-test load-test-docker stress-test stress-test-docker stress-test-single rate-test rate-test-docker profile profile-cpu profile-perf profile-alloc profile-wall profile-lock profile-mem threaddump throttling reset-data build-base ensure-base

# Prebuild shared base image containing diagnostic utilities and async-profiler
build-base:
	@echo "Building shared base image d-core-base:latest..."
	docker build -t d-core-base:latest -f Dockerfile.base .

ensure-base:
	@if [ -z "$$(docker images -q d-core-base:latest 2>/dev/null)" ]; then \
		echo "Base image d-core-base:latest not found. Building it now..."; \
		$(MAKE) build-base; \
	fi

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

up:
	docker compose up -d

up-build: ensure-base
	docker compose up --build -d

# Bring the stack up with CAP_SYS_ADMIN so `asprof -e cpu` (perf_events) works.
# Only needed on Docker Desktop, where the VM sets kernel.perf_event_paranoid=4;
# on a normal Linux host CAP_PERFMON in the base compose file is enough.
# ctimer/alloc/wall/lock profiling works without this.
up-perf:
	docker compose -f docker-compose.yaml -f docker-compose.perf.yaml up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

K6_HOST ?= $(shell command -v k6 2>/dev/null)

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
SERVICE ?= async-proxy-ingress
DURATION ?= 30
EVENT ?= ctimer

profile:
	@mkdir -p profiles
	@CID=$$(docker compose ps -q $(SERVICE)); \
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
	@CID=$$(docker compose ps -q $(SERVICE)); \
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

# Fast benchmark data reset
reset-data:
	@echo "Stopping actors and API nodes to flush in-flight state..."
	@docker compose stop election-actors-1 election-actors-2 election-api-1 election-api-2
	@echo "Resetting PostgreSQL tables..."
	@docker compose exec -T postgres psql -U election -d election_db -c "TRUNCATE TABLE ballots, precinct_tallies, election_tallies, actor_snapshots;"
	@echo "Purging NATS JetStream messages..."
	@docker compose exec -T nats nats stream purge -f proxy_votes >/dev/null 2>&1 || true
	@docker compose exec -T nats nats stream purge -f proxy_actor_precinct >/dev/null 2>&1 || true
	@docker compose exec -T nats nats stream purge -f proxy_actor_election >/dev/null 2>&1 || true
	@echo "Starting actors and API nodes with clean state..."
	@docker compose start election-actors-1 election-actors-2 election-api-1 election-api-2
	@echo "Restarting reverse proxy..."
	@docker compose restart election-api
	@echo "Waiting for cluster to become healthy..."
	@until [ "$$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/health 2>/dev/null)" = "200" ]; do sleep 1; done
	@echo "Data reset complete and cluster is ready."

clean:
	rm -rf target .cpcache .nrepl-port profiles

