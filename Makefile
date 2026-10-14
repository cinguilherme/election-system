.PHONY: tests unit integration benchmark lint clean up down logs load-test stress-test profile profile-cpu profile-mem threaddump reset-data build-base ensure-base

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

down:
	docker compose down -v

logs:
	docker compose logs -f

# Default progressive 5-stage stress staircase (50 -> 150 -> 300 -> 500 VUs over 2m45s)
load-test:
	docker compose run --rm k6

# Extreme breaking-point test (100 -> 300 -> 600 -> 1000 VUs with zero think-time)
stress-test:
	docker compose run --rm \
		-e STAGE_1_VUS=100 -e STAGE_1_DURATION=20s \
		-e STAGE_2_VUS=300 -e STAGE_2_DURATION=45s \
		-e STAGE_3_VUS=600 -e STAGE_3_DURATION=60s \
		-e STAGE_4_VUS=1000 -e STAGE_4_DURATION=60s \
		-e STAGE_5_DURATION=20s \
		-e SLEEP_S=0 \
		k6

# JVM Diagnostics & Flamegraphs using async-profiler
# Usage: make profile SERVICE=async-proxy-ingress DURATION=30 EVENT=itimer
SERVICE ?= async-proxy-ingress
DURATION ?= 30
EVENT ?= itimer

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
	@$(MAKE) profile SERVICE=$(SERVICE) DURATION=$(DURATION) EVENT=itimer

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

# Fast benchmark data reset
reset-data:
	@echo "Resetting PostgreSQL tables..."
	@docker compose exec -T postgres psql -U election -d election_db -c "TRUNCATE TABLE ballots, precinct_tallies, election_tallies, actor_snapshots;"
	@echo "Purging NATS JetStream messages..."
	@docker compose exec -T nats nats stream purge -f proxy_votes >/dev/null 2>&1 || true
	@docker compose exec -T nats nats stream purge -f proxy_actor_precinct >/dev/null 2>&1 || true
	@docker compose exec -T nats nats stream purge -f proxy_actor_election >/dev/null 2>&1 || true
	@echo "Restarting actors, api nodes, and reverse proxy to reset state..."
	@docker compose restart election-actors-1 election-actors-2 election-api-1 election-api-2 election-api
	@echo "Data reset complete."

clean:
	rm -rf target .cpcache .nrepl-port profiles

