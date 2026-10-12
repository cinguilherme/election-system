.PHONY: tests unit integration benchmark lint clean up down logs load-test stress-test

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

up-build:
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

clean:
	rm -rf target .cpcache .nrepl-port
