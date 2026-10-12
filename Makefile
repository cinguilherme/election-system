.PHONY: tests unit integration benchmark lint clean up down logs load-test

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
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f

load-test:
	docker compose run --rm k6

clean:
	rm -rf target .cpcache .nrepl-port
