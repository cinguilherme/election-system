BUMP ?= patch

lint:
	clojure -M:lint --lint src

tests:
	clojure -M:test

build:
	clojure -T:build jar

publish:
	clojure -T:build publish :bump :$(BUMP)

publish-patch:
	$(MAKE) publish BUMP=patch
