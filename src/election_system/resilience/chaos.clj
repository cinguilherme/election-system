(ns election-system.resilience.chaos
  "Chaos runtime wiring for the deployable roles.

   Inert unless `CHAOS_ENABLED=1`: when disabled this namespace allocates no
   controller, no journal and installs **no decorator**, so the request path is
   the plain un-instrumented one. That property is what makes it safe to ship
   the wiring in every image.

   When enabled, every decorator routes through one `ChaosController` and one
   `EventJournal` per node, so a fault injected on one boundary is visible in
   the same causal sequence as the operations around it."
  (:require [clojure.string :as str]
            [d-core.chaos.controller :as ctrl]
            [d-core.chaos.instrument :as inst]
            [d-core.chaos.journal :as journal]
            [d-core.core.metrics.tracing :as otel]))

(def ^:private default-max-events 50000)

(def ^:private journal-modes #{:full :meta :off})

(defn trace-id-fn
  "Returns a thunk resolving the current W3C trace id, so every journal event can be
   joined to the span that produced it. Returns nil outside a span."
  []
  (fn []
    (when-let [traceparent (otel/current-traceparent)]
      (second (str/split (str traceparent) #"-")))))

(defn make-runtime
  "Builds the chaos runtime for a node.

   Options:
     :enabled?           - master switch (default false)
     :journal-mode       - :full (payloads), :meta (shape only), :off (inject, record nothing)
     :sample             - fraction of events retained, 0.0-1.0 (default 1.0)
     :max-payload-chars  - payloads above this become an `:omitted-bytes` marker
     :trace-fn           - `(fn [] trace-id)`; defaults to the current OTEL span
     :max-events         - journal ring-buffer size (default 50000)
     :instance-id        - node identity stamped on every journal event
     :rules              - optional pre-armed rules (tests, replay)
     :allowed-components - components this environment may fault (nil = all)
     :max-total-hits     - blast-radius cap; reaching it disables the controller
     :audit-max-entries  - audit ring size"
  [{:keys [enabled? journal-mode max-events instance-id rules
           allowed-components max-total-hits audit-max-entries
           sample max-payload-chars trace-fn]
    :or {enabled? false
         journal-mode :full
         max-events default-max-events}}]
  (let [mode (if (contains? journal-modes journal-mode) journal-mode :full)]
    (if-not enabled?
      {:enabled? false
       :controller nil
       :journal nil
       :journal-mode :off
       :instance-id instance-id}
      {:enabled? true
       :controller (ctrl/make-controller
                    (cond-> {:rules (vec (or rules []))}
                      allowed-components (assoc :allowed-components allowed-components)
                      max-total-hits (assoc :max-total-hits max-total-hits)
                      audit-max-entries (assoc :audit-max-entries audit-max-entries)))
       :journal (when (#{:full :meta} mode)
                  (journal/make-journal
                   (cond-> {:mode mode
                            :max-events (long (or max-events default-max-events))}
                     sample (assoc :sample (double sample))
                     max-payload-chars (assoc :max-payload-chars (long max-payload-chars))
                     true (assoc :trace-fn (or trace-fn (trace-id-fn))))))
       :journal-mode mode
       :instance-id instance-id})))

(defn describe
  "Human-readable summary of a runtime, for boot logs."
  [runtime]
  {:enabled? (boolean (:enabled? runtime))
   :journal-mode (:journal-mode runtime)
   :journal? (some? (:journal runtime))
   :instance-id (:instance-id runtime)})

(defn default-instance-id
  "Best-effort stable node identity: `HOSTNAME` (always set by Docker and equal to
   the container id), then the JVM hostname, then a random suffix so two nodes
   never end up sharing a journal label."
  []
  (or (not-empty (str (System/getenv "HOSTNAME")))
      (try (not-empty (.getHostName (java.net.InetAddress/getLocalHost)))
           (catch Exception _ nil))
      (str "node-" (subs (str (random-uuid)) 0 8))))

(defn- component-id
  [runtime component]
  (str (or (:instance-id runtime) "node") "/" (name component)))

(defn unwrap
  "Returns the underlying component if `x` is an instrumented decorator."
  [x]
  (inst/unwrap x))

;; ============================================================================
;; Decorators — each one is a no-op when chaos is disabled
;; ============================================================================

(defn instrument-db
  [runtime db]
  (if (:enabled? runtime)
    (inst/instrument-sql db (:controller runtime) (:journal runtime)
                         {:instance-id (component-id runtime :db)})
    db))

(defn instrument-transport
  ([runtime transport]
   (instrument-transport runtime transport "actor-transport"))
  ([runtime transport label]
   (if (:enabled? runtime)
     (inst/instrument-actor-transport transport (:controller runtime) (:journal runtime)
                                      {:instance-id (str (or (:instance-id runtime) "node") "/" label)})
     transport)))

(defn instrument-client
  [runtime client]
  (if (:enabled? runtime)
    (inst/instrument-actor-client client (:controller runtime) (:journal runtime)
                                  {:instance-id (component-id runtime :actor-client)})
    client))

(defn instrument-snapshot-store
  [runtime store]
  (if (:enabled? runtime)
    (inst/instrument-snapshot-store store (:controller runtime) (:journal runtime)
                                    {:instance-id (component-id runtime :snapshot)})
    store))

(defn instrument-effect-handler
  [runtime effect-handler]
  (if (:enabled? runtime)
    (inst/instrument-effect-handler effect-handler (:controller runtime) (:journal runtime)
                                    {:instance-id (component-id runtime :actor-effect)})
    effect-handler))

(defn instrument-lease
  [runtime lease]
  (if (and (:enabled? runtime) lease)
    (inst/instrument-leader-election lease (:controller runtime) (:journal runtime)
                                     {:instance-id (component-id runtime :lease)})
    lease))

(defn instrument-producer
  "Provided for completeness: election-system itself has no `Producer` component
   (the proxy service owns one), so nothing calls this yet."
  [runtime producer]
  (if (and (:enabled? runtime) producer)
    (inst/instrument-producer producer (:controller runtime) (:journal runtime)
                              {:instance-id (component-id runtime :producer)})
    producer))

(defn instrument-breaker
  "Wraps a circuit breaker so its state transitions land in the journal.
   Returns `breaker` unchanged when chaos is disabled."
  [runtime breaker breaker-name]
  (if (and (:enabled? runtime) breaker)
    (inst/instrument-circuit-breaker breaker breaker-name (:controller runtime) (:journal runtime)
                                     {:instance-id (component-id runtime :circuit-breaker)})
    breaker))
