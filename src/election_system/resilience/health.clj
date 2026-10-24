(ns election-system.resilience.health
  "Health checks for the actors tier.

   The API tier's readiness is derived from circuit-breaker state
   (`start-api-node`), which answers \"did my breaker trip?\" rather than
   \"can I reach my dependency?\". The actors tier owns partitions and the write
   path, so it gets real probes: a database round-trip, a broker socket, a lease
   store operation, and the partition count this node actually holds.

   Probes deliberately use the **raw** (un-instrumented) components: a health
   check must observe the dependency, not add chaos events to the journal."
  (:require [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.core.leader-election.protocol :as lease-p]
            [d-core.health.registry :as health-reg])
  (:import (java.net InetSocketAddress Socket)))

(def ^:private default-check-timeout-ms 2000)

(defn- tcp-reachable?
  [host port timeout-ms]
  (try
    (with-open [socket (Socket.)]
      (.connect socket (InetSocketAddress. (str host) (int port)) (int timeout-ms))
      true)
    (catch Exception _
      false)))

(defn- db-check
  "A real round-trip: `SELECT ballot_id FROM ballots LIMIT 1`.
   `ballots` is created at boot in both engines, so a failure here means the
   connection or the database is gone, not that a table is missing."
  [db]
  (fn [_opts]
    (let [rows (sql/select db {:table :ballots :columns [:ballot_id] :limit 1})]
      {:status :up
       :details {:probe :select :rows (count rows)}})))

(defn- broker-check
  "Can this node reach the async-proxy gRPC port? A socket connect is the
   cheapest honest answer; it does not prove the broker behind the proxy is
   healthy, which is what the proxy's own readiness is for."
  [host port timeout-ms]
  (fn [_opts]
    (if (tcp-reachable? host port timeout-ms)
      {:status :up :details {:probe :tcp :host host :port port}}
      {:status :down
       :details {:probe :tcp :host host :port port :reachable? false}
       :error (str "cannot reach " host ":" port)})))

(defn- lease-store-check
  "Reads one lease key through the lease protocol. Exercises the same client and
   Lua path the renewer uses, without acquiring anything."
  [lease]
  (fn [_opts]
    (lease-p/status lease "health-probe" {})
    {:status :up :details {:probe :lease-status}}))

(defn- partitions-check
  "`owned-fn` returns `{:owned n :total m}`. Owning nothing is not fatal — a node
   that has just started, or a scale-down target, is legitimately empty — so it
   degrades rather than fails."
  [owned-fn]
  (fn [_opts]
    (let [{:keys [owned total]} (owned-fn)]
      {:status (if (pos? (long (or owned 0))) :up :degraded)
       :details {:partitions-owned (long (or owned 0))
                 :partitions-total (long (or total 0))}})))

(defn make-actors-registry
  "Builds the actors-tier `HealthRegistry`.

   Options:
     :service-name     - used in `/health` and the `service_health_status` gauge
     :metrics          - optional MetricsProtocol (Prometheus)
     :logger           - optional Duct logger
     :db               - raw SQL component (required)
     :broker           - {:host string :port long} of the async-proxy gRPC endpoint
     :lease            - raw lease component (nil when ownership is disabled)
     :owned-fn         - (fn [] {:owned n :total m})
     :check-timeout-ms - per-check timeout"
  [{:keys [service-name metrics logger db broker lease owned-fn check-timeout-ms]
    :or {service-name "election-actors"
         check-timeout-ms default-check-timeout-ms}}]
  (let [checks (cond-> {:liveness {:check-fn (fn [_] {:status :up})
                                   :critical? true
                                   :tags #{:liveness}}
                        :db {:check-fn (db-check db)
                             :critical? true
                             :timeout-ms check-timeout-ms
                             :tags #{:readiness}}
                        :broker {:check-fn (broker-check (:host broker) (:port broker) check-timeout-ms)
                                 :critical? true
                                 :timeout-ms check-timeout-ms
                                 :tags #{:readiness}}}
                 lease (assoc :lease-store
                              {:check-fn (lease-store-check lease)
                               :critical? true
                               :timeout-ms check-timeout-ms
                               :tags #{:readiness}})
                 owned-fn (assoc :owning-partitions
                                 {:check-fn (partitions-check owned-fn)
                                  :critical? false
                                  :timeout-ms check-timeout-ms
                                  :tags #{:readiness}}))]
    (health-reg/make-health-registry
     {:service-name service-name
      :metrics metrics
      :logger logger
      :checks checks})))
