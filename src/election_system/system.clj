(ns election-system.system
  "Duct / Integrant components and topology builders for the Election System:
   - API Ingress node
   - Dedicated Sharded Actor Worker node(s)
   - Full multi-tier testbed assembly"
  (:require [d-core.actor-system.core :as actor-core]
            [d-core.actor-system.storage.sql-adapter :as sql-adapter]
            [d-core.actor-system.system]
            [d-core.actor-system.transport.async-proxy :as ap-transport]
            [d-core.core.clients.postgres.client :as pg-client]
            [d-core.core.clients.sqlite.client :as sqlite-client]
            [d-core.core.databases.postgres]
            [d-core.core.databases.sqlite]
            [d-core.core.metrics.prometheus :as prom]
            [d-core.core.metrics.tracing :as otel]
            [d-core.core.producers.common :as common-prod]
            [d-core.core.producers.in-memory :as im-prod]
            [d-core.queue]
            [d-core-async-proxy.grpc.server :as grpc-srv]
            [d-core-async-proxy.subscriptions.registry :as reg]
            [election-system.actors.precinct :as precinct]
            [election-system.actors.tally :as tally]
            [election-system.api.routes :as routes]
            [election-system.db.schema :as schema]
            [election-system.db.writer :as writer]
            [integrant.core :as ig]
            [ring.adapter.jetty :as jetty]))

(defn make-entity-specs
  "Builds the `:precinct` and `:election-tally` entity behavior specifications."
  [{:keys [election-id opens-at closes-at batch-size flush-every-n tick-interval-ms]}]
  {:precinct (precinct/make-precinct-spec {:election-id election-id
                                           :opens-at opens-at
                                           :closes-at closes-at
                                           :batch-size (or batch-size 50)
                                           :tick-interval-ms tick-interval-ms})
   :election-tally (tally/make-election-tally-spec {:flush-every-n (or flush-every-n 10)
                                                    :tick-interval-ms tick-interval-ms})})

(defmethod ig/init-key :election-system/entities
  [_ opts]
  (make-entity-specs opts))

(defmethod ig/init-key :election-system.db/init
  [_ {:keys [db engine]}]
  (schema/create-tables! db (or engine :sqlite))
  db)

(defmethod ig/init-key :election-system.db/metrics
  [_ _opts]
  (writer/make-write-metrics))

(defmethod ig/init-key :election-system.db/effect-handler
  [_ {:keys [db metrics logger prom-metrics]}]
  (writer/make-effect-handler db metrics logger prom-metrics))

(defmethod ig/init-key :election-system.api/handler
  [_ ctx]
  (routes/make-handler ctx))

(defmethod ig/init-key :election-system.api/jetty
  [_ {:keys [handler port]}]
  (jetty/run-jetty handler {:port (or port 8080) :join? false}))

(defmethod ig/halt-key! :election-system.api/jetty
  [_ server]
  (when server
    (.stop ^org.eclipse.jetty.server.Server server)))

(defn- init-prometheus
  [jvm-metrics?]
  (let [reg (ig/init-key :d-core.core.metrics.prometheus/registry
                         {:jvm-metrics? (boolean jvm-metrics?)})
        metrics (ig/init-key :d-core.core.metrics.prometheus/metrics
                             {:registry reg})]
    {:prom-registry reg
     :prom-metrics metrics}))

(defn connect-sql-database
  "Initializes either a pooled PostgreSQL database (with startup retry) or a local SQLite database,
   and ensures all Election System tables exist."
  [{:keys [engine jdbc-url username password pool-size max-retries retry-delay-ms]
    :or {engine :sqlite
         pool-size 4
         max-retries 30
         retry-delay-ms 1000}}]
  (if (= :postgres engine)
    (loop [attempt 1]
      (let [res (try
                  (let [client (pg-client/make-client
                                {:jdbc-url jdbc-url
                                 :username (or username "postgres")
                                 :password (or password "postgres")
                                 :pool? true
                                 :pool {:pool-name "election-pg-pool"
                                        :maximum-pool-size (int pool-size)
                                        :minimum-idle 1
                                        :connection-timeout-ms 5000}})
                        db (ig/init-key :d-core.core.databases.postgres/db
                                        {:postgres-client client
                                         :storage-schema schema/storage-schema})]
                    (schema/create-tables! db :postgres)
                    {:db db :pg-client client :engine :postgres})
                  (catch Exception e
                    {:error e}))]
        (if-let [err (:error res)]
          (if (< attempt max-retries)
            (do
              (Thread/sleep (long retry-delay-ms))
              (recur (inc attempt)))
            (throw err))
          res)))
    (let [tmp-file (when-not jdbc-url (java.io.File/createTempFile "election-system-" ".db"))
          url (or jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath tmp-file)))
          sqlite-c (sqlite-client/make-client {:jdbc-url url})
          db (ig/init-key :d-core.core.databases.sqlite/db
                          {:sqlite-client sqlite-c
                           :storage-schema schema/storage-schema})]
      (schema/create-tables! db :sqlite)
      {:db db :tmp-file tmp-file :engine :sqlite})))

(defn start-api-node
  "Starts a dedicated Election API container node connected to `d-core-async-proxy` over gRPC
   and backed by SQL for `/audit` queries and Prometheus `/metrics`."
  [{:keys [http-port proxy-host proxy-grpc-port worker-count db-opts timeout-ms max-threads]
    :or {http-port 8080
         proxy-host "localhost"
         proxy-grpc-port 50051
         worker-count 4
         timeout-ms 10000
         max-threads 120}}]
  (let [{:keys [db pg-client tmp-file]} (connect-sql-database (or db-opts {:engine :sqlite}))
        {:keys [prom-registry prom-metrics]} (init-prometheus true)
        tracer-ctx (otel/init-tracer {:service-name (or (System/getenv "OTEL_SERVICE_NAME") "election-api")})
        metrics-atom (writer/make-write-metrics)
        ingress-transport (ap-transport/create-async-proxy-transport
                           {:transport :netty
                            :host proxy-host
                            :port proxy-grpc-port
                            :partitions worker-count})
        actor-client (actor-core/create-client ingress-transport {:partitions worker-count
                                                                  :timeout-ms timeout-ms})
        handler (routes/make-handler {:actor-client actor-client
                                      :db db
                                      :metrics-atom metrics-atom
                                      :prom-metrics prom-metrics
                                      :prom-registry prom-registry
                                      :timeout-ms timeout-ms})
        jetty-server (jetty/run-jetty handler {:port (int http-port)
                                               :max-threads (int (or max-threads 120))
                                               :min-threads 16
                                               :join? false})]
    {:role :api
     :db db
     :pg-client pg-client
     :tmp-file tmp-file
     :prom-registry prom-registry
     :prom-metrics prom-metrics
     :tracer-ctx tracer-ctx
     :metrics-atom metrics-atom
     :ingress-transport ingress-transport
     :actor-client actor-client
     :jetty-server jetty-server}))

(defn stop-api-node!
  [{:keys [jetty-server ingress-transport pg-client tmp-file tracer-ctx]}]
  (when jetty-server
    (.stop ^org.eclipse.jetty.server.Server jetty-server))
  (when ingress-transport
    (ap-transport/close-async-proxy-transport! ingress-transport))
  (when tracer-ctx
    (otel/close-tracer! tracer-ctx))
  (when pg-client
    (pg-client/close! pg-client))
  (when tmp-file
    (.delete ^java.io.File tmp-file)))

(defn- make-actors-metrics-handler
  [prom-registry metrics-atom worker-count]
  (let [scrape-handler (prom/ring-metrics-handler prom-registry)]
    (fn [request]
      (if (= "/metrics" (:uri request))
        (scrape-handler request)
        {:status 200
         :headers {"Content-Type" "application/json; charset=utf-8"}
         :body (str "{\"status\":\"ok\",\"role\":\"actors\",\"workers\":" worker-count
                    ",\"sql_statements\":" (get @metrics-atom :total-sql-statements 0) "}")}))))

(defn start-actors-node
  "Starts a dedicated Election Actors container node hosting `worker-count` sharded actor
   partitions connected to `d-core-async-proxy` over gRPC, writing batches to SQL, and
   exposing `/metrics` on `metrics-port`."
  [{:keys [metrics-port proxy-host proxy-grpc-port worker-count batch-size flush-every-n
           tick-interval-ms election-id opens-at closes-at db-opts]
    :or {metrics-port 8081
         proxy-host "localhost"
         proxy-grpc-port 50051
         worker-count 4
         batch-size 50
         flush-every-n 5
         tick-interval-ms 2000
         election-id "national-2026"}}]
  (let [{:keys [db pg-client tmp-file]} (connect-sql-database (or db-opts {:engine :sqlite}))
        {:keys [prom-registry prom-metrics]} (init-prometheus true)
        tracer-ctx (otel/init-tracer {:service-name (or (System/getenv "OTEL_SERVICE_NAME") "election-actors")})
        metrics-atom (writer/make-write-metrics)
        effect-handler (writer/make-effect-handler db metrics-atom nil prom-metrics)
        snapshot-store (sql-adapter/create-sql-snapshot-store db)
        entities (make-entity-specs {:election-id election-id
                                     :opens-at opens-at
                                     :closes-at closes-at
                                     :batch-size batch-size
                                     :flush-every-n flush-every-n
                                     :tick-interval-ms tick-interval-ms})
        workers (mapv (fn [idx]
                        (let [t (ap-transport/create-async-proxy-transport
                                 {:transport :netty
                                  :host proxy-host
                                  :port proxy-grpc-port
                                  :partition idx
                                  :partitions worker-count
                                  :poll-ms 10})
                              rt (actor-core/start-runtime
                                  {:transport t
                                   :storage snapshot-store
                                   :entities entities
                                   :db db
                                   :metrics prom-metrics
                                   :effect-handler effect-handler
                                   :options {:partition idx
                                             :partitions worker-count
                                             :poll-ms 10
                                             :watchdog-interval-ms (min 500 (long (or tick-interval-ms 2000)))}})]
                          {:partition idx
                           :transport t
                           :runtime rt}))
                      (range worker-count))
        metrics-handler (make-actors-metrics-handler prom-registry metrics-atom worker-count)
        jetty-server (jetty/run-jetty metrics-handler {:port (int metrics-port) :join? false})]
    {:role :actors
     :db db
     :pg-client pg-client
     :tmp-file tmp-file
     :prom-registry prom-registry
     :prom-metrics prom-metrics
     :tracer-ctx tracer-ctx
     :metrics-atom metrics-atom
     :snapshot-store snapshot-store
     :workers workers
     :jetty-server jetty-server}))

(defn stop-actors-node!
  [{:keys [jetty-server workers pg-client tmp-file tracer-ctx]}]
  (when jetty-server
    (.stop ^org.eclipse.jetty.server.Server jetty-server))
  (doseq [{:keys [runtime transport]} workers]
    (actor-core/stop-runtime! runtime)
    (ap-transport/close-async-proxy-transport! transport))
  (when tracer-ctx
    (otel/close-tracer! tracer-ctx))
  (when pg-client
    (pg-client/close! pg-client))
  (when tmp-file
    (.delete ^java.io.File tmp-file)))

(defn start-multi-tier-test-cluster
  "Starts a complete multi-tier Election System cluster in-process:
   1. Constrained SQL database (SQLite default, or provided Postgres/MySQL `db` + `engine`)
   2. `d-core-async-proxy` gRPC server (hosting message queues, subscription registry, and sharding router)
   3. `N` dedicated sharded Actor Worker runtimes (`0 .. worker-count-1`), each connected to `async-proxy` over gRPC
   4. API Ingress node (with its own `ActorClient` connected to `async-proxy` over gRPC and Ring handler)"
  [{:keys [db engine worker-count batch-size flush-every-n opens-at closes-at election-id]
    :or {worker-count 2
         batch-size 50
         flush-every-n 5
         election-id "national-2026"}}]
  (let [tmp-file (when-not db (java.io.File/createTempFile "election-system-" ".db"))
        sqlite-c (when tmp-file
                   (sqlite-client/make-client {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath tmp-file))}))
        actual-db (or db
                      (ig/init-key :d-core.core.databases.sqlite/db
                                   {:sqlite-client sqlite-c
                                    :storage-schema schema/storage-schema}))
        _ (schema/create-tables! actual-db (or engine :sqlite))
        {:keys [prom-registry prom-metrics]} (init-prometheus false)
        metrics-atom (writer/make-write-metrics)
        effect-handler (writer/make-effect-handler actual-db metrics-atom nil prom-metrics)
        snapshot-store (sql-adapter/create-sql-snapshot-store actual-db)
        in-proc-name (str "election-async-proxy-" (random-uuid))
        queues (ig/init-key :d-core.queue/in-memory-queues {:topics [:default]})
        im-producer (im-prod/->InMemoryProducer queues nil)
        routing {:publish {:default {:targets :in-memory}}}
        proxy-producer (common-prod/->CommonProducer :in-memory {:in-memory im-producer} routing nil nil)
        proxy-registry (reg/make-registry {:queues queues
                                           :dispatcher-ctx {:max-attempts 3 :timeout-ms 5000}
                                           :metrics prom-metrics
                                           :logger nil})
        proxy-server (grpc-srv/start-grpc-server {:producer proxy-producer
                                                  :registry proxy-registry
                                                  :metrics prom-metrics
                                                  :transport :in-process
                                                  :in-process-name in-proc-name})
        entities (make-entity-specs {:election-id election-id
                                     :opens-at opens-at
                                     :closes-at closes-at
                                     :batch-size batch-size
                                     :flush-every-n flush-every-n})
        workers (mapv (fn [idx]
                        (let [t (ap-transport/create-async-proxy-transport
                                 {:transport :in-process
                                  :in-process-name in-proc-name
                                  :partition idx
                                  :partitions worker-count
                                  :poll-ms 5})
                              rt (actor-core/start-runtime
                                  {:transport t
                                   :storage snapshot-store
                                   :entities entities
                                   :db actual-db
                                   :metrics prom-metrics
                                   :effect-handler effect-handler
                                   :options {:partition idx
                                             :partitions worker-count
                                             :poll-ms 5
                                             :watchdog-interval-ms 60000}})]
                          {:partition idx
                           :transport t
                           :runtime rt}))
                      (range worker-count))
        ingress-transport (ap-transport/create-async-proxy-transport
                           {:transport :in-process
                            :in-process-name in-proc-name
                            :partitions worker-count})
        actor-client (actor-core/create-client ingress-transport {:partitions worker-count
                                                                  :timeout-ms 5000})
        api-handler (routes/make-handler {:actor-client actor-client
                                          :db actual-db
                                          :metrics-atom metrics-atom
                                          :prom-metrics prom-metrics
                                          :prom-registry prom-registry
                                          :timeout-ms 5000})]
    {:db actual-db
     :tmp-file tmp-file
     :metrics-atom metrics-atom
     :prom-registry prom-registry
     :prom-metrics prom-metrics
     :snapshot-store snapshot-store
     :proxy-server proxy-server
     :proxy-registry proxy-registry
     :workers workers
     :ingress-transport ingress-transport
     :actor-client actor-client
     :api-handler api-handler}))

(defn stop-multi-tier-test-cluster!
  [{:keys [workers ingress-transport proxy-server proxy-registry tmp-file]}]
  (doseq [{:keys [runtime transport]} workers]
    (actor-core/stop-runtime! runtime)
    (ap-transport/close-async-proxy-transport! transport))
  (ap-transport/close-async-proxy-transport! ingress-transport)
  (ig/halt-key! :d-core-async-proxy.grpc/server proxy-server)
  (ig/halt-key! :d-core-async-proxy.subscriptions/registry proxy-registry)
  (when tmp-file
    (.delete ^java.io.File tmp-file)))
