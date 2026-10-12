(ns election-system.system
  "Duct / Integrant components and topology builders for the Election System:
   - API Ingress node
   - Dedicated Sharded Actor Worker node(s)
   - Full multi-tier testbed assembly"
  (:require [d-core.actor-system.core :as actor-core]
            [d-core.actor-system.storage.sql-adapter :as sql-adapter]
            [d-core.actor-system.system]
            [d-core.actor-system.transport.async-proxy :as ap-transport]
            [d-core.core.clients.sqlite.client :as sqlite-client]
            [d-core.core.databases.sqlite]
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
  [_ {:keys [db metrics logger]}]
  (writer/make-effect-handler db metrics logger))

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
        metrics-atom (writer/make-write-metrics)
        effect-handler (writer/make-effect-handler actual-db metrics-atom nil)
        snapshot-store (sql-adapter/create-sql-snapshot-store actual-db)
        in-proc-name (str "election-async-proxy-" (random-uuid))
        queues (ig/init-key :d-core.queue/in-memory-queues {:topics [:default]})
        im-producer (im-prod/->InMemoryProducer queues nil)
        routing {:publish {:default {:targets :in-memory}}}
        proxy-producer (common-prod/->CommonProducer :in-memory {:in-memory im-producer} routing nil nil)
        proxy-registry (reg/make-registry {:queues queues
                                           :dispatcher-ctx {:max-attempts 3 :timeout-ms 5000}
                                           :logger nil})
        proxy-server (grpc-srv/start-grpc-server {:producer proxy-producer
                                                  :registry proxy-registry
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
                                          :timeout-ms 5000})]
    {:db actual-db
     :tmp-file tmp-file
     :metrics-atom metrics-atom
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
