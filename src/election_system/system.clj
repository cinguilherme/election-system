(ns election-system.system
  "Duct / Integrant components and topology builders for the Election System:
   - API Ingress node
   - Dedicated Sharded Actor Worker node(s)
   - Full multi-tier testbed assembly"
  (:require [d-core.actor-system.core :as actor-core]
            [d-core.actor-system.protocols :as actor-p]
            [d-core.actor-system.runtime.engine :as engine]
            [d-core.actor-system.storage.sql-adapter :as sql-adapter]
            [d-core.actor-system.system]
            [d-core.actor-system.transport.async-proxy :as ap-transport]
            [d-core.core.clients.postgres.client :as pg-client]
            [d-core.core.clients.sqlite.client :as sqlite-client]
            [d-core.core.clients.valkey.client :as valkey-client]
            [d-core.core.consumers.async-proxy :as ap-cons]
            [d-core.core.databases.postgres]
            [d-core.core.databases.sqlite]
            [d-core.core.leader-election.valkey]
            [d-core.core.metrics.prometheus :as prom]
            [d-core.core.metrics.protocol :as metrics]
            [d-core.core.metrics.tracing :as otel]
            [d-core.core.producers.common :as common-prod]
            [d-core.core.producers.in-memory :as im-prod]
            [d-core.libs.time :as time]
            [d-core.queue]
            [d-core-async-proxy.grpc.server :as grpc-srv]
            [d-core-async-proxy.subscriptions.registry :as reg]
            [election-system.actors.precinct :as precinct]
            [election-system.actors.tally :as tally]
            [election-system.api.routes :as routes]
            [election-system.db.schema :as schema]
            [election-system.db.writer :as writer]
            [election-system.membership :as membership]
            [election-system.ownership :as own]
            [integrant.core :as ig]
            [ring.adapter.jetty :as jetty]))

(defn make-entity-specs
  "Builds the `:precinct` and `:election-tally` entity behavior specifications."
  [{:keys [election-id opens-at closes-at batch-size flush-every-n tick-interval-ms
           tick-min-batch tick-max-age-ms snapshot-every-n tell-interval-ms]}]
  ;; batch-size is a floor, not the target: a precinct only reaches it if it sees
  ;; enough votes. Under the load test each of ~250 precincts saw ~23 votes/s, so
  ;; the old batch-size 50 was unreachable and the 2s tick flushed ~6-ballot
  ;; batches instead -- 85k statements for 547k ballots (13% of intended size).
  ;; tick-min-batch gates the tick so it stops manufacturing tiny batches, and
  ;; tick-max-age-ms keeps a real staleness bound independent of tick frequency.
  {:precinct (precinct/make-precinct-spec
              (cond-> {:election-id election-id
                       :opens-at opens-at
                       :closes-at closes-at
                       :batch-size (or batch-size 200)
                       :tick-min-batch (or tick-min-batch (or batch-size 200))
                       :tick-max-age-ms (or tick-max-age-ms 10000)
                       :tick-interval-ms tick-interval-ms
                       :tell-interval-ms tell-interval-ms}
                ;; Snapshots are recovery state, not the vote path: every one is
                ;; a full-state write through the same connection pool as the
                ;; batches, and measured p99 was ~9s under load. Sparser
                ;; checkpoints cost a little more replay on restart and remove
                ;; the largest inline-effect tail.
                (some? snapshot-every-n) (assoc :snapshot-every-n (long snapshot-every-n))))
   :election-tally (tally/make-election-tally-spec
                    (cond-> {:flush-every-n (or flush-every-n 10)
                             :tick-interval-ms tick-interval-ms}
                      (some? snapshot-every-n) (assoc :snapshot-every-n (long snapshot-every-n))))})

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
  [{:keys [http-port proxy-host proxy-grpc-port worker-count total-partitions db-opts timeout-ms max-threads]
    :or {http-port 8080
         proxy-host "localhost"
         proxy-grpc-port 50051
         timeout-ms 10000
         max-threads 120}}]
  (let [tot-partitions (long (or total-partitions worker-count 4))
        {:keys [db pg-client tmp-file]} (connect-sql-database (or db-opts {:engine :sqlite}))
        {:keys [prom-registry prom-metrics]} (init-prometheus true)
        tracer-ctx (otel/init-tracer {:service-name (or (System/getenv "OTEL_SERVICE_NAME") "election-api")})
        ;; Report the name the tracer actually registered. Spans from this process
        ;; were showing up in Jaeger under a different service, and without this
        ;; line there is no way to tell "wrong name" from "wrong data in Jaeger".
        _ (println (str "[tracing] service-name=" (:service-name tracer-ctx)
                        " endpoint=" (or (System/getenv "OTEL_EXPORTER_OTLP_ENDPOINT") "none")
                        " sampling=" (or (System/getenv "OTEL_SAMPLING_RATIO") "1.0")))
        metrics-atom (writer/make-write-metrics)
        ingress-transport (ap-transport/create-async-proxy-transport
                           {:transport :netty
                            :host proxy-host
                            :port proxy-grpc-port
                            :partitions tot-partitions})
        actor-client (actor-core/create-client ingress-transport {:partitions tot-partitions
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

(defn- flush-partition-state!
  "Emits a final flush and snapshots every active actor of a drained partition.

   Runs only after the queue is empty: the snapshot the next owner rebuilds from
   must include everything this owner was handed, otherwise a handoff would lose
   the last batch of work that was already acknowledged."
  [runtime]
  (let [engine (:engine runtime)]
    (doseq [k (keys (:active-map engine))
            :when (vector? k)]
      (let [[entity-type entity-id] k]
        (try
          (engine/dispatch-envelope! engine entity-type entity-id
                                     {:type :message
                                      :payload {:type :flush}
                                      :timestamp (time/now-ms)})
          (actor-core/flush-actor! runtime entity-type entity-id)
          (catch Exception _ nil))))))

(defn- subscribe-read-lane!
  "Subscribes a second lane per entity on `actor.<type>.read`, feeding the same
   actor instance.

   Reads must not share the ballot lane: they queue behind its backlog and are
   blocked when its queue fills. Same leases, same fencing, different durable."
  [transport runtime entities partition partitions]
  (let [engine (:engine runtime)
        ;; Reads are answered from the actor's committed state, without entering
        ;; its mailbox. That is the whole point of the lane: the mailbox is
        ;; serialized, so a read queued behind a burst of tells waited seconds
        ;; for an answer the actor already had in memory.
        ;;
        ;; `peek-state` deliberately does not activate: if the entity is not
        ;; loaded we fall through to the mailbox path, which loads it (and is
        ;; slow, but correct -- and rare).
        ]
    (mapv (fn [[entity-type _]]
            (actor-p/subscribe-entities!
             transport entity-type
             (fn [etype eid env]
               (let [msg-type (keyword (or (get-in env [:payload :type])
                                           (:type env)
                                           :none))
                     reply-fn (:reply-fn env)
                     state (when (and (= :get-tally msg-type) reply-fn)
                             (engine/peek-state engine etype eid))]
                 (if state
                   ;; Answer immediately from memory; no mailbox, no queue.
                   (let [carrier (or (:metadata env) (get-in env [:payload :metadata]) (:context env))]
                     (otel/with-span [nil "actor.peek_state"
                                      {:carrier carrier
                                       :kind :internal
                                       :attributes {"entity.type" (name etype)
                                                    "entity.id" (str eid)
                                                    "op" (name msg-type)}}]
                       (reply-fn (case (keyword etype)
                                   :precinct (precinct/tally-view state)
                                   :election-tally (tally/tally-view state)
                                   (precinct/tally-view state))))
                     nil)
                   (engine/dispatch-envelope! engine etype eid env))))
             {:partition partition
              :partitions partitions
              :topic-suffix ".read"}))
          entities)))

(defn start-actors-node
  "Starts a dedicated Election Actors container node hosting sharded actor
   partitions connected to `d-core-async-proxy` over gRPC, writing batches to SQL, and
   exposing `/metrics` on `metrics-port`."
  [{:keys [metrics-port proxy-host proxy-grpc-port worker-count batch-size flush-every-n
           tick-interval-ms tick-min-batch tick-max-age-ms election-id opens-at closes-at db-opts
           assigned-partitions total-partitions ownership drain-timeout-ms snapshot-every-n
           tell-interval-ms]
    :or {metrics-port 8081
         proxy-host "localhost"
         proxy-grpc-port 50051
         batch-size 200
         flush-every-n 50
         tick-interval-ms 2000
         tick-max-age-ms 10000
         election-id "national-2026"}}]
  (let [assigned (vec (or assigned-partitions (range (or worker-count 4))))
        tot-partitions (long (or total-partitions (max (count assigned) (or worker-count 4))))
        {:keys [db pg-client tmp-file]} (connect-sql-database (or db-opts {:engine :sqlite}))
        {:keys [prom-registry prom-metrics]} (init-prometheus true)
        tracer-ctx (otel/init-tracer {:service-name (or (System/getenv "OTEL_SERVICE_NAME") "election-actors")
                                      :instance-id (:instance-id ownership)})
        ;; Report the name the tracer actually registered. Spans from this process
        ;; were showing up in Jaeger under a different service, and without this
        ;; line there is no way to tell "wrong name" from "wrong data in Jaeger".
        _ (println (str "[tracing] service-name=" (:service-name tracer-ctx)
                        " endpoint=" (or (System/getenv "OTEL_EXPORTER_OTLP_ENDPOINT") "none")
                        " sampling=" (or (System/getenv "OTEL_SAMPLING_RATIO") "1.0")))
        metrics-atom (writer/make-write-metrics)
        effect-handler (writer/make-effect-handler db metrics-atom nil prom-metrics)
        snapshot-store (sql-adapter/create-sql-snapshot-store db)
        entities (make-entity-specs {:election-id election-id
                                     :opens-at opens-at
                                     :closes-at closes-at
                                     :batch-size batch-size
                                     :flush-every-n flush-every-n
                                     :tick-min-batch (or tick-min-batch batch-size)
                                     :tick-max-age-ms (or tick-max-age-ms 10000)
                                     :tick-interval-ms tick-interval-ms
                                     :snapshot-every-n snapshot-every-n
                                     :tell-interval-ms tell-interval-ms})
        ;; Runtime lifecycle for one partition. Used by the ownership manager when
        ;; leases are enabled, and directly by the static path when they are not.
        start-partition!
        (fn [{:keys [partition lease-id token]}]
          (let [;; The drain handshake resolves this promise from the consumer's
                ;; control frame, so shutdown can wait for the backlog without
                ;; reaching into consumer internals.
                ;; Two lanes per entity now (write + read), so the handshake
                ;; completes only when both have reported drained.
                drained-count (atom 0)
                expected-drains (* 2 (count entities))
                drained-p (promise)
                t (ap-transport/create-async-proxy-transport
                   (cond-> {:transport :netty
                            :host proxy-host
                            :port proxy-grpc-port
                            :partition partition
                            :partitions tot-partitions
                            :poll-ms 10}
                     ;; Credentials only when we actually hold a lease: the proxy
                     ;; refuses a subscription whose token is not current.
                     lease-id (assoc :group (or (:group ownership) "actors")
                                     :lease-id lease-id
                                     :token token
                                     :on-drained (fn [_]
                                         (when (>= (swap! drained-count inc)
                                                   expected-drains)
                                           (deliver drained-p true))))))
                rt (actor-core/start-runtime
                    {:transport t
                     :storage snapshot-store
                     :entities entities
                     :db db
                     :metrics prom-metrics
                     :effect-handler effect-handler
                     :options {:partition partition
                               :partitions tot-partitions
                               :poll-ms 10
                               :watchdog-interval-ms (min 500 (long (or tick-interval-ms 2000)))}})
                ;; Read lane: a second subscription per entity, on its own topic
                ;; (`actor.precinct.read`), feeding the SAME actor instance.
                ;; Same leases and fencing, different durable -- which is the
                ;; whole point: a read no longer queues behind the ballot
                ;; backlog, and a full write queue cannot stall it either.
                read-subs (subscribe-read-lane! t rt entities partition tot-partitions)]
            {:partition partition
             :transport t
             :runtime rt
             :read-subs read-subs
             :drained-p drained-p}))

        stop-partition!
        (fn [{:keys [runtime read-subs transport]}]
          (doseq [sub read-subs]
            (try (actor-p/unsubscribe! transport sub nil) (catch Exception _ nil)))
          (when runtime (actor-core/stop-runtime! runtime))
          (when transport (ap-transport/close-async-proxy-transport! transport)))

        ;; Drain -> finish the queue -> flush state, in that order. Resigning is
        ;; the manager's job and happens only after this returns.
        drain-partition!
        (fn [{:keys [runtime read-subs drained-p]}]
          (doseq [sub (:subscriptions runtime)]
            (ap-cons/drain! (:consumer sub)))
          (doseq [sub read-subs]
            (ap-cons/drain! (:consumer sub)))
          (let [drained? (deref drained-p (long (or drain-timeout-ms 20000)) false)]
            (when drained?
              (flush-partition-state! runtime))
            drained?))

        ownership-log (fn [level event data]
                        (println (str "[ownership] " (name level) " " event " " (pr-str data))))

        ;; Two numbers that must agree for the system to be whole: how many
        ;; partitions exist, and how many leases the cluster currently holds.
        ;; sum(election_leaders_held) != election_partitions_total means some
        ;; shard is unowned — which is exactly the state this node was in when a
        ;; 1-of-8 assignment lost its single partition.
        partitions-gauge (when prom-metrics
                           (metrics/gauge prom-metrics
                                          {:name :election_partitions_total
                                           :help "Configured partition count for the election"}))
        leaders-gauge (when prom-metrics
                        (metrics/gauge prom-metrics
                                       {:name :election_leaders_held
                                        :help "Partitions this node currently holds a lease for"}))
        _ (when (and prom-metrics partitions-gauge)
            (metrics/set! prom-metrics partitions-gauge tot-partitions))
        set-leaders! (fn [snapshot]
                       (when (and prom-metrics leaders-gauge)
                         (metrics/set! prom-metrics leaders-gauge
                                       (count (filter (fn [[_ e]] (= :active (:state e)))
                                                      snapshot)))))
        _ (when (and prom-metrics leaders-gauge)
            (metrics/set! prom-metrics leaders-gauge 0))
        member-registry (when (:enabled? ownership)
                          (membership/make-membership
                           {:valkey-client (valkey-client/make-client
                                            {:uri (or (:valkey-uri ownership)
                                                      "redis://localhost:6379")})
                            :group (or (:group ownership) "actors")
                            :instance-id (:instance-id ownership)
                            :heartbeat-ttl-ms (long (or (:heartbeat-ttl-ms ownership) 20000))}))
        manager
        (when (:enabled? ownership)
          (let [ttl (long (or (:lease-ttl-ms ownership) 15000))]
            (own/make-manager
             {:lease (ig/init-key :d-core.core.leader-election.valkey/valkey
                                  {:valkey-client (valkey-client/make-client
                                                   {:uri (or (:valkey-uri ownership)
                                                             "redis://localhost:6379")})
                                   :default-lease-ms ttl})
              :group (or (:group ownership) "actors")
              :instance-id (:instance-id ownership)
              :members-fn (fn [] (membership/members member-registry))
              :heartbeat-fn (fn [] (membership/beat! member-registry))
              :leave-fn (fn [] (membership/leave! member-registry))
              :on-state! set-leaders!
              :partitions tot-partitions
              :lease-ttl-ms ttl
              :max-partitions (:max-partitions ownership)
              :grace-ms (:grace-ms ownership)
              :release-hold-off-ms (:hold-off-ms ownership)
              :start-partition! start-partition!
              :stop-partition! stop-partition!
              :drain-partition! drain-partition!
              :logger ownership-log})))

        _ (when manager (own/start! manager))
        ;; Without this the leases were acquired once and never renewed: they
        ;; simply expired after the TTL, taking take-over and eviction with them.
        _ (when manager (own/start-renewer! manager))

        owned (when manager
                (->> (own/snapshot manager)
                     (filter (fn [[_ e]] (= :active (:state e))))
                     (mapv key)
                     sort))

        ;; Static mode keeps the old behaviour: this instance runs exactly the
        ;; partitions it was told to.
        workers (when-not manager
                  (mapv (fn [idx] (start-partition! {:partition idx})) assigned))

        actual-workers-count (if manager (count owned) (count workers))

        metrics-handler (make-actors-metrics-handler prom-registry metrics-atom actual-workers-count)
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
     :ownership-manager manager
     :assigned-partitions (or owned assigned)
     :total-partitions tot-partitions
     :workers workers
     :jetty-server jetty-server}))

(defn stop-actors-node!
  "Stops an actors node, releasing partition ownership in the right order.

   With leases enabled the manager owns the runtimes: it drains each partition,
   flushes its state, stops it and only then resigns, so a peer cannot start
   consuming a partition this node is still writing to."
  [{:keys [jetty-server workers ownership-manager pg-client tmp-file tracer-ctx]}]
  (when jetty-server
    (.stop ^org.eclipse.jetty.server.Server jetty-server))
  (if ownership-manager
    (own/stop! ownership-manager)
    (doseq [{:keys [runtime transport]} workers]
      (when runtime (actor-core/stop-runtime! runtime))
      (when transport (ap-transport/close-async-proxy-transport! transport))))
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
  [{:keys [db engine worker-count assigned-partitions total-partitions batch-size flush-every-n opens-at closes-at election-id]
    :or {worker-count 2
         batch-size 50
         flush-every-n 5
         election-id "national-2026"}}]
  (let [assigned (vec (or assigned-partitions (range (or worker-count 2))))
        tot-partitions (long (or total-partitions (max (count assigned) (or worker-count 2))))
        tmp-file (when-not db (java.io.File/createTempFile "election-system-" ".db"))
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
                                  :partitions tot-partitions
                                  :poll-ms 5})
                              rt (actor-core/start-runtime
                                  {:transport t
                                   :storage snapshot-store
                                   :entities entities
                                   :db actual-db
                                   :metrics prom-metrics
                                   :effect-handler effect-handler
                                   :options {:partition idx
                                             :partitions tot-partitions
                                             :poll-ms 5
                                             :watchdog-interval-ms 60000}})
                              read-subs (subscribe-read-lane! t rt entities idx tot-partitions)]
                          {:partition idx
                           :transport t
                           :runtime rt
                           :read-subs read-subs}))
                      assigned)
        ingress-transport (ap-transport/create-async-proxy-transport
                           {:transport :in-process
                            :in-process-name in-proc-name
                            :partitions tot-partitions})
        actor-client (actor-core/create-client ingress-transport {:partitions tot-partitions
                                                                  :timeout-ms 5000})
        api-handler (routes/make-handler {:actor-client actor-client
                                          :db actual-db
                                          :metrics-atom metrics-atom
                                          :prom-metrics prom-metrics
                                          :prom-registry prom-registry
                                          :timeout-ms 5000})]
     {:db actual-db
      :tmp-file tmp-file
      :in-process-name in-proc-name
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
