(ns election-system.main
  "Main entrypoint for containerized Election System nodes (`ROLE=api` or `ROLE=actors`)."
  (:gen-class)
  (:require [clojure.string :as str]
            [election-system.system :as system]))

(defn- env-str
  [k default-val]
  (let [v (System/getenv k)]
    (if (and v (not (str/blank? v)))
      (str/trim v)
      default-val)))

(defn- env-long
  [k default-val]
  (if-let [v (env-str k nil)]
    (try
      (Long/parseLong v)
      (catch Exception _
        default-val))
    default-val))

(defn- env-double
  [k default-val]
  (if-let [v (env-str k nil)]
    (try
      (Double/parseDouble v)
      (catch Exception _
        default-val))
    default-val))

(defn- env-bool
  [k default-val]
  (if-let [v (env-str k nil)]
    (Boolean/parseBoolean v)
    default-val))

(defn- env-list
  [k]
  (when-let [v (env-str k nil)]
    (mapv str/trim (str/split v #","))))

(defn- default-instance-id
  []
  (or (env-str "HOSTNAME" nil)
      (try (.getHostName (java.net.InetAddress/getLocalHost))
           (catch Exception _ nil))
      (str "actors-" (subs (str (random-uuid)) 0 8))))

(defn parse-partition-list
  "Parses a comma-delimited partition list string (e.g. '0,1,2,3') into a vector of longs."
  [s]
  (when (and s (not (str/blank? s)))
    (mapv #(Long/parseLong (str/trim %)) (str/split (str/trim s) #","))))

(defn build-config-from-env
  []
  (let [role (-> (env-str "ROLE" "api") str/lower-case keyword)
        engine (-> (env-str "DB_ENGINE" "postgres") str/lower-case keyword)
        jdbc-url (env-str "JDBC_URL" nil)
        db-user (env-str "DB_USER" "postgres")
        db-password (env-str "DB_PASSWORD" "postgres")
        pool-size (env-long "DB_POOL_SIZE" 4)
        assigned-parts (parse-partition-list (env-str "ASSIGNED_PARTITIONS" nil))
        worker-cnt (env-long "WORKER_COUNT" (if (seq assigned-parts) (count assigned-parts) 4))
        total-parts (env-long "TOTAL_PARTITIONS" (or (when (seq assigned-parts) (max worker-cnt (count assigned-parts))) 8))
        explicit-instance-id (env-str "INSTANCE_ID" nil)
        ownership-enabled? (if (some? (env-str "OWNERSHIP_ENABLED" nil))
                             (env-bool "OWNERSHIP_ENABLED" true)
                             (some? explicit-instance-id))
        instance-id (or explicit-instance-id
                        (when ownership-enabled? (default-instance-id)))
        ownership-members (env-list "OWNERSHIP_MEMBERS")
        actors-replicas (env-long "ACTORS_REPLICAS" nil)
        max-parts (or (env-long "MAX_PARTITIONS_PER_INSTANCE" nil)
                      (when (and total-parts actors-replicas (pos? actors-replicas))
                        (long (Math/ceil (/ (double total-parts) (double actors-replicas))))))]
    {:role role
     :http-port (env-long "HTTP_PORT" 8080)
     :metrics-port (env-long "METRICS_PORT" 8081)
     :proxy-host (env-str "ASYNC_PROXY_HOST" "localhost")
     :proxy-grpc-port (env-long "ASYNC_PROXY_GRPC_PORT" 50051)
     :worker-count worker-cnt
     :assigned-partitions assigned-parts
     :total-partitions total-parts
     ;; Partition ownership. Enabled via OWNERSHIP_ENABLED=true (defaulting
     ;; INSTANCE_ID to the container's HOSTNAME so scaled replicas need no
     ;; per-instance config) or by setting INSTANCE_ID directly. Live membership
     ;; comes from Valkey heartbeats. With this off the node falls back to
     ;; ASSIGNED_PARTITIONS, which is what local runs and the multi-tier test
     ;; cluster use.
     :drain-timeout-ms (env-long "DRAIN_TIMEOUT_MS" 20000)
     ;; Full-state snapshot every N messages per actor. Recovery state, not the
     ;; vote path; nil keeps each entity type's own default.
     :snapshot-every-n (env-long "SNAPSHOT_EVERY_N" nil)
     ;; How often a precinct ships accumulated deltas to the national tally.
     ;; These are NATS messages, not DB writes -- and because the tally flushes
     ;; every N tells, coalescing reduces DB flushes too (fewer, larger batches).
     :tell-interval-ms (env-long "TELL_INTERVAL_MS" 200)
     :ownership {:enabled? (and (some? instance-id) ownership-enabled?)
                 :group (env-str "OWNERSHIP_GROUP" "actors")
                 :instance-id instance-id
                 :members (or (seq ownership-members)
                              (when instance-id [instance-id]))
                 :valkey-uri (env-str "VALKEY_URI" "redis://localhost:6379")
                 :lease-ttl-ms (env-long "LEASE_TTL_MS" 15000)
                 ;; Membership heartbeat TTL. Long enough that a missed beat does
                 ;; not eject a healthy node, short enough that a dead node's
                 ;; partitions become claimable in seconds, not minutes.
                 :heartbeat-ttl-ms (env-long "HEARTBEAT_TTL_MS" 20000)
                 ;; Even distribution. nil = pure rendezvous (no rebalancing);
                 ;; a number caps what one instance will hold. Derived as
                 ;; ceil(TOTAL_PARTITIONS / ACTORS_REPLICAS) when not explicitly set.
                 :max-partitions max-parts
                 ;; How long a survivor may exceed the cap before forgoing the
                 ;; excess, so one healthy node can serve everything briefly
                 ;; after a failure instead of leaving shards unowned.
                 :grace-ms (env-long "LEADERSHIP_GRACE_MS" 60000)
                 ;; How long a forgoed partition stays unclaimable, so it moves
                 ;; to another node instead of bouncing back.
                 :hold-off-ms (env-long "LEADERSHIP_HOLD_OFF_MS" 3000)}
     :batch-size (env-long "BATCH_SIZE" 200)
     :flush-every-n (env-long "FLUSH_EVERY_N" 50)
     :tick-interval-ms (env-long "TICK_INTERVAL_MS" 2000)
     :tick-min-batch (env-long "TICK_MIN_BATCH" 200)
     :tick-max-age-ms (env-long "TICK_MAX_AGE_MS" 10000)
     :election-id (env-str "ELECTION_ID" "national-2026")
     :timeout-ms (env-long "TIMEOUT_MS" 10000)
     :max-threads (env-long "JETTY_MAX_THREADS" 120)
     ;; Chaos is opt-in. docker-compose.chaos.yaml is the only place that sets
     ;; CHAOS_ENABLED=1; with it off no decorator, journal or admin listener is
     ;; created, so the request path is the plain one.
     ;;
     ;; CHAOS_ALLOWED_COMPONENTS narrows what a rule may target (comma-separated,
     ;; nil = no restriction); CHAOS_MAX_TOTAL_HITS is the blast-radius cap -- when
     ;; it is reached the controller disables itself instead of injecting forever.
     :chaos {:enabled? (env-bool "CHAOS_ENABLED" false)
             :admin-port (env-long "CHAOS_ADMIN_PORT" 8082)
             :admin-token (env-str "CHAOS_ADMIN_TOKEN" nil)
             ;; :full keeps payloads (bounded), :meta keeps shape only, :off injects
             ;; without recording. :sample trades fidelity for overhead.
             :journal-mode (-> (env-str "CHAOS_JOURNAL_MODE" "full") str/lower-case keyword)
             :max-events (env-long "CHAOS_MAX_EVENTS" 50000)
             :sample (env-double "CHAOS_SAMPLE" 1.0)
             :max-payload-chars (env-long "CHAOS_MAX_PAYLOAD_CHARS" 4000)
             :allowed-components (some->> (env-list "CHAOS_ALLOWED_COMPONENTS")
                                          (map #(keyword (str/lower-case %)))
                                          set
                                          not-empty)
             :max-total-hits (env-long "CHAOS_MAX_TOTAL_HITS" 100000)
             :audit-max-entries (env-long "CHAOS_AUDIT_MAX_ENTRIES" 2000)}
     :db-opts {:engine engine
               :jdbc-url jdbc-url
               :username db-user
               :password db-password
               :pool-size pool-size}}))

(defn -main
  [& _args]
  (let [cfg (build-config-from-env)
        role (:role cfg)]
    (println (str "[election-system] Starting node with role=" (name role)
                  " proxy=" (:proxy-host cfg) ":" (:proxy-grpc-port cfg)
                  " db-engine=" (name (get-in cfg [:db-opts :engine]))
                  " pool-size=" (get-in cfg [:db-opts :pool-size])
                  (when (:assigned-partitions cfg)
                    (str " assigned-partitions=" (pr-str (:assigned-partitions cfg))))
                  " total-partitions=" (:total-partitions cfg)))
    (let [node (case role
                 :actors (system/start-actors-node cfg)
                 :api (system/start-api-node cfg)
                 (throw (ex-info (str "Unknown ROLE: " role) {:cfg cfg})))]
      (.addShutdownHook
       (Runtime/getRuntime)
       (Thread. ^Runnable
                (fn []
                  (println (str "[election-system] Stopping node role=" (name role)))
                  (if (= :actors role)
                    (system/stop-actors-node! node)
                    (system/stop-api-node! node)))))
      (println (str "[election-system] Node role=" (name role) " started and ready."))
      @(promise))))
