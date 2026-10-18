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

(defn- env-bool
  [k default-val]
  (if-let [v (env-str k nil)]
    (Boolean/parseBoolean v)
    default-val))

(defn- env-list
  [k]
  (when-let [v (env-str k nil)]
    (mapv str/trim (str/split v #","))))

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
        instance-id (env-str "INSTANCE_ID" nil)
        ownership-members (env-list "OWNERSHIP_MEMBERS")]
    {:role role
     :http-port (env-long "HTTP_PORT" 8080)
     :metrics-port (env-long "METRICS_PORT" 8081)
     :proxy-host (env-str "ASYNC_PROXY_HOST" "localhost")
     :proxy-grpc-port (env-long "ASYNC_PROXY_GRPC_PORT" 50051)
     :worker-count worker-cnt
     :assigned-partitions assigned-parts
     :total-partitions total-parts
     ;; Partition ownership. Enabled by giving the node an INSTANCE_ID; the
     ;; rendezvous assignment needs the full member list, and the leases need a
     ;; valkey to live in. With this off the node falls back to
     ;; ASSIGNED_PARTITIONS, which is what local runs and the multi-tier test
     ;; cluster use.
     :drain-timeout-ms (env-long "DRAIN_TIMEOUT_MS" 20000)
     ;; Full-state snapshot every N messages per actor. Recovery state, not the
     ;; vote path; nil keeps each entity type's own default.
     :snapshot-every-n (env-long "SNAPSHOT_EVERY_N" nil)
     :ownership {:enabled? (and (some? instance-id)
                                (env-bool "OWNERSHIP_ENABLED" true))
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
                 ;; a number caps what one instance will hold. Set it to
                 ;; ceil(TOTAL_PARTITIONS / replicas): with 12 over 2 that is 6.
                 :max-partitions (env-long "MAX_PARTITIONS_PER_INSTANCE" nil)
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
