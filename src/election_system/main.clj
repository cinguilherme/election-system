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

(defn build-config-from-env
  []
  (let [role (-> (env-str "ROLE" "api") str/lower-case keyword)
        engine (-> (env-str "DB_ENGINE" "postgres") str/lower-case keyword)
        jdbc-url (env-str "JDBC_URL" nil)
        db-user (env-str "DB_USER" "postgres")
        db-password (env-str "DB_PASSWORD" "postgres")
        pool-size (env-long "DB_POOL_SIZE" 4)]
    {:role role
     :http-port (env-long "HTTP_PORT" 8080)
     :metrics-port (env-long "METRICS_PORT" 8081)
     :proxy-host (env-str "ASYNC_PROXY_HOST" "localhost")
     :proxy-grpc-port (env-long "ASYNC_PROXY_GRPC_PORT" 50051)
     :worker-count (env-long "WORKER_COUNT" 4)
     :batch-size (env-long "BATCH_SIZE" 50)
     :flush-every-n (env-long "FLUSH_EVERY_N" 5)
     :tick-interval-ms (env-long "TICK_INTERVAL_MS" 2000)
     :election-id (env-str "ELECTION_ID" "national-2026")
     :timeout-ms (env-long "TIMEOUT_MS" 10000)
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
                  " pool-size=" (get-in cfg [:db-opts :pool-size])))
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
