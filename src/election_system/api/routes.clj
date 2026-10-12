(ns election-system.api.routes
  "HTTP API Ingress routes for the Election System.
   Routes incoming votes and real-time tally queries through `ActorClient`
   (backed by `AsyncProxyTransport` over gRPC) and exposes a SQL audit endpoint."
  (:require [cheshire.core :as json]
            [d-core.actor-system.core :as actor]
            [d-core.core.metrics.prometheus :as prom]
            [d-core.core.metrics.protocol :as metrics-proto]
            [d-core.core.metrics.wrappers :as wrappers]
            [d-core.libs.time :as time]
            [election-system.db.writer :as writer]
            [election-system.domain.election :as domain]
            [reitit.ring :as ring]))

(defn- json-response
  [body status]
  {:status status
   :headers {"content-type" "application/json"}
   :body (json/generate-string body)})

(defn- parse-body
  [req]
  (let [b (:body req)]
    (cond
      (map? b) b
      (string? b) (try (json/parse-string b true) (catch Exception _ {}))
      (instance? java.io.InputStream b)
      (try
        (json/parse-stream (java.io.InputStreamReader. ^java.io.InputStream b "UTF-8") true)
        (catch Exception _ {}))
      :else {})))

(defn- handle-health
  [_ctx]
  (fn [_req]
    (json-response {:status "ok"
                    :service "election-system"
                    :timestamp (time/now-ms)}
                   200)))

(defn- handle-configure-window
  [{:keys [actor-client timeout-ms]}]
  (fn [req]
    (let [election-id (get-in req [:path-params :election-id])
          precinct-id (get-in req [:path-params :precinct-id])
          body (parse-body req)
          res (actor/ask-sync! actor-client
                               :precinct
                               precinct-id
                               (cond-> {:type :configure-window
                                        :election-id election-id}
                                 (contains? body :opens_at) (assoc :opens-at (:opens_at body))
                                 (contains? body :opens-at) (assoc :opens-at (:opens-at body))
                                 (contains? body :closes_at) (assoc :closes-at (:closes_at body))
                                 (contains? body :closes-at) (assoc :closes-at (:closes-at body))
                                 (:batch_size body) (assoc :batch-size (:batch_size body))
                                 (:batch-size body) (assoc :batch-size (:batch-size body)))
                               {:timeout-ms (or timeout-ms 5000)})]
      (json-response res 200))))

(defn- status-code-for-vote-reply
  [reply]
  (case (:status reply)
    :accepted 200
    :rejected (case (:reason reply)
                :duplicate-vote 409
                :election-closed 403
                400)
    500))

(defn- record-vote-metric!
  [metrics votes-counter election-id candidate-id mode status]
  (when (and metrics votes-counter)
    (wrappers/inc-labeled! metrics votes-counter
                           [election-id (or candidate-id "unknown") mode status])))

(defn- handle-cast-vote
  [{:keys [actor-client metrics votes-counter timeout-ms]}]
  (fn [req]
    (let [election-id (get-in req [:path-params :election-id])
          body (parse-body req)
          validation (domain/validate-vote-payload (assoc body :election-id election-id))]
      (if-not (:valid? validation)
        (do
          (record-vote-metric! metrics votes-counter election-id nil :sync :invalid-payload)
          (json-response {:status "rejected"
                          :reason "invalid-payload"
                          :error (:error validation)}
                         400))
        (let [{:keys [precinct-id candidate-id] :as vote} (:vote validation)
              async? (or (true? (:async body))
                         (= "true" (get-in req [:query-params "async"])))
              msg (assoc vote :type :cast-vote)]
          (if async?
            (do
              (actor/tell! actor-client :precinct precinct-id msg {})
              (record-vote-metric! metrics votes-counter election-id candidate-id :async :accepted)
              (json-response {:status "accepted"
                              :mode "async"
                              :ballot-id (:ballot-id vote)
                              :precinct-id precinct-id}
                             202))
            (let [reply (actor/ask-sync! actor-client
                                         :precinct
                                         precinct-id
                                         msg
                                         {:timeout-ms (or timeout-ms 5000)})
                  vote-status (or (:status reply) :error)]
              (record-vote-metric! metrics votes-counter election-id candidate-id :sync vote-status)
              (json-response reply (status-code-for-vote-reply reply)))))))))

(defn- handle-flush-precinct
  [{:keys [actor-client timeout-ms]}]
  (fn [req]
    (let [precinct-id (get-in req [:path-params :precinct-id])
          res (actor/ask-sync! actor-client
                               :precinct
                               precinct-id
                               {:type :flush}
                               {:timeout-ms (or timeout-ms 5000)})]
      (json-response res 200))))

(defn- handle-precinct-tally
  [{:keys [actor-client timeout-ms]}]
  (fn [req]
    (let [precinct-id (get-in req [:path-params :precinct-id])
          res (actor/ask-sync! actor-client
                               :precinct
                               precinct-id
                               {:type :get-tally}
                               {:timeout-ms (or timeout-ms 5000)})]
      (json-response res 200))))

(defn- handle-election-tally
  [{:keys [actor-client timeout-ms]}]
  (fn [req]
    (let [election-id (get-in req [:path-params :election-id])
          res (actor/ask-sync! actor-client
                               :election-tally
                               election-id
                               {:type :get-tally}
                               {:timeout-ms (or timeout-ms 5000)})]
      (json-response res 200))))

(defn- handle-election-audit
  [{:keys [actor-client db metrics-atom timeout-ms]}]
  (fn [req]
    (let [election-id (get-in req [:path-params :election-id])
          live-tally (actor/ask-sync! actor-client
                                      :election-tally
                                      election-id
                                      {:type :get-tally}
                                      {:timeout-ms (or timeout-ms 5000)})
          db-audit (when db (writer/query-db-audit db election-id))
          write-metrics (when metrics-atom @metrics-atom)]
      (json-response {:election-id election-id
                      :live-actor-tally live-tally
                      :db-audit db-audit
                      :write-metrics write-metrics
                      :consistent? (if db-audit
                                     (= (:total-votes live-tally) (:ballot-count db-audit))
                                     true)}
                     200))))

(defn- handle-metrics
  [{:keys [metrics]}]
  (if metrics
    (prom/ring-metrics-handler metrics)
    (fn [_req]
      {:status 200
       :headers {"content-type" "text/plain; version=0.0.4"}
       :body "# HELP election_system_up Election system up indicator\n# TYPE election_system_up gauge\nelection_system_up 1.0\n"})))

(defn make-handler
  "Creates the Ring handler for the Election System API Ingress service."
  [{:keys [metrics prom-metrics] :as ctx}]
  (let [eff-metrics (or metrics prom-metrics)
        votes-counter (when eff-metrics
                        (metrics-proto/counter
                         eff-metrics
                         {:name :election_votes_cast_total
                          :help "Total ballot submissions processed by election API"
                          :labels [:election_id :candidate_id :mode :status]}))
        enriched-ctx (assoc ctx
                            :metrics eff-metrics
                            :votes-counter votes-counter)
        base-handler (ring/ring-handler
                      (ring/router
                       [["/health" {:get {:handler (handle-health enriched-ctx)}}]
                        ["/metrics" {:get {:handler (handle-metrics enriched-ctx)}}]
                        ["/v1/elections/:election-id"
                         ["/votes" {:post {:handler (handle-cast-vote enriched-ctx)}}]
                         ["/tally" {:get {:handler (handle-election-tally enriched-ctx)}}]
                         ["/audit" {:get {:handler (handle-election-audit enriched-ctx)}}]
                         ["/precincts/:precinct-id/window" {:post {:handler (handle-configure-window enriched-ctx)}}]
                         ["/precincts/:precinct-id/flush" {:post {:handler (handle-flush-precinct enriched-ctx)}}]
                         ["/precincts/:precinct-id/tally" {:get {:handler (handle-precinct-tally enriched-ctx)}}]]])
                      (ring/create-default-handler))]
    (if eff-metrics
      (wrappers/wrap-http-metrics base-handler eff-metrics {:service "election-api"})
      base-handler)))

