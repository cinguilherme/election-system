(ns election-system.integration.scalability-benchmark-test
  (:require [cheshire.core :as json]
            [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.core :as actor]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.transport.async-proxy :as ap-transport]
            [d-core.libs.time :as time]
            [election-system.db.writer :as writer]
            [election-system.system :as sys]))

(defn- post-json [handler uri body]
  (let [resp (handler {:request-method :post
                       :uri uri
                       :headers {"content-type" "application/json"}
                       :body (json/generate-string body)})]
    (assoc resp :data (json/parse-string (:body resp) true))))

(defn- get-json [handler uri]
  (let [resp (handler {:request-method :get
                       :uri uri
                       :headers {}})]
    (assoc resp :data (json/parse-string (:body resp) true))))

(deftest multi-tier-election-lifecycle-and-scalability-benchmark-test
  (testing "End-to-end Election System: API Ingress -> AsyncProxy (gRPC) -> 4 Sharded Actor Workers -> Batched SQL DB"
    (let [open-inst (time/instant "2026-11-03T08:00:00Z")
          close-inst (time/instant "2026-11-03T20:00:00Z")
          clk (time/manual-clock (time/instant "2026-11-03T07:00:00Z"))]
      (time/with-clock clk
        (let [cluster (sys/start-multi-tier-test-cluster
                       {:election-id "national-2026"
                        :worker-count 4
                        :batch-size 50
                        :flush-every-n 5
                        :opens-at open-inst
                        :closes-at close-inst})
              handler (:api-handler cluster)
              actor-client (:actor-client cluster)
              metrics-atom (:metrics-atom cluster)]
          (try
            ;; 1. Verify health endpoint
            (let [h (get-json handler "/health")]
              (is (= 200 (:status h)))
              (is (= "ok" (get-in h [:data :status]))))

            ;; 2. Pre-election vote rejected with 403 (:election-closed)
            (let [early (post-json handler "/v1/elections/national-2026/votes"
                                   {:precinct_id "precinct-0"
                                    :voter_id "voter-early"
                                    :candidate_id "alice"})]
              (is (= 403 (:status early)))
              (is (= "election-closed" (get-in early [:data :reason]))))

            ;; 3. Open the election window at 08:00:00Z
            (time/set-clock! clk open-inst)

            ;; 4. High-throughput concurrent vote storm:
            ;;    20 precincts * 50 unique voters = 1,000 valid votes
            ;;    + 200 duplicate vote attempts across those 20 precincts
            (let [precinct-ids (mapv #(str "precinct-" %) (range 20))
                  candidates ["alice" "bob" "charlie"]
                  vote-tasks (for [p-idx (range 20)
                                   v-idx (range 50)]
                               {:precinct_id (nth precinct-ids p-idx)
                                :voter_id (str "voter-" p-idx "-" v-idx)
                                :candidate_id (nth candidates (mod (+ p-idx v-idx) 3))})
                  dup-tasks (take 200 vote-tasks)
                  ;; Dispatch all 1,000 valid votes concurrently across virtual threads
                  valid-futures (mapv (fn [v]
                                        (future
                                          (post-json handler "/v1/elections/national-2026/votes" v)))
                                      vote-tasks)
                  valid-responses (mapv deref valid-futures)
                  ;; Dispatch 200 duplicate votes concurrently
                  dup-futures (mapv (fn [v]
                                      (future
                                        (post-json handler "/v1/elections/national-2026/votes" v)))
                                    dup-tasks)
                  dup-responses (mapv deref dup-futures)]
              (is (every? #(= 200 (:status %)) valid-responses))
              (is (every? #(= 409 (:status %)) dup-responses))

              ;; Flush any remaining dirty state on precinct and national tally actors
              (doseq [pid precinct-ids]
                (actor/ask-sync! actor-client :precinct pid {:type :flush}))
              ;; Wait briefly for async child->parent :tell messages to arrive at :election-tally
              (loop [attempts 0]
                (let [t (actor/ask-sync! actor-client :election-tally "national-2026" {:type :get-tally})]
                  (when (and (< (:total-votes t) 1000) (< attempts 40))
                    (Thread/sleep 25)
                    (recur (inc attempts)))))
              (actor/ask-sync! actor-client :election-tally "national-2026" {:type :flush})

              ;; 5. Verify Single-Writer Sharding Guarantee across the 4 dedicated Actor Worker runtimes
              (let [worker-precinct-sets
                    (mapv (fn [{:keys [runtime]}]
                            (set (keep (fn [[[etype eid] _]]
                                         (when (= :precinct etype) eid))
                                       (p/active-actors (:engine runtime) {}))))
                          (:workers cluster))]
                ;; No two workers ever activated the same precinct!
                (doseq [i (range (count worker-precinct-sets))
                        j (range (inc i) (count worker-precinct-sets))]
                  (is (empty? (set/intersection (nth worker-precinct-sets i)
                                                (nth worker-precinct-sets j)))))
                ;; All 20 precincts were activated across the 4 workers
                (is (= 20 (count (apply set/union worker-precinct-sets)))))

              ;; 6. Verify Audit Endpoint & Database Write Reduction
              (let [audit-resp (get-json handler "/v1/elections/national-2026/audit")
                    audit (:data audit-resp)
                    live-tally (:live-actor-tally audit)
                    db-audit (:db-audit audit)
                    w-metrics @metrics-atom]
                (is (= 200 (:status audit-resp)))
                (is (true? (:consistent? audit)))
                (is (= 1000 (:total-votes live-tally)))
                (is (= 20 (:precincts-reporting live-tally)))
                (is (= 1000 (:ballot-count db-audit)))
                (is (= (:tallies live-tally) (:ballot-tallies db-audit)))
                (is (= (:tallies live-tally) (get-in db-audit [:election-tally :tallies])))

                ;; Verify DB scalability: 1,000 ballots + 20 precinct tallies + national tally
                ;; required only ~20 batch inserts + ~25 upserts (< 60 total SQL statements vs 2,200+ unbatched!)
                (is (= 20 (:batch-inserts-count w-metrics)))
                (is (= 1000 (:ballots-persisted-count w-metrics)))
                (is (< (:total-sql-statements w-metrics) 65)))

              ;; 7. Close election window at 20:00:00Z -> late vote rejected with 403
              (time/set-clock! clk close-inst)
              (let [late (post-json handler "/v1/elections/national-2026/votes"
                                    {:precinct_id "precinct-0"
                                     :voter_id "voter-late"
                                     :candidate_id "alice"})]
                (is (= 403 (:status late)))
                (is (= "election-closed" (get-in late [:data :reason]))))

              ;; 8. Passivate all actors and verify seamless rehydration from SqlSnapshotStore
              (time/advance-clock! clk {:minutes 15})
              (doseq [{:keys [runtime]} (:workers cluster)]
                (actor/sweep-idle-actors! runtime))
              (let [rehydrated (get-json handler "/v1/elections/national-2026/precincts/precinct-0/tally")]
                (is (= 200 (:status rehydrated)))
                (is (= 50 (get-in rehydrated [:data :total-votes])))
                ;; Duplicate check still works after passivation & rehydration from SQL snapshot!
                (time/set-clock! clk (time/instant "2026-11-03T12:00:00Z"))
                (let [dup-after-rehydrate (post-json handler "/v1/elections/national-2026/votes"
                                                     {:precinct_id "precinct-0"
                                                      :voter_id "voter-0-0"
                                                      :candidate_id "alice"})]
                  (is (= 409 (:status dup-after-rehydrate)))
                  (is (= "duplicate-vote" (get-in dup-after-rehydrate [:data :reason])))))

              ;; 9. Verify Prometheus /metrics endpoint captures all multi-tier metrics
              (let [metrics-resp (handler {:request-method :get :uri "/metrics" :headers {}})
                    body (:body metrics-resp)]
                (is (= 200 (:status metrics-resp)))
                (is (.contains ^String body "http_requests_total"))
                (is (.contains ^String body "election_votes_cast_total"))
                (is (.contains ^String body "async_proxy_published_total"))
                (is (.contains ^String body "actor_messages_processed_total"))
                (is (.contains ^String body "actor_active_entities"))
                (is (.contains ^String body "sql_statements_total"))
                (is (.contains ^String body "sql_batch_rows_total"))))
            (finally
              (sys/stop-multi-tier-test-cluster! cluster))))))))

(deftest multi-node-actor-sharding-integration-test
  (testing "Multi-node Actor Partition Sharding: Node 1 handles partitions [0 1], Node 2 handles partitions [2 3] out of 4 total"
    (let [election-id "sharded-election-2026"
          open-inst (time/instant "2026-11-03T08:00:00Z")
          close-inst (time/instant "2026-11-03T20:00:00Z")
          clk (time/manual-clock open-inst)]
      (time/with-clock clk
        (let [cluster-node-1 (sys/start-multi-tier-test-cluster
                              {:election-id election-id
                               :assigned-partitions [0 1]
                               :total-partitions 4
                               :batch-size 10
                               :flush-every-n 2
                               :opens-at open-inst
                               :closes-at close-inst})
              in-proc-name (:in-process-name cluster-node-1)
              shared-db (:db cluster-node-1)
              prom-metrics (:prom-metrics cluster-node-1)
              snapshot-store (:snapshot-store cluster-node-1)
              metrics-atom (:metrics-atom cluster-node-1)
              entities (sys/make-entity-specs {:election-id election-id
                                               :opens-at open-inst
                                               :closes-at close-inst
                                               :batch-size 10
                                               :flush-every-n 2})
              node-2-workers (mapv (fn [idx]
                                     (let [t (ap-transport/create-async-proxy-transport
                                              {:transport :in-process
                                               :in-process-name in-proc-name
                                               :partition idx
                                               :partitions 4
                                               :poll-ms 5})
                                           rt (actor/start-runtime
                                               {:transport t
                                                :storage snapshot-store
                                                :entities entities
                                                :db shared-db
                                                :metrics prom-metrics
                                                :effect-handler (writer/make-effect-handler shared-db metrics-atom nil prom-metrics)
                                                :options {:partition idx
                                                          :partitions 4
                                                          :poll-ms 5
                                                          :watchdog-interval-ms 60000}})]
                                       {:partition idx :transport t :runtime rt}))
                                   [2 3])
              actor-client (:actor-client cluster-node-1)
              handler (:api-handler cluster-node-1)]
          (try
            (doseq [p-idx (range 8)]
              (let [precinct-id (str "p-" p-idx)
                    resp (post-json handler (str "/v1/elections/" election-id "/votes")
                                    {:precinct_id precinct-id
                                     :voter_id (str "v-" p-idx)
                                     :candidate_id "alice"})]
                (is (= 200 (:status resp)))))
            (doseq [p-idx (range 8)]
              (let [precinct-id (str "p-" p-idx)
                    flush-resp (post-json handler (str "/v1/elections/" election-id "/precincts/" precinct-id "/flush") {})]
                (is (= 200 (:status flush-resp)))))
            ;; Wait for async child->parent :tell messages to arrive at :election-tally
            (loop [attempts 0]
              (let [t (actor/ask-sync! actor-client :election-tally election-id {:type :get-tally})]
                (when (and (< (long (or (:total-votes t) 0)) 8) (< attempts 40))
                  (Thread/sleep 25)
                  (recur (inc attempts)))))
            (actor/ask-sync! actor-client :election-tally election-id {:type :flush})
            (let [tally-resp (get-json handler (str "/v1/elections/" election-id "/tally"))]
              (is (= 200 (:status tally-resp)))
              (is (= 8 (get-in tally-resp [:data :total-votes])))
              (is (= 8 (get-in tally-resp [:data :tallies :alice]))))
            ;; Verify that both node 1 and node 2 processed disjoint subsets of precincts
            (let [node-1-precincts (set (mapcat (fn [{:keys [runtime]}]
                                                  (keep (fn [[[etype eid] _]] (when (= :precinct etype) eid))
                                                        (p/active-actors (:engine runtime) {})))
                                                (:workers cluster-node-1)))
                  node-2-precincts (set (mapcat (fn [{:keys [runtime]}]
                                                  (keep (fn [[[etype eid] _]] (when (= :precinct etype) eid))
                                                        (p/active-actors (:engine runtime) {})))
                                                node-2-workers))]
              (is (pos? (count node-1-precincts)))
              (is (pos? (count node-2-precincts)))
              (is (empty? (set/intersection node-1-precincts node-2-precincts)))
              (is (= 8 (count (set/union node-1-precincts node-2-precincts)))))
            (finally
              (doseq [{:keys [runtime transport]} node-2-workers]
                (actor/stop-runtime! runtime)
                (ap-transport/close-async-proxy-transport! transport))
              (sys/stop-multi-tier-test-cluster! cluster-node-1))))))))

