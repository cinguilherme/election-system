(ns election-system.stateflow.db-chaos-flow-test
  "Flagship State-Flow 2: Database Transient Failure, Ack-Loss (`:fail-after`),
   and Circuit Breaker Recovery Flow.
   Verifies:
   1. When `:ballots` batch insert suffers `:fail-after` (ack lost after DB commit),
      re-inserting or retrying the same ballot batch is 100% idempotent (`ON CONFLICT DO NOTHING`)
      and produces zero duplicate ballots in the audit ledger.
   2. When SQL `:select` fails repeatedly (`:throw`), the `election-db` circuit breaker
      trips to `:open`, causing `/ready` and `/health` to report `503 down` and `/audit`
      to fail-fast with `503 circuit-breaker-open` without hitting the database.
   3. Clearing the fault and resetting/recovering the circuit breaker restores full
      audit consistency."
  (:require [clojure.test :refer [deftest]]
            [d-core.chaos.journal :as chaos-journal]
            [d-core.core.circuit-breaker.protocol :as cb]
            [d-core.libs.time :as time]
            [d-core.stateflow.actor :as sf-actor]
            [d-core.stateflow.chaos :as sf-chaos]
            [d-core.stateflow.core :as flow]
            [d-core.stateflow.http :as sf-http]
            [d-core.stateflow.journal :as sf-journal]
            [election-system.db.writer :as writer]
            [election-system.system :as sys]
            [matcher-combinators.matchers :as m]))

(defn- start-chaos-world
  []
  (let [open-inst (time/instant "2026-11-03T08:00:00Z")
        close-inst (time/instant "2026-11-03T20:00:00Z")
        clk (time/manual-clock open-inst)
        cluster (time/with-clock clk
                  (sys/start-multi-tier-test-cluster
                   {:election-id "national-2026"
                    :worker-count 2
                    :batch-size 2
                    :flush-every-n 1
                    :tell-interval-ms 0
                    :cb-failure-threshold 2
                    :cb-reset-timeout-ms 500
                    :opens-at open-inst
                    :closes-at close-inst}))]
    {:clock clk
     :cluster cluster}))

(defn- stop-chaos-world!
  [{:keys [cluster]}]
  (when cluster
    (sys/stop-multi-tier-test-cluster! cluster)))

(deftest db-ack-loss-idempotency-and-circuit-breaker-chaos-flow-test
  (let [{:keys [clock] :as world} (start-chaos-world)]
    (try
      (time/with-clock clock
        (flow/run-with-lifecycle!
         (flow/flow "Simulate DB ack-loss (:fail-after) on ballot batch insert and DB outage tripping circuit breaker"

           ;; 1. Arm a `:fail-after` fault on the very first `:ballots` `:insert-batch!`.
           ;;    This commits the 2 ballots to SQLite, then throws an exception to the caller
           ;;    (simulating a network drop right after PostgreSQL committed the transaction).
           (sf-journal/clear!)
           (sf-chaos/inject! {:id :ballot-ack-loss
                              :component :db
                              :op :insert-batch!
                              :target :ballots
                              :times 1
                              :fault {:type :fail-after
                                      :ex-msg "Simulated network reset after DB commit"}})

           ;; Cast 2 votes in p-1 -> triggers batch flush (batch-size = 2)
           [v1 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-1" :voter_id "v-1" :candidate_id "alice"})
            v2 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-1" :voter_id "v-2" :candidate_id "bob"})]
           (flow/match? {:status 200 :data {:status "accepted"}} v1)
           (flow/match? {:status 200 :data {:status "accepted" :flushed? true}} v2)
           (flow/settle!)

           ;; Verify the journal captured `:chaos-fail-after` on `:ballots` AND `:error` on `:db/flush-precinct`
           (flow/match? (m/embeds
                         [{:component :db
                           :op :insert-batch!
                           :target :ballots
                           :status :chaos-fail-after
                           :chaos-rule-id :ballot-ack-loss
                           :row-count 2}
                          {:component :actor-effect
                           :op :db/flush-precinct
                           :status :error}])
                        (sf-journal/events))

           ;; Now simulate the retry / replay of the exact same ballot batch (as happens when a consumer
           ;; or recovery replays unacknowledged deliveries): `ON CONFLICT DO NOTHING` must ensure
           ;; zero duplicate rows in `:ballots`!
           [_ (flow/invoke!
               (fn [w]
                 (let [db (get-in w [:cluster :db])
                       committed-rows (get-in (first (filter #(= :insert-batch! (:op %))
                                                             (chaos-journal/events (get-in w [:cluster :event-journal]))))
                                              [:args :rows])]
                   (writer/flush-precinct-to-db!
                    db nil
                    {:election-id "national-2026"
                     :precinct-id "p-1"
                     :ballots committed-rows
                     :tallies {:alice 1 :bob 1}
                     :total-votes 2
                     :flushes 1}))))]
           (flow/settle!)

           ;; Verify audit is 100% consistent and ballot count is strictly 2 (no double-counting!)
           [_ (sf-actor/ask! :election-tally "national-2026" {:type :flush})
            audit-1 (sf-http/get! "/v1/elections/national-2026/audit")]
           (flow/match? {:status 200
                         :data {:consistent? true
                                :db-audit {:ballot-count 2
                                           :ballot-tallies {:alice 1 :bob 1}}}}
                        audit-1)

           ;; 2. Inject a hard DB outage (`:throw`) on `:select` and verify the DB Circuit Breaker
           ;;    trips after 2 failures (`:cb-failure-threshold 2`), turning `/ready` and `/health` to 503.
           (sf-journal/clear!)
           (sf-chaos/inject! {:id :db-select-outage
                              :component :db
                              :op :select
                              :fault {:type :throw
                                      :ex-msg "PostgreSQL connection pool exhausted"}})

           ;; First 2 `/audit` calls hit the DB fault and increment the circuit breaker failure count
           [_ (flow/invoke! (fn [w] (try ((get-in w [:cluster :api-handler])
                                          {:request-method :get
                                           :uri "/v1/elections/national-2026/audit"})
                                         (catch Throwable _ :failed))))
            _ (flow/invoke! (fn [w] (try ((get-in w [:cluster :api-handler])
                                          {:request-method :get
                                           :uri "/v1/elections/national-2026/audit"})
                                         (catch Throwable _ :failed))))]

           ;; Journal records the circuit-breaker transition `:closed -> :open`!
           (flow/match? [{:component :circuit-breaker
                          :op :state-transition
                          :target :election-db
                          :args {:from :closed :to :open}}]
                        (sf-journal/select {:component :circuit-breaker}))

           ;; 3rd `/audit` call fails fast with HTTP 503 `circuit-breaker-open`
           [audit-open (sf-http/get! "/v1/elections/national-2026/audit")
            ready-open (sf-http/get! "/ready")
            health-open (sf-http/get! "/health")]
           (flow/match? {:status 503 :data {:status "error" :reason "circuit-breaker-open"}} audit-open)
           (flow/match? {:status 503 :data {:status "down" :healthy? false}} ready-open)
           (flow/match? {:status 503
                         :data {:status "down"
                                :components {:db {:status "down" :details {:state "open"}}
                                             :async-proxy {:status "up"}}}}
                        health-open)

           ;; Voting still works while the read/audit DB breaker is open because actors buffer in memory!
           [v3 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-2" :voter_id "v-3" :candidate_id "alice"})]
           (flow/match? {:status 200 :data {:status "accepted" :precinct-total 1}} v3)

           ;; 3. Clear the DB fault and reset the breaker -> `/ready` and `/audit` recover immediately
           (sf-chaos/clear!)
           (flow/invoke! (fn [w] (cb/reset! (get-in w [:cluster :db-breaker]))))
           [_ (sf-http/post! "/v1/elections/national-2026/precincts/p-2/flush" {})
            _ (flow/settle!)
            _ (sf-actor/ask! :election-tally "national-2026" {:type :flush})
            ready-healed (sf-http/get! "/ready")
            audit-healed (sf-http/get! "/v1/elections/national-2026/audit")]
           (flow/match? {:status 200 :data {:status "ok" :healthy? true}} ready-healed)
           (flow/match? {:status 200
                         :data {:consistent? true
                                :live-actor-tally {:total-votes 3
                                                   :tallies {:alice 2 :bob 1}}
                                :db-audit {:ballot-count 3
                                           :ballot-tallies {:alice 2 :bob 1}}}}
                        audit-healed))
         {:init (constantly world)}))
      (finally
        (stop-chaos-world! world)))))
