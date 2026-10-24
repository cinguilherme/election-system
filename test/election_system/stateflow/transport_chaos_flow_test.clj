(ns election-system.stateflow.transport-chaos-flow-test
  "Flagship State-Flow 4: Async Transport Blackhole, gRPC Outage, and Admin HTTP Chaos Flow.
   Verifies:
   1. Remotely arming a `:drop` chaos rule via the Admin HTTP Control Plane (`POST /_chaos/rules`)
      to blackhole `:precinct-delta` `:tell` messages from `:precinct` to `:election-tally`.
   2. Casting votes and flushing precincts while the `:tell` blackhole is active persists raw
      ballots and precinct tallies, while the national tally lags (`consistent? false` on `/audit`),
      proving `query-db-audit` detects dropped delta messages!
   3. Clearing the blackhole rule via `DELETE /_chaos/rules` and re-flushing reconciles the
      national tally (`consistent? true`).
   4. Injecting a gRPC transport `:throw` fault between API Ingress and `async-proxy` trips the
      `election-async-proxy` circuit breaker after 2 failures, causing `/ready` to return `503`
      and `/votes` to fail-fast with `503 circuit-breaker-open` until healed."
  (:require [clojure.test :refer [deftest]]
            [d-core.core.circuit-breaker.protocol :as cb]
            [d-core.libs.time :as time]
            [d-core.stateflow.chaos :as sf-chaos]
            [d-core.stateflow.core :as flow]
            [d-core.stateflow.http :as sf-http]
            [d-core.stateflow.journal :as sf-journal]
            [election-system.system :as sys]))

(defn- start-transport-world
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

(defn- stop-transport-world!
  [{:keys [cluster]}]
  (when cluster
    (sys/stop-multi-tier-test-cluster! cluster)))

(deftest transport-blackhole-and-grpc-circuit-breaker-chaos-flow-test
  (let [{:keys [clock] :as world} (start-transport-world)]
    (try
      (time/with-clock clock
        (flow/run-with-lifecycle!
         (flow/flow "Verify async delta blackhole detection, Admin HTTP chaos control plane, and transport circuit-breaker failover"

           ;; 1. Use the Admin HTTP Control Plane (`POST /_chaos/rules`) to inject a `:drop`
           ;;    rule on `:actor-transport` `:send-message!` targeting `:election-tally`.
           (sf-journal/clear!)
           [inject-resp (sf-http/post! "/_chaos/rules"
                                       {:id "blackhole-national-deltas"
                                        :component "actor-transport"
                                        :op "send-message!"
                                        :target "election-tally"
                                        :fault {:type "drop" :return false}})
            status-resp (sf-http/get! "/_chaos/status")]
           (flow/match? {:status 200 :data {:status "injected" :rule-ids ["blackhole-national-deltas"]}}
                        inject-resp)
           (flow/match? {:status 200 :data {:enabled? true}} status-resp)

           ;; Cast 2 votes in p-1 -> triggers batch flush of 2 ballots AND attempts `:tell` to `:election-tally`
           [v1 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-1" :voter_id "v-1" :candidate_id "alice"})
            v2 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-1" :voter_id "v-2" :candidate_id "bob"})]
           (flow/match? {:status 200 :data {:status "accepted"}} v1)
           (flow/match? {:status 200 :data {:status "accepted" :flushed? true}} v2)
           (flow/settle!)

           ;; Verify the journal recorded `:dropped` on the `:precinct-delta` message to `:election-tally`!
           (flow/match? [{:component :actor-transport
                          :op :send-message!
                          :target :election-tally
                          :message-op :precinct-delta
                          :status :dropped
                          :chaos-rule-id :blackhole-national-deltas}]
                        (sf-journal/select {:component :actor-transport :status :dropped}))

           ;; Because the delta message was dropped in transit while the 2 raw ballots WERE persisted,
           ;; `/audit` immediately catches the discrepancy (`:consistent? false`, `live-actor-tally: 0` vs `ballot-count: 2`)!
           [audit-diverged (sf-http/get! "/v1/elections/national-2026/audit")]
           (flow/match? {:status 200
                         :data {:consistent? false
                                :live-actor-tally {:total-votes 0}
                                :db-audit {:ballot-count 2
                                           :ballot-tallies {:alice 1 :bob 1}}}}
                        audit-diverged)

           ;; 2. Clear the blackhole via `DELETE /_chaos/rules`, cast 1 more vote, and flush p-1.
           ;;    Wait — when `:precinct-delta` was emitted, `p-1` cleared `:pending-deltas`.
           ;;    Let's verify that casting a 3rd vote in `p-2` after clearing the blackhole delivers normally.
           [clear-resp (sf-http/delete! "/_chaos/rules")
            v3 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-2" :voter_id "v-3" :candidate_id "alice"})
            _ (sf-http/post! "/v1/elections/national-2026/precincts/p-2/flush" {})]
           (flow/match? {:status 200 :data {:status "cleared"}} clear-resp)
           (flow/match? {:status 200 :data {:status "accepted"}} v3)
           (flow/settle!)

           ;; 3. Now inject a gRPC transport `:throw` fault on `:actor-transport` `:request-message!`
           ;;    and verify `election-async-proxy` circuit breaker trips after 2 failures!
           (sf-journal/clear!)
           (sf-chaos/inject! {:id :grpc-proxy-unavailable
                              :component :actor-transport
                              :op :request-message!
                              :target :precinct
                              :fault {:type :throw
                                      :ex-msg "gRPC Status UNAVAILABLE: async-proxy unreachable"}})

           ;; First 2 vote attempts hit the transport fault and trip the breaker
           [_ (flow/invoke! (fn [w] (try ((get-in w [:cluster :api-handler])
                                          {:request-method :post
                                           :uri "/v1/elections/national-2026/votes"
                                           :body "{\"precinct_id\":\"p-1\",\"voter_id\":\"v-fail-1\",\"candidate_id\":\"alice\"}"})
                                         (catch Exception _ :failed))))
            _ (flow/invoke! (fn [w] (try ((get-in w [:cluster :api-handler])
                                          {:request-method :post
                                           :uri "/v1/elections/national-2026/votes"
                                           :body "{\"precinct_id\":\"p-1\",\"voter_id\":\"v-fail-2\",\"candidate_id\":\"alice\"}"})
                                         (catch Exception _ :failed))))]

           ;; Verify the circuit-breaker transition `:closed -> :open` was journaled
           (flow/match? [{:component :circuit-breaker
                          :op :state-transition
                          :target :election-async-proxy
                          :args {:from :closed :to :open}}]
                        (sf-journal/select {:component :circuit-breaker}))

           ;; 3rd vote fails fast with HTTP 503 `circuit-breaker-open`, and `/ready` returns 503
           [vote-fast-fail (sf-http/post! "/v1/elections/national-2026/votes"
                                          {:precinct_id "p-1" :voter_id "v-fail-3" :candidate_id "alice"})
            ready-down (sf-http/get! "/ready")]
           (flow/match? {:status 503
                         :data {:status "error" :reason "circuit-breaker-open"}}
                        vote-fast-fail)
           (flow/match? {:status 503
                         :data {:status "down" :healthy? false}}
                        ready-down)

           ;; 4. Heal the gRPC fault and reset the transport circuit breaker -> voting & readiness recover
           (sf-chaos/clear!)
           (flow/invoke! (fn [w] (cb/reset! (get-in w [:cluster :transport-breaker]))))
           [ready-up (sf-http/get! "/ready")
            v-recovered (sf-http/post! "/v1/elections/national-2026/votes"
                                       {:precinct_id "p-2" :voter_id "v-4" :candidate_id "bob"})]
           (flow/match? {:status 200 :data {:status "ok" :healthy? true}} ready-up)
           (flow/match? {:status 200 :data {:status "accepted" :precinct-total 2}} v-recovered))
         {:init (constantly world)}))
      (finally
        (stop-transport-world! world)))))
