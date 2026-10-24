(ns election-system.stateflow.write-sequence-flow-test
  "Flagship State-Flow 1: Causal Write Sequence & Batch Compression Invariant Verification.
   Asserts not just what ends up in SQLite/PostgreSQL, but the exact causal order
   of side-effects across HTTP API -> ActorClient -> AsyncProxy -> Precinct Actor ->
   Coalesced `:tell` Delta -> National Tally Actor -> Batched SQL Statements."
  (:require [clojure.test :refer [deftest]]
            [d-core.libs.time :as time]
            [d-core.stateflow.actor :as sf-actor]
            [d-core.stateflow.clock :as sf-clock]
            [d-core.stateflow.core :as flow]
            [d-core.stateflow.http :as sf-http]
            [d-core.stateflow.journal :as sf-journal]
            [election-system.system :as sys]
            [matcher-combinators.matchers :as m]))

(defn- start-election-world
  []
  (let [open-inst (time/instant "2026-11-03T08:00:00Z")
        close-inst (time/instant "2026-11-03T20:00:00Z")
        clk (time/manual-clock (time/instant "2026-11-03T07:30:00Z"))
        cluster (time/with-clock clk
                  (sys/start-multi-tier-test-cluster
                   {:election-id "national-2026"
                    :worker-count 2
                    :batch-size 4
                    :flush-every-n 2
                    :tell-interval-ms 0
                    :opens-at open-inst
                    :closes-at close-inst}))]
    {:clock clk
     :open-inst open-inst
     :close-inst close-inst
     :cluster cluster}))

(defn- stop-election-world!
  [{:keys [cluster]}]
  (when cluster
    (sys/stop-multi-tier-test-cluster! cluster)))

(deftest causal-write-sequence-and-batch-compression-flow-test
  (let [{:keys [clock] :as world} (start-election-world)]
    (try
      (time/with-clock clock
        (flow/run-with-lifecycle!
         (flow/flow "Verify causal sequence of side-effects across pre-open, sub-batch, batch threshold, duplicate rejection, and explicit flush"

           ;; Phase 1: Vote before election window opens -> rejected 403, ZERO SQL writes
           (sf-journal/clear!)
           [early-resp (sf-http/post! "/v1/elections/national-2026/votes"
                                      {:precinct_id "p-1"
                                       :voter_id "v-early"
                                       :candidate_id "alice"})]
           (flow/match? {:status 403
                         :data {:status "rejected" :reason "election-closed"}}
                        early-resp)
           (flow/match? [] (sf-journal/sql-writes))

           ;; Phase 2: Open election at 08:00:00Z and cast 3 votes (< batch-size 4)
           (sf-clock/set! "2026-11-03T08:00:00Z")
           (sf-journal/clear!)
           [v1 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-1" :voter_id "v-1" :candidate_id "alice"})
            v2 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-1" :voter_id "v-2" :candidate_id "bob"})
            v3 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-1" :voter_id "v-3" :candidate_id "alice"})]
           (flow/match? {:status 200 :data {:status "accepted" :precinct-total 1}} v1)
           (flow/match? {:status 200 :data {:status "accepted" :precinct-total 2}} v2)
           (flow/match? {:status 200 :data {:status "accepted" :precinct-total 3}} v3)

           ;; Duplicate vote for v-1 is rejected in O(1) memory with ZERO DB reads or writes
           [dup (sf-http/post! "/v1/elections/national-2026/votes"
                               {:precinct_id "p-1" :voter_id "v-1" :candidate_id "bob"})]
           (flow/match? {:status 409 :data {:status "rejected" :reason "duplicate-vote"}} dup)
           (flow/match? [] (sf-journal/sql-writes))

           ;; Live actor read returns the 3 buffered votes immediately without touching SQL
           [live-p1 (sf-http/get! "/v1/elections/national-2026/precincts/p-1/tally")]
           (flow/match? {:status 200
                         :data {:total-votes 3
                                :pending-ballots-count 3
                                :rejected-duplicates 1
                                :tallies {:alice 2 :bob 1}}}
                        live-p1)
           (flow/match? [] (sf-journal/sql-writes))

           ;; Phase 3: Cast 4th vote -> crosses batch-size (4)!
           ;; Must trigger:
           ;;   1) `:db/flush-precinct` actor effect
           ;;   2) ONE multi-row `:insert-batch!` into `:ballots` with 4 rows
           ;;   3) ZERO `:upsert!` on `:precinct_tallies` (Lever 3: hot-path tally suppression!)
           ;;   4) Asynchronous `:tell` (`:precinct-delta`) to `:election-tally`
           (sf-journal/clear!)
           [v4 (sf-http/post! "/v1/elections/national-2026/votes"
                              {:precinct_id "p-1" :voter_id "v-4" :candidate_id "alice"})]
           (flow/match? {:status 200
                         :data {:status "accepted"
                                :precinct-total 4
                                :flushed? true}}
                        v4)
           (flow/settle!)

           ;; Verify exact SQL write sequence on batch trigger: ONLY `:ballots` insert-batch!
           (flow/match? [{:op :insert-batch!
                          :table :ballots
                          :status :ok
                          :row-count 4}]
                        (sf-journal/sql-writes))

           ;; Verify causal sequence of actor & DB events during that 4th vote:
           ;; request(:cast-vote) -> deliver(:cast-vote) -> effect(:db/flush-precinct) ->
           ;; db(:insert-batch! :ballots) -> transport(:send-message! :election-tally :precinct-delta)
           (flow/match? (m/embeds
                         [{:component :actor-client :op :ask! :target :precinct}
                          {:component :actor-delivery :op :deliver-envelope! :target :precinct}
                          {:component :actor-effect :op :db/flush-precinct :target :precinct :row-count 4}
                          {:component :db :op :insert-batch! :target :ballots :row-count 4}
                          {:component :actor-transport :op :send-message! :target :election-tally :message-op :precinct-delta}
                          {:component :actor-delivery :op :deliver-envelope! :target :election-tally :message-op :precinct-delta}])
                        (sf-journal/events))

           ;; Phase 4: Explicit precinct & national flush persists the checkpoint upserts in order
           (sf-journal/clear!)
           [flush-p1 (sf-http/post! "/v1/elections/national-2026/precincts/p-1/flush" {})
            _ (sf-actor/ask! :election-tally "national-2026" {:type :flush})]
           (flow/match? {:status 200 :data {:status "noop"}} flush-p1)

           ;; Verify end-to-end audit endpoint reconciles live actors against persisted SQL
           [audit (sf-http/get! "/v1/elections/national-2026/audit")]
           (flow/match? {:status 200
                         :data {:consistent? true
                                :live-actor-tally {:total-votes 4
                                                   :precincts-reporting 1
                                                   :tallies {:alice 3 :bob 1}}
                                :db-audit {:ballot-count 4
                                           :ballot-tallies {:alice 3 :bob 1}
                                           :election-tally {:total-votes 4
                                                            :tallies {:alice 3 :bob 1}}}}}
                        audit))
         {:init (constantly world)}))
      (finally
        (stop-election-world! world)))))
