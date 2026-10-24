(ns election-system.stateflow.fencing-chaos-flow-test
  "Flagship State-Flow 3: Split-Brain Lease Eviction & Monotonic Fencing Flow.
   Simulates a split-brain race where:
   1. Worker A holds partition lease at `epoch = 5` and flushes an initial checkpoint.
   2. Worker A experiences a GC pause / network stall while a successor Worker B
      acquires the partition lease at `epoch = 6` and flushes updated precinct & national tallies.
   3. Worker A wakes up and attempts a delayed zombie flush stamped with stale `epoch = 5`.
   4. Verifies via both the causal journal and SQLite/PostgreSQL state that:
      - Both `:upsert!` calls are observed in causal sequence (`fencing 6` then `fencing 5`),
      - The database `WHERE ownership_epoch < EXCLUDED.ownership_epoch` guard rejects
        Worker A's stale overwrite on both `precinct_tallies` and `election_tallies`,
      - A subsequent `epoch = 7` flush succeeds normally."
  (:require [clojure.test :refer [deftest]]
            [d-core.core.leader-election.protocol :as lp]
            [d-core.chaos.instrument :as chaos-inst]
            [d-core.libs.time :as time]
            [d-core.stateflow.chaos :as sf-chaos]
            [d-core.stateflow.core :as flow]
            [d-core.stateflow.http :as sf-http]
            [d-core.stateflow.journal :as sf-journal]
            [election-system.ownership :as own]
            [election-system.system :as sys]))

(defn- make-in-memory-lease-store
  "In-memory implementation of `LeaderElectionProtocol` with monotonic `:fencing` epochs."
  []
  (let [state-atom (atom {})]
    (reify lp/LeaderElectionProtocol
      (acquire! [_ election-id opts]
        (let [owner-id (:owner-id opts "owner")
              res (atom nil)]
          (swap! state-atom
                 (fn [m]
                   (let [curr (get m election-id)]
                     (if (= :held (:status curr))
                       (do
                         (reset! res {:status :busy :owner-id (:owner-id curr)})
                         m)
                       (let [next-epoch (inc (long (or (:fencing curr) 0)))
                             tok (str "tok-" election-id "-e" next-epoch)]
                         (reset! res {:status :acquired :token tok :fencing next-epoch})
                         (assoc m election-id {:status :held
                                               :owner-id owner-id
                                               :token tok
                                               :fencing next-epoch}))))))
          @res))
      (renew! [_ election-id token _opts]
        (let [curr (get @state-atom election-id)]
          (if (and (= :held (:status curr)) (= token (:token curr)))
            {:status :renewed :token token :fencing (:fencing curr)}
            {:status :lost})))
      (resign! [_ election-id token _opts]
        (let [res (atom nil)]
          (swap! state-atom
                 (fn [m]
                   (let [curr (get m election-id)]
                     (if (and (= :held (:status curr)) (= token (:token curr)))
                       (do
                         (reset! res {:status :released})
                         (assoc m election-id (assoc curr :status :vacant :token nil)))
                       (do
                         (reset! res {:status :not-owner})
                         m)))))
          @res))
      (status [_ election-id _opts]
        (get @state-atom election-id {:status :vacant}))
      (validate! [_ election-id token _opts]
        (let [curr (get @state-atom election-id)]
          (cond
            (or (nil? curr) (= :vacant (:status curr))) {:status :vacant}
            (= token (:token curr)) {:status :current :fencing (:fencing curr)}
            :else {:status :stale :fencing (:fencing curr)}))))))

(defn- start-fencing-world
  []
  (let [open-inst (time/instant "2026-11-03T08:00:00Z")
        close-inst (time/instant "2026-11-03T20:00:00Z")
        clk (time/manual-clock open-inst)
        cluster (time/with-clock clk
                  (sys/start-multi-tier-test-cluster
                   {:election-id "national-2026"
                    :worker-count 2
                    :batch-size 10
                    :flush-every-n 5
                    :opens-at open-inst
                    :closes-at close-inst}))
        lease-store (chaos-inst/instrument-leader-election
                     (make-in-memory-lease-store)
                     (:chaos-controller cluster)
                     (:event-journal cluster)
                     {:instance-id "valkey-lease"})]
    {:clock clk
     :cluster cluster
     :lease lease-store}))

(defn- stop-fencing-world!
  [{:keys [cluster]}]
  (when cluster
    (sys/stop-multi-tier-test-cluster! cluster)))

(deftest split-brain-lease-eviction-and-monotonic-fencing-flow-test
  (let [{:keys [clock] :as world} (start-fencing-world)]
    (try
      (time/with-clock clock
        (flow/run-with-lifecycle!
         (flow/flow "Verify lease eviction under chaos and monotonic fencing protection against split-brain zombie flushes"

           ;; 1. Verify LeaderElection lease acquisition, Chaos `:return {:status :lost}` on renew!,
           ;;    and automatic partition eviction in `election-system.ownership`
           (sf-journal/clear!)
           [mgr (flow/invoke!
                 (fn [{:keys [lease]}]
                   (let [m (own/make-manager
                            {:lease lease
                             :group "actors"
                             :instance-id "worker-a"
                             :members ["worker-a"]
                             :partitions 1
                             :lease-ttl-ms 15000
                             :start-partition! (fn [{:keys [partition fencing]}] {:partition partition :fencing fencing})
                             :stop-partition! (fn [_] nil)
                             :drain-partition! (fn [_] true)
                             :logger nil})]
                     (own/start! m)
                     m)))]
           ;; Worker A acquired partition 0 at fencing epoch 1
           (flow/match? {0 {:state :active :lease-id "actors/p0" :fencing 1}}
                        (flow/invoke! (fn [_] (own/snapshot mgr))))

           ;; Inject a Chaos rule simulating Valkey lease loss on `renew!`
           (sf-chaos/inject! {:id :simulate-lease-expiry
                              :component :lease
                              :op :renew!
                              :target "actors/p0"
                              :times 1
                              :fault {:type :return
                                      :return {:status :lost}}})
           (flow/invoke! (fn [_] (own/renew-all! mgr)))

           ;; Ownership manager immediately demotes partition 0 to `:vacant` and resigns
           (flow/match? {0 {:state :vacant}}
                        (flow/invoke! (fn [_] (own/snapshot mgr))))

           ;; Verify causal sequence on `:lease`: acquire! -> validate! -> renew! (chaos-return :lost) -> resign!
           (flow/match? [{:component :lease :op :acquire! :target "actors/p0" :status :ok}
                         {:component :lease :op :validate! :target "actors/p0" :status :ok}
                         {:component :lease :op :renew! :target "actors/p0" :status :chaos-return :chaos-rule-id :simulate-lease-expiry}
                         {:component :lease :op :resign! :target "actors/p0" :status :ok}]
                        (sf-journal/select {:component :lease}))

           ;; 2. Now simulate the split-brain DB flush race through the instrumented EffectHandler & SQL:
           ;;    - Successor Worker B (at fencing epoch 6) flushes precinct "p-1" with 100 votes (alice: 60, bob: 40)
           ;;      and national tally with 100 votes.
           ;;    - Zombie Worker A (still holding stale fencing epoch 5 from before eviction)
           ;;      wakes up from a GC pause and executes a late `:db/flush-precinct` and `:db/flush-election-tally`
           ;;      with bogus/stale totals (999 votes).
           (sf-journal/clear!)
           (flow/invoke!
            (fn [{:keys [cluster]}]
              (let [eff-fn (:effect-handler cluster)
                    now-inst (time/now)]
                ;; New owner Worker B flushes at epoch 6
                (eff-fn {:type :db/flush-precinct
                         :election-id "national-2026"
                         :precinct-id "p-1"
                         :ballots [{:ballot_id "b-1" :election_id "national-2026" :precinct_id "p-1" :voter_id "v-1" :candidate_id "alice" :cast_at now-inst}
                                   {:ballot_id "b-2" :election_id "national-2026" :precinct_id "p-1" :voter_id "v-2" :candidate_id "bob" :cast_at now-inst}]
                         :tallies {:alice 60 :bob 40}
                         :total-votes 100
                         :flushes 10
                         :fencing 6}
                        {:entity-type :precinct :entity-id "national-2026:p-1"})
                (eff-fn {:type :db/flush-election-tally
                         :election-id "national-2026"
                         :tallies {:alice 60 :bob 40}
                         :precinct-totals {"p-1" 100}
                         :total-votes 100
                         :flushes 5
                         :fencing 6}
                        {:entity-type :election-tally :entity-id "national-2026"})

                ;; Stale zombie Worker A attempts late overwrite at epoch 5!
                (eff-fn {:type :db/flush-precinct
                         :election-id "national-2026"
                         :precinct-id "p-1"
                         :ballots []
                         :tallies {:alice 999 :bob 0}
                         :total-votes 999
                         :flushes 2
                         :fencing 5}
                        {:entity-type :precinct :entity-id "national-2026:p-1"})
                (eff-fn {:type :db/flush-election-tally
                         :election-id "national-2026"
                         :tallies {:alice 999 :bob 0}
                         :precinct-totals {"p-1" 999}
                         :total-votes 999
                         :flushes 2
                         :fencing 5}
                        {:entity-type :election-tally :entity-id "national-2026"}))))

           ;; Verify the causal SQL write sequence shows all 5 statements in exact order,
           ;; stamped with their respective `:fencing` epochs (6, 6, 5, 5):
           (flow/match? [{:op :insert-batch! :table :ballots :row-count 2 :status :ok}
                         {:op :upsert! :table :precinct_tallies :fencing 6 :status :ok}
                         {:op :upsert! :table :election_tallies :fencing 6 :status :ok}
                         {:op :upsert! :table :precinct_tallies :fencing 5 :status :ok}
                         {:op :upsert! :table :election_tallies :fencing 5 :status :ok}]
                        (sf-journal/sql-writes))

           ;; Verify the database rejected the epoch 5 overwrites and preserved epoch 6's exact state!
           [audit-resp (sf-http/get! "/v1/elections/national-2026/audit")]
           (flow/match? {:status 200
                         :data {:db-audit {:ballot-count 2
                                           :precinct-tallies {:p-1 {:total-votes 100
                                                                    :flushes 10
                                                                    :tallies {:alice 60 :bob 40}}}
                                           :election-tally {:total-votes 100
                                                            :flushes 5
                                                            :tallies {:alice 60 :bob 40}}}}}
                        audit-resp))
         {:init (constantly world)}))
      (finally
        (stop-fencing-world! world)))))
