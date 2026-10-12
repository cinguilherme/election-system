(ns election-system.unit.actors-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.libs.time :as time]
            [election-system.actors.precinct :as precinct]
            [election-system.actors.tally :as tally]))

(deftest precinct-pure-behavior-and-time-window-test
  (testing "Pure precinct step enforces election window, prevents duplicate votes, and batches flushes"
    (let [open-inst (time/instant "2026-11-03T08:00:00Z")
          close-inst (time/instant "2026-11-03T20:00:00Z")
          clk (time/manual-clock (time/instant "2026-11-03T07:30:00Z"))]
      (time/with-clock clk
        (let [s0 (precinct/init-precinct-state "precinct-01"
                                               {:election-id "gen-2026"
                                                :opens-at open-inst
                                                :closes-at close-inst
                                                :batch-size 2})
              ;; 1. Before polls open -> rejected (:election-closed)
              [s1 r1 eff1] (precinct/precinct-step s0
                                                   {:type :cast-vote
                                                    :voter-id "v-1"
                                                    :candidate-id :alice}
                                                   {:instant (time/now)})]
          (is (= :rejected (:status r1)))
          (is (= :election-closed (:reason r1)))
          (is (= 1 (:rejected-window s1)))
          (is (empty? eff1))

          ;; 2. Advance clock to 08:00:00Z (polls open)
          (time/advance-clock! clk {:minutes 30})
          (let [[s2 r2 eff2] (precinct/precinct-step s1
                                                     {:type :cast-vote
                                                      :voter-id "v-1"
                                                      :candidate-id :alice}
                                                     {:instant (time/now)
                                                      :sequence "seq-1"})]
            (is (= :accepted (:status r2)))
            (is (= 1 (:total-votes s2)))
            (is (= 1 (count (:pending-ballots s2))))
            (is (empty? eff2))

            ;; 3. Duplicate vote by "v-1" -> rejected in O(1) memory
            (let [[s3 r3 eff3] (precinct/precinct-step s2
                                                       {:type :cast-vote
                                                        :voter-id "v-1"
                                                        :candidate-id :bob}
                                                       {:instant (time/now)})]
              (is (= :rejected (:status r3)))
              (is (= :duplicate-vote (:reason r3)))
              (is (= 1 (:rejected-duplicates s3)))
              (is (= 1 (:total-votes s3)))
              (is (empty? eff3))

              ;; 4. Second valid vote ("v-2") hits batch-size = 2 -> triggers immediate flush effects!
              (let [[s4 r4 eff4] (precinct/precinct-step s3
                                                         {:type :cast-vote
                                                          :voter-id "v-2"
                                                          :candidate-id :bob}
                                                         {:instant (time/now)
                                                          :sequence "seq-2"})]
                (is (= :accepted (:status r4)))
                (is (true? (:flushed? r4)))
                (is (= 2 (:total-votes s4)))
                (is (= {:alice 1 :bob 1} (:tallies s4)))
                (is (empty? (:pending-ballots s4)))
                (is (= 3 (count eff4)))
                (is (= :db/flush-precinct (:type (first eff4))))
                (is (= 2 (count (:ballots (first eff4)))))
                (is (= :tell (:type (second eff4))))
                (is (= {:alice 1 :bob 1} (get-in (second eff4) [:message :deltas])))

                ;; 5. Advance clock to 20:00:01Z (polls closed) -> rejected (:election-closed)
                (time/set-clock! clk (time/instant "2026-11-03T20:00:01Z"))
                (let [[s5 r5 _] (precinct/precinct-step s4
                                                        {:type :cast-vote
                                                         :voter-id "v-3"
                                                         :candidate-id :alice}
                                                        {:instant (time/now)})]
                  (is (= :rejected (:status r5)))
                  (is (= :election-closed (:reason r5)))
                  (is (= 2 (:rejected-window s5)))
                  (is (= 2 (:total-votes s5))))))))))))

(deftest national-election-tally-pure-behavior-test
  (testing "Pure national tally step aggregates precinct deltas and flushes on threshold"
    (let [s0 (tally/init-election-tally-state "gen-2026" {:flush-every-n 2})
          [s1 r1 eff1] (tally/tally-step s0
                                         {:type :precinct-delta
                                          :precinct-id "p-1"
                                          :deltas {:alice 30 :bob 20}
                                          :precinct-total 50}
                                         {})
          [s2 r2 eff2] (tally/tally-step s1
                                         {:type :precinct-delta
                                          :precinct-id "p-2"
                                          :deltas {:alice 15 :bob 35}
                                          :precinct-total 50}
                                         {})]
      (is (= :updated (:status r1)))
      (is (empty? eff1))
      (is (= :updated (:status r2)))
      (is (true? (:flushed? r2)))
      (is (= 100 (:total-votes s2)))
      (is (= {:alice 45 :bob 55} (:tallies s2)))
      (is (= {"p-1" 50 "p-2" 50} (:precinct-totals s2)))
      (is (= :db/flush-election-tally (:type (first eff2)))))))

(deftest multi-election-composite-precinct-actor-id-test
  (testing "Composite precinct actor ID isolates election-id and precinct-id and routes deltas to matching election-tally actor"
    (let [s0 (precinct/init-precinct-state "senate-sp-2026:sp-p01" {:batch-size 1})
          [_s1 r1 eff1] (precinct/precinct-step s0
                                                {:type :cast-vote
                                                 :voter-id "v-sp-1"
                                                 :candidate-id :alice}
                                                {:instant (time/now)
                                                 :sequence "seq-1"})]
      (is (= "senate-sp-2026" (:election-id s0)))
      (is (= "sp-p01" (:precinct-id s0)))
      (is (= :accepted (:status r1)))
      (is (true? (:flushed? r1)))
      (is (= {:type :tell
              :entity-type :election-tally
              :entity-id "senate-sp-2026"
              :message {:type :precinct-delta
                        :election-id "senate-sp-2026"
                        :precinct-id "sp-p01"
                        :deltas {:alice 1}
                        :precinct-total 1
                        :flushes 1}}
             (second eff1))))))

