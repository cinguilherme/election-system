(ns election-system.unit.flush-policy-test
  "Pins the precinct flush policy.

   Why this exists: under load the flush path was issuing 85,005 ballot-batch
   statements for 547,220 ballots -- 6.44 ballots per statement against a
   configured batch-size of 50, i.e. 13% of intended size -- and an equal number
   of full-state `precinct_tallies` upserts (exactly 1:1 with ballot batches,
   ~52% of all SQL statements). Both were policy choices, not requirements:

     - the tally row is a snapshot of state the actor already owns and serves
       reads from, so it no longer belongs on the ingestion hot path;
     - the 2s tick flushed unconditionally, so it manufactured ~6-ballot batches
       before the batch-size threshold was ever reachable.

   These tests lock both fixes in place."
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.libs.time :as time]
            [election-system.actors.precinct :as precinct]))

(def ^:private open-inst (time/instant "2026-11-03T08:00:00Z"))
(def ^:private close-inst (time/instant "2026-11-03T20:00:00Z"))

(defn- state [opts]
  (let [clk (time/manual-clock (time/instant "2026-11-03T09:00:00Z"))]
    (time/with-clock clk
      (precinct/init-precinct-state "p-01"
                                    (merge {:election-id "gen-2026"
                                            :opens-at open-inst
                                            :closes-at close-inst}
                                           opts)))))

(defn- cast! [s voter]
  (let [clk (time/manual-clock (time/instant "2026-11-03T09:00:00Z"))]
    (time/with-clock clk
      (precinct/precinct-step s {:type :cast-vote :voter-id voter :candidate-id :alice}
                              {:instant (time/now)}))))

(defn- db-effect [effects]
  (first (filter #(= :db/flush-precinct (:type %)) effects)))

(deftest batch-flush-writes-ballots-but-not-the-tally-row
  (testing "reaching batch-size persists ballots without rewriting the tally snapshot"
    (let [s0 (state {:batch-size 2})
          [s1 _ _] (cast! s0 "v-1")
          [s2 r2 eff2] (cast! s1 "v-2")
          eff (db-effect eff2)]
      (is (true? (:flushed? r2)) "second vote should have tripped the batch threshold")
      (is (some? eff) "a DB flush effect is still emitted")
      (is (= 2 (count (:ballots eff))) "ballots are still persisted")
      (is (not (contains? eff :tallies))
          "the hot path must NOT write the full precinct tally row")
      (is (not (contains? eff :total-votes)))
      (is (empty? (:pending-ballots s2)) "pending ballots are cleared after flush"))))

(deftest explicit-flush-still-writes-the-tally-checkpoint
  (testing ":flush writes the tally row so the DB keeps a periodic checkpoint"
    (let [s0 (state {:batch-size 1000})
          [s1 _ _] (cast! s0 "v-1")
          ;; one call: the transition is pure, and s1 is left unflushed
          [_s2 _r effs] (precinct/execute-flush-transition s1)
          eff (db-effect effs)]
      (is (some? eff))
      (is (contains? eff :tallies) "explicit flush writes the tally")
      (is (contains? eff :total-votes))
      (is (= 1 (count (:ballots eff)))))))

(deftest tick-defers-small-batches-and-flushes-on-age
  (testing "a tick with a small pending batch does not manufacture a tiny write"
    (let [s0 (state {:batch-size 200 :tick-min-batch 200 :tick-max-age-ms 10000})
          [s1 _ _] (cast! s0 "v-1")
          ;; fresh last-flush-at => small batch, not yet old => defer
          [s2 r2 eff2] (precinct/precinct-step s1 {:type :tick} {})]
      (is (= :deferred (:status r2)))
      (is (empty? eff2) "no DB write for a small, fresh batch")
      (is (= 1 (count (:pending-ballots s2))) "ballots stay pending for the next flush")))

  (testing "the same small batch is written once it exceeds the staleness bound"
    (let [s0 (state {:batch-size 200 :tick-min-batch 200 :tick-max-age-ms 10000})
          [s1 _ _] (cast! s0 "v-1")
          stale (assoc s1 :last-flush-at (- (System/currentTimeMillis) 60000))
          [_ r eff] (precinct/precinct-step stale {:type :tick} {})]
      (is (= :flushed (:status r)))
      (is (some? (db-effect eff)) "age forces the write")))

  (testing "a tick flushes immediately once the minimum batch is present"
    (let [s0 (state {:batch-size 1000 :tick-min-batch 2 :tick-max-age-ms 10000})
          [s1 _ _] (cast! s0 "v-1")
          [s2 _ _] (cast! s1 "v-2")
          [_ r eff] (precinct/precinct-step s2 {:type :tick} {})]
      (is (= :flushed (:status r)))
      (is (some? (db-effect eff))))))
