(ns election-system.unit.chaos-telemetry-test
  "Chaos must be visible without flooding: first hit per rule + a periodic summary, and a
   counter for every hit."
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.metrics.wrappers :as metrics-wrappers]
            [duct.logger :as logger]
            [election-system.chaos-telemetry :as telemetry]))

(defn- capturing-logger
  [lines]
  (reify logger/Logger
    (-log [_ level _ns _f _l _id event data]
      (swap! lines conj {:level level :event event :data data}))))

(deftest first-hit-and-summary
  (let [lines (atom [])
        l (capturing-logger lines)
        on-hit (telemetry/make-telemetry {:logger l :window-ms 0})]
    (on-hit {:rule-id :r1 :component :db :op :select :fault :delay})
    (testing "the first hit of a rule is announced"
      (is (= 1 (count (filter #(= :chaos-engaged (:event %)) @lines)))))
    (on-hit {:rule-id :r1 :component :db :op :select :fault :delay})
    (testing "a second hit does not repeat the announcement"
      (is (= 1 (count (filter #(= :chaos-engaged (:event %)) @lines)))))
    (testing "but the window summary reports the count"
      (is (some #(= :chaos-engaged-summary (:event %)) @lines))
      (is (some #(= {"db/delay" 1} (get-in % [:data :hits]))
                (filter #(= :chaos-engaged-summary (:event %)) @lines))))))

(deftest every-hit-increments-the-counter
  (let [calls (atom 0)
        on-hit (telemetry/make-telemetry {:logger nil :metrics nil :counter :fake :window-ms 60000})]
    (with-redefs [metrics-wrappers/inc-labeled! (fn [_ _ _] (swap! calls inc))]
      (on-hit {:rule-id :r1 :component :db :fault :throw})
      (on-hit {:rule-id :r1 :component :db :fault :throw}))
    (is (= 2 @calls))))
