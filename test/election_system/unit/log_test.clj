(ns election-system.unit.log-test
  "The node's logger must be a Duct Logger that emits one JSON line per event, with the
   correlation id of the request that caused it."
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.tracing :as tracing]
            [duct.logger :as logger]
            [election-system.log :as log]))

(defn- lines [s] (->> (clojure.string/split-lines s) (remove clojure.string/blank?)))

(deftest one-json-line-per-event-with-the-correlation-id
  (let [l (log/make-logger {:service "election-api" :instance-id "inst-1"})
        out (with-out-str
              (tracing/with-ctx {:trace-id "abc123456789" :span-id "span-1"}
                (logger/log l :info :http-request {:status 200 :uri "/audit"})))]
    (is (= 1 (count (lines out))))
    (let [line (json/parse-string (first (lines out)) true)]
      (is (= "info" (name (:level line))))
      (is (= "election-api" (:service line)))
      (is (= "abc123456789" (:traceId line)) "the correlation id travels with the log")
      (is (= 200 (:status line)))
      (is (= "/audit" (:uri line))))))

(deftest levels-filter
  (let [l (log/make-logger {:service "s" :level :warn})]
    (is (clojure.string/blank? (with-out-str (logger/log l :info :quiet {}))))
    (is (not (clojure.string/blank? (with-out-str (logger/log l :error :loud {})))))))
