(ns election-system.log
  "The node's logger: a Duct `Logger` that prints one JSON line per event.

   Duct is the ecosystem's logging abstraction (`duct.logger/log`), so components keep
   receiving a logger rather than a bespoke function. JSON lines are what Loki parses
   without a regex, and the correlation id is read from `d-core.tracing/*ctx*` at call
   time -- so every line is joinable to the HTTP request and the traces that caused it."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [d-core.tracing :as tracing]
            [duct.logger :as logger]))

(def ^:private level-rank
  {:debug 0 :info 1 :warn 2 :error 3 :fatal 4 :report 5})

(defn- rank [level]
  (get level-rank level 1))

(defn- ctx-fields
  []
  (when-let [ctx tracing/*ctx*]
    (cond-> {:traceId (:trace-id ctx) :spanId (:span-id ctx)}
      (:cid ctx) (assoc :cid (:cid ctx))
      (:parent-span-id ctx) (assoc :parentSpanId (:parent-span-id ctx)))))

(defn- safe-json
  [m]
  (try
    (json/generate-string m)
    (catch Exception _
      (json/generate-string (update m :data #(pr-str (or % {})))))))

(defn make-logger
  "Options: `:service`, `:instance-id`, `:level` (default :info)."
  [{:keys [service instance-id level] :or {level :info}}]
  (let [threshold (rank level)]
    (reify logger/Logger
      (-log [_ lvl ns-str _file _line _id event data]
        (when (>= (rank lvl) threshold)
          (println (safe-json
                    (merge {:ts (str (java.time.Instant/ofEpochMilli (System/currentTimeMillis)))
                            :level (name lvl)
                            :service service
                            :instance instance-id
                            :event (str event)}
                           (ctx-fields)
                           (when ns-str {:ns ns-str})
                           (when (map? data) data)))))))))

(defn log
  "Logs through a Duck logger, tolerating nil (tests, tools) and plain functions."
  [logger level event data]
  (try
    (if (ifn? logger)
      (logger level event data)
      (logger/log logger level event data))
    (catch Exception _ nil)))
