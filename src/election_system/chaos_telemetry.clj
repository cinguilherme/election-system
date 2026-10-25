(ns election-system.chaos-telemetry
  "Turns a chaos firing into visible signals.

   A per-hit log line would flood under load (the actors process ~18k messages/min), so:
   - one `:chaos-engaged` line the first time each rule fires (with rule/component/fault),
   - a `:chaos-engaged-summary` every `window-ms` with counts per component/fault,
   - a `chaos_hits_total` counter for every hit (metrics, not logs).

   The callback runs on the request path: it must never throw."
  (:require [d-core.core.metrics.wrappers :as metrics-wrappers]
            [election-system.log :as log]))

(defn make-telemetry
  [{:keys [logger metrics counter window-ms] :or {window-ms 10000}}]
  (let [state (atom {:announced #{} :hits {} :since (System/currentTimeMillis)})]
    (fn on-hit [{:keys [rule-id component op fault experiment-id]}]
      (try
        (when counter
          ;; Positional, in the counter's label order [:component :fault]. A map here is
          ;; not a sequence of values: it throws inside the guard and the whole callback
          ;; (including the log line) disappears -- which is exactly what happened.
          (metrics-wrappers/inc-labeled! metrics counter
                                         [(str (name (or component :unknown)))
                                          (str (name (or fault :unknown)))]))
        (let [now (System/currentTimeMillis)
              first? (not (contains? (:announced @state) rule-id))
              bucket [(or component :unknown) (or fault :unknown)]]
          (swap! state (fn [st]
                         (-> st
                             (update :announced conj rule-id)
                             (update-in [:hits bucket] (fnil inc 0)))))
          (when first?
            (log/log logger :warn :chaos-engaged
                     {:rule rule-id
                      :component component
                      :op op
                      :fault fault
                      :experiment-id experiment-id}))
          (when (>= (- now (long (:since @state))) (long window-ms))
            (let [hits (:hits @state)]
              (swap! state assoc :hits {} :since now)
              (log/log logger :warn :chaos-engaged-summary
                       {:windowMs window-ms
                        :hits (into {}
                                    (map (fn [[[c f] n]]
                                           [(str (name c) "/" (name f)) n]))
                                    hits)}))))
        (catch Throwable _ nil)))))
