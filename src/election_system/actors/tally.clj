(ns election-system.actors.tally
  "Pure behavior for the `:election-tally` parent aggregation actor.
   Receives batched `:precinct-delta` messages from `:precinct` actors,
   maintains live national and per-precinct tallies in memory, and periodically
   flushes national summary rows to the database."
  (:require [election-system.domain.election :as domain]))

(defn init-election-tally-state
  ([election-id]
   (init-election-tally-state election-id {}))
  ([election-id {:keys [flush-every-n]}]
   {:election-id (str (or election-id "national-2026"))
    :tallies {}
    :precinct-totals {}
    :total-votes 0
    :updates-received 0
    :flush-every-n (long (or flush-every-n 10))
    :dirty? false
    :flushes 0}))

(defn execute-tally-flush
  [state]
  (if-not (:dirty? state)
    [state {:status :noop :flushes (:flushes state)} []]
    (let [next-flushes (inc (long (or (:flushes state) 0)))
          flushed (assoc state :dirty? false :flushes next-flushes)
          effects [{:type :db/flush-election-tally
                    :election-id (:election-id flushed)
                    :tallies (:tallies flushed)
                    :precinct-totals (:precinct-totals flushed)
                    :total-votes (:total-votes flushed)
                    :flushes next-flushes}
                   {:type :snapshot}]]
      [flushed
       {:status :flushed
        :election-id (:election-id flushed)
        :total-votes (:total-votes flushed)
        :flushes next-flushes}
       effects])))

(defn tally-step
  "Pure step function `(fn [state msg ctx] -> [next-state reply effects])` for `:election-tally`."
  [state msg _ctx]
  (let [msg-type (or (:type msg) (:op msg))]
    (case msg-type
      :precinct-delta
      (let [deltas (:deltas msg)
            precinct-id (str (:precinct-id msg))
            precinct-total (long (or (:precinct-total msg) 0))
            new-tallies (domain/apply-deltas (:tallies state) deltas)
            new-total (domain/compute-total-votes new-tallies)
            new-updates (inc (long (or (:updates-received state) 0)))
            updated (-> state
                        (assoc :tallies new-tallies
                               :total-votes new-total
                               :updates-received new-updates
                               :dirty? true)
                        (assoc-in [:precinct-totals precinct-id] precinct-total))
            flush-every (long (or (:flush-every-n updated) 10))]
        (if (zero? (mod new-updates flush-every))
          (let [[flushed-state _r effects] (execute-tally-flush updated)]
            [flushed-state {:status :updated :total-votes new-total :flushed? true} effects])
          [updated {:status :updated :total-votes new-total} []]))

      (:tick :flush)
      (execute-tally-flush state)

      :get-tally
      [state
       {:election-id (:election-id state)
        :tallies (:tallies state)
        :precinct-totals (:precinct-totals state)
        :precincts-reporting (count (:precinct-totals state))
        :total-votes (:total-votes state)
        :updates-received (:updates-received state)
        :flushes (:flushes state)}
       []]

      [state {:status :error :error (str "Unknown message type: " msg-type)} []])))

(defn make-election-tally-spec
  "Constructs an `EntitySpec` for `:election-tally` actors."
  ([]
   (make-election-tally-spec {}))
  ([{:keys [flush-every-n tick-interval-ms idle-timeout-ms snapshot-every-n]}]
   (cond-> {:init-fn #(init-election-tally-state % {:flush-every-n flush-every-n})
            :step-fn tally-step
            :idle-timeout-ms (or idle-timeout-ms 600000)
            :snapshot-every-n (or snapshot-every-n 100)}
     tick-interval-ms (assoc :tick-interval-ms tick-interval-ms))))
