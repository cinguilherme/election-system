(ns election-system.actors.tally
  "Pure behavior for the `:election-tally` parent aggregation actor.
   Receives batched `:precinct-delta` messages from `:precinct` actors,
   maintains live national and per-precinct tallies in memory, and periodically
   flushes national summary rows to the database."
  (:require [cheshire.core :as json]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [election-system.domain.election :as domain]))

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
  ([state] (execute-tally-flush state nil))
  ([state fencing]
  (if-not (:dirty? state)
    [state {:status :noop :flushes (:flushes state)} []]
    (let [next-flushes (inc (long (or (:flushes state) 0)))
          flushed (assoc state :dirty? false :flushes next-flushes)
          effects [(cond-> {:type :db/flush-election-tally
                            :election-id (:election-id flushed)
                            :tallies (:tallies flushed)
                            :precinct-totals (:precinct-totals flushed)
                            :total-votes (:total-votes flushed)
                            :flushes next-flushes}
                     ;; Fencing epoch of the delivery that triggered this flush,
                     ;; so the national tally upsert can refuse a stale write.
                     ;; Absent when ownership is not enforced.
                     (some? fencing) (assoc :fencing fencing))]]
      [flushed
       {:status :flushed
        :election-id (:election-id flushed)
        :total-votes (:total-votes flushed)
        :flushes next-flushes}
       effects]))))

(defn tally-view
  "The read projection of a tally actor's state. Shared by the step handler and
   the read lane's direct path so both answer identically."
  [state]
  {:election-id (:election-id state)
   :tallies (:tallies state)
   :precinct-totals (:precinct-totals state)
   :precincts-reporting (count (:precinct-totals state))
   :total-votes (:total-votes state)
   :updates-received (:updates-received state)
   :flushes (:flushes state)})

(defn tally-step
  "Pure step function `(fn [state msg ctx] -> [next-state reply effects])` for `:election-tally`."
  [state msg ctx]
  (let [msg-type (or (:type msg) (:op msg))]
    (case msg-type
      :precinct-delta
      (let [deltas (:deltas msg)
            precinct-id (str (:precinct-id msg))
            precinct-total (long (or (:precinct-total msg) 0))
            prev-precinct-total (long (get-in state [:precinct-totals precinct-id] 0))
            ;; When hydrating from `ballots` on cold start or receiving a redelivered
            ;; `:precinct-delta`, `state` may already include this precinct's votes up
            ;; to `prev-precinct-total`. Guard against double-counting.
            already-counted? (and (pos? precinct-total)
                                  (<= precinct-total prev-precinct-total))
            new-tallies (if already-counted?
                          (:tallies state)
                          (domain/apply-deltas (:tallies state) deltas))
            new-total (domain/compute-total-votes new-tallies)
            new-updates (inc (long (or (:updates-received state) 0)))
            updated (-> state
                        (assoc :tallies new-tallies
                               :total-votes new-total
                               :updates-received new-updates
                               :dirty? true)
                        (assoc-in [:precinct-totals precinct-id]
                                  (max prev-precinct-total precinct-total)))
            flush-every (long (or (:flush-every-n updated) 10))]
        (if (zero? (mod new-updates flush-every))
          (let [[flushed-state _r effects] (execute-tally-flush updated (:fencing ctx))]
            [flushed-state {:status :updated :total-votes new-total :flushed? true} effects])
          [updated {:status :updated :total-votes new-total} []]))

      (:tick :flush)
      (execute-tally-flush state (:fencing ctx))

      :get-tally
      [state (tally-view state) []]

      [state {:status :error :error (str "Unknown message type: " msg-type)} []])))

(defn- row-val [row k]
  (or (get row k)
      (some (fn [[rk rv]]
              (when (= (name rk) (name k))
                rv))
            row)))

(defn hydrate-election-tally-state
  "Hydrates an `:election-tally` actor's state from durable `ballots` and
   `election_tallies` rows in SQL when `:db` is present in `effect-ctx` and SQL
   holds votes not yet reflected in `base-state` (e.g. after a crash before
   flushing or checkpointing)."
  [_entity-id base-state {:keys [db]}]
  (if-not db
    base-state
    (try
      (let [eid (str (:election-id base-state))
            ballot-rows (sql/select db {:table :ballots
                                        :where {:election_id eid}})
            tally-row (first (sql/select db {:table :election_tallies
                                             :where {:election_id eid}}))
            ballot-count (count ballot-rows)
            row-total (long (or (some-> tally-row (row-val :total_votes)) 0))
            base-total (long (or (:total-votes base-state) 0))]
        (cond
          (and (pos? ballot-count) (>= ballot-count row-total) (> ballot-count base-total))
          (let [tallies (reduce (fn [acc row]
                                  (let [cand (keyword (row-val row :candidate_id))]
                                    (update acc cand (fnil inc 0))))
                                {}
                                ballot-rows)
                by-precinct (into {}
                                  (map (fn [[pid rs]] [(str pid) (count rs)]))
                                  (group-by #(row-val % :precinct_id) ballot-rows))]
            (assoc base-state
                   :tallies tallies
                   :precinct-totals by-precinct
                   :total-votes ballot-count
                   :flushes (max (long (or (:flushes base-state) 0))
                                 (long (or (some-> tally-row (row-val :flushes)) 0)))
                   :dirty? (> ballot-count row-total)))

          (and tally-row (> row-total base-total))
          (let [tallies (if-let [raw (row-val tally-row :tallies_json)]
                          (json/parse-string (str raw) true)
                          {})]
            (assoc base-state
                   :tallies tallies
                   :total-votes row-total
                   :flushes (long (or (row-val tally-row :flushes) 0))))

          :else base-state))
      (catch Exception _
        base-state))))

(defn make-election-tally-spec
  "Constructs an `EntitySpec` for `:election-tally` actors."
  ([]
   (make-election-tally-spec {}))
  ([{:keys [flush-every-n tick-interval-ms idle-timeout-ms snapshot-every-n]}]
   (cond-> {:init-fn #(init-election-tally-state % {:flush-every-n flush-every-n})
            :hydrate-fn hydrate-election-tally-state
            :step-fn tally-step
            :idle-timeout-ms (or idle-timeout-ms 600000)
            :snapshot-every-n (or snapshot-every-n 100)}
     tick-interval-ms (assoc :tick-interval-ms tick-interval-ms))))
