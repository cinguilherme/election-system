(ns election-system.db.writer
  "Batched SQL persistence writer and audit queries for the Election System.
   Executes multi-row `insert-batch!` for ballots and `upsert!` for precinct/national tallies."
  (:require [cheshire.core :as json]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.libs.time :as time]
            [duct.logger :as logger]))

(defn make-write-metrics
  "Creates an atom tracking batched SQL statement execution statistics."
  []
  (atom {:batch-inserts-count 0
         :ballots-persisted-count 0
         :precinct-upserts-count 0
         :election-upserts-count 0
         :total-sql-statements 0}))

(defn- record-metric!
  [metrics-atom k delta]
  (when metrics-atom
    (swap! metrics-atom
           (fn [m]
             (-> m
                 (update k (fnil + 0) delta)
                 (update :total-sql-statements (fnil inc 0)))))))

(defn flush-precinct-to-db!
  "Persists a precinct's pending ballot batch via `sql/insert-batch!` and upserts
   the precinct tally watermark via `sql/upsert!`."
  [db metrics-atom {:keys [election-id precinct-id ballots tallies total-votes flushes last-sequence]}]
  (let [now-inst (time/now)]
    (when (seq ballots)
      (sql/insert-batch! db ballots {:table :ballots
                                     :on-conflict :ignore})
      (record-metric! metrics-atom :batch-inserts-count 1)
      (when metrics-atom
        (swap! metrics-atom update :ballots-persisted-count (fnil + 0) (count ballots))))
    (sql/upsert! db
                 {:election_id (str election-id)
                  :precinct_id (str precinct-id)
                  :tallies_json (json/generate-string (or tallies {}))
                  :total_votes (long (or total-votes 0))
                  :flushes (long (or flushes 0))
                  :last_sequence (some-> last-sequence str)
                  :updated_at now-inst}
                 {:table :precinct_tallies
                  :conflict-columns [:election_id :precinct_id]})
    (record-metric! metrics-atom :precinct-upserts-count 1)))

(defn flush-election-tally-to-db!
  "Upserts the national election tally summary row via `sql/upsert!`."
  [db metrics-atom {:keys [election-id tallies precinct-totals total-votes flushes]}]
  (let [now-inst (time/now)]
    (sql/upsert! db
                 {:election_id (str election-id)
                  :tallies_json (json/generate-string (or tallies {}))
                  :precincts_reporting (long (count (or precinct-totals {})))
                  :total_votes (long (or total-votes 0))
                  :flushes (long (or flushes 0))
                  :updated_at now-inst}
                 {:table :election_tallies
                  :conflict-columns [:election_id]})
    (record-metric! metrics-atom :election-upserts-count 1)))

(defn make-effect-handler
  "Creates an actor `:effect-handler` function that executes `:db/flush-precinct`
   and `:db/flush-election-tally` against `db`."
  ([db]
   (make-effect-handler db nil nil))
  ([db metrics-atom logger]
   (fn [effect _ctx]
     (try
       (case (:type effect)
         :db/flush-precinct
         (flush-precinct-to-db! db metrics-atom effect)

         :db/flush-election-tally
         (flush-election-tally-to-db! db metrics-atom effect)

         nil)
       (catch Exception e
         (when logger
           (logger/log logger :error ::db-flush-failed
                       {:effect-type (:type effect)
                        :error (.getMessage e)}))
         (throw e))))))

(defn- row-val [row k]
  (or (get row k)
      (some (fn [[rk rv]]
              (when (= (name rk) (name k))
                rv))
            row)))

(defn query-db-audit
  "Reads persisted ballots, precinct tallies, and national election tallies from `db`
   for `election-id` to verify consistency against live actor state."
  [db election-id]
  (let [eid (str election-id)
        ballot-rows (sql/select db {:table :ballots
                                    :where {:election_id eid}})
        precinct-rows (sql/select db {:table :precinct_tallies
                                      :where {:election_id eid}})
        election-rows (sql/select db {:table :election_tallies
                                      :where {:election_id eid}})
        ballot-tallies (reduce (fn [acc row]
                                 (let [cand (keyword (row-val row :candidate_id))]
                                   (update acc cand (fnil inc 0))))
                               {}
                               ballot-rows)
        precinct-summaries (into {}
                                 (map (fn [row]
                                        [(str (row-val row :precinct_id))
                                         {:tallies (json/parse-string (str (row-val row :tallies_json)) true)
                                          :total-votes (long (or (row-val row :total_votes) 0))
                                          :flushes (long (or (row-val row :flushes) 0))
                                          :updated-at (row-val row :updated_at)}]))
                                 precinct-rows)
        election-row (first election-rows)
        election-summary (when election-row
                           {:tallies (json/parse-string (str (row-val election-row :tallies_json)) true)
                            :precincts-reporting (long (or (row-val election-row :precincts_reporting) 0))
                            :total-votes (long (or (row-val election-row :total_votes) 0))
                            :flushes (long (or (row-val election-row :flushes) 0))
                            :updated-at (row-val election-row :updated_at)})]
    {:election-id eid
     :ballot-count (count ballot-rows)
     :ballot-tallies ballot-tallies
     :precinct-tallies precinct-summaries
     :election-tally election-summary}))
