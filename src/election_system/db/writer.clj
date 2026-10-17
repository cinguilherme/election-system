(ns election-system.db.writer
  "Batched SQL persistence writer and audit queries for the Election System.
   Executes multi-row `insert-batch!` for ballots and `upsert!` for precinct/national tallies."
  (:require [cheshire.core :as json]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.core.metrics.tracing :as otel]
            [d-core.core.metrics.wrappers :as wrappers]
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

(defn- execute-with-sql-metrics
  [prom-metrics sql-inst opts f]
  (if (and prom-metrics sql-inst)
    (wrappers/with-sql-metrics prom-metrics sql-inst opts f)
    (f)))

(defn- detect-engine
  [db]
  (let [cls-name (some-> db class .getName)]
    (cond
      (and cls-name (.contains cls-name "PostgresDatabase")) :postgres
      (and cls-name (.contains cls-name "MySQLDatabase")) :mysql
      :else :sqlite)))

(defn- instant-temporal-opts
  [db col]
  (let [stored-as (case (detect-engine db)
                    :postgres "TIMESTAMPTZ"
                    :mysql "DATETIME(6)"
                    "TEXT")]
    {:temporal {col {:domain :instant :stored-as stored-as}}}))

(defn- ensure-instant
  [v]
  (cond
    (nil? v) (time/now)
    (instance? java.time.Instant v) v
    (string? v) (java.time.Instant/parse v)
    (number? v) (java.time.Instant/ofEpochMilli (long v))
    :else v))

(defn- normalize-ballot-row
  [row]
  (if (contains? row :cast_at)
    (update row :cast_at ensure-instant)
    row))

(defn flush-precinct-to-db!
  "Persists a precinct's pending ballot batch via `sql/insert-batch!` and upserts
   the precinct tally watermark via `sql/upsert!`."
  ([db metrics-atom effect]
   (flush-precinct-to-db! db metrics-atom nil nil effect))
  ([db metrics-atom prom-metrics sql-inst
    {:keys [election-id precinct-id ballots tallies total-votes flushes last-sequence]}]
   (let [now-inst (time/now)
         norm-ballots (mapv normalize-ballot-row ballots)
         ballot-count (count norm-ballots)
         ballot-opts (merge {:table :ballots
                             :on-conflict :ignore}
                            (instant-temporal-opts db :cast_at))
         tally-opts (merge {:table :precinct_tallies
                            :conflict-columns [:election_id :precinct_id]}
                           (instant-temporal-opts db :updated_at))]
     (when (seq norm-ballots)
       (execute-with-sql-metrics
        prom-metrics sql-inst
        {:table :ballots :operation :insert_batch :rows ballot-count}
        #(sql/insert-batch! db norm-ballots ballot-opts))
       (record-metric! metrics-atom :batch-inserts-count 1)
       (when metrics-atom
         (swap! metrics-atom update :ballots-persisted-count (fnil + 0) ballot-count)))
     ;; The tally row is a snapshot of state the ACTOR already owns and now serves
     ;; reads from, so the ingestion hot path omits it (`:tallies` absent) and only
     ;; explicit/tick flushes write it. Ballot rows -- the actual immutable record
     ;; -- are still written above on every flush.
     (when (some? tallies)
       (execute-with-sql-metrics
        prom-metrics sql-inst
        {:table :precinct_tallies :operation :upsert :rows 1}
        #(sql/upsert! db
                      {:election_id (str election-id)
                       :precinct_id (str precinct-id)
                       :tallies_json (json/generate-string (or tallies {}))
                       :total_votes (long (or total-votes 0))
                       :flushes (long (or flushes 0))
                       :last_sequence (some-> last-sequence str)
                       :updated_at now-inst}
                      tally-opts))
       (record-metric! metrics-atom :precinct-upserts-count 1)))))

(defn flush-election-tally-to-db!
  "Upserts the national election tally summary row via `sql/upsert!`."
  ([db metrics-atom effect]
   (flush-election-tally-to-db! db metrics-atom nil nil effect))
  ([db metrics-atom prom-metrics sql-inst
    {:keys [election-id tallies precinct-totals total-votes flushes]}]
   (let [now-inst (time/now)
         tally-opts (merge {:table :election_tallies
                            :conflict-columns [:election_id]}
                           (instant-temporal-opts db :updated_at))]
     (execute-with-sql-metrics
      prom-metrics sql-inst
      {:table :election_tallies :operation :upsert :rows 1}
      #(sql/upsert! db
                    {:election_id (str election-id)
                     :tallies_json (json/generate-string (or tallies {}))
                     :precincts_reporting (long (count (or precinct-totals {})))
                     :total_votes (long (or total-votes 0))
                     :flushes (long (or flushes 0))
                     :updated_at now-inst}
                    tally-opts))
     (record-metric! metrics-atom :election-upserts-count 1))))

(defn make-effect-handler
  "Creates an actor `:effect-handler` function that executes `:db/flush-precinct`
   and `:db/flush-election-tally` against `db`."
  ([db]
   (make-effect-handler db nil nil nil))
  ([db metrics-atom logger]
   (make-effect-handler db metrics-atom logger nil))
  ([db metrics-atom logger prom-metrics]
   (let [default-sql-inst (when prom-metrics (wrappers/sql-instruments prom-metrics))]
     (fn [effect ctx]
       (let [eff-metrics (or prom-metrics (get-in ctx [:effect-ctx :metrics]))
             sql-inst (or default-sql-inst
                          (when eff-metrics (wrappers/sql-instruments eff-metrics)))
             eff-type (:type effect)
             ballot-cnt (count (:ballots effect))]
         (otel/with-span [nil "actor.db_flush"
                          {:kind :client
                           :attributes {"db.system" (name (detect-engine db))
                                        "effect.type" (name eff-type)
                                        "ballot.count" ballot-cnt}}]
           (try
             (case eff-type
               :db/flush-precinct
               (flush-precinct-to-db! db metrics-atom eff-metrics sql-inst effect)

               :db/flush-election-tally
               (flush-election-tally-to-db! db metrics-atom eff-metrics sql-inst effect)

               nil)
             (catch Exception e
               (when logger
                 (logger/log logger :error ::db-flush-failed
                             {:effect-type eff-type
                              :error (.getMessage e)}))
               (throw e)))))))))

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
        ballot-rows (sql/select db (merge {:table :ballots
                                           :where {:election_id eid}}
                                          (instant-temporal-opts db :cast_at)))
        precinct-rows (sql/select db (merge {:table :precinct_tallies
                                             :where {:election_id eid}}
                                            (instant-temporal-opts db :updated_at)))
        election-rows (sql/select db (merge {:table :election_tallies
                                             :where {:election_id eid}}
                                            (instant-temporal-opts db :updated_at)))
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
                                          :updated-at (some-> (row-val row :updated_at) str)}]))
                                 precinct-rows)
        election-row (first election-rows)
        election-summary (when election-row
                           {:tallies (json/parse-string (str (row-val election-row :tallies_json)) true)
                            :precincts-reporting (long (or (row-val election-row :precincts_reporting) 0))
                            :total-votes (long (or (row-val election-row :total_votes) 0))
                            :flushes (long (or (row-val election-row :flushes) 0))
                            :updated-at (some-> (row-val election-row :updated_at) str)})]
    {:election-id eid
     :ballot-count (count ballot-rows)
     :ballot-tallies ballot-tallies
     :precinct-tallies precinct-summaries
     :election-tally election-summary}))

(defn query-election-tally-summary
  "Reads the aggregated national election tally summary row from `db` for `election-id`."
  [db election-id]
  (when db
    (try
      (let [eid (str election-id)
            election-rows (sql/select db (merge {:table :election_tallies
                                                 :where {:election_id eid}}
                                                (instant-temporal-opts db :updated_at)))
            row (first election-rows)]
        (when row
          {:election-id eid
           :tallies (if-let [tj (row-val row :tallies_json)]
                      (json/parse-string (str tj) true)
                      {})
           :precincts-reporting (long (or (row-val row :precincts_reporting) 0))
           :total-votes (long (or (row-val row :total_votes) 0))
           :flushes (long (or (row-val row :flushes) 0))
           :updated-at (some-> (row-val row :updated_at) str)}))
      (catch Exception _ nil))))

(defn query-precinct-tally-summary
  "Reads the aggregated precinct tally summary row from `db` for `election-id` and `precinct-id`."
  [db election-id precinct-id]
  (when db
    (try
      (let [eid (str election-id)
            pid (str precinct-id)
            precinct-rows (sql/select db (merge {:table :precinct_tallies
                                                 :where {:election_id eid
                                                         :precinct_id pid}}
                                                (instant-temporal-opts db :updated_at)))
            row (first precinct-rows)]
        (when row
          {:election-id eid
           :precinct-id pid
           :tallies (if-let [tj (row-val row :tallies_json)]
                      (json/parse-string (str tj) true)
                      {})
           :total-votes (long (or (row-val row :total_votes) 0))
           :flushes (long (or (row-val row :flushes) 0))
           :updated-at (some-> (row-val row :updated_at) str)}))
      (catch Exception _ nil))))
