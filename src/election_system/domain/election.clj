(ns election-system.domain.election
  "Pure domain functions for election window enforcement, ballot validation,
   and candidate tally aggregation."
  (:require [clojure.string :as str]
            [d-core.libs.time :as time]))

(defn election-open?
  "Pure function: checks whether `now-instant` falls within `[opens-at, closes-at]`
   (inclusive of `opens-at`, exclusive of `closes-at`).
   If `:opens-at` or `:closes-at` is nil, that boundary is unbounded."
  [{:keys [opens-at closes-at]} now-instant]
  (let [now-i (time/instant (or now-instant (time/now)))
        open-i (when opens-at (time/instant opens-at))
        close-i (when closes-at (time/instant closes-at))
        after-open? (or (nil? open-i) (not (time/before? now-i open-i)))
        before-close? (or (nil? close-i) (time/before? now-i close-i))]
    (and after-open? before-close?)))

(defn validate-vote-payload
  "Validates a vote request map. Returns `{:valid? true :vote ...}` or `{:valid? false :error ...}`."
  [payload]
  (let [election-id (or (:election-id payload) (:election_id payload) "national-2026")
        precinct-id (or (:precinct-id payload) (:precinct_id payload))
        voter-id (or (:voter-id payload) (:voter_id payload))
        candidate-id (or (:candidate-id payload) (:candidate_id payload))
        ballot-id (or (:ballot-id payload) (:ballot_id payload) (str (random-uuid)))]
    (cond
      (or (nil? precinct-id) (str/blank? (str precinct-id)))
      {:valid? false :error "Missing precinct-id"}

      (or (nil? voter-id) (str/blank? (str voter-id)))
      {:valid? false :error "Missing voter-id"}

      (or (nil? candidate-id) (str/blank? (str candidate-id)))
      {:valid? false :error "Missing candidate-id"}

      :else
      {:valid? true
       :vote {:ballot-id (str ballot-id)
              :election-id (str election-id)
              :precinct-id (str precinct-id)
              :voter-id (str voter-id)
              :candidate-id (keyword candidate-id)}})))

(defn apply-deltas
  "Purely merges a map of `{candidate-id delta-count}` into `tallies`."
  [tallies deltas]
  (reduce-kv (fn [acc candidate-id delta]
               (update acc (keyword candidate-id) (fnil + 0) (long delta)))
             (or tallies {})
             (or deltas {})))

(defn compute-total-votes
  "Sums all vote counts across candidates in `tallies`."
  ^long [tallies]
  (reduce-kv (fn [^long sum _k v]
               (+ sum (long v)))
             0
             (or tallies {})))

(defn precinct-actor-id
  "Constructs the actor entity-id for a `(election-id, precinct-id)` pair.
   For `\"national-2026\"` (or when `precinct-id` is already composite), preserves
   `precinct-id` directly for backward compatibility; otherwise returns `\"<election-id>:<precinct-id>\"`."
  [election-id precinct-id]
  (let [eid (str (or election-id "national-2026"))
        pid (str precinct-id)]
    (if (or (= "national-2026" eid)
            (str/includes? pid ":"))
      pid
      (str eid ":" pid))))

(defn parse-precinct-actor-id
  "Parses a precinct actor entity-id into `{:election-id ... :precinct-id ...}`."
  [actor-id default-election-id]
  (let [s (str actor-id)
        idx (str/index-of s ":")]
    (if (and idx (pos? idx))
      {:election-id (subs s 0 idx)
       :precinct-id (subs s (inc idx))}
      {:election-id (str (or default-election-id "national-2026"))
       :precinct-id s})))
