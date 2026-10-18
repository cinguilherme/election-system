(ns election-system.actors.precinct
  "Pure behavior for the `:precinct` entity actor.
   Guarantees:
   - Single-writer serialization per precinct
   - O(1) in-memory duplicate voter prevention (`:voted-ids` set) without DB reads
   - Election window enforcement (`:opens-at` / `:closes-at`) via `d-core.libs.time`
   - Batched ballot and tally persistence effects (`:db/flush-precinct`) + parent delta `:tell`"
  (:require [d-core.libs.time :as time]
            [election-system.domain.election :as domain]))

(defn init-precinct-state
  ([precinct-id]
   (init-precinct-state precinct-id {}))
  ([precinct-id {:keys [election-id opens-at closes-at batch-size tick-min-batch
                        tick-interval-ms tick-max-age-ms tell-interval-ms]}]
   (let [parsed (domain/parse-precinct-actor-id precinct-id election-id)]
     {:precinct-id (:precinct-id parsed)
      :election-id (:election-id parsed)
      :opens-at opens-at
      :closes-at closes-at
      :batch-size (long (or batch-size 50))
      :tick-min-batch (long (or tick-min-batch 0))
      :tick-interval-ms (long (or tick-interval-ms 2000))
      ;; Independent of tick FREQUENCY: how long a partial batch may sit before it
      ;; is written regardless of size. This is the actual staleness bound.
      :tick-max-age-ms (long (or tick-max-age-ms 10000))
      :last-flush-at (System/currentTimeMillis)
      :voted-ids #{}
      :tallies {}
      :total-votes 0
      :rejected-duplicates 0
      :rejected-window 0
      :pending-ballots []
      :pending-deltas {}
      ;; Deltas for the national tally are coalesced: accumulating them and
      ;; telling at most every `tell-interval-ms` cuts the tally actor's inbox
      ;; pressure by an order of magnitude. Those are NATS messages, not DB
      ;; writes -- and since the tally flushes every N tells, fewer tells means
      ;; FEWER, LARGER DB flushes, not more.
      :last-tell-at 0
      :tell-interval-ms (long (or tell-interval-ms 200))
      :dirty? false
      :flushes 0
      :last-sequence nil})))

(defn execute-flush-transition
  "Purely transitions a dirty precinct state into a flushed state and emits the
   batched DB write effect, parent tally delta `:tell` effect, and snapshot effect.

   `write-tally?` controls whether the flush also upserts the full precinct tally
   row. That row is a snapshot of state the ACTOR already owns and serves reads
   from, so writing it on the ingestion hot path was pure overhead: measured at
   exactly 1:1 with ballot batches, and ~52% of all SQL statements under load.
   The batch-triggered flush passes false; explicit/tick flushes still write it
   so the DB retains a periodic tally checkpoint."
  ([state] (execute-flush-transition state true nil false))
  ([state write-tally?] (execute-flush-transition state write-tally? nil false))
  ([state write-tally? fencing] (execute-flush-transition state write-tally? fencing false))
  ([state write-tally? fencing force-tell?]
   (if-not (or (:dirty? state) (seq (:pending-ballots state)) (seq (:pending-deltas state)))
    [state {:status :noop :flushes (:flushes state)} []]
    (let [now (System/currentTimeMillis)
          next-flushes (inc (long (or (:flushes state) 0)))
          ballots (:pending-ballots state)
          deltas (:pending-deltas state)
          ;; Send the accumulated deltas only when the interval has elapsed (or
          ;; when an explicit flush/passivation forces it). Until then they stay
          ;; in `:pending-deltas` and ride along with the next tell.
          tell? (and (seq deltas)
                     (or force-tell?
                         (>= (- now (long (or (:last-tell-at state) 0)))
                             (long (or (:tell-interval-ms state) 200)))))
          election-id (:election-id state)
          precinct-id (:precinct-id state)
          effects (cond-> [(cond-> {:type :db/flush-precinct
                                    :election-id election-id
                                    :precinct-id precinct-id
                                    :ballots ballots
                                    :flushes next-flushes
                                    :last-sequence (:last-sequence state)}
                             ;; Ownership epoch of the delivery that produced
                             ;; this write, so the upsert can refuse to apply
                             ;; over a newer owner. Absent when ownership is not
                             ;; enforced, which keeps the write unguarded.
                             (some? fencing) (assoc :fencing fencing)
                             write-tally?
                             (assoc :tallies (:tallies state)
                                    :total-votes (:total-votes state)))]
                    tell?
                    (conj {:type :tell
                           :entity-type :election-tally
                           :entity-id election-id
                           :message {:type :precinct-delta
                                     :election-id election-id
                                     :precinct-id precinct-id
                                     :deltas deltas
                                     :precinct-total (:total-votes state)
                                     :flushes next-flushes}}))
          flushed-state (assoc state
                               :pending-ballots []
                               :pending-deltas (if tell? {} deltas)
                               :last-tell-at (if tell? now (:last-tell-at state))
                               :dirty? false
                               :flushes next-flushes
                               :last-flush-at (System/currentTimeMillis))]
      [flushed-state
       {:status :flushed
        :precinct-id precinct-id
        :flushed-ballots (count ballots)
        :flushes next-flushes}
       effects]))))

(defn- handle-cast-vote
  [state msg ctx]
  (let [now-inst (or (:instant ctx) (time/now))
        seq-id (:sequence ctx)
        validation (domain/validate-vote-payload
                    (assoc msg
                           :precinct-id (or (:precinct-id msg) (:precinct-id state))
                           :election-id (or (:election-id msg) (:election-id state))))]
    (if-not (:valid? validation)
      [state {:status :rejected :reason :invalid-payload :error (:error validation)} []]
      (let [{:keys [ballot-id election-id precinct-id voter-id candidate-id]} (:vote validation)]
        (cond
          (not (domain/election-open? state now-inst))
          (let [next-state (update state :rejected-window (fnil inc 0))]
            [next-state
             {:status :rejected
              :reason :election-closed
              :precinct-id precinct-id
              :voter-id voter-id}
             []])

          (contains? (:voted-ids state) voter-id)
          (let [next-state (update state :rejected-duplicates (fnil inc 0))]
            [next-state
             {:status :rejected
              :reason :duplicate-vote
              :precinct-id precinct-id
              :voter-id voter-id}
             []])

          :else
          (let [ballot-row {:ballot_id ballot-id
                            :election_id election-id
                            :precinct_id precinct-id
                            :voter_id voter-id
                            :candidate_id (name candidate-id)
                            :cast_at now-inst}
                updated (-> state
                            (assoc :election-id election-id
                                   :dirty? true)
                            (cond-> seq-id (assoc :last-sequence seq-id))
                            (update :voted-ids conj voter-id)
                            (update-in [:tallies candidate-id] (fnil inc 0))
                            (update :total-votes (fnil inc 0))
                            (update :pending-ballots conj ballot-row)
                            (update-in [:pending-deltas candidate-id] (fnil inc 0)))
                batch-size (long (or (:batch-size updated) 50))
                reply {:status :accepted
                       :ballot-id ballot-id
                       :precinct-id precinct-id
                       :voter-id voter-id
                       :candidate-id candidate-id
                       :precinct-total (:total-votes updated)}]
            (if (>= (count (:pending-ballots updated)) batch-size)
              (let [[flushed-state _flush-reply effects]
                    (execute-flush-transition updated false (:fencing ctx))]
                [flushed-state (assoc reply :flushed? true) effects])
              [updated reply []])))))))

(defn tally-view
  "The tally projection of a precinct's state.

   Shared by the step handler and the read lane's direct path, so both answer
   with exactly the same shape -- a divergence here would surface as fields that
   appear or vanish depending on which path served the read."
  [state]
  {:precinct-id (:precinct-id state)
   :election-id (:election-id state)
   :tallies (:tallies state)
   :total-votes (:total-votes state)
   :rejected-duplicates (:rejected-duplicates state)
   :rejected-window (:rejected-window state)
   :pending-ballots-count (count (:pending-ballots state))
   :flushes (:flushes state)
   ;; Eventual-consistency marker: this reply may exclude ballots still in the
   ;; queue. A client can compare last-sequence across polls instead of assuming
   ;; monotonic totals.
   :last-sequence (:last-sequence state)})

(defn precinct-step
  "Pure step function `(fn [state msg ctx] -> [next-state reply effects])` for `:precinct`."
  [state msg ctx]
  (let [msg-type (or (:type msg) (:op msg) :cast-vote)]
    (case msg-type
      :configure-window
      (let [next-state (cond-> state
                         (contains? msg :opens-at) (assoc :opens-at (:opens-at msg))
                         (contains? msg :closes-at) (assoc :closes-at (:closes-at msg))
                         (:election-id msg) (assoc :election-id (str (:election-id msg)))
                         (:batch-size msg) (assoc :batch-size (long (:batch-size msg))))]
        [next-state
         {:status :configured
          :precinct-id (:precinct-id next-state)
          :opens-at (:opens-at next-state)
          :closes-at (:closes-at next-state)
          :batch-size (:batch-size next-state)}
         []])

      :cast-vote
      (handle-cast-vote state msg ctx)

      :flush
      (execute-flush-transition state true (:fencing ctx) true)

      :tick
      ;; The tick used to flush unconditionally every tick-interval-ms, which at
      ;; 250 precincts meant batches of ~6 ballots instead of the configured
      ;; batch-size -- 85k insert statements for 547k ballots (13% of intended
      ;; batch size). Require a minimum pending batch, falling back to flushing
      ;; on age so nothing lingers indefinitely.
      (let [pending (count (:pending-ballots state))
            min-batch (long (or (:tick-min-batch state) 0))
            last-at (long (or (:last-flush-at state) 0))
            age-ms (- (System/currentTimeMillis) last-at)
            max-age (long (or (:tick-max-age-ms state) 10000))]
        (if (or (zero? pending)
                (>= pending min-batch)
                (>= age-ms max-age))
          ;; Ballots only. The tally row is a DB checkpoint for reporting and for
          ;; the no-actor fallback, and this tick is the dominant flush path --
          ;; writing it here put a single-row upsert on the hot path at the same
          ;; rate as ballot batches (measured 241 upserts/s against 242 inserts/s).
          (execute-flush-transition state false (:fencing ctx))
          [state {:status :deferred :pending-ballots pending} []]))

      :get-tally
      [state (tally-view state) []]

      [state {:status :error :error (str "Unknown message type: " msg-type)} []])))

(defn make-precinct-spec
  "Constructs an `EntitySpec` for `:precinct` actors."
  ([]
   (make-precinct-spec {}))
  ([{:keys [election-id opens-at closes-at batch-size tick-interval-ms idle-timeout-ms snapshot-every-n]}]
   (cond-> {:init-fn #(init-precinct-state % {:election-id election-id
                                              :opens-at opens-at
                                              :closes-at closes-at
                                              :batch-size batch-size})
            :step-fn precinct-step
            :idle-timeout-ms (or idle-timeout-ms 300000)
            :snapshot-every-n (or snapshot-every-n 500)}
     tick-interval-ms (assoc :tick-interval-ms tick-interval-ms))))
