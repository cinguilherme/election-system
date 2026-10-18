(ns election-system.ownership
  "Who owns which partition, and the lease that guards it.

  Two jobs, deliberately separated:

  - **Assignment** is a pure function of `(instance-id, members, total-partitions)`.
    Rendezvous hashing, so adding or removing an instance moves only its own
    shares. The application decides this; the proxy never does.
  - **Ownership** is a lease per partition. It does not decide assignment, it
    *guards* it: the proxy refuses a subscriber whose token is not the current
    holder, and stamps the resulting epoch on every delivery so a former owner
    cannot overwrite the new owner's state.

  The runtime lifecycle (start/stop/drain a partition) is injected, so this
  namespace can be tested with fakes and the real wiring stays in `system`."
  (:require [clojure.string :as str]
            [d-core.core.leader-election.protocol :as lease]
            [duct.logger :as logger]))

;; ---------------------------------------------------------------------------
;; Assignment
;; ---------------------------------------------------------------------------

(defn election-id
  "One lease per partition, not per topic.

   A partition is the unit of actor placement: a precinct actor and its tally
   actor on the same shard must be owned by the same instance, so they share an
   ownership epoch. Per-topic leases would allow the two to diverge, which the
   actor model cannot represent."
  [group partition]
  (str group "/p" (long partition)))

(defn- score
  "Stable across processes: Murmur3 over id/partition, never `hash` of a map or
   an object identity."
  ^long [instance-id partition]
  (long (clojure.lang.Murmur3/hashUnencodedChars
         (str instance-id "/" (long partition)))))

(defn owner-of
  "Rendezvous (highest-random-weight) owner of `partition` among `members`.

   Ties break on the instance id so every process computes the same answer."
  [partition members]
  (when (seq members)
    (->> members
         (sort-by (fn [id] [(score id partition) (str id)]))
         last
         str)))

(defn assigned-partitions
  "The partitions `instance-id` should own, given the full member list.

   Returns [] when the instance is not in `members` — it owns nothing, which is
   the correct answer for a pod that started before the membership was updated."
  [instance-id members partitions]
  (let [members (->> members (map str) (remove str/blank?) set vec)
        instance-id (str instance-id)]
    (if-not (contains? (set members) instance-id)
      []
      (->> (range (long partitions))
           (filter #(= instance-id (owner-of % members)))
           vec))))

(defn partition-load
  "Partitions per member, for logging and for spotting a skewed assignment."
  [members partitions]
  (let [members (->> members (map str) (remove str/blank?) vec)]
    (into {} (map (fn [id] [id (count (assigned-partitions id members partitions))]) members))))

;; ---------------------------------------------------------------------------
;; Lease lifecycle
;; ---------------------------------------------------------------------------

(defn- log!
  "Accepts either a duct logger or a plain `(fn [level event data])`.

   The fn form exists because a node may not have a logger component; passing nil
   instead made every ownership event invisible, which is how a silently expiring
   lease went unnoticed."
  [logger level event data]
  (when logger
    (try
      (if (ifn? logger)
        (logger level event data)
        (logger/log logger level event data))
      (catch Throwable _ nil))))

(defn make-manager
  "Builds the per-partition ownership manager.

  - `:lease`               anything satisfying `LeaderElectionProtocol`
  - `:group`               lease namespace, e.g. \"actors\"
  - `:instance-id`         this instance
  - `:members`             all instance ids (rendezvous input)
  - `:partitions`          total partition count
  - `:lease-ttl-ms`        lease TTL; renew runs at a third of it
  - `:start-partition!`    `(fn [{:keys [partition lease-id token fencing]}] -> handle)`
  - `:stop-partition!`     `(fn [handle])`
  - `:drain-partition!`    `(fn [handle] -> truthy when drained within budget)`
  - `:members-fn`          `(fn [] [instance-id ...])`, read on every tick. When
                           absent, `:members` is a fixed list (tests, single node).
  - `:on-state!`           `(fn [snapshot])`, called after every ownership change
                           so a caller can publish gauges.
  - `:max-partitions`      cap per instance. nil = pure rendezvous (no rebalancing).
  - `:grace-ms`            how long an instance may exceed the cap before it forgoes
                           the excess. A lone survivor may hold everything during
                           this window rather than leave shards unowned.
  - `:release-hold-off-ms` how long a released partition stays unclaimable, so it
                           does not bounce straight back to the releaser."
  [{:keys [lease group instance-id members members-fn partitions lease-ttl-ms
           start-partition! stop-partition! drain-partition! logger on-state! leave-fn
           heartbeat-fn max-partitions grace-ms release-hold-off-ms]
    :or {lease-ttl-ms 15000}}]
  (let [;; An empty read must fall through: a node that starts before any
        ;; heartbeat exists still owns everything until it learns about peers.
        initial (->> (or (seq (when members-fn (members-fn))) (seq members) [instance-id])
                     (map str)
                     vec)]
    {:lease lease
     :group (str group)
     :instance-id (str instance-id)
     ;; Live membership: a dead node's heartbeat expires and the survivors
     ;; recompute. Static config cannot express "this node is gone".
     :members-atom (atom initial)
     :members-fn members-fn
     :partitions (long partitions)
     :lease-ttl-ms (long lease-ttl-ms)
     :start-partition! start-partition!
     :stop-partition! stop-partition!
     :drain-partition! drain-partition!
     :logger logger
     :on-state! on-state!
     :leave-fn leave-fn
     :max-partitions (when max-partitions (long max-partitions))
     :grace-ms (long (or grace-ms 60000))
     :release-hold-off-ms (long (or release-hold-off-ms 3000))
     :over-cap-since (atom nil)
     :heartbeat-fn heartbeat-fn
     ;; partition -> {:state :vacant|:active :token :fencing :handle}
     :state (atom {})
     :renewer (atom nil)
     :stopping? (atom false)}))

(defn- members-of
  [manager]
  @(:members-atom manager))

(defn- refresh-members!
  "Re-reads live membership.

   Guards against a store hiccup returning an empty set: honouring that would
   make this node release every partition it owns, which is a far worse outcome
   than acting on a stale list for one tick."
  [manager]
  (if-let [f (:members-fn manager)]
    (let [live (try (->> (f) (map str) (remove str/blank?) vec)
                    (catch Exception e
                      (log! (:logger manager) :warn ::membership-read-failed
                            {:error (.getMessage e)})
                      nil))]
      (cond
        (nil? live) (members-of manager)
        (empty? live)
        (let [kept (members-of manager)]
          (log! (:logger manager) :warn ::membership-empty {:kept kept})
          (if (seq kept)
            kept
            ;; Nothing at all yet: assume we are alone rather than assigning
            ;; ourselves no work. The next beat corrects it.
            (do (reset! (:members-atom manager) [(:instance-id manager)])
                [(:instance-id manager)])))
        (not-any? #{(:instance-id manager)} live)
        ;; Our heartbeat is missing. Honouring that means releasing everything —
        ;; correct if we really fell out, but it also happens for a beat if the
        ;; store hiccuped on our own key, so say so loudly.
        (do (log! (:logger manager) :warn ::membership-lost-self
                  {:live live})
            (reset! (:members-atom manager) live)
            live)
        :else (do (reset! (:members-atom manager) live) live)))
    (members-of manager)))

(declare snapshot heartbeat!)

(defn- notify-state!
  [manager]
  (when-let [f (:on-state! manager)]
    (try (f (snapshot manager)) (catch Throwable _ nil))))

(defn- put-state!
  [manager partition entry]
  (swap! (:state manager) assoc partition entry))

(defn- entry
  [manager partition]
  (get @(:state manager) partition))

(defn- claim!
  "Tries to take the lease for one partition. Returns the new entry."
  [manager partition]
  (let [id (election-id (:group manager) partition)
        result (try
                 (lease/acquire! (:lease manager) id {:lease-ms (:lease-ttl-ms manager)})
                 (catch Exception e
                   (log! (:logger manager) :error ::claim-failed
                         {:partition partition :lease-id id :error (.getMessage e)})
                   nil))]
    (cond
      (nil? result)
      {:state :vacant :error :lease-unavailable}

      (= :acquired (:status result))
      (let [handle (try
                     ((:start-partition! manager)
                      {:partition partition
                       :lease-id id
                       :token (:token result)
                       :fencing (:fencing result)})
                     (catch Exception e
                       ;; We hold a lease we cannot use. Give it back immediately
                       ;; rather than blocking the partition for a whole TTL.
                       (log! (:logger manager) :error ::partition-start-failed
                             {:partition partition :error (.getMessage e)})
                       (try (lease/resign! (:lease manager) id (:token result) nil)
                            (catch Exception _ nil))
                       ::start-failed))]
        (if (= ::start-failed handle)
          {:state :vacant :error :start-failed}
          (do
            (log! (:logger manager) :report ::partition-claimed
                  {:partition partition :lease-id id :fencing (:fencing result)})
            {:state :active
             :lease-id id
             :token (:token result)
             :fencing (:fencing result)
             :handle handle})))

      :else
      (do
        (log! (:logger manager) :debug ::partition-held-elsewhere
              {:partition partition :lease-id id :holder (:owner-id result)})
        {:state :vacant :error :held-elsewhere}))))

(defn start!
  "Claims every partition this instance is assigned. Busy partitions are left
   vacant and retried by the renewal loop — a peer may still be draining them."
  [manager]
  ;; Be visible before asking who else is: a node that reads membership before
  ;; publishing its own heartbeat sees an empty set and assigns itself nothing.
  (heartbeat! manager)
  (refresh-members! manager)
  (let [members (members-of manager)
        assigned (assigned-partitions (:instance-id manager)
                                      members
                                      (:partitions manager))]
    (log! (:logger manager) :report ::ownership-starting
          {:instance-id (:instance-id manager)
           :members members
           :assigned assigned
           :load (partition-load members (:partitions manager))})
    ;; Claim at most `cap`. Without this a node whose rendezvous share exceeds
    ;; the cap takes everything and then has to forgo the excess -- a pointless
    ;; acquire/release cycle that also arms hold-offs on leases it never needed.
    (let [cap (:max-partitions manager)
          to-claim (if cap (take cap assigned) assigned)]
      (doseq [p to-claim]
        (put-state! manager p (claim! manager p))))
    (notify-state! manager)
    (deref (:state manager))))

(defn- evict!
  "Stop consuming and drop the lease entry without draining.

   Called when the lease is gone: at that point the partition is not ours to
   finish, and draining would only delay the new owner."
  [manager partition reason]
  (let [{:keys [handle lease-id token]} (entry manager partition)]
    (when handle
      (try ((:stop-partition! manager) handle)
           (catch Exception e
             (log! (:logger manager) :error ::evict-stop-failed
                   {:partition partition :error (.getMessage e)}))))
    (when (and lease-id token)
      ;; No hold-off here: an eviction means the lease is already gone, and
      ;; arming a marker would only delay the partition being picked up.
      (try (lease/resign! (:lease manager) lease-id token nil)
           (catch Exception _ nil)))
    (put-state! manager partition {:state :vacant})
    (log! (:logger manager) :warn ::partition-evicted
          {:partition partition :reason reason})))

(defn- release-partition!
  "Graceful handoff of one partition: drain, stop, resign.

   The ordering lives here because it is used twice — at shutdown and when a
   rebalance takes a partition away — and getting it wrong once is enough to
   expose an unfinished write to the next owner."
  [manager partition {:keys [handle lease-id token]}]
  (let [drained? (try
                   (if handle ((:drain-partition! manager) handle) true)
                   (catch Exception e
                     (log! (:logger manager) :error ::drain-failed
                           {:partition partition :error (.getMessage e)})
                     false))]
    (when-not drained?
      (log! (:logger manager) :warn ::drain-incomplete
            {:partition partition
             :consequence "releasing anyway; duplicates are fenced by the epoch"}))
    (when handle
      (try ((:stop-partition! manager) handle)
           (catch Exception e
             (log! (:logger manager) :error ::stop-partition-failed
                   {:partition partition :error (.getMessage e)}))))
    (when (and lease-id token)
      ;; Hold the partition off for a while: without it the releaser (or a peer
      ;; racing it) can re-acquire immediately, which is the thrash the whole
      ;; forgo-excess step exists to avoid.
      (try (lease/resign! (:lease manager) lease-id token
                          {:hold-off-ms (:release-hold-off-ms manager)})
           (catch Exception e
             (log! (:logger manager) :warn ::resign-failed
                   {:partition partition :error (.getMessage e)}))))
    (put-state! manager partition {:state :vacant})
    (log! (:logger manager) :report ::partition-released
          {:partition partition :drained? (boolean drained?)})))

(defn- heartbeat!
  "Keeps this instance's membership key alive. Best-effort: a missed beat must not
   stop the renewal loop, and a store outage is already visible on the lease
   path."
  [manager]
  (when-let [f (:heartbeat-fn manager)]
    (try (f) (catch Exception e
               (log! (:logger manager) :warn ::heartbeat-failed
                     {:error (.getMessage e)})))))

(defn renew-all!
  "Renews every active lease, claims vacant assigned partitions, and hands back
   anything the current membership no longer assigns to this node.

   A `:lost` renewal means this instance is no longer the owner, so it stops
   consuming immediately rather than continuing to work on a partition it cannot
   protect.

   Releases run on their own thread: a drain can take seconds, and blocking the
   renewal loop for it would let every other lease expire — turning a graceful
   rebalance into a mass eviction."
  [manager]
  (heartbeat! manager)
  (refresh-members! manager)
  (let [members (members-of manager)
        assigned (set (assigned-partitions (:instance-id manager) members
                                          (:partitions manager)))
        ;; Reconcile against the store before counting anything as held. A
        ;; partition we believe is ours but the lease disagrees on is demoted
        ;; first, so `election_leaders_held` can never overstate coverage and
        ;; hide an unowned shard -- which is exactly what happened when
        ;; cap-claimed partitions stopped being renewed: the gauge read 6/6
        ;; while two partitions had no lease at all.
        _ (doseq [p (->> @(:state manager)
                         (filter (fn [[_ e]] (= :active (:state e))))
                         (map key)
                         sort)
                  :let [{:keys [lease-id token]} (entry manager p)]
                  :when (and lease-id token)]
            (let [result (try (lease/validate! (:lease manager) lease-id token nil)
                              (catch Exception e
                                (log! (:logger manager) :warn ::validate-failed
                                      {:partition p :error (.getMessage e)})
                                {:status :unknown}))]
              (when (#{:vacant :stale} (:status result))
                (evict! manager p :reconcile-mismatch))))
        owned (->> @(:state manager)
                   (filter (fn [[_ e]] (= :active (:state e))))
                   (map key)
                   set)
        ;; With a cap configured the *cap* decides distribution, not the hash:
        ;; ownership outside the rendezvous share is intentional (that is how an
        ;; uneven draw evens out), so releasing "not mine" here fights the
        ;; claim-free step below and the two flap the same partition forever
        ;; (observed: p0/p1 released and re-claimed every tick).
        to-release (if (:max-partitions manager)
                     []
                     (sort (remove assigned owned)))]
    (let [cap (:max-partitions manager)
          ;; Tracked as the loop runs: a claim in this same pass must count
          ;; towards the cap, or a node under it could take every vacant share
          ;; in one tick.
          active-set (atom (->> @(:state manager)
                                (filter (fn [[_ e]] (= :active (:state e))))
                                (map key)
                                set))]
      (doseq [p (sort (into assigned owned))
              :let [{:keys [state lease-id token] :as current} (entry manager p)]]
        (case state
          :active
          (let [result (try
                         (lease/renew! (:lease manager) lease-id token
                                       {:lease-ms (:lease-ttl-ms manager)})
                         (catch Exception e
                           (log! (:logger manager) :warn ::renew-failed
                                 {:partition p :error (.getMessage e)})
                           {:status :lost}))]
            (when (= :lost (:status result))
              (evict! manager p :lease-lost)
              (swap! active-set disj p))
            (when (= :renewed (:status result))
              (put-state! manager p (assoc current :fencing (or (:fencing result)
                                                                (:fencing current))))))

          ;; `nil` means we never held this partition -- it was assigned to a peer
          ;; when this node started, and this tick is the first time it is ours.
          ;; Skipping it here is how a rebalance produced an owner that owned
          ;; nothing: the member list shrank, the assignment grew, and no claim
          ;; was ever attempted.
          ;;
          ;; The cap check is what stops a forgoed partition from coming straight
          ;; back: after forgoing the excess this node sits AT the cap, so its
          ;; own vacant share is no longer claimable by it.
          (:vacant nil)
          (when (or (nil? cap) (< (count @active-set) cap))
            (let [claimed (claim! manager p)]
              (put-state! manager p claimed)
              (when (= :active (:state claimed))
                (swap! active-set conj p))))

          nil)))

    ;; --- cap: forgo the excess after a grace window --------------------------
    ;; A lone survivor legitimately holds everything; it must not be punished
    ;; for it. But it must also not keep it forever, or a returning peer can
    ;; never take its share. So: hold above the cap during grace, then hand the
    ;; excess back with a hold-off so it does not bounce straight back.
    (let [cap (:max-partitions manager)
          held (->> @(:state manager)
                    (filter (fn [[_ e]] (= :active (:state e))))
                    (map key)
                    sort vec)
          now (System/currentTimeMillis)]
      (if (and cap (> (count held) cap))
        (let [since (or @(:over-cap-since manager)
                        (do (reset! (:over-cap-since manager) now) now))]
          (when (>= (- now since) (:grace-ms manager))
            (reset! (:over-cap-since manager) nil)
            (let [mine (set (assigned-partitions (:instance-id manager) members
                                                (:partitions manager)))
                  ;; Give away what is least "mine" first, highest index first.
                  excess (->> held
                              (sort-by (fn [p] [(if (contains? mine p) 1 0) (- p)]))
                              (take (- (count held) cap)))]
              (log! (:logger manager) :report ::forgo-excess
                    {:cap cap :held (count held) :releasing (vec excess)})
              (doseq [p excess
                      :let [current (entry manager p)]]
                (put-state! manager p {:state :releasing})
                (future
                  (try (release-partition! manager p current)
                       (catch Throwable t
                         (log! (:logger manager) :error ::release-failed
                               {:partition p :error (.getMessage t)}))))))))
        (reset! (:over-cap-since manager) nil)))

    ;; --- cap: claim free partitions while under it ---------------------------
    ;; This is what makes the cap an *evening* mechanism rather than a pure
    ;; ceiling: an unowned partition is picked up by whoever is under its cap.
    ;;
    ;; Deliberately no "lowest id probes first" gate. We cannot see a peer's held
    ;; count, so gating on id would stall: if the lowest-id node sits at its cap
    ;; it never probes, and no one else is allowed to either, leaving the
    ;; partition unowned. The store's atomic acquire is the arbiter -- two nodes
    ;; racing costs one `:busy` reply, and the cap bounds the outcome.
    (let [cap (:max-partitions manager)
          active (->> @(:state manager)
                      (filter (fn [[_ e]] (= :active (:state e))))
                      (map key)
                      set)
          mine (set (assigned-partitions (:instance-id manager) members
                                        (:partitions manager)))]
      (when cap
        (loop [candidates (->> (range (:partitions manager))
                               (remove active)
                               (sort-by (fn [p] [(if (contains? mine p) 0 1) p])))
               active active]
          (when (and (seq candidates) (< (count active) cap))
            (let [p (first candidates)
                  claimed (claim! manager p)]
              (put-state! manager p claimed)
              (recur (rest candidates)
                     (if (= :active (:state claimed)) (conj active p) active)))))))

    ;; Mark before spawning so a claim cannot race a release of the same shard.
    (doseq [p to-release
            :let [current (entry manager p)]]
      (put-state! manager p {:state :releasing})
      (log! (:logger manager) :report ::partition-not-assigned
            {:partition p :members members})
      (future
        (try
          (release-partition! manager p current)
          (catch Throwable t
            (log! (:logger manager) :error ::release-failed
                  {:partition p :error (.getMessage t)})))))

    (notify-state! manager)
    (deref (:state manager))))

(defn start-renewer!
  "Renews at a third of the TTL, and claims vacant partitions on the same tick."
  [manager]
  (reset! (:stopping? manager) false)
  (let [interval (max 1000 (quot (:lease-ttl-ms manager) 3))]
    (reset! (:renewer manager)
            (future
              (while (not @(:stopping? manager))
                (try
                  (when-not @(:stopping? manager) (renew-all! manager))
                  (catch Throwable t
                    (log! (:logger manager) :error ::renew-loop-error
                          {:error (.getMessage t)})))
                (dotimes [_ (quot interval 200)]
                  (when-not @(:stopping? manager) (Thread/sleep 200))))))))

(defn stop!
  "Graceful handoff, per partition: drain → stop → resign, then leave the
   membership set so peers can rebalance immediately instead of waiting out the
   heartbeat TTL."
  [manager]
  (reset! (:stopping? manager) true)
  (when-let [renewer @(:renewer manager)]
    (deref renewer 2000 nil))
  (let [active (->> @(:state manager)
                    (filter (fn [[_p e]] (= :active (:state e))))
                    (map (fn [[p e]] [p e]))
                    (sort-by first))]
    (doseq [[p e] active]
      (release-partition! manager p e))
    (when-let [leave! (:leave-fn manager)]
      (try (leave!) (catch Exception _ nil)))
    (notify-state! manager)
    (deref (:state manager))))

(defn snapshot
  [manager]
  (into {} (map (fn [[p e]] [p (select-keys e [:state :lease-id :fencing])]))
        @(:state manager)))
