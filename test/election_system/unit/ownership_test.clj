(ns election-system.unit.ownership-test
  "Assignment and the partition lease lifecycle.

   Assignment is pure and gets the strict tests: identical on every instance,
   stable when membership is stable, and moving as little as possible when it is
   not. The lifecycle tests use a fake lease so the ordering rules (claim →
   start, drain → stop → resign, lost → evict) are asserted with no broker."
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.protocol :as lp]
            [election-system.ownership :as own]))

;; ---------------------------------------------------------------------------
;; Assignment
;; ---------------------------------------------------------------------------

(deftest every-partition-has-exactly-one-owner
  (doseq [members [["a"] ["a" "b"] ["a" "b" "c"] ["a" "b" "c" "d" "e"]]]
    (let [partitions 12
          assigned (mapcat #(own/assigned-partitions % members partitions) members)]
      (is (= (sort (range partitions)) (sort assigned))
          (str "assignment must cover every partition exactly once for " (pr-str members))))))

(deftest assignment-is-deterministic
  (testing "two instances computing independently must agree, whatever order they see members in"
    (let [members ["p-0" "p-1" "p-2"]]
      (is (= (own/assigned-partitions "p-1" members 16)
             (own/assigned-partitions "p-1" (reverse members) 16))))))

(deftest a-single-instance-owns-everything
  (is (= (vec (range 8)) (own/assigned-partitions "solo" ["solo"] 8))))

(deftest an-unknown-instance-owns-nothing
  (testing "a pod that started before its membership was published must not claim work"
    (is (= [] (own/assigned-partitions "ghost" ["a" "b"] 8)))))

(deftest scaling-moves-only-its-share
  (testing "3 -> 4 members should move roughly 1/4 of partitions, not reshuffle everything"
    (let [partitions 12
          before (into {} (for [id ["a" "b" "c"]]
                            [id (set (own/assigned-partitions id ["a" "b" "c"] partitions))]))
          after (into {} (for [id ["a" "b" "c" "d"]]
                           [id (set (own/assigned-partitions id ["a" "b" "c" "d"] partitions))]))
          moved (set (for [id ["a" "b" "c"]
                           :let [lost (set/difference (get before id) (get after id))]
                           p lost]
                       p))]
      ;; Modulo hashing would move nearly all 12 here.
      (is (<= (count moved) 6)
          (str "moved " (count moved) " of " partitions)))))

(deftest one-lease-per-partition
  (is (= "actors/p0" (own/election-id "actors" 0)))
  (is (= "actors/p11" (own/election-id "actors" 11))))

;; ---------------------------------------------------------------------------
;; Lifecycle
;; ---------------------------------------------------------------------------

(defn- fake-lease
  "Stand-in for LeaderElectionProtocol. `:ons` records lifecycle events in order
   so the tests can assert sequencing rather than just outcomes."
  [{:keys [busy? renew-status ons]
    :or {busy? (constantly false) renew-status :renewed ons (atom [])}}]
  {:ons ons
   :lease
   (reify lp/LeaderElectionProtocol
     (acquire! [_ id _]
       (swap! ons conj [:acquire id])
       (if (busy? id)
         {:status :busy :owner-id "someone-else"}
         {:status :acquired :token (str "tok-" id) :fencing 7}))
     (renew! [_ id token _]
       (swap! ons conj [:renew id])
       (if (= renew-status :lost)
         {:status :lost}
         {:status :renewed :token token :fencing 7}))
     (resign! [_ id token _]
       (swap! ons conj [:resign id token])
       {:status :released})
     (status [_ _ _] {:status :held})
     (validate! [_ _ _ _] {:status :current}))})

(defn- manager
  ([lease opts] (manager lease opts (atom [])))
  ([lease {:keys [drained?]} ons]
   (own/make-manager
    {:lease lease
     :group "actors"
     :instance-id "inst-0"
     :members ["inst-0"]
     :partitions 2
     :lease-ttl-ms 15000
     :start-partition! (fn [{:keys [partition]}] (swap! ons conj [:start partition]) {:handle partition})
     :stop-partition! (fn [{:keys [handle]}] (swap! ons conj [:stop handle]))
     :drain-partition! (fn [{:keys [handle]}] (swap! ons conj [:drain handle]) (boolean drained?))
     :logger nil})))

(deftest start-claims-assigned-partitions-and-records-the-epoch
  (let [{:keys [lease ons]} (fake-lease {})
        m (manager lease {:drained? true} ons)]
    (own/start! m)
    (testing "one claim per partition, under the per-partition lease id"
      (is (= [[:acquire "actors/p0"] [:start 0]
              [:acquire "actors/p1"] [:start 1]]
             @ons)))
    (testing "the epoch recorded is the one the lease returned"
      (is (= {:state :active :lease-id "actors/p0" :fencing 7}
             (get (own/snapshot m) 0))))))

(deftest a-busy-partition-is-left-for-later
  (let [{:keys [lease ons]} (fake-lease {:busy? (constantly true)})
        m (manager lease {:drained? true} ons)]
    (own/start! m)
    (is (= :vacant (get-in (own/snapshot m) [0 :state]))
        "a peer may still be draining it; this instance must not subscribe")
    (is (not-any? #{:start 0} @ons))))

(deftest lost-lease-hands-the-partition-back-immediately
  (let [{:keys [lease ons]} (fake-lease {:renew-status :lost})
        m (manager lease {:drained? true} ons)]
    (own/start! m)
    (own/renew-all! m)
    (testing "eviction drops ownership: it must not keep working on a partition it cannot protect"
      (is (= :vacant (get-in (own/snapshot m) [0 :state]))))
    (testing "and releases the token so the next owner is not blocked for a whole TTL"
      (is (some #{[:resign "actors/p0" "tok-actors/p0"]} @ons)))))

(deftest graceful-stop-drains-then-stops-then-resigns
  (let [{:keys [lease ons]} (fake-lease {})
        m (manager lease {:drained? true} ons)]
    (own/start! m)
    (reset! ons [])
    (own/stop! m)
    (is (= [[:drain 0] [:stop 0] [:resign "actors/p0" "tok-actors/p0"]
            [:drain 1] [:stop 1] [:resign "actors/p1" "tok-actors/p1"]]
           @ons)
        "drain must precede stop, and resign must follow both")))

(deftest a-partition-that-cannot-drain-is-still-released
  (let [{:keys [lease ons]} (fake-lease {})
        m (manager lease {:drained? false} ons)]
    (own/start! m)
    (own/stop! m)
    (is (= :vacant (get-in (own/snapshot m) [0 :state]))
        "holding a lease we cannot drain would stall the partition; duplicates are fenced")
    (testing "and it still resigns rather than leaving the peer blocked"
      (is (some #{[:resign "actors/p0" "tok-actors/p0"]} @ons)))))

;; ---------------------------------------------------------------------------
;; Rebalance: membership is a live fact
;; ---------------------------------------------------------------------------

(deftest losing-membership-releases-everything
  (let [ons (atom [])
        members (atom ["a" "b"])
        m (own/make-manager
           {:lease (reify lp/LeaderElectionProtocol
                     (acquire! [_ id _] {:status :acquired :token (str "tok-" id) :fencing 1})
                     (renew! [_ _ token _] {:status :renewed :token token :fencing 1})
                     (resign! [_ id token _] (swap! ons conj :resign) {:status :released})
                     (status [_ _ _] {:status :held})
                     (validate! [_ _ _ _] {:status :current}))
            :group "actors"
            :instance-id "b"
            ;; Live membership, exactly as the valkey heartbeat provides it.
            :members-fn (fn [] @members)
            :partitions 8
            :lease-ttl-ms 15000
            :start-partition! (fn [{:keys [partition]}] {:handle partition})
            :stop-partition! (fn [_] (swap! ons conj :stop))
            :drain-partition! (fn [_] (swap! ons conj :drain) true)
            :logger nil})]
    (own/start! m)
    (let [active (fn [] (filter (fn [[_ e]] (= :active (:state e))) (own/snapshot m)))]
      (is (seq (active)) "b owns something while it is a member")
      (reset! members ["a"])
      (own/renew-all! m)
      (Thread/sleep 200)
      (is (empty? (active)) "a node that is no longer a member must own nothing")
      (is (some #{:drain} @ons) "and must drain before releasing"))))

(deftest an-empty-membership-read-does-not-release-anything
  (let [ons (atom [])
        members (atom ["a" "b"])
        m (own/make-manager
           {:lease (reify lp/LeaderElectionProtocol
                     (acquire! [_ id _] {:status :acquired :token (str "tok-" id) :fencing 1})
                     (renew! [_ _ token _] {:status :renewed :token token :fencing 1})
                     (resign! [_ id token _] (swap! ons conj :resign) {:status :released})
                     (status [_ _ _] {:status :held})
                     (validate! [_ _ _ _] {:status :current}))
            :group "actors"
            :instance-id "b"
            :members-fn (fn [] @members)
            :partitions 8
            :lease-ttl-ms 15000
            :start-partition! (fn [{:keys [partition]}] {:handle partition})
            :stop-partition! (fn [_] (swap! ons conj :stop))
            :drain-partition! (fn [_] (swap! ons conj :drain) true)
            :logger nil})]
    (own/start! m)
    (let [before (count (filter (fn [[_ e]] (= :active (:state e))) (own/snapshot m)))]
      ;; A store hiccup returning [] must not be read as "everybody died".
      (reset! members [])
      (own/renew-all! m)
      (Thread/sleep 100)
      (is (= before (count (filter (fn [[_ e]] (= :active (:state e))) (own/snapshot m))))
          "an empty read keeps the previous member list")
      (is (not-any? #{:drain} @ons)))))

(deftest joining-the-membership-claims-newly-assigned-partitions
  (let [ons (atom [])
        members (atom ["a"])
        claim! (fn [id] (swap! ons conj [:acquire id])
                 {:status :acquired :token (str "tok-" id) :fencing 1})
        m (own/make-manager
           {:lease (reify lp/LeaderElectionProtocol
                     (acquire! [_ id _] (claim! id))
                     (renew! [_ _ token _] {:status :renewed :token token :fencing 1})
                     (resign! [_ _ _ _] {:status :released})
                     (status [_ _ _] {:status :held})
                     (validate! [_ _ _ _] {:status :current}))
            :group "actors"
            :instance-id "b"
            :members-fn (fn [] @members)
            :partitions 8
            :lease-ttl-ms 15000
            :start-partition! (fn [{:keys [partition]}] {:handle partition})
            :stop-partition! (fn [_] nil)
            :drain-partition! (fn [_] true)
            :logger nil})]
    (own/start! m)
    (is (empty? (filter (fn [[_ e]] (= :active (:state e))) (own/snapshot m))))
    ;; A peer's heartbeat appears: b is now assigned a share it never held.
    (reset! members ["a" "b"])
    (own/renew-all! m)
    (is (= 0 (count (remove (fn [[_ e]] (= :active (:state e))) (own/snapshot m))))
        "every newly-assigned partition must be claimed, not skipped")))

;; ---------------------------------------------------------------------------
;; Cap: even distribution with a grace window and a release hold-off
;; ---------------------------------------------------------------------------

(defn- cap-manager
  [opts]
  (let [resigns (atom [])
        lease (reify lp/LeaderElectionProtocol
                (acquire! [_ id _]
                  {:status :acquired :token (str "tok-" id "-" (rand-int 100000)) :fencing 1})
                (renew! [_ _ token _] {:status :renewed :token token :fencing 1})
                (resign! [_ id _ resign-opts]
                  (swap! resigns conj {:id id :opts resign-opts})
                  {:status :released})
                (status [_ _ _] {:status :held})
                (validate! [_ _ _ _] {:status :current}))]
    {:resigns resigns
     :m (own/make-manager
         (merge {:lease lease
                 :group "actors"
                 :instance-id "b"
                 :members ["a" "b"]
                 :partitions 8
                 :lease-ttl-ms 15000
                 :release-hold-off-ms 3000
                 :start-partition! (fn [{:keys [partition]}] {:handle partition})
                 :stop-partition! (fn [_] nil)
                 :drain-partition! (fn [_] true)
                 :logger nil}
                opts))}))

(defn- active-partitions
  [m]
  (->> (own/snapshot m)
       (filter (fn [[_ e]] (= :active (:state e))))
       (map key)
       set))

(deftest over-cap-forgoes-the-excess-after-the-grace-window
  ;; A node only ends up above its cap when the cap appears *after* it already
  ;; holds the partitions -- a config change, or membership shrinking. Claiming
  ;; itself is cap-gated, so this is the path that has to forgo.
  (let [{:keys [m resigns]} (cap-manager {:max-partitions nil :grace-ms 0})
        _ (own/start! m)
        m (assoc m :max-partitions 2)]
    (is (> (count (active-partitions m)) 2) "starts above the later cap")
    (own/renew-all! m)
    (Thread/sleep 250)
    (is (= 2 (count (active-partitions m))) "drops to the cap")
    (is (every? #(= 3000 (get-in % [:opts :hold-off-ms])) @resigns)
        "every forgoed lease is released with a hold-off, so it cannot bounce back")))

(deftest within-the-grace-window-the-excess-is-kept
  (let [{:keys [m resigns]} (cap-manager {:max-partitions 2 :grace-ms 60000})]
    (own/start! m)
    (let [before (count (active-partitions m))]
      (own/renew-all! m)
      (Thread/sleep 150)
      (is (= before (count (active-partitions m)))
          "a lone survivor may hold everything while the grace window is open")
      (is (empty? @resigns)))))

(deftest under-cap-claims-free-partitions-up-to-the-cap
  (let [{:keys [m]} (cap-manager {:max-partitions 8 :grace-ms 0})]
    (own/start! m)
    (own/renew-all! m)
    (Thread/sleep 100)
    (is (= 8 (count (active-partitions m)))
        "fills up to the cap from partitions outside its rendezvous share")))

(deftest reconciliation-demotes-partitions-the-store-disagrees-on
  ;; The store is the source of truth. If a partition we believe we hold comes
  ;; back :stale -- someone else holds it -- we drop it and must NOT count it.
  ;; (A :vacant partition is different: it is free again, and re-acquiring it is
  ;; the right move, so it is deliberately not the case tested here.)
  (let [taken (atom #{})
        m (own/make-manager
           {:lease (reify lp/LeaderElectionProtocol
                     (acquire! [_ id _]
                       (let [p (parse-long (subs (second (clojure.string/split id #"/")) 1))]
                         (if (contains? @taken p)
                           {:status :busy :owner-id "someone-else"}
                           {:status :acquired :token (str "tok-" id) :fencing 1})))
                     (renew! [_ _ token _] {:status :renewed :token token :fencing 1})
                     (resign! [_ _ _ _] {:status :released})
                     (status [_ _ _] {:status :held})
                     (validate! [_ id token _]
                       (let [p (parse-long (subs (second (clojure.string/split id #"/")) 1))]
                         {:status (if (contains? @taken p) :stale :current)})))
            :group "actors"
            :instance-id "a"
            :members ["a"]
            :partitions 4
            :lease-ttl-ms 15000
            :max-partitions 4
            :grace-ms 0
            :start-partition! (fn [{:keys [partition]}] {:handle partition})
            :stop-partition! (fn [_] nil)
            :drain-partition! (fn [_] true)
            :logger nil})]
    (own/start! m)
    (is (contains? (active-partitions m) 3) "believed held before reconciliation")
    (reset! taken #{3})   ;; someone else took it)
    (own/renew-all! m)
    (is (not (contains? (active-partitions m) 3))
        "a lease the store says someone else holds is no longer counted as ours")))
