(ns election-system.unit.rebalance-resiliency-test
  "Regression and root-cause specification tests for partition rebalancing,
   scale-up/scale-down, epoch fencing, and crash recovery in `election-system`.

   These tests codify the expected resiliency contracts that fail under chaos
   engineering when actor containers are killed or scaled."
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.core :as actor-core]
            [d-core.actor-system.storage.sql-adapter :as sql-adapter]
            [d-core.actor-system.transport.async-proxy :as ap-transport]
            [d-core.core.clients.sqlite.client :as sqlite-client]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.core.databases.sqlite]
            [d-core.core.leader-election.protocol :as lease]
            [d-core.core.producers.common :as common-prod]
            [d-core.core.producers.in-memory :as im-prod]
            [d-core.libs.time :as time]
            [d-core.queue]
            [d-core-async-proxy.grpc.server :as grpc-srv]
            [d-core-async-proxy.subscriptions.registry :as reg]
            [election-system.db.schema :as schema]
            [election-system.db.writer :as writer]
            [election-system.ownership :as own]
            [election-system.system :as sys]
            [integrant.core :as ig]))

;; -----------------------------------------------------------------------------
;; Shared in-memory lease store with explicit expiry simulation
;; -----------------------------------------------------------------------------

(defn- make-cluster-lease-store
  []
  (let [leases (atom {})
        fencing (atom {})
        hold-offs (atom {})]
    {:leases leases
     :fencing fencing
     :hold-offs hold-offs
     :expire-owner! (fn [owner-id]
                      (swap! leases
                             (fn [m]
                               (into {}
                                     (remove (fn [[_ v]] (= owner-id (:owner-id v))))
                                     m))))
     :store
     (reify lease/LeaderElectionProtocol
       (acquire! [_ lease-id {:keys [owner-id lease-ms]}]
         (let [id (str lease-id)
               now (System/currentTimeMillis)
               hold-until (get @hold-offs id 0)]
           (cond
             (< now hold-until)
             {:status :busy :owner-id "" :fencing nil :remaining-ttl-ms (- hold-until now)}

             (contains? @leases id)
             (let [cur (get @leases id)]
               {:status :busy
                :owner-id (:owner-id cur)
                :fencing (:fencing cur)
                :remaining-ttl-ms (or lease-ms 15000)})

             :else
             (let [f (swap! fencing update id (fnil inc 0))
                   tok (str "tok-" owner-id "-" (get f id))
                   entry {:owner-id owner-id
                          :token tok
                          :fencing (get f id)
                          :lease-ms (or lease-ms 15000)}]
               (swap! leases assoc id entry)
               {:status :acquired
                :lease-id id
                :owner-id owner-id
                :token tok
                :fencing (:fencing entry)
                :ttl-ms (:lease-ms entry)}))))

       (renew! [_ lease-id token {:keys [lease-ms]}]
         (let [id (str lease-id)
               cur (get @leases id)]
           ;; Mirrors redis_common.clj `renew-lua`: renewal extends TTL and
           ;; returns the EXISTING fencing epoch without incrementing it.
           (if (and cur (= token (:token cur)))
             {:status :renewed
              :lease-id id
              :owner-id (:owner-id cur)
              :fencing (:fencing cur)
              :token token
              :ttl-ms (or lease-ms 15000)}
             {:status :lost
              :lease-id id
              :owner-id (:owner-id cur)
              :fencing (:fencing cur)})))

       (resign! [_ lease-id token {:keys [hold-off-ms]}]
         (let [id (str lease-id)
               cur (get @leases id)]
           (if (and cur (= token (:token cur)))
             (do (swap! leases dissoc id)
                 (when (and hold-off-ms (pos? (long hold-off-ms)))
                   (swap! hold-offs assoc id (+ (System/currentTimeMillis) (long hold-off-ms))))
                 {:status :released :lease-id id :owner-id (:owner-id cur) :fencing (:fencing cur)})
             {:status :not-owner :lease-id id})))

       (status [_ lease-id _]
         (let [id (str lease-id)]
           (if-let [cur (get @leases id)]
             {:status :held :lease-id id :owner-id (:owner-id cur) :fencing (:fencing cur) :remaining-ttl-ms 15000}
             {:status :vacant :lease-id id})))

       (validate! [_ lease-id token _]
         (let [id (str lease-id)
               cur (get @leases id)]
           (cond
             (nil? cur) {:status :vacant :lease-id id}
             (= token (:token cur)) {:status :current :lease-id id :owner-id (:owner-id cur) :fencing (:fencing cur) :remaining-ttl-ms 15000}
             :else {:status :stale :lease-id id :owner-id (:owner-id cur) :fencing (:fencing cur) :remaining-ttl-ms 15000}))))}))

(defn- owned-partitions
  [manager]
  (->> (own/snapshot manager)
       (filter (fn [[_ e]] (= :active (:state e))))
       (map key)
       set))

(defn- make-node-manager
  [lease-store members-atom instance-id {:keys [partitions max-partitions grace-ms hold-off-ms]}]
  (own/make-manager
   {:lease lease-store
    :group "actors"
    :instance-id instance-id
    :members-fn #(vec @members-atom)
    :partitions partitions
    :lease-ttl-ms 15000
    :max-partitions max-partitions
    :grace-ms (or grace-ms 60000)
    :release-hold-off-ms (or hold-off-ms 0)
    :start-partition! (fn [ctx] {:handle ctx})
    :stop-partition! (fn [_] nil)
    :drain-partition! (fn [_] true)
    :logger (fn [& _] nil)}))

;; -----------------------------------------------------------------------------
;; 1. Scale-Down / Container Kill: Survivor blocked by static :max-partitions cap
;; -----------------------------------------------------------------------------

(deftest survivor-claims-orphaned-partitions-when-peer-dies-under-cap
  (testing "When 1 of 2 actor containers is killed, the surviving container must claim all 32 partitions during the grace window instead of stalling at :max-partitions (16)"
    (let [{:keys [store expire-owner!]} (make-cluster-lease-store)
          members (atom ["actors-1" "actors-2"])
          total-partitions 32
          ;; Matches `election-system.main`: ceil(TOTAL_PARTITIONS / ACTORS_REPLICAS) = ceil(32 / 2) = 16
          cap 16
          m1 (make-node-manager store members "actors-1"
                                {:partitions total-partitions :max-partitions cap :grace-ms 60000})
          m2 (make-node-manager store members "actors-2"
                                {:partitions total-partitions :max-partitions cap :grace-ms 60000})]
      (own/start! m1)
      (own/start! m2)
      (own/renew-all! m1)
      (own/renew-all! m2)

      (is (= 16 (count (owned-partitions m1))))
      (is (= 16 (count (owned-partitions m2))))
      (is (= 32 (count (set/union (owned-partitions m1) (owned-partitions m2))))
          "All 32 partitions are owned before chaos kill")

      ;; Simulate `docker kill actors-2`: heartbeat and leases expire in Valkey
      (swap! members #(vec (remove #{"actors-2"} %)))
      (expire-owner! "actors-2")

      ;; Survivor runs its renewal/rebalance loop
      (own/renew-all! m1)

      (is (= 32 (count (owned-partitions m1)))
          "BUG: Survivor stays capped at 16 partitions forever (`(< (count @active-set) cap)` in ownership.clj:423,486) leaving 16 partitions permanently unowned"))))

;; -----------------------------------------------------------------------------
;; 2. Scale-Up: Existing nodes at :max-partitions refuse to release to new peer
;; -----------------------------------------------------------------------------

(deftest cluster-scales-up-and-hands-off-partitions-when-new-replica-joins
  (testing "Scaling from 2 to 3 actor containers must hand off partitions to the 3rd container even when :max-partitions is configured"
    (let [{:keys [store]} (make-cluster-lease-store)
          members (atom ["actors-1" "actors-2"])
          total-partitions 12
          ;; Initial 2-replica cap: ceil(12 / 2) = 6
          cap 6
          m1 (make-node-manager store members "actors-1"
                                {:partitions total-partitions :max-partitions cap :hold-off-ms 0})
          m2 (make-node-manager store members "actors-2"
                                {:partitions total-partitions :max-partitions cap :hold-off-ms 0})]
      (own/start! m1)
      (own/start! m2)
      (own/renew-all! m1)
      (own/renew-all! m2)
      (is (= 6 (count (owned-partitions m1))))
      (is (= 6 (count (owned-partitions m2))))

      ;; Scale up: a 3rd actor container joins the membership set
      (swap! members conj "actors-3")
      (let [m3 (make-node-manager store members "actors-3"
                                  {:partitions total-partitions :max-partitions cap :hold-off-ms 0})]
        (own/start! m3)
        ;; Run renewal ticks across the cluster and wait briefly for async release futures
        (own/renew-all! m1)
        (own/renew-all! m2)
        (Thread/sleep 100)
        (own/renew-all! m3)

        (is (pos? (count (owned-partitions m3)))
            "BUG: `to-release` is hardcoded to `[]` when `:max-partitions` is set (ownership.clj:384), and `(> (count held) cap)` is false (6 > 6), so m1/m2 never release any partition to m3")))))

;; -----------------------------------------------------------------------------
;; 3. Epoch Guard: Same-epoch flushes rejected by strict `<`, and `:tick` unfenced
;; -----------------------------------------------------------------------------

(deftest fenced-owner-can-flush-multiple-times-within-same-lease-epoch
  (testing "A lease holder keeps the same fencing epoch across renewals; its 2nd+ flushes must update SQL"
    (let [tmp (java.io.File/createTempFile "epoch-same-owner-" ".db")
          c (sqlite-client/make-client {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath tmp))})
          db (ig/init-key :d-core.core.databases.sqlite/db
                          {:sqlite-client c :storage-schema schema/storage-schema})]
      (try
        (schema/create-tables! db :sqlite)
        ;; First flush by owner at fencing epoch 5
        (writer/flush-election-tally-to-db!
         db nil
         {:election-id "gen-2026"
          :tallies {:alice 50}
          :precinct-totals {"p-1" 50}
          :total-votes 50
          :flushes 1
          :fencing 5})
        ;; Second flush by the SAME owner (still fencing epoch 5, because renew! does not increment fencing)
        (writer/flush-election-tally-to-db!
         db nil
         {:election-id "gen-2026"
          :tallies {:alice 120}
          :precinct-totals {"p-1" 120}
          :total-votes 120
          :flushes 2
          :fencing 5})
        (let [row (first (sql/select db {:table :election_tallies
                                         :where {:election_id "gen-2026"}}))]
          (is (= 120 (:total_votes row))
              "BUG: writer.clj:139 uses strict `<` (`ownership_epoch < EXCLUDED.ownership_epoch`), so `5 < 5` is false and every subsequent fenced flush by the current owner is silently dropped"))
        (finally
          (.delete tmp))))))

(deftest superseded-owner-unfenced-tick-must-not-clobber-fenced-tally
  (testing "Background `:tick` and `flush-partition-state!` dispatches carry `:fencing nil` and must not overwrite a fenced row"
    (let [tmp (java.io.File/createTempFile "epoch-tick-clobber-" ".db")
          c (sqlite-client/make-client {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath tmp))})
          db (ig/init-key :d-core.core.databases.sqlite/db
                          {:sqlite-client c :storage-schema schema/storage-schema})]
      (try
        (schema/create-tables! db :sqlite)
        ;; Current owner (epoch 7) writes 200 votes
        (writer/flush-election-tally-to-db!
         db nil
         {:election-id "gen-2026"
          :tallies {:alice 200}
          :precinct-totals {"p-1" 200}
          :total-votes 200
          :flushes 4
          :fencing 7})
        ;; Evicted/stale owner's 2s `:tick` fires (`engine/dispatch-due-ticks!` passes no `:fencing` in ctx)
        (writer/flush-election-tally-to-db!
         db nil
         {:election-id "gen-2026"
          :tallies {:alice 80}
          :precinct-totals {"p-1" 80}
          :total-votes 80
          :flushes 2
          :fencing nil})
        (let [row (first (sql/select db {:table :election_tallies
                                         :where {:election_id "gen-2026"}}))]
          (is (= 200 (:total_votes row))
              "BUG: `:tick` dispatches have `:fencing nil`, which disables `:conflict-where` in writer.clj and overwrites epoch 7's tally with stale data"))
        (finally
          (.delete tmp))))))

;; -----------------------------------------------------------------------------
;; 4. Container Kill State Loss: Cold start ignores SQL tables & sparse snapshots
;; -----------------------------------------------------------------------------

(deftest actor-kill-recovery-preserves-committed-votes-and-prevents-duplicate-voting
  (testing "When an actor worker crashes after flushing ballots to SQL (but before hitting snapshot-every-n=2000), the replacement worker must not reset state to 0 or accept duplicate votes"
    (let [tmp (java.io.File/createTempFile "actor-kill-recovery-" ".db")
          c (sqlite-client/make-client {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath tmp))})
          db (ig/init-key :d-core.core.databases.sqlite/db
                          {:sqlite-client c :storage-schema schema/storage-schema})
          _ (schema/create-tables! db :sqlite)
          snapshot-store (sql-adapter/create-sql-snapshot-store db)
          effect-handler (writer/make-effect-handler db)
          open-inst (time/instant "2026-11-03T08:00:00Z")
          close-inst (time/instant "2026-11-03T20:00:00Z")
          clk (time/manual-clock (time/instant "2026-11-03T10:00:00Z"))
          entities (sys/make-entity-specs {:election-id "gen-2026"
                                           :opens-at open-inst
                                           :closes-at close-inst
                                           :batch-size 2
                                           :flush-every-n 1
                                           :tell-interval-ms 0
                                           :snapshot-every-n 2000})]
      (try
        (time/with-clock clk
          ;; 1. Worker 1 processes 2 votes; batch-size=2 flushes both ballots to SQL `ballots`
          ;;    and tells `:election-tally`, which flushes `total_votes=2` to `election_tallies`.
          (let [rt-1 (actor-core/start-runtime {:storage snapshot-store
                                                :entities entities
                                                :db db
                                                :effect-handler effect-handler
                                                :options {:watchdog-interval-ms 60000}})
                client-1 (actor-core/create-client (:transport rt-1))]
            (is (= :accepted (:status (actor-core/ask-sync! client-1 :precinct "p-1"
                                                            {:type :cast-vote :voter-id "v-1" :candidate-id :alice}))))
            (is (= :accepted (:status (actor-core/ask-sync! client-1 :precinct "p-1"
                                                            {:type :cast-vote :voter-id "v-2" :candidate-id :alice}))))
            (Thread/sleep 50)
            ;; Verify both ballots and the national tally are durably in SQL before the kill
            (is (= 2 (count (sql/select db {:table :ballots}))))
            (is (= 2 (:total_votes (first (sql/select db {:table :election_tallies
                                                          :where {:election_id "gen-2026"}})))))
            ;; Simulate `SIGKILL` (`docker kill`): rt-1 is abandoned without running graceful stop/finally,
            ;; and because 2 < snapshot-every-n (2000), `actor_snapshots` has 0 rows.
            (deliver (get-in rt-1 [:engine :watchdog-stop-p]) true))

          ;; 2. Worker 2 takes over the partition against the same SQL database + snapshot-store
          (let [rt-2 (actor-core/start-runtime {:storage snapshot-store
                                                :entities entities
                                                :db db
                                                :effect-handler effect-handler
                                                :options {:watchdog-interval-ms 60000}})
                client-2 (actor-core/create-client (:transport rt-2))]
            (try
              ;; Voter "v-1" (whose ballot is ALREADY in SQL `ballots`) retries after failover
              (let [dup-reply (actor-core/ask-sync! client-2 :precinct "p-1"
                                                    {:type :cast-vote :voter-id "v-1" :candidate-id :alice})
                    new-reply (actor-core/ask-sync! client-2 :precinct "p-1"
                                                    {:type :cast-vote :voter-id "v-3" :candidate-id :bob})]
                (Thread/sleep 50)
                (is (= :rejected (:status dup-reply))
                    "BUG: After container kill, `:precinct` re-initializes with empty `:voted-ids #{}` and accepts duplicate votes from voters already recorded in SQL `ballots`")
                (is (= 3 (:total-votes (actor-core/ask-sync! client-2 :precinct "p-1" {:type :get-tally})))
                    "BUG: `:precinct` lost its pre-crash vote count because `snapshot-every-n=2000` wrote no snapshot and cold start never hydrates from SQL `ballots`/`precinct_tallies`")
                (let [tally-view (actor-core/ask-sync! client-2 :election-tally "gen-2026" {:type :get-tally})
                      ballot-count (count (sql/select db {:table :ballots}))]
                  (is (= ballot-count (:total-votes tally-view))
                      "BUG: `:election-tally` cold-starts at 0 and applies incremental `:deltas`, permanently diverging from the SQL `ballots` audit count (`consistent?: false`)")))
              (finally
                (actor-core/stop-runtime! rt-2)))))
        (finally
          (.delete tmp))))))

;; -----------------------------------------------------------------------------
;; 5. Yellow Uncommitted Buffer Recovery: Replica terminated before DB flush
;; -----------------------------------------------------------------------------

(deftest uncommitted-yellow-buffer-recovers-when-replica-terminated-before-db-flush
  (testing "When Replica 1 accepts votes into its in-memory yellow buffer (< batch-size, 0 rows in SQL) and is terminated before flushing, the proxy rolls back the uncommitted buffer and replays those votes to Replica 2"
    (let [tmp (java.io.File/createTempFile "yellow-buffer-recovery-" ".db")
          c (sqlite-client/make-client {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath tmp))})
          db (ig/init-key :d-core.core.databases.sqlite/db
                          {:sqlite-client c :storage-schema schema/storage-schema})
          _ (schema/create-tables! db :sqlite)
          snapshot-store (sql-adapter/create-sql-snapshot-store db)
          effect-handler (writer/make-effect-handler db)
          open-inst (time/instant "2026-11-03T08:00:00Z")
          close-inst (time/instant "2026-11-03T20:00:00Z")
          clk (time/manual-clock (time/instant "2026-11-03T10:00:00Z"))
          ;; batch-size=50 and tick-min-batch=50 so 3 votes stay in the yellow uncommitted buffer
          entities (sys/make-entity-specs {:election-id "gen-2026"
                                           :opens-at open-inst
                                           :closes-at close-inst
                                           :batch-size 50
                                           :tick-min-batch 50
                                           :tick-max-age-ms 60000
                                           :flush-every-n 1
                                           :tell-interval-ms 0
                                           :snapshot-every-n 2000})
          in-proc-name (str "yellow-buf-proxy-" (random-uuid))
          queues (ig/init-key :d-core.queue/in-memory-queues {:topics [:default]})
          im-producer (im-prod/->InMemoryProducer queues nil)
          routing {:publish {:default {:targets :in-memory}}}
          proxy-producer (common-prod/->CommonProducer :in-memory {:in-memory im-producer} routing nil nil)
          proxy-registry (reg/make-registry {:queues queues
                                             :dispatcher-ctx {:max-attempts 2 :timeout-ms 2000}
                                             :logger nil})
          proxy-server (grpc-srv/start-grpc-server {:producer proxy-producer
                                                    :registry proxy-registry
                                                    :transport :in-process
                                                    :in-process-name in-proc-name})
          t-ingress (ap-transport/create-async-proxy-transport
                     {:transport :in-process
                      :in-process-name in-proc-name
                      :partitions 1})
          api-client (actor-core/create-client t-ingress {:partitions 1 :timeout-ms 2000})]
      (try
        (time/with-clock clk
          ;; 1. Start Replica 1 on partition 0
          (let [t-rep1 (ap-transport/create-async-proxy-transport
                        {:transport :in-process
                         :in-process-name in-proc-name
                         :partition 0
                         :partitions 1
                         :poll-ms 5})
                rt-rep1 (actor-core/start-runtime
                         {:transport t-rep1
                          :storage snapshot-store
                          :entities entities
                          :db db
                          :effect-handler effect-handler
                          :options {:partition 0 :partitions 1 :poll-ms 5 :watchdog-interval-ms 60000}})]
            ;; Cast 3 votes via synchronous API `ask-sync!` (< batch-size 50)
            (doseq [[v cand] [["voter-1" :alice] ["voter-2" :bob] ["voter-3" :alice]]]
              (let [res (actor-core/ask-sync! api-client :precinct "p-101"
                                              {:type :cast-vote :voter-id v :candidate-id cand})]
                (is (= :accepted (:status res)))
                (is (nil? (:flushed? res)))))

            ;; Verify all 3 votes are ONLY in Replica 1's yellow uncommitted RAM buffer (0 in SQL, 0 in snapshots!)
            (is (= 0 (count (sql/select db {:table :ballots})))
                "Before flush, all 3 votes reside only in Replica 1's yellow uncommitted buffer")
            (is (= 0 (count (sql/select db {:table :actor_snapshots})))
                "No actor snapshot exists before snapshot-every-n (2000)")

            ;; Simulate `SIGKILL` of Replica 1: transport stream breaks without running
            ;; `flush-partition-state!` or `stop-actor-engine!` (so 0 snapshots and 0 ballots in SQL)
            (deliver (get-in rt-rep1 [:engine :watchdog-stop-p]) true)
            (doseq [s (:subscriptions rt-rep1)]
              (d-core.actor-system.protocols/unsubscribe! t-rep1 s {}))
            (ap-transport/close-async-proxy-transport! t-rep1))

          ;; 2. Start Replica 2 on partition 0: proxy replays the 3 uncommitted votes
          (let [t-rep2 (ap-transport/create-async-proxy-transport
                        {:transport :in-process
                         :in-process-name in-proc-name
                         :partition 0
                         :partitions 1
                         :poll-ms 5})
                rt-rep2 (actor-core/start-runtime
                         {:transport t-rep2
                          :storage snapshot-store
                          :entities entities
                          :db db
                          :effect-handler effect-handler
                          :options {:partition 0 :partitions 1 :poll-ms 5 :watchdog-interval-ms 60000}})]
            (try
              ;; Wait briefly for Replica 2 to consume the replayed yellow-buffer messages
              (Thread/sleep 100)
              (let [live-precinct (actor-core/ask-sync! api-client :precinct "p-101" {:type :get-tally})]
                (is (= 3 (:total-votes live-precinct))
                    "Replica 2 recovered all 3 uncommitted votes from the transport replay")
                (is (= {:alice 2 :bob 1} (:tallies live-precinct))))

              ;; Trigger a single `:tick` on Replica 2: because the batch contains replayed
              ;; messages (`:has-replayed-ballots? true`), it bypasses `tick-min-batch=50` and flushes!
              (actor-core/trigger-tick! rt-rep2 :precinct "p-101")
              (Thread/sleep 50)

              (is (= 3 (count (sql/select db {:table :ballots})))
                  "Replica 2 flushed all 3 recovered yellow-buffer votes to SQL on the next tick")
              (let [tally-view (actor-core/ask-sync! api-client :election-tally "gen-2026" {:type :get-tally})]
                (is (= 3 (:total-votes tally-view)))
                (is (= {:alice 2 :bob 1} (:tallies tally-view))))
              (finally
                (actor-core/stop-runtime! rt-rep2)
                (ap-transport/close-async-proxy-transport! t-rep2)))))
        (finally
          (ap-transport/close-async-proxy-transport! t-ingress)
          (ig/halt-key! :d-core-async-proxy.grpc/server proxy-server)
          (ig/halt-key! :d-core-async-proxy.subscriptions/registry proxy-registry)
          (.delete tmp))))))
