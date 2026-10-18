(ns election-system.unit.epoch-guard-test
  "The last mile of partition ownership: a superseded owner cannot overwrite the
   current owner's tally.

   Everything upstream — the lease, `validate!`, the fencing stamp on deliveries —
   only *labels* writes with an epoch. This is where the label is actually
   enforced, so it is the test that decides whether the whole scheme protects
   anything."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [d-core.core.clients.sqlite.client :as sqlite-client]
            [d-core.core.databases.sqlite]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [election-system.db.schema :as schema]
            [election-system.db.writer :as writer]
            [integrant.core :as ig]))

(def ^:private db* (atom nil))

;; A temp file, not :memory: -- a connection pool would give each connection its
;; own in-memory database and the schema would vanish between statements.
(use-fixtures :each
  (fn [f]
    (let [tmp (java.io.File/createTempFile "epoch-guard-" ".db")
          c (sqlite-client/make-client {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath tmp))})
          db (ig/init-key :d-core.core.databases.sqlite/db
                          {:sqlite-client c :storage-schema schema/storage-schema})]
      (schema/create-tables! db :sqlite)
      (reset! db* db)
      (try (f) (finally (.delete tmp))))))

(defn- make-db [] @db*)

(defn- precinct-row
  [db precinct-id]
  (first (sql/select db {:table :precinct_tallies
                         :where {:election_id "e1" :precinct_id precinct-id}})))

(defn- flush!
  [db precinct-id fencing total-votes]
  (writer/flush-precinct-to-db!
   db nil
   {:election-id "e1"
    :precinct-id precinct-id
    :ballots []
    :tallies {"alice" total-votes}
    :total-votes total-votes
    :flushes 1
    :fencing fencing}))

(deftest a-stale-owner-cannot-overwrite-a-newer-tally
  (let [db (make-db)]
    (testing "the current owner's write lands"
      (flush! db "p1" 5 100)
      (is (= 100 (:total_votes (precinct-row db "p1"))))
      (is (= 5 (:ownership_epoch (precinct-row db "p1")))))

    (testing "a superseded owner's late flush is refused"
      (flush! db "p1" 3 999)
      (is (= 100 (:total_votes (precinct-row db "p1")))
          "epoch 3 must not clobber epoch 5")
      (is (= 5 (:ownership_epoch (precinct-row db "p1")))))

    (testing "a newer owner wins"
      (flush! db "p1" 7 200)
      (is (= 200 (:total_votes (precinct-row db "p1"))))
      (is (= 7 (:ownership_epoch (precinct-row db "p1")))))))

(deftest an-unfenced-write-still-works
  (testing "no ownership means no guard, not an epoch of zero that always loses"
    (let [db (make-db)]
      (flush! db "p1" 5 100)
      (flush! db "p1" nil 150)
      (is (= 150 (:total_votes (precinct-row db "p1")))
          "deployments without leases must keep behaving as before"))))

(deftest the-first-write-does-not-need-an-epoch
  (let [db (make-db)]
    (flush! db "p1" 4 42)
    (is (= 42 (:total_votes (precinct-row db "p1"))))
    (is (= 4 (:ownership_epoch (precinct-row db "p1"))))))

(deftest the-national-tally-is-guarded-too
  (let [db (make-db)
        read-national (fn []
                        (first (sql/select db {:table :election_tallies
                                               :where {:election_id "e1"}})))
        flush-national! (fn [fencing total]
                          (writer/flush-election-tally-to-db!
                           db nil
                           {:election-id "e1"
                            :tallies {"alice" total}
                            :precinct-totals {"p1" total}
                            :total-votes total
                            :flushes 1
                            :fencing fencing}))]
    (flush-national! 5 100)
    (is (= 100 (:total_votes (read-national))))
    (flush-national! 2 999)
    (is (= 100 (:total_votes (read-national)))
        "a stale national tally must not overwrite a newer one")
    (flush-national! 6 200)
    (is (= 200 (:total_votes (read-national))))))
