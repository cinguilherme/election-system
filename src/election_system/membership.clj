(ns election-system.membership
  "Live membership, derived from heartbeats in the lease store.

  A static member list cannot express 'this node died': with `actors-0` still
  listed, the survivor computes that the orphaned partition belongs to the dead
  node and never claims it. So membership has to be a live fact.

  Each node writes `<prefix><instance-id>` with a TTL, and the member list is the
  set of unexpired keys. That reuses the store that already holds the leases, so
  there is no second coordination system to operate.

  Deliberately *not* a lease and not a leader election: this answers 'who is
  around', and the per-partition leases answer 'who owns p5'. Merging the two
  would make one failure mode look like the other."
  (:require [taoensso.carmine :as car]))

(defn- prefix
  [group]
  (str "dcore:ownership:" group ":member:"))

(defn make-membership
  "Builds the heartbeat registry for `instance-id` in `group`.

  `:heartbeat-ttl-ms` should exceed the renewal interval by a comfortable margin
  (a missed beat must not eject a healthy node), and stay short enough that a
  dead node's partitions are claimable quickly."
  [{:keys [valkey-client group instance-id heartbeat-ttl-ms]
    :or {heartbeat-ttl-ms 20000}}]
  (when-not (and valkey-client instance-id)
    (throw (ex-info "membership requires :valkey-client and :instance-id"
                    {:instance-id instance-id})))
  (let [conn (:conn valkey-client)
        group (str (or group "actors"))
        pfx (prefix group)]
    {:conn conn
     :prefix pfx
     :instance-id (str instance-id)
     :ttl-ms (long heartbeat-ttl-ms)
     :key (str pfx instance-id)}))

(defn beat!
  "Refreshes this instance's heartbeat. Returns the new TTL in ms."
  [{:keys [conn key ttl-ms]}]
  ;; Second granularity: carmine/valkey EX takes whole seconds.
  (let [seconds (max 1 (quot (long ttl-ms) 1000))]
    (car/wcar conn (car/set key (str (System/currentTimeMillis)) "EX" seconds))
    (* seconds 1000)))

(defn members
  "Currently live instance ids, sorted for determinism.

   Uses KEYS on purpose: the set is a handful of keys written by this very
   deployment, and SCAN's cursor handling would add a failure mode for no gain at
   this size."
  [{:keys [conn prefix]}]
  (->> (car/wcar conn (car/keys (str prefix "*")))
       (map #(subs % (count prefix)))
       sort
       vec))

(defn leave!
  "Removes this instance's heartbeat immediately, so peers can rebalance without
   waiting for the TTL."
  [{:keys [conn key]}]
  (car/wcar conn (car/del key)))
