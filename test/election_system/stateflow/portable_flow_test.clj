(ns election-system.stateflow.portable-flow-test
  "One flow, two tiers.

   The body below is the same code in both tests: it votes over HTTP and asserts the
   audit reconciles. The only difference is the world -- an in-process cluster for
   `:flow`, a compose stack for `:stack` -- which is the point of item 10: a flow is
   a specification of behaviour, not of where the system happens to be running."
  (:require [clojure.test :refer [deftest]]
            [d-core.libs.time :as time]
            [d-core.stateflow.core :as flow]
            [d-core.stateflow.http :as sf-http]
            [d-core.stateflow.journal :as sf-journal]
            [d-core.stateflow.stack :as stack]
            [election-system.system :as sys]
            [state-flow.core :as sf]))

(def ^:private election-id "national-2026")

(defn- vote-and-audit-flow
  "The portable body: cast a vote, wait for the asynchronous write to settle, and
   assert the audit ledger reconciles."
  []
  (flow/flow "a vote is accepted and the audit reconciles"
    (sf-journal/clear!)
    [v (sf-http/post! (str "/v1/elections/" election-id "/votes")
                      {:precinct_id "p-1" :voter_id "v-1" :candidate_id "alice"})]
    (flow/match? {:status 200 :data {:status "accepted"}} v)
    (flow/settle!)
    [audit (sf-http/get! (str "/v1/elections/" election-id "/audit"))]
    (flow/match? {:status 200 :data {:consistent? true}} audit)))

;; ============================================================================
;; Tier :flow -- in-process cluster, no Docker
;; ============================================================================

(defn- start-in-process-world
  []
  (let [open-inst (time/instant "2026-11-03T08:00:00Z")
        close-inst (time/instant "2026-11-03T20:00:00Z")
        clk (time/manual-clock open-inst)]
    {:clock clk
     :cluster (time/with-clock clk
                (sys/start-multi-tier-test-cluster
                 {:election-id election-id
                  :worker-count 2
                  :batch-size 2
                  :flush-every-n 1
                  :tell-interval-ms 0
                  :opens-at open-inst
                  :closes-at close-inst}))}))

(deftest ^{:tier :flow} the-flow-passes-in-process
  (let [{:keys [clock cluster]} (start-in-process-world)]
    (try
      (time/with-clock clock
        (flow/run-with-lifecycle! (vote-and-audit-flow)
                                  {:init (fn [] {:cluster cluster})
                                   :cleanup (fn [_] nil)}))
      (finally
        (sys/stop-multi-tier-test-cluster! cluster)))))

;; ============================================================================
;; Tier :stack -- the same body against docker compose
;; ============================================================================

(deftest ^{:tier :stack} the-flow-passes-against-compose
  (if-not (stack/enabled?)
    (println "[skip] stack tier: run `make chaos-up` then `ELECTION_STACK=1 make test-stack`")
    (stack/with-stack [st {:files ["docker-compose.yaml" "docker-compose.chaos.yaml"]
                           :project "election-stack-test"
                           :urls {:api "http://127.0.0.1:8080"
                                  :engine "http://127.0.0.1:8088"}
                           :token (System/getenv "CHAOS_TOKEN")}]
      (sf/run! (vote-and-audit-flow) (stack/world st)))))
