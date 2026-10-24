(ns election-system.unit.resilience-wiring-test
  "Item 1 of docs/resilience-roadmap.md: the chaos runtime is inert by default,
   the admin plane is token-guarded and only exists when chaos is enabled, and the
   actors tier gets real health checks."
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.chaos.journal :as chaos-journal]
            [d-core.core.circuit-breaker.protocol :as cb]
            [d-core.core.health.protocol :as health-p]
            [d-core.resilience.core :as resilience]
            [election-system.api.routes :as routes]
            [d-core.actor-system.protocols :as actor-p]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.core.leader-election.protocol :as lease-p]
            [election-system.resilience.admin :as admin]
            [election-system.resilience.chaos :as chaos]
            [election-system.resilience.health :as health])
  (:import (java.net ServerSocket)))

(defn- fake-sql
  []
  (reify sql/SimpleSQLProtocol
    (insert! [_ _ _] {:inserted 1})
    (insert-batch! [_ _ _] {:inserted 1})
    (upsert! [_ _ _] {:upserted 1})
    (select [_ _] [])
    (update! [_ _ _] {:updated 1})
    (delete! [_ _] {:deleted 1})
    (execute! [_ _ _] {:executed true})))

(defn- fake-transport
  []
  (reify actor-p/ActorTransport
    (send-message! [_this _entity-type _entity-id _envelope _opts] true)
    (request-message! [_this _entity-type _entity-id _envelope _opts] {:reply true})
    (subscribe-entities! [_this _entity-type _handler-fn _opts] {:sub :fake})
    (unsubscribe! [_ _ _] true)))

(defn- fake-lease
  []
  (reify lease-p/LeaderElectionProtocol
    (acquire! [_ _ _] {:status :busy})
    (renew! [_ _ _ _] {:status :lost})
    (resign! [_ _ _ _] {:status :not-owner})
    (status [_ _ _] {:status :vacant})
    (validate! [_ _ _ _] {:status :vacant})))

(defn- req
  ([method uri] (req method uri {}))
  ([method uri headers]
   {:request-method method :uri uri :headers headers}))

(defn- body
  [resp]
  (json/parse-string (:body resp) true))

;; ============================================================================
;; Runtime: inert by default
;; ============================================================================

(deftest chaos-runtime-is-inert-by-default
  (testing "no controller, no journal, no admin plane when disabled"
    (let [rt (chaos/make-runtime {})]
      (is (false? (:enabled? rt)))
      (is (nil? (:controller rt)))
      (is (nil? (:journal rt)))
      (is (= :off (:journal-mode rt)))))

  (testing "decorators are identity functions when disabled"
    (let [rt (chaos/make-runtime {})
          db (fake-sql)
          transport (fake-transport)]
      (is (identical? db (chaos/instrument-db rt db)))
      (is (identical? transport (chaos/instrument-transport rt transport)))))

  (testing "enabled with journal-mode :off injects but records nothing"
    (let [rt (chaos/make-runtime {:enabled? true :journal-mode :off})]
      (is (true? (:enabled? rt)))
      (is (some? (:controller rt)))
      (is (nil? (:journal rt)))
      (is (= :off (:journal-mode rt)))))

  (testing "enabled with journal-mode :full allocates the ring buffer"
    (let [rt (chaos/make-runtime {:enabled? true :journal-mode :full :max-events 128})]
      (is (some? (:journal rt)))
      (is (= :full (:journal-mode rt)))
      (is (= 128 (:max-events (:journal rt))))))

  (testing "an unknown journal mode falls back to :full rather than failing silently"
    (let [rt (chaos/make-runtime {:enabled? true :journal-mode :bogus})]
      (is (= :full (:journal-mode rt)))
      (is (some? (:journal rt))))))

;; ============================================================================
;; Admin plane
;; ============================================================================

(deftest admin-plane-is-closed-when-chaos-is-disabled
  (let [handler (admin/make-handler {:role :api :instance-id "api-1"})]
    (testing "/_chaos returns 404 chaos-disabled"
      (let [resp (handler (req :get "/_chaos/status"))]
        (is (= 404 (:status resp)))
        (is (= "chaos-disabled" (:reason (body resp))))))

    (testing "root reports what is enabled"
      (let [resp (handler (req :get "/"))
            b (body resp)]
        (is (= 200 (:status resp)))
        (is (= "api" (:role b)))
        (is (= "api-1" (:instance-id b)))
        (is (false? (get-in b [:chaos :enabled?])))
        (is (= ["/live" "/ready" "/health"] (:endpoints b)))))))

(deftest admin-plane-requires-a-token
  (let [rt (chaos/make-runtime {:enabled? true})
        handler (admin/make-handler {:role :actors
                                     :instance-id "actors-1"
                                     :controller (:controller rt)
                                     :journal (:journal rt)
                                     :token "s3cret"})]
    (testing "no token header"
      (let [resp (handler (req :get "/_chaos/status"))]
        (is (= 401 (:status resp)))
        (is (= "unauthorized" (:status (body resp))))))

    (testing "wrong token"
      (is (= 401 (:status (handler (req :get "/_chaos/status" {"x-chaos-token" "nope"}))))))

    (testing "correct token"
      (let [resp (handler (req :get "/_chaos/status" {"x-chaos-token" "s3cret"}))]
        (is (= 200 (:status resp)))
        (is (true? (:enabled? (body resp))))))))

(deftest admin-plane-injects-and-reports-rules
  (let [rt (chaos/make-runtime {:enabled? true})
        handler (admin/make-handler {:role :actors
                                     :instance-id "actors-1"
                                     :controller (:controller rt)
                                     :journal (:journal rt)})]
    (testing "inject a rule over HTTP"
      (let [resp (handler {:request-method :post
                           :uri "/_chaos/rules"
                           :headers {"content-type" "application/json"}
                           :body (json/generate-string {:id "lease-blackhole"
                                                        :component "lease"
                                                        :op "renew!"
                                                        :times 1
                                                        :fault {:type "return"
                                                                :return {:status "lost"}}})})]
        (is (= 200 (:status resp)))
        (is (= ["lease-blackhole"] (map name (:rule-ids (body resp)))))))

    (testing "status reports the active rule"
      (let [b (body (handler (req :get "/_chaos/status")))]
        (is (true? (:enabled? b)))
        (is (= 1 (count (:rules b))))
        (is (= "lease-blackhole" (-> b :rules first :id name)))))

    (testing "journal endpoint answers"
      (let [resp (handler (req :get "/_chaos/journal"))]
        (is (= 200 (:status resp)))
        (is (= 0 (:count (body resp))))))))

;; ============================================================================
;; Actors health registry
;; ============================================================================

(deftest actors-health-registry-registers-real-checks
  (with-open [server (ServerSocket. 0)]
    (let [port (.getLocalPort server)
          registry (health/make-actors-registry
                    {:db (fake-sql)
                     :broker {:host "127.0.0.1" :port port}
                     :lease (fake-lease)
                     :owned-fn (fn [] {:owned 2 :total 4})})
          handler (admin/make-handler {:role :actors
                                       :instance-id "actors-1"
                                       :health-registry registry})]

      (testing "all five checks are registered"
        (is (= #{:liveness :db :broker :lease-store :owning-partitions}
               (set (keys @(:checks-atom registry))))))

      (testing "readiness is 200 when every dependency answers"
        (let [resp (handler (req :get "/ready"))
              b (body resp)]
          (is (= 200 (:status resp)))
          (is (true? (:healthy? b)))))

      (testing "/health breaks the result down per component"
        (let [resp (handler (req :get "/health"))
              b (body resp)]
          (is (= 200 (:status resp)))
          (is (= "up" (get-in b [:components :db :status])))
          (is (= "up" (get-in b [:components :broker :status])))
          (is (= "up" (get-in b [:components :lease-store :status])))
          (is (= 2 (get-in b [:components :owning-partitions :details :partitions-owned])))))

      (testing "liveness stays 200 even when a dependency is down"
        (let [down (health/make-actors-registry
                    {:db (fake-sql)
                     :broker {:host "127.0.0.1" :port 1}
                     :lease (fake-lease)
                     :owned-fn (fn [] {:owned 0 :total 4})})
              h (admin/make-handler {:role :actors :health-registry down})]
          (is (= 200 (:status (h (req :get "/live")))))
          (is (= 503 (:status (h (req :get "/ready")))))
          (testing "owning nothing degrades rather than failing"
            (is (= "degraded"
                   (get-in (body (h (req :get "/health"))) [:components :owning-partitions :status])))))))))

;; ============================================================================
;; Flight recorder wiring (roadmap item 4)
;; ============================================================================

(deftest journal-modes-are-wired
  (testing ":meta builds a journal that keeps shape, not payloads"
    (let [rt (chaos/make-runtime {:enabled? true :journal-mode :meta})]
      (is (= :meta (:journal-mode rt)))
      (is (= :meta (chaos-journal/mode (:journal rt))))))

  (testing ":off injects without recording"
    (let [rt (chaos/make-runtime {:enabled? true :journal-mode :off})]
      (is (nil? (:journal rt)))
      (is (= :off (:journal-mode rt)))))

  (testing "sampling and payload bounds are passed through"
    (let [rt (chaos/make-runtime {:enabled? true :journal-mode :full
                                  :sample 0.25 :max-payload-chars 128})]
      (is (= 0.25 (:sample (:journal rt))))
      (is (= 128 (:max-payload-chars (:journal rt))))))

  (testing "an unknown mode falls back to :full"
    (let [rt (chaos/make-runtime {:enabled? true :journal-mode :bogus})]
      (is (= :full (:journal-mode rt))))))

(deftest the-api-binds-the-experiment-header
  (let [seen (atom ::unbound)
        handler (routes/wrap-experiment-context
                 (fn [_req]
                   (reset! seen chaos-journal/*experiment-id*)
                   {:status 200}))]
    (handler {:headers {"x-experiment-id" "exp-1"}})
    (is (= "exp-1" @seen))
    (testing "and leaves it nil when the header is absent"
      (handler {:headers {}})
      (is (nil? @seen)))))

(deftest the-trace-thunk-resolves-a-span-or-nil
  (let [f (chaos/trace-id-fn)]
    (is (fn? f))
    (is (nil? (f)))))

;; ============================================================================
;; Resilience guards (roadmap item 7)
;; ============================================================================

(deftest a-guard-is-a-drop-in-for-the-breaker-the-app-already-uses
  (let [g (resilience/make-guard {:name "election-db"
                                  :timeout-ms 1000
                                  :breaker {:failure-threshold 2}})]
    (testing "it satisfies the protocols routes and the health registry expect"
      (is (satisfies? cb/CircuitBreakerProtocol g))
      (is (satisfies? health-p/HealthCheckProtocol g)))
    (testing "protect! runs the operation and records failures"
      (is (= :ok (cb/protect! g (fn [] :ok))))
      (dotimes [_ 2]
        (try (cb/protect! g (fn [] (throw (ex-info "x" {:type :test}))))
             (catch Exception _ nil)))
      (is (= :open (cb/state g)))
      (is (= :down (:status (health-p/check-health g))))
      (testing "and trip/reset still work"
        (cb/reset! g)
        (is (= :closed (cb/state g)))))))
