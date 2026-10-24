(ns election-system.unit.resiliency-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.circuit-breaker.in-memory :as cb-mem]
            [d-core.core.circuit-breaker.protocol :as cb]
            [d-core.health.registry :as health-reg]
            [election-system.api.routes :as routes]))

(deftest election-api-resiliency-and-health-test
  (let [db-breaker (cb-mem/make-circuit-breaker
                    {:name "election-db"
                     :failure-threshold 2
                     :reset-timeout-ms 200})
        transport-breaker (cb-mem/make-circuit-breaker
                           {:name "election-async-proxy"
                            :failure-threshold 2
                            :reset-timeout-ms 200})
        health-registry (health-reg/make-health-registry
                         {:service-name "election-api"
                          :checks {:db {:component db-breaker
                                        :critical? true
                                        :tags #{:readiness}}
                                   :async-proxy {:component transport-breaker
                                                 :critical? true
                                                 :tags #{:readiness}}
                                   :liveness {:check-fn (fn [_] {:status :up})
                                              :critical? true
                                              :tags #{:liveness}}}})
        fake-actor-client (reify Object)
        handler (routes/make-handler {:actor-client fake-actor-client
                                      :db-breaker db-breaker
                                      :transport-breaker transport-breaker
                                      :health-registry health-registry})]

    (testing "Liveness endpoint (/live)"
      (let [resp (handler {:request-method :get :uri "/live"})
            body (json/parse-string (:body resp) true)]
        (is (= 200 (:status resp)))
        (is (= "ok" (:status body)))
        (is (= "election-api" (:service body)))))

    (testing "Readiness endpoint (/ready) when healthy"
      (let [resp (handler {:request-method :get :uri "/ready"})
            body (json/parse-string (:body resp) true)]
        (is (= 200 (:status resp)))
        (is (= "ok" (:status body)))
        (is (true? (:healthy? body)))))

    (testing "Diagnostic endpoint (/health) returns component breakdown"
      (let [resp (handler {:request-method :get :uri "/health"})
            body (json/parse-string (:body resp) true)]
        (is (= 200 (:status resp)))
        (is (= "ok" (:status body)))
        (is (= "up" (get-in body [:components :db :status])))
        (is (= "up" (get-in body [:components :async-proxy :status])))))

    (testing "When async-proxy circuit breaker trips"
      (cb/trip! transport-breaker)
      (is (= :open (cb/state transport-breaker)))

      (testing "Readiness reflects :down (503)"
        (let [resp (handler {:request-method :get :uri "/ready"})
              body (json/parse-string (:body resp) true)]
          (is (= 503 (:status resp)))
          (is (= "down" (:status body)))
          (is (false? (:healthy? body)))))

      (testing "Health reflects :down with component details"
        (let [resp (handler {:request-method :get :uri "/health"})
              body (json/parse-string (:body resp) true)]
          (is (= 503 (:status resp)))
          (is (= "down" (:status body)))
          (is (= "down" (get-in body [:components :async-proxy :status])))
          (is (= "open" (get-in body [:components :async-proxy :details :state])))))

      (testing "Voting request fails fast with 503 circuit-breaker-open"
        (let [vote-payload {:election_id "el-1"
                            :precinct_id "p-1"
                            :voter_id "v-1"
                            :candidate_id "cand-1"
                            :signature "sig"}
              resp (handler {:request-method :post
                             :uri "/v1/elections/el-1/votes"
                             :path-params {:election-id "el-1"}
                             :body (json/generate-string vote-payload)})
              body (json/parse-string (:body resp) true)]
          (is (= 503 (:status resp)))
          (is (= "circuit-breaker-open" (:reason body)))))

      (testing "Recovery after reset"
        (cb/reset! transport-breaker)
        (is (= :closed (cb/state transport-breaker)))
        (let [resp (handler {:request-method :get :uri "/ready"})
              body (json/parse-string (:body resp) true)]
          (is (= 200 (:status resp)))
          (is (= "ok" (:status body))))))

    (testing "When database circuit breaker trips"
      (cb/trip! db-breaker)
      (let [resp (handler {:request-method :get :uri "/health"})
            body (json/parse-string (:body resp) true)]
        (is (= 503 (:status resp)))
        (is (= "down" (:status body)))
        (is (= "down" (get-in body [:components :db :status])))
        (is (= "open" (get-in body [:components :db :details :state]))))
      (cb/reset! db-breaker)
      (let [resp (handler {:request-method :get :uri "/health"})
            body (json/parse-string (:body resp) true)]
        (is (= 200 (:status resp)))
        (is (= "ok" (:status body)))))))
