(ns election-system.unit.health-v2-test
  "Health v2 wired into the tiers (roadmap item 9): the actors' `/ready` must be a
   real probe against a real database, and lifecycle must gate it."
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.health.http :as health-http]
            [d-core.health.registry :as hr]
            [election-system.resilience.health :as actors-health]
            [election-system.system :as sys])
  (:import (java.net ServerSocket)))

(deftest the-actors-readiness-probe-works-against-a-real-database
  (let [{:keys [db tmp-file]} (sys/connect-sql-database {:engine :sqlite})]
    (with-open [server (ServerSocket. 0)]
      (let [port (.getLocalPort server)
            registry (actors-health/make-actors-registry
                      {:db db
                       :broker {:host "127.0.0.1" :port port}
                       :lease nil
                       :owned-fn nil})
            ready (health-http/make-ready-handler registry)
            health (health-http/make-health-handler registry)
            live (health-http/make-live-handler registry)]
        (try
          (testing "a starting node is live but not ready"
            (let [resp (ready {})]
              (is (= 503 (:status resp)))
              (is (= "starting" (:lifecycle (json/parse-string (:body resp) true))))
              (is (= 200 (:status (live {}))))))

          (testing "once running, the database and broker probes make it ready"
            (hr/set-lifecycle! registry :running)
            (let [resp (ready {})]
              (is (= 200 (:status resp)) (pr-str (:body resp))))
            (let [body (-> (health {}) :body (json/parse-string true))]
              (is (= "up" (get-in body [:components :db :status])))
              (is (= "up" (get-in body [:components :broker :status])))
              (is (true? (:ready? body)))))

          (testing "the evaluation is cached, so probes do not stampede the db"
            (let [body (-> (health {}) :body (json/parse-string true))]
              (is (true? (:cached? body)))))

          (testing "draining takes it out of rotation while it is still alive"
            (hr/set-lifecycle! registry :draining)
            (is (= 503 (:status (ready {}))))
            (is (= 200 (:status (live {})))))

          (finally
            (hr/stop-registry! registry)
            (when tmp-file (.delete ^java.io.File tmp-file))))))))

(deftest a-broken-dependency-fails-readiness
  ;; The broker port is closed, so the probe must fail -- this is the check that
  ;; replaces "the port is open" in the compose healthcheck. One failure is damped;
  ;; a consecutive one is a real down.
  (let [{:keys [db tmp-file]} (sys/connect-sql-database {:engine :sqlite})
        registry (actors-health/make-actors-registry
                  {:db db :broker {:host "127.0.0.1" :port 1} :lease nil :owned-fn nil
                   :ttl-ms 0
                   :lifecycle :running})
        ready (health-http/make-ready-handler registry)]
    (try
      (let [first-pass (hr/evaluate registry)]
        (is (= :degraded (get-in first-pass [:components :broker :status]))
            "one failed probe is damped to degraded")
        (is (true? (:healthy? first-pass))))
      (let [second-pass (hr/evaluate registry)]
        (is (= :down (get-in second-pass [:components :broker :status]))
            "a second consecutive failure is a real down")
        (is (false? (:healthy? second-pass))))
      (testing "and readiness reports it"
        (is (= 503 (:status (ready {})))))
      (finally
        (hr/stop-registry! registry)
        (when tmp-file (.delete ^java.io.File tmp-file))))))
