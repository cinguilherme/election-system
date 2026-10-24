(ns election-system.resilience.admin
  "Internal admin listener for a deployable role.

   One port (`:8082` by default) carries:
     GET  /_chaos/status | /_chaos/rules | /_chaos/journal   (fault control plane)
     GET  /live | /ready | /health                          (this role's probes)
     GET  /                                                 (identity + what is enabled)

   The listener is internal by design: it is not published in `docker-compose.yaml`,
   not routed by the `election-api` nginx, and not scraped by Prometheus. Requests
   to `/_chaos/*` require `X-Chaos-Token` whenever `CHAOS_ADMIN_TOKEN` is set."
  (:require [d-core.chaos.http :as chaos-http]
            [d-core.health.http :as health-http]
            [ring.adapter.jetty :as jetty]
            [cheshire.core :as json]
            [clojure.string :as str])
  (:import (java.security MessageDigest)))

(def ^:private default-admin-port 8082)

(defn- json-response
  [status body]
  {:status status
   :headers {"content-type" "application/json"}
   :body (json/generate-string body)})

(defn- constant-time=
  [a b]
  (and a b
       (MessageDigest/isEqual (.getBytes ^String a "UTF-8")
                              (.getBytes ^String b "UTF-8"))))

(defn- chaos-authorized?
  [token req]
  (or (str/blank? (str token))
      (constant-time= (str token) (get-in req [:headers "x-chaos-token"]))))

(defn make-handler
  "Builds the admin Ring handler.

   Options:
     :role             - :api | :actors (informational)
     :instance-id      - node identity
     :controller      - ChaosController (nil when chaos is disabled)
     :journal         - EventJournal (nil in :off journal-mode)
     :health-registry - this role's HealthRegistry (optional)
     :token           - shared secret required on `/_chaos/*`"
  [{:keys [role instance-id controller journal health-registry token]}]
  (let [chaos-handler (when (or controller journal)
                        (chaos-http/make-chaos-handler {:controller controller :journal journal}))
        live-handler (when health-registry (health-http/make-live-handler health-registry))
        ready-handler (when health-registry (health-http/make-ready-handler health-registry))
        health-handler (when health-registry (health-http/make-health-handler health-registry))
        probes? (some? health-registry)]
    (fn [req]
      (let [uri (:uri req)]
        (cond
          (and (= "/live" uri) live-handler)
          (live-handler req)

          (and (= "/ready" uri) ready-handler)
          (ready-handler req)

          (and (= "/health" uri) health-handler)
          (health-handler req)

          (and (= "/live" uri) (not probes?))
          (json-response 200 {:status "ok" :role (some-> role name) :probes? false})

          (str/starts-with? uri "/_chaos")
          (cond
            (not chaos-handler)
            (json-response 404 {:status "not-found" :reason "chaos-disabled"})

            (not (chaos-authorized? token req))
            (json-response 401 {:status "unauthorized"
                                :reason "missing or invalid X-Chaos-Token"})

            :else
            (or (chaos-handler req)
                (json-response 404 {:status "not-found" :uri uri})))

          (= "/" uri)
          (json-response 200 {:service "election-system"
                              :role (some-> role name)
                              :instance-id instance-id
                              :chaos {:enabled? (some? controller)
                                      :journal? (some? journal)}
                              :endpoints (cond-> ["/live" "/ready" "/health"]
                                           chaos-handler (into ["/_chaos/status"
                                                               "/_chaos/rules"
                                                               "/_chaos/journal"]))})

          :else
          (json-response 404 {:status "not-found" :uri uri}))))))

(defn start-server!
  "Starts the admin listener and returns the Jetty server.
   Options are `make-handler`'s plus `:port` and `:max-threads`."
  [{:keys [port max-threads] :or {port default-admin-port max-threads 4} :as opts}]
  (let [handler (make-handler (dissoc opts :port :max-threads))]
    (jetty/run-jetty handler {:port (int port)
                              :max-threads (int max-threads)
                              :min-threads 1
                              :join? false})))

(defn stop-server!
  [server]
  (when server
    (.stop ^org.eclipse.jetty.server.Server server)))
