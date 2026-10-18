ARG BASE_IMAGE=d-core-base:latest
FROM ${BASE_IMAGE}

WORKDIR /workspace

# Copy dependency manifests first to leverage Docker layer caching
COPY d-core-std/deps.edn /workspace/d-core-std/deps.edn
COPY d-core-metrics/deps.edn /workspace/d-core-metrics/deps.edn
COPY d-core-db/deps.edn /workspace/d-core-db/deps.edn
COPY d-core-http/deps.edn /workspace/d-core-http/deps.edn
COPY d-core/deps.edn /workspace/d-core/deps.edn
COPY d-core-async-messaging/deps.edn /workspace/d-core-async-messaging/deps.edn
COPY d-core-grpc/deps.edn /workspace/d-core-grpc/deps.edn
COPY d-core-async-proxy/deps.edn /workspace/d-core-async-proxy/deps.edn
COPY d-core-actor-system/deps.edn /workspace/d-core-actor-system/deps.edn
COPY d-core-leader-election/deps.edn /workspace/d-core-leader-election/deps.edn
COPY election-system/deps.edn /workspace/election-system/deps.edn

WORKDIR /workspace/election-system

# Pre-fetch Maven dependencies into the container image layer (cached unless deps.edn changes)
RUN clojure -P

# Copy application source code
COPY d-core-std /workspace/d-core-std
COPY d-core-metrics /workspace/d-core-metrics
COPY d-core-db /workspace/d-core-db
COPY d-core-http /workspace/d-core-http
COPY d-core /workspace/d-core
COPY d-core-async-messaging /workspace/d-core-async-messaging
COPY d-core-grpc /workspace/d-core-grpc
COPY d-core-async-proxy /workspace/d-core-async-proxy
COPY d-core-actor-system /workspace/d-core-actor-system
COPY d-core-leader-election /workspace/d-core-leader-election
COPY election-system /workspace/election-system

ENV ROLE=api
ENV HTTP_PORT=8080
ENV METRICS_PORT=8081
EXPOSE 8080/tcp 8081/tcp

CMD ["clojure", "-M", "-m", "election-system.main"]
