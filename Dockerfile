FROM clojure:temurin-21-tools-deps

WORKDIR /workspace

# Install diagnostic utilities and async-profiler for on-demand flamegraphs
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends curl procps ca-certificates && \
    ARCH=$(uname -m | sed -e 's/x86_64/x64/' -e 's/aarch64/arm64/') && \
    mkdir -p /opt/async-profiler && \
    curl -sL "https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-${ARCH}.tar.gz" | tar -xz -C /opt/async-profiler --strip-components=1 && \
    ln -s /opt/async-profiler/bin/asprof /usr/local/bin/asprof && \
    rm -rf /var/lib/apt/lists/*

# Copy local d-core ecosystem libraries required by election-system
COPY d-core-std /workspace/d-core-std
COPY d-core-metrics /workspace/d-core-metrics
COPY d-core-db /workspace/d-core-db
COPY d-core-http /workspace/d-core-http
COPY d-core /workspace/d-core
COPY d-core-async-messaging /workspace/d-core-async-messaging
COPY d-core-grpc /workspace/d-core-grpc
COPY d-core-async-proxy /workspace/d-core-async-proxy
COPY d-core-actor-system /workspace/d-core-actor-system
COPY election-system /workspace/election-system

WORKDIR /workspace/election-system

# Pre-fetch Maven dependencies into the container image layer
RUN clojure -Sthreads 1 -P

ENV ROLE=api
ENV HTTP_PORT=8080
ENV METRICS_PORT=8081
EXPOSE 8080/tcp 8081/tcp

CMD ["clojure", "-M", "-m", "election-system.main"]
