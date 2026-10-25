# Loki with its config baked in, same pattern as Prometheus/Grafana: a worker needs
# nothing but the compose files and the registry.
FROM grafana/loki:3.4.2
COPY --chmod=644 election-system/observability/loki/config.yml /etc/loki/config.yml
