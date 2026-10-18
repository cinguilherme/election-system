# Prometheus with its scrape config and alert rules baked in.
FROM prom/prometheus:v2.53.0
COPY election-system/observability/prometheus/prometheus.yml /etc/prometheus/prometheus.yml
COPY election-system/observability/prometheus/rules /etc/prometheus/rules
