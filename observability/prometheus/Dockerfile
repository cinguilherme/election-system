# Prometheus with its scrape config and alert rules baked in.
#
# `--chmod` rather than a `RUN chmod`: the image runs as `nobody`, so a chmod
# step would execute without the privileges to change ownership/permissions, and
# a file copied in at 0600 is unreadable to the server -- whose only symptom is
# "error loading config ... one or more errors occurred".
FROM prom/prometheus:v2.53.0
COPY --chmod=644 election-system/observability/prometheus/prometheus.yml /etc/prometheus/prometheus.yml
COPY --chmod=755 election-system/observability/prometheus/rules /etc/prometheus/rules
