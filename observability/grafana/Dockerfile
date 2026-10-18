# Grafana with provisioned datasources and dashboards baked in.
# `--chmod` for the same reason as the prometheus image: the container does not
# run as root, so permissions must be right as the files land.
FROM grafana/grafana:11.1.0
COPY --chmod=755 election-system/observability/grafana/provisioning /etc/grafana/provisioning
