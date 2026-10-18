# Proxy gateway: nginx + its config. See docker/nginx/Dockerfile for why this is
# an image instead of a bind mount.
FROM nginx:1.27-alpine
COPY election-system/observability/nginx/nginx.conf /etc/nginx/nginx.conf
# No `nginx -t` here: the upstream server names (async-proxy-1, election-api-1…)
# only resolve inside the compose network, so validating at build time fails on
# DNS rather than on the config. The container's healthcheck runs `nginx -t` at
# runtime, where the names exist.
