# API-tier load balancer: nginx + its config, as an image.
#
# The config used to be bind-mounted from the repo, which made every worker
# machine responsible for having the file at the right relative path. Baking it
# in means a worker needs nothing but the compose files and the registry, and it
# makes the config versioned by image tag rather than by whatever was on disk.
FROM nginx:1.27-alpine
COPY election-system/docker/nginx/nginx.conf /etc/nginx/nginx.conf
# No `nginx -t` here: the upstream server names (async-proxy-1, election-api-1…)
# only resolve inside the compose network, so validating at build time fails on
# DNS rather than on the config. The container's healthcheck runs `nginx -t` at
# runtime, where the names exist.
