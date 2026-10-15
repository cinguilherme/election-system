#!/usr/bin/env bash
# Container CPU throttling / quota / memory / OOM, read from cAdvisor metrics
# already in Prometheus.
#
# This is the check that catches "every application dashboard looks healthy but
# the container is being throttled" -- the exact blind spot that hid the
# async-proxy bottleneck, where both proxies sat pinned at their 0.60 CPU quota
# and were throttled in 22-26% of CFS periods while all app metrics looked fine.
#
# Usage:  make throttling          (or)   ./observability/scripts/container-resources.sh
# Override the Prometheus endpoint with PROM=http://host:9090
set -euo pipefail

exec python3 - "${PROM:-http://localhost:9090}" <<'PY'
import json, sys, urllib.parse, urllib.request

prom = sys.argv[1]

def q(expr):
    url = f"{prom}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=15) as fh:
        body = json.load(fh)
    if body.get("status") != "success":
        raise SystemExit(f"Prometheus query failed: {body}")
    return {r["metric"].get("service") or "(host/root)": float(r["value"][1])
            for r in body["data"]["result"]}

throttled = q('sum by (service) (rate(container_cpu_cfs_throttled_periods_total[2m]))'
              ' / sum by (service) (rate(container_cpu_cfs_periods_total[2m]))')
usage     = q('sum by (service) (rate(container_cpu_usage_seconds_total{service!=""}[2m]))')
quota     = q('sum by (service) (container_spec_cpu_quota{service!=""}'
              ' / container_spec_cpu_period{service!=""})')
mem       = q('sum by (service) (container_memory_working_set_bytes{service!=""}) / 1024 / 1024')
oom       = q('sum by (service) (container_oom_events_total)')

if not usage:
    raise SystemExit(
        "No container metrics found.\n"
        "Is the `cadvisor` service running, scraped by Prometheus, and healthy?\n"
        "  docker compose ps cadvisor\n"
        "  open http://localhost:9090/targets"
    )

print(f"  {'service':26} {'throttled':>9}  {'cpu / quota (cores)':>21} {'mem MiB':>9} {'OOM':>5}")
for svc in sorted(usage, key=lambda k: -usage[k]):
    pct = throttled.get(svc, 0.0) * 100
    print(f"  {svc:26} {pct:8.1f}%  {usage[svc]:9.3f} / {quota.get(svc, 0.0):<7.2f} "
          f"{mem.get(svc, 0.0):9.1f} {oom.get(svc, 0.0):5.0f}")
PY
