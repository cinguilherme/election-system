# Chaos lab findings

Observations from running the RFC drills against the compose lab. Findings are recorded
here with their status; a finding is not a decision — some need a code fix, some a
threshold change, some only a note in the RFC.

Lab topology: 2 API replicas, 2 actors replicas, 32 partitions, chaos overlay enabled,
`CHAOS_TOKEN` set, cockpit at `127.0.0.1:8090`, engine at `127.0.0.1:8088`.
Date: 2026-10-25.

## F1 — actors do not recover the live tally after a replica loss (open)

**Observed.** After an actors replica was killed and restarted (during D2), the audit
reconciled against the database but the live actor tally did not:

```
live-actor-tally.total-votes: 372
db-audit.ballot-count:       519
consistent?:                 false
```

The same divergence appeared after every actor/API restart, and `make reset-data` (truncate
+ purge streams) is what restores a consistent baseline. Until then, the drills refuse to
start at steady state — which is the correct behaviour (a drill on an unsteady system
proves nothing), but it masks a genuine recovery gap.

**Why it matters.** A node loss leaves the in-memory tally short of the durable count.
Users of `/audit` see `consistent?: false` indefinitely, and any claim about "writes
continue during a rebalance" is measured against a total that no longer matches the DB.

**Not investigated yet.** The likely area is tally recovery on start (snapshot + stream
replay): the actors restart with a load of partitions but the tally does not replay every
ballot it missed. This is a code fix, not a chaos-tooling fix.

**Reproduce.** Kill one actors replica (`POST /api/scenarios/drill-rebalance-actors-replica/run`
or the cockpit), wait for the restart, then compare
`GET /v1/elections/national-2026/audit` fields. `make reset-data` clears it.

## F2 — API replica failover costs ~16 s and ~5% of requests (measured)

**Observed.** D1 (`drill-failover-api-replica`, `replica: 2`, clean baseline):

| Claim | Target | Measured |
| :-- | :-- | :-- |
| RTO | ≤ 15 s | **16.2 s** |
| Error budget | ≤ 0.5% | **5.2%** (7/134) |

The kill and restart both fired, the audit invariant passed, and no faults were left
behind. This is the real failover cost of the current balancer configuration
(`max_fails=3 fail_timeout=10s`, no `proxy_next_upstream` on the API upstream).

**Decision needed**: tune nginx (next_upstream / shorter fail_timeout) and re-measure, or
set the RFC target to what the system actually does. Both are legitimate; only one should
be claimed.

## F3 — the failback claim measures the service, not the replica (tooling, open)

The D1 `:failback` claim passed with `0 ms`: it measures availability through the
balancer, and the surviving replica served traffic throughout, so "failback" never
happened from the balancer's point of view. The claim needs a watch on the **restarted
replica's own** `/health` (with `:act :last`), not a probe through the LB.

## F4 — D2 runs correctly now; the rebalance does not happen within 45 s (measured, under load)

With the tooling fixed (kill resolves to the replica being watched, verified in the engine:
`actors-3` = 172.28.0.20 = registry replica 2; watch on replica 1 = 172.28.0.18), D2 ran
against the 2-replica lab **while the 100-VU soak was running**:

| Evidence | Result |
| :-- | :-- |
| Kill / restart steps | fired on `election-system-election-actors-3` (replica 2) |
| Survivor's owned partitions | **16 → stayed 16** for the whole 45 s window (claim expected 32) |
| Write error budget | **100%** (168/168 failed) — the API answered **503 circuit-breaker-open** |
| DB-side invariant | failed |

So the rebalance RTO is still unmeasured, but the reason is now the system, not the tool:
the survivor never reported taking over the dead replica's partitions, and the API's
`election-async-proxy` breaker opened under load and stopped serving. Two things to
separate — whether the rebalance ever happens and how long it takes, and whether the
breaker is too eager under load — before D2's numbers can be quoted.

Earlier revision of this finding (kept for the trail): the first D2 attempts killed the
watched container because replica identity was resolved twice against different orderings.
That is fixed and verified; the IP ordering is `actors-1=.18`, `actors-3=.20`, i.e. the
container names are not in IP order.

## F6 — the chaos runtime was disabled by `CHAOS_ENABLED=1` (fixed)

`env-bool` used `Boolean/parseBoolean`, which only accepts `"true"`. Every compose file
sets `CHAOS_ENABLED=1`, so the runtime was off: no admin listener on 8082, no journal, no
instrumentation. The control room showed every replica as unreachable because it probes
`:8082/health`, which did not exist. Fixed in `parse-bool` (accepts 1/true/yes/on) with a
test; `[chaos] enabled … admin-port=8082` now appears at startup.

## F7 — consistency is a property at rest, not under load (fixed in the drills)

While writes are in flight, the audit's live tally and the DB ballot count legitimately
differ (the tally leads/ lags by the in-flight window). The drills required
`consistent?: true` at steady state, so **every drill refused to start while the soak was
running** — this is what the failed runs with no steps were. The drills now assert that the
API and DB are serving and that the DB-side tallies sum to the ballot count; reconciliation
is a separate check at rest.

## F8 — the async-proxy breaker opens under load + a node loss (open)

During the driven soak (100 VUs) and an actor kill, the API's `election-async-proxy` breaker
opened and `/audit` returned 503 for the rest of the drill; the write probe failed 100%.
The breaker should half-open and close when the proxy recovers; it did not within the
window. This needs its own investigation: breaker reset behaviour under sustained load and
recovery.

## F4 (original) — tooling history

The first D2 attempt killed the container that was being watched as the survivor. Cause:
replica identity was resolved twice, against two orderings (the registry sorts by IP, docker
by container name), and a kill changes the set of replicas so the registry renumbers.

Fixed since: identity is captured once per run (container names + target snapshot), and a
kill resolves the registry's IP to a container name. The live run view (needed to follow a
drill in the UI) is also fixed and verified. A clean D2 run is pending; **do not cite a
rebalance RTO until it exists.**

## F5 — the UI could not boot (fixed)

`localhost:8090` was blank: the JS bundle was served with no `Content-Type` (Ring's
`mime.types` resource is absent from the uberjar), so the browser refused the module
script. The BFF now names the types it serves. Verified: `text/javascript`, `text/css`,
deep links return the shell.

## F9 — baked configs drift from the repo; the dashboards broke that way (fixed, operational)

Every dashboard panel except the container one showed "No data". Prometheus's targets were
all down with `lookup election-api-1 on 127.0.0.11:53: no such host`: the **running** config
was a static one listing old container names (`election-api-1`, `async-proxy-2`, …), while
the repo's `prometheus.yml` had already moved to `dns_sd_configs` with IP resolution. The
config is **baked into the image** (`COPY --chmod`), and `make up` uses the registry's
`latest`, so the image still carried the old file.

Fix: rebuild `election-system-prometheus` from the current config and recreate the
container. 11/11 targets up, votes/actor/sql/http metrics flowing.

The lesson is the one that also bit the API and gateway images: **a compose file change is
not live until the image is rebuilt and pushed** (or built locally). `make up` will happily
run a stale baked config forever.

Under the still-running 100-VU soak the dashboards now show ~11 5xx/s from the API
(`http_requests_total{status=~"5.."}`) — the breaker finding F8, visible rather than hidden.
