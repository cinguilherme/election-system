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

## F4 — D2 rebalance RTO is not measured yet (tooling, pending rerun)

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
