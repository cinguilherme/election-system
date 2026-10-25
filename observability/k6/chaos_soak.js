// Steady soak load for chaos experiments.
//
// 100 constant VUs, no ramps: the point is a system whose load is *stable* while chaos is
// applied, so any change in latency or errors is attributable to the experiment and not
// to the load generator. Mostly reads (the audit path) with ~10% writes (the vote path).
//
// No thresholds on purpose: during an experiment, failures are the signal, not a gate.
// Watch the Grafana dashboard or the k6 summary; a chaos drill's own claims are the
// authoritative verdict.
//
//   k6 run observability/k6/chaos_soak.js
//   API_URL=http://127.0.0.1:8080 DURATION=45m EXP_ID=exp-123 k6 run ...
//
// EXP_ID is forwarded as x-experiment-id so the chaos journal can be correlated with the
// load that was running at the time.

import http from 'k6/http'
import { check, sleep } from 'k6'

const BASE = __ENV.API_URL || 'http://127.0.0.1:8080'
const ELECTION = __ENV.ELECTION_ID || 'national-2026'
const DURATION = __ENV.DURATION || '30m'
const VUS = Number(__ENV.VUS || 100)
const WRITE_RATIO = Number(__ENV.WRITE_RATIO || 0.1)
const EXP_ID = __ENV.EXP_ID || ''

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      gracefulStop: '15s',
    },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  // No thresholds: see the header.
}

function headers() {
  const h = { 'content-type': 'application/json' }
  if (EXP_ID) h['x-experiment-id'] = EXP_ID
  return h
}

export default function () {
  if (Math.random() < WRITE_RATIO) {
    const res = http.post(
      `${BASE}/v1/elections/${ELECTION}/votes`,
      JSON.stringify({
        precinct_id: `p-${1 + Math.floor(Math.random() * 4)}`,
        // Unique per iteration: a repeated ballot would make write errors look like
        // rebalance damage.
        voter_id: `soak-${__VU}-${__ITER}`,
        candidate_id: Math.random() < 0.5 ? 'alice' : 'bob',
      }),
      { headers: headers(), tags: { kind: 'vote' } },
    )
    check(res, { 'vote accepted': (r) => r.status === 200 || r.status === 202 })
  } else {
    const res = http.get(`${BASE}/v1/elections/${ELECTION}/audit`, {
      headers: headers(),
      tags: { kind: 'audit' },
    })
    // Only the status is checked per request. `consistent?` compares the live actor
    // tally with the DB count and flaps while writes are in flight -- with 100 VUs
    // writing, ~3% of reads see the window between the two. Data integrity is asserted
    // by the drill's :audit-after invariant, after the writes settle, not per request.
    check(res, { 'audit 200': (r) => r.status === 200 })
  }

  // ~1s think time with 100 VUs -> roughly 80-100 rps of stable, mixed traffic.
  sleep(0.9 + Math.random() * 0.2)
}
