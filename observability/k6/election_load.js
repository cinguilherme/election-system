import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// Treat 200, 202, and 409 as expected application responses
http.setResponseCallback(http.expectedStatuses(200, 202, 409));

// Custom latency trends and business counters
const voteIngestDuration = new Trend('vote_ingest_duration', true);
const tallyQueryDuration = new Trend('tally_query_duration', true);
const precinctTallyDuration = new Trend('precinct_tally_duration', true);
const serverErrors = new Counter('server_5xx_or_timeout_errors');
const votesAccepted = new Counter('votes_accepted_total');
const votesDeduplicated = new Counter('votes_deduplicated_total');

const BASE_URL = __ENV.API_URL || 'http://election-api:8080';
const STATES = (__ENV.STATES || 'sp,rj,mg,ba,pr,rs,pe,ce,pa,sc').split(',');
const PRECINCTS_PER_STATE = parseInt(__ENV.PRECINCTS_PER_STATE || '25', 10);
const SLEEP_S = parseFloat(__ENV.SLEEP_S || '0.001');
const VOTER_SLEEP_S = parseFloat(__ENV.VOTER_SLEEP_S || SLEEP_S.toString());
const OBSERVER_SLEEP_S = parseFloat(__ENV.OBSERVER_SLEEP_S || '0.1');
const REQ_TIMEOUT = __ENV.REQ_TIMEOUT || '5s';
const CANDIDATES = ['alice', 'bob', 'charlie', 'diana'];

// Helper to calculate VU distribution between voters (~90%) and observers (~10%)
function calcVus(totalVus, ratio, minVal = 0) {
  if (totalVus === 0) return 0;
  return Math.max(minVal, Math.round(totalVus * ratio));
}

const s1Duration = __ENV.STAGE_1_DURATION || '20s';
const s2Duration = __ENV.STAGE_2_DURATION || '40s';
const s3Duration = __ENV.STAGE_3_DURATION || '45s';
const s4Duration = __ENV.STAGE_4_DURATION || '45s';
const s5Duration = __ENV.STAGE_5_DURATION || '15s';

const s1Total = parseInt(__ENV.STAGE_1_VUS || '50', 10);
const s2Total = parseInt(__ENV.STAGE_2_VUS || '150', 10);
const s3Total = parseInt(__ENV.STAGE_3_VUS || '300', 10);
const s4Total = parseInt(__ENV.STAGE_4_VUS || '500', 10);

export const options = {
  setupTimeout: '60s',
  teardownTimeout: '90s',
  scenarios: {
    // Dedicated Pool 1: High-throughput ballot casting (hotpath)
    voters: {
      executor: 'ramping-vus',
      exec: 'castVotes',
      startVUs: 0,
      stages: [
        { duration: s1Duration, target: parseInt(__ENV.VOTER_STAGE_1_VUS || calcVus(s1Total, 0.90, 5), 10) },
        { duration: s2Duration, target: parseInt(__ENV.VOTER_STAGE_2_VUS || calcVus(s2Total, 0.90, 10), 10) },
        { duration: s3Duration, target: parseInt(__ENV.VOTER_STAGE_3_VUS || calcVus(s3Total, 0.92, 20), 10) },
        { duration: s4Duration, target: parseInt(__ENV.VOTER_STAGE_4_VUS || calcVus(s4Total, 0.92, 30), 10) },
        { duration: s5Duration, target: 0 },
      ],
      gracefulRampDown: '5s',
    },
    // Dedicated Pool 2: Tally readers & public election observers
    tally_observers: {
      executor: 'ramping-vus',
      exec: 'observeTallies',
      startVUs: 0,
      stages: [
        { duration: s1Duration, target: parseInt(__ENV.OBSERVER_STAGE_1_VUS || calcVus(s1Total, 0.10, 2), 10) },
        { duration: s2Duration, target: parseInt(__ENV.OBSERVER_STAGE_2_VUS || calcVus(s2Total, 0.10, 5), 10) },
        { duration: s3Duration, target: parseInt(__ENV.OBSERVER_STAGE_3_VUS || calcVus(s3Total, 0.08, 10), 10) },
        { duration: s4Duration, target: parseInt(__ENV.OBSERVER_STAGE_4_VUS || calcVus(s4Total, 0.08, 15), 10) },
        { duration: s5Duration, target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<250'],
    vote_ingest_duration: ['p(95)<200'],
    tally_query_duration: ['p(95)<150'],
    server_5xx_or_timeout_errors: ['count<100'],
  },
};

export function setup() {
  let healthRes = null;
  for (let i = 0; i < 30; i++) {
    healthRes = http.get(`${BASE_URL}/health`);
    if (healthRes && healthRes.status === 200) {
      break;
    }
    sleep(1);
  }
  check(healthRes, {
    'API health is 200': (r) => r && r.status === 200,
  });
  return { startTime: Date.now() };
}

function selectElectionContext(state, iter) {
  const ctxIdx = iter % 3;
  if (ctxIdx === 0) {
    return 'president-2026';
  } else if (ctxIdx === 1) {
    return `senate-${state}-2026`;
  }
  return `governor-${state}-2026`;
}

// Scenario 1: Dedicated voting pool
export function castVotes() {
  const roll = Math.random();
  const stateIdx = (__VU + __ITER) % STATES.length;
  const state = STATES[stateIdx];
  const electionId = selectElectionContext(state, __ITER);
  const precinctIdx = Math.floor(Math.random() * PRECINCTS_PER_STATE);
  const precinctId = `${state}-p${precinctIdx}`;
  const candidateId = CANDIDATES[(precinctIdx + __ITER) % CANDIDATES.length];
  const reqParams = {
    headers: { 'Content-Type': 'application/json' },
    timeout: REQ_TIMEOUT,
  };

  if (roll < 0.90) {
    // 90%: Unique valid ballot submission
    const voterId = `voter-${state}-vu${__VU}-it${__ITER}-${Date.now()}`;
    const payload = JSON.stringify({
      precinct_id: precinctId,
      voter_id: voterId,
      candidate_id: candidateId,
      async: true,
    });
    const res = http.post(
      `${BASE_URL}/v1/elections/${electionId}/votes`,
      payload,
      reqParams
    );
    voteIngestDuration.add(res.timings.duration);
    if (res.status === 0 || res.status >= 500) {
      serverErrors.add(1);
    } else if (res.status === 200 || res.status === 202) {
      votesAccepted.add(1);
    }
    check(res, {
      'vote ingested by async-proxy (200/202)': (r) => r.status === 200 || r.status === 202,
    });
  } else {
    // 10%: Deterministic duplicate voter ID -> tests fast in-memory dedup
    const dupVoterId = `dup-voter-${electionId}-${precinctId}`;
    const payload = JSON.stringify({
      precinct_id: precinctId,
      voter_id: dupVoterId,
      candidate_id: candidateId,
      async: true,
    });
    const res = http.post(
      `${BASE_URL}/v1/elections/${electionId}/votes`,
      payload,
      reqParams
    );
    voteIngestDuration.add(res.timings.duration);
    if (res.status === 0 || res.status >= 500) {
      serverErrors.add(1);
    } else {
      votesDeduplicated.add(1);
    }
    check(res, {
      'duplicate vote ingested for actor dedup (200/202/409)': (r) =>
        r.status === 200 || r.status === 202 || r.status === 409,
    });
  }

  if (VOTER_SLEEP_S > 0) {
    sleep(VOTER_SLEEP_S);
  }
}

// Scenario 2: Dedicated observers pool
export function observeTallies() {
  const roll = Math.random();
  const stateIdx = (__VU + __ITER) % STATES.length;
  const state = STATES[stateIdx];
  const electionId = selectElectionContext(state, __ITER);
  const precinctIdx = Math.floor(Math.random() * PRECINCTS_PER_STATE);
  const precinctId = `${state}-p${precinctIdx}`;
  const reqParams = { timeout: REQ_TIMEOUT };

  if (roll < 0.75) {
    // 75%: National live election tally (cached DB query)
    const res = http.get(`${BASE_URL}/v1/elections/${electionId}/tally`, reqParams);
    tallyQueryDuration.add(res.timings.duration);
    if (res.status === 0 || res.status >= 500) {
      serverErrors.add(1);
    }
    check(res, {
      'live election tally is 200': (r) => r.status === 200,
    });
  } else if (roll < 0.90) {
    // 15%: Precinct-level tally
    const res = http.get(
      `${BASE_URL}/v1/elections/${electionId}/precincts/${precinctId}/tally`,
      reqParams
    );
    precinctTallyDuration.add(res.timings.duration);
    if (res.status === 0 || res.status >= 500) {
      serverErrors.add(1);
    }
    check(res, {
      'live precinct tally is 200': (r) => r.status === 200,
    });
  } else {
    // 10%: Election audit consistency check
    const res = http.get(`${BASE_URL}/v1/elections/${electionId}/audit`, reqParams);
    if (res.status === 0 || res.status >= 500) {
      serverErrors.add(1);
    }
    check(res, {
      'audit endpoint returns 200': (r) => r.status === 200,
    });
  }

  if (OBSERVER_SLEEP_S > 0) {
    sleep(OBSERVER_SLEEP_S);
  }
}

// Fallback default function if scenario exec is omitted
export default function () {
  castVotes();
}

export function teardown() {
  const sampleElections = [
    'president-2026',
    `senate-${STATES[0]}-2026`,
    `governor-${STATES[1 % STATES.length]}-2026`,
  ];
  // Wait for JetStream backlog drain and periodic actor flushes across the precincts
  let auditRes = null;
  for (let i = 0; i < 15; i++) {
    sleep(2);
    auditRes = http.get(`${BASE_URL}/v1/elections/${sampleElections[0]}/audit`, { timeout: '10s' });
    if (auditRes && auditRes.status === 200) {
      try {
        const body = JSON.parse(auditRes.body);
        if (body['consistent?'] === true) {
          break;
        }
      } catch (_e) {
        // continue polling
      }
    }
  }
  check(auditRes, {
    'audit endpoint returns 200': (r) => r && r.status === 200,
  });
  for (const eid of sampleElections) {
    const res = http.get(`${BASE_URL}/v1/elections/${eid}/audit`, { timeout: '10s' });
    if (res && res.status === 200) {
      console.log(`[k6 Multi-Election Audit: ${eid}] ${res.body}`);
    }
  }
}
