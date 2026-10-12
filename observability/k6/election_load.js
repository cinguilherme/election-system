import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// Treat 200, 202, and 409 as expected application responses
http.setResponseCallback(http.expectedStatuses(200, 202, 409));

const voteIngestDuration = new Trend('vote_ingest_duration', true);
const tallyQueryDuration = new Trend('tally_query_duration', true);
const serverErrors = new Counter('server_5xx_or_timeout_errors');

const BASE_URL = __ENV.API_URL || 'http://election-api:8080';
const STATES = (__ENV.STATES || 'sp,rj,mg,ba,pr,rs,pe,ce,pa,sc').split(',');
const PRECINCTS_PER_STATE = parseInt(__ENV.PRECINCTS_PER_STATE || '25', 10);
const SLEEP_S = parseFloat(__ENV.SLEEP_S || '0.002');
const REQ_TIMEOUT = __ENV.REQ_TIMEOUT || '5s';
const CANDIDATES = ['alice', 'bob', 'charlie', 'diana'];

function selectElectionContext(state, iter) {
  const ctxIdx = iter % 3;
  if (ctxIdx === 0) {
    return 'president-2026';
  } else if (ctxIdx === 1) {
    return `senate-${state}-2026`;
  }
  return `governor-${state}-2026`;
}

export const options = {
  setupTimeout: '60s',
  teardownTimeout: '90s',
  stages: [
    // Stage 1: Warmup to 50 VUs
    { duration: __ENV.STAGE_1_DURATION || '20s', target: parseInt(__ENV.STAGE_1_VUS || '50', 10) },
    // Stage 2: Ramp to 150 VUs (Sustained High Load across 21 Elections / 750 Precincts)
    { duration: __ENV.STAGE_2_DURATION || '40s', target: parseInt(__ENV.STAGE_2_VUS || '150', 10) },
    // Stage 3: Ramp to 300 VUs (Heavy Stress on 1-CPU / 1.5GB JVM Containers)
    { duration: __ENV.STAGE_3_DURATION || '45s', target: parseInt(__ENV.STAGE_3_VUS || '300', 10) },
    // Stage 4: Ramp to 500 VUs (Breaking-Point Pressure)
    { duration: __ENV.STAGE_4_DURATION || '45s', target: parseInt(__ENV.STAGE_4_VUS || '500', 10) },
    // Stage 5: Cooldown
    { duration: __ENV.STAGE_5_DURATION || '15s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<250'],
    vote_ingest_duration: ['p(95)<200'],
    tally_query_duration: ['p(95)<500'],
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

export default function () {
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

  if (roll < 0.85) {
    // 85%: Cast unique valid ballot via async-proxy -> JetStream (tell! profile)
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
    }
    check(res, {
      'vote ingested by async-proxy (200/202)': (r) => r.status === 200 || r.status === 202,
    });
  } else if (roll < 0.95) {
    // 10%: Submit deterministic voter ID per (election, precinct) -> deduplicated in O(1) by actor
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
    }
    check(res, {
      'duplicate vote ingested for actor dedup (200/202/409)': (r) =>
        r.status === 200 || r.status === 202 || r.status === 409,
    });
  } else {
    // 5%: Real-time election tally query (ask-sync! profile -> direct actor stream query)
    const tallyRes = http.get(`${BASE_URL}/v1/elections/${electionId}/tally`, {
      timeout: REQ_TIMEOUT,
    });
    tallyQueryDuration.add(tallyRes.timings.duration);
    if (tallyRes.status === 0 || tallyRes.status >= 500) {
      serverErrors.add(1);
    }
    check(tallyRes, {
      'live tally is 200': (r) => r.status === 200,
    });
  }

  if (SLEEP_S > 0) {
    sleep(SLEEP_S);
  }
}

export function teardown() {
  const sampleElections = [
    'president-2026',
    `senate-${STATES[0]}-2026`,
    `governor-${STATES[1 % STATES.length]}-2026`,
  ];
  // Wait for JetStream backlog drain and periodic actor flushes across the 750 precincts
  let auditRes = null;
  for (let i = 0; i < 12; i++) {
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
