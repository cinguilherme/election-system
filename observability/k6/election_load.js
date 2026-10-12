import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.API_URL || 'http://election-api:8080';
const ELECTION_ID = __ENV.ELECTION_ID || 'national-2026';
const PRECINCT_COUNT = parseInt(__ENV.PRECINCT_COUNT || '50', 10);
const CANDIDATES = ['alice', 'bob', 'charlie', 'diana'];

export const options = {
  stages: [
    { duration: __ENV.STAGE_1_DURATION || '10s', target: parseInt(__ENV.STAGE_1_VUS || '20', 10) },
    { duration: __ENV.STAGE_2_DURATION || '20s', target: parseInt(__ENV.STAGE_2_VUS || '50', 10) },
    { duration: __ENV.STAGE_3_DURATION || '5s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'],
  },
};

export function setup() {
  const healthRes = http.get(`${BASE_URL}/health`);
  check(healthRes, {
    'API health is 200': (r) => r.status === 200,
  });
  return { startTime: Date.now() };
}

export default function () {
  const roll = Math.random();
  const precinctIdx = Math.floor(Math.random() * PRECINCT_COUNT);
  const precinctId = `precinct-${precinctIdx}`;
  const candidateId = CANDIDATES[(precinctIdx + __ITER) % CANDIDATES.length];

  if (roll < 0.85) {
    // 85%: Cast unique valid ballot
    const voterId = `voter-vu${__VU}-it${__ITER}-${Date.now()}`;
    const payload = JSON.stringify({
      precinct_id: precinctId,
      voter_id: voterId,
      candidate_id: candidateId,
    });
    const res = http.post(
      `${BASE_URL}/v1/elections/${ELECTION_ID}/votes`,
      payload,
      { headers: { 'Content-Type': 'application/json' } }
    );
    check(res, {
      'valid vote accepted (200)': (r) => r.status === 200,
    });
  } else if (roll < 0.95) {
    // 10%: Submit a deterministic voter ID per precinct to exercise O(1) in-memory duplicate rejection (409)
    const dupVoterId = `dup-voter-${precinctId}`;
    const payload = JSON.stringify({
      precinct_id: precinctId,
      voter_id: dupVoterId,
      candidate_id: candidateId,
    });
    const res = http.post(
      `${BASE_URL}/v1/elections/${ELECTION_ID}/votes`,
      payload,
      { headers: { 'Content-Type': 'application/json' } }
    );
    check(res, {
      'duplicate or first seed vote handled (200 or 409)': (r) => r.status === 200 || r.status === 409,
    });
  } else {
    // 5%: Real-time national tally & audit queries
    const tallyRes = http.get(`${BASE_URL}/v1/elections/${ELECTION_ID}/tally`);
    check(tallyRes, {
      'live tally is 200': (r) => r.status === 200,
    });
  }

  sleep(0.01);
}

export function teardown() {
  // Allow periodic actor tick flushes to settle, then query the audit endpoint
  sleep(3);
  const auditRes = http.get(`${BASE_URL}/v1/elections/${ELECTION_ID}/audit`);
  check(auditRes, {
    'audit endpoint returns 200': (r) => r.status === 200,
  });
  if (auditRes.status === 200) {
    console.log(`[k6 Election Audit Summary] ${auditRes.body}`);
  }
}
