import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

// P2 sustained load: >=10,000 VUs for >=120s while instances are scaled
// 3 -> 5 -> 2 by loadtest/scaling.sh.
//
// The k6 process runs OUTSIDE the application pods, as required. Controlled
// rejections (429 from admission control) are counted separately; everything
// else counts against the <1% error criterion.

const BASE = __ENV.BASE_URL || 'http://client-api:8080';
const USER = __ENV.K6_USER || 'field-user';
const PASS = __ENV.K6_PASSWORD || 'field-pass-2026';
const INTERNAL_KEY = __ENV.INTERNAL_KEY || '';

const controlled = new Counter('controlled_rejections');
const unexpected = new Counter('unexpected_errors');
const unexpectedStatus = new Counter('unexpected_status');
const unexpectedNetwork = new Counter('unexpected_network');
const unexpectedHTTP = new Counter('unexpected_http');
const unexpected4xx = new Counter('unexpected_4xx');
const unexpected5xx = new Counter('unexpected_5xx');
const errorRate = new Rate('error_rate_excl_controlled');

export const options = {
  scenarios: {
    sustained: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 10000),
      duration: __ENV.DURATION || '120s',
      gracefulStop: '30s',
    },
  },
  thresholds: {
    error_rate_excl_controlled: ['rate<0.01'],
    'http_req_duration{expected_response:true}': ['p(95)<1000'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  if (INTERNAL_KEY) {
    return { token: '' };
  }
  const res = http.post(`${BASE}/auth/login`, JSON.stringify({ username: USER, password: PASS }), {
    headers: { 'Content-Type': 'application/json' },
    tags: { endpoint: 'login' },
  });
  if (res.status !== 200) {
    throw new Error(`login failed: ${res.status} ${res.body}`);
  }
  return { token: res.json('data.accessToken') };
}

export default function (data) {
  const headers = INTERNAL_KEY
    ? { 'X-Internal-Key': INTERNAL_KEY, 'X-Correlation-ID': `k6-${__VU}-${__ITER}` }
    : { Authorization: `Bearer ${data.token}`, 'X-Correlation-ID': `k6-${__VU}-${__ITER}` };
  const res = http.get(`${BASE}/hazards?limit=50`, {
    headers,
    tags: { endpoint: 'hazards', role: USER === 'media-user' ? 'media' : 'field' },
    responseCallback: http.expectedStatuses(200, 429),
  });

  if (res.status === 200) {
    check(res, { 'status is 200': (r) => r.status === 200 });
    errorRate.add(false);
  } else if (res.status === 429) {
    controlled.add(1);
    errorRate.add(false); // controlled rejection, not an error
  } else {
    unexpected.add(1);
    unexpectedStatus.add(1, { status: String(res.status) });
    if (res.status === 0) {
      unexpectedNetwork.add(1);
    } else {
      unexpectedHTTP.add(1);
      if (res.status >= 400 && res.status < 500) {
        unexpected4xx.add(1);
      } else if (res.status >= 500) {
        unexpected5xx.add(1);
      }
    }
    errorRate.add(true);
  }

  // Keep the VU loop realistic; the arrival rate is what stresses the system.
  sleep(Number(__ENV.SLEEP || 0.05));
}
