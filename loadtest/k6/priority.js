import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

// P5 criterion 5: under overload, Tim Lapangan (field) keeps being served while
// Media is rejected in a controlled way. Run with ALWAYS:
//   k6 run -e VUS=4000 loadtest/k6/priority.js
// Field VUs outnumber media VUs only in the metric labels; both are identical
// clients except for credentials.

const BASE = __ENV.BASE_URL || 'http://client-api:8080';
const mediaControlled = new Counter('media_controlled_rejections');
const fieldControlled = new Counter('field_controlled_rejections');
const fieldServed = new Counter('field_served');
const mediaServed = new Counter('media_served');
const fieldErrorRate = new Rate('field_error_rate');

export const options = {
  scenarios: {
    field: {
      executor: 'constant-vus',
      exec: 'field',
      vus: Number(__ENV.FIELD_VUS || 3000),
      duration: __ENV.DURATION || '90s',
    },
    media: {
      executor: 'constant-vus',
      exec: 'media',
      vus: Number(__ENV.MEDIA_VUS || 3000),
      duration: __ENV.DURATION || '90s',
    },
  },
  thresholds: {
    field_error_rate: ['rate<0.05'],
  },
};

export function setup() {
  return {
    field: login('field-user', 'field-pass-2026'),
    media: login('media-user', 'media-pass-2026'),
  };
}

function login(username, password) {
  const res = http.post(`${BASE}/auth/login`, JSON.stringify({ username, password }), {
    headers: { 'Content-Type': 'application/json' },
    tags: { endpoint: 'login' },
  });
  if (res.status !== 200) throw new Error(`login ${username} failed: ${res.status}`);
  return res.json('data.accessToken');
}

function hit(token, role) {
  const res = http.get(`${BASE}/hazards?limit=20`, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { endpoint: 'hazards', role },
    responseCallback: http.expectedStatuses(200, 429),
  });
  return res.status;
}

export function field(data) {
  const status = hit(data.field, 'field');
  if (status === 200) {
    fieldServed.add(1);
    fieldErrorRate.add(false);
  } else if (status === 429) {
    fieldControlled.add(1);
    fieldErrorRate.add(true);
  } else {
    fieldErrorRate.add(true);
  }
  sleep(0.02);
}

export function media(data) {
  const status = hit(data.media, 'media');
  if (status === 200) {
    mediaServed.add(1);
  } else if (status === 429) {
    mediaControlled.add(1);
  }
  sleep(0.02);
}
