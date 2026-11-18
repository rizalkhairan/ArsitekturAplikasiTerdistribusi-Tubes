#!/usr/bin/env bash
# P2 criterion 7: throughput(3 instances) >= 2.0 x throughput(1 instance).
# Uses a fixed closed-loop VU count so the only variable is Aggregator capacity.
set -euo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
BASE="${BASE_URL:-http://client-api:8080}"
OUT="${OUT:-loadtest/results}"
VUS="${VUS:-2000}"
DURATION="${DURATION:-60s}"
PYTHON="${PYTHON:-}"
if [ -z "$PYTHON" ]; then
  if python3 -c 'import sys' >/dev/null 2>&1; then PYTHON=python3; else PYTHON=python; fi
fi
mkdir -p "$OUT"

if [ "${SKIP_TTL_ENV:-0}" != "1" ]; then
  kubectl -n "$NS" set env deployment/client-api TOKEN_TTL_SECONDS=3600 >/dev/null
  kubectl -n "$NS" set env deployment/auth ACCESS_TOKEN_TTL_SECONDS=3600 >/dev/null
  kubectl -n "$NS" rollout status deployment/client-api --timeout=120s >/dev/null
  kubectl -n "$NS" rollout status deployment/auth --timeout=120s >/dev/null
fi

run() {
  local replicas="$1" label="$2"
  kubectl -n "$NS" scale deployment/aggregator --replicas="$replicas"
  kubectl -n "$NS" rollout status deployment/aggregator --timeout=120s
  sleep 5
  # Target the Aggregator Service directly so the measurement isolates the
  # Aggregator read path; client-api is a separate stateless tier.
  k6 run \
    -e BASE_URL="${AGG_URL:-http://aggregator:8085}" \
    -e INTERNAL_KEY="${INTERNAL_KEY:-dev-internal-key}" \
    -e VUS="$VUS" -e DURATION="$DURATION" -e SLEEP=0 \
    --summary-export "$OUT/throughput-$label.json" \
    loadtest/k6/sustained.js > "$OUT/throughput-$label.log" 2>&1
}

run 1 one
run 3 three

"$PYTHON" - "$OUT/throughput-one.json" "$OUT/throughput-three.json" <<'PY'
import json, sys
one = json.load(open(sys.argv[1]))["metrics"]["http_reqs"]["rate"]
three = json.load(open(sys.argv[2]))["metrics"]["http_reqs"]["rate"]
ratio = three / one if one else 0
print(f"1 instance : {one:.2f} req/s")
print(f"3 instances: {three:.2f} req/s")
print(f"ratio      : {ratio:.3f} (target >= 2.0)")
sys.exit(0 if ratio >= 2.0 else 1)
PY
