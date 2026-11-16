#!/usr/bin/env bash
# P2 sustained load with a scripted 3 -> 5 -> 2 rollout of the Aggregator.
# Run from the repository root on the demo cluster.
set -euo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
BASE="${BASE_URL:-http://client-api:8080}"
OUT="${OUT:-loadtest/results}"
mkdir -p "$OUT"

# Raise the token TTL for the duration of the load test. The 60s production TTL
# would otherwise turn a load test into an auth-refresh test.
kubectl -n "$NS" set env deployment/client-api TOKEN_TTL_SECONDS=3600 >/dev/null
kubectl -n "$NS" set env deployment/auth ACCESS_TOKEN_TTL_SECONDS=3600 >/dev/null

kubectl -n "$NS" scale deployment/aggregator --replicas=3
kubectl -n "$NS" rollout status deployment/aggregator --timeout=120s

echo "[load] starting 10k VUs / 120s at $(date -u +%FT%TZ)"
k6 run \
  -e BASE_URL="$BASE" \
  -e VUS="${VUS:-10000}" \
  -e DURATION="${DURATION:-120s}" \
  --out "json=$OUT/scaling-raw.json" \
  --summary-export "$OUT/scaling-summary.json" \
  loadtest/k6/sustained.js &
K6_PID=$!

sleep 20
echo "[load] scale 3 -> 5"
kubectl -n "$NS" scale deployment/aggregator --replicas=5
sleep 40
echo "[load] scale 5 -> 2"
kubectl -n "$NS" scale deployment/aggregator --replicas=2

set +e
wait "$K6_PID"
K6_STATUS=$?
set -e

kubectl -n "$NS" get pods -l app=aggregator -o wide > "$OUT/scaling-pods.txt" || true
kubectl -n "$NS" describe deployment/aggregator > "$OUT/scaling-deployment.txt" || true
kubectl -n "$NS" scale deployment/aggregator --replicas=3 >/dev/null

echo "[load] k6 exit=$K6_STATUS"
echo "[load] summaries in $OUT"
exit "$K6_STATUS"
