#!/usr/bin/env bash
# P2 criterion 8: rolling update during load with zero failed requests.
# Triggers a real pod-template change, then waits for the rollout to finish.
set -euo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
BASE="${BASE_URL:-http://client-api:8080}"
OUT="${OUT:-loadtest/results}"
mkdir -p "$OUT"

kubectl -n "$NS" set env deployment/client-api TOKEN_TTL_SECONDS=3600 >/dev/null
kubectl -n "$NS" set env deployment/auth ACCESS_TOKEN_TTL_SECONDS=3600 >/dev/null
kubectl -n "$NS" scale deployment/aggregator --replicas=3 >/dev/null
kubectl -n "$NS" rollout status deployment/aggregator --timeout=120s

k6 run \
  -e BASE_URL="$BASE" -e VUS="${VUS:-2000}" -e DURATION="${DURATION:-90s}" \
  --out "json=$OUT/rolling-raw.json" \
  --summary-export "$OUT/rolling-summary.json" \
  loadtest/k6/sustained.js &
K6_PID=$!

sleep 25
REV="$(date +%s)"
echo "[rolling] new revision $REV"
kubectl -n "$NS" set env deployment/aggregator ROLLOUT_REVISION="$REV"
kubectl -n "$NS" rollout status deployment/aggregator --timeout=180s
echo "[rolling] rollout complete"

set +e
wait "$K6_PID"
K6_STATUS=$?
set -e

python3 - "$OUT/rolling-summary.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))["metrics"]
unexpected = m.get("unexpected_errors", {}).get("count", 0)
controlled = m.get("controlled_rejections", {}).get("count", 0)
print(f"unexpected_errors={unexpected} controlled_rejections={controlled}")
sys.exit(0 if unexpected == 0 else 1)
PY
exit "$K6_STATUS"
