#!/usr/bin/env bash
# P2 criterion 8: replace the Aggregator version during load testing without
# failed requests.
#
# The v2 image must already be present on every node. On the demo cluster
# without Docker, create the test-only version tag with:
#   ./scripts/tag-aggregator-version.sh
# Then run:
#   VUS=500 NEW_IMAGE=bnpb-gateway/aggregator:m2-v2 ./loadtest/rolling-update.sh
#
# Criterion 8 has no 10k-VU mandate. Use the highest load that gives zero
# controlled 429s and zero unexpected errors so "no failed requests" is
# unambiguous. k6 runs outside the application pods; the test-only NodePort
# Service is deleted on exit.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=loadtest/lib.sh
source "$ROOT/loadtest/lib.sh"

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
OUT="${OUT:-$ROOT/loadtest/results}"
VUS="${VUS:-500}"
DURATION="${DURATION:-120s}"
SLEEP="${SLEEP:-0.05}"
INTERNAL_KEY="${INTERNAL_KEY:-}"
NEW_IMAGE="${NEW_IMAGE:-bnpb-gateway/aggregator:m2-v2}"
ROLLOUT_AT="${ROLLOUT_AT:-30}"
ALLOW_CONTROLLED="${ALLOW_CONTROLLED:-0}"
NODEPORT="${NODEPORT:-30085}"

pick_python
mkdir -p "$OUT"
require_k6_local

cleanup() {
  delete_aggregator_nodeport
  restore_aggregator_replicas
}
trap cleanup EXIT

raise_token_ttl
kubectl -n "$NS" scale deployment/aggregator --replicas=3 >/dev/null
kubectl -n "$NS" rollout status deployment/aggregator --timeout=180s >/dev/null

old_image="$(kubectl -n "$NS" get deployment/aggregator -o jsonpath='{.spec.template.spec.containers[0].image}')"
if [ "$old_image" = "$NEW_IMAGE" ]; then
  echo "[rolling] deployment already uses $NEW_IMAGE; choose a new version tag" >&2
  exit 1
fi

create_aggregator_nodeport
if ! curl -fsS "$AGG_URL/readyz" >/dev/null 2>&1; then
  echo "[rolling] $AGG_URL/readyz is not ready" >&2
  exit 1
fi

kubectl -n "$NS" rollout history deployment/aggregator > "$OUT/rolling-history-before.txt" 2>&1 || true
echo "[rolling] version $old_image -> $NEW_IMAGE"
echo "[rolling] load: vus=$VUS duration=$DURATION rollout_at=${ROLLOUT_AT}s"

k6 run \
  -e "BASE_URL=$AGG_URL" \
  -e "INTERNAL_KEY=$INTERNAL_KEY" \
  -e "VUS=$VUS" \
  -e "DURATION=$DURATION" \
  -e "SLEEP=$SLEEP" \
  --summary-export "$OUT/rolling-summary.json" \
  "$ROOT/loadtest/k6/sustained.js" > "$OUT/rolling.log" 2>&1 &
K6_PID=$!

sleep "$ROLLOUT_AT"
echo "[rolling] replacing image at $(date -u +%H:%M:%S)"
kubectl -n "$NS" set image deployment/aggregator "aggregator=$NEW_IMAGE"
if ! kubectl -n "$NS" rollout status deployment/aggregator --timeout=240s; then
  echo "[rolling] rollout failed; reverting to $old_image" >&2
  kubectl -n "$NS" set image deployment/aggregator "aggregator=$old_image" || true
  kubectl -n "$NS" rollout status deployment/aggregator --timeout=180s || true
  set +e
  wait "$K6_PID"
  set -e
  exit 1
fi
echo "[rolling] rollout complete at $(date -u +%H:%M:%S)"

set +e
wait "$K6_PID"
k6_status=$?
set -e
echo "[rolling] k6 exit=$k6_status (threshold exit is informational)"

if [ ! -s "$OUT/rolling-summary.json" ]; then
  echo "[rolling] no summary produced" >&2
  exit 1
fi

read -r unexpected controlled total success < <("$PYTHON" - "$OUT/rolling-summary.json" <<'PY' | tr -d '\r'
import json
import sys

m = json.load(open(sys.argv[1], encoding="utf-8"))["metrics"]
total = m.get("http_reqs", {}).get("count", 0) or 0
unexpected = m.get("unexpected_errors", {}).get("count", 0) or 0
controlled = m.get("controlled_rejections", {}).get("count", 0) or 0
success = m.get("successful_responses", {}).get("count", 0) or 0
print(unexpected, controlled, total, success)
PY
)

images="$(kubectl -n "$NS" get pods -l app=aggregator \
  -o jsonpath='{range .items[*]}{.spec.containers[0].image}{"\n"}{end}' | sort -u | tr -d '\r')"
runs="$(kubectl -n "$NS" get pods -l app=aggregator \
  -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' | sort -u | tr -d '\r')"
kubectl -n "$NS" get pods -l app=aggregator -o wide > "$OUT/rolling-pods.txt" 2>&1 || true
kubectl -n "$NS" rollout history deployment/aggregator > "$OUT/rolling-history-after.txt" 2>&1 || true

echo "[rolling] requests=$total successful=$success unexpected=$unexpected controlled_429=$controlled"
echo "[rolling] pod images:"
printf '%s\n' "$images"
if [ "$runs" != "Running" ]; then
  echo "[rolling] not all aggregator pods are Running" >&2
  exit 1
fi
if [ "$images" != "$NEW_IMAGE" ]; then
  echo "[rolling] deployment pods are not all running $NEW_IMAGE" >&2
  exit 1
fi

failure=0
if [ "$unexpected" -ne 0 ]; then
  echo "[rolling] FAIL: $unexpected unexpected failed requests" >&2
  failure=1
fi
if [ "$ALLOW_CONTROLLED" != "1" ] && [ "$controlled" -ne 0 ]; then
  echo "[rolling] FAIL: $controlled controlled 429s; lower VUS or set ALLOW_CONTROLLED=1" >&2
  failure=1
fi
if [ "$total" -eq 0 ] || [ "$success" -eq 0 ]; then
  echo "[rolling] FAIL: no successful requests were recorded" >&2
  failure=1
fi

if [ "$failure" -ne 0 ]; then
  echo "[rolling] RESULT: FAIL"
  exit 1
fi
echo "[rolling] RESULT: PASS (zero failed requests while $old_image -> $NEW_IMAGE)"
