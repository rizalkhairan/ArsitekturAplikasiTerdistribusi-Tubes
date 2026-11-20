#!/usr/bin/env bash
# P2 criterion 7: throughput(3 instances) >= 2.0 x throughput(1 instance).
#
# The load runs outside the application pods. It targets the Aggregator
# directly through a temporary NodePort Service, not kubectl port-forward (a
# single tunnel is itself a throughput bottleneck). The test-only Service is
# deleted on exit.
#
# Calibrate the VU count first if needed:
#   CALIBRATE=1 BASE_URL=http://192.168.56.10:30081 ./loadtest/throughput.sh
#
# Then run the comparison:
#   VUS=2000 DURATION=60s ./loadtest/throughput.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=loadtest/lib.sh
source "$ROOT/loadtest/lib.sh"

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
OUT="${OUT:-$ROOT/loadtest/results}"
VUS="${VUS:-2000}"
DURATION="${DURATION:-60s}"
SLEEP="${SLEEP:-0}"
INTERNAL_KEY="${INTERNAL_KEY:-}"
NODEPORT="${NODEPORT:-30085}"
CALIBRATE="${CALIBRATE:-0}"
CALIBRATE_VUS="${CALIBRATE_VUS:-500 1000 2000}"
CALIBRATE_DURATION="${CALIBRATE_DURATION:-30s}"

pick_python
mkdir -p "$OUT"
require_k6_local
DURATION_SECONDS="$(duration_seconds "$DURATION")"
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || [ "$DURATION_SECONDS" -lt 1 ]; then
  echo "[throughput] invalid DURATION=$DURATION" >&2
  exit 2
fi

cleanup() {
  delete_aggregator_nodeport
  restore_aggregator_replicas
}
trap cleanup EXIT

raise_token_ttl
kubectl -n "$NS" scale deployment/aggregator --replicas=3 >/dev/null
kubectl -n "$NS" rollout status deployment/aggregator --timeout=180s >/dev/null

create_aggregator_nodeport

if ! curl -fsS "$AGG_URL/readyz" >/dev/null 2>&1; then
  echo "[throughput] $AGG_URL/readyz is not ready" >&2
  exit 1
fi

run_leg() {
  local replicas="$1"
  local label="$2"
  local summary="$OUT/throughput-$label.json"
  local log="$OUT/throughput-$label.log"

  kubectl -n "$NS" scale deployment/aggregator --replicas="$replicas"
  kubectl -n "$NS" rollout status deployment/aggregator --timeout=180s >/dev/null
  # Connections are established when k6 starts; wait until the Service exposes
  # all replicas, otherwise keep-alive pins every VU to the first ready pod.
  wait_for_aggregator_endpoints aggregator-load "$replicas"
  sleep 2

  echo "[throughput] $label: replicas=$replicas vus=$VUS duration=$DURATION"
  set +e
  k6 run \
    -e "BASE_URL=$AGG_URL" \
    -e "INTERNAL_KEY=$INTERNAL_KEY" \
    -e "VUS=$VUS" \
    -e "DURATION=$DURATION" \
    -e "SLEEP=$SLEEP" \
    --summary-export "$summary" \
    "$ROOT/loadtest/k6/sustained.js" > "$log" 2>&1
  local k6_status=$?
  set -e
  if [ ! -s "$summary" ]; then
    echo "[throughput] $label produced no summary (k6 exit=$k6_status)" >&2
    return 1
  fi
  echo "[throughput] $label k6 exit=$k6_status (threshold exit is informational)"
}

rate_of() {
  "$PYTHON" - "$1" "$2" <<'PY'
import json
import sys

path, duration = sys.argv[1], float(sys.argv[2])
metrics = json.load(open(path, encoding="utf-8"))["metrics"]
success = metrics.get("successful_responses", {}).get("count")
if success is None:
    # Older summaries do not have the explicit counter; use the total request
    # count and keep the error-rate validity check below.
    success = metrics.get("http_reqs", {}).get("count", 0)
print(f"{float(success) / duration:.6f}")
PY
}

errors_of() {
  "$PYTHON" - "$1" <<'PY'
import json
import sys

metrics = json.load(open(sys.argv[1], encoding="utf-8"))["metrics"]
http = metrics.get("http_reqs", {}).get("count", 0) or 0
unexpected = metrics.get("unexpected_errors", {}).get("count", 0) or 0
rate = unexpected / http if http else 1.0
print(f"{unexpected} {rate:.6f}")
PY
}

if [ "$CALIBRATE" = "1" ]; then
  cal_seconds="$(duration_seconds "$CALIBRATE_DURATION")"
  for v in $CALIBRATE_VUS; do
    VUS="$v"
    DURATION="$CALIBRATE_DURATION"
    run_leg 1 "calibrate-$v"
    read -r rate < <(rate_of "$OUT/throughput-calibrate-$v.json" "$cal_seconds")
    echo "[throughput] VUS=$v success_rate=$rate/s"
  done
  echo "[throughput] calibration complete; choose the lowest VU count where 1 replica plateaus"
  exit 0
fi

run_leg 1 one
run_leg 3 three

read -r one_unexpected one_error_rate < <(errors_of "$OUT/throughput-one.json")
read -r three_unexpected three_error_rate < <(errors_of "$OUT/throughput-three.json")
one_rate="$(rate_of "$OUT/throughput-one.json" "$DURATION_SECONDS")"
three_rate="$(rate_of "$OUT/throughput-three.json" "$DURATION_SECONDS")"

"$PYTHON" - "$one_rate" "$three_rate" "$one_error_rate" "$three_error_rate" <<'PY'
import sys

one = float(sys.argv[1])
three = float(sys.argv[2])
one_error = float(sys.argv[3])
three_error = float(sys.argv[4])
ratio = three / one if one else 0.0
print(f"1 instance : {one:.2f} successful req/s (unexpected error rate={one_error:.4%})")
print(f"3 instances: {three:.2f} successful req/s (unexpected error rate={three_error:.4%})")
print(f"ratio      : {ratio:.3f} (target >= 2.0)")
if one_error >= 0.01 or three_error >= 0.01:
    print("INVALID: a leg has >=1% unexpected errors; the throughput number is not meaningful")
    sys.exit(2)
sys.exit(0 if ratio >= 2.0 else 1)
PY
