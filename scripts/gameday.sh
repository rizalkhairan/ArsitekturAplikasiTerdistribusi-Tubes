#!/usr/bin/env bash
# Game day driver. Runs Tester Agent practice scenarios and sensor profile
# combinations, collecting metrics snapshots and logs for the post-mortem.
#
# Usage:
#   ./scripts/gameday.sh                      # list scenarios, run none
#   ./scripts/gameday.sh kill-pod kill-node   # run named practice scenarios
#   RUN_ALL=1 ./scripts/gameday.sh            # run every listed practice scenario
set -uo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
TESTER_NS="${TESTER_NS:-tester-agent}"
OUT="${OUT:-docs/report/gameday-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT"

say() { echo "[gameday] $*" | tee -a "$OUT/timeline.log"; }

snapshot() {
  local label="$1"
  say "snapshot $label"
  kubectl -n "$NS" get pods -o wide > "$OUT/$label-pods.txt" 2>&1 || true
  kubectl -n "$NS" get deploy,svc,pdb,pvc > "$OUT/$label-objects.txt" 2>&1 || true
  for app in coordinator canonical-store ingest-pipeline field-notifier sensor-pantai; do
    kubectl -n "$NS" logs "deploy/$app" --tail=200 > "$OUT/$label-$app.log" 2>&1 || true
  done
  kubectl -n "$NS" logs deploy/aggregator --tail=300 > "$OUT/$label-aggregator.log" 2>&1 || true
  # Metrics through the Aggregator Service (read API + /metrics on 8085).
  kubectl -n "$NS" port-forward svc/aggregator 18085:8085 >/dev/null 2>&1 &
  local pf=$!
  sleep 2
  curl -fsS http://127.0.0.1:18085/metrics > "$OUT/$label-metrics.txt" 2>&1 || true
  curl -fsS http://127.0.0.1:18085/sensor/status > "$OUT/$label-sensor-status.json" 2>&1 || true
  kill "$pf" 2>/dev/null || true
  # Field notifier receipt stats.
  kubectl -n "$NS" port-forward svc/field-notifier 18097:8080 >/dev/null 2>&1 &
  pf=$!
  sleep 2
  curl -fsS http://127.0.0.1:18097/stats > "$OUT/$label-notifier-stats.json" 2>&1 || true
  kill "$pf" 2>/dev/null || true
}

tester() {
  kubectl -n "$TESTER_NS" exec daemonset/tester-agent -- /app "$@"
}

set_sensor_profile() {
  local profile="$1" seed="$2"
  say "sensor profile=$profile seed=$seed"
  kubectl -n "$NS" set env deployment/sensor-pantai "PANTAI_PROFILE=$profile" "PANTAI_SEED=$seed" >/dev/null
  kubectl -n "$NS" rollout status deployment/sensor-pantai --timeout=120s || true
  sleep 10
}

scenarios=("$@")
if [ "${#scenarios[@]}" -eq 0 ] && [ "${RUN_ALL:-0}" != "1" ]; then
  say "available Tester Agent scenarios:"
  tester list | tee -a "$OUT/tester-list.txt" || true
  say "re-run with scenario names, or RUN_ALL=1"
  exit 0
fi

if [ "${RUN_ALL:-0}" = "1" ] && [ "${#scenarios[@]}" -eq 0 ]; then
  mapfile -t scenarios < <(tester list 2>/dev/null | awk '/practice/ {print $NF}')
fi

snapshot "before"
say "tester scenarios: ${scenarios[*]:-none}"

for scenario in "${scenarios[@]}"; do
  say "run practice $scenario"
  ( tester run practice "$scenario" 2>&1 | tee -a "$OUT/scenario-$scenario.log" ) || true
  snapshot "after-$scenario"
done

# Sensor misbehavior combinations. These are the provided test knobs only; no
# application logic depends on the values.
for combo in "practice 42" "practice 7" "practice 123"; do
  set_sensor_profile $combo
  snapshot "sensor-${combo// /-}"
done
set_sensor_profile practice 42

snapshot "final"
say "done; evidence in $OUT"
