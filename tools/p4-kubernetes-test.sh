#!/usr/bin/env bash
# Controlled Kubernetes checks, live feed, official workload and reference.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
OUT="${OUT:-$ROOT/test-results/p4-k8s-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT"
for tool in kubectl curl jq; do command -v "$tool" >/dev/null || exit 1; done
for file in "${P4_REFERENCE_FILE:-}" "${P4_LOAD_SCRIPT:-}"; do
  [[ -n "$file" && -f "$file" ]] || { echo 'BLOCKED: set P4_REFERENCE_FILE and P4_LOAD_SCRIPT to assistant reference and workload adapter files'; exit 2; }
done
# Keep official reference separate: it is not the reference for our mock fixture.
official_reference="$P4_REFERENCE_FILE"
unset P4_REFERENCE_FILE
OUT="$OUT/controlled" P4_BACKEND=kubernetes bash tools/p4-curl-test.sh local | tee "$OUT/controlled-results.txt"
export P4_REFERENCE_FILE="$official_reference"
kubectl -n "$NAMESPACE" rollout status deployment/satellite-processor --timeout=180s
kubectl -n "$NAMESPACE" get pods -o wide >"$OUT/pods-before.txt"
bash scripts/port-forward-loop.sh satellite-processor 8096 8080 >"$OUT/port-forward.log" 2>&1 &
forward_pid=$!
cleanup() {
  kill "$forward_pid" 2>/dev/null || true
  kubectl -n "$NAMESPACE" logs deployment/satellite-processor --tail=2000 >"$OUT/processor.log" 2>&1 || true
}
trap cleanup EXIT
export P4_LIVE_URL=http://localhost:8096
export P4_LIVE_SECONDS="${P4_LIVE_SECONDS:-180}"
bash tools/p4-curl-test.sh --live-external | tee "$OUT/live-results.txt"
# Adapter must run the provided workload to completion and produce its real
# end timestamp and final offset, never a fabricated local snapshot.
export P4_LOAD_RECEIPT="$OUT/workload-receipt.json"
bash "$P4_LOAD_SCRIPT" >"$OUT/workload.log" 2>&1 &
load_pid=$!
while kill -0 "$load_pid" 2>/dev/null; do
  curl -fsS --max-time 5 "$P4_LIVE_URL/status" >>"$OUT/burst-status.jsonl"
  printf '\n' >>"$OUT/burst-status.jsonl"
  sleep 1
done
wait "$load_pid"
jq -e '.speed == 60 and (.final_offset | type == "number") and (.ended_at_unix | type == "number")' "$P4_LOAD_RECEIPT" >/dev/null
final_offset="$(jq -r .final_offset "$P4_LOAD_RECEIPT")"
ended="$(jq -r .ended_at_unix "$P4_LOAD_RECEIPT")"
[[ "$ended" -le "$(date +%s)" ]] || { echo 'FAIL: workload ended in future'; exit 1; }
# The live observer alone permits backlog. Explicitly require it to drain.
drained=0
for _ in $(seq 1 60); do
  curl -fsS --max-time 5 "$P4_LIVE_URL/status" >"$OUT/final-status.json"
  if jq -e --argjson target "$final_offset" '.source_offset >= $target and .processed_offset >= $target and .pending_canonical_mutations == 0 and (.p95_input_to_store_seconds | type == "number")' "$OUT/final-status.json" >/dev/null; then drained=1; break; fi
  [[ $(( $(date +%s) - ended )) -le 60 ]] || break
  sleep 1
done
[[ "$drained" == 1 ]] || { echo 'FAIL: backlog/canonical writes did not drain during observation'; exit 1; }
elapsed=$(( $(date +%s) - ended ))
[[ "$elapsed" -le 60 ]] || { echo 'FAIL: official post-burst recovery exceeded 60 seconds'; exit 1; }
curl -fsS "$P4_LIVE_URL/dashboard" >"$OUT/dashboard.html"
grep -q 'Processing lag' "$OUT/dashboard.html"
grep -q 'p95 receipt to store' "$OUT/dashboard.html"
echo "PASS: P4.4 official 60x catch-up=${elapsed}s; p95=$(jq -r .p95_input_to_store_seconds "$OUT/final-status.json")s"
# Read actual canonical state, traversing all pages and rejecting partial data.
bash scripts/port-forward-loop.sh aggregator 18085 8085 >"$OUT/audit-forward.log" 2>&1 &
audit_pid=$!
trap 'kill "$audit_pid" 2>/dev/null || true; cleanup' EXIT
if [[ -z "${AUDIT_TOKEN:-}" ]]; then
  AUDIT_TOKEN="$(kubectl -n "$NAMESPACE" get secret bnpb-secrets -o json | jq -r '.data.AUDIT_TOKEN' | base64 -d)"
fi
for _ in $(seq 1 30); do if curl -fsS --max-time 2 -H "Authorization: Bearer $AUDIT_TOKEN" 'http://localhost:18085/internal/audit/events?source=SATELIT&limit=1' >/dev/null; then break; fi; sleep 1; done
cursor=''; page=0
while :; do
  args=(--get --data-urlencode source=SATELIT --data-urlencode limit=1000)
  [[ -z "$cursor" ]] || args+=(--data-urlencode "cursor=$cursor")
  curl -fsS --max-time 30 -H "Authorization: Bearer $AUDIT_TOKEN" "${args[@]}" http://localhost:18085/internal/audit/events >"$OUT/page-$page.json"
  jq -e '.partial == false and (.items | type == "array")' "$OUT/page-$page.json" >/dev/null || { echo 'FAIL: partial or invalid canonical snapshot'; exit 1; }
  cursor="$(jq -r '.next_cursor // empty' "$OUT/page-$page.json")"
  [[ -n "$cursor" ]] || break
  page=$((page+1))
done
jq -s '[.[].items[]]' "$OUT"/page-*.json >"$OUT/actual-all.json"
# Exclude only exact identities recorded by this controlled fixture. Historical
# unrelated records remain visible and correctly cause a session mismatch.
jq -S --slurpfile synthetic "$OUT/controlled/synthetic-refs.json" '[.[] | select(.source_ref_id as $id | ($synthetic[0] | index($id)) == null) | {source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,attributes}] | sort_by(.source_ref_id)' "$OUT/actual-all.json" >"$OUT/actual.json"
jq -S '[.fires[] | {source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,attributes}] | sort_by(.source_ref_id)' "$P4_REFERENCE_FILE" >"$OUT/expected.json"
diff -u "$OUT/expected.json" "$OUT/actual.json" >"$OUT/reference.diff" || { echo "FAIL: P4.6 reference differs; inspect $OUT/reference.diff"; exit 1; }
echo 'PASS: P4.6 complete canonical projection matches official reference'
echo 'P4 Kubernetes assertions completed.'
echo "Evidence: $OUT"
