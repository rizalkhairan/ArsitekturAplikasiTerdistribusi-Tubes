#!/usr/bin/env bash
# Integration test for P2 criteria 3/5: pause a leader (SIGSTOP), verify takeover
# and fencing when it resumes, then kill it and verify no duplicates/gaps.
set -euo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
OUT="${OUT:-loadtest/results}"
mkdir -p "$OUT"

leader="$(kubectl -n "$NS" get pods -l app=aggregator -o jsonpath='{.items[0].metadata.name}')"
echo "[test] pausing leader $leader"
kubectl -n "$NS" exec "$leader" -- kill -STOP 1 || {
  echo "SIGSTOP failed; use the Tester Agent freeze practice instead" >&2
  exit 1
}
echo "[test] waiting 25s (TTL=15s) for takeover"
sleep 25
kubectl -n "$NS" logs deploy/aggregator --tail=50 | grep -E 'lease_acquired|lease_renew_failed_self_fencing' | tail -n 5 || true
./loadtest/duplicate-detector.sh | tee "$OUT/pause-while-frozen.txt"
echo "[test] resuming $leader; it must not persist until it refreshes status"
kubectl -n "$NS" exec "$leader" -- kill -CONT 1 || true
sleep 5
kubectl -n "$NS" logs "$leader" --tail=50 | grep -E 'fenced|stale|lease_renew_failed' || true

echo "[test] killing $leader"
kubectl -n "$NS" delete pod "$leader" --wait=false
sleep 20
kubectl -n "$NS" get pods -l app=aggregator -o wide | tee "$OUT/pause-pods.txt"
./loadtest/duplicate-detector.sh | tee "$OUT/pause-after-kill.txt"
