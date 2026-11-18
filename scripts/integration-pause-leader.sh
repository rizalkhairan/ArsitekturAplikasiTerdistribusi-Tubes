#!/usr/bin/env bash
# P2 criterion 3: remove the active Aggregator leader and verify that another
# replica takes over within the lease bound while exactly-once deduplication
# still holds.
#
# This script deliberately deletes the leader Pod, which is valid on kind and
# on a real cluster. P2 criterion 5 (VM/process freeze) must be driven by the
# course Tester Agent on real VMs; the previous SIGSTOP-on-PID-1 attempt did not
# freeze the process because PID 1 ignores default-action signals.
set -euo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
OUT="${OUT:-loadtest/results}"
mkdir -p "$OUT"

leader_pod() {
  local pod
  for pod in $(kubectl -n "$NS" get pods -l app=aggregator \
    -o jsonpath='{.items[*].metadata.name}'); do
    if kubectl -n "$NS" exec "$pod" -- \
      wget -qO- http://127.0.0.1:8085/metrics 2>/dev/null \
      | grep -Eq 'lease_holder\{[^}]*\} 1'; then
      echo "$pod"
      return 0
    fi
  done
  return 1
}

leader="$(leader_pod || true)"
if [ -z "$leader" ]; then
  echo "[test] no leader found through aggregator metrics" >&2
  exit 1
fi

echo "[test] deleting leader $leader"
kubectl -n "$NS" delete pod "$leader" --wait=false

new_leader=""
for _ in $(seq 1 40); do
  sleep 1
  candidate="$(leader_pod || true)"
  if [ -n "$candidate" ] && [ "$candidate" != "$leader" ]; then
    new_leader="$candidate"
    break
  fi
done

if [ -z "$new_leader" ]; then
  echo "[test] no replacement leader acquired within 40s" >&2
  kubectl -n "$NS" get pods -l app=aggregator -o wide | tee "$OUT/pause-pods.txt"
  exit 1
fi

echo "[test] new leader $new_leader"
kubectl -n "$NS" get pods -l app=aggregator -o wide | tee "$OUT/pause-pods.txt"
for pod in $(kubectl -n "$NS" get pods -l app=aggregator \
  -o jsonpath='{.items[*].metadata.name}'); do
  kubectl -n "$NS" exec "$pod" -- \
    wget -qO- http://127.0.0.1:8085/metrics 2>/dev/null \
    | grep -E 'lease_holder\{|lease_epoch\{' || true
done | tee "$OUT/pause-lease-metrics.txt"

./loadtest/duplicate-detector.sh | tee "$OUT/pause-after-kill.txt"
