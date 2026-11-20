#!/usr/bin/env bash
# Preflight for the P2 load tests. Run before criteria 6/7/8.
#
# It checks the cluster is quiet, the Aggregator has three ready replicas, and
# the external generator prerequisites are present. It never runs k6.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=loadtest/lib.sh
source "$ROOT/loadtest/lib.sh"

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
BASE_URL="${BASE_URL:-http://192.168.56.10:30081}"

pick_python
require_k6_local

echo "[preflight] cluster nodes"
kubectl get nodes -o wide
not_ready="$(kubectl get nodes --no-headers | awk '$2 != "Ready" {print $1}')"
if [ -n "$not_ready" ]; then
  echo "[preflight] nodes not Ready: $not_ready" >&2
  exit 1
fi

echo "[preflight] workload state"
kubectl -n "$NS" get deploy aggregator client-api canonical-api -o wide
bad="$(kubectl -n "$NS" get pods --no-headers | awk '$3 != "Running" && $3 != "Completed" {print $1" "$3}')"
if [ -n "$bad" ]; then
  echo "[preflight] pods not Running/Completed:" >&2
  printf '%s\n' "$bad" >&2
  exit 1
fi

ready="$(kubectl -n "$NS" get deployment/aggregator -o jsonpath='{.status.readyReplicas}')"
desired="$(kubectl -n "$NS" get deployment/aggregator -o jsonpath='{.spec.replicas}')"
if [ "${ready:-0}" != "3" ] || [ "${desired:-0}" != "3" ]; then
  echo "[preflight] aggregator is ${ready:-0}/${desired:-0}, expected 3/3" >&2
  exit 1
fi

echo "[preflight] client-api endpoint"
if command -v curl >/dev/null 2>&1; then
  if ! curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    echo "[preflight] $BASE_URL/health is unreachable" >&2
    exit 1
  fi
else
  echo "[preflight] curl not found; skipping external endpoint check" >&2
fi

if [ -n "${GENERATOR_HOSTS:-}" ]; then
  read -r -a hosts <<< "$GENERATOR_HOSTS"
  for host in "${hosts[@]}"; do
    echo "[preflight] generator $host"
    ssh -o BatchMode=yes "$host" 'command -v k6 >/dev/null && command -v python3 >/dev/null' || {
      echo "[preflight] $host needs k6 and python3" >&2
      exit 1
    }
  done
fi

echo "[preflight] READY"
