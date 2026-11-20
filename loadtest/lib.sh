#!/usr/bin/env bash
# Shared helpers for the load-test scripts. Source this file; do not execute it.
#
# All k6 processes are launched from outside the Kubernetes application pods.
# The only cluster-side artifact is a temporary NodePort Service used to reach
# the Aggregator directly during throughput/rolling tests.

set -u

pick_python() {
  if [ -n "${PYTHON:-}" ]; then
    return 0
  fi
  if command -v python3 >/dev/null 2>&1 && python3 -c 'import sys' >/dev/null 2>&1; then
    PYTHON=python3
  elif command -v python >/dev/null 2>&1; then
    PYTHON=python
  else
    echo "[loadtest] python3/python is required" >&2
    return 1
  fi
  export PYTHON
}

require_k6_local() {
  command -v k6 >/dev/null 2>&1 || {
    echo "[loadtest] k6 is required" >&2
    return 1
  }
}

raise_token_ttl() {
  if [ "${SKIP_TTL_ENV:-0}" = "1" ]; then
    return 0
  fi
  kubectl -n "$NS" set env deployment/client-api TOKEN_TTL_SECONDS=3600 >/dev/null
  kubectl -n "$NS" set env deployment/auth ACCESS_TOKEN_TTL_SECONDS=3600 >/dev/null
  kubectl -n "$NS" rollout status deployment/client-api --timeout=120s >/dev/null
  kubectl -n "$NS" rollout status deployment/auth --timeout=120s >/dev/null
}

restore_aggregator_replicas() {
  if [ -n "${NS:-}" ]; then
    kubectl -n "$NS" scale deployment/aggregator --replicas="${RESTORE_REPLICAS:-3}" >/dev/null 2>&1 || true
  fi
}

wait_for_aggregator_endpoints() {
  local name="$1"
  local expected="${2:-1}"
  local i count
  for i in $(seq 1 60); do
    count="$(kubectl -n "$NS" get endpoints "$name" \
      -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | wc -w | tr -d ' ' || true)"
    if ! [[ "$count" =~ ^[0-9]+$ ]]; then
      count=0
    fi
    if [ "$count" -ge "$expected" ]; then
      return 0
    fi
    sleep 1
  done
  echo "[loadtest] Service $name has ${count:-0} ready endpoints, expected at least $expected" >&2
  return 1
}

duration_seconds() {
  case "$1" in
    *ms) echo $(( ${1%ms} / 1000 )) ;;
    *s)  echo "${1%s}" ;;
    *m)  echo $(( ${1%m} * 60 )) ;;
    *h)  echo $(( ${1%h} * 3600 )) ;;
    *)   echo "$1" ;;
  esac
}

# create_aggregator_nodeport installs a test-only NodePort Service. k6 runs on
# the host/VM and therefore needs a route to the Aggregator that does not use
# kubectl port-forward (a single tunnel is a throughput bottleneck).
# Set AGG_URL to skip this helper and use an existing endpoint.
create_aggregator_nodeport() {
  if [ -n "${AGG_URL:-}" ]; then
    echo "[loadtest] using existing AGG_URL=$AGG_URL"
    return 0
  fi

  local nodeport="${NODEPORT:-30085}"
  cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Service
metadata:
  name: aggregator-load
  namespace: ${NS}
  labels: {app: aggregator-load, role: loadtest}
spec:
  type: NodePort
  selector: {app: aggregator}
  ports:
    - {name: http, port: 8085, targetPort: 8085, nodePort: ${nodeport}}
EOF
  AGG_CREATED=1

  local ip="${NODE_IP:-}"
  if [ -z "$ip" ]; then
    ip="$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)"
  fi
  if [ -z "$ip" ]; then
    echo "[loadtest] cannot determine a node InternalIP; set NODE_IP" >&2
    return 1
  fi
  NODE_IP="$ip"
  AGG_URL="http://${ip}:${nodeport}"
  export NODE_IP AGG_URL

  wait_for_aggregator_endpoints aggregator-load
  echo "[loadtest] test-only Aggregator NodePort: $AGG_URL"
}

delete_aggregator_nodeport() {
  if [ "${AGG_CREATED:-0}" = "1" ] && [ -n "${NS:-}" ]; then
    kubectl -n "$NS" delete service aggregator-load --ignore-not-found >/dev/null 2>&1 || true
  fi
}
