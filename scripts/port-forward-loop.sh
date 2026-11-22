#!/usr/bin/env bash
# Keep a Kubernetes Service port-forward alive across pod restarts and rollouts.
# Usage: scripts/port-forward-loop.sh <service> <local-port> <target-port>
set -euo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
SERVICE="${1:?usage: port-forward-loop.sh <service> <local-port> <target-port>}"
LOCAL_PORT="${2:?usage: port-forward-loop.sh <service> <local-port> <target-port>}"
TARGET_PORT="${3:?usage: port-forward-loop.sh <service> <local-port> <target-port>}"
forward_pid=''
stop_forward() {
  if [[ -n "$forward_pid" ]]; then kill "$forward_pid" 2>/dev/null || true; wait "$forward_pid" 2>/dev/null || true; fi
}
trap 'stop_forward; exit 0' TERM INT
trap stop_forward EXIT

while true; do
  kubectl -n "$NS" port-forward "svc/${SERVICE}" "${LOCAL_PORT}:${TARGET_PORT}" &
  forward_pid=$!
  wait "$forward_pid" || true
  forward_pid=''
  sleep 2
done
