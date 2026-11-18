#!/usr/bin/env bash
# Keep a Kubernetes Service port-forward alive across pod restarts and rollouts.
# Usage: scripts/port-forward-loop.sh <service> <local-port> <target-port>
set -euo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
SERVICE="${1:?usage: port-forward-loop.sh <service> <local-port> <target-port>}"
LOCAL_PORT="${2:?usage: port-forward-loop.sh <service> <local-port> <target-port>}"
TARGET_PORT="${3:?usage: port-forward-loop.sh <service> <local-port> <target-port>}"

while true; do
  kubectl -n "$NS" port-forward "svc/${SERVICE}" "${LOCAL_PORT}:${TARGET_PORT}" || true
  sleep 2
done
