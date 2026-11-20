#!/usr/bin/env bash
# Create a distinct Aggregator image tag on every node without Docker.
#
# The demo cluster has the m2 image in containerd on each node. This helper
# tags it as the rollout version (m2-v2 by default) so loadtest/
# rolling-update.sh can perform a real image change. If you have a genuinely
# different build, import it under TARGET_IMAGE instead of using this helper.
set -euo pipefail

SOURCE_IMAGE="${SOURCE_IMAGE:-docker.io/bnpb-gateway/aggregator:m2}"
TARGET_IMAGE="${TARGET_IMAGE:-docker.io/bnpb-gateway/aggregator:m2-v2}"
SSH_USER="${SSH_USER:-ubuntu}"

if [ -z "${NODES:-}" ]; then
  NODES="$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}')"
fi

if [ -z "$NODES" ]; then
  echo "[tag] no nodes found through kubectl" >&2
  exit 1
fi

for ip in $NODES; do
  echo "[tag] $ip: $SOURCE_IMAGE -> $TARGET_IMAGE"
  ssh -o BatchMode=yes "$SSH_USER@$ip" \
    "sudo -n k3s ctr images tag --force '$SOURCE_IMAGE' '$TARGET_IMAGE'"
  ssh -o BatchMode=yes "$SSH_USER@$ip" \
    "sudo -n k3s ctr images ls | grep -F '$TARGET_IMAGE'" >/dev/null
  echo "[tag] $ip: ok"
done

echo "[tag] use NEW_IMAGE=bnpb-gateway/aggregator:m2-v2 for loadtest/rolling-update.sh"
