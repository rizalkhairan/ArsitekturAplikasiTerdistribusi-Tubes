#!/usr/bin/env bash
# One-command deployment after images are available on the nodes.
# Usage: scripts/deploy.sh <control-node> <app-node> <store-node>
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <control-node> <app-node> <store-node>" >&2
  exit 2
fi

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bash "$ROOT/scripts/label-nodes.sh" "$1" "$2" "$3"
bash "$ROOT/scripts/citus-secrets.sh"

kubectl apply -k "$ROOT/deploy"

# Apply real application secrets after the dev placeholder Secret, otherwise
# the kustomize dev Secret would overwrite them.
if [[ -f "$ROOT/deploy/secrets.yaml" ]]; then
  kubectl apply -f "$ROOT/deploy/secrets.yaml"
  kubectl -n "$NS" rollout restart deployment/aggregator deployment/client-api deployment/auth \
    deployment/satellite-processor deployment/field-notifier deployment/grafana
fi

kubectl -n "$NS" wait --for=condition=complete job/citus-init --timeout=300s
kubectl -n "$NS" rollout status deployment/canonical-api --timeout=180s
kubectl -n "$NS" rollout status deployment/aggregator --timeout=180s
