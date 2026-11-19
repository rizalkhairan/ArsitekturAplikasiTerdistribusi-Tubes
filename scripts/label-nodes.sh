#!/usr/bin/env bash
# Apply the portable role contract used by deploy/kubernetes/base.
# Usage: scripts/label-nodes.sh <control-node> <app-node> <store-node>
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <control-node> <app-node> <store-node>" >&2
  exit 2
fi

CONTROL="$1"
APP="$2"
STORE="$3"

kubectl label node "$CONTROL" bnpb.io/role=control bnpb.io/app-eligible=false \
  app-eligible=false store-eligible=true aggregator-eligible=true --overwrite
kubectl label node "$APP" bnpb.io/role=app bnpb.io/app-eligible=true \
  app-eligible=true store-eligible=true aggregator-eligible=true --overwrite
kubectl label node "$STORE" bnpb.io/role=store bnpb.io/app-eligible=true \
  app-eligible=false store-eligible=true aggregator-eligible=true --overwrite

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"
kubectl -n "$NS" create configmap citus-node-map \
  --from-literal="CITUS_WORKER_1=$APP" \
  --from-literal="CITUS_WORKER_2=$STORE" \
  --from-literal="CITUS_WORKER_3=$APP" \
  --from-literal="CITUS_COORDINATOR=$CONTROL" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "node role labels applied: control=$CONTROL app=$APP store=$STORE"
