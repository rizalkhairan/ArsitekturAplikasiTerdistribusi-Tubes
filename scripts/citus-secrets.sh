#!/usr/bin/env bash
# Create the three Secrets required by the portable lean-Citus deployment.
# Values come from the environment, or from deploy/k3s/.canonical.env.
set -euo pipefail

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"

if [[ -f "$ROOT/deploy/k3s/.canonical.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  . "$ROOT/deploy/k3s/.canonical.env"
  set +a
fi

: "${CANONICAL_APP_PASSWORD:?set CANONICAL_APP_PASSWORD or deploy/k3s/.canonical.env}"
SUPERUSER_PASSWORD="${PATRONI_SUPERUSER_PASSWORD:-${POSTGRES_PASSWORD:-}}"
: "${SUPERUSER_PASSWORD:?set POSTGRES_PASSWORD or PATRONI_SUPERUSER_PASSWORD}"

# deploy/k3s/.canonical.env is often checked out with CRLF on Windows.
CANONICAL_APP_PASSWORD="${CANONICAL_APP_PASSWORD%$'\r'}"
SUPERUSER_PASSWORD="${SUPERUSER_PASSWORD%$'\r'}"

apply() {
  kubectl -n "$NS" create secret generic "$1" --from-literal="$2" --dry-run=client -o yaml \
    | kubectl apply -f -
}

kubectl -n "$NS" create secret generic bnpb-canonical-cluster \
  --from-literal="PATRONI_SUPERUSER_PASSWORD=$SUPERUSER_PASSWORD" \
  --from-literal="CANONICAL_APP_PASSWORD=$CANONICAL_APP_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

apply bnpb-citus-admin "CANONICAL_ADMIN_DSN=postgresql://postgres@citus-coordinator:5432/canonical?sslmode=disable"
apply bnpb-citus-api "CANONICAL_DSN=postgresql://canonical_app:${CANONICAL_APP_PASSWORD}@citus-coordinator:5432/canonical?sslmode=disable"

echo "Citus Secrets applied in namespace $NS"
