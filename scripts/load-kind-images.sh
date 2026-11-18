#!/usr/bin/env bash
# Load all application images into a kind cluster.
# Usage: KIND_CLUSTER=bnpb-dev scripts/load-kind-images.sh
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-bnpb-dev}"
TAG="${TAG:-m2}"

command -v kind >/dev/null 2>&1 || {
  echo "kind is required for this helper" >&2
  exit 1
}

kind load docker-image --name "$CLUSTER" \
  "bnpb-gateway/aggregator:${TAG}" \
  "bnpb-gateway/client-api:${TAG}" \
  "bnpb-gateway/auth:${TAG}" \
  "bnpb-gateway/bmkg:${TAG}" \
  "bnpb-gateway/pvmbg:${TAG}" \
  "bnpb-gateway/coordinator:${TAG}" \
  "bnpb-gateway/canonical-store:${TAG}" \
  "bnpb-gateway/field-notifier:${TAG}" \
  "bnpb-gateway/ingest-pipeline:${TAG}" \
  "bnpb-gateway/dashboard-updater:${TAG}" \
  "bnpb-gateway/notifier:${TAG}" \
  "bnpb-gateway/analytics-consumer:${TAG}" \
  "bnpb-gateway/satellite-processor:${TAG}"
