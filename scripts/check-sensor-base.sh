#!/usr/bin/env bash
# Guard against drift between the required verbatim manifest and the kustomize
# copy. They must contain the same Deployment (only the explanatory header in
# base/kustomization.yaml is different).
set -euo pipefail
cd "$(dirname "$0")/.."
if diff -q deploy/sensor-pantai/base.yaml deploy/sensor-pantai/base/deployment.yaml >/dev/null; then
  echo "sensor base manifests match"
else
  echo "ERROR: deploy/sensor-pantai/base.yaml and base/deployment.yaml drifted" >&2
  diff -u deploy/sensor-pantai/base.yaml deploy/sensor-pantai/base/deployment.yaml || true
  exit 1
fi
