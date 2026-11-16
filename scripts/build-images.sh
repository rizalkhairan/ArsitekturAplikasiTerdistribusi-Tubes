#!/usr/bin/env bash
# Build every image referenced by deploy/. Tag names must match deploy/*.yaml.
# For a k3s cluster, import the images on each node or push to a registry.
set -euo pipefail

TAG="${TAG:-m2}"

docker build -f services/bnpb/Dockerfile -t "bnpb-gateway/aggregator:${TAG}" .
docker build -f services/client-api/Dockerfile -t "bnpb-gateway/client-api:${TAG}" .
docker build -f services/auth/Dockerfile -t "bnpb-gateway/auth:${TAG}" services/auth
docker build -f services/bmkg/Dockerfile -t "bnpb-gateway/bmkg:${TAG}" services/bmkg
docker build -f services/pvmbg/Dockerfile -t "bnpb-gateway/pvmbg:${TAG}" services/pvmbg
docker build -f services/coordinator/Dockerfile -t "bnpb-gateway/coordinator:${TAG}" .
docker build -f services/canonical-store/Dockerfile -t "bnpb-gateway/canonical-store:${TAG}" .
docker build -f services/field-notifier/Dockerfile -t "bnpb-gateway/field-notifier:${TAG}" .
docker build -f services/ingest-pipeline/Dockerfile -t "bnpb-gateway/ingest-pipeline:${TAG}" services/ingest-pipeline

echo "built images with tag ${TAG}"
