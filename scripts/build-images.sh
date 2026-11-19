#!/usr/bin/env bash
# Build every image referenced by deploy/. Tag names must match deploy/*.yaml.
# For a k3s cluster, import the images on each node or push to a registry.
set -euo pipefail

TAG="${TAG:-m2}"

docker pull docker.io/citusdata/citus:13.0.1-pg16
docker build -f services/bnpb/Dockerfile -t "bnpb-gateway/aggregator:${TAG}" .
docker build -f services/client-api/Dockerfile -t "bnpb-gateway/client-api:${TAG}" .
docker build -f services/auth/Dockerfile -t "bnpb-gateway/auth:${TAG}" services
docker build -f services/bmkg/Dockerfile -t "bnpb-gateway/bmkg:${TAG}" services/bmkg
docker build -f services/pvmbg/Dockerfile -t "bnpb-gateway/pvmbg:${TAG}" services/pvmbg
docker build -f services/coordinator/Dockerfile -t "bnpb-gateway/coordinator:${TAG}" .
docker build -f services/field-notifier/Dockerfile -t "bnpb-gateway/field-notifier:${TAG}" .
docker build -f services/ingest-pipeline/Dockerfile -t "bnpb-gateway/ingest-pipeline:${TAG}" services/ingest-pipeline
docker build -f services/dashboard-updater/Dockerfile -t "bnpb-gateway/dashboard-updater:${TAG}" services/dashboard-updater
docker build -f services/notifier/Dockerfile -t "bnpb-gateway/notifier:${TAG}" services/notifier
docker build -f services/analytics-consumer/Dockerfile -t "bnpb-gateway/analytics-consumer:${TAG}" services/analytics-consumer
docker build -f services/satellite-processor/Dockerfile -t "bnpb-gateway/satellite-processor:${TAG}" services

echo "built images with tag ${TAG}"
