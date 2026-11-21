#!/bin/sh
# Create the kind cluster `fraud` and join it to the compose network, so pods reach the Layer 1 / ml services
# (seaweedfs, kafka, ml-db, redis, mlflow) by name. Images come from GHCR (ghcr.io/cvinhnguyen), so the node needs no
# registry setup: the public packages need no login, and the private fraud-predictor is pulled with the Secret
# serving/ghcr-pull (services/predictor/Jenkinsfile).
# Needs: docker compose --profile core --profile ml up -d. Idempotent.
set -e
cd "$(dirname "$0")"
kind get clusters | grep -qx fraud || kind create cluster --config kind.yaml
node=fraud-control-plane
docker network connect fraud_default "$node" 2>/dev/null || true
kubectl config use-context kind-fraud
kubectl get nodes
