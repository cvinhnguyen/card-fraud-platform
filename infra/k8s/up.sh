#!/bin/sh
# Create the kind cluster `fraud` and join it to the compose network, so pods reach the Layer 1 / ml services
# (seaweedfs, kafka, ml-db, redis, mlflow) by name, and the node pulls images from the compose `registry`.
# Needs: docker compose --profile core --profile ml up -d. Idempotent.
set -e
cd "$(dirname "$0")"
kind get clusters | grep -qx fraud || kind create cluster --config kind.yaml
node=fraud-control-plane
docker network connect fraud_default "$node" 2>/dev/null || true
# localhost:5001 (what the host pushes to) -> registry:5000 on the compose network (what the node can reach)
docker exec "$node" mkdir -p /etc/containerd/certs.d/localhost:5001
printf '[host."http://registry:5000"]\n' | docker exec -i "$node" cp /dev/stdin /etc/containerd/certs.d/localhost:5001/hosts.toml
kubectl config use-context kind-fraud
kubectl get nodes
