#!/bin/sh
# Create the kind cluster `fraud` and join it to the compose network, so pods reach the Layer 1 / ml services
# (seaweedfs, kafka, ml-db, redis, mlflow) by name, and the node pulls images from the compose `registry`.
# Needs: docker compose --profile core --profile ml up -d. Idempotent.
set -e
cd "$(dirname "$0")"
kind get clusters | grep -qx fraud || kind create cluster --config kind.yaml
node=fraud-control-plane
docker network connect fraud_default "$node" 2>/dev/null || true
# localhost:5001 (what the host pushes to) and registry:5000 (the name pods reach too, used by the signed images of
# infra/k8s/supply-chain.sh) -> registry:5000 on the compose network, over HTTP
for r in localhost:5001 registry:5000; do
  docker exec "$node" mkdir -p "/etc/containerd/certs.d/$r"
  printf '[host."http://registry:5000"]\n' | docker exec -i "$node" cp /dev/stdin "/etc/containerd/certs.d/$r/hosts.toml"
done
kubectl config use-context kind-fraud
kubectl get nodes
