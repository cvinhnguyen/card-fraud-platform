#!/bin/sh
# Signed images (novel idea, docs/novel_supply_chain.md): cosign signatures on the platform's images and a Kyverno
# admission policy that lets only signed images of the platform registry run in card-support.
#   sh infra/k8s/supply-chain.sh                      install / update (Kyverno, policy, the node's registry:5000)
#   sh infra/k8s/supply-chain.sh sign <repo> <tag>    sign a pushed image, e.g. card-support-mcp 265b1ba
# Images are pushed to localhost:5001 from the host and referenced as registry:5000/<repo> in the cluster: the same
# compose registry, under the name both the kind node and the pods (Kyverno) reach. The private key is
# infra/.secrets/cosign.key, encrypted with COSIGN_PASSWORD (infra/.env); it only exists in the signing container's
# environment. Signatures stay in the registry (no upload to the public Rekor log).
set -e
cd "$(dirname "$0")"
KYVERNO_CHART=3.9.1 COSIGN=ghcr.io/sigstore/cosign/cosign:v2.6.5 NODE=fraud-control-plane

if [ "$1" = sign ]; then
  repo=$2 tag=$3
  digest=$(curl -fsSI -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "http://${REGISTRY:-localhost:5001}/v2/$repo/manifests/$tag" | tr -d '\r' | sed -n 's/^[Dd]ocker-[Cc]ontent-[Dd]igest: //p')
  [ -n "$digest" ] || { echo "no digest for $repo:$tag" >&2; exit 1; }
  COSIGN_KEY="${COSIGN_KEY:-$(cat ../.secrets/cosign.key)}" COSIGN_PASSWORD="${COSIGN_PASSWORD:-$(grep '^COSIGN_PASSWORD=' ../.env | cut -d= -f2-)}" \
    docker run --rm --network fraud_default -e COSIGN_KEY -e COSIGN_PASSWORD "$COSIGN" sign --key env://COSIGN_KEY \
    --allow-http-registry --tlog-upload=false -y "registry:5000/$repo@$digest"
  echo "signed registry:5000/$repo@$digest"
  exit 0
fi

# the kind node pulls registry:5000/... over HTTP from the compose registry (as localhost:5001/... in up.sh)
docker exec "$NODE" mkdir -p /etc/containerd/certs.d/registry:5000
printf '[host."http://registry:5000"]\n' | docker exec -i "$NODE" cp /dev/stdin /etc/containerd/certs.d/registry:5000/hosts.toml
helm upgrade -i kyverno kyverno --repo https://kyverno.github.io/kyverno -n kyverno --create-namespace \
  --version "$KYVERNO_CHART" -f supply-chain/kyverno-values.yaml --wait --timeout 10m
kubectl apply -f supply-chain/signed-images.yaml
kubectl wait --for=condition=Ready clusterpolicy/card-support-signed-images --timeout=120s
