#!/bin/sh
# Signed images (novel idea, docs/operations/novel_supply_chain.md): cosign signatures on the platform's images and a Kyverno
# admission policy that lets only signed images of the platform registry run in card-support.
#   sh infra/k8s/supply-chain.sh                      install / update (Kyverno, policy)
#   sh infra/k8s/supply-chain.sh sign <repo> <tag>    sign a pushed image, e.g. card-support-mcp 265b1ba
# The platform registry is GHCR, ghcr.io/cvinhnguyen; card-support-mcp is a public package, so the kind node and
# Kyverno read it without a login (fraud-predictor is private and is not under the policy). Pushing the signature needs one: Jenkins has cosign in its image and uses its `docker login
# ghcr.io`; by hand the cosign container gets GHCR_USER and GHCR_TOKEN (a classic PAT with write:packages) from the
# environment. The private key is infra/.secrets/cosign.key, encrypted with COSIGN_PASSWORD (infra/.env); it only
# exists in the signing process's environment. Signatures stay in the registry (no upload to the public Rekor log).
set -e
cd "$(dirname "$0")"
KYVERNO_CHART=3.9.1 COSIGN=ghcr.io/sigstore/cosign/cosign:v2.6.5 REGISTRY=${REGISTRY:-ghcr.io/cvinhnguyen}

if [ "$1" = sign ]; then
  repo=$2 tag=$3
  digest=$(docker buildx imagetools inspect "$REGISTRY/$repo:$tag" | sed -n 's/^Digest: *//p' | head -n 1)
  [ -n "$digest" ] || { echo "no digest for $REGISTRY/$repo:$tag" >&2; exit 1; }
  key=${COSIGN_KEY:-$(cat ../.secrets/cosign.key 2>/dev/null)}   # CI: the Jenkins credential; by hand: the local file
  [ -n "$key" ] || { echo "no signing key: set COSIGN_KEY or create infra/.secrets/cosign.key" >&2; exit 1; }
  export COSIGN_KEY="$key" COSIGN_PASSWORD="${COSIGN_PASSWORD:-$(grep '^COSIGN_PASSWORD=' ../.env | cut -d= -f2-)}"
  if command -v cosign >/dev/null 2>&1; then
    cosign sign --key env://COSIGN_KEY --tlog-upload=false -y "$REGISTRY/$repo@$digest"
  else   # by hand: the token is on the container's command line, so only on this machine for the seconds it runs
    docker run --rm -e COSIGN_KEY -e COSIGN_PASSWORD "$COSIGN" sign --key env://COSIGN_KEY --tlog-upload=false -y \
      --registry-username "${GHCR_USER:?set GHCR_USER}" --registry-password "${GHCR_TOKEN:?set GHCR_TOKEN}" \
      "$REGISTRY/$repo@$digest"
  fi
  echo "signed $REGISTRY/$repo@$digest"
  exit 0
fi

helm upgrade -i kyverno kyverno --repo https://kyverno.github.io/kyverno -n kyverno --create-namespace \
  --version "$KYVERNO_CHART" -f supply-chain/kyverno-values.yaml --wait --timeout 10m
kubectl apply -f supply-chain/signed-images.yaml
kubectl wait --for=condition=Ready clusterpolicy/card-support-signed-images --timeout=120s
