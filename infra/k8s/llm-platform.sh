#!/bin/sh
# Install the Layer 3 LLM/agent platform into the kind cluster (after up.sh). Idempotent; versions pinned.
#   Gateway API 1.6.0 + Inference Extension 1.6.2 CRDs
#   agentgateway 1.6.0 (control plane, inference extension on) + infra/k8s/agentgateway.yaml (Gateway, LLM route,
#     policy, model server pods)
#   llm-d router 0.9.0 (InferencePool qwen3-8b + endpoint picker), the version agentgateway 1.6 is tested with
#   kagent 0.10.3 without its bundled demo agents and tools; the generated default ModelConfig is the agents' global
#     model config, pointing at the gateway (model qwen3-8b, key = AGW_API_KEY via the agentgateway-key secret)
# Needs: the model servers running on the host (llama.cpp, see docs) and infra/.env (make_env.py).
set -e
cd "$(dirname "$0")"
GWAPI=v1.6.0 INFERENCE_EXT=v1.6.2 AGW=v1.6.0 LLMD_ROUTER=v0.9.0 KAGENT=0.10.3

kubectl apply --server-side -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/$GWAPI/standard-install.yaml"
kubectl apply -f "https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases/download/$INFERENCE_EXT/manifests.yaml"
helm upgrade -i agentgateway-crds oci://cr.agentgateway.dev/charts/agentgateway-crds -n agentgateway-system \
  --create-namespace --version "$AGW"
helm upgrade -i agentgateway oci://cr.agentgateway.dev/charts/agentgateway -n agentgateway-system --version "$AGW" \
  --set inferenceExtension.enabled=true --wait
kubectl create namespace kagent --dry-run=client -o yaml | kubectl apply -f -
sh secrets.sh   # llm-upstream, agentgateway-api-keys (agentgateway-system), agentgateway-key (kagent)

helm upgrade -i qwen3-8b oci://ghcr.io/llm-d/charts/llm-d-router-gateway -n agentgateway-system --version "$LLMD_ROUTER" \
  -f llm-d-router.yaml
kubectl apply -f agentgateway.yaml

helm upgrade -i kagent-crds oci://ghcr.io/kagent-dev/kagent/helm/kagent-crds -n kagent --version "$KAGENT" \
  --set kmcp.enabled=false
set --
for a in k8s-agent kgateway-agent istio-agent promql-agent observability-agent argo-rollouts-agent helm-agent \
         cilium-policy-agent cilium-manager-agent cilium-debug-agent grafana-mcp kagent-tools; do
  set -- "$@" --set "$a.enabled=false"
done
helm upgrade -i kagent oci://ghcr.io/kagent-dev/kagent/helm/kagent -n kagent --version "$KAGENT" "$@" \
  --set kmcp.enabled=false \
  --set providers.default=openAI \
  --set providers.openAI.model=qwen3-8b \
  --set providers.openAI.apiKeySecretRef=agentgateway-key --set providers.openAI.apiKeySecretKey=key \
  --set providers.openAI.config.baseUrl=http://agentgateway-proxy.agentgateway-system.svc.cluster.local/v1 \
  --wait --timeout 10m
kubectl -n kagent get modelconfig
echo "llm platform ready"
