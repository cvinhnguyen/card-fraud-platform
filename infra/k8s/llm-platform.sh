#!/bin/sh
# Install the Layer 3 LLM/agent platform into the kind cluster (after up.sh). Idempotent; versions pinned.
#   Gateway API 1.6.0 + Inference Extension 1.6.2 CRDs
#   agentgateway 1.6.0 (control plane, inference extension on) + infra/k8s/agentgateway.yaml (Gateway, LLM route,
#     policy, model server pods)
#   llm-d router 0.9.0 (InferencePool qwen3-8b + endpoint picker), the version agentgateway 1.6 is tested with
#   kagent 0.10.3 without its bundled demo agents and tools; the generated default ModelConfig is the agents' global
#     model config, pointing at the gateway (model qwen3-8b, key = AGW_API_KEY via the agentgateway-key secret);
#     proxy.url sends every agent-to-agent call through the gateway too (A2A gateway, agents/chart)
#   Agent Substrate 0.0.9 (the version kagent 0.10.3 pins): SandboxAgents run per conversation in gVisor actors
#     restored from warm snapshots on a shared worker pool (kagent-default, 1 worker = 1 active conversation)
#   KEDA 2.21.0 (agent autoscaling)
#   agentregistry 0.4.0 (agent + MCP server catalog, UI :12121); its database is `agentregistry` on ml-db
# Needs: the model servers running on the host (llama.cpp, see docs) and infra/.env (make_env.py).
set -e
cd "$(dirname "$0")"
GWAPI=v1.6.0 INFERENCE_EXT=v1.6.2 AGW=v1.6.0 LLMD_ROUTER=v0.9.0 KAGENT=0.10.3 KEDA=2.21.0 AREG=0.4.0 SUBSTRATE=0.0.9

kubectl apply --server-side -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/$GWAPI/standard-install.yaml"
kubectl apply -f "https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases/download/$INFERENCE_EXT/manifests.yaml"
helm upgrade -i agentgateway-crds oci://cr.agentgateway.dev/charts/agentgateway-crds -n agentgateway-system \
  --create-namespace --version "$AGW"
helm upgrade -i agentgateway oci://cr.agentgateway.dev/charts/agentgateway -n agentgateway-system --version "$AGW" \
  --set inferenceExtension.enabled=true --wait
for ns in kagent agentregistry; do kubectl create namespace $ns --dry-run=client -o yaml | kubectl apply -f -; done
sh secrets.sh   # gateway keys (agentgateway-system), agent keys (kagent), catalog database URL (agentregistry)

helm upgrade -i qwen3-8b oci://ghcr.io/llm-d/charts/llm-d-router-gateway -n agentgateway-system --version "$LLMD_ROUTER" \
  -f llm-d-router.yaml
kubectl apply -f agentgateway.yaml

helm upgrade -i substrate-crds oci://ghcr.io/kagent-dev/substrate/helm/substrate-crds -n ate-system --create-namespace \
  --version "$SUBSTRATE" --wait
helm upgrade -i substrate oci://ghcr.io/kagent-dev/substrate/helm/substrate -n ate-system --version "$SUBSTRATE" \
  --wait --timeout 10m
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
  --set proxy.url=http://agentgateway-proxy.agentgateway-system.svc.cluster.local \
  --set otel.tracing.enabled=true --set otel.tracing.exporter.otlp.endpoint=jaeger.monitoring.svc.cluster.local:4317 \
  --set controller.substrate.enabled=true --set controller.substrate.ateApiEndpoint=dns:///api.ate-system.svc:443 \
  --set controller.substrate.ateApiInsecure=true --set substrateWorkerPool.create=true \
  `# ateApiInsecure: TLS stays on but the ate-api's pod-issued certificate is not verified (kagent 0.10.3 has no CA` \
  `# option for it); the controller sends a 1 h, audience-bound token. Known limitation, revisit with Substrate mtls mode` \
  --set substrateWorkerPool.replicas=1 --set substrateWorkerPool.ateomImage=ghcr.io/kagent-dev/substrate/ateom-gvisor:v$SUBSTRATE \
  --wait --timeout 10m
kubectl -n kagent get modelconfig
# KEDA: autoscaling of the agents (ScaledObjects come with each agent)
helm upgrade -i keda keda --repo https://kedacore.github.io/charts -n keda --create-namespace --version "$KEDA" --wait
# agentregistry: catalog only (entries in agents/registry/); RBAC limited to the agents' namespace
helm upgrade -i agentregistry oci://ghcr.io/agentregistry-dev/agentregistry/charts/agentregistry -n agentregistry \
  --version "$AREG" -f ../../agents/registry/values.yaml --wait
echo "llm platform ready"
