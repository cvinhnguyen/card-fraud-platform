#!/bin/sh
# Create / update the Kubernetes secrets from infra/.env (values never printed): the one place secrets cross
# from the compose world into the cluster.
#   s3-credentials     AWS keys for SeaweedFS      (kubeflow: pipeline steps + Spark executors)
#   feast-credentials  registry + Redis passwords  (kubeflow: Feast in the pipeline steps)
#   llm-upstream       llama.cpp key               (agentgateway-system: model server pods -> llama.cpp)
#   agentgateway-api-keys  SHA-256 of the clients' keys (agentgateway-system: API key auth; raw keys are not stored).
#                      user_id = who calls: agents (LLM), card-data-agent / risk-agent (MCP, each its own tools),
#                      coordinator (A2A to the sub-agents), support-app (A2A to the coordinator)
#   agentgateway-key   the agents' LLM key         (kagent: ModelConfig of the agents)
#   mcp-key-<agent>    one agent's MCP key         (kagent: RemoteMCPServer headers of that agent)
#   a2a-key-coordinator  the coordinator's A2A key (kagent: headers of its agent tools)
#   card-support-mcp   store passwords + embedder key (card-support: the MCP server)
#   grafana-admin      Grafana login               (monitoring)
#   agentregistry-db   catalog database URL (ml-db) (agentregistry)
set -e
cd "$(dirname "$0")/../.."
val() { grep "^$1=" infra/.env | cut -d= -f2-; }
apply() { kubectl apply -f - >/dev/null; }
for ns in kubeflow; do
  kubectl get ns "$ns" >/dev/null 2>&1 || continue
  kubectl -n "$ns" create secret generic s3-credentials --from-literal=access_key="$(val S3_ACCESS_KEY)" \
    --from-literal=secret_key="$(val S3_SECRET_KEY)" --dry-run=client -o yaml | apply
  kubectl -n "$ns" create secret generic feast-credentials --from-literal=ml_db_password="$(val ML_DB_PASSWORD)" \
    --from-literal=redis_password="$(val REDIS_PASSWORD)" --dry-run=client -o yaml | apply
  echo "secrets in $ns: ok"
done
if kubectl get ns agentgateway-system >/dev/null 2>&1; then
  kubectl -n agentgateway-system create secret generic llm-upstream --from-literal=Authorization="$(val LLM_API_KEY)" \
    --dry-run=client -o yaml | apply
  entry() { echo "$1={\"keyHash\": \"sha256:$(printf '%s' "$(val "$2")" | sha256sum | cut -d' ' -f1)\", \"metadata\": {\"user_id\": \"$1\"}}"; }
  kubectl -n agentgateway-system create configmap agentgateway-api-keys --from-literal="$(entry agents AGW_API_KEY)" \
    --from-literal="$(entry card-data-agent MCP_KEY_CARD_DATA_AGENT)" --from-literal="$(entry risk-agent MCP_KEY_RISK_AGENT)" \
    --from-literal="$(entry coordinator A2A_KEY_COORDINATOR)" --from-literal="$(entry support-app A2A_KEY_SUPPORT_APP)" \
    --dry-run=client -o yaml | kubectl label --local -f - agentgateway.dev/apikey=true -o yaml | apply
  echo "secrets in agentgateway-system: ok"
fi
if kubectl get ns kagent >/dev/null 2>&1; then
  kubectl -n kagent create secret generic agentgateway-key --from-literal=key="$(val AGW_API_KEY)" \
    --dry-run=client -o yaml | apply
  kubectl -n kagent create secret generic mcp-key-card-data-agent \
    --from-literal=Authorization="Bearer $(val MCP_KEY_CARD_DATA_AGENT)" --dry-run=client -o yaml | apply
  kubectl -n kagent create secret generic mcp-key-risk-agent \
    --from-literal=Authorization="Bearer $(val MCP_KEY_RISK_AGENT)" --dry-run=client -o yaml | apply
  kubectl -n kagent create secret generic a2a-key-coordinator \
    --from-literal=Authorization="Bearer $(val A2A_KEY_COORDINATOR)" --dry-run=client -o yaml | apply
  echo "secrets in kagent: ok"
fi
if kubectl get ns card-support >/dev/null 2>&1; then
  kubectl -n card-support create secret generic card-support-mcp --from-literal=ML_DB_PASSWORD="$(val ML_DB_PASSWORD)" \
    --from-literal=REDIS_PASSWORD="$(val REDIS_PASSWORD)" --from-literal=VECTOR_DB_PASSWORD="$(val VECTOR_DB_PASSWORD)" \
    --from-literal=LLM_API_KEY="$(val LLM_API_KEY)" --dry-run=client -o yaml | apply
  echo "secrets in card-support: ok"
fi
if kubectl get ns monitoring >/dev/null 2>&1; then
  kubectl -n monitoring create secret generic grafana-admin --from-literal=admin-user=admin \
    --from-literal=admin-password="$(val GRAFANA_ADMIN_PASSWORD)" --dry-run=client -o yaml | apply
  echo "secrets in monitoring: ok"
fi
if kubectl get ns agentregistry >/dev/null 2>&1; then
  kubectl -n agentregistry create secret generic agentregistry-db --from-literal=AGENT_REGISTRY_DATABASE_URL="postgres://ml:$(val ML_DB_PASSWORD)@ml-db:5432/agentregistry?sslmode=disable" \
    --dry-run=client -o yaml | apply
  echo "secrets in agentregistry: ok"
fi
