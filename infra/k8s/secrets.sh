#!/bin/sh
# Every Kubernetes secret of the platform comes from HashiCorp Vault (KV v2 at secret/card-fraud/<namespace>/<name>):
# this script writes the values from infra/.env into Vault (as JSON on stdin: never printed, never a command-line
# argument) and one ExternalSecret per secret, which the External Secrets Operator turns into the Kubernetes Secret.
# Needs infra/k8s/vault.sh once. Only namespaces that exist get their secrets; re-run after adding a namespace.
#   kubeflow             s3-credentials (SeaweedFS keys), feast-credentials (registry + Redis passwords)
#   agentgateway-system  llm-upstream (llama.cpp key: model server pods -> llama.cpp), llm-groq (Groq API key: LLM
#                        version B of the A/B test, only when GROQ_API_KEY is set)
#   kagent               agentgateway-key (agents' LLM key, ModelConfig), mcp-key-<agent> (RemoteMCPServer headers),
#                        a2a-key-coordinator (headers of its agent tools)
#   card-support         card-data-mcp (store passwords), policy-rag-mcp (pgvector password + embedder key)
#   monitoring           grafana-admin (Grafana login)
#   agentregistry        agentregistry-db (catalog database URL on ml-db)
#   kagent, agentregistry, monitoring, card-support   edge-basic-auth (htpasswd of NGINX user admin)
#   nginx-ingress        wildcard-tls (Let's Encrypt *.<DUCKDNS_DOMAIN>, once tls.sh has obtained it)
# Not a secret, applied directly: agentgateway-api-keys, the SHA-256 of the gateway clients' keys (user_id = agents
# (LLM), card-data-agent / risk-agent (MCP tools), coordinator (A2A to the sub-agents), support-app (A2A clients)).
set -e
cd "$(dirname "$0")/../.."
val() { grep "^$1=" infra/.env | cut -d= -f2-; }
sync=$(date +%s)
# Vault's HTTP API through a port-forward: values go on curl's stdin, the token header from a 600 temp file
# (kubectl exec -i under Git Bash sometimes delivered EOF before the data)
kubectl -n vault port-forward svc/vault 18200:8200 >/dev/null 2>&1 &
pf=$!
hdr=$(mktemp)
trap 'kill $pf 2>/dev/null; rm -f "$hdr"' EXIT
chmod 600 "$hdr"
printf 'header = "X-Vault-Token: %s"\n' "$(val VAULT_ROOT_TOKEN)" > "$hdr"
n=0; until curl -fsS -o /dev/null http://127.0.0.1:18200/v1/sys/health 2>/dev/null; do
  n=$((n + 1)); [ "$n" -lt 30 ] || { echo "vault not reachable" >&2; exit 1; }; sleep 1
done
# secret <namespace> <name> <type> <key>=<value>...  (values are token_urlsafe / URLs / htpasswd / base64: JSON-safe)
secret() {
  ns=$1 name=$2 type=$3; shift 3
  kubectl get ns "$ns" >/dev/null 2>&1 || return 0
  json=$(for kv in "$@"; do printf '"%s":"%s",' "${kv%%=*}" "${kv#*=}"; done)
  printf '{"data":{%s}}' "${json%,}" | curl -fsS -o /dev/null -K "$hdr" --data @- \
    "http://127.0.0.1:18200/v1/secret/data/card-fraud/$ns/$name" || { echo "vault write failed: $ns/$name" >&2; return 1; }
  kubectl apply -f - >/dev/null <<EOF
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: $name
  namespace: $ns
  annotations: {force-sync: "$sync"}   # sync now, not at the next refresh
spec:
  refreshInterval: 1h
  secretStoreRef: {kind: ClusterSecretStore, name: vault}
  target: {name: $name, creationPolicy: Owner, template: {type: $type}}
  dataFrom: [{extract: {key: card-fraud/$ns/$name, decodingStrategy: ${DECODE:-None}}}]
EOF
  echo "  vault -> $ns/$name"
}
secret kubeflow s3-credentials Opaque access_key="$(val S3_ACCESS_KEY)" secret_key="$(val S3_SECRET_KEY)"
secret kubeflow feast-credentials Opaque ml_db_password="$(val ML_DB_PASSWORD)" redis_password="$(val REDIS_PASSWORD)"
secret agentgateway-system llm-upstream Opaque Authorization="$(val LLM_API_KEY)"
[ -z "$(val GROQ_API_KEY)" ] || secret agentgateway-system llm-groq Opaque Authorization="$(val GROQ_API_KEY)"
secret kagent agentgateway-key Opaque key="$(val AGW_API_KEY)"
secret kagent mcp-key-card-data-agent Opaque Authorization="Bearer $(val MCP_KEY_CARD_DATA_AGENT)"
secret kagent mcp-key-risk-agent Opaque Authorization="Bearer $(val MCP_KEY_RISK_AGENT)"
secret kagent a2a-key-coordinator Opaque Authorization="Bearer $(val A2A_KEY_COORDINATOR)"
# each MCP server gets only what its tools read (fraud-score reads no store: no secret)
secret card-support card-data-mcp Opaque ML_DB_PASSWORD="$(val ML_DB_PASSWORD)" REDIS_PASSWORD="$(val REDIS_PASSWORD)" \
  VECTOR_DB_PASSWORD="$(val VECTOR_DB_PASSWORD)"
secret card-support policy-rag-mcp Opaque ML_DB_PASSWORD="$(val ML_DB_PASSWORD)" \
  VECTOR_DB_PASSWORD="$(val VECTOR_DB_PASSWORD)" LLM_API_KEY="$(val LLM_API_KEY)"
secret monitoring grafana-admin Opaque admin-user=admin admin-password="$(val GRAFANA_ADMIN_PASSWORD)"
secret agentregistry agentregistry-db Opaque \
  AGENT_REGISTRY_DATABASE_URL="postgres://ml:$(val ML_DB_PASSWORD)@ml-db:5432/agentregistry?sslmode=disable"
htpasswd="admin:$(printf '%s' "$(val EDGE_ADMIN_PASSWORD)" | openssl passwd -apr1 -stdin)"
for ns in kagent agentregistry monitoring card-support; do secret "$ns" edge-basic-auth nginx.org/htpasswd htpasswd="$htpasswd"; done
# wildcard certificate from tls.sh (PEM has newlines: stored base64 in Vault, decoded by ESO)
crt="infra/.certs/certificates/_.$(val DUCKDNS_DOMAIN || true).crt"
if [ -f "$crt" ]; then
  DECODE=Base64 secret nginx-ingress wildcard-tls kubernetes.io/tls tls.crt="$(base64 < "$crt" | tr -d '\n')" \
    tls.key="$(base64 < "${crt%.crt}.key" | tr -d '\n')"
fi

if kubectl get ns agentgateway-system >/dev/null 2>&1; then
  entry() { echo "$1={\"keyHash\": \"sha256:$(printf '%s' "$(val "$2")" | sha256sum | cut -d' ' -f1)\", \"metadata\": {\"user_id\": \"$1\"}}"; }
  kubectl -n agentgateway-system create configmap agentgateway-api-keys --from-literal="$(entry agents AGW_API_KEY)" \
    --from-literal="$(entry card-data-agent MCP_KEY_CARD_DATA_AGENT)" --from-literal="$(entry risk-agent MCP_KEY_RISK_AGENT)" \
    --from-literal="$(entry coordinator A2A_KEY_COORDINATOR)" --from-literal="$(entry support-app A2A_KEY_SUPPORT_APP)" \
    --dry-run=client -o yaml | kubectl label --local -f - agentgateway.dev/apikey=true -o yaml | kubectl apply -f - >/dev/null
  echo "  agentgateway-system/agentgateway-api-keys (key hashes)"
fi
