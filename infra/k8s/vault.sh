#!/bin/sh
# Central secret management: HashiCorp Vault + External Secrets Operator (run once after up.sh, before the other
# scripts; again after a cluster restart). Idempotent.
#   Vault 2.0.4 (chart 0.34.1), dev mode: in-memory and auto-unsealed, root token VAULT_ROOT_TOKEN from infra/.env.
#     A restart empties it: run this script again; it re-creates the config and re-seeds (secrets.sh), and the
#     Kubernetes Secrets ESO already synced stay in place meanwhile. Production: HA Raft storage + KMS auto-unseal.
#   ESO 2.11.0: ClusterSecretStore `vault`, Kubernetes auth (role external-secrets, read-only on secret/card-fraud/*);
#     no Vault token is stored in the cluster.
set -e
cd "$(dirname "$0")"
VAULT=0.34.1 ESO=2.11.0
# Values on stdin (helm -f -): the root token stays off the command line. Readiness asks the HTTP health endpoint
# instead of running the vault CLI, and 512Mi leaves room for the CLI calls of this script: at 256Mi the ~500 MB
# binary thrashed the page cache (vault status took 21 s, the probe timed out and dropped Vault's endpoint).
# The StatefulSet updates OnDelete: after changing these values, `kubectl -n vault delete pod vault-0` and re-run.
cat <<EOF | helm upgrade -i vault vault --repo https://helm.releases.hashicorp.com -n vault --create-namespace \
  --version "$VAULT" -f - --wait
injector: {enabled: false}
server:
  dev: {enabled: true, devRootToken: "$(grep '^VAULT_ROOT_TOKEN=' ../.env | cut -d= -f2-)"}
  resources: {requests: {memory: 128Mi}, limits: {memory: 512Mi}}
  readinessProbe: {path: "/v1/sys/health?standbyok=true&sealedcode=204&uninitcode=204", periodSeconds: 10}
EOF
kubectl -n vault wait --for=condition=Ready pod/vault-0 --timeout=180s
# idempotent; retried because kubectl exec -i under Git Bash sometimes delivers EOF before the script
for try in 1 2 3; do
  kubectl -n vault exec -i vault-0 -- sh -s <<'EOF' && break
set -e
export VAULT_TOKEN=$VAULT_DEV_ROOT_TOKEN_ID
vault auth list | grep -q '^kubernetes/' || vault auth enable kubernetes >/dev/null
vault write auth/kubernetes/config kubernetes_host=https://kubernetes.default.svc >/dev/null
printf 'path "secret/data/card-fraud/*" { capabilities = ["read"] }\n' | vault policy write card-fraud-read - >/dev/null
vault write auth/kubernetes/role/external-secrets bound_service_account_names=external-secrets \
  bound_service_account_namespaces=external-secrets policies=card-fraud-read ttl=1h >/dev/null
echo "vault: kubernetes auth + policy card-fraud-read + role external-secrets"
EOF
  [ "$try" -lt 3 ] || { echo "vault config failed" >&2; exit 1; }; sleep 2
done
helm upgrade -i external-secrets external-secrets --repo https://charts.external-secrets.io -n external-secrets \
  --create-namespace --version "$ESO" --set resources.requests.memory=64Mi --set resources.limits.memory=256Mi \
  --set webhook.resources.requests.memory=32Mi --set webhook.resources.limits.memory=128Mi \
  --set certController.resources.requests.memory=32Mi --set certController.resources.limits.memory=128Mi --wait
kubectl apply -f - <<'EOF'
apiVersion: external-secrets.io/v1
kind: ClusterSecretStore
metadata:
  name: vault
spec:
  provider:
    vault:
      server: http://vault.vault.svc:8200
      path: secret
      version: v2
      auth:
        kubernetes:
          mountPath: kubernetes
          role: external-secrets
          serviceAccountRef: {name: external-secrets, namespace: external-secrets}
EOF
sh secrets.sh
