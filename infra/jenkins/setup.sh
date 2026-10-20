#!/bin/sh
# CI/CD bootstrap (after the cluster scripts): the deploy ServiceAccount (rbac.yaml), its kubeconfig for Jenkins
# (infra/.secrets/jenkins-kubeconfig, gitignored; the API server by its name on the compose network) and the Jenkins
# container (compose profile cicd). Idempotent. UI: http://localhost:8095 - user admin, JENKINS_ADMIN_PASSWORD.
set -e
cd "$(dirname "$0")/../.."
kubectl apply -f infra/jenkins/rbac.yaml >/dev/null
until token=$(kubectl -n ci get secret jenkins-deployer-token -o jsonpath='{.data.token}' 2>/dev/null) && [ -n "$token" ]; do
  sleep 1
done
ca=$(kubectl config view --raw -o jsonpath='{.clusters[?(@.name=="kind-fraud")].cluster.certificate-authority-data}')
mkdir -p infra/.secrets
umask 077
cat > infra/.secrets/jenkins-kubeconfig <<EOF
apiVersion: v1
kind: Config
clusters: [{name: kind-fraud, cluster: {server: "https://fraud-control-plane:6443", certificate-authority-data: $ca}}]
users: [{name: jenkins-deployer, user: {token: $(printf '%s' "$token" | base64 -d)}}]
contexts: [{name: ci, context: {cluster: kind-fraud, user: jenkins-deployer}}]
current-context: ci
EOF
docker compose -f infra/docker-compose.yaml --profile cicd up -d --build jenkins
echo "jenkins: http://localhost:8095 (admin / JENKINS_ADMIN_PASSWORD)"
