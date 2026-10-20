#!/bin/sh
# Create / update the Kubernetes secrets from infra/.env (values never printed): the one place secrets cross
# from the compose world into the cluster.
#   s3-credentials     AWS keys for SeaweedFS      (kubeflow: pipeline steps + Spark executors)
#   feast-credentials  registry + Redis passwords  (kubeflow: Feast in the pipeline steps)
set -e
cd "$(dirname "$0")/../.."
val() { grep "^$1=" infra/.env | cut -d= -f2-; }
for ns in kubeflow; do
  kubectl get ns "$ns" >/dev/null 2>&1 || continue
  kubectl -n "$ns" create secret generic s3-credentials --from-literal=access_key="$(val S3_ACCESS_KEY)" \
    --from-literal=secret_key="$(val S3_SECRET_KEY)" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n "$ns" create secret generic feast-credentials --from-literal=ml_db_password="$(val ML_DB_PASSWORD)" \
    --from-literal=redis_password="$(val REDIS_PASSWORD)" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  echo "secrets in $ns: ok"
done
