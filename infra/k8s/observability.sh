#!/bin/sh
# Install the monitoring stack into the kind cluster (after llm-platform.sh). Idempotent; versions pinned.
#   kube-prometheus-stack 91.9.0: Prometheus (also the source of the agents' KEDA scaling) + Grafana
#   infra/k8s/monitors.yaml: what Prometheus scrapes besides Kubernetes
# Grafana: kubectl -n monitoring port-forward svc/monitoring-grafana 3000:80  (user admin, GRAFANA_ADMIN_PASSWORD)
set -e
cd "$(dirname "$0")"
KPS=91.9.0
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
sh secrets.sh   # grafana-admin
helm upgrade -i monitoring kube-prometheus-stack --repo https://prometheus-community.github.io/helm-charts \
  -n monitoring --version "$KPS" -f observability-values.yaml --wait --timeout 10m
kubectl apply -f monitors.yaml
echo "observability ready"
