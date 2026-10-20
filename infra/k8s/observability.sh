#!/bin/sh
# Install the observability stack into the kind cluster (after llm-platform.sh). Idempotent; versions pinned.
#   metrics  kube-prometheus-stack 91.9.0: Prometheus (also the source of the agents' KEDA scaling), Grafana,
#            node-exporter + kube-state-metrics (CPU, RAM, disk, network); monitors.yaml adds the gateway and the
#            Scoring API; dashboards/*.json: Web API, LLM and agent telemetry
#   logs     VictoriaLogs v1.53 (victoria-logs-single 0.13.10) with its Vector agent shipping every pod's logs
#   traces   Jaeger 2.21 + the gateway's tracing policy (tracing.yaml); kagent agents export spans too
# UIs (P7 puts them behind NGINX):
#   kubectl -n monitoring port-forward svc/monitoring-grafana 3000:80              user admin, GRAFANA_ADMIN_PASSWORD
#   kubectl -n monitoring port-forward svc/victoria-logs-victoria-logs-single-server 9428:9428   /select/vmui
#   kubectl -n monitoring port-forward svc/jaeger 16686:16686
set -e
cd "$(dirname "$0")"
KPS=91.9.0 VLOGS=0.13.10
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
sh secrets.sh   # grafana-admin
helm upgrade -i monitoring kube-prometheus-stack --repo https://prometheus-community.github.io/helm-charts \
  -n monitoring --version "$KPS" -f observability-values.yaml --wait --timeout 10m
kubectl apply -f monitors.yaml
# Grafana's sidecar loads every ConfigMap labelled grafana_dashboard=1
for f in dashboards/*.json; do
  name="dashboard-$(basename "$f" .json)"
  kubectl -n monitoring create configmap "$name" --from-file="$f" --dry-run=client -o yaml \
    | kubectl label --local -f - grafana_dashboard=1 -o yaml | kubectl apply -f -
done
helm upgrade -i victoria-logs victoria-logs-single --repo https://victoriametrics.github.io/helm-charts -n monitoring \
  --version "$VLOGS" --set server.retentionPeriod=3d --set server.persistentVolume.size=2Gi \
  --set server.resources.requests.memory=64Mi --set server.resources.limits.memory=384Mi \
  --set vector.enabled=true --set vector.resources.requests.memory=64Mi --set vector.resources.limits.memory=256Mi \
  --wait --timeout 5m
kubectl apply -f tracing.yaml
echo "observability ready"
