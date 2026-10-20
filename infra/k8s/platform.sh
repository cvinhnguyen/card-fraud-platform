#!/bin/sh
# Install the ML platform into the kind cluster (after up.sh). Idempotent; versions pinned.
#   KFP 2.17.2 standalone (platform-agnostic: its own MySQL + object store), API on NodePort 31888, UI on 31880
#   cert-manager (KServe's webhook certificates), KServe 0.21 in Standard mode (formerly RawDeployment, no Knative)
set -e
cd "$(dirname "$0")"
KFP=2.17.2 CERT_MANAGER=v1.21.1 KSERVE=v0.21.0

kubectl apply -f namespaces.yaml
# KFP: kustomize from the release tag
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=$KFP"
kubectl wait --for condition=established --timeout=120s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=$KFP"
kubectl apply -f nodeports.yaml -f spark-rbac.yaml
# cert-manager + KServe
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER/cert-manager.yaml"
kubectl -n cert-manager wait --for=condition=Available deploy --all --timeout=300s
helm upgrade --install kserve-crd oci://ghcr.io/kserve/charts/kserve-crd --version "$KSERVE" -n kserve --create-namespace
# 0.21: the chart is kserve-resources, and RawDeployment is called Standard (plain Deployment + Service)
helm upgrade --install kserve oci://ghcr.io/kserve/charts/kserve-resources --version "$KSERVE" -n kserve \
  --set kserve.controller.deploymentMode=Standard --wait --timeout 10m
sh secrets.sh
kubectl -n kubeflow wait --for=condition=Available deploy --all --timeout=900s
# NodePorts are reachable from the compose network (fraud-control-plane:31888); from the host use
#   kubectl -n kubeflow port-forward svc/ml-pipeline 31888:8888   and   svc/ml-pipeline-ui 31880:80
echo "platform ready"
