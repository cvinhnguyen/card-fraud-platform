#!/bin/sh
# Install the ML platform into the kind cluster (after up.sh). Idempotent; versions pinned.
#   KFP 2.17.2 standalone (platform-agnostic: its own MySQL + object store), API on NodePort 31888, UI on 31880
#   cert-manager (KServe's webhook certificates), KServe 0.21 in Standard mode (formerly RawDeployment, no Knative Serving)
#   Knative Eventing (Broker + Trigger only): the InferenceService's payload logger -> Broker -> Drift API
#   The model server itself (kserve-card-fraud.yaml) is applied per model release, see docs/ml-platform/serving.md
set -e
cd "$(dirname "$0")"
KFP=2.17.2 CERT_MANAGER=v1.21.1 KSERVE=v0.21.0 KNATIVE_EVENTING=knative-v1.23.1

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
# Knative Eventing, the smallest install that gives a Broker: CRDs + core, in-memory channel, MT channel-based broker.
# Core first: its webhook validates the channel and broker config maps.
# ponytail: the in-memory channel loses the events in flight when its dispatcher restarts; a Kafka-backed broker
# (knative-extensions/eventing-kafka-broker) is the upgrade if drift counts must survive restarts.
ke="https://github.com/knative/eventing/releases/download/$KNATIVE_EVENTING"
kubectl apply -f "$ke/eventing-crds.yaml"
kubectl wait --for condition=established --timeout=120s crd/brokers.eventing.knative.dev crd/triggers.eventing.knative.dev
kubectl apply -f "$ke/eventing-core.yaml"
kubectl -n knative-eventing wait --for=condition=Available deploy --all --timeout=300s
kubectl apply -f "$ke/in-memory-channel.yaml" -f "$ke/mt-channel-broker.yaml"
kubectl -n knative-eventing wait --for=condition=Available deploy --all --timeout=300s
# the Broker and the Trigger to the Drift API are applied with the model server (kserve-card-fraud.yaml)
sh secrets.sh
kubectl -n kubeflow wait --for=condition=Available deploy --all --timeout=900s
# NodePorts are reachable from the compose network (fraud-control-plane:31888); from the host use
#   kubectl -n kubeflow port-forward svc/ml-pipeline 31888:8888   and   svc/ml-pipeline-ui 31880:80
echo "platform ready"
