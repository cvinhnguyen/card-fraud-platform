#!/bin/sh
# Wait until each Agent runs its current spec: kagent has processed this generation (status.observedGeneration) and
# its Ready condition is True. Usage (KUBECONFIG set): sh agents/wait_ready.sh card-data-agent risk-agent
# `kubectl wait --for=condition=Ready` is not enough: kubectl 1.37 also waits for the Ready condition's own
# observedGeneration, and kagent refreshes that only when the agent's pod changes. A spec change that leaves the pod as
# it was (the same prompt text, moved inline) then never counts as Ready.
set -e
for agent in "$@"; do
  for i in $(seq 60); do
    set -- $(kubectl -n kagent get agent "$agent" \
      -o jsonpath='{.metadata.generation} {.status.observedGeneration} {.status.conditions[?(@.type=="Ready")].status}')
    [ "$1" = "$2" ] && [ "$3" = True ] && break
    sleep 5
  done
  [ "$1" = "$2" ] && [ "$3" = True ] || { echo "$agent: not Ready on generation $1 after 5 min" >&2; exit 1; }
  echo "$agent: Ready on generation $1"
done
