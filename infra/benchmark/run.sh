#!/bin/sh
# Run agentic.yaml against the model server, directly or through the platform (agentgateway -> llm-d router ->
# InferencePool), and keep the reports in infra/benchmark/results/<name>/.
#   sh infra/benchmark/run.sh <name> direct|gateway
# Needs: docker build -t fraud/inference-perf:0.7.0 infra/benchmark, llama.cpp on the host (:8085), the kind cluster.
# The keys come from infra/.env and only exist inside the container: the config is piped in, never written to disk,
# and the copy inference-perf keeps with its reports is deleted.
set -e
cd "$(dirname "$0")"
name=$1
key() { grep "^$1=" ../.env | cut -d= -f2-; }
case $2 in
  direct)  url=http://host.docker.internal:8085; api_key=$(key LLM_API_KEY) ;;
  gateway) url=http://fraud-control-plane:$(kubectl -n agentgateway-system get svc agentgateway-proxy \
             -o jsonpath='{.spec.ports[0].nodePort}'); api_key=$(key AGW_API_KEY) ;;
  *) echo "usage: run.sh <name> direct|gateway" >&2; exit 1 ;;
esac
docker rm -f "bench-$name" >/dev/null 2>&1 || true
sed -e "s|@BASE_URL@|$url|" -e "s|@API_KEY@|$api_key|" agentic.yaml | docker run -i --name "bench-$name" \
  --network kind --add-host host.docker.internal:host-gateway -v inference-perf-hf:/root/.cache/huggingface \
  --entrypoint sh fraud/inference-perf:0.7.0 -c 'cat > /tmp/config.yaml && inference-perf --config_file /tmp/config.yaml'
rm -rf "results/$name" && mkdir -p results
docker cp "bench-$name:/bench/reports" "results/$name" >/dev/null
docker rm "bench-$name" >/dev/null
find "results/$name" -name 'config.yaml' -delete   # holds the API key
find "results/$name" \( -name 'inference-perf.partial.*' -o -name 'validation.json' \) -delete   # nothing reads them
echo "reports: infra/benchmark/results/$name"
