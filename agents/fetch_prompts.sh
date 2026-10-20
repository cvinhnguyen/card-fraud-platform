#!/bin/sh
# Pull each agent's system prompt (Langfuse prompt card-support/<agent>, label production) into
# agents/chart/prompts/<agent>.md and print the helm flags that record the deployed versions:
#   flags=$(sh agents/fetch_prompts.sh card-data-agent risk-agent)   ->   --set promptVersions.card-data-agent=3 ...
# Needs LANGFUSE_HOST, LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY (Jenkins credentials); the keys go to curl on stdin.
set -e
cd "$(dirname "$0")"
for agent in "$@"; do
  json=$(printf 'user = "%s:%s"\n' "$LANGFUSE_PUBLIC_KEY" "$LANGFUSE_SECRET_KEY" \
    | curl -fsS -K - "$LANGFUSE_HOST/api/public/v2/prompts/card-support%2F$agent?label=production")
  printf '%s' "$json" | jq -e '.type == "text"' >/dev/null || { echo "card-support/$agent: not a text prompt" >&2; exit 1; }
  printf '%s' "$json" | jq -j .prompt > "chart/prompts/$agent.md"
  [ -s "chart/prompts/$agent.md" ] || { echo "card-support/$agent: empty prompt" >&2; exit 1; }
  version=$(printf '%s' "$json" | jq -r .version)
  echo "card-support/$agent v$version -> agents/chart/prompts/$agent.md" >&2
  printf ' --set promptVersions.%s=%s' "$agent" "$version"
done
