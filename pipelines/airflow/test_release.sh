#!/bin/sh
# Self-check of release.sh on a temporary directory: seed, install, switch, rollback, pruning. Runs in the Airflow
# image in CI (pipelines/airflow/Jenkinsfile, stage DAG check), or anywhere with GNU coreutils:
#   sh pipelines/airflow/test_release.sh
set -eu
S="$(cd "$(dirname "$0")" && pwd)/release.sh"
sh -n "$S"
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/src/pipelines/airflow/dags" "$T/src/pipelines/spark" "$T/src/pipelines/dbt" "$T/src/ml/fraud_ml" "$T/rel"
touch "$T/src/pipelines/airflow/dags/common.py" "$T/src/pipelines/spark/build_silver.py" "$T/src/pipelines/dbt/dbt_project.yml"
tar -C "$T/src" -cf "$T/code.tar" pipelines ml
export RELEASES_DIR="$T/rel"
run() { sh "$S" "$@"; }
link() { readlink "$T/rel/$1"; }

run seed && run seed   # twice: the second keeps current
[ "$(link current)" = local ] && [ "$(link local/dags)" = /opt/airflow/dags ] && [ "$(link local/spark)" = /opt/jobs ]

run install 1a2b3c4 < "$T/code.tar"
[ -f "$T/rel/1a2b3c4/dags/common.py" ] && [ -f "$T/rel/1a2b3c4/spark/build_silver.py" ]
[ -f "$T/rel/1a2b3c4/dbt/dbt_project.yml" ] && [ -d "$T/rel/1a2b3c4/ml/fraud_ml" ]
run install 1a2b3c4 < "$T/code.tar" | grep -q "already installed"
if run install not-a-sha < /dev/null 2>/dev/null; then echo "FAIL: accepted a bad name" >&2; exit 1; fi

run switch 1a2b3c4
[ "$(link current)" = 1a2b3c4 ] && [ "$(link previous)" = local ]
if run switch 9999999 2>/dev/null; then echo "FAIL: switched to a missing release" >&2; exit 1; fi
[ "$(link current)" = 1a2b3c4 ]

for i in 1 2 3 4 5 6 7; do
  run install "000000$i" < "$T/code.tar" >/dev/null && sleep 1 && run switch "000000$i" >/dev/null
done
run switch previous   # rollback: the two swap
[ "$(link current)" = 0000006 ] && [ "$(link previous)" = 0000007 ]
kept=$(find "$T/rel" -mindepth 1 -maxdepth 1 -type d ! -name local | wc -l)
[ "$kept" -le 7 ] && [ ! -d "$T/rel/1a2b3c4" ] && [ -d "$T/rel/local" ]   # 5 newest + current + previous
echo "release.sh: all checks passed ($kept releases kept)"
