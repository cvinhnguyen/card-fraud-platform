#!/bin/sh
# Releases of the batch code (pipelines/airflow/Jenkinsfile) in the volume fraud_batch_release, mounted at
# /opt/release in Airflow. One directory per commit, and the symlink `current` that Airflow's DAG folder and the
# Spark / dbt / ml paths go through. Moving the symlink switches all of them at once, between two tasks (the
# git-sync pattern of Airflow's Helm chart). Runs as root in a throwaway container (Jenkins, or by hand):
#
#   seed            compose (batch-release-init): `local` = the working tree mounts; current -> local if unset
#   install <sha>   tar of the commit on stdin -> <sha>/{dags,spark,dbt,ml}
#   switch <name>   current -> <sha> | local | previous; the old target becomes `previous`
#   show            current and previous
#
# By hand, e.g. back to the working tree:
#   docker run -i --rm --user 0 --entrypoint sh -v fraud_batch_release:/opt/release fraud/airflow:2.10.5 \
#     -s switch local < pipelines/airflow/release.sh
set -eu
R=${RELEASES_DIR:-/opt/release}
KEEP=5   # newest releases kept, besides local, current and previous
cd "$R"

case "${1:-}" in
seed)
  mkdir -p local
  ln -sfn /opt/airflow/dags local/dags
  ln -sfn /opt/jobs local/spark
  ln -sfn /opt/airflow/dbt local/dbt
  ln -sfn /opt/airflow/ml local/ml
  [ -e current ] || ln -s local current
  echo "release volume: current -> $(readlink current)"
  ;;
install)
  sha=${2:-}
  echo "$sha" | grep -Eq '^[0-9a-f]{7,40}$' || { echo "install: not a commit sha: $sha" >&2; exit 2; }
  if [ -d "$sha" ]; then   # a commit's files never change: keep the copy Airflow may be reading
    cat >/dev/null
    echo "release $sha already installed"
    exit 0
  fi
  rm -rf ".tmp-$sha" && mkdir -p ".tmp-$sha/r"
  tar -x -C ".tmp-$sha"
  t=".tmp-$sha"
  mv "$t/pipelines/airflow/dags" "$t/r/dags"
  mv "$t/pipelines/spark" "$t/r/spark"
  mv "$t/pipelines/dbt" "$t/r/dbt"
  mv "$t/ml" "$t/r/ml"
  mv "$t/r" "$sha" && rm -rf "$t"
  echo "release $sha installed"
  ;;
switch)
  to=${2:-}
  if [ "$to" = previous ]; then to=$(readlink previous || true); fi
  [ -n "$to" ] && [ -d "$to" ] || { echo "switch: no release '$to'" >&2; exit 2; }
  old=$(readlink current || true)
  ln -sfn "$to" current.new && mv -T current.new current   # rename over the old link: atomic
  if [ -n "$old" ] && [ "$old" != "$to" ]; then ln -sfn "$old" previous; fi
  keep=" local $to $(readlink previous || true) "
  n=0
  for d in $(ls -1t); do   # newest first
    [ -d "$d" ] && [ ! -L "$d" ] || continue
    case "$keep" in *" $d "*) continue ;; esac
    n=$((n + 1))
    if [ "$n" -gt "$KEEP" ]; then rm -rf "$d"; fi
  done
  echo "current -> $to (previous -> $(readlink previous || echo -))"
  ;;
show)
  echo "current -> $(readlink current || echo -) (previous -> $(readlink previous || echo -))"
  ;;
*)
  echo "usage: release.sh seed | install <sha> | switch <sha|local|previous> | show" >&2
  exit 2
  ;;
esac
