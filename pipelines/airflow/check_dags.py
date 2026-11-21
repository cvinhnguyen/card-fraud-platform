"""CI check of the batch DAGs (stage `DAG check` of pipelines/airflow/Jenkinsfile), run in the Airflow image.

Every file in the DAG folder must import, each batch DAG must have exactly the tasks below and carry the tag of its
release, and common.release_of must name a release from its path. A DAG that gains or loses a task fails here
until this list is updated with it.

  python pipelines/airflow/check_dags.py pipelines/airflow/dags
"""
import sys

from airflow.models import DagBag

EXPECTED = {
    "dp1_source_to_bronze": {
        "wait_for_export", "create_branch", "snapshot_core_banking", "ingest_exports", "ingest_stream_landing",
        "commit_branch", "validate_dp1.run_expectations", "validate_dp1.publish_results", "merge_to_main",
        "publish_bronze"},
    "dp2_bronze_to_silver": {
        "build_silver", "validate_dp2_silver.run_expectations", "validate_dp2_silver.publish_results",
        "publish_silver"},
    "dp2_silver_to_gold": {
        "dbt_build", "dbt_docs", "build_labels", "validate_dp2_gold.run_expectations",
        "validate_dp2_gold.publish_results", "publish_gold"},
    "dp3_gold_to_features": {
        "build_features", "validate_dp3.run_expectations", "validate_dp3.publish_results", "publish_features"},
    "feast_materialize": {"materialize_incremental", "check_online"},
    "drift_monitoring": {"compute_drift", "decide", "trigger_retraining", "no_drift"},
    "datahub_metadata": {"core_banking", "kafka_topics", "dbt"},
}


def main(folder: str) -> None:
    """Parse the folder like the scheduler does, then compare each batch DAG with EXPECTED."""
    sys.path.insert(0, folder)   # the scheduler puts its DAG folder on sys.path: `from common import ...`
    import common

    # the release is the directory under /opt/release a file resolves to; anything else is the working tree
    assert common.release_of("/opt/release/1a2b3c4/dags/common.py") == "1a2b3c4"
    assert common.release_of("/opt/airflow/dags/common.py") == "local"
    assert common.release_of(f"{folder}/common.py") == "local"

    bag = DagBag(folder, include_examples=False)
    assert not bag.import_errors, f"import errors: {bag.import_errors}"
    for dag_id, tasks in EXPECTED.items():
        dag = bag.dags.get(dag_id)
        assert dag is not None, f"{dag_id} not found in {folder}"
        assert set(dag.task_ids) == tasks, (f"{dag_id}: missing {sorted(tasks - set(dag.task_ids))}, "
                                            f"unexpected {sorted(set(dag.task_ids) - tasks)}")
        assert common.RELEASE_TAG in dag.tags, f"{dag_id}: no tag {common.RELEASE_TAG} in {dag.tags}"
        print(f"ok  {dag_id:<22} {len(tasks):>2} tasks  tags {sorted(dag.tags)}")
    print(f"{len(bag.dags)} DAGs parsed, {len(EXPECTED)} batch DAGs checked, no import errors")


if __name__ == "__main__":
    main(sys.argv[1])
