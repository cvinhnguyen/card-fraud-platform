"""Pieces shared by the DP1-DP3 DAGs: Spark tasks and the validate stage.

Nothing here names a host or holds a credential. Hosts and secrets are Airflow connections (spark_lakehouse,
lakehouse_s3, core_banking_db, spark_thrift), paths are Airflow variables; both are rendered at run time, so parsing
a DAG never touches the metadata database. Data never goes through XCom, only the path of a results file.
"""
import json
from datetime import datetime, timedelta, timezone

from airflow.decorators import task
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

# depends_on_past: every load is incremental (labels as of ds, MERGE by export date), so day D must not run before
# day D-1 succeeded. Without it a failed D-1 let D run first and D-1's labels were then skipped as already written.
DEFAULT_ARGS = {"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=2), "depends_on_past": True}

# The Spark driver runs in the scheduler container and reads S3 / JDBC credentials from its environment.
# bronze.raw_transactions is in the lakeFS repository `bronze` (bucket s3a://bronze/ in spark-defaults.conf): lakeFS keys.
S3_ENV = {"AWS_ACCESS_KEY_ID": "{{ conn.lakehouse_s3.login }}", "AWS_SECRET_ACCESS_KEY": "{{ conn.lakehouse_s3.password }}",
          "LAKEFS_ACCESS_KEY_ID": "{{ conn.lakefs.login }}", "LAKEFS_SECRET_ACCESS_KEY": "{{ conn.lakefs.password }}"}
CORE_DB_ENV = {
    "CORE_DB_URL": "jdbc:postgresql://{{ conn.core_banking_db.host }}:{{ conn.core_banking_db.port }}"
                   "/{{ conn.core_banking_db.schema }}",
    "CORE_DB_USER": "{{ conn.core_banking_db.login }}",
    "CORE_DB_PASSWORD": "{{ conn.core_banking_db.password }}",
}
GE_PYTHON = "/opt/venvs/ge/bin/python"   # driver Python with Great Expectations (Airflow image)
# Layer 2: the Feast venv of the Airflow image; secrets rendered from connections, read by feature_store.yaml as ${...}
FEAST = "/opt/venvs/feast/bin"
FEAST_ENV = {
    **S3_ENV,
    "ML_DB_PASSWORD": "{{ conn.feast_registry.password }}",
    "REDIS_PASSWORD": "{{ conn.feast_online.password }}",
    "PYTHONPATH": "/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/opt/airflow/ml",
}
SPARK_CONF = {
    # ponytail: 1.5 GB executors (2 GB in spark-defaults) so a pipeline run fits next to DataHub in the 8 GB Docker VM
    "spark.executor.memory": "1536m",
    # OpenLineage names its jobs after this: every table in DataHub shows which DAG's job produced it
    "spark.openlineage.appName": "{{ dag.dag_id }}",
    # The scheduler is on two networks (default + airflow-docker for DockerOperator): its hostname resolves to the
    # airflow-docker address first, so a driver bound there refuses the executors, which reach it on the default one
    "spark.driver.bindAddress": "0.0.0.0",
}


def spark_job(task_id: str, script: str, args: list[str] | None = None, env: dict | None = None,
              conf: dict | None = None, **kwargs) -> SparkSubmitOperator:
    """spark-submit pipelines/spark/<script> to the standalone master. Pool `spark` (1 slot) runs one application at a time,
    because the 3 GB worker fits one 2 GB executor next to the Thrift Server."""
    return SparkSubmitOperator(task_id=task_id, conn_id="spark_lakehouse",
                               application="{{ var.value.spark_jobs_dir }}/" + script, application_args=args or [],
                               env_vars={**S3_ENV, **(env or {})}, conf={**SPARK_CONF, **(conf or {})}, pool="spark",
                               **kwargs)


def emit_assertions(report: dict, run_id: str) -> int:
    """Send every expectation result to DataHub as an assertion on its table (Validation tab): AssertionInfo says what
    is checked, AssertionRunEvent says whether this run passed. The assertion id is stable per (table, expectation,
    column), so each table shows the history across runs. Tables are hive URNs, the same names OpenLineage reports
    (a result may name another platform, e.g. feast for the RAG knowledge base)."""
    from datahub.emitter.mce_builder import datahub_guid, make_assertion_urn, make_dataset_urn
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (AssertionInfoClass, AssertionResultClass, AssertionResultTypeClass,
                                                 AssertionRunEventClass, AssertionRunStatusClass, AssertionStdOperatorClass,
                                                 AssertionTypeClass, DatasetAssertionInfoClass,
                                                 DatasetAssertionScopeClass)
    from datahub_airflow_plugin.hooks.datahub import DatahubRestHook

    emitter = DatahubRestHook("datahub_rest_default").make_emitter()
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    for r in report["results"]:
        dataset = make_dataset_urn(r.get("platform", "hive"), r["table"], "PROD")   # Layer 3 results name feast
        col = r["kwargs"].get("column")
        urn = make_assertion_urn(datahub_guid({"table": r["table"], "expectation": r["expectation"], "column": col}))
        info = AssertionInfoClass(type=AssertionTypeClass.DATASET, description=f"{r['expectation']} {r['kwargs']}",
                                  datasetAssertion=DatasetAssertionInfoClass(
                                      dataset=dataset, operator=AssertionStdOperatorClass._NATIVE_,
                                      nativeType=r["expectation"],
                                      nativeParameters={k: str(v)[:200] for k, v in r["kwargs"].items()},
                                      scope=DatasetAssertionScopeClass.DATASET_COLUMN if col else DatasetAssertionScopeClass.DATASET_ROWS,
                                      fields=[f"urn:li:schemaField:({dataset},{col})"] if col else []))
        result = AssertionResultClass(type=AssertionResultTypeClass.SUCCESS if r["success"] else AssertionResultTypeClass.FAILURE,
                                      unexpectedCount=r["unexpected_count"], rowCount=r["element_count"],
                                      nativeResults={"observed_value": str(r["observed_value"]), "filter": str(r["filter"])})
        emitter.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=info))
        emitter.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=AssertionRunEventClass(
            timestampMillis=now, runId=run_id, asserteeUrn=dataset, assertionUrn=urn,
            status=AssertionRunStatusClass.COMPLETE, result=result)))
    return len(report["results"])


@task
def publish_results(suite: str, ds: str | None = None, run_id: str | None = None) -> str:
    """Read the suite's GE results, log one line per expectation, send them to DataHub (when [datahub] enabled,
    i.e. with the gov profile), then fail the run if any failed.
    Returns the results path (the only thing that goes to XCom)."""
    key = f"{Variable.get('validation_prefix')}/{suite}/ds={ds}/results.json"
    report = json.loads(S3Hook(aws_conn_id="lakehouse_s3").read_key(key, Variable.get("lakehouse_bucket")))
    for r in report["results"]:
        print(f"{'ok  ' if r['success'] else 'FAIL'} {r['table']:<32} {r['expectation']:<48} "
              f"unexpected={r['unexpected_count']} observed={r['observed_value']}")
    # One switch for governance: [datahub] enabled turns on the DataHub listener and these results (gov profile up).
    if conf.getboolean("datahub", "enabled", fallback=False):   # an emit error fails the task, it is not a warning
        print(f"sent {emit_assertions(report, f'{suite}-{ds}-{run_id}')} assertion results to DataHub")
    else:
        print("[datahub] enabled is false: results not sent to DataHub")
    failed = [r for r in report["results"] if not r["success"]]
    if failed:
        raise AirflowException(f"{len(failed)} of {len(report['results'])} expectations failed: "
                               + "; ".join(f"{r['table']} {r['expectation']}" for r in failed[:5]))
    return f"s3://{Variable.get('lakehouse_bucket')}/{key}"


def validate(suite: str, args: list[str] | None = None) -> TaskGroup:
    """Validate stage: GE expectations run in Spark (pipelines/spark/validate.py), then the results are published and gate
    the pipeline. The publish step is separate so the results are recorded even when the data is bad."""
    with TaskGroup(f"validate_{suite}") as group:
        run = spark_job("run_expectations", "validate.py", ["--suite", suite, "--ds", "{{ ds }}", *(args or [])],
                        conf={"spark.pyspark.driver.python": GE_PYTHON})
        run >> publish_results(suite)
    return group


# Write-Audit-Publish on lakeFS (EDAI-1 novel idea, docs/novel_lakefs_wap.md): a DP1 run appends the nightly export on its
# own branch, the checks read that branch, and only a run that passed is merged into main, where the metastore table
# bronze.raw_transactions points.
LAKEFS_REPO = "bronze"


def lakefs(method: str, path: str, body: dict | None = None, ok: tuple = (200, 201)) -> dict:
    """One call to the lakeFS API of repository bronze with the `lakefs` connection; raise on any other status."""
    import requests
    from airflow.hooks.base import BaseHook
    c = BaseHook.get_connection("lakefs")
    r = requests.request(method, f"{c.host}/api/v1/repositories/{LAKEFS_REPO}{path}", json=body,
                         auth=(c.login, c.password), timeout=300)
    if r.status_code == 400 and "no changes" in r.text:   # a run with no new export: nothing to commit or merge
        return {}
    if r.status_code not in ok:
        raise AirflowException(f"lakeFS {method} {path}: {r.status_code} {r.text[:300]}")
    return r.json() if r.headers.get("content-type", "").startswith("application/json") else {}


@task
def create_branch(branch: str) -> str:
    """Start the run's branch from main. A branch left by an earlier failed run of the same day is replaced."""
    lakefs("DELETE", f"/branches/{branch}", ok=(204, 404))
    lakefs("POST", "/branches", {"name": branch, "source": "main"})
    return branch


@task
def commit_branch(branch: str, ds: str | None = None, run_id: str | None = None) -> str:
    """Commit what the run wrote on its branch: the exact state the checks read and the merge publishes."""
    c = lakefs("POST", f"/branches/{branch}/commits",
               {"message": f"DP1 load for {ds}", "metadata": {"airflow_run_id": run_id, "ds": ds}})
    print(f"{branch}: commit {c['id']}" if c else f"{branch}: no new export, nothing to commit")
    return c.get("id", "")


@task
def merge_to_main(branch: str, ds: str | None = None, run_id: str | None = None) -> str:
    """Publish: merge the validated branch into main (readers see the new export in one step), then delete the branch."""
    m = lakefs("POST", f"/refs/{branch}/merge/main",
               {"message": f"Publish DP1 {ds}: validated", "metadata": {
                   "airflow_run_id": run_id, "validation": f"s3://lakehouse/_validation/dp1/ds={ds}/results.json"}})
    lakefs("DELETE", f"/branches/{branch}", ok=(204,))
    print(f"{branch} merged into main: {m['reference']}" if m else f"{branch}: nothing to merge")
    return m.get("reference", "")
