"""Metadata ingestion into DataHub (gov profile): the sources OpenLineage does not describe.

Lineage of the lakehouse tables arrives on its own (the OpenLineage listener in every Spark job, docs/openlineage.md) and so
do the DAG / task entities (DataHub's Airflow listener). This DAG adds what no job run reports:

| Task | Recipe (`datahub_recipes/`) | What appears in DataHub |
|---|---|---|
| `core_banking` | `core_banking.yml` | the 6 Postgres tables of the source system with columns and row counts |
| `kafka_topics` | `kafka.yml` | `card_authorizations`, `app_events`, `card_features_rt` |
| `dbt` | `dbt.yml` | dbt models with descriptions, column docs, model lineage and the dbt test results |

Hosts and credentials come from Airflow connections; the recipes read them from the environment.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from common import DEFAULT_ARGS

DATAHUB = "/opt/venvs/datahub/bin/datahub"
RECIPES = "/opt/airflow/dags/datahub_recipes"
ENV = {
    "DATAHUB_GMS_URL": "{{ conn.datahub_rest_default.host }}",
    "DATAHUB_TELEMETRY_ENABLED": "false",
    "CORE_DB_HOST_PORT": "{{ conn.core_banking_db.host }}:{{ conn.core_banking_db.port }}",
    "CORE_DB_USER": "{{ conn.core_banking_db.login }}",
    "CORE_DB_PASSWORD": "{{ conn.core_banking_db.password }}",
    "KAFKA_BOOTSTRAP": "{{ conn.kafka_broker.host }}:{{ conn.kafka_broker.port }}",
    "DBT_TARGET": "{{ var.value.dbt_artifacts_dir }}/target",
}

with DAG("datahub_metadata", schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args={**DEFAULT_ARGS, "depends_on_past": False}, doc_md=__doc__, tags=["governance"]) as dag:
    for name in ["core_banking", "kafka_topics", "dbt"]:
        recipe = {"kafka_topics": "kafka"}.get(name, name)
        BashOperator(task_id=name, bash_command=f"{DATAHUB} ingest -c {RECIPES}/{recipe}.yml", env=ENV, append_env=True)
