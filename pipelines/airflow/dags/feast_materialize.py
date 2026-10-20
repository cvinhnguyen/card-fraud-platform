"""Layer 2: offline -> online store. Triggered by DP3 with the same logical date, after the feature rows of `ds`
are written and validated.

| Task | What it does |
|---|---|
| `materialize_incremental` | `feast materialize-incremental <ds + 1>`: loads into Redis only the rows newer than the last materialization (Feast keeps that watermark in the registry), latest value per card |
| `check_online` | reads a sample of the cards active on `ds` back from Redis through the feature service; fails if their stats are missing or older than the offline rows |

Streaming features do not go through here: push job A writes them to Redis as they arrive.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from common import DEFAULT_ARGS, FEAST, FEAST_ENV

with DAG("feast_materialize", schedule=None, start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args={**DEFAULT_ARGS, "owner": "ml-platform"}, doc_md=__doc__, tags=["layer2", "feast"]) as dag:
    materialize = BashOperator(
        task_id="materialize_incremental", pool="spark", append_env=True, env=FEAST_ENV,
        bash_command=f"cd {{{{ var.value.feast_repo }}}} && {FEAST}/feast materialize-incremental "
                     "{{ macros.ds_add(ds, 1) }}T00:00:00")   # DP3 rows of ds are stamped ds + 1 00:00

    check = BashOperator(
        task_id="check_online", append_env=True, env=FEAST_ENV,
        bash_command=f"{FEAST}/python /opt/airflow/ml/tools/check_online.py --repo {{{{ var.value.feast_repo }}}} --ds {{{{ ds }}}}")

    materialize >> check
