"""Create the Airflow connections, variables and pools the DAGs use (run by the airflow-init container).

Values come from the init container's environment (infra/.env); they are stored in the Airflow metadata database,
passwords encrypted with the Fernet key, and are never printed. Re-running replaces them, so a rotated secret only
needs `make_env.py` + `docker compose ... up airflow-init`.
"""
import json
import os

from airflow.models import Connection, Pool, Variable
from airflow.utils.session import create_session

E = os.environ
CONNECTIONS = [
    # Spark standalone master; drivers run in the scheduler container (Python has no cluster deploy mode).
    Connection(conn_id="spark_lakehouse", conn_type="spark", host="spark://spark-master", port=7077,
               extra=json.dumps({"deploy-mode": "client", "spark-binary": "spark-submit"}),
               description="Spark standalone master for DP1-DP3 jobs"),
    Connection(conn_id="core_banking_db", conn_type="postgres", host="postgres", port=5432, schema="core_banking",
               login=E["CORE_DB_USER"], password=E["CORE_DB_PASSWORD"],
               description="Core banking database (another department), source of DP1"),
    Connection(conn_id="lakehouse_s3", conn_type="aws", login=E["S3_ACCESS_KEY"], password=E["S3_SECRET_KEY"],
               extra=json.dumps({"endpoint_url": "http://seaweedfs:8333", "region_name": "us-east-1"}),
               description="SeaweedFS S3: export bucket of core banking + the lakehouse bucket"),
    Connection(conn_id="spark_thrift", conn_type="generic", host="spark-thrift", port=10000, schema="gold",
               description="Spark Thrift Server used by dbt-spark (no auth, local network only)"),
    Connection(conn_id="kafka_broker", conn_type="generic", host="kafka", port=19092,
               description="Kafka broker (internal listener): streaming sources, DataHub's event bus"),
    Connection(conn_id="datahub_rest_default", conn_type="datahub_rest", host="http://datahub-gms:8080",
               description="DataHub GMS (gov profile): Airflow listener + GE assertion results"),
    # Layer 2 (ml profile + kind cluster, whose node joins the compose network as fraud-control-plane)
    Connection(conn_id="feast_registry", conn_type="postgres", host="ml-db", port=5432, schema="feast", login="ml",
               password=E["ML_DB_PASSWORD"], description="Feast SQL registry"),
    Connection(conn_id="feast_online", conn_type="redis", host="redis", port=6379, password=E["REDIS_PASSWORD"],
               description="Feast online store (Redis)"),
    Connection(conn_id="mlflow", conn_type="http", host="http://mlflow:5000",
               description="MLflow tracking server + model registry"),
    Connection(conn_id="kfp_api", conn_type="http", host="http://fraud-control-plane:31888",
               description="Kubeflow Pipelines API (NodePort of the kind cluster): retraining runs"),
    # Layer 3 (RAG knowledge base)
    Connection(conn_id="feast_kb_online", conn_type="postgres", host="vector-db", port=5432, schema="kb", login="kb",
               password=E["VECTOR_DB_PASSWORD"], description="Feast online store of the RAG knowledge base (pgvector)"),
    Connection(conn_id="llm_embeddings", conn_type="http", host="http://host.docker.internal:8086",
               password=E["LLM_API_KEY"], description="Embedding server (llama.cpp on the host GPU machine)"),
]
VARIABLES = {
    "spark_jobs_dir": "/opt/jobs",                         # pipelines/spark/ is mounted here in the Spark and Airflow containers
    "dbt_project_dir": "/opt/airflow/dbt",
    "dbt_artifacts_dir": "/opt/airflow/dbt-artifacts",
    "export_bucket": "core-banking-export",                # nightly transaction exports, one folder per export_date
    "lakehouse_bucket": "lakehouse",
    "validation_prefix": "_validation",                    # GE results: s3://lakehouse/_validation/<suite>/ds=<ds>/
    "feast_repo": "/opt/airflow/feature_repo",             # Layer 2: ml/feature_repo/ mounted read-only
    "feast_kb_repo": "/opt/airflow/ml/feature_repo_kb",    # Layer 3: the knowledge base's Feast repo (in the ml/ mount)
    "drift_psi_threshold": "0.25",                         # PSI above this on any input = major drift
    "drift_pr_auc_drop": "0.05",                           # PR-AUC this far below the model's test PR-AUC = decay
}

if __name__ == "__main__":
    with create_session() as s:
        for c in CONNECTIONS:
            s.query(Connection).filter(Connection.conn_id == c.conn_id).delete()
            s.add(c)
    for k, v in VARIABLES.items():
        Variable.set(k, v, description=None)
    # One Spark application at a time: the worker has 3 GB, one 2 GB executor + the Thrift Server's 1 GB.
    Pool.create_or_update_pool("spark", slots=1, description="Spark applications on the 3 GB worker",
                               include_deferred=False)
    print(f"connections: {', '.join(c.conn_id for c in CONNECTIONS)}; variables: {', '.join(VARIABLES)}; pool: spark=1")
