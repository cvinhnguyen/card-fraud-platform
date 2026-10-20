"""Layer 3 RAG data pipeline: the bank's card documents -> chunks -> embeddings -> Feast (knowledge base of the
card support agents). Run when the documents change (manually or from CI); every step is idempotent.

| Task | What it does |
|---|---|
| `chunk_documents` | reads `s3://bank-documents/*.md`, one chunk per numbered clause / section / FAQ question |
| `embed_chunks` | embedding server on the host GPU machine (connection `llm_embeddings`), 1024-dim vectors |
| `write_feature_store` | Feast project `card_support_kb`: offline history Parquet + online store (pgvector) |
| `validate_rag.run_checks` | checks on the written chunks (row count, unique ids, text, embedding size, all documents, online = offline) |
| `validate_rag.publish_results` | logs the checks, sends them to DataHub as assertions on the feature view, fails the run on any failure |

The steps (`ml/jobs/rag_ingest.py`) run in the ml image the CI pipeline built and released: Airflow Variable
`rag_image` (pipelines/rag/Jenkinsfile sets it per release; fraud/ml-train:latest until then), as sibling
containers on the compose network. Lineage to DataHub is declared with inlets / outlets.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from datahub_airflow_plugin.entities import Dataset

from common import DEFAULT_ARGS, S3_ENV, publish_results

DOCUMENTS = ["card_terms", "dispute_policy", "fee_schedule", "authentication_rules", "faq"]
RAG_ENV = {   # Airflow masks connection passwords in logs and rendered templates
    **S3_ENV,
    "ML_DB_PASSWORD": "{{ conn.feast_registry.password }}",
    "VECTOR_DB_PASSWORD": "{{ conn.feast_kb_online.password }}",
    "EMBEDDINGS_URL": "{{ conn.llm_embeddings.host }}",
    "LLM_API_KEY": "{{ conn.llm_embeddings.password }}",
}
IMAGE = "{{ var.value.get('rag_image', 'fraud/ml-train:latest') }}"
DOCS = [Dataset("s3", f"bank-documents/{d}.md") for d in DOCUMENTS]
CHUNKS = Dataset("s3", "lakehouse/kb/staging/chunks")
EMBEDDED = Dataset("s3", "lakehouse/kb/staging/embedded")
HISTORY = Dataset("s3", "lakehouse/kb/policy_chunks")
FEATURE_VIEW = Dataset("feast", "card_support_kb.policy_chunks")


def step(task_id: str, name: str, inlets: list, outlets: list) -> DockerOperator:
    """One step of ml/jobs/rag_ingest.py in the released ml image, for the run's date."""
    return DockerOperator(task_id=task_id, image=IMAGE, environment=RAG_ENV, inlets=inlets, outlets=outlets,
                          command=f"python3 /opt/ml/jobs/rag_ingest.py --step {name} --ds {{{{ ds }}}}",
                          network_mode="fraud_default", docker_url="unix://var/run/docker.sock",
                          # no host bind of Airflow's temp dir (the daemon runs on the host, not in this container)
                          mount_tmp_dir=False, auto_remove="success", retrieve_output=False)


with DAG("rag_ingest", schedule=None, start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args={**DEFAULT_ARGS, "owner": "llm-platform", "depends_on_past": False}, doc_md=__doc__,
         tags=["layer3", "rag", "feast"]) as dag:
    chunk = step("chunk_documents", "chunk", DOCS, [CHUNKS])
    embed = step("embed_chunks", "embed", [CHUNKS], [EMBEDDED])
    write = step("write_feature_store", "write", [EMBEDDED], [HISTORY, FEATURE_VIEW])
    with TaskGroup("validate_rag") as validate:
        run_checks = step("run_checks", "validate", [FEATURE_VIEW], [])
        run_checks >> publish_results("rag_ingest")
    chunk >> embed >> write >> validate
