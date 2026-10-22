# Card Fraud Platform

Real-time card fraud detection and card support agents for a Vietnamese digital bank, built as the EDAI K11
coursework in three layers: a data platform (Layer 1), an ML platform (Layer 2) and an LLM and agent platform
(Layer 3).

## Table of contents
- [Business domain](#business-domain)
- [Architecture](#architecture)
  - [Layer 1: data platform](#layer-1-data-platform) · [Layer 2: ML platform](#layer-2-ml-platform) · [Layer 3: LLM and agent platform](#layer-3-llm-and-agent-platform)
  - [System diagram](#system-diagram) · [Layer 3 system diagram](#layer-3-system-diagram) · [Deployment](#deployment)
- [Repo structure](#repo-structure)
- [Quick start](#quick-start) · [Layer 3](#layer-3)
- [Docs](#docs)

## Business domain

A Vietnamese digital bank issues debit and credit cards and runs a mobile banking app. Fraud shows up as card payments
the cardholder did not make (mostly card-not-present, often abroad) and as account takeovers in the app (password
reset, new device, new payee, transfer). The bank needs to score each card authorization in real time and to train
and monitor the model on history, which means a data platform that brings together data owned by other departments:

| Source | Owner | How it arrives | Problems it brings |
|---|---|---|---|
| Customers, accounts, cards, merchants, disputes | core banking (Postgres) | DP1 reads it over JDBC | SCD2 history, province names before / after the 2025-07-01 reorganization (63 -> 34 units) |
| Settled transactions | core banking nightly export (files on S3) | DP1 loads the new export folders | 2% duplicates from export re-runs, auth columns added on 2024-07-01 (schema evolution), merchant and city skew |
| Card authorizations | card network | Kafka `card_authorizations` | late events from offline POS terminals, network-retry duplicates |
| App events | mobile app | Kafka `app_events` | events out of order within a session |
| Fraud labels | disputes, reported days to weeks later | DP2 (`gold.label_transaction`) | label delay |
| Card documents (terms, fees, disputes, authentication rules, FAQ) | product and legal teams | markdown files in `s3://bank-documents` | must be searchable by meaning, kept in sync with what the agents cite |

The same customers then call card support: why was my payment declined, what does a withdrawal abroad cost, am I
liable for a payment I did not make. Layer 3 answers with agents that use the platform's own data: the card's
features from the feature store, the fraud model's verdict through the Scoring API, and the bank's card documents
through a RAG pipeline.

All data is synthetic (`generator/`), with every problem injected at a measured rate and checked
([docs/data_characteristics.md](docs/data_characteristics.md)). Layer 1 is the data platform; Layer 2 (feature
store, training, serving, drift) consumes its `feat_*` tables and the realtime features; Layer 3 (agents) consumes
Layer 2's online features and model.

## Architecture

![Platform overview: Layer 1 builds the features, Feast hands them to Layer 2, which trains, serves and monitors the model, and Layer 3's agents answer card support questions from the same features, the model and the bank's documents; below them the CI/CD path (GitHub, Jenkins, registry, deploy) and what runs in the cloud (SaaS and the Azure IaC resources)](docs/images/platform-overview.png)

Layer 1 turns the source systems into feature tables (batch) and realtime features (stream). Feast is the hand-over:
the same 20 features for training (offline store, point-in-time joins) and for serving (online store). Layer 2 trains
on them, serves the model behind the Scoring API and retrains when the drift check fails. Layer 3 reads the same
online features and the Scoring API through its MCP server, adds the bank's documents as a second Feast project
(chunks and vectors), and runs the agents behind one gateway. The numbers are the steps of the tables below. The
two rows at the bottom show how a change ships (CI/CD: GitHub, Jenkins, the signed image, deploy and check) and
what runs in the cloud: SaaS, and the Azure resources an IaC run creates ([Deployment](#deployment)).

### Layer 1: data platform

| Step | Component | What it does |
|---|---|---|
| 01 · DP1 | Spark ingest | Copies `core_banking` tables (JDBC) and the daily export files (S3A) into bronze `raw_*` Delta tables, unchanged |
| 02 · DP2 | Spark silver | Dedup, `mergeSchema` for the 2024-07-01 auth columns, 63 → 34 unit normalization, skew salting → `stg_*` |
| 03 · DP2 | dbt-spark (novel idea 1) | Gold `dim_*` / `fact_*` on Spark (own session in Airflow, Thrift Server by hand), SCD2 `dim_customer` model, incremental fact, 22 dbt tests |
| 04 · DP3 | Spark features | `feat_*` tables with `event_timestamp` and `created` for the Layer 2 feature store |
| 05 | Trino | SQL over gold and feature tables through the Hive Metastore |
| 06 | Flink | Watermarks, late events, dedup and windows on Kafka; raw events → bronze, realtime features → `card_features_rt` |
| 07 | Airflow | Runs DP1 → DP2 → DP3 (each: ingest stage + Great Expectations validate stage); connections and variables in Airflow |
| 08 | DataHub | Tables of every zone, lineage sent by every Spark job through OpenLineage (novel idea 2), validation results as assertions |

### Layer 2: ML platform

| Step | Component | What it does |
|---|---|---|
| 09 | Feast (`ml/feature_repo/`) | Offline store = the lakehouse Delta tables (Spark), online store = Redis; daily stats materialized by Airflow, realtime and device features pushed from Kafka (`ml/jobs/push_job.py`) |
| 10 | Notebook | The same 7 steps the pipeline runs, one by one, ending with both models saved and an MLflow run (`ml/notebooks/fraud_model.ipynb`) |
| 11 | Kubeflow Pipelines on kind | `train_fraud_model`: labels -> Feast point-in-time features -> data checks -> time split -> distributed XGBoost (Spark executor pods) -> evaluation gate -> MLflow registry |
| 12 | MLflow | Experiment runs, data versions, model registry with aliases `challenger` / `production` |
| 13 | KServe | InferenceService (Standard mode) serving the `production` booster baked into the image |
| 14 | Fraud Scoring API | FastAPI: online features from Feast + KServe score -> approve / step_up / decline, with a rule fallback |
| 15 | Drift API + DAG | Logs every scored request, PSI per feature against the training profile; Airflow `drift_monitoring` triggers a retraining run in KFP |

Five classes carry the ML logic:

| Class | File | Job |
|---|---|---|
| `TrainingDataService` | `ml/fraud_ml/data.py` | Labels as of a date, Feast point-in-time join, versioned training set (Delta) |
| `FraudModelTrainer` | `ml/fraud_ml/train.py` | Logistic-regression baseline + distributed `SparkXGBClassifier` |
| `ModelEvaluator` | `ml/fraud_ml/evaluate.py` | Thresholds on validation, test metrics, gate vs baseline and `production` |
| `ModelRegistry` | `ml/fraud_ml/registry.py` | MLflow logging, model versions and aliases |
| `FraudScorer` | `services/scoring_api/app/scorer.py` | Request -> features -> score -> decision, fallback when KServe is down |

### Layer 3: LLM and agent platform

| Step | Component | What it does |
|---|---|---|
| 16 | RAG pipeline | Airflow `rag_ingest`: bank documents -> 57 chunks -> 1024-dim vectors -> Feast `card_support_kb` (pgvector online, Parquet offline); 7 checks and lineage to DataHub |
| 17 | llama.cpp | Qwen3-8B Q4_K_M chat model on the host GPU (GTX 1070) and the Qwen3-Embedding-0.6B embedding server |
| 18 | llm-d | InferencePool `qwen3-8b` and its endpoint picker in front of the model server |
| 19 | agentgateway | One entry point for LLM (`/v1`), MCP (`/mcp`) and A2A calls: API keys, rate limits, PII masking, tool access per agent |
| 20 | card-support-mcp | FastAPI Web API + MCP server with four tools: card data, policy chunk, policy search (RAG), fraud score |
| 21 | NGINX Ingress | Every UI and the Web API on `*.cvinhnguyen.duckdns.org`: Let's Encrypt HTTPS, basic auth, rate limits |
| 22 | Coordinator agent | Talks to the customer, asks the two specialist agents over A2A |
| 23 | Card-data + risk agents | kagent agents with MCP tools through the gateway; 2-4 replicas each (KEDA) |
| 24 | Agent Substrate | Sandboxed copies of the agents in gVisor, restored from warm snapshots on a shared worker pool |
| 25 | agentregistry | Catalog of the agents and the MCP server they use |
| 26 | Operations | Jenkins CI/CD (prompts from Langfuse, eval gate, signed images), Prometheus + Grafana, VictoriaLogs, Jaeger, Vault |

### System diagram

![System architecture: deployable units and the numbered data flows between them](docs/images/platform-architecture.png)

Every box is a deployable unit: a compose container or a workload on the kind cluster (Feast is a library inside
them, so it has no box). Arrows follow the data and carry it as their label; a call without data (B1, M7) points from
caller to callee. Each flow has its own colour, letter and numbering:

| Flow | Who / when | Path |
|---|---|---|
| **S** Payment scoring | end user, every authorization | card network -> Scoring API (features from Redis, score from KServe) -> decision; every decision logged to Kafka |
| **R** Streaming features | continuous | Kafka -> Flink -> Kafka -> push jobs -> Redis (online) and Delta (offline) |
| **B** Batch data | daily, Airflow | Postgres + export files -> Spark -> bronze -> silver -> gold -> feat_* -> Feast materialize -> Redis |
| **T** Training and deploy | ML engineer | lakehouse -> notebook / Kubeflow pipeline -> MLflow -> predictor image -> KServe |
| **M** Monitoring and retraining | live + daily | prediction log -> Drift API (PSI); drift DAG (PR-AUC, PSI) -> MLflow -> retraining run in KFP |

Every numbered arrow, one line each: [docs/architecture.md](docs/architecture.md#numbered-arrows).

### Layer 3 system diagram

![Layer 3 system architecture: agents, gateway, model servers, RAG ingest, CI/CD and observability, numbered per flow](docs/images/platform-layer3.png)

Support staff reach the agents through NGINX over HTTPS. Every agent call (A2A, MCP, LLM) then passes agentgateway,
which checks keys, limits requests and masks personal data before the llm-d router hands LLM calls to llama.cpp on
the host GPU. The MCP server takes a card's features from the same Feast online store as Layer 2 and a fraud score
from the Scoring API; the RAG pipeline writes policy chunks and their vectors into a second Feast project (pgvector).

| Flow | Who / when | Path |
|---|---|---|
| **A** Agent conversation | support staff, every question | NGINX -> kagent -> coordinator -> gateway -> card-data / risk agents -> gateway -> MCP server (Redis, pgvector, embeddings, Scoring API); LLM calls -> gateway -> llm-d -> llama.cpp |
| **K** Knowledge ingest | Airflow `rag_ingest`, per release | bank documents (S3) -> chunks -> embeddings -> pgvector online + Parquet offline -> DataHub checks and lineage |
| **C** CI/CD | developer push | GitHub -> Jenkins (prompts from Langfuse) -> signed image in the registry -> helm --atomic -> eval gate through the gateway -> Langfuse experiment |
| **O** Observability and scaling | continuous | gateway + MCP metrics -> Prometheus -> Grafana and KEDA (agent replicas 2-4); agent spans -> Jaeger |

Every Layer 3 arrow, one line each: [docs/architecture.md](docs/architecture.md#numbered-arrows-layer-3).

### Deployment

![Deployment: host tools and model servers, compose profiles in the Docker Desktop VM, the kind cluster's namespaces, the Azure resources of the IaC runs, and the SaaS on the internet](docs/images/platform-deployment.png)

| Where | What runs there | Docs |
|---|---|---|
| The Windows PC (16 GB RAM, GTX 1070) | all of Layers 1-3. The compose profiles start what a phase needs; the kind cluster (joined to the compose network) holds Kubeflow Pipelines and KServe for Layer 2 and the whole Layer 3 platform, one namespace per part. The two llama.cpp model servers run on the host next to the GPU, which the cluster reaches through `host.docker.internal` | this README |
| Azure (`rg-cardfraud-dev`, Poland Central) | during an IaC run only: Terraform creates the network, a VM, AKS and a container registry; Ansible deploys the fraud model server and node_exporter to the VM; AKS runs the same model image from the registry. `terraform destroy` removes all of it | [iac.md](docs/iac.md) |
| SaaS | GitHub (Jenkins polls it), Langfuse Cloud EU (agent prompts, eval experiments), Groq (side B of the LLM A/B test), Let's Encrypt with DuckDNS (the HTTPS certificate) | [cicd.md](docs/cicd.md), [ab_testing.md](docs/ab_testing.md), [edge_security.md](docs/edge_security.md) |

The rest of the platform is containers and Helm charts that could run on Azure too. The Azure for Students
subscription keeps it on the PC: 6 vCPU and 3 public IPs per region, a quota of 0 for the T4, A10, A100 and H100 GPU
families, and $100 of credit, which a 16 GB VM would use up in about 18 days
([what would move where, and the numbers](docs/iac.md#why-not-all-of-it-on-azure)). Diagram sources:
`python docs/diagrams/platform.py` (stdlib only; PNG through headless Chrome; SVG next to each PNG).

## Repo structure

Grouped by role: data pipelines (Layer 1), ML code (Layer 2), online services, agents (Layer 3), infrastructure. Repo
paths differ from the paths inside the containers (`pipelines/spark/` is `/opt/jobs`, `ml/` is `/opt/ml` in the ML
image).

```
pipelines/            Layer 1 data pipelines, and the Layer 3 RAG pipeline's CI
  airflow/            DAGs (DP1-DP3, DataHub ingestion, Feast materialization, drift monitoring, rag_ingest), connections/variables bootstrap
  spark/              Spark jobs (mounted at /opt/jobs), benchmarks (skew, storage layout, 100 GB)
  dbt/                dbt-spark project: gold models and tests
  flink/              PyFlink jobs (mounted at /opt/flink/jobs) and their check scripts
  rag/                Jenkinsfile of the RAG data pipeline
ml/                   Layer 2 ML code (Dockerfile = the fraud/ml-train code image, also the image the RAG steps run in)
  feature_repo/       Feast repo card_fraud: entities, sources, feature views, on-demand features, feature services
  feature_repo_kb/    Feast repo card_support_kb (Layer 3): policy chunks and their vectors (pgvector)
  fraud_ml/           library: the 5 classes above and the 7 pipeline steps
  pipelines/          Kubeflow pipeline train_fraud_model (.py) and its compiled .yaml
  jobs/               push jobs, drift job, drift forwarder, rag_ingest.py (RAG steps), Jenkinsfile.stream
  tools/              run by hand: replay_scoring, check_online, drift_by_day, stream_smoke
  notebooks/          fraud_model.ipynb, executed with outputs (models/ = the saved models, gitignored)
services/             scoring_api (FastAPI), drift_api (FastAPI), predictor (KServe custom model)
  card_support_mcp/   Layer 3 Web API + MCP server: app, tests, Helm chart (gateway route, tool access), Jenkinsfile
agents/               Layer 3 agents: Helm chart (agents, A2A routes, KEDA, sandbox, network policy), prompts, two Jenkinsfiles,
                      eval/ (dataset + Langfuse eval gate), registry/ (agentregistry catalog), locustfile
generator/            synthetic source systems: data with injected problems, Kafka producer, checks; knowledge/ = the card documents
infra/                docker-compose (profiles), secrets bootstrap (make_env.py, .env.example), bucket/topic init
  spark/ flink/ airflow/ hive/ ml/ mlflow/   the images built here (multi-stage, see docs/docker_optimization.md)
  k8s/                kind cluster, KFP + KServe, the Layer 3 platform (llm-platform.sh, agentgateway.yaml, llm-d-router.yaml),
                      observability, edge/ (NGINX routes), vault.sh + secrets.sh, tls.sh, supply-chain/ (cosign + Kyverno), dashboards/,
                      aks/ (the model server on Azure AKS)
  jenkins/            Jenkins image, configuration as code (credentials, the 5 jobs), deploy ServiceAccount
  terraform/          Azure: modules network / vm / aks / acr, envs/dev
  ansible/            roles that configure the VM and deploy the model server on it
  benchmark/          inference-perf load test of the model server, results
  conf/               service configs (Spark, Hive, Trino, SeaweedFS, DataHub, notebook)
  sql/                Postgres serving SQL (indexes + EXPLAIN)
docs/                 one file per major part; diagrams/ = the scripts that draw the pictures (platform.py: the 4 above)
```

## Quick start

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt
.venv/Scripts/python infra/make_env.py                        # once: random local secrets (gitignored)
docker compose -f infra/docker-compose.yaml --profile core up -d
.venv/Scripts/python infra/init_infra.py                      # buckets + Kafka topics
.venv/Scripts/python generator/generate.py                    # source data -> data/source
.venv/Scripts/python generator/check_data.py                  # assert every injected problem
.venv/Scripts/python generator/load_source.py                 # Postgres core_banking (127.0.0.1:5433) + s3://core-banking-export
.venv/Scripts/python generator/producer.py --speed 200            # replay card_authorizations into Kafka (--topic app_events, --replay-from, --limit)

# P3: Spark + Hive Metastore + Trino, then DP1 (source -> bronze Delta tables)
docker compose -f infra/docker-compose.yaml --profile core --profile batch up -d --build
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/ingest_bronze.py
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_silver.py
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_labels.py
docker exec -it trino trino --execute "SELECT count(*) FROM delta.bronze.raw_transactions"

# P4 gold: dbt-spark through the Spark Thrift Server
docker compose -f infra/docker-compose.yaml --profile dbt up -d spark-thrift
.venv/Scripts/dbt build --project-dir pipelines/dbt --profiles-dir pipelines/dbt
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_features.py   # DP3, reads gold

# P6 streaming: Flink session cluster (UI http://127.0.0.1:8088), each check replays a slice and verifies the job
docker compose -f infra/docker-compose.yaml --profile core --profile stream up -d --build
.venv/Scripts/python pipelines/flink/check_windows.py --replay-from "2026-06-01 00:00" --limit 20000

# P7 + P8: Airflow runs DP1 -> DP2 -> DP3; DataHub receives lineage (OpenLineage) and validation results
docker compose -f infra/docker-compose.yaml --profile core --profile batch --profile orch --profile gov up -d --build
docker exec airflow-scheduler airflow dags backfill dp1_source_to_bronze -s 2026-10-12 -e 2026-10-15   # DP2/DP3 follow

# Layer 2: Feast, MLflow, notebook, APIs (profile ml); KFP + KServe on kind
docker build -t fraud/ml:0.1 infra/ml                              # dependencies image, on top of fraud/spark:4.1.3 (batch profile build)
docker build -f ml/Dockerfile -t fraud/ml-train:latest .           # code image: notebook, push jobs, KFP steps, RAG steps
docker compose -f infra/docker-compose.yaml --profile core --profile ml up -d
docker compose -f infra/docker-compose.yaml exec notebook feast -c /opt/ml/feature_repo apply   # order + bootstrap: docs/feature_store.md
bash infra/k8s/up.sh && bash infra/k8s/platform.sh                 # kind cluster, KFP 2.17, KServe 0.21
python ml/pipelines/train_fraud_model.py --run --label-as-of 2026-10-15                # see docs/ml_training.md
```

### Layer 3

The model servers run on the host first (llama.cpp start commands in [docs/llm_platform.md](docs/llm_platform.md#start-the-model-servers)),
then the cluster scripts in this order; each is idempotent and reads its secrets from `infra/.env`.

```bash
sh infra/k8s/vault.sh                  # Vault + External Secrets; secrets.sh writes every secret into Vault
sh infra/k8s/llm-platform.sh           # Gateway API, agentgateway, llm-d router, kagent, Agent Substrate, KEDA, agentregistry
sh infra/k8s/observability.sh          # Prometheus + Grafana (dashboards), VictoriaLogs, Jaeger
sh infra/k8s/tls.sh && sh infra/k8s/edge.sh   # HTTPS certificate (DuckDNS, Let's Encrypt), NGINX Ingress + routes
.venv/Scripts/python generator/load_source.py --part documents && docker exec airflow-scheduler airflow dags trigger rag_ingest
sh infra/jenkins/setup.sh              # Jenkins (profile cicd): its 5 pipelines build, test and deploy the MCP server and the agents
arctl apply -f agents/registry/catalog.yaml   # the registry catalog (kubectl -n agentregistry port-forward svc/agentregistry 12121:12121)
```

By hand instead of Jenkins: `docker build --target runtime -t localhost:5001/card-support-mcp:<tag> services/card_support_mcp`,
push it, `helm upgrade --install card-support-mcp services/card_support_mcp/chart -n card-support --atomic --set image.tag=<tag>`,
then `helm upgrade --install card-support-data-agents agents/chart -n kagent -f agents/chart/values-data-agents.yaml` and
the same with `card-support-coordinator` / `values-coordinator.yaml`.

UIs: Spark master http://127.0.0.1:8080, running job http://127.0.0.1:4040, finished jobs http://127.0.0.1:18080, Trino http://127.0.0.1:8090, Flink http://127.0.0.1:8088, Airflow http://127.0.0.1:8082 (user `admin`), DataHub http://127.0.0.1:9002 (user `datahub`), MLflow http://127.0.0.1:5000, Jupyter http://127.0.0.1:8899 (token `JUPYTER_TOKEN`), KFP UI http://127.0.0.1:31880 (port-forward, `infra/k8s/platform.sh`), Scoring API http://127.0.0.1:8000/docs, Drift API http://127.0.0.1:8001/docs, Jenkins http://127.0.0.1:8095 (user `admin`); Layer 3 behind NGINX: https://agents, registry, grafana, logs, traces and api `.cvinhnguyen.duckdns.org` (user `admin`, `EDGE_ADMIN_PASSWORD`; Grafana has its own login); passwords are in `infra/.env`. The 8 GB Docker VM does not hold every profile at once: run `stream` on its own, keep `trino` and the Thrift Server (`dbt` profile) off while `orch` + `gov` run, and stop the kind cluster while Airflow and DataHub run for the RAG pipeline. From Git Bash, prefix `docker compose exec` with `MSYS_NO_PATHCONV=1` so `/opt/...` paths are not rewritten.

Data volume is set in `generator/config.yaml` (`volume`, ~1 GB: about 7 min to generate, `check_data.py` needs ~4.3 GB RAM). For a quick smoke test: `generate.py --scale 0.05`. What the data contains and the measured problems: [docs/data_characteristics.md](docs/data_characteristics.md).

## Docs
- [Architecture](docs/architecture.md): overview, system diagrams with every numbered arrow (Layers 1-2 and Layer 3), deployment.
- [Data characteristics](docs/data_characteristics.md): datasets, injected problems, measured numbers.
- [Schema design](docs/schema_design.md): zones, naming, grain of every table, gold ER diagram.
- [Spark jobs](docs/spark_jobs.md): DP1 bronze ingest (schema evolution demos), DP2 silver (dedup, unit normalization, HCMC chart), dbt gold star schema, labels with label delay, DP3 features.
- [Spark optimization](docs/spark_optimization.md): merchant join skew, baseline vs salting vs AQE vs broadcast, skew level sweep.
- [Flink jobs](docs/flink_jobs.md): late events, duplicates, realtime features, out-of-order sessions, raw events to bronze; each checked against the Parquet truth.
- [Storage optimization](docs/storage_optimization.md): small files, partitioning, `OPTIMIZE` + Z-order on Delta; B-tree and BRIN indexes on the Postgres serving table.
- [Airflow](docs/airflow.md): DP1 / DP2 / DP3 DAGs, connections and variables in Airflow, validate stages (GE on Spark), backfill.
- [DataHub](docs/datahub.md): tables of every zone, lineage from source to features, which DAG produced each table, validation results.
- [Novel idea 1: dbt-spark](docs/novel_dbt.md): gold star schema in dbt, compared row by row with the same layer written by hand in PySpark.
- [Novel idea 2: OpenLineage](docs/novel_openlineage.md): lineage from every Spark job without a line of lineage code.
- [Docker optimization](docs/docker_optimization.md) and the [100 GB run](docs/scale_100g.md).
- Layer 2: [Feature store](docs/feature_store.md), [ML training](docs/ml_training.md), [Serving](docs/serving.md), [Drift](docs/drift.md), [Testing](docs/testing.md).
- Layer 3: [LLM platform](docs/llm_platform.md): llama.cpp behind the llm-d InferencePool, agentgateway's LLM route (API keys, rate limits, PII masking), the agents' global model config.
- Layer 3: [LLM benchmark](docs/llm_benchmark.md): inference-perf against the model server, three configurations, the prompt-cache cap kept, speculative decoding.
- Layer 3: [Agents](docs/agents.md): the MCP server (FastAPI, pydantic, async, Helm rolling update with automatic rollback), the three agents (KEDA, network policies, Substrate sandbox), MCP and A2A gateway policies, chat in the kagent UI, the agent registry.
- Layer 3: [RAG pipeline](docs/rag_pipeline.md): documents to chunks to vectors in Feast, run on Airflow, checks and lineage in DataHub.
- Layer 3: [CI/CD](docs/cicd.md): five Jenkins pipelines (RAG data pipeline, MCP server, data agents, coordinator, stream feature jobs), configured as code, secrets as Jenkins credentials, the stage views and runs of each.
- Layer 3: [Testing](docs/testing.md#layer-3-the-card-support-mcp-server): MCP server 97 tests at 97.84% coverage, EP/BVA, hypothesis with crosshair, mutmut 243/245, locust load tests of the Web API and the agents.
- Layer 3: [Edge and secrets](docs/edge_security.md): NGINX Ingress for every UI and the Web API, HTTPS with a Let's Encrypt wildcard, basic auth and rate limits, HashiCorp Vault + External Secrets.
- Layer 3: [Observability](docs/observability.md): Web API, compute, LLM and agent dashboards in Grafana, logs in VictoriaLogs, traces in Jaeger.
- Layer 3: [Infrastructure as code](docs/iac.md): Terraform (VNet, VM, AKS, ACR on Azure), Ansible roles that deploy the model server to the VM, the same model image on AKS, and why the rest of the platform stays on the PC.
- Layer 3: [A/B testing](docs/ab_testing.md): an agent's model config and two LLM versions split at the gateway, a Grafana dashboard per side, evaluations, the decisions.
- Layer 3 novel idea 1: [Eval gate in the agents' CD](docs/novel_eval_gate.md): every deploy answers an eval dataset as a Langfuse experiment; below 0.6 accuracy the release rolls back.
- Layer 3 novel idea 2: [Only signed images run](docs/novel_supply_chain.md): CI signs the MCP image with cosign; Kyverno admits only signed images of the platform registry in card-support.
