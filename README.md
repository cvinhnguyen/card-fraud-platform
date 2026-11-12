# Card Fraud Platform

Real-time card fraud detection and card support agents for a Vietnamese digital bank, built as the EDAI K11
coursework in three layers: a data platform (Layer 1), an ML platform (Layer 2) and an LLM and agent platform
(Layer 3).

## Table of contents
- [Business domain](#business-domain)
- [Architecture](#architecture)
  - [Layer 1: data platform](#layer-1-data-platform) · [Pipelines on Airflow](#pipelines-on-airflow) · [Layer 2: ML platform](#layer-2-ml-platform) · [Layer 3: LLM and agent platform](#layer-3-llm-and-agent-platform)
  - [System diagram, Layers 1 and 2](#system-diagram-layers-1-and-2) · [System diagram, Layer 3](#system-diagram-layer-3) · [Deployment](#deployment)
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
| Customers, accounts, cards, merchants, disputes | core banking (Postgres) | DP1 (the ingest pipeline) reads it over JDBC | SCD2 history, province names before / after the 2025-07-01 reorganization (63 -> 34 units) |
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
([docs/data-platform/data_characteristics.md](docs/data-platform/data_characteristics.md)). Layer 1 is the data platform; Layer 2 (feature
store, training, serving, drift) consumes its `feat_*` tables and the realtime features; Layer 3 (agents) consumes
Layer 2's online features and model.

## Architecture

Four pictures describe the platform, each answering one question:

| Picture | Answers |
|---|---|
| [Overview](#architecture) (below) | What are the parts, and how does data move from the bank's systems to a fraud decision and to the agents? |
| [System diagram, Layers 1 and 2](#system-diagram-layers-1-and-2) | Which containers exchange which data, step by step, in each flow (scoring, streaming, batch, training, monitoring)? |
| [System diagram, Layer 3](#system-diagram-layer-3) | The same for the agents: a question, the knowledge ingest, CI/CD, monitoring |
| [Deployment](#deployment) | Where does each container run: the PC, the Docker VM, the kind cluster, Azure, the internet? |

Start with the overview. The bank's data comes in on the left. Layer 1 (the top band) turns it into clean tables
and features. In Layer 2 (bottom right) a model scores every card payment in real time and is retrained when the
data drifts. In Layer 3 (bottom left) agents answer the support staff's questions about a card with the same
data. Under the layers, the CI/CD row shows how a change reaches the platform, and the last row holds the services
every layer uses.

| In the picture | Means |
|---|---|
| A box | one part of the platform: its name, the technology under the name, then what it does as short bullets |
| Dashed frame with steps | a pipeline, drawn as its steps from top to bottom |
| White step with pointed ends | a check inside a pipeline; its two outcomes, no and yes, sit under it |
| Grey box | a system outside the platform: core banking, the card network, the mobile banking app |
| Green box | a pipeline or a job: it processes data |
| White box | a service: it answers requests |
| Blue box | a store: where data rests (a lakehouse zone, a feature store, a Kafka topic) |
| Yellow box | the event stream (Kafka) |
| Purple box | an entry point or gateway |
| Dashed box | a note for the reader |
| Raspberry `NOVEL IDEA` pill | one of the four novel ideas, beyond the course |
| Arrow | data or a call, from the part that sends it to the part that receives it; the words on it say what moves |
| Arrow colour | its flow: orange payment scoring, blue streaming, green batch data, dashed purple training and release, brown monitoring, wine agent question (key at the bottom of the picture) |

The whole platform on one page:

![Platform overview: the bank's sources on the left; in Layer 1, DP1, the DP2 silver and gold pipelines and DP3 each read one zone and write the next, Flink jobs turn Kafka events into live features and alerts, and Feast keeps the features offline and online; in Layer 2, the Scoring API decides each payment with the KServe model, KServe's logger feeds the Drift API through a Knative broker and the training pipeline registers every model; in Layer 3, the support chat, the agents, the MCP tool servers and the model servers meet at agentgateway; each pipeline is drawn as its steps and checks; the CI/CD path and the shared services are in the bottom rows](docs/images/platform-overview.png)

Words in the picture:

| Word | Means |
|---|---|
| Zone | one stage of the data in the lakehouse: bronze, the raw zone (exact copy of the sources; Flink's raw files land in `landing/` first), silver (cleaned), gold (modelled for analysis and labels), features (per-card statistics) |
| DP1, DP2, DP3 | the course's names for the three batch pipelines: DP1 loads the sources into bronze, DP2 turns bronze into silver and gold (two DAGs here, one per zone), DP3 turns gold into features |
| Feast offline / online store | the same features kept twice: as they were at any past moment (to train), and only the latest value per card (to score in milliseconds) |
| PSI | population stability index: how far a feature's values have moved away from the training data; above 0.25 counts as drift here |
| MCP | Model Context Protocol: how an agent calls a tool server |
| A2A | agent-to-agent protocol: how one agent asks another |

### Layer 1: data platform

Layer 1 collects the data that other departments own and turns it into the features the fraud model reads. Batch
pipelines run daily on Spark and pass data to each other only through the zones of the lakehouse (Delta tables on
SeaweedFS, registered in the Hive Metastore): each one reads the zone the one before it wrote. Streaming jobs on
Flink compute the realtime features within seconds of each payment.

| Part | What it does | Docs |
|---|---|---|
| DP1, data pipeline: ingest to bronze | Spark copies the `core_banking` tables (JDBC), the daily export files (S3A, `mergeSchema` for the 2024-07-01 auth columns) and Flink's landing files into bronze `raw_*` Delta tables, unchanged, then validates them. The export lands on a lakeFS branch and is merged into `main` only after its checks pass (novel idea 2), so a broken export never reaches readers | [spark_jobs.md](docs/data-platform/spark_jobs.md), [novel_lakefs_wap.md](docs/data-platform/novel_lakefs_wap.md) |
| DP2, data pipeline: bronze to silver and gold | Silver: removes duplicates, maps the 63 old province names to the 34 units, fills `auth_method` for rows before 2024-07-01 (join skew: salting, AQE and broadcast compared in [`pipelines/spark/skew_join.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/spark/skew_join.py)). Gold: dbt-spark builds the `dim_*` / `fact_*` star schema with the SCD2 `dim_customer` and an incremental fact, checked by 22 dbt tests; Spark writes the fraud labels as they were known on each day. Each zone is validated before the next step reads it | [spark_jobs.md](docs/data-platform/spark_jobs.md), [dbt_gold.md](docs/data-platform/dbt_gold.md) |
| DP3, feature pipeline: gold to features | `feat_*` tables of per-card statistics over 30, 90 and 180 days, with `event_timestamp` and `created`, which make them the Feast offline store for Layer 2 | [spark_jobs.md](docs/data-platform/spark_jobs.md#dp3-features) |
| Streaming jobs (Flink) | Read the payments from Kafka with watermarks, late events, dedup and windows; write the raw events as `landing/` JSON files (DP1 loads them into bronze) and the realtime features to `card_features_rt`; find account takeover patterns in the app events with `MATCH_RECOGNIZE` (novel idea 1) | [flink_jobs.md](docs/data-platform/flink_jobs.md), [novel_flink_cep.md](docs/data-platform/novel_flink_cep.md) |
| Push jobs | Two always-on containers: `push-online` writes the realtime features into Redis for scoring, `push-offline` appends them to Delta for training ([`ml/jobs/push_job.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/ml/jobs/push_job.py)). Jenkins tests, builds and redeploys them | [feature_store.md](docs/ml-platform/feature_store.md), [cicd.md](docs/operations/cicd.md) |
| Trino | SQL over every zone (`delta.<zone>.<table>`, the export as `lakefs.bronze.raw_transactions`): it finds a table's location in the Hive Metastore and reads its Delta files from S3 | [schema_design.md](docs/data-platform/schema_design.md) |
| Airflow | Runs the pipelines below; connections and variables live in Airflow | [airflow.md](docs/data-platform/airflow.md) |
| DataHub | The tables of every zone, the lineage every Spark job sends through OpenLineage (which pipeline wrote which table), and the validation results as assertions | [datahub.md](docs/data-platform/datahub.md), [openlineage.md](docs/data-platform/openlineage.md) |

### Pipelines on Airflow

Each pipeline is one Airflow DAG (DP2 is two, one per zone it writes) and has its own validate stage (Great
Expectations), so bad data stops where it is found. The batch pipelines pass data only through the lakehouse, as in
the overview. No pipeline starts another one. DP1 is the only one on a clock. Each pipeline ends with a publish
task that emits an Airflow dataset event for the zone it wrote (`lakehouse://bronze`, `silver`, `gold`,
`features`), with the business day on it, and the pipeline that reads that zone is scheduled on that dataset. So a
pipeline depends on the table it reads and does not know which pipeline writes it. The publish task runs only after
the checks pass. Each pipeline can also be started by hand at any time: a reader of a Delta table always gets the
last committed version, never a half-written one.

| Pipeline | Airflow DAG | Runs | Stages | Reads | Writes |
|---|---|---|---|---|---|
| DP1, ingest to bronze | `dp1_source_to_bronze` | daily; waits for the day's export folder | create a lakeFS branch, wait for the export; copy the core banking tables, the export (on the branch) and the landing files; commit; validate; merge the branch into `main`; publish bronze | Postgres `core_banking`, `s3://core-banking-export`, Flink's landing files | bronze `raw_*` |
| DP2, bronze to silver | `dp2_bronze_to_silver` | when bronze is published | build silver; validate silver; publish silver | bronze | silver `stg_*` |
| DP2, silver to gold | `dp2_silver_to_gold` | when silver is published | dbt build of gold (and its docs) next to the labels job; validate gold; publish gold | silver, bronze reference tables | gold `dim_*` / `fact_*`, `gold.label_transaction` |
| DP3, gold to features | `dp3_gold_to_features` | when gold is published | build the features; validate them; publish features | gold | `gold.feat_card_txn_stats_daily` |
| Incremental materialize | `feast_materialize` | when the features are published | copy the rows newer than the last run into Redis; read a sample back to check it | the feature table (Feast offline store) | Redis (Feast online store) |
| Data drift | `drift_monitoring` | daily, on its own schedule | compute drift and accuracy on the last 7 labelled days; decide; start a training pipeline run, or do nothing | payments, labels, point-in-time features, the `production` model | an MLflow run; a Kubeflow training run on drift |
| RAG data pipeline (Layer 3) | `rag_ingest` | after each new image (Jenkins), or by hand | chunk the documents; embed the chunks; write them to the feature store; validate | `s3://bank-documents` | Feast `card_support_kb` (pgvector online, Parquet offline) |
| DataHub metadata | `datahub_metadata` | daily | ingest the source tables, the Kafka topics and the dbt models | Postgres, Kafka, dbt artifacts | DataHub |

### Layer 2: ML platform

| Part | What it does | Docs |
|---|---|---|
| Feast feature store ([`ml/feature_repo/`](https://github.com/cvinhnguyen/card-fraud-platform/tree/822721cab1ac597d118f100f1c49949f137f8a7d/ml/feature_repo)) | One definition of the model's 20 features, used twice: the offline store (the lakehouse Delta tables, read with Spark and point-in-time joins) builds training sets, the online store (Redis) answers the Scoring API. Daily features arrive through the materialize pipeline, realtime and device features through the push jobs | [feature_store.md](docs/ml-platform/feature_store.md) |
| Notebook | Explores the data and trains the first model step by step, calling the same seven step functions as the training pipeline, and ends with both models saved and an MLflow run ([`ml/notebooks/fraud_model.ipynb`](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/ml/notebooks/fraud_model.ipynb)) | [ml_training.md](docs/ml-platform/ml_training.md) |
| Training pipeline (Kubeflow Pipelines on kind) | `train_fraud_model`, one pod per step: (1) load the labels as of a date, (2) build the training set with a Feast point-in-time join and save it as a new Delta version, so the model can be traced back to its exact rows, (3) validate the data, (4) split by time, (5) train a logistic-regression baseline and a distributed XGBoost on Spark executor pods, (6) evaluate against the baseline and the `production` model, (7) register the model in MLflow as a new version, tagged `passed` True or False; only a model that passed gets the alias `challenger` | [ml_training.md](docs/ml-platform/ml_training.md) |
| MLflow | Experiment runs with their data versions; the model registry, where alias `challenger` marks a new candidate and `production` the model being served | [ml_training.md](docs/ml-platform/ml_training.md#evaluation-and-promotion) |
| KServe | InferenceService (Standard mode) serving the `production` model baked into the image | [serving.md](docs/ml-platform/serving.md) |
| Fraud Scoring API | FastAPI: the payment's online features from Feast and the KServe score give approve, step_up (ask for an OTP or biometric check) or decline, with a rule fallback when the model is late or down | [serving.md](docs/ml-platform/serving.md) |
| Drift monitoring | KServe's payload logger sends every request and response of the model through Knative Eventing to the Drift API, which counts them and returns the PSI of each feature against the training profile; the data drift pipeline checks accuracy on labelled payments daily and starts a training pipeline run on drift | [drift.md](docs/ml-platform/drift.md) |

Five classes carry the ML logic:

| Class | File | Job |
|---|---|---|
| `TrainingDataService` | [`ml/fraud_ml/data.py` L38](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/ml/fraud_ml/data.py#L55) | Labels as of a date, Feast point-in-time join, versioned training set (Delta) |
| `FraudModelTrainer` | [`ml/fraud_ml/train.py` L27](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/ml/fraud_ml/train.py#L27) | Logistic-regression baseline + distributed `SparkXGBClassifier` |
| `ModelEvaluator` | [`ml/fraud_ml/evaluate.py` L30](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/ml/fraud_ml/evaluate.py#L30) | Thresholds on validation, test metrics, gate vs baseline and `production` |
| `ModelRegistry` | [`ml/fraud_ml/registry.py` L24](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/ml/fraud_ml/registry.py#L26) | MLflow logging, model versions and aliases |
| `FraudScorer` | [`services/scoring_api/app/scorer.py` L24](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/services/scoring_api/app/scorer.py#L24) | Request -> features -> score -> decision, fallback when KServe is down |

### Layer 3: LLM and agent platform

Support staff ask the agents in a chat. The coordinator agent splits the question and asks two specialist agents,
which answer with tools: a card's features, the fraud model's verdict and a search of the bank's documents. Every
call between them passes one gateway that decides what each agent may do.

| Part | What it does | Docs |
|---|---|---|
| RAG data pipeline | Airflow `rag_ingest`: the bank documents become 57 chunks, each with a 1024-number vector, stored in Feast `card_support_kb` (pgvector online, Parquet offline); 7 checks and lineage go to DataHub | [rag_pipeline.md](docs/llm-platform/rag_pipeline.md) |
| llama.cpp | Model servers on the host GPU (GTX 1070): Qwen3-8B Q4_K_M for chat and Qwen3-Embedding-0.6B for vectors | [llm_platform.md](docs/llm-platform/llm_platform.md) |
| llm-d | The InferencePool `qwen3-8b` and its endpoint picker, which chooses a model server for each prompt | [llm_platform.md](docs/llm-platform/llm_platform.md) |
| Support chat | The page the support staff open (`support.<domain>`, behind NGINX). It sends each question to the gateway route `/a2a/coordinator` and adds its API key on the server side, so the gateway is the first stop of a question, before any agent | [agents.md](docs/llm-platform/agents.md) |
| agentgateway | The one entry for every agent call: the support chat's question (`/a2a/coordinator`), LLM prompts (`/v1`), tool calls (MCP, `/mcp`) and agent-to-agent calls (A2A). Each caller sends its own API key; the gateway uses it to decide which routes and tools that agent may call, limits its requests and tokens per minute, and masks card numbers, emails, phone numbers and citizen IDs before a prompt reaches the model | [llm_platform.md](docs/llm-platform/llm_platform.md), [agents.md](docs/llm-platform/agents.md) |
| card-data-mcp, fraud-score-mcp, policy-rag-mcp | The agents' tools: three MCP servers from one FastAPI code base (pydantic, `/healthz` + `/readyz` probes, async), one Helm release each. card-data returns a card's features and a policy chunk by id (the Web API), fraud-score asks the Scoring API, policy-rag searches the policies | [agents.md](docs/llm-platform/agents.md) |
| NGINX Ingress | Every UI and the Web API on `*.cvinhnguyen.duckdns.org`: Let's Encrypt HTTPS, basic auth, rate limits | [edge_security.md](docs/operations/edge_security.md) |
| Coordinator agent | Receives the support staff's question from the gateway and asks the two specialist agents over A2A | [agents.md](docs/llm-platform/agents.md) |
| Card-data and risk agents | kagent agents that call their MCP tools through the gateway; 2 to 4 replicas each, scaled by KEDA | [agents.md](docs/llm-platform/agents.md) |
| Agent Substrate | Sandboxed copies of the agents: one gVisor sandbox per conversation, restored from a warm snapshot on a shared worker pool | [agents.md](docs/llm-platform/agents.md#sandboxed-agents-on-agent-substrate-warm-up) |
| agentregistry | Catalog of the agents and the three MCP servers they use, for people; not on the call path | [agents.md](docs/llm-platform/agents.md#agent-registry) |
| Operations | Prometheus + Grafana (metrics), VictoriaLogs (logs), Jaeger (traces), Vault (secrets) | [observability.md](docs/operations/observability.md), [edge_security.md](docs/operations/edge_security.md) |
| CI/CD | Jenkins, five pipelines: the MCP servers, the data agents, the coordinator, the RAG data pipeline and the push jobs. Agent prompts come from Langfuse; images are signed (Layer 3 novel idea 2); each agent release passes an eval gate (Layer 3 novel idea 1) | [cicd.md](docs/operations/cicd.md) |

### System diagram, Layers 1 and 2

The overview shows what the platform does; this diagram shows the deployable units and the data between them. Every
box is a compose container or a workload on the kind cluster (Feast is a library inside them, so it has no box).
Arrows follow the data and carry it as their label; a call without data (B1, M7) points from caller to callee. Each
flow has its own colour, letter and numbering:

![System diagram, Layers 1 and 2: deployable units and the numbered data flows between them](docs/images/platform-system-layers-1-2.png)

| Flow | Who / when | Path |
|---|---|---|
| **S** Payment scoring | end user, every authorization | card network -> Scoring API (features from Redis, score from KServe) -> decision; every decision logged to Kafka |
| **R** Streaming features | continuous | Kafka -> Flink -> Kafka -> push jobs -> Redis (online) and Delta (offline) |
| **B** Batch data | daily, Airflow | Postgres + export files -> DP1 (bronze) -> DP2 silver pipeline (silver) -> DP2 gold pipeline (gold) -> DP3 (feat_*) -> materialize pipeline -> Redis; each one starts on the dataset event of the zone it reads |
| **T** Training and deploy | ML engineer | lakehouse -> notebook / training pipeline -> MLflow -> model image -> KServe |
| **M** Monitoring and retraining | live + daily | KServe payload logger -> Knative Broker -> Drift API (PSI); data drift pipeline (PR-AUC, PSI) -> MLflow -> training pipeline run |

Every numbered arrow, one line each: [docs/architecture.md](docs/architecture.md#numbered-arrows).

### System diagram, Layer 3

Support staff reach the agents through NGINX over HTTPS. Every agent call (A2A, MCP, LLM) then passes agentgateway,
which checks the agent's API key, limits its requests and masks personal data before the llm-d router hands LLM
calls to llama.cpp on the host GPU. The card-data MCP server takes a card's features from the same Feast online
store as Layer 2, the fraud-score server asks the Scoring API, and the policy-rag server searches the policy chunks
and vectors that the RAG data pipeline writes into a second Feast project (pgvector):

![System diagram, Layer 3: agents, gateway, model servers, RAG data pipeline, CI/CD and observability, numbered per flow](docs/images/platform-system-layer-3.png)

| Flow | Who / when | Path |
|---|---|---|
| **A** Agent conversation | support staff, every question | NGINX -> support chat -> gateway -> coordinator -> gateway -> card-data / risk agents -> gateway -> MCP servers card-data (Redis, pgvector), fraud-score (Scoring API), policy-rag (embeddings, pgvector); LLM calls -> gateway -> llm-d -> llama.cpp |
| **K** Knowledge ingest | RAG data pipeline `rag_ingest`, per release | bank documents (S3) -> chunks -> embeddings -> pgvector online + Parquet offline -> DataHub checks and lineage |
| **C** CI/CD | developer push | GitHub -> Jenkins (prompts from Langfuse) -> signed image in the registry -> helm --atomic -> eval gate through the gateway -> Langfuse experiment |
| **O** Observability and scaling | continuous | gateway + MCP metrics -> Prometheus -> Grafana and KEDA (agent replicas 2-4); agent spans -> Jaeger |

Every Layer 3 arrow, one line each: [docs/architecture.md](docs/architecture.md#numbered-arrows-layer-3).

### Deployment

Where each part runs: the host with its tools and the two model servers, the compose profiles in the Docker Desktop
VM, the namespaces of the kind cluster, the Azure resources of an IaC run, and the SaaS used over the internet:

![Deployment: host tools and model servers, compose profiles in the Docker Desktop VM, the kind cluster's namespaces, the Azure resources of the IaC runs, and the SaaS on the internet](docs/images/platform-deployment.png)

| Where | What runs there | Docs |
|---|---|---|
| The Windows PC (16 GB RAM, GTX 1070) | all of Layers 1-3. The compose profiles start what a phase needs; the kind cluster (joined to the compose network) holds Kubeflow Pipelines and KServe for Layer 2 and the whole Layer 3 platform, one namespace per part. The two llama.cpp model servers run on the host next to the GPU, which the cluster reaches through `host.docker.internal` | this README |
| Azure (`rg-cardfraud-dev`, Poland Central) | during an IaC run only: Terraform creates the network, a VM, AKS and a container registry; Ansible deploys the fraud model server and node_exporter to the VM; AKS runs the same model image from the registry. `terraform destroy` removes all of it | [iac.md](docs/operations/iac.md) |
| SaaS | GitHub (Jenkins polls it), Langfuse Cloud EU (agent prompts, eval experiments), Groq (side B of the LLM A/B test), Let's Encrypt with DuckDNS (the HTTPS certificate) | [cicd.md](docs/operations/cicd.md), [ab_testing.md](docs/llm-platform/ab_testing.md), [edge_security.md](docs/operations/edge_security.md) |

The rest of the platform is containers and Helm charts that could run on Azure too. The Azure for Students
subscription keeps it on the PC: 6 vCPU and 3 public IPs per region, a quota of 0 for the T4, A10, A100 and H100 GPU
families, and $100 of credit, which a 16 GB VM would use up in about 18 days
([what would move where, and the numbers](docs/operations/iac.md#why-not-all-of-it-on-azure)). Diagram sources:
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
  jobs/               push jobs, drift job, rag_ingest.py (RAG steps), Jenkinsfile.stream
  tools/              run by hand: replay_scoring, check_online, drift_by_day, stream_smoke
  notebooks/          fraud_model.ipynb, executed with outputs (models/ = the saved models, gitignored)
services/             scoring_api (FastAPI), drift_api (FastAPI), predictor (KServe custom model)
  card_support_mcp/   Layer 3: the three MCP servers (one code base, MCP_SERVER picks one): app, tests, Helm chart + values per server (gateway route, tool access), Jenkinsfile
agents/               Layer 3 agents: Helm chart (agents, A2A routes, KEDA, sandbox, network policy), prompts, two Jenkinsfiles,
                      eval/ (dataset + Langfuse eval gate), registry/ (agentregistry catalog), locustfile
generator/            synthetic source systems: data with injected problems, Kafka producer, checks; knowledge/ = the card documents
infra/                docker-compose (profiles), secrets bootstrap (make_env.py, .env.example), bucket/topic init
  spark/ flink/ airflow/ hive/ ml/ mlflow/   the images built here (multi-stage, see docs/performance/docker_optimization.md)
  k8s/                kind cluster, KFP + KServe, the Layer 3 platform (llm-platform.sh, agentgateway.yaml, llm-d-router.yaml),
                      observability, edge/ (NGINX routes), vault.sh + secrets.sh, tls.sh, supply-chain/ (cosign + Kyverno), dashboards/,
                      aks/ (the model server on Azure AKS)
  jenkins/            Jenkins image, configuration as code (credentials, the 5 jobs), deploy ServiceAccount
  terraform/          Azure: modules network / vm / aks / acr, envs/dev
  ansible/            roles that configure the VM and deploy the model server on it
  benchmark/          inference-perf load test of the model server, results
  conf/               service configs (Spark, Hive, Trino, SeaweedFS, DataHub, notebook)
  sql/                Postgres serving SQL (indexes + EXPLAIN)
docs/                 architecture.md, then one folder per area (data-platform/, performance/, ml-platform/,
                      llm-platform/, operations/); images/ = screenshots and diagrams; diagrams/ = the scripts that draw
                      them (platform.py: the 5 above, logos.json: the logos they use)
```

## Quick start

From the repo root, with Docker and Python 3. The commands use the Windows venv path `.venv/Scripts`; on macOS or
Linux it is `.venv/bin`:

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt
.venv/Scripts/python infra/make_env.py                        # once: random local secrets (gitignored)
docker compose -f infra/docker-compose.yaml --profile core up -d
.venv/Scripts/python infra/init_infra.py                      # buckets + Kafka topics
.venv/Scripts/python generator/generate.py                    # source data -> data/source
.venv/Scripts/python generator/check_data.py                  # assert every injected problem
.venv/Scripts/python generator/load_source.py                 # Postgres core_banking (127.0.0.1:5433) + s3://core-banking-export
.venv/Scripts/python generator/producer.py --speed 200            # replay card_authorizations into Kafka (--topic app_events, --replay-from, --limit)

# Batch stack: Spark + Hive Metastore + Trino, then the DP1, DP2 and DP3 jobs by hand
docker compose -f infra/docker-compose.yaml --profile core --profile batch up -d --build
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/ingest_bronze.py --txn-root s3a://lakehouse/bronze
.venv/Scripts/python infra/init_lakefs.py                     # the export table moves under lakeFS (zero-copy); later loads: DP1 branches
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_silver.py
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_labels.py
docker exec -it trino trino --execute "SELECT count(*) FROM lakefs.bronze.raw_transactions"

# Gold with dbt-spark through the Spark Thrift Server, then the DP3 features
docker compose -f infra/docker-compose.yaml --profile dbt up -d spark-thrift
.venv/Scripts/dbt build --project-dir pipelines/dbt --profiles-dir pipelines/dbt
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_features.py   # DP3, reads gold

# Streaming: Flink session cluster (UI http://127.0.0.1:8088), each check replays a slice and verifies the job
docker compose -f infra/docker-compose.yaml --profile core --profile stream up -d --build
.venv/Scripts/python pipelines/flink/check_windows.py --replay-from "2026-06-01 00:00" --limit 20000

# Pipelines on Airflow: DP1 runs daily; DP2, DP3 and materialize run on the dataset events of the zones they read
# (unpause them first, see docs/data-platform/airflow.md); DataHub receives lineage and validation results
docker compose -f infra/docker-compose.yaml --profile core --profile batch --profile orch --profile gov up -d --build
docker exec airflow-scheduler airflow dags backfill dp1_source_to_bronze -s 2026-10-12 -e 2026-10-15   # DP2/DP3 follow on the events

# Layer 2: Feast, MLflow, notebook, APIs (profile ml); KFP + KServe on kind
docker build -t fraud/ml:0.1 infra/ml                              # dependencies image, on top of fraud/spark:4.1.3 (batch profile build)
docker build -f ml/Dockerfile -t fraud/ml-train:latest .           # code image: notebook, push jobs, KFP steps, RAG steps
docker compose -f infra/docker-compose.yaml --profile core --profile ml up -d
docker compose -f infra/docker-compose.yaml exec notebook feast -c /opt/ml/feature_repo apply   # order + bootstrap: docs/ml-platform/feature_store.md
bash infra/k8s/up.sh && bash infra/k8s/platform.sh                 # kind cluster, KFP 2.17, KServe 0.21
python ml/pipelines/train_fraud_model.py --run --label-as-of 2026-10-15                # see docs/ml-platform/ml_training.md
```

### Layer 3

The model servers run on the host first (llama.cpp start commands in [docs/llm-platform/llm_platform.md](docs/llm-platform/llm_platform.md#start-the-model-servers)),
then the cluster scripts in this order; each is idempotent and reads its secrets from `infra/.env`.

```bash
sh infra/k8s/vault.sh                  # Vault + External Secrets; secrets.sh writes every secret into Vault
sh infra/k8s/llm-platform.sh           # Gateway API, agentgateway, llm-d router, kagent, Agent Substrate, KEDA, agentregistry
sh infra/k8s/observability.sh          # Prometheus + Grafana (dashboards), VictoriaLogs, Jaeger
sh infra/k8s/tls.sh && sh infra/k8s/edge.sh   # HTTPS certificate (DuckDNS, Let's Encrypt), NGINX Ingress + routes
.venv/Scripts/python generator/load_source.py --part documents && docker exec airflow-scheduler airflow dags trigger rag_ingest
sh infra/jenkins/setup.sh              # Jenkins (profile cicd): its 5 pipelines build, test and deploy the MCP servers and the agents
arctl apply -f agents/registry/catalog.yaml   # the registry catalog (kubectl -n agentregistry port-forward svc/agentregistry 12121:12121)
```

By hand instead of Jenkins: `docker build --target runtime -t localhost:5001/card-support-mcp:<tag> services/card_support_mcp`,
push it, then for each server `s` in card-data, fraud-score, policy-rag:
`helm upgrade --install $s-mcp services/card_support_mcp/chart -f services/card_support_mcp/chart/values-$s.yaml -n card-support --atomic --set image.tag=<tag>`,
then `helm upgrade --install card-support-data-agents agents/chart -n kagent -f agents/chart/values-data-agents.yaml` and
the same with `card-support-coordinator` / `values-coordinator.yaml`.

UIs: Spark master http://127.0.0.1:8080, running job http://127.0.0.1:4040, finished jobs http://127.0.0.1:18080, Trino http://127.0.0.1:8090, lakeFS http://127.0.0.1:8001, Flink http://127.0.0.1:8088, Airflow http://127.0.0.1:8082 (user `admin`), DataHub http://127.0.0.1:9002 (user `datahub`), MLflow http://127.0.0.1:5000, Jupyter http://127.0.0.1:8899 (token `JUPYTER_TOKEN`), KFP UI http://127.0.0.1:31880 (port-forward, `infra/k8s/platform.sh`), Scoring API http://127.0.0.1:8000/docs, Drift API http://127.0.0.1:8002/docs, Jenkins http://127.0.0.1:8095 (user `admin`); Layer 3 behind NGINX: https://agents, registry, grafana, logs, traces and api `.cvinhnguyen.duckdns.org` (user `admin`, `EDGE_ADMIN_PASSWORD`; Grafana has its own login); passwords are in `infra/.env`. The 8 GB Docker VM does not hold every profile at once: run `stream` on its own, keep `trino` and the Thrift Server (`dbt` profile) off while `orch` + `gov` run, and stop the kind cluster while Airflow and DataHub run for the RAG pipeline. From Git Bash, prefix `docker compose exec` with `MSYS_NO_PATHCONV=1` so `/opt/...` paths are not rewritten.

Data volume is set in `generator/config.yaml` (`volume`, ~1 GB: about 7 min to generate, `check_data.py` needs ~4.3 GB RAM). For a quick smoke test: `generate.py --scale 0.05`. What the data contains and the measured problems: [docs/data-platform/data_characteristics.md](docs/data-platform/data_characteristics.md).

## Docs

Start with the [architecture](docs/architecture.md): every diagram explained, arrow by arrow. The other docs are
grouped by area; each one opens with what that part does and why the bank needs it, then how it works, how to run it
and what was measured.

**Layer 1, data platform** ([docs/data-platform/](https://github.com/cvinhnguyen/card-fraud-platform/tree/822721cab1ac597d118f100f1c49949f137f8a7d/docs/data-platform))

| Doc | What it tells you |
|---|---|
| [Data characteristics](docs/data-platform/data_characteristics.md) | what the synthetic data contains and which real-world problems were put in, at what measured rate |
| [Schema design](docs/data-platform/schema_design.md) | the lakehouse zones, every table's grain, which job writes and reads it, ER diagrams of bronze, silver and gold |
| [Airflow](docs/data-platform/airflow.md) | the pipelines on Airflow, what starts each one, their check tasks, backfill |
| [Spark jobs](docs/data-platform/spark_jobs.md) | what DP1, DP2 and DP3 do to the data, job by job: dedup, province mapping, labels, features |
| [dbt-spark gold](docs/data-platform/dbt_gold.md) | the gold star schema built with dbt, compared row by row with the same layer written in PySpark |
| [Flink jobs](docs/data-platform/flink_jobs.md) | the streaming jobs: late events, duplicates, live features, the hot key, each checked against the truth |
| [DataHub](docs/data-platform/datahub.md) | the data catalog: every table, where it came from, its check results |
| [OpenLineage](docs/data-platform/openlineage.md) | how every Spark job reports what it read and wrote, without lineage code |
| Novel idea 1: [Flink CEP](docs/data-platform/novel_flink_cep.md) | account takeover patterns found in seconds with `MATCH_RECOGNIZE` |
| Novel idea 2: [Write-Audit-Publish](docs/data-platform/novel_lakefs_wap.md) | the nightly export is checked on a lakeFS branch, so a broken export never reaches readers |

**Performance** ([docs/performance/](https://github.com/cvinhnguyen/card-fraud-platform/tree/822721cab1ac597d118f100f1c49949f137f8a7d/docs/performance))

| Doc | What it tells you |
|---|---|
| [Spark optimization](docs/performance/spark_optimization.md) | join skew, schema evolution and duplicates: baseline, fix, measured gain |
| [Storage optimization](docs/performance/storage_optimization.md) | small files, partitioning, Z-order on Delta; indexes on the Postgres serving table |
| [Docker optimization](docs/performance/docker_optimization.md) | smaller images that start faster, and the jars baked in |
| [The 100 GB run](docs/performance/scale_100g.md) | what breaks at bank scale and how the jobs were changed to finish |

**Layer 2, ML platform** ([docs/ml-platform/](https://github.com/cvinhnguyen/card-fraud-platform/tree/822721cab1ac597d118f100f1c49949f137f8a7d/docs/ml-platform))

| Doc | What it tells you |
|---|---|
| [Feature store](docs/ml-platform/feature_store.md) | one set of features for training and scoring: Feast, its offline and online stores, the push jobs |
| [ML training](docs/ml-platform/ml_training.md) | the notebook and the training pipeline's seven tasks, evaluation, promotion in MLflow |
| [Serving](docs/ml-platform/serving.md) | how a payment gets its decision in milliseconds: Scoring API, KServe, thresholds, fallback |
| [Drift](docs/ml-platform/drift.md) | how the platform sees the model getting worse, and when it retrains |

**Layer 3, LLM and agent platform** ([docs/llm-platform/](https://github.com/cvinhnguyen/card-fraud-platform/tree/822721cab1ac597d118f100f1c49949f137f8a7d/docs/llm-platform))

| Doc | What it tells you |
|---|---|
| [LLM platform](docs/llm-platform/llm_platform.md) | the model servers, the router and the gateway's LLM route (keys, limits, PII masking) |
| [LLM benchmark](docs/llm-platform/llm_benchmark.md) | how fast the model server answers, and the configuration kept |
| [Agents](docs/llm-platform/agents.md) | the agents and their tools, one question followed end to end, who may call what |
| [RAG pipeline](docs/llm-platform/rag_pipeline.md) | how the bank's documents become searchable text for the agents |
| [A/B testing](docs/llm-platform/ab_testing.md) | how a new model or model config is tried on part of the traffic first |
| Novel idea 1: [Eval gate](docs/llm-platform/novel_eval_gate.md) | every agent release answers a test set; below 0.6 accuracy it rolls back |

**Operations** ([docs/operations/](https://github.com/cvinhnguyen/card-fraud-platform/tree/822721cab1ac597d118f100f1c49949f137f8a7d/docs/operations))

| Doc | What it tells you |
|---|---|
| [CI/CD](docs/operations/cicd.md) | the five Jenkins pipelines: what each tests, builds and deploys, and what happens on a bad release |
| Novel idea 2: [Only signed images run](docs/operations/novel_supply_chain.md) | images are signed in CI and the cluster refuses unsigned ones |
| [Testing](docs/operations/testing.md) | the tests of the APIs and MCP servers: kinds, coverage, mutation, load |
| [Edge and secrets](docs/operations/edge_security.md) | HTTPS, login and rate limits in front of every UI; secrets in Vault |
| [Observability](docs/operations/observability.md) | the dashboards, logs and traces, and what each one shows |
| [Infrastructure as code](docs/operations/iac.md) | Terraform and Ansible on Azure, and why the rest stays on the PC |
