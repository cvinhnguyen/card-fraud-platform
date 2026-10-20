# Architecture

Three views of the same platform, all drawn by `docs/diagrams/platform.py` from the tables in that script (stdlib only).
Redraw with `python docs/diagrams/platform.py`: it writes `docs/images/platform-*.svg`, renders the PNGs with headless
Chrome or Edge, prints the arrow table below, and warns when an arrow crosses a box or a label overlaps one.

| View | Question it answers | Boxes are |
|---|---|---|
| [Overview](#overview) | What does the platform do, step by step? | the 15 steps of the README tables |
| [System diagram](#system-diagram) | Which unit sends which data to which unit? | deployable units |
| [Deployment](#deployment) | Where does each unit run, on which port? | containers and Kubernetes workloads |

## Overview

![Platform overview](images/platform-overview.png)

Two bands and a bar between them. Layer 1 (top) turns the source systems into feature tables, daily with Spark and
continuously with Flink. Layer 2 (bottom) trains, registers, serves and monitors the model. Feast (the bar) is the
hand-over: Layer 1 writes into it, Layer 2 reads from it, so training and serving see the same 20 features
(`fraud_model_v1`). The loop at the bottom is the retraining path: the drift check starts a pipeline run with the
newest labels. Airflow, the lakehouse and DataHub run under everything.

## System diagram

![System architecture](images/platform-architecture.png)

The rules of the README diagram, and how the picture follows them:

| Rule | In the picture |
|---|---|
| Every box is a deployable unit | each box is one compose service or one Kubernetes workload; the tag says where (compose profile or namespace). Feast is a library inside Scoring API, push jobs, Airflow, Jupyter and the KFP steps, so it has no box; its stores (Redis, SeaweedFS) and registry (ml-db) do |
| Arrows follow the data, the data on the arrow | Redis -> Scoring API (S2), not the other way, because features flow to the API; a call without data (B1 spark-submit, M7 retraining run) points from caller to callee |
| Numbered, one numbering per user flow | five flows, each with its own colour, letter and 1..n: S for the end user's payment, T for the ML engineer, R / B / M for the system's own flows |
| Few dashed lines, only off the main path | no dashed arrows; the three dashed boxes are support services used by many units (Hive Metastore, ml-db, DataHub), whose arrows would cross every flow |

Colours: the five flow colours are at least 17 apart (OKLab x 100) for normal vision; for colour-blind readers the
letter in each badge carries the flow. One crossing is left (M7 over T2), drawn with a gap.

### Numbered arrows

| # | Flow | Data / call |
|---|---|---|
| S1 | Payment scoring | The switch sends each authorization to the API (card, amount, channel, device) |
| S2 | Payment scoring | Feast reads the card's stored features from Redis (feature service fraud_model_v1) |
| S3 | Payment scoring | Feature vector to the model server (KServe V1 protocol, NodePort 31080) |
| S4 | Payment scoring | Probability of fraud from the production XGBoost booster |
| S5 | Payment scoring | Decision by the two thresholds; rule fallback when the model is late or down |
| S6 | Payment scoring | Every decision is logged to Kafka topic fraud_predictions (scoring never waits on monitoring) |
| R1 | Streaming features | Every authorization (retries included) to Kafka topic card_authorizations |
| R2 | Streaming features | Flink reads card_authorizations (watermarks, dedup of network retries) |
| R3 | Streaming features | Realtime features to card_features_rt, first (card, device) pairs to card_device_seen |
| R4 | Streaming features | raw_landing writes the raw authorizations to bronze.raw_card_authorizations (Delta) |
| R5 | Streaming features | push job A consumes card_features_rt and card_device_seen |
| R6 | Streaming features | Writes them to the Redis online store through Feast push (offsets committed after the write) |
| R7 | Streaming features | push job B consumes the same two topics |
| R8 | Streaming features | Appends them to gold.feat_card_auth_rt / gold.feat_card_device: the offline copy for training |
| B1 | Batch data | Airflow runs DP1 -> DP2 -> DP3 on the Spark cluster (call, no data); dbt and GE validate stages |
| B2 | Batch data | DP1 copies the core banking tables (customers, accounts, cards, merchants, disputes) |
| B3 | Batch data | DP1 loads the new daily export folders from s3://core-banking-export |
| B4 | Batch data | Delta tables bronze -> silver -> gold (dbt) -> feat_* in s3://lakehouse |
| B5 | Batch data | Trino reads the gold and feature tables for analysts (catalog: Hive Metastore) |
| B6 | Batch data | DAG feast_materialize (after DP3) reads feat_card_txn_stats_daily through the Feast offline store |
| B7 | Batch data | ... and loads the newest row per card into Redis (feast materialize-incremental) |
| T1 | Training and deploy | The notebook builds the training set: labels + Feast point-in-time features |
| T2 | Training and deploy | Metrics, plots and both models of the notebook run |
| T3 | Training and deploy | Pipeline steps 1-2 read the labels as of label_as_of and the point-in-time features |
| T4 | Training and deploy | Step 2 MERGEs the run's rows into ml/training_set_fraud_model_v1 (one Delta version per run) |
| T5 | Training and deploy | Steps 5-7 log params, data versions, metrics and the booster; alias challenger when the gate passes |
| T6 | Training and deploy | ML engineer: fetch_model.py --alias production, docker build + push of fraud/predictor:<v> |
| T7 | Training and deploy | kubectl apply of the InferenceService; the node pulls the image from the registry |
| M1 | Monitoring and retraining | The forwarder consumes the prediction log |
| M2 | Monitoring and retraining | Batches to POST /v1/events |
| M3 | Monitoring and retraining | Bin counts per feature and minute in Redis db 1; GET /v1/drift returns the PSI vs the training profile |
| M4 | Monitoring and retraining | DAG drift_monitoring: last 7 days of payments, ground truth, point-in-time features |
| M5 | Monitoring and retraining | Loads the production model to score them |
| M6 | Monitoring and retraining | Logs PR-AUC, recall and PSI as a run of experiment card_fraud_monitoring |
| M7 | Monitoring and retraining | On drift: a KFP run of train_fraud_model with label_as_of = the DAG's day (call, no data) |

## Deployment

![Deployment](images/platform-deployment.png)

| Decision | Why |
|---|---|
| One Docker Desktop VM, seven compose profiles | each phase starts only what it needs; the 8 GB VM cannot hold every profile at once (run `stream` alone, keep `trino` and `dbt` off while `orch` + `gov` run) |
| kind only for Kubeflow Pipelines and KServe | the two pieces that need Kubernetes; APIs, push jobs, MLflow and Redis stay in compose |
| kind node joined to `fraud_default` | pods reach the compose services by name (`lakehouse-s3:8333`, `kafka:19092`, `ml-db`, `redis`, `mlflow:5000`), and compose reaches the cluster through NodePorts (31080 KServe, 31888 KFP API) |
| Local registry `registry:3` | the host pushes to `localhost:5001`; containerd on the node maps that name to `registry:5000` on the compose network |
| Alias `lakehouse-s3` for SeaweedFS | KFP installs its own `seaweedfs` Service, which shadows the short name inside the cluster |
| Ports published on 127.0.0.1 only | nothing is reachable from the network; secrets stay in `infra/.env` (gitignored) |
