# DataHub: tables, lineage, validation

When a number in a report looks wrong, the first questions are: which table did it come from, which job wrote that
table, from which sources, and did that day's checks pass? DataHub answers them for this platform (data governance in
the course's terms). DataHub v1.5.0.6, the course version, is the platform's metadata catalog: the lakehouse tables of
every zone, their lineage from the core banking sources to the features, the Airflow pipelines that produce them and
the result of every validation run, in one place.

| | |
|---|---|
| Code | [`infra/docker-compose.yaml` L457-L566](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/infra/docker-compose.yaml#L457-L566), [`pipelines/airflow/dags/datahub_metadata.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/datahub_metadata.py), [`pipelines/airflow/dags/datahub_recipes/`](https://github.com/cvinhnguyen/card-fraud-platform/tree/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/datahub_recipes) |
| Runs on | compose profile `gov`: GMS (the metadata service) on 127.0.0.1:8083, UI on http://127.0.0.1:9002 |
| Reads | OpenLineage events of every Spark job, the Airflow listener, Great Expectations (GE) results, `datahub ingest` of Postgres `core_banking`, the Kafka topics and the dbt artifacts |
| Writes | its own Postgres and OpenSearch |
| Related | [openlineage.md](openlineage.md), [airflow.md](airflow.md), [dbt_gold.md](dbt_gold.md) |

## Where it fits

DataHub is one of the shared services under the three layers of the
[overview](../architecture.md#what-starts-the-pipelines-and-the-shared-services). Four things feed it. Every Spark
job of Layer 1 (the data platform), dbt included, posts the tables it read and wrote through OpenLineage
([openlineage.md](openlineage.md)). Airflow's DataHub listener adds the DAGs (pipelines of tasks), their tasks and
their runs. The validate stage of every Airflow pipeline ([airflow.md](airflow.md)) posts its Great Expectations
results, and Layer 3's RAG data pipeline ([rag_pipeline.md](../llm-platform/rag_pipeline.md)) does the same for its
checks and lineage. The daily `datahub_metadata` DAG loads what no listener covers: the core banking tables, the Kafka
topics, the dbt models and tests. DataHub's readers are people: the UI answers where a table came from, which pipeline
wrote it and whether its checks passed.

## How it works

1. Every Spark job runs with the OpenLineage listener. After each Spark action it posts the job, the tables it read
   and wrote and the column mappings between them to GMS, which turns the events into datasets, jobs and lineage
   edges. Lineage comes from the executed plan, so it cannot drift from the code ([openlineage.md](openlineage.md)).
2. Airflow's DataHub listener adds the DAGs, their tasks and their runs. Each Spark job is named after the DAG that
   ran it, which ties a table to the pipeline that produced it
   ([Which pipeline produced a table](#which-pipeline-produced-a-table)).
3. The validate stage of every pipeline posts each GE check as an assertion on its table, with one run event per DAG
   run, so a table's page shows its checks and their history ([Validation results](#validation-results)).
4. Once a day the `datahub_metadata` DAG runs one `datahub ingest` per recipe for the sources no listener sees: the
   core banking Postgres tables, the Kafka topics, and the dbt models, docs and tests.
5. DataHub keeps all of it in its own Postgres and OpenSearch and uses the core Kafka as its event bus. Its UI user is
   `datahub`; the password is `DATAHUB_ADMIN_PASSWORD` in `infra/.env`, which `make_env.py` writes into the frontend's
   `user.props` in place of the default `datahub:datahub`.

## Run it

DataHub needs the `gov` profile next to the batch stack and Airflow, and its admin password in `infra/.env`:

```bash
python infra/make_env.py      # DATAHUB_ADMIN_PASSWORD in infra/.env, written to the frontend's user.props
docker compose -f infra/docker-compose.yaml --profile core --profile batch --profile orch --profile gov up -d --build
docker exec airflow-scheduler airflow dags trigger datahub_metadata    # core banking tables, Kafka topics, dbt
```

Then open http://127.0.0.1:9002 and log in as `datahub`.

## What reaches DataHub

| What | Sent by | How |
|---|---|---|
| Lakehouse tables (bronze / silver / gold / feat), their schema and lineage, column-level lineage | every Spark job, dbt included | OpenLineage listener in [`infra/conf/spark/spark-defaults.conf` L34-L44](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/infra/conf/spark/spark-defaults.conf#L34-L44), posting to GMS `/openapi/openlineage/api/v1/lineage` ([openlineage.md](openlineage.md)) |
| DAGs, tasks, task runs | Airflow | DataHub's Airflow listener (`acryl-datahub-airflow-plugin`) |
| Validation results (GE) | `publish_results` task of every validate stage ([`pipelines/airflow/dags/common.py` L89-L145](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/common.py#L89-L145)) | `AssertionInfo` + `AssertionRunEvent` per expectation, on the table's hive URN (DataHub's entity id) |
| Core banking tables, Kafka topics, dbt models / docs / tests | `datahub_metadata` DAG | `datahub ingest` recipes in [`pipelines/airflow/dags/datahub_recipes/`](https://github.com/cvinhnguyen/card-fraud-platform/tree/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/datahub_recipes) |

The `datahub_metadata` DAG runs one `datahub ingest` per recipe
([`pipelines/airflow/dags/datahub_metadata.py`
L33-L37](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/datahub_metadata.py#L33-L37)):

```python
with DAG("datahub_metadata", schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args={**DEFAULT_ARGS, "depends_on_past": False}, doc_md=__doc__, tags=["governance"]) as dag:
    for name in ["core_banking", "kafka_topics", "dbt"]:
        recipe = {"kafka_topics": "kafka"}.get(name, name)
        BashOperator(task_id=name, bash_command=f"{DATAHUB} ingest -c {RECIPES}/{recipe}.yml", env=ENV, append_env=True)
```

## What DataHub holds

DataHub holds these entities (GraphQL counts) after the runs of DP1 (ingest to bronze), DP2 (bronze to silver and
gold) and DP3 (feature pipeline) from 2026-10-12 to 2026-10-15 and one run of `datahub_metadata`. DP2 was then one
DAG, `dp2_bronze_to_gold`. It has since been split into `dp2_bronze_to_silver` and `dp2_silver_to_gold`, so the DAG
count is now one higher. The runs of 2026-10-12 to 2026-10-15 also predate the move of `bronze.raw_transactions` into
lakeFS on 2026-10-24. DP1 now appends each export on the run's branch (`s3a://bronze/dp1_<ds>/raw_transactions`) and
merges it into `main` after the checks ([novel_lakefs_wap.md](novel_lakefs_wap.md)). The counts:

| Entity | Count |
|---|---:|
| Lakehouse tables (hive): 8 bronze, 2 silver, 7 gold (dims, fact, labels, features) + the 5 tables of `gold_pyspark` | 22 |
| Core banking tables (Postgres) | 6 (12 with the `core_banking.public` schema entities) |
| Kafka topics | 4 |
| dbt models / sources (shown as siblings of the hive tables they build) | 10 |
| Pipelines: Airflow DAGs (listener) and the Spark jobs of each DAG (OpenLineage) | 4 + 4 |
| Spark jobs (one per Spark action): 42 in the three DP pipelines + 11 of `gold_pyspark` | 53 |
| Airflow tasks | 22 |
| Job-dataset lineage edges | 74 |
| Assertions: 88 from GE (4 suites) + 27 from the 22 dbt tests (a test on two tables, the 4 `relationships` tests and the silver reconciliation, is one assertion on each table) | 115 |

### Tables of every zone

Every `bronze.*`, `silver.*` and `gold.*` table is there with its zone as the container. The tables dbt builds are
merged with their dbt model (two icons: dbt and Hive), so one entity carries the model's SQL, docs and tests and the
table's lineage. The datasets filtered on the Hive platform, 22 results:

![DataHub datasets filtered on platform Hive: 22 results, each in its zone container; raw_accounts carries both the dbt and the Hive icon](../images/datahub_tables.png)

### Lineage from the source to the features

The impact analysis upstream of the feature table, *all degrees*, finds 37 entities: 19 datasets and 18 tasks over
Spark, Hive, dbt and PostgreSQL. The chain reaches the core banking Postgres tables through `bronze.raw_cards` /
`raw_accounts` (7 hops):

![Feature table: impact analysis upstream, all degrees, 37 entities (19 datasets, 18 tasks)](../images/datahub_impact_feat.png)

The lineage graph of the feature table shows its direct inputs, `gold.fact_transaction` and `bronze.raw_disputes`:

![Feature table lineage: built from gold.fact_transaction and bronze.raw_disputes](../images/datahub_lineage_feat.png)

`silver.stg_transactions` sits in the middle. The graph below is from the DP2 run of 2026-11-07, when DP2 was still the
single DAG `dp2_bronze_to_gold`. Upstream are the three bronze tables that `build_silver` reads: `raw_customers`,
`raw_ref_province_mapping` and `raw_transactions`. They link through the task, because OpenLineage reports DP2's MERGE
without its source and the task declares them
([`pipelines/airflow/dags/dp2_bronze_to_silver.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/dp2_bronze_to_silver.py),
where `build_silver` now lives; [openlineage.md](openlineage.md#how-it-is-wired)). Downstream are dbt (the fact and
`dim_date`), the labels and the PySpark comparison job:

![silver.stg_transactions lineage: upstream bronze.raw_customers, bronze.raw_ref_province_mapping and bronze.raw_transactions through build_silver and the MERGE job; downstream dbt, gold.dim_date, gold.fact_transaction and gold_pyspark.fact_transaction](../images/datahub_lineage_silver.png)

### Which pipeline produced a table

Each table's upstream job carries the DAG's name. The OpenLineage job name is `spark.openlineage.appName`, which is
the DAG id, set for every Spark task in
[`pipelines/airflow/dags/common.py`
L68-L76](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/common.py#L68-L76).

`bronze.raw_transactions` takes one more step. DP1 writes the export into the lakeFS repository on a branch and
publishes it with a merge, an API call OpenLineage cannot see, so the DAG declares that hop
([`pipelines/airflow/dags/dp1_source_to_bronze.py`
L39-L43](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/dp1_source_to_bronze.py#L39-L43),
[`pipelines/airflow/dags/dp1_source_to_bronze.py`
L54](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/dp1_source_to_bronze.py#L54),
[`pipelines/airflow/dags/dp1_source_to_bronze.py`
L59](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/dp1_source_to_bronze.py#L59);
[novel_lakefs_wap.md](novel_lakefs_wap.md#how-it-is-wired)). After the DP1 run of 2026-11-07, the repository's
`bronze/raw_transactions` (S3) sits between the export and the table: `ingest_exports` and its Spark job write it from
`core-banking-export/transactions`, and `merge_to_main` reads it and produces `bronze.raw_transactions`. The table also
keeps its first producer, `create_table.bronze_raw_transactions`, the load that created it before lakeFS:

![DataHub lineage of bronze/raw_transactions in the lakeFS repository (16 columns): core-banking-export/transactions, then ingest_exports and its Spark job, then the repository dataset, then the hovered task merge_to_main of dp1_source_to_bronze, then bronze.raw_transactions](../images/datahub_lineage_bronze.png)

Each Airflow DAG is an entity of its own, with its documentation, tasks and runs from the Airflow listener. Below is the
pipeline `dp2_bronze_to_gold` with its DAG doc, as it was before DP2 was split. Once `dp2_bronze_to_silver` and
`dp2_silver_to_gold` have run, DataHub shows them in its place:

![The Airflow pipeline dp2_bronze_to_gold in DataHub with its DAG doc, before DP2 was split into two DAGs](../images/datahub_airflow_dp2.png)

### Validation results

Each GE result becomes an `AssertionInfo` (what is checked) and an `AssertionRunEvent` (this run's result); dbt tests
(from `run_results.json`) sit next to them. The assertion id is a hash of table, expectation and column, so every
run lands on the same assertion
([`pipelines/airflow/dags/common.py`
L104-L121](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/airflow/dags/common.py#L104-L121)):

```python
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
```

On `silver.stg_transactions` there are the 10 GE checks of that table and the dbt reconciliation test. The 10 GE
checks pass after the silver validation drill in [airflow.md](airflow.md); the drill's failing run is in each
assertion's history. The Quality tab sums up the 10 passing GE checks:

![silver.stg_transactions, Quality summary: 10 passing](../images/datahub_quality_silver.png)

The Assertions tab lists them, Passing (10) and External (11), with the dbt test
`fact_transaction_reconciles_with_silver` first:

![silver.stg_transactions, assertions: Passing (10), External (11), the dbt reconciliation test and the GE checks](../images/datahub_assertions_silver.png)

### A job nobody declared

[`pipelines/spark/build_gold_pyspark.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/pipelines/spark/build_gold_pyspark.py)
(the hand-written comparison for the dbt gold layer, never run by Airflow) appears with its inputs and outputs after its
first run:

![Lineage of gold_pyspark.fact_transaction, created by the listener alone](../images/datahub_new_job_lineage.png)

Column-level lineage is stored on each job (16 column mappings for that fact, see [openlineage.md](openlineage.md))
and readable through the API. The lineage canvas draws column edges only when the target table has a schema in
DataHub, which the tables written by path do not get from the listener.

## Gotchas

- GMS consumers stopped, so nothing was stored or searchable. The course compose sets
  `SPRING_KAFKA_PROPERTIES_USE_LATEST_VERSION` / `AUTO_REGISTER_SCHEMAS` for both the system-update job and GMS. With
  the internal schema registry, GMS's Kafka consumers then ask for the "latest version" of a schema, get
  `lookUpSchemaUnderSubject method not implemented` / `Error retrieving Avro value schema version for id 16`, and stop.
  OpenLineage events were accepted (HTTP 200) but entities had no properties and search returned 0. DataHub's own
  quickstart sets those variables (and `SCHEMA_REGISTRY_SYSTEM_UPDATE`) on system-update only; doing the same fixed it
  ([`infra/docker-compose.yaml`
  L504-L509](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/infra/docker-compose.yaml#L504-L509)).
- Our broker has auto-create off, and GMS still consumes the legacy topic `MetadataChangeEvent_v4` (and its failed /
  audit topics), which system-update does not create. `infra/init_infra.py` creates them
  ([`infra/init_infra.py`
  L16-L18](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/infra/init_infra.py#L16-L18)).
- DataHub (GMS 1 GB heap, OpenSearch 512 MB) fits next to batch + orch only without the Spark Thrift Server (see
  [airflow.md](airflow.md)); in the 8 GB Docker VM, swapping made Docker's DNS fail.
- `acryl-datahub-airflow-plugin` 1.6.x needs Airflow 3; 1.5.0.6 pins a `sqlglotc` build no longer on PyPI; 1.5.0.17
  works with Airflow 2.10 and the v1.5.0.6 server
  ([`infra/airflow/Dockerfile`
  L54-L57](https://github.com/cvinhnguyen/card-fraud-platform/blob/822721cab1ac597d118f100f1c49949f137f8a7d/infra/airflow/Dockerfile#L54-L57)).
