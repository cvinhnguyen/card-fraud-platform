# Automatic lineage with OpenLineage

OpenLineage is an open standard for lineage events (a job run read these datasets and wrote those, with column-level
detail) plus integrations that emit them without code changes. Here a Spark listener reads the query plan of every
Layer 1 Spark job, dbt included, and posts the events to DataHub's OpenLineage endpoint; DataHub turns them into
datasets, jobs and lineage edges. No Spark job declares its lineage; two DAG tasks declare theirs where OpenLineage
misses a hop ([below](#how-it-is-wired)).

| | |
|---|---|
| Code | [`infra/conf/spark/spark-defaults.conf` L34-L44](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/infra/conf/spark/spark-defaults.conf#L34-L44), [`infra/spark/Dockerfile` L8-L14](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/infra/spark/Dockerfile#L8-L14), [`pipelines/airflow/dags/common.py` L41-L49](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/airflow/dags/common.py#L41-L49) |
| Runs on | inside every Spark driver: the Spark containers, the Airflow image (drivers of DAG tasks, dbt's session) and the Thrift Server; DataHub GMS in compose profile `gov` receives the events |
| Reads | the query plan of every Spark action |
| Writes | lineage events to GMS `/openapi/openlineage/api/v1/lineage` |
| Related | [datahub.md](datahub.md), [dbt_gold.md](dbt_gold.md), [airflow.md](airflow.md) |

## Run it

Nothing runs for OpenLineage itself: with DataHub up, every Spark job posts its lineage
([airflow.md](airflow.md) runs the pipelines).

```bash
docker compose -f infra/docker-compose.yaml --profile core --profile batch --profile orch --profile gov up -d --build
# a job that no DAG declares: it appears in DataHub after its first run
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit \
    --conf spark.cores.max=2 --conf spark.executor.memory=1g /opt/jobs/build_gold_pyspark.py
```

## How it is wired

In the course (lesson L12, DAG `orders_pipeline`), every task declares its tables by hand
(`inlets=[DatahubDataset(...)]`, `outlets=[...]`) or emits `UpstreamLineage` itself. Each new job or new table means
editing the DAG, and nothing checks that the declaration matches what the code really reads and writes.

Here it takes the `openlineage-spark_2.13:1.53.0` jar in the Spark image
([`infra/spark/Dockerfile` L8-L14](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/infra/spark/Dockerfile#L8-L14))
and five lines in
[`infra/conf/spark/spark-defaults.conf`](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/infra/conf/spark/spark-defaults.conf),
shared by every Spark container, the Airflow image (drivers of DAG tasks, dbt's session) and the Thrift Server
([`infra/conf/spark/spark-defaults.conf` L37-L41](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/infra/conf/spark/spark-defaults.conf#L37-L41)):

```properties
spark.extraListeners                           io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type               http
spark.openlineage.transport.url                http://datahub-gms:8080
spark.openlineage.transport.endpoint           /openapi/openlineage/api/v1/lineage
spark.openlineage.namespace                    card-fraud-platform
```

Each Airflow task adds one setting, `spark.openlineage.appName = {{ dag.dag_id }}`, so the jobs carry the name of the
DAG that ran them. Every `spark_job()` of the DAGs gets it from `SPARK_CONF`, next to the executor memory and the
driver's bind address
([`pipelines/airflow/dags/common.py` L41-L49](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/airflow/dags/common.py#L41-L49)):

```python
SPARK_CONF = {
    # ponytail: 1.5 GB executors (2 GB in spark-defaults) so a pipeline run fits next to DataHub in the 8 GB Docker VM
    "spark.executor.memory": "1536m",
    # OpenLineage names its jobs after this: every table in DataHub shows which DAG's job produced it
    "spark.openlineage.appName": "{{ dag.dag_id }}",
    # The scheduler is on two networks (default + airflow-docker for DockerOperator): its hostname resolves to the
    # airflow-docker address first, so a driver bound there refuses the executors, which reach it on the default one
    "spark.driver.bindAddress": "0.0.0.0",
}
```

Apart from these settings, no Spark job declares lineage. Two DAG tasks declare their tables with `inlets` /
`outlets` (added 2026-10-25), where OpenLineage misses a hop:

- DP1 writes the export on a lakeFS branch and publishes it with a merge, an API call OpenLineage cannot see; there,
  [`infra/conf/spark/spark-defaults.conf` L42-L44](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/infra/conf/spark/spark-defaults.conf#L42-L44) also drops the branch from the written dataset's name
  ([`pipelines/airflow/dags/dp1_source_to_bronze.py` L37-L41](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/airflow/dags/dp1_source_to_bronze.py#L37-L41), [`pipelines/airflow/dags/dp1_source_to_bronze.py` L52](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/airflow/dags/dp1_source_to_bronze.py#L52), [`pipelines/airflow/dags/dp1_source_to_bronze.py` L59](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/airflow/dags/dp1_source_to_bronze.py#L59); [novel_lakefs_wap.md](novel_lakefs_wap.md#how-it-is-wired)).
- DP2's `build_silver`: OpenLineage reports its MERGE into `silver.stg_transactions` with the target only, without
  `bronze.raw_transactions`. A probe on 2026-10-25 (OpenLineage 1.53, Delta 4) gave the same with a SQL MERGE,
  without the cache and without source materialization, while a MERGE from `bronze.raw_ref_province_mapping` kept
  its source. The task now declares the three bronze tables it reads and the two silver tables it writes
  ([`pipelines/airflow/dags/dp2_bronze_to_gold.py` L24-L28](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/airflow/dags/dp2_bronze_to_gold.py#L24-L28), [`pipelines/airflow/dags/dp2_bronze_to_gold.py` L47](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/airflow/dags/dp2_bronze_to_gold.py#L47)).

## Lineage of the whole pipeline, with no lineage code in the jobs

After one run of DP1, DP2 and DP3, DataHub holds (counts from its GraphQL API; [datahub.md](datahub.md) has the
screenshots):

| | Count |
|---|---:|
| Spark pipelines (one per DAG, `spark.openlineage.appName = dag_id`) | 3 |
| Jobs (one per Spark action) in those pipelines | 17 / 22 / 3 = 42 |
| Job-dataset edges written by DP1 / DP2 / DP3 | 23 / 30 / 4 = 57 |
| Lakehouse tables with lineage | 17 (8 bronze, 2 silver, 7 gold) |
| Longest chain | from core banking Postgres through bronze, silver and the gold fact (dbt) to the features: 7 hops, 37 upstream entities |

Each Spark action becomes one DataHub job (`<dag>.<action>.<table>`, e.g.
`dp2_bronze_to_gold.execute_merge_into_command.stg_transactions`), linked to the tables it read (`Consumes`) and
wrote (`Produces`). The tables carry hive URNs (`bronze.raw_transactions`), the same names the GE assertions use, so
lineage and validation land on the same entity. Postgres inputs keep their source identity
(`core_banking.public.customers`). In DataHub after the DP1 run of 2026-11-07, the export's way into bronze:
`core-banking-export/transactions`, the Spark job and the Airflow task `ingest_exports`, the lakeFS repository's
`bronze/raw_transactions` (one dataset for every branch, thanks to the path pattern above), then `merge_to_main` and
`bronze.raw_transactions`:

![DataHub lineage of bronze/raw_transactions in the lakeFS repository (16 columns): core-banking-export/transactions, then ingest_exports and its Spark job, then the repository dataset, then the hovered task merge_to_main of dp1_source_to_bronze, then bronze.raw_transactions](images/datahub_lineage_bronze.png)

`silver.stg_transactions` sits between its bronze inputs, linked through the task `build_silver` (declared, see
above), and the jobs that read it:

![DataHub: silver.stg_transactions between bronze.raw_customers, bronze.raw_ref_province_mapping and bronze.raw_transactions (through build_silver and the MERGE job) and the jobs that read it](images/datahub_lineage_silver.png)

## Column-level lineage

The listener also reports, for every output column, the input columns it comes from and whether it is a copy or a
computation; GMS keeps them because of `DATAHUB_OPENLINEAGE_CAPTURE_COLUMN_LEVEL_LINEAGE`
([`infra/docker-compose.yaml` L535-L537](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/infra/docker-compose.yaml#L535-L537)).
For the fact built in
[`pipelines/spark/build_gold_pyspark.py` L68-L79](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/spark/build_gold_pyspark.py#L68-L79),
DataHub shows 16 column mappings, for example:

| Output column (`fact_transaction`) | From | Kind |
|---|---|---|
| `transaction_id`, `card_id`, `merchant_id`, `txn_ts`, `txn_unit` | `silver.stg_transactions` (same names) | IDENTITY |
| `date_key` | `silver.stg_transactions.txn_date` | TRANSFORMATION (`date_format` + cast) |
| `customer_sk` | `dim_customer.customer_sk` (point-in-time join) | IDENTITY |

For silver it shows `stg_transactions.txn_unit <- raw_ref_province_mapping.new_unit_name`, the example the plan asked
for: the lookup from 63 provinces to 34 units in
[`pipelines/spark/build_silver.py` L20-L35](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/spark/build_silver.py#L20-L35).
The course's `inlets`/`outlets` are table-level, so they cannot declare this at
all.

## A new job needs nothing

[`pipelines/spark/build_gold_pyspark.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/spark/build_gold_pyspark.py)
was written to compare with dbt. Airflow never runs it and it has no DataHub code: its entry point is plain Spark
([`pipelines/spark/build_gold_pyspark.py` L123-L136](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/spark/build_gold_pyspark.py#L123-L136)).
It appeared in DataHub with its 5 output tables and 17 job-dataset edges on its first run, column lineage included.
The lineage of its `gold_pyspark.fact_transaction`, created by the listener alone:

![DataHub: lineage of gold_pyspark.fact_transaction, created by the listener alone](images/datahub_new_job_lineage.png)

With the course's approach the same job would need its inputs and outputs written into a DAG first:

| | Course (L12): `inlets` / `outlets` / emitter | OpenLineage |
|---|---|---|
| Lineage statements to write for this pipeline | one per job input and output: 57 | 0 (5 config lines + 1 jar, shared) |
| Column-level | no | yes (16 mappings on the fact alone) |
| New job / new table | edit the DAG | nothing |
| Can drift from the code | yes (a declaration is a promise) | no (read from the executed plan) |
| Cost when DataHub is down | DAG code must handle it | listener logs the HTTP error, the job goes on |

## Limits

- A write by path (`.save(s3a://...)`) followed by `CREATE TABLE ... LOCATION` (the `save()` helper of
  [`pipelines/spark/ingest_bronze.py` L43-L54](https://github.com/cvinhnguyen/card-fraud-platform/blob/612ffdc2aa8a38f9098d6190f54bd46c2ea70315/pipelines/spark/ingest_bronze.py#L43-L54))
  produces both an `s3` dataset (`lakehouse/bronze/raw_transactions`) and the `hive` one. Lineage is attached to the
  hive one (the listener resolves the Delta path to the table); the s3 ones are extra entities. Since
  `bronze.raw_transactions` moved into lakeFS (2026-10-24), the helper registers a table only for
  `s3a://lakehouse/bronze` and lakeFS `main`; a DP1 run appends the export on its own lakeFS branch, which the
  metastore does not point at ([novel_lakefs_wap.md](novel_lakefs_wap.md)).
- There is one job per Spark action, not per Airflow task: a task with 3 writes shows 3 jobs. The DataHub Airflow
  listener adds the DAG / task entities next to them; the shared name (`spark.openlineage.appName = dag_id`) is what
  ties the two together.
- `ContextFactory: Query execution is null: can't emit event` errors in the logs are commands without a query plan
  (e.g. `SET`); harmless.
- OpenLineage needs `datahub-gms` resolvable from every container. When the Docker VM swapped, the listener logged
  `UnknownHostException: datahub-gms` and those events were lost; the jobs themselves carried on.
