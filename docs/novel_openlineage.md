# Novel idea 2: automatic lineage with OpenLineage

OpenLineage was not taught in the course. It is an open standard for lineage events (a job run read these datasets
and wrote those, with column-level detail) plus integrations that emit them without code changes. Here a Spark
listener reads each query plan and posts the events to DataHub's OpenLineage endpoint; DataHub turns them into
datasets, jobs and lineage edges.

In the course (lesson L12, DAG `orders_pipeline`), every task declares its tables by hand
(`inlets=[DatahubDataset(...)]`, `outlets=[...]`) or emits `UpstreamLineage` itself. Each new job or new table means
editing the DAG, and nothing checks that the declaration matches what the code really reads and writes.

Here it takes the `openlineage-spark_2.13:1.53.0` jar in the Spark image (`infra/spark/Dockerfile`) and five lines in
`infra/conf/spark/spark-defaults.conf`, shared by every Spark container, the Airflow image (drivers of DAG tasks,
dbt's session) and the Thrift Server:

```properties
spark.extraListeners                  io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type      http
spark.openlineage.transport.url       http://datahub-gms:8080
spark.openlineage.transport.endpoint  /openapi/openlineage/api/v1/lineage
spark.openlineage.namespace           card-fraud-platform
```

and one setting per Airflow task, `spark.openlineage.appName = {{ dag.dag_id }}`, so the jobs carry the name of the
DAG that ran them. No job and no DAG contains a lineage statement.

## Proof 1: lineage of the whole pipeline, with no lineage code

After one DP1 -> DP2 -> DP3 run, DataHub holds (counts from its GraphQL API, see `docs/datahub.md` for the
screenshots):

| | Count |
|---|---:|
| Spark pipelines (one per DAG, `spark.openlineage.appName = dag_id`) | 3 |
| Jobs (one per Spark action) in those pipelines | 17 / 22 / 3 = 42 |
| Job-dataset edges written by DP1 / DP2 / DP3 | 23 / 30 / 4 = 57 |
| Lakehouse tables with lineage | 17 (8 bronze, 2 silver, 7 gold) |
| Longest chain | core banking Postgres -> bronze -> silver -> gold fact (dbt) -> features: 7 hops, 37 upstream entities |

Each Spark action becomes one DataHub job (`<dag>.<action>.<table>`, e.g.
`dp2_bronze_to_gold.execute_merge_into_command.stg_transactions`), linked to the tables it read (`Consumes`) and
wrote (`Produces`). The tables carry hive URNs (`bronze.raw_transactions`), the same names the GE assertions use, so
lineage and validation land on the same entity. Postgres inputs keep their source identity
(`core_banking.public.customers`).

## Proof 2: column-level lineage

The listener also reports, for every output column, the input columns it comes from and whether it is a copy or a
computation. For the fact built in `build_gold_pyspark.py`, DataHub shows 16 column mappings, for example:

| Output column (`fact_transaction`) | From | Kind |
|---|---|---|
| `transaction_id`, `card_id`, `merchant_id`, `txn_ts`, `txn_unit` | `silver.stg_transactions` (same names) | IDENTITY |
| `date_key` | `silver.stg_transactions.txn_date` | TRANSFORMATION (`date_format` + cast) |
| `customer_sk` | `dim_customer.customer_sk` (point-in-time join) | IDENTITY |

and for silver, `stg_transactions.txn_unit <- raw_ref_province_mapping.new_unit_name` (the 63 -> 34 unit lookup), the
example the plan asked for. The course's `inlets`/`outlets` are table-level, so they cannot declare this at all.

## Proof 3: a new job needs nothing

`pipelines/spark/build_gold_pyspark.py` (written for novel idea 1, never run by Airflow, no DataHub code) appeared in DataHub
with its 5 output tables and 17 job-dataset edges on its first run, column lineage included. With the course's
approach the same job would need its inputs and outputs written into a DAG first.

| | Course (L12): `inlets` / `outlets` / emitter | OpenLineage |
|---|---|---|
| Lineage statements to write for this pipeline | one per job input and output: 57 | 0 (5 config lines + 1 jar, shared) |
| Column-level | no | yes (16 mappings on the fact alone) |
| New job / new table | edit the DAG | nothing |
| Can drift from the code | yes (a declaration is a promise) | no (read from the executed plan) |
| Cost when DataHub is down | DAG code must handle it | listener logs the HTTP error, the job goes on |

## Limits seen

- A write by path (`.save(s3a://...)`) followed by `CREATE TABLE ... LOCATION` produces both an `s3` dataset
  (`lakehouse/bronze/raw_transactions`) and the `hive` one. Lineage is attached to the hive one (the listener
  resolves the Delta path to the table); the s3 ones are extra entities.
- There is one job per Spark action, not per Airflow task: a task with three writes shows three jobs. The DataHub Airflow
  listener adds the DAG / task entities next to them; the shared name (`spark.openlineage.appName = dag_id`) is what
  ties the two together.
- `ContextFactory: Query execution is null: can't emit event` errors in the logs are commands without a query plan
  (e.g. `SET`); harmless.
- OpenLineage needs `datahub-gms` resolvable from every container. When the Docker VM swapped, the listener logged
  `UnknownHostException: datahub-gms` and those events were lost; the jobs themselves carried on.
