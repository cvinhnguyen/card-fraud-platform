# Airflow 2.10 (same minor as the course) + a Spark client identical to the cluster, so SparkSubmitOperator can run
# drivers in the scheduler container (client mode; standalone has no cluster mode for Python).
# Multi-stage (docs/docker_optimization.md):
#   spark  the image the workers run: the final stage copies its JRE and only the Spark parts a client needs
#   venvs  Great Expectations, dbt, the DataHub CLI and (Layer 2) Feast, each in its own venv (pins clash with Airflow's),
#          built here and copied without pip, bytecode caches and test suites
# infra/spark/Dockerfile: Spark 4.1.3 + the lakehouse jars, the image the cluster runs
FROM fraud/spark:4.1.3 AS spark

FROM apache/airflow:2.10.5-python3.10 AS venvs
USER root
RUN mkdir -p /opt/venvs && chown airflow:0 /opt/venvs
USER airflow
# GE runs inside Spark drivers (spark.pyspark.driver.python); pyspark itself comes from SPARK_HOME at run time.
RUN python -m venv /opt/venvs/ge && /opt/venvs/ge/bin/pip install --no-cache-dir "great_expectations==0.18.19"
# thrift-sasl + pure-sasl: PyHive needs them to reach the Thrift Server; the PyHive extra does not pull them on Linux.
RUN python -m venv /opt/venvs/dbt && /opt/venvs/dbt/bin/pip install --no-cache-dir "dbt-core==1.11.16" \
      "dbt-spark[PyHive]==1.11.0" "thrift-sasl==0.4.3" "pure-sasl==0.6.2"
# DataHub ingestion CLI: only the sources we ingest (no delta-lake extra: it pulled pyspark + deltalake, 480 MB;
# no hive-metastore extra: it compiles sasl). Lakehouse tables reach DataHub through OpenLineage.
RUN python -m venv /opt/venvs/datahub && /opt/venvs/datahub/bin/pip install --no-cache-dir \
      "acryl-datahub[datahub-rest,postgres,kafka,dbt]==1.5.0.17"
# Layer 2: Feast (materialize to Redis; the drift job reads the offline store and scores with the production model).
# pyspark comes from SPARK_HOME at run time, as for dbt; delta-spark without deps for `delta.tables`.
RUN python -m venv /opt/venvs/feast && /opt/venvs/feast/bin/pip install --no-cache-dir "feast[redis,postgres]==0.66.0" \
      "mlflow-skinny==3.16.1" "xgboost-cpu==3.2.0" "scikit-learn==1.7.2" "kfp==2.17.0" "deltalake==0.25.5" \
    && /opt/venvs/feast/bin/pip install --no-cache-dir --no-deps "delta-spark==4.4.1"
# Test suites of the packages go, except two that are code: dbt/include/*/macros/materializations/tests holds the `test`
# materialization and get_where_subquery (without it every dbt test fails to compile), and numpy 2 imports
# numpy._core.tests._natype at run time (numpy.testing, pulled by scipy / xgboost). Both found by the P9 functional run.
RUN find /opt/venvs -depth \( -name __pycache__ -o \( -name tests -not -path "*/site-packages/dbt/*" \
      -not -path "*/site-packages/numpy/*" \) \) -type d \
      -exec rm -rf {} + \
    && for v in /opt/venvs/*; do $v/bin/pip uninstall -y -q pip; done

FROM apache/airflow:2.10.5-python3.10
USER root
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
# A Spark client needs bin/, jars/ and the Python libs; examples, R, data, tests and sbin stay in the Spark image.
COPY --from=spark /opt/spark/bin /opt/spark/bin
COPY --from=spark /opt/spark/jars /opt/spark/jars
COPY --from=spark /opt/spark/python/lib /opt/spark/python/lib
COPY --from=spark /opt/spark/RELEASE /opt/spark/RELEASE
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:$PATH
# dbt-artifacts is a named volume: created here so it is owned by group 0 and writable by uid 185 (spark)
RUN mkdir -p /opt/spark/conf /opt/airflow/dbt-artifacts && chown airflow:0 /opt/airflow/dbt-artifacts \
    && chmod g+rwx /opt/airflow/dbt-artifacts
COPY --from=venvs --chown=airflow:0 /opt/venvs /opt/venvs

USER airflow
# One layer. The Spark provider without its dependencies: it declares pyspark (340 MB) but SparkSubmitOperator only
# calls the spark-submit binary; delta-spark (no deps) for `from delta.tables import` in the drivers. DataHub listener 1.5.0.x: the last line for Airflow 2, matching the server
# (v1.5.0.6; 1.5.0.6 itself pins a sqlglotc build no longer on PyPI, so 1.5.0.17).
# Docker provider 4.4.5 (the last for Airflow 2.10): the RAG DAG runs its steps in the CI-built ml image.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "acryl-datahub-airflow-plugin==1.5.0.17" \
      "apache-airflow-providers-docker==4.4.5" \
    && pip install --no-cache-dir --no-deps "apache-airflow-providers-apache-spark==5.0.0" "delta-spark==4.4.1" \
    && find /home/airflow/.local -depth -name __pycache__ -type d -exec rm -rf {} +
