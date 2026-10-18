# Spark 4.1.3 with the lakehouse jars baked in (Delta, S3A, Postgres JDBC, OpenLineage), for every Spark container
# and, through COPY --from, the Airflow image.
# Multi-stage (docs/docker_optimization.md): the build stage resolves the Maven packages once with Ivy; the final
# image gets only the jars Spark does not already ship. Before, `spark.jars.packages` resolved them at every
# spark-submit and the standalone worker copied them (~600 MB) into each application's work dir: 94 GB after a day.
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu AS jars
USER root
RUN echo "" > /tmp/noop.py && /opt/spark/bin/spark-submit --master "local[1]" --conf spark.jars.ivy=/tmp/ivy \
      --packages io.delta:delta-spark_4.1_2.13:4.4.1,org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.14,io.openlineage:openlineage-spark_2.13:1.53.0 \
      /tmp/noop.py
# Keep only what /opt/spark/jars lacks: Ivy also pulls e.g. an older jackson, which breaks Spark on the classpath.
RUN mkdir /tmp/keep && cd /tmp/ivy/jars && cp io.delta_*.jar io.unitycatalog_*.jar org.apache.hadoop_hadoop-aws-*.jar \
      software.amazon.awssdk_bundle-*.jar software.amazon.s3.analyticsaccelerator_*.jar org.wildfly.openssl_*.jar \
      org.postgresql_postgresql-*.jar io.openlineage_openlineage-spark_*.jar /tmp/keep/

FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
USER root
COPY --from=jars /tmp/keep/ /opt/spark/jars/
# `from delta.tables import DeltaTable` in the jobs: with --packages the Python module came from the jar on the
# PYTHONPATH; now it is the PyPI package of the same version (no deps: pyspark is /opt/spark/python).
RUN pip3 install --no-cache-dir --no-deps "delta-spark==4.4.1"
USER 185
