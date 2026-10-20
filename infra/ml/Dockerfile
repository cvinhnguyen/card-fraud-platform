# Layer 2 ML image: notebook, KFP components, Spark-on-Kubernetes executors (same image, so the distributed
# XGBoost workers have the same Python), Feast materialize and push jobs.
# Base: the Layer 1 Spark image (Spark 4.1.3, Python 3.10, Delta / S3A jars baked in).
FROM fraud/spark:4.1.3
USER 0
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir --upgrade pip==25.2 \
 && python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt
# pyspark from the Spark install (exactly the JVM version), so pip does not bring a second Spark
ENV PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
USER 185
WORKDIR /opt/ml
