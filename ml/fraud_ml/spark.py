"""Spark session for the ML steps: local (notebook, most pipeline steps) or on Kubernetes (distributed training).

On Kubernetes the step's pod is the driver (client mode) and the executors are pods of the same image, so the
XGBoost workers have the driver's Python packages. The driver is reached by its pod IP (POD_IP, downward API).
"""
import os

from pyspark.sql import SparkSession

IMAGE = os.environ.get("ML_IMAGE", "localhost:5001/fraud/ml-train:latest")
S3 = {
    "spark.hadoop.fs.s3a.endpoint": "http://lakehouse-s3:8333",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}


def session(app: str, executors: int = 0, executor_memory: str = "1g", driver_memory: str = "1500m") -> SparkSession:
    """executors = 0: local[2]; > 0: that many executor pods in the step's namespace."""
    b = (SparkSession.builder.appName(app)
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC")   # naive timestamps are UTC wall clock, as in feature_store.yaml
         .config("spark.driver.memory", driver_memory)
         .config("spark.sql.shuffle.partitions", "16")
         # pandas <-> Spark through Arrow: without it createDataFrame pickles 1M training rows and OOMs a 4.5 GiB step
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         # tables are read and written by S3 path: keep the catalog's empty default database out of the working dir
         .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
         .config("spark.ui.showConsoleProgress", "false"))
    for k, v in S3.items():
        b = b.config(k, v)
    if executors == 0:
        return b.master("local[2]").getOrCreate()
    b = (b.master("k8s://https://kubernetes.default.svc:443")
         .config("spark.kubernetes.namespace", os.environ.get("POD_NAMESPACE", "kubeflow"))
         .config("spark.kubernetes.container.image", IMAGE)
         .config("spark.kubernetes.authenticate.serviceAccountName", os.environ.get("SPARK_SA", "pipeline-runner"))
         .config("spark.kubernetes.executor.podNamePrefix", app.replace("_", "-"))
         .config("spark.executor.instances", str(executors))
         .config("spark.executor.cores", "1")
         .config("spark.executor.memory", executor_memory)
         .config("spark.driver.host", os.environ["POD_IP"])
         .config("spark.driver.bindAddress", "0.0.0.0"))
    for env, key in (("AWS_ACCESS_KEY_ID", "access_key"), ("AWS_SECRET_ACCESS_KEY", "secret_key")):
        # executors read S3 too: they get the keys from the Kubernetes secret, not as values in the Spark conf
        b = b.config(f"spark.kubernetes.executor.secretKeyRef.{env}", f"s3-credentials:{key}")
    return b.getOrCreate()
