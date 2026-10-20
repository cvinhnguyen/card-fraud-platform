# MLflow tracking server + model registry (Layer 2): Postgres backend store, artifacts proxied to S3 (SeaweedFS).
FROM python:3.11-slim
RUN pip install --no-cache-dir mlflow==3.16.1 "psycopg[binary]==3.2.10" boto3==1.40.40 \
 && useradd --uid 1000 --create-home mlflow
USER mlflow
EXPOSE 5000
