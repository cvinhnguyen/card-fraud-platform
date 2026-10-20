# Layer 2 code image: the training library, the Feast repos, the jobs and tools on top of fraud/ml (dependencies only),
# so a code change rebuilds in seconds. Build from the repo root: docker build -f ml/Dockerfile -t fraud/ml-train .
# Stages: code -> test (CI: docker build --target test ... && docker run --rm <image>) -> runtime (the default, last).
ARG BASE=fraud/ml:0.1
FROM ${BASE} AS code
USER 0
RUN python3 -m pip install --no-cache-dir kfp-kubernetes==2.17.0   # pipeline compile / submit (ml/pipelines/)
USER 185
COPY --chown=185:0 ml/fraud_ml /opt/ml/fraud_ml
COPY --chown=185:0 ml/feature_repo /opt/ml/feature_repo
COPY --chown=185:0 ml/feature_repo_kb /opt/ml/feature_repo_kb
COPY --chown=185:0 ml/pipelines /opt/ml/pipelines
COPY --chown=185:0 ml/jobs /opt/ml/jobs
COPY --chown=185:0 ml/tools /opt/ml/tools
ENV PYTHONPATH=/opt/ml:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    FEAST_REPO=/opt/ml/feature_repo FEAST_KB_REPO=/opt/ml/feature_repo_kb

# unit tests of the jobs (push jobs, RAG ingest); the bank's documents are the RAG tests' fixture
FROM code AS test
USER 0
RUN python3 -m pip install --no-cache-dir pytest==9.1.1
USER 185
COPY --chown=185:0 generator/knowledge /opt/ml/jobs/tests/knowledge
WORKDIR /opt/ml/jobs
CMD ["python3", "-m", "pytest", "-q", "tests"]

FROM code AS runtime
