# KServe predictor with the model inside the image: run fetch_model.py (MLflow alias -> ./model), then build.
# The image is immutable: a deployment is reproducible, a rollback is the previous image tag.
# Stages: fetch (CI runs fetch_model.py in it) -> base -> test (CI: the release check) -> runtime (the default, last).
# MODEL is the folder with the fetched model: ./model by hand, model-<alias> in CI (services/predictor/Jenkinsfile).
FROM python:3.11-slim AS fetch
RUN pip install --no-cache-dir mlflow==3.16.1 xgboost-cpu==3.2.0 && useradd --uid 1000 --create-home fetch
WORKDIR /app
COPY fetch_model.py .
USER 1000
CMD ["python", "fetch_model.py", "--help"]

FROM python:3.11-slim AS base
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 1000 kserve
COPY predictor.py .
ARG MODEL=model
COPY ${MODEL}/ /models/

FROM base AS test
RUN pip install --no-cache-dir pytest==9.1.1
COPY test_predictor.py .
USER 1000
CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider", "test_predictor.py"]

FROM base AS runtime
USER 1000
ENTRYPOINT ["python", "predictor.py"]
