# KServe predictor with the model inside the image: run fetch_model.py (MLflow alias -> ./model), then build.
# The image is immutable: a deployment is reproducible, a rollback is the previous image tag.
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 1000 kserve
COPY predictor.py .
COPY model/ /models/
USER 1000
ENTRYPOINT ["python", "predictor.py"]
