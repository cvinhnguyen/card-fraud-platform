# Drift Detection API. Build stage: dependencies into a venv; runtime: venv + code, non-root.
FROM python:3.11-slim AS build
RUN python -m venv /venv
COPY requirements.txt /tmp/
RUN /venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

# CI: docker build --target test -t drift-test . && docker run --rm drift-test
FROM build AS test
COPY requirements-dev.txt /tmp/
RUN /venv/bin/pip install --no-cache-dir -r /tmp/requirements-dev.txt
ENV PATH=/venv/bin:$PATH
WORKDIR /app
COPY . .
CMD ["pytest"]

FROM python:3.11-slim AS runtime
COPY --from=build /venv /venv
ENV PATH=/venv/bin:$PATH PYTHONUNBUFFERED=1
WORKDIR /app
COPY app/ app/
RUN useradd --uid 1000 api
USER 1000
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
