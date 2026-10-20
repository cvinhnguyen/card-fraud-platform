# Card support MCP server. Build stage: dependencies into a venv; runtime: the venv + code only, non-root.
FROM python:3.11-slim AS build
RUN python -m venv /venv
COPY requirements.txt /tmp/
RUN /venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

# CI: docker build --target test -t mcp-test . && docker run --rm mcp-test   (pytest + coverage gate)
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
COPY feature_repo_fraud/ feature_repo_fraud/
COPY feature_repo_kb/ feature_repo_kb/
COPY app/ app/
RUN useradd --uid 1000 mcp
USER 1000
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
