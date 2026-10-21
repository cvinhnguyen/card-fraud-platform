# inference-perf, the load generator llm-d's benchmark harness runs (kubernetes-sigs/inference-perf), from its PyPI
# release: the published image is only tagged by commit, and uvloop rules out a native Windows install.
FROM python:3.13-slim
RUN pip install --no-cache-dir inference-perf==0.7.0
WORKDIR /bench
ENTRYPOINT ["inference-perf"]
