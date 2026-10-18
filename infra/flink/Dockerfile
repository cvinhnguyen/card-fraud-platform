# Flink 2.2 with PyFlink (same version) and the Kafka connector, for the `stream` profile.
# Multi-stage (docs/docker_optimization.md): PyFlink is installed into a venv in a build stage that has pip and venv;
# the final image gets the venv and the Python runtime only.
FROM flink:2.2.1-scala_2.12-java17 AS build
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-venv
RUN python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir apache-flink==2.2.1
# pyflink/lib is a copy of the Flink 2.2.1 jars already in /opt/flink/lib (267 MB): link instead of copy.
# Bytecode caches and test suites are not needed at run time.
RUN cd /opt/venv/lib/python3.12/site-packages \
    && rm -rf pyflink/lib && ln -s /opt/flink/lib pyflink/lib \
    && find /opt/venv -depth \( -name __pycache__ -o -name tests \) -type d -exec rm -rf {} + \
    && /opt/venv/bin/pip uninstall -y -q pip

FROM flink:2.2.1-scala_2.12-java17
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*
COPY --from=build --chown=flink:flink /opt/venv /opt/venv
# `python` (the default python.executable of the Python workers) resolves to the venv
ENV PATH=/opt/venv/bin:$PATH
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/
USER flink
