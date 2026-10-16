"""Deployment view of Layer 1: which container runs in which compose profile.

Diagram as code (mingrammer/diagrams, needs Graphviz `dot` on PATH).
Usage: python docs/diagrams/deployment.py  ->  docs/images/layer1-deployment.png
"""
from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.generic.storage import Storage
from diagrams.onprem.analytics import Dbt, Flink, Hive, Spark, Trino
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.queue import Kafka
from diagrams.onprem.workflow import Airflow
from diagrams.programming.language import Python

OUT = Path(__file__).resolve().parents[1] / "images" / "layer1-deployment"
GRAPH = {"fontname": "Helvetica", "fontsize": "16", "pad": "0.4", "nodesep": "0.5", "ranksep": "1.0", "splines": "spline"}
NODE = {"fontname": "Helvetica", "fontsize": "11"}
EDGE = {"fontname": "Helvetica", "fontsize": "10", "color": "#4f5d75", "fontcolor": "#4f5d75"}
LINEAGE = {"color": "#7a8c47", "fontcolor": "#7a8c47", "style": "dashed"}


def profile(label: str, running: bool) -> Cluster:
    """A compose profile. Solid border = running today, dashed = planned phase."""
    style = {"style": "rounded", "bgcolor": "#ffffff", "pencolor": "#2d3142"} if running else \
            {"style": "rounded,dashed", "bgcolor": "#fafafa", "pencolor": "#9aa1b1"}
    return Cluster(label, graph_attr={**style, "fontname": "Helvetica", "fontsize": "12", "margin": "16"})


with Diagram("Layer 1 deployment · docker compose project `fraud`", filename=str(OUT), show=False,
             direction="LR", graph_attr=GRAPH, node_attr=NODE, edge_attr=EDGE):
    with Cluster("Host · Windows 11 · repo .venv", graph_attr={"bgcolor": "#f5f5f5", "pencolor": "#c8ccd4", "fontname": "Helvetica"}):
        gen = Python("generator\ngenerate.py · load_source.py")
        prod = Python("producer\n(P2, planned)")

    with Cluster("Docker Desktop · WSL2 · disk on E: · ports bound to 127.0.0.1",
                 graph_attr={"bgcolor": "#eef0f3", "pencolor": "#c8ccd4", "fontname": "Helvetica"}):
        with profile("core · running", True):
            pg = PostgreSQL("postgres:16-alpine\ncore_banking :5433")
            s3 = Storage("seaweedfs:4.48\nS3 :8333 · filer :8888")
            kafka = Kafka("apache/kafka:3.9.0\nKRaft :9092")

        with profile("batch · P3–P4", False):
            spark = Spark("Spark master\n+ 2 workers")
            thrift = Dbt("Spark Thrift Server\n← dbt-spark")
            hms = Hive("Hive Metastore")
            trino = Trino("Trino")

        with profile("stream · P6", False):
            flink = Flink("Flink · JobManager + TaskManager\nraw events → bronze (s3a)")

        with profile("orch · P7", False):
            airflow = Airflow("Airflow\n+ Great Expectations")

        with profile("gov · P8 · run alone (~6–8 GB)", False):
            datahub = Server("DataHub\nGMS + frontend")

    gen >> Edge(label="COPY") >> pg
    gen >> Edge(label="S3 PUT") >> s3
    prod >> Edge(label="produce") >> kafka
    pg >> Edge(label="JDBC") >> spark
    s3 >> Edge(label="s3a") >> spark
    kafka >> Edge(label="consume") >> flink
    spark >> Edge(label="tables") >> hms
    trino >> Edge(label="catalog", style="dashed") >> hms
    airflow >> Edge(label="spark-submit") >> spark
    airflow >> Edge(label="dbt build") >> thrift
    spark >> Edge(label="OpenLineage", **LINEAGE) >> datahub
    airflow >> Edge(label="OpenLineage", **LINEAGE) >> datahub
