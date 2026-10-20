"""Feast definitions of the RAG knowledge base (Layer 3): one row per chunk of the bank's card documents.

Written by the Airflow DAG `rag_ingest`: the chunk history as Parquet in the lakehouse (offline) and the latest
version of every chunk in Postgres/pgvector (online). Read by the card support MCP server: `get_online_features`
by `chunk_id`, and `retrieve_online_documents_v2` for the chunks most similar to a question.
"""
from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Array, Float32, String

EMBEDDING_MODEL = "qwen3-embedding-0.6b"   # served by llama.cpp; 1024-dimensional vectors
EMBEDDING_DIM = 1024

chunk = Entity(name="chunk", join_keys=["chunk_id"], value_type=ValueType.STRING,
               description="One chunk of a bank document: `<document>#<section number>`, e.g. `card_terms#4.2`")

policy_chunks_source = FileSource(
    name="policy_chunks_source",
    path="s3://lakehouse/kb/policy_chunks.parquet",
    timestamp_field="event_timestamp",   # the document version the chunk comes from
    created_timestamp_column="created",
    s3_endpoint_override="http://lakehouse-s3:8333",
)

policy_chunks = FeatureView(
    name="policy_chunks",
    entities=[chunk],
    ttl=timedelta(0),   # no expiry: a chunk is valid until its document is ingested again
    schema=[
        Field(name="chunk_id", dtype=String),
        Field(name="document", dtype=String),
        Field(name="section", dtype=String),
        Field(name="text", dtype=String),
        # lowercase: Feast's postgres store rejects "COSINE" (the spelling in Feast's docs)
        Field(name="embedding", dtype=Array(Float32), vector_index=True, vector_search_metric="cosine"),
    ],
    source=policy_chunks_source,
    online=True,
    tags={"layer": "3", "owner": "card-support"},
)
