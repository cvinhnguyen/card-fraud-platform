"""Steps of the Airflow DAG `rag_ingest` (Layer 3): the bank's documents -> chunks -> embeddings -> Feast.

  chunk     s3://bank-documents/*.md -> one chunk per numbered clause (`card_terms#4.2`), one per section without
            clauses (fee tables), one per FAQ question; the document title and section heading are kept in the text
            so a chunk is understandable on its own -> s3://lakehouse/kb/staging/ds=<ds>/chunks.parquet
  embed     every chunk through the embedding server (OpenAI-compatible /v1/embeddings, Qwen3-Embedding-0.6B)
            -> .../embedded.parquet
  write     Feast project card_support_kb: appends the chunks to the offline history
            (s3://lakehouse/kb/policy_chunks.parquet, the FileSource) and writes them to the online store (pgvector)
  validate  checks on the written chunks, in the report format of the Layer 1 validate stage
            -> s3://lakehouse/_validation/rag_ingest/ds=<ds>/results.json (published to DataHub by the DAG)

Steps exchange S3 paths, never data. Documents are embedded as plain text; questions get Qwen3-Embedding's
instruction prefix at query time (MCP server).

Usage (Airflow feast venv): python ml/jobs/rag_ingest.py --step chunk --ds 2026-10-20
"""
import argparse
import datetime as dt
import json
import os
import re
import sys
import urllib.request

import pandas as pd
import pyarrow.fs as pafs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # ml/, for fraud_ml
from fraud_ml.steps import read_parquet, s3, write_parquet   # noqa: E402

DOCS_BUCKET = "bank-documents"
KB = "s3a://lakehouse/kb"
HISTORY = f"{KB}/policy_chunks.parquet"   # the FileSource of feature view policy_chunks
TABLE = "card_support_kb.policy_chunks"   # how DataHub names the feature view (platform feast)
EMBEDDING_MODEL, EMBEDDING_DIM = "qwen3-embedding-0.6b", 1024
CLAUSE = re.compile(r"^(\d+\.\d+)\s+(.+)")
SECTION = re.compile(r"^(\d+)\.\s+(.+)")


def staging(ds: str, name: str) -> str:
    """Path of one step's output for run date ds."""
    return f"{KB}/staging/ds={ds}/{name}.parquet"


def split_document(doc: str, text: str) -> list[dict]:
    """Chunks of one Markdown document: numbered clauses, else whole `##` sections (FAQ: one per question)."""
    title = text.splitlines()[0].lstrip("# ").strip()
    version = re.search(r"Version (\d{4}-\d{2}-\d{2})", text).group(1)
    chunks = []
    for i, block in enumerate(re.split(r"^## ", text, flags=re.M)[1:], start=1):
        heading, *body = block.strip().splitlines()
        lines = [line.strip() for line in body if line.strip()]
        clauses = [m for m in map(CLAUSE.match, lines) if m]
        numbered = SECTION.match(heading)
        if clauses:
            parts = [(m.group(1), m.group(2)) for m in clauses]
        else:   # a fee table or an FAQ answer: the whole section is one chunk
            parts = [(numbered.group(1) if numbered else str(i), "\n".join(lines))]
        for number, content in parts:
            chunks.append({"chunk_id": f"{doc}#{number}", "document": doc, "section": number,
                           "text": f"{title} > {heading}\n{content}", "version": version})
    return chunks


def chunk(ds: str) -> str:
    """Read every document of the bucket and write the chunks."""
    fs = s3()
    infos = [i for i in fs.get_file_info(pafs.FileSelector(DOCS_BUCKET)) if i.path.endswith(".md")]
    rows = []
    for info in infos:
        with fs.open_input_stream(info.path) as f:
            rows += split_document(os.path.basename(info.path).removesuffix(".md"), f.read().decode("utf-8"))
    df = pd.DataFrame(rows)
    print(f"{len(infos)} documents -> {len(df)} chunks: " + ", ".join(f"{d} {n}" for d, n in df.document.value_counts().items()))
    return write_parquet(df, staging(ds, "chunks"))


def embed_texts(texts: list[str], batch: int = 32) -> list[list[float]]:
    """Vectors from the embedding server (EMBEDDINGS_URL, Bearer LLM_API_KEY), in input order."""
    out = []
    for i in range(0, len(texts), batch):
        req = urllib.request.Request(f"{os.environ['EMBEDDINGS_URL']}/v1/embeddings",
                                     json.dumps({"model": EMBEDDING_MODEL, "input": texts[i:i + batch]}).encode(),
                                     {"Content-Type": "application/json",
                                      "Authorization": f"Bearer {os.environ['LLM_API_KEY']}"})
        with urllib.request.urlopen(req, timeout=120) as r:
            out += [d["embedding"] for d in sorted(json.load(r)["data"], key=lambda d: d["index"])]
    return out


def embed(ds: str) -> str:
    """Add the embedding of every chunk."""
    df = read_parquet(staging(ds, "chunks"))
    df["embedding"] = embed_texts(df.text.tolist())
    print(f"{len(df)} chunks embedded ({EMBEDDING_MODEL}, {len(df.embedding.iloc[0])} dimensions)")
    return write_parquet(df, staging(ds, "embedded"))


def feature_rows(ds: str) -> pd.DataFrame:
    """The embedded chunks in the feature view's schema: event_timestamp = document version, created = now."""
    df = read_parquet(staging(ds, "embedded"))
    df["event_timestamp"] = pd.to_datetime(df.pop("version"), utc=True)
    df["created"] = pd.Timestamp.now(tz="UTC")
    return df


def write(ds: str) -> str:
    """Append to the offline history and write the latest version of every chunk to the online store."""
    from feast import FeatureStore
    df = feature_rows(ds)
    path = HISTORY.removeprefix("s3a://")
    exists = s3().get_file_info(path).type != pafs.FileType.NotFound
    history = pd.concat([read_parquet(HISTORY), df]) if exists else df
    # ponytail: re-ingesting the same document version keeps one row per (chunk, version, text): the newest
    history = history.drop_duplicates(["chunk_id", "event_timestamp", "text"], keep="last")
    write_parquet(history, HISTORY)
    store = FeatureStore(repo_path=os.environ["FEAST_KB_REPO"])
    store.write_to_online_store("policy_chunks", df)
    # ponytail: a section deleted from a document stays online until the online table is rebuilt (Feast has no
    # delete by entity); the bank's documents only grow between versions here.
    print(f"offline history {len(history)} rows ({HISTORY}); online: {len(df)} chunks written")
    return HISTORY


def check(name: str, success: bool, observed, element_count: int, unexpected: int = 0, **kwargs) -> dict:
    """One result in the Layer 1 report format (what publish_results reads and sends to DataHub)."""
    return {"table": TABLE, "platform": "feast", "expectation": name, "kwargs": kwargs, "success": bool(success),
            "observed_value": observed, "element_count": element_count, "unexpected_count": int(unexpected),
            "filter": "chunks of this run"}


def validate(ds: str) -> str:
    """Checks on this run's chunks, offline and online; the DAG's publish step fails the run if any failed."""
    from feast import FeatureStore
    df = feature_rows(ds)
    n = len(df)
    docs = sorted(df.document.unique())
    dims = df.embedding.map(len)
    short = (df.text.str.len() < 20).sum()
    online = FeatureStore(repo_path=os.environ["FEAST_KB_REPO"]).get_online_features(
        features=["policy_chunks:text"], entity_rows=[{"chunk_id": c} for c in df.chunk_id]).to_dict()
    mismatched = sum(o != t for o, t in zip(online["text"], df.text))
    results = [
        check("expect_table_row_count_to_be_between", n >= 20, n, n, min_value=20),
        check("expect_column_values_to_be_unique", df.chunk_id.is_unique, int(df.chunk_id.duplicated().sum()), n,
              int(df.chunk_id.duplicated().sum()), column="chunk_id"),
        check("expect_column_values_to_not_be_null", df.text.notna().all(), int(df.text.isna().sum()), n,
              int(df.text.isna().sum()), column="text"),
        check("expect_column_value_lengths_to_be_between", short == 0, int(short), n, int(short), column="text",
              min_value=20),
        check("expect_embedding_dimension_to_equal", (dims == EMBEDDING_DIM).all(), sorted(dims.unique().tolist()), n,
              int((dims != EMBEDDING_DIM).sum()), column="embedding", value=EMBEDDING_DIM),
        check("expect_every_document_to_have_chunks", len(docs) >= 5, docs, n, max(0, 5 - len(docs)), column="document",
              min_documents=5),
        check("expect_online_store_to_match_offline", mismatched == 0, f"{n - mismatched}/{n} chunks identical online",
              n, mismatched, column="text"),
    ]
    key = f"lakehouse/_validation/rag_ingest/ds={ds}/results.json"
    with s3().open_output_stream(key) as f:
        f.write(json.dumps({"suite": "rag_ingest", "ds": ds, "results": results}, default=str).encode())
    for r in results:
        print(f"{'ok  ' if r['success'] else 'FAIL'} {r['expectation']:<44} observed={r['observed_value']}")
    return f"s3://{key}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--step", choices=["chunk", "embed", "write", "validate"], required=True)
    ap.add_argument("--ds", type=dt.date.fromisoformat, required=True)
    a = ap.parse_args()
    print({"chunk": chunk, "embed": embed, "write": write, "validate": validate}[a.step](str(a.ds)))
