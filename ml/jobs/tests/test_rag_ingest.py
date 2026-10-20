"""RAG ingest steps: chunking of the bank's documents, embedding calls, feature rows, report format."""
import io
import json
import pathlib

import pandas as pd

import rag_ingest

DOCS = pathlib.Path(__file__).parent / "knowledge"   # generator/knowledge, copied in by the image's test stage
TERMS = """# Card Terms

Version 2026-01-01

## 4. Liability
4.1 You report a lost card at once.
4.2 You are not liable after reporting.

## 5. Fees
| Fee | Amount |
| Foreign | 2.5% |
"""


def test_split_document_clauses_and_sections():
    chunks = rag_ingest.split_document("card_terms", TERMS)
    assert [c["chunk_id"] for c in chunks] == ["card_terms#4.1", "card_terms#4.2", "card_terms#5"]
    assert chunks[1]["text"] == "Card Terms > 4. Liability\nYou are not liable after reporting."   # readable alone
    assert chunks[2]["section"] == "5" and "Foreign | 2.5%" in chunks[2]["text"]                  # whole table
    assert {c["version"] for c in chunks} == {"2026-01-01"}


def test_split_document_unnumbered_sections_are_numbered_in_order():
    faq = "# FAQ\n\nVersion 2026-02-01\n\n## How do I lock my card?\nIn the app.\n\n## Can I dispute?\nYes.\n"
    assert [c["chunk_id"] for c in rag_ingest.split_document("faq", faq)] == ["faq#1", "faq#2"]


def test_the_bank_documents_give_57_unique_chunks():
    chunks = [c for p in sorted(DOCS.glob("*.md")) for c in rag_ingest.split_document(p.stem, p.read_text("utf-8"))]
    ids = [c["chunk_id"] for c in chunks]
    assert len(ids) == 57 and len(set(ids)) == 57
    assert {c["document"] for c in chunks} == {"authentication_rules", "card_terms", "dispute_policy", "faq",
                                              "fee_schedule"}
    assert all(len(c["text"]) >= 20 for c in chunks)


def test_embed_texts_keeps_input_order_across_batches(monkeypatch):
    monkeypatch.setenv("EMBEDDINGS_URL", "http://embed")
    monkeypatch.setenv("LLM_API_KEY", "k")
    calls = []

    def urlopen(req, timeout):
        texts = json.loads(req.data)["input"]
        calls.append((req.full_url, req.headers["Authorization"], texts))
        data = [{"index": i, "embedding": [float(len(t))]} for i, t in enumerate(texts)][::-1]   # server reorders
        return io.BytesIO(json.dumps({"data": data}).encode())

    monkeypatch.setattr(rag_ingest.urllib.request, "urlopen", urlopen)
    assert rag_ingest.embed_texts(["a", "bb", "ccc"], batch=2) == [[1.0], [2.0], [3.0]]
    assert [c[2] for c in calls] == [["a", "bb"], ["ccc"]]
    assert calls[0][:2] == ("http://embed/v1/embeddings", "Bearer k")


def test_feature_rows_event_time_is_the_document_version(monkeypatch):
    monkeypatch.setattr(rag_ingest, "read_parquet", lambda path: pd.DataFrame(
        {"chunk_id": ["faq#1"], "version": ["2026-02-01"], "text": ["x" * 30], "embedding": [[0.0]]}))
    df = rag_ingest.feature_rows("2026-10-20")
    assert df.event_timestamp.iloc[0] == pd.Timestamp("2026-02-01", tz="UTC") and "version" not in df
    assert str(df.created.dt.tz) == "UTC"


def test_staging_path_and_check_report_format():
    assert rag_ingest.staging("2026-10-20", "chunks") == "s3a://lakehouse/kb/staging/ds=2026-10-20/chunks.parquet"
    r = rag_ingest.check("expect_table_row_count_to_be_between", True, 57, 57, min_value=20)
    assert r == {"table": "card_support_kb.policy_chunks", "platform": "feast",
                 "expectation": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 20}, "success": True,
                 "observed_value": 57, "element_count": 57, "unexpected_count": 0, "filter": "chunks of this run"}
