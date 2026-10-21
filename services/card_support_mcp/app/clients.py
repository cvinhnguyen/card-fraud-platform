"""Adapters to the outside world, each small enough to be replaced by a fake in tests:

  FeatureReader   Feast project card_fraud, online store Redis: one card's features (async)
  KnowledgeBase   Feast project card_support_kb, online store Postgres/pgvector: chunks by id, candidate ids by
                  vector (Feast's postgres store has no async read: run in a worker thread)
  Embedder        OpenAI-compatible /v1/embeddings (llama.cpp, Qwen3-Embedding-0.6B)
  ScoringClient   Fraud Scoring API (Layer 2) POST /v1/score
"""
import asyncio

import httpx

PROFILE = ["card_type", "network", "card_issue_ts"]
DAILY = ["txn_count_30d", "distinct_merchants_30d", "avg_amount_90d", "std_amount_90d", "ecom_ratio_90d",
         "foreign_txn_ratio_90d", "dispute_count_180d", "stats_ts"]
REALTIME = ["auth_count_10m", "amount_sum_1h", "distinct_merchants_1h", "distinct_units_1h", "rt_ts"]
CHUNK_FIELDS = ["document", "section", "text", "embedding"]
# Qwen3-Embedding: documents are embedded as plain text (rag_ingest), questions with this instruction
QUERY_PREFIX = "Instruct: Given a customer question, retrieve the bank policy passage that answers it\nQuery: "


class FeatureReader:
    """The card's features from the fraud model's online store."""

    def __init__(self, repo: str):
        """Open the Feast repo (registry in Postgres, online store Redis)."""
        from feast import FeatureStore
        self.store = FeatureStore(repo_path=repo)
        self.features = ([f"card_profile:{f}" for f in PROFILE] + [f"card_txn_stats_daily:{f}" for f in DAILY]
                         + [f"card_auth_realtime:{f}" for f in REALTIME])

    async def card(self, card_id: str) -> dict:
        """Feature name -> value for one card (None where the store has nothing)."""
        resp = await self.store.get_online_features_async(features=self.features, entity_rows=[{"card_id": card_id}])
        return {name: values[0] for name, values in resp.to_dict().items() if name != "card_id"}

    async def ready(self) -> bool:
        """Registry loaded and Redis answering."""
        await self.card("_probe")
        return True


class KnowledgeBase:
    """Chunks of the bank's documents from the knowledge base's online store."""

    def __init__(self, repo: str):
        """Open the Feast repo (registry in Postgres, online store Postgres/pgvector)."""
        from feast import FeatureStore
        self.store = FeatureStore(repo_path=repo)

    def _chunks(self, ids: list[str]) -> dict[str, dict | None]:
        """Blocking Feast read of chunks by id (run in a thread by chunks)."""
        resp = self.store.get_online_features(features=[f"policy_chunks:{f}" for f in CHUNK_FIELDS],
                                              entity_rows=[{"chunk_id": i} for i in ids]).to_dict()
        return {i: None if resp["text"][n] is None else {f: resp[f][n] for f in CHUNK_FIELDS}
                for n, i in enumerate(resp["chunk_id"])}

    async def chunks(self, ids: list[str]) -> dict[str, dict | None]:
        """chunk_id -> {document, section, text, embedding}, or None for an unknown id."""
        return await asyncio.to_thread(self._chunks, ids) if ids else {}

    def _candidates(self, vector: list[float], n: int) -> list[str]:
        """Blocking Feast vector search for about n candidate chunk ids (run in a thread by candidates)."""
        # a stored feature must be asked for (the entity key chunk_id alone matches no row); section is the smallest
        df = self.store.retrieve_online_documents_v2(features=["policy_chunks:section"], query=vector, top_k=n,
                                                     distance_metric="cosine").to_df()
        return [str(i) for i in df["chunk_id"].dropna()]

    async def candidates(self, vector: list[float], n: int) -> list[str]:
        """Ids of about the n nearest chunks (order and attached values are not trusted, see logic.rank)."""
        return await asyncio.to_thread(self._candidates, vector, n)

    async def ready(self) -> bool:
        """Registry loaded and Postgres answering."""
        await self.chunks(["faq#1"])
        return True


class Embedder:
    """Question -> vector through the embedding server."""

    def __init__(self, url: str, api_key: str, timeout_s: float, client: httpx.AsyncClient | None = None):
        """One pooled HTTP client."""
        self.url = f"{url}/v1/embeddings"
        self.client = client or httpx.AsyncClient(timeout=timeout_s, headers={"Authorization": f"Bearer {api_key}"})

    async def embed_query(self, question: str) -> list[float]:
        """The question's vector, with the model's retrieval instruction in front."""
        r = await self.client.post(self.url, json={"model": "qwen3-embedding-0.6b", "input": [QUERY_PREFIX + question]})
        r.raise_for_status()
        return r.json()["data"][0]["embedding"]

    async def ready(self) -> bool:
        """The server answers its health endpoint."""
        r = await self.client.get(self.url.removesuffix("/v1/embeddings") + "/health")
        return r.status_code == 200


class ScoringClient:
    """The Layer 2 Fraud Scoring API: features from Feast + model on KServe + thresholds, with its rule fallback."""

    def __init__(self, url: str, timeout_s: float, client: httpx.AsyncClient | None = None):
        """One pooled HTTP client."""
        self.url = url
        self.client = client or httpx.AsyncClient(timeout=timeout_s)

    async def score(self, body: dict) -> dict:
        """The API's ScoreResponse for one authorization."""
        r = await self.client.post(self.url, json=body)
        r.raise_for_status()
        return r.json()
