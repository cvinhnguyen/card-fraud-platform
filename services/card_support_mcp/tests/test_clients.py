"""The adapters: HTTP ones against respx mocks, Feast ones against a stand-in store (no Redis / Postgres)."""
import httpx
import pandas as pd
import pytest
import respx

from app.clients import QUERY_PREFIX, Embedder, FeatureReader, KnowledgeBase, ScoringClient

EMB_URL = "http://embed:8086"
SCORE_URL = "http://scoring-api:8000/v1/score"


@respx.mock
async def test_embedder_sends_instruction_and_key():
    route = respx.post(f"{EMB_URL}/v1/embeddings").respond(json={"data": [{"embedding": [0.1, 0.2]}]})
    assert await Embedder(EMB_URL, "k3y", 1.0).embed_query("lost card") == [0.1, 0.2]
    req = route.calls.last.request
    assert req.headers["authorization"] == "Bearer k3y"
    assert b'"input":["' + QUERY_PREFIX.replace("\n", "\\n").encode() + b'lost card"]' in req.content


@respx.mock
async def test_embedder_errors_and_health():
    respx.post(f"{EMB_URL}/v1/embeddings").respond(500)
    respx.get(f"{EMB_URL}/health").respond(200)
    emb = Embedder(EMB_URL, "", 1.0)
    with pytest.raises(httpx.HTTPStatusError):
        await emb.embed_query("lost card")
    assert await emb.ready() is True


@respx.mock
async def test_scoring_client():
    route = respx.post(SCORE_URL).respond(json={"decision": "approve"})
    assert await ScoringClient(SCORE_URL, 1.0).score({"auth_id": "a"}) == {"decision": "approve"}
    assert route.calls.last.request.content == b'{"auth_id":"a"}'
    respx.post(SCORE_URL).respond(503)
    with pytest.raises(httpx.HTTPStatusError):
        await ScoringClient(SCORE_URL, 1.0).score({})


@respx.mock
async def test_scoring_client_health_is_the_apis_liveness():
    route = respx.get("http://scoring-api:8000/healthz").respond(200)
    assert await ScoringClient(SCORE_URL, 1.0).ready() is True
    route.respond(500)
    assert await ScoringClient(SCORE_URL, 1.0).ready() is False


class Online:
    """Stand-in for Feast's OnlineResponse."""

    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return self.data

    def to_df(self):
        return pd.DataFrame(self.data)


class FakeStore:
    """Stand-in for feast.FeatureStore: records what is asked, answers fixed rows."""

    def __init__(self):
        self.asked = []

    async def get_online_features_async(self, features, entity_rows):
        self.asked.append((features, entity_rows))
        return Online({"card_id": ["card_1"], "network": ["visa"], "auth_count_10m": [2]})

    def get_online_features(self, features, entity_rows):
        ids = [r["chunk_id"] for r in entity_rows]
        known = {"faq#3": ("faq", "3", "Lock your card.", [1.0])}
        cols = {f: [known.get(i, (None,) * 4)[n] for i in ids] for n, f in
                enumerate(["document", "section", "text", "embedding"])}
        return Online({"chunk_id": ids} | cols)

    def retrieve_online_documents_v2(self, features, query, top_k, distance_metric):
        self.asked.append((features, top_k, distance_metric))
        return Online({"chunk_id": ["faq#3", None, "fee_schedule#2"]})


def adapter(cls):
    """The adapter around a FakeStore, without opening a Feast repo."""
    a = cls.__new__(cls)
    a.store = FakeStore()
    if cls is FeatureReader:
        a.features = ["card_profile:network", "card_auth_realtime:auth_count_10m"]
    return a


async def test_feature_reader_drops_the_entity_column():
    reader = adapter(FeatureReader)
    assert await reader.card("card_1") == {"network": "visa", "auth_count_10m": 2}
    assert reader.store.asked[0][1] == [{"card_id": "card_1"}]
    assert await reader.ready() is True


async def test_knowledge_base_chunks_and_candidates():
    kb = adapter(KnowledgeBase)
    got = await kb.chunks(["faq#3", "faq#99"])
    assert got == {"faq#3": {"document": "faq", "section": "3", "text": "Lock your card.", "embedding": [1.0]},
                   "faq#99": None}
    assert await kb.chunks([]) == {}
    assert await kb.candidates([1.0], 6) == ["faq#3", "fee_schedule#2"]   # null ids dropped
    assert kb.store.asked[-1] == (["policy_chunks:section"], 6, "cosine")
    assert await kb.ready() is True
