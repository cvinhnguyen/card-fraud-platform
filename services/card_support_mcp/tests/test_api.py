"""The three servers of the real app over fake adapters: each serves only its own tools (REST and MCP), REST status
codes (200 / 404 / 422 / 503), probes that check only the server's own dependencies, metrics, and the MCP endpoint
spoken as JSON-RPC the way kagent's agents call it (tools/list, tools/call)."""
import pytest
from prometheus_client import CollectorRegistry

from app.main import SERVERS, build_service, create_app
from app.settings import Settings
from tests.conftest import FakeEmbedder, FakeFeatures, FakeKnowledge, FakeScoring, make_service

MCP_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
PAYMENT = {"card_id": "card_00001", "customer_id": "cus_00001", "amount_vnd": 4_500_000, "channel": "pos"}
FAKES = {"features": FakeFeatures, "knowledge": FakeKnowledge, "embedder": FakeEmbedder, "scoring": FakeScoring}


def rpc(client, method, params=None):
    """One JSON-RPC call to /mcp (stateless server: no initialize handshake needed)."""
    r = client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}},
                    headers=MCP_HEADERS)
    assert r.status_code == 200, r.text
    return r.json()["result"]


def call(client, tool, **arguments):
    return rpc(client, "tools/call", {"name": tool, "arguments": arguments})


# REST

def test_card_found_unknown_and_malformed(client):
    assert client.get("/v1/cards/card_00001").json()["realtime"]["auth_count_10m"] == 3
    assert client.get("/v1/cards/card_99999").status_code == 404
    assert client.get("/v1/cards/card.1").status_code == 422


def test_chunk_found_unknown_and_malformed(client):
    assert client.get("/v1/chunks/faq%233").json()["text"] == "Lock your card."   # %23 = '#'
    assert client.get("/v1/chunks/faq%2399").status_code == 404
    assert client.get("/v1/chunks/FAQ%233").status_code == 422


def test_search(make_client):
    c = make_client(server="policy-rag")
    hits = c.post("/v1/search", json={"question": "someone used my card", "k": 2}).json()["hits"]
    assert [h["chunk_id"] for h in hits] == ["card_terms#4.2", "faq#3"]
    assert c.post("/v1/search", json={"question": "fees?", "k": 11}).status_code == 422


def test_score(make_client):
    c = make_client(server="fraud-score")
    assert c.post("/v1/score", json=PAYMENT).json()["decision"] == "decline"
    assert c.post("/v1/score", json=PAYMENT | {"amount_vnd": 0}).status_code == 422


@pytest.mark.parametrize("server, method, path", [
    ("card-data", "POST", "/v1/score"), ("card-data", "POST", "/v1/search"),
    ("fraud-score", "GET", "/v1/cards/card_00001"), ("fraud-score", "POST", "/v1/search"),
    ("policy-rag", "GET", "/v1/chunks/faq%233"), ("policy-rag", "POST", "/v1/score"),
])
def test_a_server_has_no_routes_of_the_others(make_client, server, method, path):
    assert make_client(server=server).request(method, path, json=PAYMENT).status_code == 404


@pytest.mark.parametrize("server, fake, method, path, body", [
    ("card-data", {"features": FakeFeatures(error=ConnectionError())}, "GET", "/v1/cards/card_00001", None),
    ("policy-rag", {"embedder": FakeEmbedder(error=TimeoutError())}, "POST", "/v1/search", {"question": "fees?"}),
    ("fraud-score", {"scoring": FakeScoring(error=ConnectionError())}, "POST", "/v1/score", PAYMENT),
])
def test_failing_dependency_is_503(make_client, server, fake, method, path, body):
    r = make_client(make_service(**fake), server).request(method, path, json=body)
    assert r.status_code == 503 and r.json()["detail"].endswith(f"unavailable: {type(next(iter(fake.values())).error).__name__}")


@pytest.mark.parametrize("server, broken", [
    ("card-data", "features"), ("card-data", "knowledge"), ("fraud-score", "scoring"),
    ("policy-rag", "knowledge"), ("policy-rag", "embedder"),
])
def test_probes(make_client, server, broken):
    up = make_client(server=server)
    assert up.get("/healthz").json() == {"status": "ok"}
    assert up.get("/readyz").json() == {"status": "ready"}
    down = make_client(make_service(**{broken: FAKES[broken](error=ConnectionError())}), server)
    assert down.get("/healthz").status_code == 200          # liveness never checks dependencies
    r = down.get("/readyz")
    assert r.status_code == 503 and r.json()["detail"] == f"not ready: {broken} ConnectionError"


def test_readiness_checks_only_the_servers_own_dependencies(make_client):
    stores_down = make_service(features=FakeFeatures(error=ConnectionError()),
                               knowledge=FakeKnowledge(error=ConnectionError()))
    assert make_client(stores_down, "fraud-score").get("/readyz").status_code == 200   # it reads no Feast store


def test_a_dependency_that_answers_not_ok_is_not_ready(make_client):
    class Unhealthy(FakeEmbedder):
        async def ready(self):
            return False   # Embedder.ready: /health answered, but not 200
    r = make_client(make_service(embedder=Unhealthy()), "policy-rag").get("/readyz")
    assert r.status_code == 503 and r.json()["detail"] == "not ready: embedder"


def test_metrics_count_tool_calls_by_outcome(make_client):
    c = make_client(server="card-data")
    c.get("/v1/cards/card_00001")
    s = make_client(make_service(scoring=FakeScoring(error=ConnectionError())), "fraud-score")
    s.post("/v1/score", json=PAYMENT)
    assert 'card_support_tool_calls_total{outcome="ok",tool="get_card_data"} 1.0' in c.get("/metrics").text
    text = s.get("/metrics").text
    assert 'card_support_tool_calls_total{outcome="error",tool="score_payment"} 1.0' in text
    assert 'card_support_tool_seconds_count{tool="score_payment"} 1.0' in text


# MCP

@pytest.mark.parametrize("server", SERVERS)
def test_mcp_lists_only_the_servers_tools(make_client, server):
    assert {t["name"] for t in rpc(make_client(server=server), "tools/list")["tools"]} == SERVERS[server][0]


def test_mcp_tool_schemas_carry_the_input_rules(make_client):
    data = {t["name"]: t for t in rpc(make_client(server="card-data"), "tools/list")["tools"]}
    assert data["get_card_data"]["inputSchema"]["properties"]["card_id"]["pattern"]
    rag = {t["name"]: t for t in rpc(make_client(server="policy-rag"), "tools/list")["tools"]}
    assert rag["search_policy"]["inputSchema"]["properties"]["k"]["maximum"] == 10


def test_mcp_tool_calls(make_client):
    c = make_client(server="card-data")
    card = call(c, "get_card_data", card_id="card_00001")
    assert not card["isError"] and card["structuredContent"]["found"] is True
    chunk = call(c, "get_policy_chunk", chunk_id="faq#3")
    assert "Lock your card." in chunk["content"][0]["text"]
    hits = call(make_client(server="policy-rag"), "search_policy", question="foreign fee?", k=1)["structuredContent"]["hits"]
    assert len(hits) == 1
    score = call(make_client(server="fraud-score"), "score_payment", **PAYMENT)["structuredContent"]
    assert score["decision"] == "decline" and score["fallback"] is False


def test_mcp_tool_of_another_server_is_an_error(make_client):
    assert call(make_client(server="card-data"), "score_payment", **PAYMENT)["isError"] is True


@pytest.mark.parametrize("server, tool, arguments", [
    ("card-data", "get_card_data", {"card_id": "x; DROP"}),
    ("card-data", "get_policy_chunk", {"chunk_id": "../etc"}),
    ("policy-rag", "search_policy", {"question": "hi"}),
    ("fraud-score", "score_payment", PAYMENT | {"amount_vnd": -5}),
    ("fraud-score", "score_payment", PAYMENT | {"channel": "wire"}),
])
def test_mcp_rejects_bad_arguments_as_tool_errors(make_client, server, tool, arguments):
    assert call(make_client(server=server), tool, **arguments)["isError"] is True


def test_mcp_failing_dependency_is_a_tool_error(make_client):
    c = make_client(make_service(scoring=FakeScoring(error=TimeoutError())), "fraud-score")
    result = call(c, "score_payment", **PAYMENT)
    assert result["isError"] is True and "score_payment unavailable: TimeoutError" in result["content"][0]["text"]


# Wiring

def test_the_server_comes_from_the_settings():
    app = create_app(None, lambda s: make_service(), Settings(mcp_server="policy-rag"), registry=CollectorRegistry())
    assert app.title == "Card support MCP server: policy-rag"


@pytest.mark.parametrize("server, built", [
    ("card-data", {"FeatureReader", "KnowledgeBase"}),
    ("fraud-score", {"ScoringClient"}),
    ("policy-rag", {"KnowledgeBase", "Embedder"}),
])
def test_build_service_wires_only_what_the_tools_use(monkeypatch, server, built):
    import app.main as main
    for name in ("FeatureReader", "KnowledgeBase", "Embedder", "ScoringClient"):   # no Feast repo, no HTTP client
        monkeypatch.setattr(main, name, type(name, (), {"__init__": lambda self, *a: None}))
    svc = build_service(Settings(mcp_server=server))
    assert {type(a).__name__ for a in (svc.features, svc.knowledge, svc.embedder, svc.scoring) if a} == built
