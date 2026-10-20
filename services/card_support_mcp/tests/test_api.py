"""The real app over fake adapters: REST status codes (200 / 404 / 422 / 503), probes, metrics, and the MCP
endpoint spoken as JSON-RPC the way kagent's agents call it (tools/list, tools/call)."""
import pytest

from tests.conftest import FakeEmbedder, FakeFeatures, FakeScoring, make_service

MCP_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
PAYMENT = {"card_id": "card_00001", "customer_id": "cus_00001", "amount_vnd": 4_500_000, "channel": "pos"}


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


def test_search(client):
    hits = client.post("/v1/search", json={"question": "someone used my card", "k": 2}).json()["hits"]
    assert [h["chunk_id"] for h in hits] == ["card_terms#4.2", "faq#3"]
    assert client.post("/v1/search", json={"question": "fees?", "k": 11}).status_code == 422


def test_score(client):
    assert client.post("/v1/score", json=PAYMENT).json()["decision"] == "decline"
    assert client.post("/v1/score", json=PAYMENT | {"amount_vnd": 0}).status_code == 422


@pytest.mark.parametrize("fake, method, path, body", [
    ({"features": FakeFeatures(error=ConnectionError())}, "GET", "/v1/cards/card_00001", None),
    ({"embedder": FakeEmbedder(error=TimeoutError())}, "POST", "/v1/search", {"question": "fees?"}),
    ({"scoring": FakeScoring(error=ConnectionError())}, "POST", "/v1/score", PAYMENT),
])
def test_failing_dependency_is_503(make_client, fake, method, path, body):
    r = make_client(make_service(**fake)).request(method, path, json=body)
    assert r.status_code == 503 and r.json()["detail"].endswith(f"unavailable: {type(next(iter(fake.values())).error).__name__}")


def test_probes(client, make_client):
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}
    down = make_client(make_service(features=FakeFeatures(error=ConnectionError())))
    assert down.get("/healthz").status_code == 200          # liveness never checks dependencies
    assert down.get("/readyz").status_code == 503


def test_metrics_count_tool_calls_by_outcome(make_client):
    c = make_client(make_service(scoring=FakeScoring(error=ConnectionError())))
    c.get("/v1/cards/card_00001")
    c.post("/v1/score", json=PAYMENT)
    text = c.get("/metrics").text
    assert 'card_support_tool_calls_total{outcome="ok",tool="get_card_data"} 1.0' in text
    assert 'card_support_tool_calls_total{outcome="error",tool="score_payment"} 1.0' in text
    assert 'card_support_tool_seconds_count{tool="score_payment"} 1.0' in text


# MCP

def test_mcp_lists_the_four_tools_with_their_input_rules(client):
    tools = {t["name"]: t for t in rpc(client, "tools/list")["tools"]}
    assert set(tools) == {"get_card_data", "get_policy_chunk", "search_policy", "score_payment"}
    assert tools["get_card_data"]["inputSchema"]["properties"]["card_id"]["pattern"]
    assert tools["search_policy"]["inputSchema"]["properties"]["k"]["maximum"] == 10


def test_mcp_tool_calls(client):
    card = call(client, "get_card_data", card_id="card_00001")
    assert not card["isError"] and card["structuredContent"]["found"] is True
    hits = call(client, "search_policy", question="foreign fee?", k=1)["structuredContent"]["hits"]
    assert len(hits) == 1
    chunk = call(client, "get_policy_chunk", chunk_id="faq#3")
    assert "Lock your card." in chunk["content"][0]["text"]
    score = call(client, "score_payment", **PAYMENT)["structuredContent"]
    assert score["decision"] == "decline" and score["fallback"] is False


@pytest.mark.parametrize("tool, arguments", [
    ("get_card_data", {"card_id": "x; DROP"}),
    ("get_policy_chunk", {"chunk_id": "../etc"}),
    ("search_policy", {"question": "hi"}),
    ("score_payment", PAYMENT | {"amount_vnd": -5}),
    ("score_payment", PAYMENT | {"channel": "wire"}),
])
def test_mcp_rejects_bad_arguments_as_tool_errors(client, tool, arguments):
    assert call(client, tool, **arguments)["isError"] is True


def test_mcp_failing_dependency_is_a_tool_error(make_client):
    c = make_client(make_service(scoring=FakeScoring(error=TimeoutError())))
    result = call(c, "score_payment", **PAYMENT)
    assert result["isError"] is True and "score_payment unavailable: TimeoutError" in result["content"][0]["text"]
