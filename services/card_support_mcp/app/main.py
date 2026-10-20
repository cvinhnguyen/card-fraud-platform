"""Card support server: the same four tools as an MCP server (for the agents) and as a REST API (for people,
tests and the load test).

  /mcp                         MCP, streamable HTTP, stateless (any replica answers any call; no sticky sessions)
  GET  /v1/cards/{card_id}     live card data            (tool get_card_data)
  GET  /v1/chunks/{chunk_id}   one policy chunk          (tool get_policy_chunk)
  POST /v1/search              policy search, RAG        (tool search_policy)
  POST /v1/score               fraud score of a payment  (tool score_payment)
  /healthz  liveness: the process serves requests (never checks dependencies)
  /readyz   readiness: both Feast online stores answer
  /metrics  HTTP metrics + calls / failures / latency per tool

Usage: uvicorn app.main:app --host 0.0.0.0 --port 8000
"""
import logging
import time
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Path, Request, Response
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Counter, Histogram, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import Field

from app.clients import Embedder, FeatureReader, KnowledgeBase, ScoringClient
from app.schemas import (CARD_ID, CHUNK_ID, CardData, Channel, Chunk, ScorePaymentRequest, ScoreResult, SearchRequest,
                         SearchResult)
from app.service import CardSupportService
from app.settings import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)
INSTRUCTIONS = ("Card support tools of the bank. Card data and fraud scores are live; policy chunks are the bank's "
                "card documents. Never show internal feature names or thresholds to the customer.")


def build_service(s: Settings) -> CardSupportService:
    """Wire the real adapters from the settings."""
    return CardSupportService(FeatureReader(s.fraud_repo), KnowledgeBase(s.kb_repo),
                              Embedder(s.embeddings_url, s.llm_api_key, s.timeout_s),
                              ScoringClient(s.scoring_url, s.timeout_s), s.candidate_factor)


def create_app(service_factory=build_service, settings: Settings | None = None, registry=REGISTRY) -> FastAPI:
    """App factory; tests pass a factory with fake adapters and their own metrics registry."""
    settings = settings or Settings()
    calls = Counter("card_support_tool_calls_total", "Tool calls by tool and outcome", ["tool", "outcome"],
                    registry=registry)
    latency = Histogram("card_support_tool_seconds", "Tool latency", ["tool"], registry=registry,
                        buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
    # DNS-rebinding protection checks the Host header against localhost; callers here are the gateway and pods
    # using the Service name, and the server is reachable only inside the cluster behind the gateway.
    mcp = FastMCP("card-support", instructions=INSTRUCTIONS, stateless_http=True, json_response=True,
                  transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))

    async def run(tool: str, call):
        """Run one tool call, counting it by outcome and timing it (agent telemetry)."""
        start = time.perf_counter()
        try:
            result = await call
        except Exception as e:
            calls.labels(tool, "error").inc()
            log.exception("tool %s failed", tool)
            # what the agent reads: some errors (httpx.ReadTimeout) have an empty message
            raise RuntimeError(f"{tool} unavailable: {type(e).__name__}") from e
        finally:
            latency.labels(tool).observe(time.perf_counter() - start)
        calls.labels(tool, "ok").inc()
        return result

    @mcp.tool()
    async def get_card_data(card_id: Annotated[str, Field(pattern=CARD_ID)]) -> CardData:
        """Live data of the customer's card: profile, history of the last 30/90/180 days up to its last active day,
        and activity of the last hour (counts are 0 when the last authorization is older than their window)."""
        return await run("get_card_data", app.state.service.card_data(card_id))

    @mcp.tool()
    async def get_policy_chunk(chunk_id: Annotated[str, Field(pattern=CHUNK_ID)]) -> Chunk | None:
        """One section of the bank's card documents by id, e.g. card_terms#4.2 or faq#3 (None if unknown)."""
        return await run("get_policy_chunk", app.state.service.chunk(chunk_id))

    @mcp.tool()
    async def search_policy(question: Annotated[str, Field(min_length=3, max_length=500)],
                            k: Annotated[int, Field(ge=1, le=10)] = 4) -> SearchResult:
        """The sections of the bank's card documents (terms, disputes, fees, authentication rules, FAQ) that best
        match the question. Cite the document and section of the text you use."""
        return await run("search_policy", app.state.service.search(question, k))

    @mcp.tool()
    async def score_payment(card_id: str, amount_vnd: int, channel: Channel, customer_id: str = "unknown",
                            currency: str = "VND", device_id: str | None = None) -> ScoreResult:
        """Fraud score and decision (approve / step_up / decline) for a payment the customer describes, from the
        bank's fraud model. channel: ecom (online), pos (shop terminal) or atm. fallback=true means the model was
        unavailable and a safe rule decided."""
        req = ScorePaymentRequest(card_id=card_id, customer_id=customer_id, amount_vnd=amount_vnd, channel=channel,
                                  currency=currency, device_id=device_id)   # same validation as the REST body
        return await run("score_payment", app.state.service.score(req))

    mcp_app = mcp.streamable_http_app()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Build the service once per process; run the MCP session manager (stateless: no sessions are kept)."""
        app.state.service = service_factory(settings)
        async with mcp.session_manager.run():
            yield

    app = FastAPI(title="Card Support MCP", lifespan=lifespan)
    Instrumentator(registry=registry, excluded_handlers=["/metrics", "/healthz", "/readyz"]).instrument(app)

    async def rest(tool: str, call):
        """A tool call behind REST: a failing store or API becomes 503, not a stack trace."""
        try:
            return await run(tool, call)
        except RuntimeError as e:   # logged and counted in run()
            raise HTTPException(503, str(e)) from e

    @app.get("/v1/cards/{card_id}", response_model=CardData)
    async def card(card_id: Annotated[str, Path(pattern=CARD_ID)]) -> CardData:
        """Live card data; 404 when the online store knows nothing about the card."""
        data = await rest("get_card_data", app.state.service.card_data(card_id))
        if not data.found:
            raise HTTPException(404, "unknown card")
        return data

    @app.get("/v1/chunks/{chunk_id}", response_model=Chunk)
    async def chunk(chunk_id: Annotated[str, Path(pattern=CHUNK_ID)]) -> Chunk:
        """One policy chunk; 404 when no chunk has this id."""
        found = await rest("get_policy_chunk", app.state.service.chunk(chunk_id))
        if found is None:
            raise HTTPException(404, "unknown chunk")
        return found

    @app.post("/v1/search", response_model=SearchResult)
    async def search(req: SearchRequest) -> SearchResult:
        """Policy search (RAG retrieval)."""
        return await rest("search_policy", app.state.service.search(req.question, req.k))

    @app.post("/v1/score", response_model=ScoreResult)
    async def score(req: ScorePaymentRequest) -> ScoreResult:
        """Fraud score of a described payment."""
        return await rest("score_payment", app.state.service.score(req))

    @app.get("/healthz")
    async def healthz() -> dict:
        """Liveness."""
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(request: Request) -> dict:
        """Readiness: both Feast online stores answer (the embedder and the Scoring API fail per tool instead)."""
        svc = request.app.state.service
        try:
            await svc.features.ready()
            await svc.knowledge.ready()
        except Exception as e:   # noqa: BLE001
            raise HTTPException(503, f"not ready: {type(e).__name__}") from e
        return {"status": "ready"}

    @app.get("/metrics")
    async def metrics() -> Response:
        """Prometheus exposition of this app's registry."""
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    app.mount("/", mcp_app)   # last: everything the routes above do not match goes to MCP (/mcp)
    return app


app = create_app()
