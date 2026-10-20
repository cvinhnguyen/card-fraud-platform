"""The four tools as one service class, shared by the REST API and the MCP server.

  card_data   live card data from the fraud model's online store (profile, daily history, recent activity)
  chunk       one bank document chunk by id
  search      the policy chunks most similar to a question (RAG retrieval)
  score       fraud score of a payment the customer describes, from the Fraud Scoring API (ML inference)
"""
import uuid
from datetime import datetime, timezone
from typing import Callable, Protocol

from app import logic
from app.schemas import (CardData, Chunk, DailyStats, Profile, Realtime, ScorePaymentRequest, ScoreResult,
                         SearchHit, SearchResult)

AGENT_MERCHANT = "agent-what-if"   # marks the agent's scoring requests in the prediction log


class Features(Protocol):
    """Anything that returns one card's features."""
    async def card(self, card_id: str) -> dict: ...


class Knowledge(Protocol):
    """Anything that returns chunks by id and candidate ids by vector."""
    async def chunks(self, ids: list[str]) -> dict[str, dict | None]: ...
    async def candidates(self, vector: list[float], n: int) -> list[str]: ...


class QueryEmbedder(Protocol):
    """Anything that turns a question into a vector."""
    async def embed_query(self, question: str) -> list[float]: ...


class Scoring(Protocol):
    """Anything that scores an authorization."""
    async def score(self, body: dict) -> dict: ...


class CardSupportService:
    """The tools' logic; the stores and APIs are injected (dependency injection: fakes in tests)."""

    def __init__(self, features: Features, knowledge: Knowledge, embedder: QueryEmbedder, scoring: Scoring,
                 candidate_factor: int = 3, clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        """Wire the adapters; `clock` is injectable so the freshness rule is tested at fixed times."""
        self.features, self.knowledge, self.embedder, self.scoring = features, knowledge, embedder, scoring
        self.candidate_factor, self.clock = candidate_factor, clock

    async def card_data(self, card_id: str) -> CardData:
        """Profile, daily stats and real-time counts aged to now; found=False when the store knows nothing."""
        v = await self.features.card(card_id)
        if all(x is None for x in v.values()):
            return CardData(card_id=card_id, found=False)
        daily = {k: v.get(k) for k in DailyStats.model_fields if k != "last_active_day"}
        return CardData(
            card_id=card_id, found=True,
            profile=Profile(**{k: v.get(k) for k in Profile.model_fields}),
            daily=DailyStats(**daily, last_active_day=logic.as_utc(v.get("stats_ts"))),
            realtime=Realtime(**logic.realtime_now(v, v.get("rt_ts"), self.clock())))

    async def chunk(self, chunk_id: str) -> Chunk | None:
        """The chunk, or None if no chunk has this id."""
        row = (await self.knowledge.chunks([chunk_id])).get(chunk_id)
        return None if row is None else Chunk(chunk_id=chunk_id, document=row["document"], section=row["section"],
                                              text=row["text"])

    async def search(self, question: str, k: int) -> SearchResult:
        """Embed the question, take k x candidate_factor candidate ids, read them by id, rank by cosine."""
        vector = await self.embedder.embed_query(question)
        ids = await self.knowledge.candidates(vector, k * self.candidate_factor)
        rows = {i: r for i, r in (await self.knowledge.chunks(ids)).items() if r is not None}
        ranked = logic.rank(vector, {i: r["embedding"] for i, r in rows.items()}, k)
        return SearchResult(hits=[SearchHit(chunk_id=i, document=rows[i]["document"], section=rows[i]["section"],
                                            text=rows[i]["text"], score=s) for i, s in ranked])

    async def score(self, req: ScorePaymentRequest) -> ScoreResult:
        """Score the payment as a new authorization now; the request is marked as the agent's in the log."""
        body = {"auth_id": f"agent-{uuid.uuid4().hex[:16]}", "card_id": req.card_id, "customer_id": req.customer_id,
                "merchant_id": AGENT_MERCHANT, "amount_vnd": req.amount_vnd, "channel": req.channel.value,
                "currency": req.currency, "device_id": req.device_id, "auth_ts": self.clock().isoformat()}
        r = await self.scoring.score(body)
        return ScoreResult(score=r.get("score"), decision=r["decision"], fallback=r.get("fallback", False),
                           model_version=str(r.get("model_version", "unknown")))
