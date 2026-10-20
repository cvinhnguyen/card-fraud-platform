"""Tool inputs and outputs. Pydantic rejects a malformed id or question before any store is read (REST: 422,
MCP: a tool error), so an agent cannot use a tool to scan the stores with arbitrary strings."""
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field

CARD_ID = r"^[A-Za-z0-9_-]{1,64}$"
CHUNK_ID = r"^[a-z_]{1,40}#\d{1,3}(\.\d{1,3})?$"   # <document>#<section>, e.g. card_terms#4.2 or faq#3


class Channel(str, Enum):
    """How the card is used."""
    ecom = "ecom"
    pos = "pos"
    atm = "atm"


class Profile(BaseModel):
    """Card attributes (feature view card_profile)."""
    card_type: str | None = None
    network: str | None = None
    card_issue_ts: datetime | None = None


class DailyStats(BaseModel):
    """History of the card up to its last active day (feature view card_txn_stats_daily)."""
    txn_count_30d: int | None = None
    distinct_merchants_30d: int | None = None
    avg_amount_90d: float | None = None
    std_amount_90d: float | None = None
    ecom_ratio_90d: float | None = None
    foreign_txn_ratio_90d: float | None = None
    dispute_count_180d: int | None = None
    last_active_day: datetime | None = None   # stats_ts: the night the stats were written


class Realtime(BaseModel):
    """Recent authorizations (feature view card_auth_realtime), aged to now: a count older than its window is 0."""
    auth_count_10m: int = 0
    amount_sum_1h: int = 0
    distinct_merchants_1h: int = 0
    distinct_units_1h: int = 0     # different provinces / cities within the hour
    minutes_since_last_auth: float | None = None


class CardData(BaseModel):
    """Everything the online feature store knows about one card."""
    card_id: str
    found: bool
    profile: Profile | None = None
    daily: DailyStats | None = None
    realtime: Realtime | None = None


class Chunk(BaseModel):
    """One chunk of a bank document (feature view policy_chunks)."""
    chunk_id: str
    document: str
    section: str
    text: str


class SearchRequest(BaseModel):
    """A customer question to find the matching policy text for."""
    question: str = Field(min_length=3, max_length=500)
    k: int = Field(4, ge=1, le=10)


class SearchHit(Chunk):
    """A chunk and its cosine similarity to the question."""
    score: float


class SearchResult(BaseModel):
    """Most similar chunks first."""
    hits: list[SearchHit]


class ScorePaymentRequest(BaseModel):
    """A payment to score: the one the customer asks about, as they describe it."""
    card_id: str = Field(pattern=CARD_ID)
    customer_id: str = Field("unknown", pattern=CARD_ID)   # only logged by the Scoring API, not a model feature
    amount_vnd: int = Field(gt=0, le=10_000_000_000)
    channel: Channel
    currency: str = Field("VND", pattern=r"^[A-Z]{3}$")
    device_id: str | None = Field(None, pattern=CARD_ID)


class ScoreResult(BaseModel):
    """Fraud probability and the decision the Scoring API would take."""
    score: float | None
    decision: str
    fallback: bool
    model_version: str
