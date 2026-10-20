"""Pure rules of the tools, no I/O, so they are tested on their own (boundaries, properties, mutations).

realtime_now  what the stored real-time counts mean now: Flink writes a count only when the card is used, so a
              count older than its window describes a past moment and is 0 now (the same rule as the model's
              on-demand view `fraud_request_features`)
rank          cosine similarity of the question to each candidate chunk, best first. Feast 0.66's
              retrieve_online_documents_v2 can attach one chunk's values to another chunk's id, so the server
              only takes candidate ids from it and ranks the chunks it reads by id itself
"""
import math
from datetime import datetime, timezone

WINDOW_S = {"auth_count_10m": 600, "amount_sum_1h": 3600, "distinct_merchants_1h": 3600, "distinct_units_1h": 3600}


def as_utc(value) -> datetime | None:
    """A Feast timestamp (datetime, naive = UTC, or epoch seconds) as an aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def realtime_now(values: dict, rt_ts, now: datetime) -> dict:
    """Counts as of `now`: kept while the last authorization is within the count's window, else 0.
    A timestamp ahead of `now` (replayed data) counts as age 0."""
    ts = as_utc(rt_ts)
    age = None if ts is None else max(0.0, (now - ts).total_seconds())
    out: dict[str, int | float | None] = {
        name: int(values[name]) if age is not None and age <= window and values.get(name) is not None else 0
        for name, window in WINDOW_S.items()}
    out["minutes_since_last_auth"] = None if age is None else round(age / 60, 1)
    return out


def cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity; 0 for a zero vector or vectors of different length."""
    if len(a) != len(b):
        return 0.0
    na, nb = math.sqrt(sum(x * x for x in a)), math.sqrt(sum(y * y for y in b))
    return 0.0 if na == 0 or nb == 0 else sum(x * y for x, y in zip(a, b)) / (na * nb)


def rank(query: list[float], embeddings: dict[str, list[float]], k: int) -> list[tuple[str, float]]:
    """The k ids most similar to the query, best first; equal scores ordered by id (the same order every call)."""
    if k < 1:
        raise ValueError("k must be at least 1")
    scored = [(chunk_id, round(cosine(query, vec), 6)) for chunk_id, vec in embeddings.items()]
    return sorted(scored, key=lambda s: (-s[1], s[0]))[:k]
