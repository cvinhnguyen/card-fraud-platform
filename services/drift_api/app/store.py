"""Live histograms in Redis, shared by every replica: one hash per minute bucket and input, bin -> count.

  drift:<minute>:<input>  HINCRBY bin 1   (expires after bucket_ttl_s)
A replica can be added or killed at any time: no state lives in the process.
"""
import time

from app.psi import bin_of

SCORE = "score"


class HistogramStore:
    """Writes events into minute buckets; reads the sum of the last `window` minutes."""

    def __init__(self, redis, edges: dict[str, list[float]], ttl_s: int, clock=time.time):
        """`edges`: reference quantile edges per input (features and `score`)."""
        self.redis, self.edges, self.ttl_s, self.clock = redis, edges, ttl_s, clock

    def _minute(self, offset: int = 0) -> int:
        """The current minute bucket (epoch minutes), minus offset minutes."""
        return int(self.clock()) // 60 - offset

    async def add(self, rows: list[dict[str, float | None]]) -> int:
        """Count each row's inputs into the current minute; returns the number of rows.
        A row carries only the inputs its event knows: the features of a request, or the score of a response."""
        minute, pipe = self._minute(), self.redis.pipeline(transaction=False)   # pragma: no mutate (MULTI or not: same counts)
        for row in rows:
            for name, value in row.items():
                key = f"drift:{minute}:{name}"
                pipe.hincrby(key, bin_of(value, self.edges[name]), 1)
                pipe.expire(key, self.ttl_s)
        if rows:
            await pipe.execute()
        return len(rows)

    async def window(self, minutes: int) -> dict[str, dict[str, int]]:
        """input -> bin -> count over the last `minutes` minutes (current minute included)."""
        pipe = self.redis.pipeline(transaction=False)   # pragma: no mutate (MULTI or not: same result)
        keys = [(name, f"drift:{self._minute(i)}:{name}") for name in self.edges for i in range(minutes)]
        for _, key in keys:
            pipe.hgetall(key)
        out: dict[str, dict[str, int]] = {name: {} for name in self.edges}
        for (name, _), h in zip(keys, await pipe.execute()):
            for b, c in h.items():
                out[name][b.decode()] = out[name].get(b.decode(), 0) + int(c)   # redis-py without decode_responses: bytes
        return out
