"""The scoring use case: features -> model -> decision, with timeouts and the fallback rule.

A failure or a timeout of the feature store or the model gives the fallback decision (flagged `fallback`), never
an error to the authorization switch. Every outcome is logged to Kafka for monitoring.

A/B test: with a challenger and ab_percent > 0, a stable hash of card_id picks the arm, so a card always meets the
same model. A failing challenger is answered by production (not by the fallback rule).
"""
import asyncio
import logging
import time
import zlib

from prometheus_client import Counter, Histogram

from app.clients import FeatureSource, ModelSource, PredictionLog
from app.policy import FallbackPolicy, ThresholdPolicy
from app.schemas import ScoreRequest, ScoreResponse

log = logging.getLogger(__name__)
DECISIONS = Counter("fraud_decisions_total", "Decisions by outcome", ["decision", "fallback"])
FALLBACKS = Counter("fraud_fallbacks_total", "Fallback decisions by cause", ["cause"])
SCORES = Histogram("fraud_score", "Model scores", buckets=[0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 0.8, 1.0])
STAGE = Histogram("fraud_stage_seconds", "Latency per stage", ["stage"],
                  buckets=[0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25])
CHALLENGER_FAILURES = Counter("fraud_challenger_failures_total", "Challenger calls answered by production", ["cause"])


def ab_arm(card_id: str, ab_percent: int) -> str:
    """Sticky arm of a card: crc32 is stable across processes and restarts (Python's hash() is salted)."""
    return "challenger" if zlib.crc32(card_id.encode()) % 100 < ab_percent else "production"


class FraudScorer:
    """Orchestrates one request; the collaborators are injected (fakes in tests, real clients in the app)."""

    def __init__(self, features: FeatureSource, model: ModelSource, policy: ThresholdPolicy,
                 fallback: FallbackPolicy, prediction_log: PredictionLog | None,
                 feature_timeout_s: float, model_timeout_s: float,
                 challenger: ModelSource | None = None, ab_percent: int = 0):
        """Keep the collaborators and the per-stage time budgets; no challenger means no A/B split."""
        self.features, self.model, self.policy, self.fallback = features, model, policy, fallback
        self.prediction_log = prediction_log
        self.feature_timeout_s, self.model_timeout_s = feature_timeout_s, model_timeout_s
        self.challenger, self.ab_percent = challenger, ab_percent if challenger else 0

    async def predict(self, feats: dict, arm: str, auth_id: str) -> tuple[float, str]:
        """The arm's model within the budget; a challenger that fails or is late hands over to production."""
        if arm == "challenger":
            try:
                return await asyncio.wait_for(self.challenger.predict(feats), self.model_timeout_s)
            except Exception as e:   # noqa: BLE001 - the experiment must not cost the payment its model decision
                cause = "timeout" if isinstance(e, TimeoutError) else "error"
                log.warning("challenger %s for %s: %r, production answers", cause, auth_id, e)
                CHALLENGER_FAILURES.labels(cause).inc()
        return await asyncio.wait_for(self.model.predict(feats), self.model_timeout_s)

    async def score(self, req: ScoreRequest) -> ScoreResponse:
        """Score one authorization; falls back instead of failing."""
        feats, cause, arm, start = None, None, ab_arm(req.card_id, self.ab_percent), time.perf_counter()
        policy = None
        try:
            t0 = time.perf_counter()
            feats = await asyncio.wait_for(self.features.features(req), self.feature_timeout_s)
            STAGE.labels("features").observe(time.perf_counter() - t0)
            t0 = time.perf_counter()
            score, version, thresholds = await self.predict(feats, arm, req.auth_id)
            STAGE.labels("model").observe(time.perf_counter() - t0)
            # each model with its own cut-offs (set on its validation); the configured ones for a model without them
            policy = ThresholdPolicy(*thresholds) if thresholds else self.policy
            resp = ScoreResponse(auth_id=req.auth_id, score=score, decision=policy.decide(score),
                                 model_version=version)
            SCORES.observe(score)
        except Exception as e:   # noqa: BLE001 - any failure of a dependency means: fall back
            cause = "features" if feats is None else "model"
            cause += "_timeout" if isinstance(e, TimeoutError) else "_error"
            log.warning("fallback (%s) for %s: %r", cause, req.auth_id, e)
            FALLBACKS.labels(cause).inc()
            resp = ScoreResponse(auth_id=req.auth_id, decision=self.fallback.decide(req.amount_vnd),
                                 model_version="fallback", fallback=True)
        DECISIONS.labels(resp.decision.value, str(resp.fallback).lower()).inc()
        if self.prediction_log:
            record = {**req.model_dump(mode="json"), "features": feats, "fallback_cause": cause, "ab_arm": arm,
                      "latency_ms": round((time.perf_counter() - start) * 1000, 1),   # features + model, for A/B
                      "t_low": policy and policy.t_low, "t_high": policy and policy.t_high}   # None: the amount rule
            record |= resp.model_dump(mode="json")   # pragma: no mutate (every response field is JSON-native anyway)
            await self.prediction_log.send(record)
        return resp
