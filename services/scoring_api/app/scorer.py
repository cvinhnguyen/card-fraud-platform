"""The scoring use case: features -> model -> decision, with timeouts and the fallback rule.

A failure or a timeout of the feature store or the model gives the fallback decision (flagged `fallback`), never
an error to the authorization switch. Every outcome is logged to Kafka for monitoring.
"""
import asyncio
import logging
import time

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


class FraudScorer:
    """Orchestrates one request; the collaborators are injected (fakes in tests, real clients in the app)."""

    def __init__(self, features: FeatureSource, model: ModelSource, policy: ThresholdPolicy,
                 fallback: FallbackPolicy, prediction_log: PredictionLog | None,
                 feature_timeout_s: float, model_timeout_s: float):
        """Keep the collaborators and the per-stage time budgets."""
        self.features, self.model, self.policy, self.fallback = features, model, policy, fallback
        self.prediction_log = prediction_log
        self.feature_timeout_s, self.model_timeout_s = feature_timeout_s, model_timeout_s

    async def score(self, req: ScoreRequest) -> ScoreResponse:
        """Score one authorization; falls back instead of failing."""
        feats, cause = None, None
        try:
            t0 = time.perf_counter()
            feats = await asyncio.wait_for(self.features.features(req), self.feature_timeout_s)
            STAGE.labels("features").observe(time.perf_counter() - t0)
            t0 = time.perf_counter()
            score, version = await asyncio.wait_for(self.model.predict(feats), self.model_timeout_s)
            STAGE.labels("model").observe(time.perf_counter() - t0)
            resp = ScoreResponse(auth_id=req.auth_id, score=score, decision=self.policy.decide(score),
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
            record = {**req.model_dump(mode="json"), "features": feats, "fallback_cause": cause}
            record |= resp.model_dump(mode="json")   # pragma: no mutate (every response field is JSON-native anyway)
            await self.prediction_log.send(record)
        return resp
