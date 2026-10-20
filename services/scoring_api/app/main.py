"""FastAPI app: POST /v1/score, health probes, Prometheus-format metrics.

  /healthz  liveness: the process serves requests (never checks dependencies: a Redis outage must not restart it)
  /readyz   readiness: feature store and model answer (compose healthcheck)
  /metrics  request rate / latency / errors (instrumentator) + decisions, fallbacks, scores, stage latency

Usage: uvicorn app.main:app --host 0.0.0.0 --port 8000
"""
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from app.clients import FeatureClient, ModelClient, PredictionLog
from app.policy import FallbackPolicy, ThresholdPolicy
from app.schemas import ScoreRequest, ScoreResponse
from app.scorer import FraudScorer
from app.settings import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def build_scorer(s: Settings) -> FraudScorer:
    """Wire the real clients from the settings."""
    return FraudScorer(FeatureClient(s.feature_repo, s.feature_service), ModelClient(s.model_url, s.model_timeout_s),
                       ThresholdPolicy(s.t_low, s.t_high), FallbackPolicy(s.fallback_step_up_vnd),
                       PredictionLog(s.kafka_bootstrap, s.prediction_topic), s.feature_timeout_s, s.model_timeout_s)


def create_app(scorer_factory=build_scorer, settings: Settings | None = None, registry=REGISTRY) -> FastAPI:
    """App factory; tests pass a factory that returns a scorer with fake clients, and their own metrics registry
    (the HTTP metrics cannot be registered twice in one registry)."""
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Build the scorer once per process; start / stop the Kafka producer."""
        app.state.scorer = scorer_factory(settings)
        if app.state.scorer.prediction_log:
            await app.state.scorer.prediction_log.start()
        yield
        if app.state.scorer.prediction_log:
            await app.state.scorer.prediction_log.stop()

    app = FastAPI(title="Fraud Scoring API", version="1.0.0", lifespan=lifespan)
    Instrumentator(excluded_handlers=["/healthz", "/readyz", "/metrics"], registry=registry).instrument(app)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        """This process's registry. Not Instrumentator.expose(): `import feast` sets PROMETHEUS_MULTIPROC_DIR, and
        expose() then reads that (empty) multiprocess directory instead: /metrics answered 200 with no body."""
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    def scorer(request: Request) -> FraudScorer:
        """Dependency: the process-wide scorer."""
        return request.app.state.scorer

    @app.post("/v1/score", response_model=ScoreResponse)
    async def score(req: ScoreRequest, s: FraudScorer = Depends(scorer)) -> ScoreResponse:
        """Score one card authorization."""
        return await s.score(req)

    @app.get("/healthz")
    async def healthz() -> dict:
        """Liveness."""
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(s: FraudScorer = Depends(scorer)) -> dict:
        """Readiness: both dependencies answer."""
        try:
            ok = await s.features.ready() and await s.model.ready()
        except Exception as e:   # noqa: BLE001
            raise HTTPException(503, f"not ready: {e!r}") from e
        if not ok:
            raise HTTPException(503, "not ready")
        return {"status": "ready"}

    return app


app = create_app()
