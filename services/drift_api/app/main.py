"""Drift Detection API.

  POST /v1/events   prediction log events (a batch from ml/jobs/drift_forwarder.py, or one event) -> live histograms
  GET  /v1/drift    PSI of every feature and of the score over the last WINDOW_MINUTES vs the training profile
  /metrics          drift_psi{input=...} gauges (refreshed on every /v1/drift and scrape), events counter
  /healthz /readyz  liveness / readiness (Redis answers)

The reference profile (quantile edges + shares of each input on the validation period) is written by the training
pipeline with the model; services/predictor/fetch_model.py downloads it with the production model and compose
mounts it, so drift is always measured against the model in production.

Usage: uvicorn app.main:app --host 0.0.0.0 --port 8000
"""
import json
from contextlib import asynccontextmanager

from fastapi import Body, Depends, FastAPI, HTTPException, Request
from prometheus_client import REGISTRY, Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator

from app.psi import level, psi
from app.schemas import DriftReport, EventBatch, FeatureDrift, PredictionEvent
from app.settings import Settings
from app.store import HistogramStore

EVENTS = Counter("drift_events_total", "Prediction events counted")
PSI = Gauge("drift_psi", "PSI of an input over the live window vs training", ["input"])
LIVE = Gauge("drift_live_count", "Events in the live window")


def load_reference(path: str) -> dict:
    """{"model_version": ..., "inputs": {name: {"edges": [...], "shares": {bin: share}}}}."""
    with open(path) as f:
        return json.load(f)


def create_app(settings: Settings | None = None, redis_factory=None, reference: dict | None = None,
               registry=REGISTRY) -> FastAPI:
    """App factory; tests inject a fake Redis and an in-memory reference."""
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Load the reference profile, connect to Redis."""
        if redis_factory:
            r = redis_factory()
        else:
            from redis.asyncio import Redis
            r = Redis.from_url(settings.redis_url, password=settings.redis_password)
        ref = reference or load_reference(settings.reference_path)
        app.state.ref = ref
        app.state.redis = r
        app.state.store = HistogramStore(r, {k: v["edges"] for k, v in ref["inputs"].items()}, settings.bucket_ttl_s)
        yield
        await r.aclose()

    app = FastAPI(title="Drift Detection API", version="1.0.0", lifespan=lifespan)
    Instrumentator(excluded_handlers=["/healthz", "/readyz", "/metrics"], registry=registry).instrument(app).expose(app)

    def state(request: Request):
        """Dependency: the app state (store, reference, redis)."""
        return request.app.state

    @app.post("/v1/events")
    async def events(body: EventBatch | PredictionEvent = Body(...), st=Depends(state)) -> dict:
        """Count events into the live histograms (a batch, or one event)."""
        batch = body.events if isinstance(body, EventBatch) else [body]
        n = await st.store.add(batch)
        EVENTS.inc(n)
        return {"counted": n}

    @app.get("/v1/drift", response_model=DriftReport)
    async def drift(st=Depends(state)) -> DriftReport:
        """PSI per input over the live window, worst first; also refreshes the gauges."""
        live = await st.store.window(settings.window_minutes)
        rows = []
        for name, spec in st.ref["inputs"].items():
            v, n = psi(spec["shares"], live[name]), sum(live[name].values())
            lvl = level(v) if n == 0 or n >= settings.min_events else "too_few_events"   # 9 events gave PSI 12
            rows.append(FeatureDrift(name=name, psi=None if v is None else round(v, 5), level=lvl, live_count=n))
            if v is not None:
                PSI.labels(name).set(v)
        LIVE.set(max((r.live_count for r in rows), default=0))
        rows.sort(key=lambda r: -1 if r.psi is None else r.psi, reverse=True)
        return DriftReport(window_minutes=settings.window_minutes, model_version=str(st.ref.get("model_version")),
                           drift=any(r.level == "major" for r in rows), inputs=rows)

    @app.get("/healthz")
    async def healthz() -> dict:
        """Liveness."""
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(st=Depends(state)) -> dict:
        """Readiness: Redis answers."""
        try:
            await st.redis.ping()
        except Exception as e:   # noqa: BLE001
            raise HTTPException(503, f"redis: {e!r}") from e
        return {"status": "ready"}

    return app


app = create_app()
