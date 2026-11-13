"""Drift Detection API.

  POST /v1/inference-events  CloudEvents of the KServe payload logger (InferenceService card-fraud -> Knative Broker
                             -> Trigger drift-detector, infra/k8s/kserve-card-fraud.yaml) -> live histograms
  GET  /v1/drift             PSI of every feature and of the score over the last WINDOW_MINUTES vs the training profile
  /metrics                   drift_psi{input=...} gauges (refreshed on every /v1/drift; only inputs with enough events),
                             drift_events_total{type=request|response}
  /healthz /readyz           liveness / readiness (Redis answers)

The reference profile (quantile edges + shares of each input on the validation period) is written by the training
pipeline with the model; services/predictor/fetch_model.py downloads it with the production model and compose
mounts it, so drift is always measured against the model in production.

Usage: uvicorn app.main:app --host 0.0.0.0 --port 8000
"""
import json
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from prometheus_client import REGISTRY, Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator

from app.psi import level, psi
from app.schemas import DriftReport, FeatureDrift
from app.settings import Settings
from app.store import SCORE, HistogramStore

log = logging.getLogger(__name__)
REQUEST, RESPONSE = "org.kubeflow.serving.inference.request", "org.kubeflow.serving.inference.response"
EVENTS = Counter("drift_events_total", "Instances counted from the payload logger's events", ["type"])
PSI = Gauge("drift_psi", "PSI of an input over the live window vs training", ["input"])
LIVE = Gauge("drift_live_count", "Events in the live window")


def load_reference(path: str) -> dict:
    """{"model_version": ..., "inputs": {name: {"edges": [...], "shares": {bin: share}}}}."""
    with open(path) as f:
        return json.load(f)


def num(value) -> float | None:
    """A JSON value as a float; None stays None (it lands in the `missing` bin)."""
    return None if value is None else float(value)


def instance(inst, features: list[str]) -> dict[str, float | None]:
    """One V1 instance as {feature: value}. The Scoring API sends a dict by name; a positional list is read in the
    model's feature order (the reference profile's, fetch_model.py checks it is the booster's) and must be complete."""
    values = dict(zip(features, inst, strict=True)) if isinstance(inst, list) else inst
    return {n: num(values.get(n)) for n in features}


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
        app.state.features = [n for n in ref["inputs"] if n != SCORE]   # the model's feature order
        app.state.redis = r
        app.state.store = HistogramStore(r, {k: v["edges"] for k, v in ref["inputs"].items()}, settings.bucket_ttl_s)
        yield
        await r.aclose()

    app = FastAPI(title="Drift Detection API", version="1.0.0", lifespan=lifespan)
    Instrumentator(excluded_handlers=["/healthz", "/readyz", "/metrics"], registry=registry).instrument(app).expose(app)

    def state(request: Request):
        """Dependency: the app state (store, reference, redis)."""
        return request.app.state

    @app.post("/v1/inference-events", status_code=202)
    async def inference_event(request: Request, ce_type: str = Header(), ce_id: str = Header(""),
                              st=Depends(state)) -> Response:
        """One CloudEvent of the payload logger in binary mode (ce-* headers, body = the V1 payload): a request counts
        its instances' features, a response its scores. The pair shares a ce-id but histograms need no join.
        A rejected event is logged here: the broker drops a 4xx without retrying it."""
        if ce_type not in (REQUEST, RESPONSE):
            log.warning("rejected event %s: unknown ce-type %r", ce_id, ce_type)
            raise HTTPException(400, f"unknown ce-type {ce_type!r}")
        kind = ce_type.rsplit(".", 1)[1]
        try:
            raw = await request.body()
            body = json.loads(raw) if raw else {}
            if not isinstance(body, dict):
                raise TypeError(f"body is a {type(body).__name__}, not an object")
            key = "instances" if ce_type == REQUEST else "predictions"
            if key not in body:   # not an inference: the logger also sends GETs (readiness, metadata) and their answers
                return Response(status_code=202)
            rows = ([instance(i, st.features) for i in body[key]] if ce_type == REQUEST
                    else [{SCORE: num(p)} for p in body[key]])
        except (ValueError, TypeError, AttributeError) as e:   # bad JSON, wrong shape, non-numeric value
            log.warning("rejected %s event %s: %r", kind, ce_id, e)
            raise HTTPException(400, f"malformed {kind} body: {e!r}") from e
        EVENTS.labels(kind).inc(await st.store.add(rows))
        # empty body: the broker reads any other answer as a reply event, fails the delivery and retries it (10x)
        return Response(status_code=202)

    @app.get("/v1/drift", response_model=DriftReport)
    async def drift(st=Depends(state)) -> DriftReport:
        """PSI per input over the live window, worst first; also refreshes the gauges."""
        live = await st.store.window(settings.window_minutes)
        rows = []
        for name, spec in st.ref["inputs"].items():
            v, n = psi(spec["shares"], live[name]), sum(live[name].values())
            lvl = level(v) if n == 0 or n >= settings.min_events else "too_few_events"   # 9 events gave PSI 12
            rows.append(FeatureDrift(name=name, psi=None if v is None else round(v, 5), level=lvl, live_count=n))
            if v is not None and lvl != "too_few_events":
                PSI.labels(name).set(v)
            else:   # too few events or none: no PSI to alert on
                with suppress(KeyError):
                    PSI.remove(name)
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
