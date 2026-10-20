"""Adapters to the outside world, each behind a small interface so the scorer can be tested with fakes:

  FeatureClient   Feast online features of the feature service (Redis), async
  ModelClient     KServe InferenceService, V1 protocol over HTTP, async
  PredictionLog   every request + response to Kafka `fraud_predictions` (drift monitoring, future labels)
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Protocol

import httpx

from app.schemas import NO_DEVICE, ScoreRequest

log = logging.getLogger(__name__)


class FeatureSource(Protocol):
    """Anything that returns the model's features for one request."""
    async def features(self, req: ScoreRequest) -> dict[str, float | None]: ...


class ModelSource(Protocol):
    """Anything that scores one feature vector."""
    async def predict(self, features: dict[str, float | None]) -> tuple[float, str]: ...


class FeatureClient:
    """Feast online retrieval through the feature service the deployed model was trained with."""

    def __init__(self, repo: str, service: str):
        """Open the feature store (registry in Postgres, online store Redis)."""
        from feast import FeatureStore
        self.store = FeatureStore(repo_path=repo)
        self.service = self.store.get_feature_service(service)
        self.names = [f.name for p in self.service.feature_view_projections for f in p.features]

    async def features(self, req: ScoreRequest) -> dict[str, float | None]:
        """Entity keys + request fields in, model features out (on-demand features computed by Feast)."""
        row = {"card_id": req.card_id, "device_id": req.device_id or NO_DEVICE, "amount_vnd": req.amount_vnd,
               "channel": req.channel.value, "currency": req.currency, "auth_method": req.auth_method,
               "request_ts": req.auth_ts}
        resp = await self.store.get_online_features_async(features=self.service, entity_rows=[row])
        values = resp.to_dict()
        return {n: values[n][0] for n in self.names}

    async def ready(self) -> bool:
        """Registry loaded and Redis answering."""
        await self.store.get_online_features_async(features=self.service, entity_rows=[
            {"card_id": "_probe", "device_id": NO_DEVICE, "amount_vnd": 1, "channel": "pos", "currency": "VND",
             "auth_method": "chip_pin", "request_ts": datetime(2026, 1, 1)}])
        return True


class ModelClient:
    """KServe V1: POST {"instances": [{feature: value}]} -> {"predictions": [score], "model_version": ...}."""

    def __init__(self, url: str, timeout_s: float, client: httpx.AsyncClient | None = None):
        """One pooled HTTP client for every request."""
        self.url = url
        self.client = client or httpx.AsyncClient(timeout=timeout_s)

    async def predict(self, features: dict[str, float | None]) -> tuple[float, str]:
        """Score of one feature vector and the model version that produced it.
        The scorer's time budget cancels only the shield: the HTTP call itself ends under httpx's own timeouts and
        returns its connection. Cancelled mid-request, httpx calls left the pool exhausted after a load burst, and
        every later request fell back (model_timeout / PoolTimeout) until the API was restarted."""
        call = asyncio.ensure_future(self.client.post(self.url, json={"instances": [features]}))
        call.add_done_callback(lambda c: c.cancelled() or c.exception())   # an abandoned call's error is not news
        r = await asyncio.shield(call)
        r.raise_for_status()
        body = r.json()
        return float(body["predictions"][0]), str(body.get("model_version", "unknown"))

    async def ready(self) -> bool:
        """The InferenceService answers its model-ready endpoint."""
        r = await self.client.get(self.url.removesuffix(":predict"))
        return r.status_code == 200


class PredictionLog:
    """Fire-and-forget Kafka producer: logging never adds latency to (or fails) a scoring request."""

    def __init__(self, bootstrap: str, topic: str):
        """Producer is created on start() (needs the running event loop)."""
        self.bootstrap, self.topic, self.producer = bootstrap, topic, None

    async def start(self) -> None:
        """Connect; if Kafka is down the API still scores, it only stops logging."""
        from aiokafka import AIOKafkaProducer
        try:
            self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap, linger_ms=20)
            await self.producer.start()
        except Exception:   # noqa: BLE001 - scoring must not depend on the log
            log.exception("prediction log disabled: Kafka unavailable")
            self.producer = None

    async def stop(self) -> None:
        """Flush and close."""
        if self.producer:
            await self.producer.stop()

    async def send(self, record: dict) -> None:
        """Queue one record keyed by card_id (no await on the broker ack)."""
        if self.producer:
            await self.producer.send(self.topic, json.dumps(record, default=str).encode(),
                                     key=record["card_id"].encode())
