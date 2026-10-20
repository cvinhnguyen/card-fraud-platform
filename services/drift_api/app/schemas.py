"""Events (what the scoring API logs to Kafka `fraud_predictions`) and drift reports."""
from pydantic import BaseModel, Field


class PredictionEvent(BaseModel):
    """One scored authorization; only the fields drift detection needs, the rest is ignored."""

    auth_id: str = Field(min_length=1, max_length=64)
    score: float | None = Field(None, ge=0, le=1)
    fallback: bool = False
    features: dict[str, float | None] | None = None


class EventBatch(BaseModel):
    """A batch of events (ml/jobs/drift_forwarder.py posts up to 1,000 at a time)."""

    events: list[PredictionEvent] = Field(min_length=1, max_length=1000)


class FeatureDrift(BaseModel):
    """PSI of one input (a feature or the score) over the live window."""

    name: str
    psi: float | None
    level: str
    live_count: int


class DriftReport(BaseModel):
    """Every input's drift, worst first; `drift` = any input at major."""

    window_minutes: int
    model_version: str
    drift: bool
    inputs: list[FeatureDrift]
