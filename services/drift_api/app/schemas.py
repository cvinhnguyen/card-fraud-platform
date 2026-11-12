"""Drift reports. The input events are the KServe payload logger's CloudEvents, read in app/main.py."""
from pydantic import BaseModel


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
