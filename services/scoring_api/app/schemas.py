"""Request / response models. Pydantic rejects a malformed authorization with 422 before any lookup happens."""
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field

NO_DEVICE = "-"   # same sentinel as the Feast repo: payments without a device never match a card_device row


class Channel(str, Enum):
    """How the card was used."""
    ecom = "ecom"
    pos = "pos"
    atm = "atm"


class Decision(str, Enum):
    """What the authorization switch does with the payment."""
    approve = "approve"
    step_up = "step_up"     # ask for OTP / biometric
    decline = "decline"


class ScoreRequest(BaseModel):
    """One card authorization, as the switch sends it."""

    auth_id: str = Field(min_length=1, max_length=64)
    card_id: str = Field(min_length=1, max_length=64)
    customer_id: str = Field(min_length=1, max_length=64)
    merchant_id: str = Field(min_length=1, max_length=64)
    amount_vnd: int = Field(gt=0, le=10_000_000_000)        # 10 billion VND: above any card limit
    channel: Channel
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    auth_method: str = Field("unknown_pre_regulation", max_length=32)
    device_id: str | None = Field(None, max_length=64)     # online payments only
    auth_ts: datetime


class ScoreResponse(BaseModel):
    """Score and decision; `fallback` = the rule decided because features or model were unavailable."""

    auth_id: str
    score: float | None = Field(None, ge=0, le=1)
    decision: Decision
    model_version: str
    fallback: bool = False
