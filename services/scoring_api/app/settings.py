"""Configuration from the environment (compose). Each model version normally brings its own thresholds (MLflow tags
t_low / t_high, sent by the predictor); T_LOW / T_HIGH here decide only for a model that sends none (an older image)."""
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Every knob of the API; env names are the upper-case field names."""

    model_config = SettingsConfigDict(env_prefix="", protected_namespaces=())

    feature_repo: str = "/app/feature_repo"
    feature_service: str = "fraud_model_v1"
    model_url: str = "http://fraud-control-plane:31080/v1/models/card-fraud:predict"   # KServe NodePort
    # A/B test: ab_percent of the cards (sticky by card_id) are scored by the challenger; 0 = off
    challenger_url: str = "http://fraud-control-plane:31081/v1/models/card-fraud-challenger:predict"
    ab_percent: int = Field(0, ge=0, le=100)
    kafka_bootstrap: str = "kafka:19092"
    prediction_topic: str = "fraud_predictions"
    t_low: float = Field(0.05, ge=0, le=1)     # step-up from here (when the model sends no thresholds)
    t_high: float = Field(0.5, ge=0, le=1)     # decline from here
    fallback_step_up_vnd: int = 2_000_000      # fallback rule: approve below, step up from here
    feature_timeout_s: float = 0.05
    model_timeout_s: float = 0.05
