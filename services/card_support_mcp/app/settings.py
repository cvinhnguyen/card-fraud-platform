"""Configuration from the environment (Helm values / compose). Secrets (store passwords, model key) come from a
Kubernetes Secret as environment variables; the Feast repos read theirs as ${...} in feature_store.yaml."""
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Every knob of the server; env names are the upper-case field names."""

    model_config = SettingsConfigDict(env_prefix="")

    fraud_repo: str = "/app/feature_repo_fraud"   # Feast project card_fraud (Redis): card features
    kb_repo: str = "/app/feature_repo_kb"         # Feast project card_support_kb (pgvector): policy chunks
    embeddings_url: str = "http://host.docker.internal:8086"
    llm_api_key: str = ""                         # Bearer key of the embedding server
    scoring_url: str = "http://scoring-api:8000/v1/score"   # Fraud Scoring API (Layer 2)
    timeout_s: float = Field(5.0, gt=0)           # per call to a store, the embedder or the scoring API
    candidate_factor: int = Field(3, ge=1, le=10) # search reads k x this candidates, then re-ranks them
