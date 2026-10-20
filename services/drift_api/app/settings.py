"""Configuration from the environment."""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Env names are the upper-case field names."""

    redis_url: str = "redis://redis:6379/1"      # db 1: Feast uses db 0
    redis_password: str | None = None
    reference_path: str = "/config/reference_profile.json"
    window_minutes: int = 15                     # live window compared with the reference
    min_events: int = 100                        # fewer in the window: PSI over 10 bins is noise, never "drift"
    bucket_ttl_s: int = 6 * 3600
