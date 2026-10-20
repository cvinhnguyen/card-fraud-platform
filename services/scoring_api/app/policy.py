"""Decisions. Two strategies behind one call: the model's score against the configured thresholds, and the
fallback rule when features or model are unavailable (a payment is never blocked by an outage)."""
from dataclasses import dataclass

from app.schemas import Decision


@dataclass(frozen=True)
class ThresholdPolicy:
    """score >= t_high: decline; t_low <= score < t_high: step-up; below t_low: approve."""

    t_low: float
    t_high: float

    def __post_init__(self):
        """Thresholds must be ordered probabilities."""
        if not 0 <= self.t_low <= self.t_high <= 1:
            raise ValueError(f"need 0 <= t_low <= t_high <= 1, got {self.t_low}, {self.t_high}")

    def decide(self, score: float) -> Decision:
        """Map a fraud probability to a decision."""
        if score >= self.t_high:
            return Decision.decline
        if score >= self.t_low:
            return Decision.step_up
        return Decision.approve


@dataclass(frozen=True)
class FallbackPolicy:
    """Conservative rule without a model: small payments pass, larger ones get extra authentication."""

    step_up_from_vnd: int

    def decide(self, amount_vnd: int) -> Decision:
        """Approve below the amount, step up from it; never decline without evidence."""
        return Decision.step_up if amount_vnd >= self.step_up_from_vnd else Decision.approve
