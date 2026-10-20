"""Evaluation on the time-split periods, with the production model scored on the same test period.

  thresholds  chosen on validation: t_high = score above which 0.1% of legitimate payments fall (decline),
              t_low = the 1% point (step-up authentication). They are recommendations stored with the model version;
              the API reads its thresholds from configuration, so risk can tune them without a release.
  metrics     on test: PR-AUC (fraud is rare: ROC-AUC flatters weak models), ROC-AUC, recall at 1% FPR,
              precision / recall of a decline, share of payments declined and stepped up
  promotion   passed = no production model yet, or PR-AUC gain >= min_pr_auc_gain and recall@1%FPR not worse
"""
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score

DECLINE_FPR, STEP_UP_FPR = 0.001, 0.01


def threshold_at_fpr(y: np.ndarray, score: np.ndarray, fpr: float) -> float:
    """Smallest score that flags at most `fpr` of the negatives."""
    return float(np.quantile(score[y == 0], 1 - fpr))


def recall_at_fpr(y: np.ndarray, score: np.ndarray, fpr: float) -> float:
    """Share of frauds caught when `fpr` of legitimate payments may be flagged."""
    return float((score[y == 1] > threshold_at_fpr(y, score, fpr)).mean())


@dataclass
class ModelEvaluator:
    """Thresholds from validation, metrics on test, and the promotion decision."""

    min_pr_auc_gain: float = 0.005

    @staticmethod
    def thresholds(y_val: np.ndarray, s_val: np.ndarray) -> dict[str, float]:
        """t_low / t_high meeting the step-up and decline false-positive budgets on validation."""
        return {"t_low": threshold_at_fpr(y_val, s_val, STEP_UP_FPR), "t_high": threshold_at_fpr(y_val, s_val, DECLINE_FPR)}

    @staticmethod
    def metrics(y: np.ndarray, s: np.ndarray, t: dict[str, float]) -> dict[str, float]:
        """Test metrics of one model at the given thresholds."""
        decline, step_up = s >= t["t_high"], (s >= t["t_low"]) & (s < t["t_high"])
        tp = int((decline & (y == 1)).sum())
        return {"pr_auc": float(average_precision_score(y, s)), "roc_auc": float(roc_auc_score(y, s)),
                "recall_at_1pct_fpr": recall_at_fpr(y, s, 0.01),
                "precision_at_t_high": tp / max(int(decline.sum()), 1), "recall_at_t_high": tp / max(int(y.sum()), 1),
                "decline_rate": float(decline.mean()), "step_up_rate": float(step_up.mean()),
                "test_rows": float(len(y)), "test_fraud_rate": float(y.mean())}

    def passed(self, new: dict[str, float], production: dict[str, float] | None) -> bool:
        """Promotion rule of the registry (a reviewer still approves before `production`)."""
        if production is None:
            return True
        return (new["pr_auc"] - production["pr_auc"] >= self.min_pr_auc_gain
                and new["recall_at_1pct_fpr"] >= production["recall_at_1pct_fpr"])

    @staticmethod
    def pr_curve(y: np.ndarray, s: np.ndarray, label: str, ax) -> None:
        """Draw the precision-recall curve of one model on a matplotlib axis."""
        p, r, _ = precision_recall_curve(y, s)
        ax.plot(r, p, linewidth=2, label=f"{label} (PR-AUC {average_precision_score(y, s):.3f})")

    @staticmethod
    def decision_table(y: np.ndarray, s: np.ndarray, t: dict[str, float]) -> pd.DataFrame:
        """Confusion table: decision (approve / step_up / decline) x label."""
        decision = np.where(s >= t["t_high"], "decline", np.where(s >= t["t_low"], "step_up", "approve"))
        return pd.crosstab(pd.Series(decision, name="decision"), pd.Series(y, name="is_fraud"))
