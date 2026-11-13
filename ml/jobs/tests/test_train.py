"""FraudModelTrainer.best_trees: the served booster stops at the best early-stopping round (no Spark needed)."""
import numpy as np
import xgboost as xgb

from fraud_ml.train import FraudModelTrainer


def booster(early_stopping: bool) -> tuple[xgb.Booster, np.ndarray]:
    rng = np.random.default_rng(0)
    x = rng.normal(size=(4000, 8))
    y = (x[:, 0] + rng.normal(size=4000) * 2 > 2).astype(int)
    train, val = xgb.DMatrix(x[:3000], y[:3000]), xgb.DMatrix(x[3000:], y[3000:])
    b = xgb.train({"max_depth": 6, "eta": 0.3, "eval_metric": "aucpr", "objective": "binary:logistic"}, train, 400,
                  evals=[(val, "val")], early_stopping_rounds=30 if early_stopping else None, verbose_eval=False)
    return b, x[3000:]


def test_best_trees_drops_the_rounds_after_the_best_one():
    b, x = booster(early_stopping=True)
    assert b.best_iteration + 1 < b.num_boosted_rounds()   # trained past the best round
    best = FraudModelTrainer.best_trees(b)
    assert best.num_boosted_rounds() == b.best_iteration + 1
    np.testing.assert_allclose(best.inplace_predict(x), b.inplace_predict(x, iteration_range=(0, b.best_iteration + 1)))


def test_best_trees_keeps_a_booster_without_early_stopping():
    b, _ = booster(early_stopping=False)
    assert FraudModelTrainer.best_trees(b) is b
