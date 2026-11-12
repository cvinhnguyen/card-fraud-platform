"""ModelRegistry.register keeps every trained model and gives an alias only to one that passed evaluation.

Runs against a throwaway MLflow file store, no server needed; part of the ml image's test stage (ml/Dockerfile):
  docker build -f ml/Dockerfile --target test -t fraud/ml-train:test . && docker run --rm fraud/ml-train:test
"""
import numpy as np
import pandas as pd
import xgboost as xgb

from fraud_ml.registry import MODEL_NAME, ModelRegistry


def test_rejected_model_is_kept_without_alias(tmp_path, monkeypatch):
    """A model that did not pass becomes a version tagged passed=False; only a passing one is challenger."""
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")   # the image has no database driver for a local MLflow
    reg = ModelRegistry(f"file://{tmp_path}/mlruns")
    x = pd.DataFrame(np.random.default_rng(0).random((50, 3)), columns=["a", "b", "c"])
    booster = xgb.train({"objective": "binary:logistic"}, xgb.DMatrix(x, label=(x.a > 0.5).astype(int)), 3)

    rejected = reg.register(reg.start({}), booster, x, {"label_as_of": "2026-10-15"}, passed=False)
    assert reg.client.get_model_version(MODEL_NAME, str(rejected)).tags["passed"] == "False"
    assert reg.version_of("challenger") is None and reg.version_of("production") is None

    accepted = reg.register(reg.start({}), booster, x, {"label_as_of": "2026-10-16"}, passed=True)
    assert accepted == rejected + 1
    assert reg.version_of("challenger") == accepted and reg.version_of("production") == accepted   # first to pass serves
