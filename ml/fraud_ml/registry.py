"""MLflow: one run per pipeline run (steps resume it by id), model registry `card_fraud_xgb` with aliases.

  challenger  set by the pipeline when a model passes evaluation
  production  set by a reviewer (or bootstrap: the first model ever); the serving image is built from it.
              Rollback = move the alias back to the previous version.
"""
import json
import os
import tempfile
from dataclasses import dataclass

import mlflow
import numpy as np
import pandas as pd
import xgboost as xgb
from mlflow import MlflowClient
from mlflow.models import infer_signature

MODEL_NAME = "card_fraud_xgb"
EXPERIMENT = "card_fraud"


@dataclass
class ModelRegistry:
    """Thin layer over MLflow so pipeline steps never repeat tracking-URI, experiment or alias details."""

    tracking_uri: str = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    def __post_init__(self):
        """Point MLflow at the tracking server and the experiment."""
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(EXPERIMENT)
        self.client = MlflowClient()

    def start(self, tags: dict[str, str]) -> str:
        """New run; returns its id for the later steps."""
        with mlflow.start_run(tags=tags) as run:
            return run.info.run_id

    def log(self, run_id: str, params: dict | None = None, metrics: dict | None = None,
            files: dict[str, str] | None = None, tables: dict[str, pd.DataFrame] | None = None) -> None:
        """Log into an existing run: params, metrics, text files {name: content}, tables as CSV."""
        with mlflow.start_run(run_id=run_id):
            if params:
                mlflow.log_params({k: str(v)[:500] for k, v in params.items()})
            if metrics:
                mlflow.log_metrics(metrics)
            for name, content in (files or {}).items():
                mlflow.log_text(content, name)
            for name, df in (tables or {}).items():
                mlflow.log_text(df.to_csv(), name)

    def log_figure(self, run_id: str, fig, name: str) -> None:
        """Log a matplotlib figure as an artifact."""
        with mlflow.start_run(run_id=run_id):
            mlflow.log_figure(fig, name)

    def register(self, run_id: str, booster: xgb.Booster, example: pd.DataFrame, extra: dict) -> int:
        """Log the booster with signature + input example, register it, set alias challenger; returns the version."""
        x = example[booster.feature_names].astype("float64")
        signature = infer_signature(x, booster.inplace_predict(x.to_numpy(), missing=np.nan))
        with mlflow.start_run(run_id=run_id):
            info = mlflow.xgboost.log_model(booster, name="model", signature=signature, input_example=x.head(5),
                                            registered_model_name=MODEL_NAME, model_format="json")
            mlflow.log_text(json.dumps(extra, indent=2, default=str), "model_card.json")
        version = int(info.registered_model_version)
        for k, v in extra.items():
            self.client.set_model_version_tag(MODEL_NAME, str(version), k, str(v))
        self.client.set_registered_model_alias(MODEL_NAME, "challenger", str(version))
        if self.version_of("production") is None:   # bootstrap: the first model ever serves until a better one
            self.client.set_registered_model_alias(MODEL_NAME, "production", str(version))
        return version

    def version_of(self, alias: str) -> int | None:
        """Model version behind an alias, or None."""
        try:
            return int(self.client.get_model_version_by_alias(MODEL_NAME, alias).version)
        except mlflow.exceptions.MlflowException:
            return None

    def load_booster(self, alias: str) -> xgb.Booster | None:
        """The booster behind an alias (for the same-test-period comparison), or None."""
        if self.version_of(alias) is None:
            return None
        with tempfile.TemporaryDirectory() as tmp:
            return mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@{alias}", dst_path=tmp)
