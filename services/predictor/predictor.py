"""KServe custom predictor: the production XGBoost booster baked into the image (model-inside-image).

V1 protocol: POST /v1/models/card-fraud:predict {"instances": [{feature: value, ...}]}
          -> {"predictions": [fraud probability], "model_version": "<registry version>"}
Instances are dicts so the caller never depends on column order: they are reordered by the booster's own
feature names, and a missing or null feature is NaN (XGBoost's missing value), as in training.

Usage (in the image): python predictor.py --model_name card-fraud
"""
import argparse
import json
import os

import numpy as np
import xgboost as xgb
from kserve import Model, ModelServer, model_server

MODEL_DIR = os.environ.get("MODEL_DIR", "/models")


class FraudModel(Model):
    """Loads model.json + metadata.json once; scores batches of instances."""

    def __init__(self, name: str):
        """Register under `name` (the InferenceService name)."""
        super().__init__(name)
        self.booster, self.version, self.ready = None, "unknown", False

    def load(self) -> bool:
        """Read the booster and its registry metadata from MODEL_DIR."""
        self.booster = xgb.Booster()
        self.booster.load_model(f"{MODEL_DIR}/model.json")
        with open(f"{MODEL_DIR}/metadata.json") as f:
            self.version = str(json.load(f)["version"])
        self.ready = True
        return self.ready

    def predict(self, payload: dict, headers: dict | None = None) -> dict:
        """Score every instance; unknown keys are ignored, missing ones are NaN."""
        names = self.booster.feature_names
        x = np.array([[np.nan if r.get(n) is None else float(r[n]) for n in names] for r in payload["instances"]],
                     dtype="float64")
        return {"predictions": self.booster.inplace_predict(x, missing=np.nan).tolist(), "model_version": self.version}


if __name__ == "__main__":
    args, _ = argparse.ArgumentParser(parents=[model_server.parser]).parse_known_args()
    m = FraudModel(args.model_name)
    m.load()
    ModelServer().start([m])
