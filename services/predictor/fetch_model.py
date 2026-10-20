"""Release step: download the model behind an MLflow alias into ./model for the image build: model.json, metadata.json
and reference_profile.json (the drift API's reference for the same model).

Usage: MLFLOW_TRACKING_URI=http://127.0.0.1:5000 python services/predictor/fetch_model.py --alias production
"""
import argparse
import json
from pathlib import Path

import mlflow
from mlflow import MlflowClient

NAME = "card_fraud_xgb"

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--alias", default="production")
    ap.add_argument("--out", type=Path, default=Path(__file__).parent / "model")
    a = ap.parse_args()
    mv = MlflowClient().get_model_version_by_alias(NAME, a.alias)
    a.out.mkdir(parents=True, exist_ok=True)
    booster = mlflow.xgboost.load_model(f"models:/{NAME}/{mv.version}")
    booster.save_model(str(a.out / "model.json"))
    meta = {"name": NAME, "version": mv.version, "alias": a.alias, "run_id": mv.run_id, **mv.tags}
    (a.out / "metadata.json").write_text(json.dumps(meta, indent=2), newline="\n")
    # the drift API's reference (same model, its validation period): compose mounts it into the drift API
    mlflow.artifacts.download_artifacts(run_id=mv.run_id, artifact_path="reference_profile.json", dst_path=str(a.out))
    print(f"{NAME} v{mv.version} ({a.alias}) -> {a.out}")
