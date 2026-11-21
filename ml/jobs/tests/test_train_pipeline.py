"""The compiled training pipeline runs every step on the one image it was compiled for (the release pin), and step 1
tags the MLflow run with that image. Part of the ml image's test stage (ml/Dockerfile); kfp only, no cluster."""
import importlib.util
import os

PIPELINE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                        "pipelines", "train_fraud_model.py")


def test_every_step_runs_the_pinned_image(tmp_path, monkeypatch):
    monkeypatch.setenv("ML_IMAGE", "ghcr.io/cvinhnguyen/fraud-ml-train:1a2b3c4")
    spec = importlib.util.spec_from_file_location("train_fraud_model", PIPELINE)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)   # IMAGE is read from ML_IMAGE at import
    out = str(tmp_path / "train_fraud_model.yaml")
    m.compiler.Compiler().compile(m.train_fraud_model, out)

    assert m.images(out) == {"ghcr.io/cvinhnguyen/fraud-ml-train:1a2b3c4"}
    spec_text = open(out, encoding="utf-8").read()
    assert spec_text.count("value: ghcr.io/cvinhnguyen/fraud-ml-train:1a2b3c4") >= 7   # ML_IMAGE env of each step
    assert '\\"ml_image\\": os.environ[\\"ML_IMAGE\\"]' in spec_text   # step 1's source: it tags the MLflow run
