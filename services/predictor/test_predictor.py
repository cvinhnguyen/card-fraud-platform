"""Release check of a predictor image (Dockerfile stage `test`): the baked model loads, scores a row, and is the
version CI fetched. CI: docker build --target test ... && docker run --rm -e EXPECTED_VERSION=<version> <image>
By hand, on the fetched model: MODEL_DIR=services/predictor/model pytest services/predictor/test_predictor.py
"""
import json
import os

import pytest
from predictor import MODEL_DIR, FraudModel


@pytest.fixture(scope="module")
def model() -> FraudModel:
    m = FraudModel("card-fraud")
    assert m.load() and m.ready
    return m


def test_predicts_a_probability_per_instance(model):
    names = model.booster.feature_names
    row = {n: 1.0 for n in names}
    out = model.predict({"instances": [row, {}, dict(reversed(list(row.items())))]})
    p = out["predictions"]
    assert len(p) == 3 and all(0.0 <= s <= 1.0 for s in p)
    assert p[0] == p[2]   # instances are read by feature name, not by position


def test_version_is_the_fetched_one(model):
    with open(f"{MODEL_DIR}/metadata.json") as f:
        meta = json.load(f)
    assert model.predict({"instances": [{}]})["model_version"] == str(meta["version"])
    expected = os.environ.get("EXPECTED_VERSION")   # the alias's version when CI planned the release
    if expected:
        assert str(meta["version"]) == expected, f"fetched v{meta['version']}, planned v{expected}: the alias moved"


def test_version_carries_its_own_thresholds(model):
    # without them the Scoring API would decide this model's scores with another model's cut-offs (T_LOW / T_HIGH)
    with open(f"{MODEL_DIR}/metadata.json") as f:
        meta = json.load(f)
    assert "t_low" in meta and "t_high" in meta, f"model v{meta['version']} has no t_low / t_high tags in MLflow"
    assert 0 <= float(meta["t_low"]) <= float(meta["t_high"]) <= 1
    out = model.predict({"instances": [{}]})
    assert (out["t_low"], out["t_high"]) == (float(meta["t_low"]), float(meta["t_high"]))


def test_reference_profile_matches_the_model(model):
    with open(f"{MODEL_DIR}/reference_profile.json") as f:
        ref = json.load(f)
    assert [n for n in ref["inputs"] if n != "score"] == model.booster.feature_names
    assert str(ref["model_version"]) == model.version   # the drift API compares against this model's own profile
