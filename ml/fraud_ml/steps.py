"""The 7 steps of the training pipeline. The notebook calls them in order; each KFP component calls one of them.

  1 load_labels        labels as of label_as_of (Delta time travel) -> 2-column label table + entity rows   (Spark)
  2 get_training_data  Feast point-in-time features -> versioned training set (Delta MERGE)                   (Spark)
  3 validate_data      schema, nulls, label rates; stops the run on failure
  4 time_split         train / val / test parquet, gaps checked
  5 train              logistic regression baseline + distributed XGBoost                                    (Spark)
  6 evaluate           thresholds on val, metrics on test, production model on the same test, passed flag
  7 register           MLflow model version + alias challenger (production on the very first run)

Steps exchange S3 paths and the MLflow run id, never data: everything lands in s3://lakehouse/ml/runs/<run id>/.
"""
import datetime as dt
import hashlib
import io
import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

LAKE = "s3a://lakehouse"
REPO = os.environ.get("FEAST_REPO", "/opt/ml/feature_repo")


def s3() -> pafs.S3FileSystem:
    """pyarrow S3 filesystem on SeaweedFS (keys from AWS_* env)."""
    return pafs.S3FileSystem(endpoint_override="lakehouse-s3:8333", scheme="http", region="us-east-1",
                             access_key=os.environ["AWS_ACCESS_KEY_ID"], secret_key=os.environ["AWS_SECRET_ACCESS_KEY"])


def write_parquet(df: pd.DataFrame, path: str) -> str:
    """Write a pandas frame to s3a://... (one file); returns the path."""
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path.removeprefix("s3a://"), filesystem=s3())
    return path


def read_parquet(path: str) -> pd.DataFrame:
    """Read a parquet file written by write_parquet."""
    return pq.read_table(path.removeprefix("s3a://"), filesystem=s3()).to_pandas()


def read_delta(path: str, version: int) -> pd.DataFrame:
    """One version of the training-set table as pandas. Spark, not the lighter `deltalake`: the table has deletion
    vectors (what makes its versions incremental), and deltalake 0.25 and 1.1 refuse to read them."""
    from fraud_ml.spark import session
    spark = session("read_training_set", driver_memory="2g")
    df = spark.read.format("delta").option("versionAsOf", version).load(path).drop("row_hash").toPandas()
    spark.stop()
    return df


def feature_names(feature_service: str) -> list[str]:
    """Model inputs = every feature of the feature service, in its order."""
    from feast import FeatureStore
    svc = FeatureStore(repo_path=REPO).get_feature_service(feature_service)
    return [f.name for p in svc.feature_view_projections for f in p.features]


def run_dir(run_id: str) -> str:
    """Where one run's intermediate data lives."""
    return f"{LAKE}/ml/runs/{run_id}"


def load_labels(label_as_of: str, negative_rate: float, feature_service: str, tags: dict | None = None) -> str:
    """Step 1. Starts the MLflow run; writes the label table and the entity rows. Returns the run id."""
    from fraud_ml.data import TrainingDataService
    from fraud_ml.registry import ModelRegistry
    from fraud_ml.spark import session
    from fraud_ml.split import TimeSplitter
    spark = session("load_labels", driver_memory="2g")
    svc = TrainingDataService(spark, dt.date.fromisoformat(label_as_of), negative_rate, feature_service)
    reg = ModelRegistry()
    run_id = reg.start({"pipeline": "train_fraud_model", **(tags or {})})
    labels, entities = svc.labels(), svc.entities()
    labels.write.mode("overwrite").parquet(f"{run_dir(run_id)}/labels")          # (transaction_id, is_fraud)
    entities.write.mode("overwrite").parquet(f"{run_dir(run_id)}/entities")
    counts = {r["split"]: r["count"] for r in entities.groupBy("split").count().collect()}
    windows = TimeSplitter(svc.label_as_of).windows()
    reg.log(run_id, params={"label_as_of": label_as_of, "negative_rate": negative_rate,
                            "feature_service": feature_service,
                            **{f"data_version.{k}": v for k, v in svc.data_versions().items()},
                            **{f"window.{k}": f"{a}..{b}" for k, (a, b) in windows.items()}},
            metrics={f"entities.{k}": v for k, v in counts.items()})
    spark.stop()
    return run_id


def get_training_data(run_id: str, label_as_of: str, feature_service: str) -> int:
    """Step 2. Feast point-in-time join; MERGE into the training-set table. Returns its Delta version."""
    from feast import FeatureStore
    from fraud_ml.data import TrainingDataService
    from fraud_ml.registry import ModelRegistry
    from fraud_ml.spark import session
    # 2g heap in a 5 GiB step: the JVM (2.9 GB RSS) + the pandas result (1.8 GB) thrashed at 4.5 GiB, and a 1500m heap
    # ran out (OutOfMemoryError in toPandas) on a training set only 0.35% larger than the one it had just built
    spark = session("get_training_data", driver_memory="2g")
    svc = TrainingDataService(spark, dt.date.fromisoformat(label_as_of), feature_service=feature_service)
    entities = spark.read.parquet(f"{run_dir(run_id)}/entities")
    df = svc.historical_features(entities, FeatureStore(repo_path=REPO))
    keep = ["transaction_id", "card_id", "request_ts", "split", "is_fraud", "sample_weight", *feature_names(feature_service)]
    df = df[keep].sort_values("transaction_id", ignore_index=True)
    path = training_set_path(feature_service)
    version = svc.save_versioned(df, path)
    ModelRegistry().log(run_id, params={"training_set.path": path, "training_set.version": version},
                        metrics={"training_set.rows": len(df)})
    spark.stop()
    return version


def training_set_path(feature_service: str) -> str:
    """Delta table holding every run's training set, one version per run."""
    return f"{LAKE}/ml/training_set_{feature_service}"


def validate_data(run_id: str, version: int, feature_service: str) -> None:
    """Step 3. Fails the run (raises) if any data check fails; the report is logged either way."""
    from fraud_ml.registry import ModelRegistry
    from fraud_ml.validation import DataValidator
    df = read_delta(training_set_path(feature_service), version)
    v = DataValidator(feature_names(feature_service))
    report = v.validate(df)
    ModelRegistry().log(run_id, files={"data_validation.json": json.dumps(report, indent=2)},
                        metrics={"data_checks.failed": sum(not r["ok"] for r in report)})
    v.raise_if_failed()


def time_split(run_id: str, version: int, label_as_of: str, feature_service: str) -> dict[str, str]:
    """Step 4. One parquet per split; asserts every split stays in its window with the gaps empty."""
    from fraud_ml.registry import ModelRegistry
    from fraud_ml.split import TimeSplitter
    df = read_delta(training_set_path(feature_service), version)
    day = pd.to_datetime(df.request_ts).dt.date
    TimeSplitter(dt.date.fromisoformat(label_as_of)).check(
        {s: (day[df.split == s].min(), day[df.split == s].max()) for s in ("train", "val", "test")})
    paths = {s: write_parquet(df[df.split == s], f"{run_dir(run_id)}/{s}.parquet") for s in ("train", "val", "test")}
    ModelRegistry().log(run_id, metrics={f"{s}.{m}": v for s in paths for m, v in
                                         (("rows", float((df.split == s).sum())),
                                          ("fraud_rate", float(df.is_fraud[df.split == s].mean())))})
    return paths


def train(run_id: str, train_path: str, val_path: str, feature_service: str, num_workers: int,
          executors: int, xgb_params: dict | None = None) -> str:
    """Step 5. Baseline + distributed XGBoost. Returns the S3 directory with both models."""
    import joblib
    from fraud_ml.registry import ModelRegistry
    from fraud_ml.spark import session
    from fraud_ml.train import XGB_PARAMS, FraudModelTrainer
    feats = feature_names(feature_service)
    tr, va = read_parquet(train_path), read_parquet(val_path)
    trainer = FraudModelTrainer(feats, {**XGB_PARAMS, **(xgb_params or {})}, num_workers)
    baseline = trainer.fit_baseline(tr)
    # 1g driver: it only ships the Arrow batches and collects the booster; the node also holds the 2 executor pods
    spark = session("train_xgboost", executors=executors, driver_memory="1g")
    booster = trainer.fit_xgboost(spark, tr, va)
    spark.stop()
    out = f"{run_dir(run_id)}/model"
    fs = s3()
    with fs.open_output_stream(f"{out.removeprefix('s3a://')}/xgb.json") as f:
        f.write(booster.save_raw("json"))
    buf = io.BytesIO()
    joblib.dump(baseline, buf)
    with fs.open_output_stream(f"{out.removeprefix('s3a://')}/baseline.joblib") as f:
        f.write(buf.getvalue())
    # The pickle crosses a shared bucket: its hash goes to MLflow (another system) and is checked before loading.
    ModelRegistry().log(run_id, params={**{f"xgb.{k}": v for k, v in trainer.xgb_params.items()},
                                        "xgb.num_workers": num_workers, "xgb.best_iteration": booster.best_iteration,
                                        "features": ",".join(feats),
                                        "baseline.sha256": hashlib.sha256(buf.getvalue()).hexdigest()})
    return out


def load_models(model_dir: str, baseline_sha256: str | None = None):
    """(booster, baseline) written by `train`; the baseline only when its expected SHA-256 is given (else None).
    joblib is pickle, so the bytes are verified against the hash train logged to MLflow before they are loaded."""
    import joblib
    import xgboost as xgb
    fs, base = s3(), model_dir.removeprefix("s3a://")
    booster = xgb.Booster()
    with fs.open_input_stream(f"{base}/xgb.json") as f:
        booster.load_model(bytearray(f.read()))
    if baseline_sha256 is None:
        return booster, None
    with fs.open_input_stream(f"{base}/baseline.joblib") as f:
        data = f.read()
    if hashlib.sha256(data).hexdigest() != baseline_sha256:
        raise ValueError(f"{base}/baseline.joblib does not match the hash logged by step 5: refusing to unpickle it")
    return booster, joblib.load(io.BytesIO(data))


def evaluate(run_id: str, model_dir: str, val_path: str, test_path: str, feature_service: str,
             min_pr_auc_gain: float) -> bool:
    """Step 6. Thresholds on val; test metrics of the new model, the baseline and production; passed flag."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from fraud_ml.evaluate import ModelEvaluator
    from fraud_ml.registry import ModelRegistry
    from fraud_ml.train import FraudModelTrainer
    reg, ev = ModelRegistry(), ModelEvaluator(min_pr_auc_gain)
    booster, baseline = load_models(model_dir, reg.client.get_run(run_id).data.params["baseline.sha256"])
    feats = feature_names(feature_service)
    va, te = read_parquet(val_path), read_parquet(test_path)
    y_va, y_te = va.is_fraud.to_numpy(), te.is_fraud.to_numpy()
    t = ev.thresholds(y_va, FraudModelTrainer.predict(booster, va))
    s_te = FraudModelTrainer.predict(booster, te)
    new = ev.metrics(y_te, s_te, t)
    s_base = baseline.predict_proba(te[feats])[:, 1]
    base = ev.metrics(y_te, s_base, ev.thresholds(y_va, baseline.predict_proba(va[feats])[:, 1]))
    prod_booster = reg.load_booster("production")
    prod = None
    fig, ax = plt.subplots(figsize=(7, 5))
    ev.pr_curve(y_te, s_te, "XGBoost (new)", ax)
    ev.pr_curve(y_te, s_base, "Logistic regression", ax)
    if prod_booster is not None:
        s_prod = FraudModelTrainer.predict(prod_booster, te)
        prod = ev.metrics(y_te, s_prod, ev.thresholds(y_va, FraudModelTrainer.predict(prod_booster, va)))
        ev.pr_curve(y_te, s_prod, "production", ax)
    ax.set(xlabel="recall", ylabel="precision", title="Test period: precision-recall")
    ax.legend()
    passed = ev.passed(new, prod)
    importance = pd.Series(booster.get_score(importance_type="gain")).sort_values(ascending=False)
    reg.log(run_id, params={"t_low": round(t["t_low"], 6), "t_high": round(t["t_high"], 6), "passed": passed},
            metrics={**{f"test.{k}": v for k, v in new.items()}, **{f"baseline.{k}": v for k, v in base.items()},
                     **({f"production.{k}": v for k, v in prod.items()} if prod else {})},
            files={"thresholds.json": json.dumps(t)},
            tables={"decision_table.csv": ev.decision_table(y_te, s_te, t),
                    "feature_importance_gain.csv": importance.to_frame("gain")})
    reg.log_figure(run_id, fig, "pr_curve.png")
    from fraud_ml.profile import build
    profile = build(va[feats].assign(score=FraudModelTrainer.predict(booster, va)))   # drift reference (val period)
    fs = s3()
    with fs.open_output_stream(f"{model_dir.removeprefix('s3a://')}/evaluation.json") as f:
        f.write(json.dumps({"thresholds": t, "test": new, "baseline": base, "production": prod,
                            "passed": passed}).encode())
    with fs.open_output_stream(f"{model_dir.removeprefix('s3a://')}/reference_profile.json") as f:
        f.write(json.dumps(profile).encode())
    return passed


def register(run_id: str, model_dir: str, test_path: str, passed: bool, label_as_of: str, feature_service: str) -> int:
    """Step 7. Register the booster if it passed; returns the version (0 = not registered)."""
    from fraud_ml.registry import ModelRegistry
    if not passed:
        print("evaluation did not pass: nothing registered")
        return 0
    booster, _ = load_models(model_dir)
    with s3().open_input_stream(f"{model_dir.removeprefix('s3a://')}/evaluation.json") as f:
        ev = json.loads(f.read())
    extra = {"feature_service": feature_service, "label_as_of": label_as_of, "t_low": ev["thresholds"]["t_low"],
             "t_high": ev["thresholds"]["t_high"], "test_pr_auc": round(ev["test"]["pr_auc"], 4), "run_id": run_id}
    example = read_parquet(test_path).head(100)
    reg = ModelRegistry()
    version = reg.register(run_id, booster, example, extra)
    with s3().open_input_stream(f"{model_dir.removeprefix('s3a://')}/reference_profile.json") as f:
        profile = {**json.loads(f.read()), "model_version": str(version)}
    reg.log(run_id, files={"reference_profile.json": json.dumps(profile)})   # fetch_model.py ships it with the model
    return version
