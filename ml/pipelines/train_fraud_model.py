"""Kubeflow pipeline `train_fraud_model`: the notebook's 7 steps as container components (fraud_ml.steps).

Every component runs the code image fraud/ml-train (library + Feast repo), so a step is reproducible and can be
re-run alone. Steps pass S3 paths and the MLflow run id; data stays in the lakehouse. `train` is distributed:
its pod is the Spark driver and starts `executors` executor pods, the XGBoost workers.

Usage (repo root, host venv with kfp + kfp-kubernetes):
  python ml/pipelines/train_fraud_model.py --compile                                   # -> ml/pipelines/train_fraud_model.yaml
  python ml/pipelines/train_fraud_model.py --run --label-as-of 2026-10-15              # submit to the KFP API (default 127.0.0.1:31888)
"""
import argparse
import os
from typing import NamedTuple

from kfp import compiler, dsl, kubernetes

IMAGE = os.environ.get("ML_IMAGE", "localhost:5001/fraud/ml-train:latest")
KFP_HOST = os.environ.get("KFP_HOST", "http://127.0.0.1:31888")


def component(f):
    """Lightweight component on the code image (kfp is preinstalled there: no pip at run time)."""
    return dsl.component(base_image=IMAGE, install_kfp_package=False)(f)


@component
def load_labels(label_as_of: str, negative_rate: float, feature_service: str, kfp_run_id: str) -> str:
    """Step 1: labels as of label_as_of + entity rows; starts the MLflow run and returns its id."""
    from fraud_ml import steps
    return steps.load_labels(label_as_of, negative_rate, feature_service, {"kfp_run_id": kfp_run_id})


@component
def get_training_data(run_id: str, label_as_of: str, feature_service: str) -> int:
    """Step 2: Feast point-in-time features -> versioned training set; returns its Delta version."""
    from fraud_ml import steps
    return steps.get_training_data(run_id, label_as_of, feature_service)


@component
def validate_data(run_id: str, version: int, feature_service: str):
    """Step 3: data checks; raises (and stops the run) on failure."""
    from fraud_ml import steps
    steps.validate_data(run_id, version, feature_service)


@component
def time_split(run_id: str, version: int, label_as_of: str, feature_service: str) -> NamedTuple(
        "Splits", train=str, val=str, test=str):
    """Step 4: train / val / test parquet with the gaps checked."""
    from fraud_ml import steps
    p = steps.time_split(run_id, version, label_as_of, feature_service)
    return p["train"], p["val"], p["test"]


@component
def train(run_id: str, train_path: str, val_path: str, feature_service: str, num_workers: int, executors: int,
          max_depth: int, learning_rate: float, n_estimators: int) -> str:
    """Step 5: baseline + distributed XGBoost; returns the model directory."""
    from fraud_ml import steps
    return steps.train(run_id, train_path, val_path, feature_service, num_workers, executors,
                       {"max_depth": max_depth, "learning_rate": learning_rate, "n_estimators": n_estimators})


@component
def evaluate(run_id: str, model_dir: str, val_path: str, test_path: str, feature_service: str,
             min_pr_auc_gain: float, metrics: dsl.Output[dsl.Metrics]) -> bool:
    """Step 6: thresholds, test metrics vs baseline and production; returns `passed` (also shown in the KFP UI)."""
    import json
    from fraud_ml import steps
    passed = steps.evaluate(run_id, model_dir, val_path, test_path, feature_service, min_pr_auc_gain)
    with steps.s3().open_input_stream(f"{model_dir.removeprefix('s3a://')}/evaluation.json") as f:
        ev = json.loads(f.read())
    for k in ("pr_auc", "roc_auc", "recall_at_1pct_fpr", "precision_at_t_high", "decline_rate", "step_up_rate"):
        metrics.log_metric(k, round(ev["test"][k], 5))
        metrics.log_metric(f"baseline_{k}", round(ev["baseline"][k], 5))
    metrics.log_metric("passed", int(passed))
    return passed


@component
def register(run_id: str, model_dir: str, test_path: str, label_as_of: str, feature_service: str) -> int:
    """Step 7: MLflow model version, alias challenger (production on the first run)."""
    from fraud_ml import steps
    return steps.register(run_id, model_dir, test_path, True, label_as_of, feature_service)


def env(task, memory: str = "2Gi", cpu: str = "1"):
    """Secrets as env (never in the pipeline spec), the MLflow URL, the pod IP for Spark, resource limits."""
    kubernetes.use_secret_as_env(task, "s3-credentials", {"access_key": "AWS_ACCESS_KEY_ID", "secret_key": "AWS_SECRET_ACCESS_KEY"})
    kubernetes.use_secret_as_env(task, "feast-credentials", {"ml_db_password": "ML_DB_PASSWORD", "redis_password": "REDIS_PASSWORD"})
    kubernetes.use_field_path_as_env(task, "POD_IP", "status.podIP")
    kubernetes.use_field_path_as_env(task, "POD_NAMESPACE", "metadata.namespace")
    task.set_env_variable("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    task.set_env_variable("ML_IMAGE", IMAGE)
    # request < limit: without a request Kubernetes reserves the whole limit, and next to KFP's own pods (3.5 GiB
    # requested) a 5 GiB step cannot be scheduled on the single 7.75 GiB node. One run at a time fits the limits.
    task.set_memory_request("1Gi").set_memory_limit(memory).set_cpu_limit(cpu).set_caching_options(False)
    return task


@dsl.pipeline(name="train_fraud_model", description="Card fraud model: Feast -> distributed XGBoost -> MLflow")
def train_fraud_model(label_as_of: str = "2026-10-15", negative_rate: float = 0.02,
                      feature_service: str = "fraud_model_v1", num_workers: int = 2, executors: int = 2,
                      max_depth: int = 6, learning_rate: float = 0.08, n_estimators: int = 400,
                      min_pr_auc_gain: float = 0.005):
    """Wire the 7 steps; register only runs when evaluation passed."""
    # steps 1-2 join ~8M-row tables in a local[2] Spark: with 1 CPU / 2 GiB step 1 spilled for over 50 minutes.
    # 3-7 hold the 1.07M-row training set (or val + test) in pandas; 3-4 read it with Spark (deletion vectors), 5 runs
    # the Spark driver of the distributed training.
    s1 = env(load_labels(label_as_of=label_as_of, negative_rate=negative_rate, feature_service=feature_service,
                         kfp_run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER), "3Gi", "2")
    s2 = env(get_training_data(run_id=s1.output, label_as_of=label_as_of, feature_service=feature_service), "5Gi", "2")
    s3 = env(validate_data(run_id=s1.output, version=s2.output, feature_service=feature_service), "4Gi")
    s4 = env(time_split(run_id=s1.output, version=s2.output, label_as_of=label_as_of,
                        feature_service=feature_service).after(s3), "4Gi")
    s5 = env(train(run_id=s1.output, train_path=s4.outputs["train"], val_path=s4.outputs["val"],
                   feature_service=feature_service, num_workers=num_workers, executors=executors,
                   max_depth=max_depth, learning_rate=learning_rate, n_estimators=n_estimators), "3Gi")
    s6 = env(evaluate(run_id=s1.output, model_dir=s5.output, val_path=s4.outputs["val"], test_path=s4.outputs["test"],
                      feature_service=feature_service, min_pr_auc_gain=min_pr_auc_gain), "3Gi")
    with dsl.If(s6.outputs["Output"] == True):  # noqa: E712 (KFP condition, not a Python comparison)
        env(register(run_id=s1.output, model_dir=s5.output, test_path=s4.outputs["test"], label_as_of=label_as_of,
                     feature_service=feature_service), "2Gi")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--compile", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--label-as-of", default="2026-10-15")
    a = ap.parse_args()
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "train_fraud_model.yaml")
    compiler.Compiler().compile(train_fraud_model, out)
    print(f"compiled {out}")
    if a.run:
        import kfp
        run = kfp.Client(host=KFP_HOST).create_run_from_pipeline_package(
            out, arguments={"label_as_of": a.label_as_of}, experiment_name="card_fraud", enable_caching=False)
        print(f"run {run.run_id}")
