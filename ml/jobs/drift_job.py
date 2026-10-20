"""Drift job of the Airflow DAG `drift_monitoring`: the live period's features from the Feast offline store, scored by
the production model, compared with the ground truth and with the training profile; results to MLflow.

  inputs   payments of the last --days days (the ground-truth label table: transaction_id, is_fraud), their
           request fields (silver), point-in-time features (Feast, same feature service as the model)
  outputs  PR-AUC / recall at 1% FPR / fraud rate on the window, PSI of every feature and of the score vs the
           model's reference profile -> one MLflow run per check (experiment card_fraud_monitoring)
           and a JSON verdict (stdout, last line) for the DAG's branch: retrain when any PSI > threshold or the
           PR-AUC fell more than --max-drop below the model's own test PR-AUC

Usage (Airflow feast venv, or the ml image): python ml/jobs/drift_job.py --end 2026-11-15 --days 7
"""
import argparse
import datetime as dt
import json
import os
import sys

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # ml/, for fraud_ml
from fraud_ml.data import AUTHORIZATIONS, NO_DEVICE, TRANSACTIONS   # noqa: E402
from fraud_ml.evaluate import recall_at_fpr   # noqa: E402
from fraud_ml.profile import psi_against   # noqa: E402

GROUND_TRUTH = "s3a://lakehouse/ml/ground_truth/labels.parquet"


def live_window(spark, end: dt.date, days: int):
    """Entity rows of the window: ground truth + request fields + device, timestamp = payment - 1 ms (as training)."""
    start = end - dt.timedelta(days=days - 1)
    txn = (spark.read.format("delta").load(TRANSACTIONS)
           .where((F.col("txn_date") >= F.lit(start)) & (F.col("txn_date") <= F.lit(end)))
           .select("transaction_id", "card_id", "txn_ts", "amount_vnd", "channel", "currency", "auth_method"))
    dev = (spark.read.format("delta").load(AUTHORIZATIONS)
           .select(F.regexp_replace("auth_id", "^auth_", "txn_").alias("transaction_id"), "device_id")
           .dropDuplicates(["transaction_id"]))
    return (spark.read.parquet(GROUND_TRUTH).join(txn, "transaction_id").join(dev, "transaction_id", "left")
            .withColumn("device_id", F.coalesce("device_id", F.lit(NO_DEVICE)))
            .withColumn("request_ts", F.col("txn_ts"))
            .withColumn("event_timestamp", F.col("txn_ts") - F.expr("INTERVAL 1 MILLISECOND")).drop("txn_ts"))


def main(end: dt.date, days: int, psi_threshold: float, max_drop: float) -> dict:
    """Compute, push and return the verdict."""
    import mlflow
    from feast import FeatureStore
    from fraud_ml.registry import MODEL_NAME, ModelRegistry
    from fraud_ml.spark import session
    from fraud_ml.steps import REPO, feature_names
    reg = ModelRegistry()
    version = reg.version_of("production")
    mv = reg.client.get_model_version(MODEL_NAME, str(version))
    booster = reg.load_booster("production")
    profile = json.loads(mlflow.artifacts.load_text(f"runs:/{mv.run_id}/reference_profile.json"))
    service = mv.tags.get("feature_service", "fraud_model_v1")
    spark = session("drift_job", driver_memory="1500m")
    store = FeatureStore(repo_path=REPO)
    df = store.get_historical_features(entity_df=live_window(spark, end, days),
                                       features=store.get_feature_service(service)).to_df()
    spark.stop()
    feats = feature_names(service)
    df["score"] = booster.inplace_predict(df[booster.feature_names].astype("float64").to_numpy(), missing=np.nan)
    y = df.is_fraud.to_numpy()
    from sklearn.metrics import average_precision_score
    pr_auc = float(average_precision_score(y, df.score)) if y.sum() else float("nan")
    psi = psi_against(profile, df[[*feats, "score"]])
    worst = max(psi, key=psi.get)
    test_pr_auc = float(mv.tags.get("test_pr_auc", "nan"))
    verdict = {"model_version": version, "window_end": str(end), "days": days, "rows": len(df),
               "fraud_rate": float(y.mean()), "pr_auc": round(pr_auc, 4), "test_pr_auc": test_pr_auc,
               "recall_at_1pct_fpr": round(recall_at_fpr(y, df.score.to_numpy(), 0.01), 4) if y.sum() else None,
               "worst_input": worst, "worst_psi": round(psi[worst], 4),
               "retrain": psi[worst] > psi_threshold or (test_pr_auc - pr_auc) > max_drop}
    log_to_mlflow(verdict, psi)
    return verdict


def log_to_mlflow(v: dict, psi: dict[str, float]) -> None:
    """One MLflow run per check in experiment card_fraud_monitoring: the history of drift and live performance."""
    import mlflow
    mlflow.set_experiment("card_fraud_monitoring")
    with mlflow.start_run(run_name=f"drift-{v['window_end']}", tags={"model_version": str(v["model_version"])}):
        mlflow.log_params({"window_end": v["window_end"], "days": v["days"], "model_version": v["model_version"]})
        mlflow.log_metrics({k: v[k] for k in ("pr_auc", "test_pr_auc", "fraud_rate", "worst_psi")
                            if isinstance(v[k], (int, float))} | {"retrain": float(v["retrain"]), "rows": v["rows"]}
                           | {f"psi.{k}": round(x, 5) for k, x in psi.items()})


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--end", type=dt.date.fromisoformat, required=True)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--psi-threshold", type=float, default=0.25)
    ap.add_argument("--max-drop", type=float, default=0.05)
    a = ap.parse_args()
    print(json.dumps(main(a.end, a.days, a.psi_threshold, a.max_drop)))
