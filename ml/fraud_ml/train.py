"""Models: a logistic regression baseline and the main model, XGBoost trained distributed on Spark.

XGBoost runs through xgboost.spark: `num_workers` Spark tasks train one model together (each holds a shard of the
rows, histograms are all-reduced every iteration). With the session on Kubernetes (fraud_ml.spark) the workers are
executor pods. Negatives of the train split were downsampled to `negative_rate`; their sample weight 1 / rate
makes the predicted probabilities match the real fraud rate, so no `scale_pos_weight` (it would distort them).
The validation split is passed as the early-stopping set (`validation_indicator_col`).
"""
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import StandardScaler

from fraud_ml.data import KEY, LABEL, WEIGHT

XGB_PARAMS = {"n_estimators": 400, "max_depth": 6, "learning_rate": 0.08, "subsample": 0.8,
              "colsample_bytree": 0.8, "min_child_weight": 5.0, "early_stopping_rounds": 30,
              "eval_metric": "aucpr", "tree_method": "hist"}


@dataclass
class FraudModelTrainer:
    """Fits both models on the train split; the booster is what gets registered and served."""

    features: list[str]
    xgb_params: dict = field(default_factory=lambda: dict(XGB_PARAMS))
    num_workers: int = 2
    seed: int = 42

    def fit_baseline(self, train: pd.DataFrame) -> Pipeline:
        """Logistic regression with class weights: simple, explainable, the bar the main model must clear."""
        model = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(),
                              LogisticRegression(class_weight="balanced", max_iter=2000, random_state=self.seed))
        return model.fit(train[self.features], train[LABEL])

    def fit_xgboost(self, spark, train: pd.DataFrame, val: pd.DataFrame) -> xgb.Booster:
        """Distributed XGBoost on Spark over train + val (val only as the early-stopping set)."""
        from xgboost.spark import SparkXGBClassifier
        cols = [*self.features, LABEL, WEIGHT, "is_val"]
        # Sorted, then shuffled with a fixed seed. Sorted: the point-in-time join returns rows in any order, the row
        # order decides which worker gets which row, and subsample draws by position within a worker, so the same
        # rows in another order gave another booster (best iteration 93 vs 28 on one test set). Shuffled:
        # xgboost.spark builds the DMatrix per Arrow batch, and a batch without any training row fails (KeyError
        # 'values' with a feature-column list); train-then-val order made the later batches val only.
        data = (pd.concat([train.assign(is_val=False), val.assign(is_val=True)], ignore_index=True)
                .sort_values(KEY, ignore_index=True)
                .sample(frac=1, random_state=self.seed, ignore_index=True)[cols])
        data[self.features] = data[self.features].astype("float64")   # NaN = missing for XGBoost
        sdf = spark.createDataFrame(data).repartition(self.num_workers)
        clf = SparkXGBClassifier(features_col=self.features, label_col=LABEL, weight_col=WEIGHT,
                                 validation_indicator_col="is_val", num_workers=self.num_workers,
                                 random_state=self.seed, **self.xgb_params)
        booster = clf.fit(sdf).get_booster()
        booster.feature_names = list(self.features)
        return booster

    @staticmethod
    def predict(booster: xgb.Booster, df: pd.DataFrame) -> np.ndarray:
        """Fraud probability per row (NaN features = missing)."""
        x = df[booster.feature_names].astype("float64")
        return booster.inplace_predict(x.to_numpy(), missing=np.nan)
