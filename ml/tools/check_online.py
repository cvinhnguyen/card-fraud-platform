"""After materialization: the cards active on `ds` must be in Redis with exactly the stats DP3 wrote for them.

Reads a sample of the offline rows of ds (event_timestamp = ds + 1, the night after the activity) with deltalake,
then the same cards from the online store through Feast; a missing card, or a different value for a card with no
newer offline row, fails the task.

Usage: python ml/tools/check_online.py --repo ml/feature_repo --ds 2026-10-15 [--sample 200]
"""
import argparse
import datetime as dt
import os

import pyarrow.compute as pc
from deltalake import DeltaTable
from feast import FeatureStore

FEATURES = ["txn_count_30d", "dispute_count_180d", "avg_amount_90d"]


def main(repo: str, ds: str, sample: int) -> None:
    """Compare a sample of offline rows of ds with what Redis returns for the same cards."""
    day = dt.datetime.fromisoformat(ds) + dt.timedelta(days=1)
    opts = {"AWS_ENDPOINT_URL": "http://lakehouse-s3:8333", "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"], "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"]}
    data = DeltaTable("s3://lakehouse/gold/feat_card_txn_stats_daily", storage_options=opts).to_pyarrow_dataset()
    rows = data.to_table(columns=["card_id", *FEATURES], filter=pc.field("event_timestamp") == day).to_pandas()
    assert len(rows), f"no offline rows for {ds}: DP3 did not write the day"
    rows = rows.sample(min(sample, len(rows)), random_state=1).set_index("card_id")
    online = FeatureStore(repo_path=repo).get_online_features(
        features=[f"card_txn_stats_daily:{f}" for f in FEATURES],
        entity_rows=[{"card_id": c} for c in rows.index]).to_df().set_index("card_id")
    missing = online[FEATURES[0]].isna().sum()
    diff = ((online[FEATURES].round(6) != rows[FEATURES].round(6)) & rows[FEATURES].notna()).any(axis=1)
    # A card active again after ds may already hold that newer row online (backfills): only cards without a newer
    # offline row must match exactly.
    newer = set(data.to_table(columns=["card_id"], filter=(pc.field("event_timestamp") > day)
                              & pc.field("card_id").isin(list(rows.index))).column("card_id").to_pylist())
    stale = int((diff & ~rows.index.isin(newer)).sum())
    print(f"{len(rows)} cards active on {ds}: {missing} missing online, {int(diff.sum())} with other values "
          f"({len(newer)} have a newer day), {stale} stale")
    assert missing == 0, f"{missing} of {len(rows)} cards missing from the online store"
    assert stale == 0, f"{stale} of {len(rows)} cards hold values that differ from DP3's row of {ds}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repo", default="feature_repo")
    ap.add_argument("--ds", required=True)
    ap.add_argument("--sample", type=int, default=200)
    a = ap.parse_args()
    main(a.repo, a.ds, a.sample)
