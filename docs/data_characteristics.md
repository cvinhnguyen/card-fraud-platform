# Data characteristics

What the source data looks like at the default volume (~1 GB), and which problems it carries on purpose.
Every number below is printed by `generator/check_data.py` (69 checks, all pass; each one is a `check()` call
with an expected range,
[`generator/check_data.py` L30-L35](https://github.com/cvinhnguyen/card-fraud-platform/blob/35d4295a1e5a409fa242bc6aa9eff9b06038c111/generator/check_data.py#L30-L35)) on the data from
`generator/generate.py` with `seed: 42`, period 2024-01-01 to 2026-10-15. These are the "before" numbers
the later phases are measured against.

## Datasets

| Dataset | Where it lands | Files | Rows | Size |
|---|---|---:|---:|---:|
| customers (SCD2 source, 100,000 customers) | Postgres `core_banking` | 1 | 193,207 | |
| accounts | Postgres | 1 | 100,000 | |
| cards | Postgres | 1 | 130,120 | |
| merchants (5,000 domestic + 200 foreign online) | Postgres | 1 | 5,200 | |
| disputes (fraud labels) | Postgres | 1 | 31,364 | |
| ref_province_mapping (63 → 34) | Postgres | 1 | 63 | |
| *core banking tables total* | | 6 | 459,954 | 11.6 MB |
| transactions (nightly export) | S3 `core-banking-export/transactions/export_date=*/` | 1,019 | 8,159,516 | 327.6 MB |
| card_authorizations | Kafka topic, key `card_id` | 1,019 | 8,119,499 | 402.2 MB |
| app_events | Kafka topic, key `customer_id` | 1,019 | 4,822,745 | 224.0 MB |
| *total* | | | | *~966 MB* |

Sizes are Parquet on disk under `data/source/`. `_truth/ato_sessions.parquet` (21,789 ATO sessions) is
ground truth for evaluation only and is never loaded into the platform.

Generate: ~7 min, 0.31 GB RAM. Check: 1 to 3.5 min (slower when the Spark containers are up), 4.24 GB RAM peak (measured before the `currency` column was added to the check).

## Problems in the batch sources

| Problem | How it shows up | Measured | Fixed in |
|---|---|---|---|
| Duplicate rows | Nightly export re-run after a timeout: same `transaction_id`, later `created_ts` ([`generate.py` L328-L330](https://github.com/cvinhnguyen/card-fraud-platform/blob/35d4295a1e5a409fa242bc6aa9eff9b06038c111/generator/generate.py#L328-L330)) | 159,990 extra rows = 2.00% of 7,999,526 unique; 100% of duplicates have a later `created_ts` | P4 dedup (keep latest) |
| Merchant skew | 10 super merchants (e-wallet top-up, ride-hailing, marketplace, convenience chains) | top-10 share 0.7000; biggest / median merchant = 10,012× | P4 salting / AQE skew join |
| Cross-border online payments | 200 foreign merchants (digital content, games, online stores, travel, software; US / SG / IE / CN / GB), online only, `currency = USD`, `amount_vnd` already converted; `city` NULL | 4.01% of transactions; 100% online with a merchant outside VN | Layer 2 `foreign_txn_ratio_90d` |
| City skew | Shares on the 34 new units | Ho Chi Minh 0.5993, Ha Noi 0.1802, Da Nang 0.0500 | P4 / P5 partitioning, P6 hot key |
| Province reorganization (63 → 34, from 2025-07-01) | Old names before the cutoff; 5% of legacy terminals keep sending old names after it ([`generate.py` L83-L93](https://github.com/cvinhnguyen/card-fraud-platform/blob/35d4295a1e5a409fa242bc6aa9eff9b06038c111/generator/generate.py#L83-L93)) | old-only names 19.62% before, 0.99% after; raw "Ho Chi Minh" share 0.4797 before the cutoff (a fake jump to 0.60 if not normalized) | P4 normalize via `ref_province_mapping` |
| Schema evolution | `auth_method`, `three_ds_version` exist only in files from 2024-07-01 ([`generate.py` L322-L324](https://github.com/cvinhnguyen/card-fraud-platform/blob/35d4295a1e5a409fa242bc6aa9eff9b06038c111/generator/generate.py#L322-L324)) | 100% of earlier files lack the columns, 100% of later files have them; `3ds_app` 30.00% of ecom, 3DS 2.2.0 85.03% | P3 / P4 `mergeSchema` |
| High cardinality | `terminal_id` on POS / ATM, null for ecom | 26,863 distinct | P5 bucketing, approx distinct |
| SCD2 | Customers move (5%/yr), get rescored (10%/yr); on 2025-07-01 the core system relabels old province names | 56.45% of customers have >1 version; 16,635 spurious relabel versions (95.09% of old-name customers) | P4 SCD2 / snapshot logic |
| As-of joins | A moved customer pays in the home unit valid at `txn_ts` | 97.08% of 1,107,271 post-move transactions | P4 point-in-time join |
| Label delay | Disputes arrive 1 to 60 days after the transaction (truncated log-normal, median 14) | median 13.65 days; last-30-day fraud rate is 0.53× the mature rate (expected 0.48) | P4 / Layer 2 maturity window of 60 days |
| Trend: ATM decline | ATM share of card payments falls 17%/yr | 2025 / 2024 = 0.8327 | Layer 2 drift monitoring |

Labels: fraud rate on mature transactions 0.0030; ecom fraud rate is 4.18× non-ecom, and foreign payments 3.03× domestic ecom (learnable signals).

![Label delay histogram](images/label_delay.svg)

Disputes peak 7 to 10 days after the transaction (median 13.7) and 5% arrive after day 40 (p95 39.8 days; the configured 45 applies before the 60-day truncation). Transactions from the last 60 days are still missing part of their fraud labels, so training data must be older than 60 days. Redraw with `python docs/diagrams/label_delay.py`.

The delay is drawn per dispute, and disputes reported after the end of the period are left out,
[`generator/generate.py` L334-L349](https://github.com/cvinhnguyen/card-fraud-platform/blob/35d4295a1e5a409fa242bc6aa9eff9b06038c111/generator/generate.py#L334-L349):

```python
def gen_disputes(rng, cfg, disputed: pd.DataFrame) -> pd.DataFrame:
    """Problem: label delay. A dispute is reported 1-60 days after the payment (truncated log-normal).
    Disputes reported after the end of the period do not exist yet, so recent months look clean."""
    ld = cfg["labels"]["dispute_delay_days"]
    mu, sigma = np.log(ld["median"]), (np.log(ld["p95"]) - np.log(ld["median"])) / 1.645
    delay = rng.lognormal(mu, sigma, len(disputed))
    while (bad := (delay < ld["min"]) | (delay > ld["max"])).any():   # redraw = truncate
        delay[bad] = rng.lognormal(mu, sigma, bad.sum())
    reported = disputed.txn_ts + pd.to_timedelta(delay * 86400, unit="s").round("s")
    fraud = disputed.is_confirmed_fraud.to_numpy()
    reason = np.where(fraud, np.where(disputed.channel == "ecom", "fraud_card_not_present", "fraud_card_present"),
                      rng.choice(["not_received", "duplicate_charge", "amount_differs"], len(disputed)))
    out = pd.DataFrame({"dispute_id": tokens("dsp", rng, len(disputed)), "transaction_id": disputed.transaction_id.to_numpy(),
                        "reported_ts": reported.to_numpy(), "reason_code": reason, "is_confirmed_fraud": fraud})
    end = pd.Timestamp(cfg["period"]["end"]) + pd.Timedelta(days=1)
    return out[out.reported_ts < end].sort_values("reported_ts", ignore_index=True)
```

## Problems in the streams

Both streams carry two timestamps: event time (`auth_ts`, `event_ts`) and arrival time (`received_ts`).
`producer.py` sends in `received_ts` order
([`generator/producer.py` L27-L33](https://github.com/cvinhnguyen/card-fraud-platform/blob/35d4295a1e5a409fa242bc6aa9eff9b06038c111/generator/producer.py#L27-L33)),
so Kafka sees the disorder.

### card_authorizations

| Problem | How it shows up | Measured | Fixed in |
|---|---|---|---|
| Late events | 10% of POS payments come from offline terminals and arrive 5 to 30 min late | 191,295 late = 2.39% of all auths (9.98% of POS); on-time delay ≤ 2 s | P6 watermark 5 s + `allowed_lateness` 30 min + side output |
| Duplicates | Network retry without an ack: same `auth_id`, `received_ts` 1 to 10 s later | 1.50% extra rows; duplicates differ only in `received_ts` | P6 keyed dedup on `auth_id` with state TTL, before any count |
| Join key to batch | `auth_id` shares the counter with `transaction_id` | 7,999,526 unique auths = unique transactions, 100% match | Layer 2 auth → transaction → dispute label |
| Device signal | `device_id` on ecom only; fraud always uses a device new to the card | 170,459 distinct devices; new device: fraud 100%, legit 3.37% | Layer 2 realtime feature |

The new device of a fraud, the late POS events and the retries are all made in `gen_auths()`,
[`generator/generate.py` L370-L390](https://github.com/cvinhnguyen/card-fraud-platform/blob/35d4295a1e5a409fa242bc6aa9eff9b06038c111/generator/generate.py#L370-L390):

```python
    # Fraudsters pay online from their own phone, so the card sees a device it never used (a realtime signal).
    new = fraud & (channel == "ecom")
    dev[new] = tokens("dev", rng, int(new.sum()))
    delay = rng.uniform(*st["network_delay_s"], n)
    late = (channel == "pos") & (rng.random(n) < st["late_share_of_pos"])
    delay[late] = rng.uniform(*st["late_delay_min"], late.sum()) * 60
    df = pd.DataFrame({
        "auth_id": txns.transaction_id.str.replace("txn_", "auth_", regex=False).to_numpy(),
        "card_id": txns.card_id.to_numpy(),
        "merchant_id": txns.merchant_id.to_numpy(),
        "amount_vnd": txns.amount_vnd.to_numpy(),
        "channel": channel,
        "terminal_id": txns.terminal_id.to_numpy(),
        "device_id": pd.Series(dev, dtype="string").where(channel == "ecom"),
        "geo_city": txns.txn_city.to_numpy(),
        "auth_ts": txns.txn_ts.to_numpy(),
        "received_ts": txns.txn_ts.to_numpy() + pd.to_timedelta(np.round(delay * 1000), unit="ms"),
    })
    dup = df.iloc[rng.choice(n, round(n * st["duplicate_rate"]), replace=False)].copy()
    dup["received_ts"] += pd.to_timedelta(np.round(rng.uniform(*st["retry_delay_s"], len(dup)) * 1000), unit="ms")
    return pd.concat([df, dup], ignore_index=True).sort_values("received_ts", ignore_index=True)
```

### app_events

2,199,812 sessions: 2,178,023 legit, 14,602 `new_device` ATO, 7,187 `remote_access` ATO.

| Problem | How it shows up | Measured | Fixed in |
|---|---|---|---|
| Out-of-order events | App uploads in parallel: an event reaches Kafka after the next event of its session ([`app_events.py` L146-L171](https://github.com/cvinhnguyen/card-fraud-platform/blob/35d4295a1e5a409fa242bc6aa9eff9b06038c111/generator/app_events.py#L146-L171)) | exactly 15.00%; max disorder 20 s; in-order delay ≤ 1 s | P6 event-time watermark 30 s, buffer per `session_id`, sort by `event_ts` (CEP / `KeyedProcessFunction`) |
| Sequence rule broken by disorder | `new_device` ATO: password_reset → add_payee → transfer within 10 min | 1,205 of 4,034 ATO sequences look broken in arrival order | same as above |
| Decision 2345/QĐ-NHNN (from 2024-07-01) | Biometric check on a new device, a transfer ≥ 10M VND, or a day total > 20M VND; a failed check ends the session | all rule checks 100%; pass rate legit 0.9895, ATO 0.1052 | P6 rule / Layer 2 feature |
| Split transfers | `remote_access` ATO (malware on the victim's phone) keeps each transfer in [9M, 10M) | 16,673 remote ATO vs 4,836 legit transfers in that band | Layer 2 `transfer_9m_10m_count_1d` |
| Trend: QR at shops | `qr_merchant` share of transfers grows 15%/yr | 2025 / 2024 = 1.1497 | Layer 2 drift monitoring |

## Scaling

Volume lives in `generator/config.yaml` (`volume`). `generate.py --scale 0.05` gives a quick smoke test.
Pandas-based checks stop at about this size (4.24 GB RAM); the 100 GB run (P9) checks with Spark.
