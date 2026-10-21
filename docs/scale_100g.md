# The 100 GB run

The rubric asks for at least 100 GB of offline source data, so that optimizations show. Everything else in this repo
runs on the ~1 GB dataset (`generator/generate.py`), which stays as it is; the 100 GB dataset is a separate run of
the same generator code with larger volumes, kept under its own prefixes.

| | 1 GB dataset | 100 GB dataset |
|---|---|---|
| Generator | `generator/generate.py` (one process, day by day) | `generator/generate_scale.py` (parallel, by day and chunk) |
| Where | Postgres `core_banking` + `s3://core-banking-export/transactions/` | `s3://core-banking-export/scale_100g/transactions/` + `merchants.parquet` |
| Transactions | 8.0M (+2% duplicates) | 4.05B (+2% duplicates) |
| Customers / cards / merchants | 100k / 130k / 5.2k | 2M / 2.6M / 20.2k |
| Silver | `silver.stg_transactions` | the skew benchmark reads the raw export; the dedup was measured on a 20-date slice (below) |

Same problems as the 1 GB data, from the same functions: merchant skew (top 10 merchants = 70% of rows), city skew
(HCMC 60%), cross-border payments, old province names after 2025-07-01, 2% export duplicates, the auth columns from
2024-07-01 (schema evolution).

```bash
python generator/generate_scale.py --transactions 4050000000 --customers 2000000 --merchants 20000 --workers 5
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit \
    --conf spark.sql.shuffle.partitions=2000 --conf spark.io.compression.codec=zstd \
    --conf spark.cleaner.periodicGC.interval=5min /opt/jobs/scale_100g.py
```

## Skew join benchmark

### Why the raw export and two columns

The first plan ran the silver transformation (dedup on `transaction_id`, a window over every row and every column)
on the 100 GB and then the benchmark on the result. A probe on 10 of the export dates (~1% of the data) took about an
hour: the Docker disk was then on a USB HDD, and one 2 GB executor for 8 cores spilled constantly while it shuffled
full rows. All of it would have taken about 4 days, with shuffle files + output (180-220 GB) close to the free disk
(201 GB). The benchmark therefore reads the raw export, and only `merchant_id` + `amount_vnd` (Parquet column
pruning: Spark reads 2 of the 13 column chunks of every row group). The 2% duplicates do not change which merchant is
hot. Shuffle files are zstd-compressed and cleaned every 5 minutes, and a watcher stops the job if the disk falls
below 25 GB.

### Results

4,131,056,674 rows (4.05B + 2% duplicates), biggest merchant **23.9%** of all rows (~990M rows), top 10 merchants
70.0%; one executor, 8 cores, 2 GB heap; 2,000 shuffle partitions; one run per method (a run is a full pass).

| Method | Wall time | Slowest task | Median task | Biggest shuffle read | |
|---|---:|---:|---:|---:|---|
| baseline (sort-merge, AQE off) | **did not finish** | | | | `OutOfMemoryError: Java heap space` after 106 min, in the reduce task that holds the hot merchant |
| salting (32 buckets) | **9,060 s** (151 min) | 377 s | 16 s | 181 MB | hot keys spread over 32 tasks |
| AQE (Spark defaults: skew 256 MB, target 64 MB) | **6,784 s** (113 min) | 477 s | 94 s | 81 MB | 2,000 shuffle partitions coalesced to 218 tasks, the hot ones split |
| broadcast | **2,866 s** (48 min) | 26 s | 20 s | 0 (no shuffle) | merchants (20k rows) copied to the executor; the scan is the whole cost |

With ~24% of 4.1B rows behind one key, the baseline cannot finish on this executor at all: one task must sort ~990M
rows alone while 1,999 others finish in seconds. The 1 GB run only hinted at this.

> [!note] A mistake of the first attempt
> The first AQE run also ran out of memory, after 67 min. `skew_join.configure` set the target partition size
> equal to the skew threshold (right for the 1 GB run, where both are 8 MB), so at 100 GB each task aimed at 256 MB
> instead of Spark's default 64 MB. The run in the table uses Spark's defaults. The broadcast of that attempt failed
> only because the executor was still broken from the OOM.

Ranking at 100 GB: broadcast 48 min < AQE 113 min < salting 151 min < baseline (fails).

- Broadcast wins whenever the small side fits in memory (20k merchants): the 4.1B rows are never shuffled, every
  task does the same work (slowest 26 s vs median 20 s), and the time is the scan of the data.
- AQE needs no code change. At run time it saw the hot partitions, split them and merged the many small ones. Some
  imbalance remains (slowest task 5x the median), but nothing runs out of memory.
- Salting works too and costs the most: a random salt column on every row, the merchant table copied 32 times, and
  code that has to know which keys are hot.
- The baseline fails outright.

Each method ran once (48 to 151 min per run), so small differences would mean nothing; the differences here are
large. The order is the same as in the 1 GB benchmark ([spark_optimization.md](spark_optimization.md): baseline
51 s, salting 46 s, AQE 38 s, broadcast 24 s), where the baseline was only 2x slower than broadcast.

## Dedup at scale

The silver dedup (latest `created_ts` per `transaction_id`, as in `pipelines/spark/build_silver.py`) was measured on a slice of
the 100 GB dataset, with `pipelines/spark/scale_dedup.py`. The full 100 GB plus its silver copy (~190 GB) does not fit the free
disk since the Docker disk moved to the C: SSD. The slice is every 51st day of the period at full daily volume:
20 export dates spread over 2024-2026, so it has both schemas (before and after 2024-07-01) and the trend.
Generating it with the same seed writes the same files as the full run (each day and chunk has its own random stream).

```bash
python generator/generate_scale.py --transactions 4050000000 --every 51 --workers 3   # 81,072,211 rows, 2.0 GiB, 8 min
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit \
    --conf spark.io.compression.codec=zstd --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=16m \
    /opt/jobs/scale_dedup.py --method broadcast --batch-days 2
```

Two choices are measured:

- Scope: global runs all dates in one job, as a one-shot backfill would run it; batched runs a few export dates per
  job, appended to the same Delta table. Batching is correct because a duplicate comes from re-running one nightly
  export, so both copies always share an `export_date`; DP2 relies on the same fact every night, when it dedups only
  the new export dates.
- Method: window is `row_number()` over `transaction_id`, the `build_silver.py` code, which shuffles and sorts every
  row with every column. Broadcast uses the fact that only 2% of the ids are duplicated: the job finds them from the
  `transaction_id` column alone, passes every other row through a broadcast anti join (no shuffle, no sort) and runs
  the window on the duplicated rows only.

Every run wrote the same 79,482,558 rows from 81,072,211 (1,589,653 duplicates removed) with the same checksum.
Setup as in the rest of P9: one 2 GB executor, 8 cores, Docker disk on the SSD; one run each.

| Method | Scope | AQE partition target | Time (20 dates) | Per date | Spill (memory / disk) |
|---|---|---:|---:|---:|---:|
| window | global | 64 MB (default) | 249 s | 12.5 s | 34.3 / 5.6 GB |
| window | batched, 2 dates | 64 MB (default) | 237 s | 11.9 s | 18.8 / 3.1 GB |
| window | global | 16 MB | 216 s | 10.8 s | 17.6 / 2.8 GB |
| window | batched, 2 dates | 16 MB | 233 s | 11.7 s | 9.4 / 1.5 GB |
| broadcast | global | 16 MB | **180 s** | **9.0 s** | 16.5 / 0.3 GB |
| broadcast | batched, 2 dates | 16 MB | 201 s | 10.0 s | **0.4 / 0.0 GB** |
| broadcast | batched, 5 dates | 16 MB | 200 s | 10.0 s | 16.1 / 0.3 GB |

The slice shows four things:

- The disk mattered most. The probe (window, global, 10 dates) ran for about an hour on the USB HDD; on the SSD the
  window job needs 4.2 min for 20 dates (global) and 2.3 min for 10 (batched).
- Smaller AQE partitions halve the spill. By default AQE merges shuffle partitions up to 64 MB of compressed data,
  which unpacks to more than one task's share of a 2 GB executor shared by 8 cores, so every sort spills. A 16 MB
  target halves the spill, and the global window gets 13% faster.
- Broadcast is the fastest method: 28% faster than the default window job. Most of its time is reading and writing
  the data; finding the duplicated ids is the only shuffle left.
- Global is a little faster than batched on 2 GB, because each batch pays for its own Spark jobs, file listing and
  Delta commit. Global cannot scale, though: at 100 GB its shuffle is the whole dataset again, and the 81M duplicated
  ids (2% of 4.1B) are too many to broadcast to a 2 GB executor. Batched with broadcast barely spills, its broadcast
  stays at one batch's ids, and its time grows linearly: 116 s for 10 dates and 201 s for 20, so about 31 s of fixed
  cost plus 8.5 s per date. Five dates per batch were no faster and spilled again.

For the whole 100 GB (1,019 export dates) that is about 31 s + 1,019 x 8.5 s, roughly **2.4 hours** on this machine,
against the ~4 days estimated from the HDD probe. This is an extrapolation from 20 dates, not a measured 100 GB run.
