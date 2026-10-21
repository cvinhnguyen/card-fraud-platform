# Spark optimization

## Merchant join skew (`pipelines/spark/skew_join.py`)

```bash
docker compose -f infra/docker-compose.yaml stop spark-thrift      # it holds worker cores
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/skew_join.py --replicate 4
```

`stg_transactions JOIN merchants ON merchant_id`, full transaction rows, result written to the `noop` sink. The biggest
merchant (an e-wallet top-up) has 23.9% of the rows and the top 10 have 70%. Setup: 1 worker, 8 cores, 2 GB executor;
the data is unioned 4 times (32M rows, 2 GB of shuffle) so tasks are long enough to measure. Each method runs once to
warm up, then 3 times; the table shows the median. Task numbers come from the slowest stage of the last run (Spark REST API).

| Method | What changes | Median (s) | Runs (s) | Slowest task | Median task |
|---|---|---:|---|---:|---:|
| baseline | sort-merge join, AQE off, broadcast off | 51.2 | 53.5, 51.2, 48.9 | 17.4 s | 0.15 s |
| salting | merchant_id + random salt 0..31, merchants copied 32 times | 45.5 | 45.8, 45.5, 44.8 | 2.2 s | 1.80 s |
| AQE skew join | `spark.sql.adaptive.enabled`, skew threshold 8 MB | 38.0 | 35.8, 38.0, 38.6 | 2.1 s | 1.68 s |
| broadcast | merchants (5.2k rows) sent to every executor | 23.9 | 24.1, 23.2, 23.9 | 1.5 s | 1.35 s |

With salting, AQE and broadcast the skew is gone: their slowest stage is the scan of the transactions.
Broadcast's 23.9 s is roughly the cost of reading the data, so the join itself costs about 27 s in the baseline,
22 s with salting, 14 s with AQE and almost nothing with broadcast.

### Baseline: one task reads the hot key

200 shuffle partitions; the task that gets the biggest merchant reads 451 MB (7.7M rows) and runs 17.4 s, while the
median task runs 0.15 s. The other 7 cores sit idle while it runs. The screenshots are from the run before the 200
foreign merchants were added (same pattern: 453 MB read, 16 s, 2.4 GB spilled, median task 3.5 MB).

![Baseline join stage: event timeline](images/skew_baseline_timeline.png)

![Baseline join stage: task metrics](images/skew_baseline_metrics.png)

### AQE: the hot partition is split at run time

AQE sees the oversized shuffle partitions and splits them (325 tasks instead of 200 in the screenshot run). The largest
task reads 17 MB, the slowest join task runs 0.5 s, nothing spills.

![AQE join stage: event timeline](images/skew_aqe_timeline.png)

![AQE join stage: task metrics](images/skew_aqe_metrics.png)

### Which fix, in which order

1. Broadcast when one side is small. `merchants` is 5.2k rows, far below Spark's 10 MB auto-broadcast threshold, so
   with default settings this join is broadcast and never skews. The benchmark turns automatic broadcast off on purpose
   to show the problem.
2. AQE for large-to-large joins: configuration only, no code change. Its defaults (a partition is skewed above
   256 MB and 5 times the median) target big data; at 1 GB the threshold had to be lowered to 8 MB. At the 100 GB target
   the hot partition is several GB and the defaults apply.
3. Salting only when AQE cannot help: skew inside an aggregation or a window (AQE splits join partitions only),
   join types AQE does not split (full outer), or a key so hot that the split pieces are still too big. It costs code,
   an extra column in the shuffle (2.5 GB instead of 2.0 GB here) and a copy of the other side per salt bucket.

### Why skew needs scale to show

A skewed task only costs wall time when the hot partition is bigger than the fair share (total / cores) and the task is
long enough to outweigh Spark's fixed overhead. A first run with 4 cores and 8M cached rows (3 columns) gave baseline
3.4 s, salting 3.7 s, AQE 2.7 s, broadcast 0.8 s: the hot key (24%) was about one core's fair share (25%), and the
slowest task took 0.8 s. With 8 cores and 32M full rows the fair share is 12.5%, so the 24% key takes twice as long as
everything else.

### Skew level sweep

`--top-shares 0.3,0.6,0.8` re-assigns `merchant_id` in memory so the top 10 merchants get 30%, 60% or 80% of the rows
(rows moved into the top group are spread evenly over the 10, so the biggest merchant is smaller than in the generated
data). The re-assignment is recomputed in every run and changes the plan (broadcast is about 4.5 s faster than on the
generated data), so compare methods within a row, not across rows. Median of 3 runs, seconds; 8 cores, 32M rows.

| Top-10 share | Biggest merchant | Baseline | Salting | AQE | Broadcast | Baseline slowest task |
|---|---:|---:|---:|---:|---:|---:|
| 30% | 8.1% | 44.4 | 44.4 | 38.7 | 19.3 | 9.2 s |
| 60% | 16.1% | 45.9 | 41.7 | 37.7 | 19.9 | 15.8 s |
| 80% | 21.5% | 48.6 | 42.4 | 38.1 | 18.7 | 19.4 s |
| 70% (as generated, no re-assignment) | 23.9% | 51.2 | 45.5 | 38.0 | 23.9 | 17.4 s |

- At 30% the biggest key (8.1%) is below one core's fair share (12.5%). Salting gains nothing over the baseline
  (44.4 s both): what it saves on the hot task it pays back for the extra column and the 32 copies of `merchants`.
  AQE is still 5.7 s faster, because the baseline's slowest task still reads 175 MB (9.2 s) and AQE splits it.
- From 60% the hot key exceeds the fair share: the baseline grows by 1.5 to 4 s per step while salting and AQE stay
  within a second of each other.
- Broadcast does not depend on skew at all, because the transactions are never shuffled.
