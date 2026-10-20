# Tests of the two APIs

Both services test in their own Docker build stage (`docker build --target test services/scoring_api`), so the
tests run against the exact dependency pins of the image. Coverage gate: `--cov-fail-under=90` in `pytest.ini`.

| Service | Tests | Coverage |
|---|---:|---:|
| Scoring API | 95 | 100% |
| Drift API | 52 | 100% |

![Scoring API coverage report: 100%, 205 statements](images/coverage_scoring_api.png)

![Drift API coverage report: 100%](images/coverage_drift_api.png)

## What each technique catches

### Fakes over mocks

The scorer gets fake feature / model / log objects (`tests/conftest.py`) that implement the
same small interfaces as the real clients and can be told to fail or hang. The real clients are tested against
recorded HTTP (respx) and a patched Kafka producer. No test needs Redis, KServe or Kafka.

The price of fakes: the first live run found a bug no test could see. `import feast` (only the real
`FeatureClient` does it) sets `PROMETHEUS_MULTIPROC_DIR` in the process, and `Instrumentator.expose()` then served
an empty multiprocess directory: `/metrics` answered 200 with no body. The endpoint now renders the app's registry
itself, and `test_metrics_survive_feast_multiprocess_env` sets that variable to pin the behaviour.

### Equivalence partitioning and boundary values

With `pytest.mark.parametrize`:

| Input | Partitions / boundaries tested |
|---|---|
| score vs thresholds | 0 and 1, inside each partition, just below / at / just above `t_low` and `t_high` |
| threshold config | `t_low > t_high`, < 0, > 1 rejected; `(0,0)`, `(0,1)`, `(1,1)`, equal thresholds accepted |
| amount (fallback) | 1, inside, just below / at / above 2,000,000 VND, the largest valid amount |
| request schema | amount -1 / 0 / 1 / 10 bn / 10 bn + 1, currency `VND` / `USD` / lower case / 2 or 4 letters, channel valid / unknown / empty, id lengths 0 / 1 / 64 / 65, device absent / 64 / 65 |
| drift batch | 0 / 1 / 1,000 / 1,001 events; score -0.01 / 0 / 1 / 1.01 |
| PSI level | 0.1 and 0.25 exactly and either side |

### Property-based tests (hypothesis)

Rules that must hold for *any* input, with hypothesis generating the cases:

- idempotency: scoring the same request twice gives the same response; the drift report over the same data is the
  same twice;
- monotonicity: a higher score never gives a milder decision, for any pair of thresholds;
- totality: every request gets a decision, whichever dependency fails.

### Mutation testing (mutmut 3.8)

Mutation runs cover the code changed in this phase only (`setup.cfg`): `policy.py`, `scorer.py` of the
Scoring API, `psi.py`, `store.py` of the Drift API. mutmut flips operators and constants (`>=` -> `>`, `0.25` ->
`1.25`, ...) and reruns the tests; a mutant that survives is a line no test really checks.

```bash
docker build --target test -t fraud/drift-api:test services/drift_api
docker run --rm -w /app fraud/drift-api:test sh -c "mutmut run --max-children 2 && mutmut results"
```

| Service | First run | Survivors | After |
|---|---|---|---|
| Drift API | 104 / 116 killed | 13 | **106 / 106** |
| Scoring API | 88 / 108 killed | 20 | **106 / 106** |

mutmut 3 runs the tests on a copy of the code with every function behind a trampoline: the coverage gate of
`pytest.ini` failed that run, so `setup.cfg` empties `addopts` for it (and uses the 3.x key names, the 2.x
`paths_to_mutate` is deprecated). What the survivors were:

- **Real gaps (tests added).** PSI had no exact-value test: `|` -> `&` over the bins, `+=` -> `=`, and the defaults
  for a bin missing on one side all survived (now three exact cases, one of them the docs example 0.416). A
  fallback event *followed by* a counted one (`continue` -> `break`), TTL set on a key other than the bucket, bucket
  names (`// 61`), the request passed to the feature store, stage latency labels and values, the fallback log line,
  and the prediction log's timestamps (python mode writes `2026-10-16 09:30:00`, not ISO).
- **Dead code (deleted).** `b.decode() if isinstance(b, bytes) else b`: redis-py without `decode_responses` always
  returns bytes, so the other branch could never run.
- **Equivalent mutants (`# pragma: no mutate`).** `pipeline(transaction=False)` -> `True` / `None` (MULTI or not, the
  same counts) and the response half of the prediction log record (every field is JSON-native in either dump mode).
  `int(clock // 60)` -> `int(clock / 60)` was equivalent too; `int(clock) // 60` makes that mutant produce a float
  bucket name, which the bucket-name test catches.

### Load test (locust)

`services/scoring_api/locustfile.py`, real card ids so Redis lookups hit, report in `docs/images/locust_report.html`.

Run 2026-10-19 (locust 2.31.8 in a container on the compose network, 5,000 real card ids from the live period;
fallbacks counted from the API's `fraud_decisions_total{fallback="true"}` before and after each run):

```bash
docker run --rm --network fraud_default -e CARDS_FILE=/w/cards.txt -v <repo>/services/scoring_api/locustfile.py:/w/locustfile.py \
    -v <cards.txt>:/w/cards.txt -v <repo>/docs/images:/out locustio/locust:2.31.8 -f /w/locustfile.py \
    --host http://scoring-api:8000 --headless -u 3 -r 3 -t 2m --html /out/locust_report.html
```

| Run | Users | Requests | Throughput | p50 / p95 / p99 | HTTP failures | Fallback decisions |
|---|---:|---:|---:|---:|---:|---:|
| SLA ([report](images/locust_report.html)) | 3 | 4,109 | 34.5 req/s | 31 / 50 / **77 ms** | 0 | 9 (0.2%) |
| Overload ([report](images/locust_overload.html)) | 20 | 3,463 | 58.0 req/s | 290 / 410 / 490 ms | 0 | 641 (18%) |

![Locust SLA report: 4,109 requests, 0 failures, 34.5 req/s, p50 31 ms, p99 77 ms](images/locust_sla.png)

- Within one worker's capacity (~45 req/s, see [serving.md](serving.md)) the API meets the design's p99 < 100 ms.
- Overloaded, it never answers with an error: requests past their 200 ms budget get the fallback rule, as designed.
  More throughput is more replicas (KEDA on requests per second, Module 2), not a longer budget.
- Right after the overload, 100 serial requests: 21 ms median, 0% fallback. Before the `asyncio.shield` fix the
  API stayed at 100% fallback after a burst until it was restarted.

```bash
docker build --target test services/scoring_api          # pytest + coverage gate
docker build --target test services/drift_api
```
