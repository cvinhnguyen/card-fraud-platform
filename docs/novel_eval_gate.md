# Novel idea: an evaluation gate in the agents' CD (Langfuse experiments)

**Idea.** The agents' pipelines already deploy a new system prompt as soon as it is promoted in Langfuse, and the
smoke test only checks that an agent answers. Whether it still answers *correctly* was not checked anywhere: an LLM
agent can pass every unit test, deploy cleanly and still get worse. The pipeline now evaluates the agent it just
deployed on a fixed set of questions with known facts, records the run as a **Langfuse experiment** (one per deploy,
comparable side by side), and **rolls the release back** when accuracy falls below a threshold. Not covered in EDAI:
evaluation-driven deployment of LLM agents (offline evals as a CD gate) and Langfuse datasets / experiments (the
course used Langfuse for prompt management only).

## How it works

```mermaid
flowchart LR
    lf["Langfuse prompt<br/>card-support/risk-agent<br/>label production"] -->|"pipeline pulls it"| j["Jenkins<br/>card-support-data-agents"]
    j --> d["helm upgrade --atomic"] --> s["A2A smoke test"] --> g["eval gate<br/>agents/eval/langfuse_gate.py"]
    g -->|"12 questions over A2A<br/>through the gateway"| ra["risk-agent"]
    g -->|"experiment + scores"| ex["Langfuse dataset<br/>card-support/risk-agent"]
    g -->|"accuracy < 0.6"| rb["helm rollback + build fails"]
```

- **Dataset** ([`agents/eval/dataset.jsonl`](../agents/eval/dataset.jsonl)): 12 card-policy questions, each with the
  facts a correct answer must state (several accepted spellings: "100,000" / "100000" / "100.000"). The gate upserts
  them into the Langfuse dataset `card-support/risk-agent` (item ids make it idempotent).
- **Experiment** ([`agents/eval/langfuse_gate.py`](../agents/eval/langfuse_gate.py), Langfuse Python SDK 5.0.3,
  `dataset.run_experiment`): the task asks the deployed risk agent each question over A2A through the gateway, as the
  coordinator does; an item evaluator scores `accuracy` 1/0, a run evaluator the average. Run name =
  `risk-agent prompt v<Langfuse version> (Jenkins #<build>)`.
- **Gate** ([`agents/Jenkinsfile.data-agents`](../agents/Jenkinsfile.data-agents), stage "Eval gate"): below
  `EVAL_MIN_ACCURACY = 0.6` the stage runs `helm rollback` to the previous revision and fails the build, so the
  coordinator pipeline is not triggered. The runner is a small image (`agents/eval/Dockerfile`); Jenkins only holds
  the Langfuse keys and the A2A key as credentials.
- The same dataset drives the A/B evaluations (`agents/eval/run_eval.py`, [A/B testing](ab_testing.md)).

## Proof it worked

The eval found a real failure before anyone looked at a trace: in 2 to 4 of the 12 answers the risk agent replied
"the policy could not be checked" in about 2 s, without calling `search_policy` or after one search that missed the
fee table row. The prompt's own rule ("if its hits do not cover the question, say the policy could not be checked")
gave it that exit. Prompt **v2** in Langfuse changed only that rule (say it only after calling `search_policy`, and
after a second search in other words; the commit message in Langfuse records why). Then:

| Jenkins build | prompt deployed | accuracy | gate (0.6) |
|---|---|---:|---|
| by hand | v1 (baseline) | 0.83 | pass |
| #20 | v1, while the LLM canary still sent 20 % of the calls to Groq (429s) | 0.58 | **fail: `helm rollback`, build failed, coordinator not triggered** |
| #21 | v1, canary stopped | 0.67 | pass |
| #23 | **v2** (promoted to `production` in Langfuse, picked up by the pipeline) | **0.92** | pass |

![Stage view: #20 failed at the eval gate, #21 and #23 passed](images/eval_gate_jenkins.png)

![Console of the eval gate stage in #20, #21 and #23](images/eval_gate_runs.png)

- Build #20 is the gate doing its job on a problem it was not written for: the deploy itself was fine, but the
  platform under it (the LLM A/B canary, see [A/B testing](ab_testing.md)) made a quarter of the answers fail, and
  the release did not go on to the coordinator.
- v1 scores 0.58 to 0.83 across runs (temperature 0.8, 12 questions), v2 0.92: the gate's threshold is set below
  the noise of a good prompt, and a single run is a smoke-level signal, not a benchmark. The runs sit next to each
  other in Langfuse (Datasets > `card-support/risk-agent` > Experiments), per question, with each answer's trace.
- Cost: the gate adds about 1.5 to 2.5 minutes to a deploy (12 agent conversations on the one GPU).
