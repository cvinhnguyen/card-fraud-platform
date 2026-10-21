"""Eval gate of the agents' CD (novel idea): the eval dataset (dataset.jsonl) is a Langfuse dataset, and every deploy
runs it as a Langfuse experiment against the agent just deployed, through the gateway like the coordinator. Each answer
gets an accuracy score (run_eval.correct: every expected fact stated), the run an average; below --min-accuracy the
script exits 1 and the pipeline rolls the release back. The experiments of successive prompt versions sit side by
side in Langfuse (Datasets > card-support/risk-agent).

  LANGFUSE_HOST=... LANGFUSE_PUBLIC_KEY=... LANGFUSE_SECRET_KEY=... GATEWAY=http://<gateway> A2A_KEY=...
  python agents/eval/langfuse_gate.py --run-name "risk-agent prompt v2 (Jenkins #12)" --min-accuracy 0.6 --repeats 2
"""
import argparse
import json
import os
import sys
from pathlib import Path

from langfuse import Evaluation, get_client

from run_eval import ask_agent, correct

HERE = Path(__file__).parent


def main() -> int:
    """Upsert the dataset in Langfuse, run it --repeats times on the deployed agent, return 1 below the bar."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--agent", default="risk-agent")
    ap.add_argument("--variant", choices=["a", "b"], default="a", help="side of the agent's A/B test to evaluate")
    ap.add_argument("--run-name", required=True)
    ap.add_argument("--min-accuracy", type=float, default=0.6)
    ap.add_argument("--repeats", type=int, default=1, help="runs of the dataset averaged by the gate")
    args = ap.parse_args()
    gw = os.environ["GATEWAY"].rstrip("/")
    name = f"card-support/{args.agent}"

    lf = get_client()
    lf.create_dataset(name=name, description="Card policy questions with the facts a correct answer states (agents/eval)")
    lines = (HERE / "dataset.jsonl").read_text(encoding="utf-8").splitlines()
    for line in lines:
        it = json.loads(line)   # the item id makes this an upsert: re-running does not duplicate the dataset
        lf.create_dataset_item(dataset_name=name, id=f"{args.agent}-{it['id']}", input={"question": it["question"]},
                               expected_output={"expect": it["expect"]}, metadata={"doc": it["doc"]})

    def task(*, item, **kwargs):
        return ask_agent(gw, args.agent, item.input["question"], args.variant)

    def accuracy(*, output, expected_output, **kwargs):
        ok = bool(output) and correct(output, expected_output["expect"])
        return Evaluation(name="accuracy", value=1.0 if ok else 0.0, comment=None if ok else "expected fact missing")

    def average(*, item_results, **kwargs):
        values = [e.value for r in item_results for e in r.evaluations if e.name == "accuracy"]
        return Evaluation(name="avg_accuracy", value=sum(values) / len(values) if values else 0.0)

    # The agent samples (temperature 0.8): one run of the dataset moves by a question or two from run to run, so the
    # gate averages --repeats runs, each its own experiment in Langfuse.
    runs = []
    for i in range(1, max(args.repeats, 1) + 1):
        run_name = args.run_name if args.repeats == 1 else f"{args.run_name} run {i}/{args.repeats}"
        result = lf.get_dataset(name).run_experiment(
            name=f"{args.agent} gate", run_name=run_name, task=task, evaluators=[accuracy], run_evaluators=[average],
            max_concurrency=1,   # one GPU: the agents answer one question at a time anyway
            metadata={"agent": args.agent, "variant": args.variant})
        lf.flush()
        print(result.format())
        values = [e.value for r in result.item_results for e in r.evaluations if e.name == "accuracy"]
        runs.append(sum(values) / len(values) if len(values) == len(result.item_results) else 0.0)  # missing score = fail
        print(f"{run_name}: accuracy {runs[-1]:.2f}")
    acc = sum(runs) / len(runs)
    print(f"accuracy {acc:.2f} over {len(runs)} run(s) of {len(lines)} questions "
          f"(gate {args.min_accuracy:.2f}): {'pass' if acc >= args.min_accuracy else 'FAIL'}")
    return 0 if acc >= args.min_accuracy else 1


if __name__ == "__main__":
    sys.exit(main())
