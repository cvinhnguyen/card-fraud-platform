"""Run the eval dataset through the platform and score the answers: an answer is correct when it states every expected
fact (dataset.jsonl: expect = groups of accepted spellings, all groups must match). Stdlib only.

  agent mode: each question to an agent over A2A through the gateway (internal route, the coordinator's key), the way
              the coordinator asks it; --variant a|b pins one side of its A/B test (header x-agent-variant)
  llm mode:   each question with its policy document as context straight to the gateway's /v1 route (the agents' key);
              --variant a|b pins one LLM version (header x-llm-variant)

  GATEWAY=http://<gateway> A2A_KEY=... AGW_KEY=... python agents/eval/run_eval.py --mode agent --variant b --out r.json
Prints one line per question and a summary (accuracy, latency); --min-accuracy makes it exit 1 below that share.
"""
import argparse
import json
import os
import re
import statistics
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).parent
KNOWLEDGE = HERE.parent.parent / "generator" / "knowledge"


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower())


def correct(answer: str, expect: list[list[str]]) -> bool:
    a = norm(answer)
    return all(any(norm(alt) in a for alt in group) for group in expect)


def post(url: str, body: dict, headers: dict, timeout: float) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "User-Agent": "card-support-eval", **headers})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def ask_agent(gw: str, agent: str, question: str, variant: str | None) -> str:
    headers = {"Authorization": f"Bearer {os.environ['A2A_KEY']}", "x-kagent-host": f"{agent}.kagent"}
    if variant:
        headers["x-agent-variant"] = variant
    body = {"jsonrpc": "2.0", "id": 1, "method": "message/send",
            "params": {"message": {"role": "user", "messageId": str(time.time_ns()),
                                   "parts": [{"kind": "text", "text": question}]}}}
    out = post(f"{gw}/", body, headers, 300)
    if "error" in out:
        raise RuntimeError(out["error"])
    return " ".join(p.get("text", "") for art in out["result"].get("artifacts", []) for p in art.get("parts", []))


def ask_llm(gw: str, doc: str, question: str, variant: str | None) -> str:
    headers = {"Authorization": f"Bearer {os.environ['AGW_KEY']}"}
    if variant:
        headers["x-llm-variant"] = variant
    context = (KNOWLEDGE / f"{doc}.md").read_text(encoding="utf-8")
    body = {"model": "qwen3-8b", "max_tokens": 300, "temperature": 0,
            "messages": [{"role": "system", "content": "Answer the customer's question from this bank document only, "
                                                       "in one or two sentences.\n\n" + context},
                         {"role": "user", "content": question}]}
    out = post(f"{gw}/v1/chat/completions", body, headers, 300)
    return out["choices"][0]["message"]["content"] or ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["agent", "llm"], default="agent")
    ap.add_argument("--agent", default="risk-agent")
    ap.add_argument("--variant", choices=["a", "b"], help="pin one side of the A/B test (default: the route's split)")
    ap.add_argument("--sleep", type=float, default=0, help="seconds between questions (provider rate limits)")
    ap.add_argument("--out", help="write the per-question results as JSON")
    ap.add_argument("--min-accuracy", type=float)
    args = ap.parse_args()
    gw = os.environ.get("GATEWAY", "http://agentgateway-proxy.agentgateway-system.svc.cluster.local").rstrip("/")
    items = [json.loads(line) for line in (HERE / "dataset.jsonl").read_text(encoding="utf-8").splitlines() if line]
    results = []
    for it in items:
        t0 = time.monotonic()
        try:
            answer = (ask_agent(gw, args.agent, it["question"], args.variant) if args.mode == "agent"
                      else ask_llm(gw, it["doc"], it["question"], args.variant))
            error = None
        except Exception as e:  # noqa: BLE001 - an error is a wrong answer, reported as such
            answer, error = "", str(e)[:200]
        r = {"id": it["id"], "correct": bool(answer) and correct(answer, it["expect"]),
             "seconds": round(time.monotonic() - t0, 2), "error": error, "answer": answer[:400]}
        results.append(r)
        print(f"{'ok  ' if r['correct'] else 'FAIL'} {r['seconds']:6.1f}s {it['id']:22} {error or answer[:90]!r}", flush=True)
        time.sleep(args.sleep)
    acc = sum(r["correct"] for r in results) / len(results)
    secs = [r["seconds"] for r in results if not r["error"]]
    summary = {"mode": args.mode, "variant": args.variant or "split", "questions": len(results), "accuracy": round(acc, 3),
               "errors": sum(bool(r["error"]) for r in results),
               "latency_p50_s": round(statistics.median(secs), 2) if secs else None,
               "latency_max_s": max(secs) if secs else None}
    print(json.dumps(summary))
    if args.out:
        Path(args.out).write_text(json.dumps({"summary": summary, "results": results}, indent=1), encoding="utf-8")
    return 1 if args.min_accuracy is not None and acc < args.min_accuracy else 0


if __name__ == "__main__":
    sys.exit(main())
