"""Speculative decoding on the agents' kind of answers: each eval question (agents/eval/dataset.jsonl) with its bank
document as context, straight to llama.cpp, decoded as the agents decode (temperature 0.8, here seeded). The answers
quote the document, so an n-gram drafter that looks the next tokens up in the prompt can propose several tokens and the
model verifies them in one pass. llama.cpp reports per request how many tokens were drafted and accepted.

  LLM_URL=http://127.0.0.1:8085 LLM_API_KEY=... python infra/benchmark/spec_decoding.py <name>
  -> infra/benchmark/results/spec-<name>.json; run once per llama-server configuration (see docs/llm_benchmark.md)
"""
import json
import os
import statistics
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "agents" / "eval"))
from run_eval import correct  # noqa: E402  (same scoring as the eval gate)

REPEATS = 2   # two passes: the second one also sees llama.cpp's prompt cache warm, as the agents do


def ask(url: str, key: str, doc: str, question: str, seed: int) -> dict:
    """One chat completion straight to llama.cpp, the question's document as context; llama.cpp's JSON with timings."""
    body = {"model": "qwen3-8b", "max_tokens": 200, "temperature": 0.8, "seed": seed,
            "messages": [{"role": "system", "content": "Answer the customer's question from this bank document only, "
                                                       "quoting its wording, in two or three sentences.\n\n" + doc},
                         {"role": "user", "content": question}]}
    req = urllib.request.Request(f"{url}/v1/chat/completions", data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=300) as r:
        return json.load(r)


def main(name: str) -> None:
    """Ask every question REPEATS times, print decode speed and draft acceptance per request, then a summary."""
    url, key = os.environ.get("LLM_URL", "http://127.0.0.1:8085"), os.environ["LLM_API_KEY"]
    items = [json.loads(l) for l in (ROOT / "agents/eval/dataset.jsonl").read_text(encoding="utf-8").splitlines() if l]
    docs = {d: (ROOT / f"generator/knowledge/{d}.md").read_text(encoding="utf-8") for d in {i["doc"] for i in items}}
    rows = []
    for rep in range(REPEATS):
        for n, it in enumerate(items):
            out = ask(url, key, docs[it["doc"]], it["question"], seed=1000 * rep + n)
            t = out.get("timings", {})
            answer = out["choices"][0]["message"]["content"] or ""
            rows.append({"id": it["id"], "pass": rep, "tokens": t.get("predicted_n"), "tok_s": t.get("predicted_per_second"),
                         "drafted": t.get("draft_n", 0), "accepted": t.get("draft_n_accepted", 0),
                         "correct": correct(answer, it["expect"])})
            r = rows[-1]
            print(f"{it['id']:22} {r['tokens']:4} tok {r['tok_s']:6.1f} tok/s  drafted {r['drafted']:4} accepted {r['accepted']:4}"
                  f"  {'ok' if r['correct'] else 'wrong'}", flush=True)
    tokens = sum(r["tokens"] for r in rows)
    decode_s = sum(r["tokens"] / r["tok_s"] for r in rows if r["tok_s"])
    drafted, accepted = sum(r["drafted"] for r in rows), sum(r["accepted"] for r in rows)
    summary = {"config": name, "requests": len(rows), "output_tokens": tokens,
               "decode_tok_s": round(tokens / decode_s, 1), "median_tok_s": round(statistics.median(r["tok_s"] for r in rows), 1),
               "drafted": drafted, "accepted": accepted, "acceptance": round(accepted / drafted, 3) if drafted else None,
               "correct": sum(r["correct"] for r in rows)}
    print(json.dumps(summary))
    out = ROOT / f"infra/benchmark/results/spec-{name}.json"
    out.write_text(json.dumps({"summary": summary, "rows": rows}, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1])
