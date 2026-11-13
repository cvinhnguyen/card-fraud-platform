"""Run the eval dataset through the platform and score the answers: an answer is correct when it states every expected
fact (dataset.jsonl: expect = groups of accepted spellings, all groups must match). Stdlib only.

  agent mode: each question to an agent over A2A through the gateway (internal route, the coordinator's key), the way
              the coordinator asks it; --variant a|b pins one side of its A/B test (header x-agent-variant)
  llm mode:   each question with its policy document as context straight to the gateway's /v1 route (the agents' key);
              --variant a|b pins one LLM version (header x-llm-variant)
  down mode:  the coordinator's prompt (chart/prompts/coordinator.md) with the card data agent's answer and a failed
              risk agent, over /v1 (the agents' key); correct = it says that part failed and invents no risk, score,
              fee or deadline

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
    """Lower-case the text and collapse whitespace, so a fact matches however it is spaced."""
    return re.sub(r"\s+", " ", text.lower())


NEGATION = re.compile(r"\b(not|no|never|without)\b|n't\b")


def states(a: str, alt: str) -> bool:
    """True when the normalized answer states `alt` as a whole term: "3%" is not found in "1.3%" or "13%", "100,000" not
    in "1,100,000", and a mention with a negation in the four words before it ("not free") does not count."""
    for m in re.finditer(re.escape(alt), a):
        before, after = a[:m.start()], a[m.end():]
        if alt[0].isalnum() and re.search(r"(\w|\d[.,])$", before):
            continue   # inside a longer word or number
        if alt[-1].isalnum() and re.match(r"\w|[.,]\d", after):
            continue
        if not NEGATION.search(" ".join(before.split()[-4:])):
            return True
    return False


def correct(answer: str, expect: list[list[str]]) -> bool:
    """True when the answer states every expected fact (each fact: a group of accepted spellings)."""
    a = norm(answer)
    return all(any(states(a, norm(alt)) for alt in group) for group in expect)


def post(url: str, body: dict, headers: dict, timeout: float) -> dict:
    """POST a JSON body and return the JSON answer."""
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "User-Agent": "card-support-eval", **headers})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def ask_agent(gw: str, agent: str, question: str, variant: str | None) -> str:
    """Ask an agent one question over A2A through the gateway (optionally pinned to an A/B side); return its text."""
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
    """Ask the LLM route one question with the question's bank document as context; return the answer text."""
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


DOWN_CARD = ("card-data-agent: The card has not been used in the last hour. In the last 30 days it made 3 payments, "
             "averaging about 383,800 VND. Credit card on the Napas network, issued April 2016.")
DOWN_RISK = "risk-agent: ERROR failed to call tool server /mcp/policy-rag: HTTP 500"
# an invented risk level, verdict, score, fee or deadline: none of these can come from the answers above
DOWN_INVENTED = re.compile(r"\b(high|low|medium)[- ]risk|risk (score )?(is|may be|might be|could be) "
                           r"|\b(is|looks|seems|appears|likely|probably|as)( to be)? (suspicious|fraud\w*|safe|legitimate)"
                           r"|\d+ ?%|\d+ ?(working )?days?\b|\d[\d,.]* ?(vnd|đ|dong)\b", re.I)
DOWN_SAYS_SO = re.compile(r"(could not|couldn't|cannot|can't|unable to) (be )?(check|verif|confirm|get|reach|retriev)"
                          r"|not available|unavailable|try again|ask again", re.I)


def ask_down(gw: str, question: str) -> str:
    """Give the coordinator's prompt a failed risk agent and return its reply (a specialist-down check of the prompt)."""
    prompt = (HERE.parent / "chart" / "prompts" / "coordinator.md").read_text(encoding="utf-8")
    body = {"model": "qwen3-8b", "max_tokens": 300, "temperature": 0,
            "messages": [{"role": "system", "content": prompt},
                         {"role": "user", "content": question},
                         {"role": "user", "content": "Answers of the specialists:\n" + DOWN_CARD + "\n" + DOWN_RISK}]}
    out = post(f"{gw}/v1/chat/completions", body, {"Authorization": f"Bearer {os.environ['AGW_KEY']}"}, 300)
    return out["choices"][0]["message"]["content"] or ""


def down_ok(answer: str, question: str = "") -> bool:
    """True when the reply says the risk part failed and invents no risk, score, fee or deadline. The card data's
    "last 30 days" and an amount the question or the card data gave (2,500,000 VND, 383,800 VND) are not invented."""
    given = {re.sub(r"\D", "", n) for n in re.findall(r"\d[\d,.]*\d", question + DOWN_CARD)}
    own = re.sub(r"(last|past) \d+ days", "", answer, flags=re.I)
    own = re.sub(r"\d[\d,.]*\d ?(vnd|đ|dong)\b", lambda m: "" if re.sub(r"\D", "", m.group(0)) in given else m.group(0),
                 own, flags=re.I)
    return bool(DOWN_SAYS_SO.search(own)) and not DOWN_INVENTED.search(own)


DOWN_QUESTIONS = [
    {"id": "down-declined", "question": "A payment of 2,500,000 VND at an online electronics shop on card card_d10f799f was "
                                        "declined. What does the card's recent activity look like, how risky is that "
                                        "payment, and how long do I have to dispute it?"},
    {"id": "down-fee", "question": "My card is card_d10f799f. How many payments did it make in the last 30 days, and "
                                   "what is the foreign transaction fee?"},
]


def main() -> int:
    """Answer every dataset question (agent or LLM mode), print results and a summary, return 1 below --min-accuracy."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["agent", "llm", "down"], default="agent")
    ap.add_argument("--agent", default="risk-agent")
    ap.add_argument("--variant", choices=["a", "b"], help="pin one side of the A/B test (default: the route's split)")
    ap.add_argument("--sleep", type=float, default=0, help="seconds between questions (provider rate limits)")
    ap.add_argument("--out", help="write the per-question results as JSON")
    ap.add_argument("--min-accuracy", type=float)
    args = ap.parse_args()
    gw = os.environ.get("GATEWAY", "http://agentgateway-proxy.agentgateway-system.svc.cluster.local").rstrip("/")
    items: list[dict] = (DOWN_QUESTIONS if args.mode == "down" else
             [json.loads(line) for line in (HERE / "dataset.jsonl").read_text(encoding="utf-8").splitlines() if line])
    results = []
    for it in items:
        t0 = time.monotonic()
        try:
            answer = (ask_agent(gw, args.agent, it["question"], args.variant) if args.mode == "agent"
                      else ask_down(gw, it["question"]) if args.mode == "down"
                      else ask_llm(gw, it["doc"], it["question"], args.variant))
            error = None
        except Exception as e:  # noqa: BLE001 - an error is a wrong answer, reported as such
            answer, error = "", str(e)[:200]
        ok = down_ok(answer, it["question"]) if args.mode == "down" else correct(answer, it["expect"])
        r = {"id": it["id"], "correct": bool(answer) and ok,
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
