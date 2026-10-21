"""Novel idea: a System-1 triage in front of the agents. Laya (Convai, Apache-2.0) is a non-generative decision model
(ModernBERT encoder + decision head, one forward pass): it reads a customer request and picks the specialist that
should answer it, on CPU, without the GPU the agents' LLM needs. Today that choice is one of the coordinator LLM's
turns; this compares the two routers on labelled requests (questions.jsonl):

  laya  Laya's choice question below, on the CPU of the machine that runs this
  llm   the same choice asked to the platform's LLM through the gateway (/v1, pinned to Qwen3-8B on llama.cpp)

  pip install laya==0.4.12
  GATEWAY=http://localhost:18080 AGW_KEY=... python agents/triage/triage.py --out triage.json
  ... --answer 3 A2A_KEY=... A2A_KEY_SUPPORT_APP=...: also answers the first 3 requests routed to one specialist
      straight from it (through the gateway) and through the coordinator, end to end
"""
import argparse
import json
import os
import statistics
import sys
import time
import urllib.request
from pathlib import Path

ROUTES = {
    "card-data-agent": "facts about one card from the bank's data: its profile, its recent payments, how much or how "
                       "often it spent over a period",
    "risk-agent": "the bank's card rules (fees, disputes, liability, OTP and authentication) or whether a payment of a "
                  "given amount would be approved or declined",
    "coordinator": "a request that needs both the card's data and the bank's rules, or several topics at once",
}
# Laya answers two yes/no statements about the request (its "noul" answers: P(true)); one choice question over the three
# routes scored 41 % zero-shot (15 / 22 with plain-word labels), the two statements 77 %.
QUESTION = {
    "data": {"type": "noul", "instructions": "The request asks for facts from one card's own records, such as its "
                                             "payments, spending or profile."},
    "rules": {"type": "noul", "instructions": "The request asks about the bank's rules or fees, or whether a payment "
                                              "would be approved or declined."},
}


def laya_route(answers: dict) -> tuple[str, float]:
    """Both statements true: the coordinator; one: its specialist (confidence = P of the deciding statement)."""
    data, rules = answers["data"]["noul"], answers["rules"]["noul"]
    if data > 0.5 and rules > 0.5:
        return "coordinator", min(data, rules)
    return ("card-data-agent", data) if data >= rules else ("risk-agent", rules)


def post(url: str, body: dict, headers: dict, timeout: float = 300) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "User-Agent": "card-support-triage", **headers})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def llm_route(gw: str, q: str) -> str:
    options = "\n".join(f"- {k}: {v}" for k, v in ROUTES.items())
    out = post(f"{gw}/v1/chat/completions", {
        "model": "qwen3-8b", "temperature": 0, "max_tokens": 12,
        "messages": [{"role": "system", "content": "Pick the specialist of a bank's card support team that should answer "
                                                   f"the request. Reply with its name only.\n{options}"},
                     {"role": "user", "content": q}]},
        {"Authorization": f"Bearer {os.environ['AGW_KEY']}", "x-llm-variant": "a"})
    text = (out["choices"][0]["message"]["content"] or "").lower()
    return next((k for k in ROUTES if k in text), "coordinator")


def a2a(gw: str, path: str, headers: dict, q: str) -> str:
    body = {"jsonrpc": "2.0", "id": 1, "method": "message/send",
            "params": {"message": {"role": "user", "messageId": str(time.time_ns()), "parts": [{"kind": "text", "text": q}]}}}
    out = post(f"{gw}{path}", body, headers)
    return " ".join(p.get("text", "") for a in out.get("result", {}).get("artifacts", []) for p in a.get("parts", []))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out")
    ap.add_argument("--answer", type=int, default=0, help="answer N specialist requests both ways, end to end")
    args = ap.parse_args()
    gw = os.environ["GATEWAY"].rstrip("/")
    items = [json.loads(l) for l in (Path(__file__).parent / "questions.jsonl").read_text(encoding="utf-8").splitlines() if l]

    from laya import Router
    t0 = time.perf_counter()
    router = Router(device="cpu", default="english", max_loaded=1)   # loads only the checkpoint a request needs
    router.predict("warm up", QUESTION)                                # English: the English checkpoint
    load_s = time.perf_counter() - t0

    rows = []
    for it in items:
        t = time.perf_counter()
        laya, conf = laya_route(router.predict(it["q"], QUESTION)["answers"])
        laya_ms = (time.perf_counter() - t) * 1000
        t = time.perf_counter()
        llm = llm_route(gw, it["q"])
        llm_ms = (time.perf_counter() - t) * 1000
        rows.append({"q": it["q"], "expected": it["route"], "laya": laya, "laya_confidence": round(conf, 3),
                     "laya_ms": round(laya_ms, 1), "llm": llm, "llm_ms": round(llm_ms, 1)})
        print(f"{it['route']:16} laya {laya:16} {laya_ms:6.0f} ms | llm {llm:16} {llm_ms:6.0f} ms | {it['q'][:60]}")

    def summary(name: str) -> dict:
        return {"accuracy": round(sum(r[name] == r["expected"] for r in rows) / len(rows), 3),
                "latency_p50_ms": round(statistics.median(r[f"{name}_ms"] for r in rows), 1),
                "latency_max_ms": max(r[f"{name}_ms"] for r in rows)}
    result = {"requests": len(rows), "laya_load_s": round(load_s, 1), "laya": summary("laya"), "llm": summary("llm"), "rows": rows}

    if args.answer:   # specialist requests Laya routed correctly: specialist directly vs the coordinator
        e2e = []
        for r in [r for r in rows if r["laya"] == r["expected"] != "coordinator"][:args.answer]:
            t = time.perf_counter()
            direct = a2a(gw, "/", {"Authorization": f"Bearer {os.environ['A2A_KEY']}", "x-kagent-host": f"{r['laya']}.kagent",
                                   "x-agent-variant": "a"}, r["q"])
            direct_s = time.perf_counter() - t
            t = time.perf_counter()
            via = a2a(gw, "/a2a/coordinator", {"Authorization": f"Bearer {os.environ['A2A_KEY_SUPPORT_APP']}"}, r["q"])
            via_s = time.perf_counter() - t
            e2e.append({"q": r["q"], "agent": r["laya"], "direct_s": round(direct_s, 1), "coordinator_s": round(via_s, 1),
                        "direct": direct[:300], "coordinator": via[:300]})
            print(f"{r['laya']:16} direct {direct_s:5.1f} s | via coordinator {via_s:5.1f} s | {r['q'][:60]}")
        result["end_to_end"] = e2e

    print(json.dumps({k: v for k, v in result.items() if k not in ("rows", "end_to_end")}))
    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
