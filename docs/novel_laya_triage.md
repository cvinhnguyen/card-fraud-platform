# Novel idea: a System-1 triage in front of the agents (Laya)

**Idea.** Today the coordinator LLM decides, in one of its turns, which specialist answers a customer request (the
card data agent, the risk agent, or both). That decision needs no generation, only a choice, yet it takes a slot of
the one GPU the agents share. [Laya](https://github.com/NandhaKishorM/laya) (Convai Innovations, Apache-2.0, released
September 2026) is a non-generative decision model: a ModernBERT encoder plus a decision head that answers typed
questions (choice, score, yes/no with a probability) in one forward pass. It runs on CPU, so the routing would no
longer queue behind the agents' generations. Not covered in EDAI: a decision model (System 1) in front of the LLM
agents (System 2).

## How it was tested ([`agents/triage/`](../agents/triage))

`questions.jsonl` holds 22 customer requests labelled with the route they need: 8 for the card data agent, 10 for the
risk agent (policy questions and payment checks), 4 that need both (the coordinator). `triage.py` routes each one two
ways and compares:

- **Laya** on the host CPU (Ryzen 5 3600, fp32, `laya==0.4.12`, English checkpoint, 421M parameters), asked two
  yes/no statements: "the request asks for facts from one card's own records" and "the request asks about the bank's
  rules or fees, or whether a payment would be approved". Both true: the coordinator.
- **The platform's LLM** (Qwen3-8B on llama.cpp, pinned with `x-llm-variant: a`) asked the same choice in a short
  prompt, temperature 0, through the gateway.

```bash
pip install laya==0.4.12
GATEWAY=http://localhost:18080 AGW_KEY=... python agents/triage/triage.py --out agents/triage/results/routing.json
```

## Results ([`results/routing.json`](../agents/triage/results/routing.json))

| router | correct | card data | risk | both | latency p50 | latency max | hardware |
|---|---:|---:|---:|---:|---:|---:|---|
| Laya, one choice question over the 3 routes | 9 / 22 | | | | 688 ms | 902 ms | CPU |
| Laya, plain-word labels | 15 / 22 | | | | 494 ms | | CPU |
| **Laya, two yes/no statements** | **17 / 22** | 6 / 8 | 10 / 10 | 1 / 4 | **623 ms** | **793 ms** | CPU |
| Qwen3-8B, choice prompt | 20 / 22 | 8 / 8 | 10 / 10 | 2 / 4 | 210 ms | 7,510 ms | GPU |

- How the question is put to Laya matters more than anything else: an abstract choice over route names (9 / 22) is
  close to chance; two statements it can check one by one, the way its NLI-style training reads text, reach 17 / 22.
- Laya routes every single-topic policy or payment request right (10 / 10). It misses two card-profile requests
  ("When was the card issued?") and three of the four mixed requests.
- **Latency is bounded on Laya, not on the LLM.** The LLM's median is lower (210 ms: a short prompt on an idle GPU),
  but its slowest call took 7.5 s: the pipeline's deploy was running agent calls at the same time, and the routing
  call waited for one of llama.cpp's two slots. Laya's slowest call was 0.8 s, whatever the GPU was doing; it loads in
  7 s and uses no GPU memory.

## Verdict

It works as a router for single-topic requests and keeps routing off the GPU, but zero-shot it is not yet as accurate
as the LLM on mixed requests (17 vs 20 of 22), so it does not replace the coordinator's choice yet. Laya's own
results show the gap closes with fine-tuning (typed decisions: 0.36 zero-shot, 0.77 fine-tuned); its fine-tuning
notebook runs on Kaggle's free 2x T4. Next step: fine-tune on routed requests from the agents' traces (Langfuse /
Jaeger), then put it in front of the coordinator with a confidence threshold, below which the coordinator decides as
today.
