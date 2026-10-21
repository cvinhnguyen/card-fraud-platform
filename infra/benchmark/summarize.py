"""One markdown table of every run in infra/benchmark/results/: per stage (requests in flight), throughput, latency
and the share of prompt tokens served from the prefix cache. Usage: python infra/benchmark/summarize.py [run ...]"""
import json
import sys
from pathlib import Path

RESULTS = Path(__file__).parent / "results"


def row(run: str, stage: dict) -> str:
    s, lat = stage["successes"], stage["successes"]["latency"]
    prompt = s["prompt_tokens"]
    cached = prompt["cached"] / prompt["total"] if prompt["total"] else 0
    return (f"| {run} | {stage['load_summary']['concurrency']} | {s['throughput']['requests_per_sec']:.2f} "
            f"| {s['throughput']['output_tokens_per_sec']:.1f} | {lat['time_to_first_token']['median']:.2f} "
            f"| {lat['time_to_first_token']['p90']:.2f} | {lat['time_per_output_token']['median'] * 1000:.0f} "
            f"| {lat['request_latency']['median']:.1f} | {lat['request_latency']['p90']:.1f} | {cached:.0%} "
            f"| {stage['failures']['count']} |")


def main(runs: list[str]) -> None:
    print("| run | in flight | req/s | output tok/s | TTFT p50 (s) | TTFT p90 (s) | TPOT p50 (ms) | latency p50 (s) "
          "| latency p90 (s) | prompt cached | failures |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for run in runs or sorted(p.name for p in RESULTS.iterdir() if p.is_dir()):
        for f in sorted((RESULTS / run).glob("stage_*_lifecycle_metrics.json")):
            print(row(run, json.loads(f.read_text())))


if __name__ == "__main__":
    main(sys.argv[1:])
