"""docs/images/llm_benchmark.png from infra/benchmark/results: output tokens/s and median TTFT per requests in flight,
and llama-server's resident memory during each run. Usage: python infra/benchmark/plot.py (needs matplotlib)."""
import csv
import json
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).parent
RUNS = {"e1-baseline": "e1 baseline: 2 slots", "e2-slots4": "e2: 4 slots, unified KV", "e3-cram1g": "e3: 2 slots, cache-ram 1 GiB"}
COLORS = ["#2a78d6", "#eb6834", "#1baf7a"]          # categorical slots 1-3, fixed order
INK, MUTED, GRID, SURFACE = "#0b0b0b", "#898781", "#e1e0d9", "#fcfcfb"


def stages(run):
    for f in sorted((ROOT / "results" / run).glob("stage_*_lifecycle_metrics.json")):
        s = json.loads(f.read_text())
        yield s["load_summary"]["concurrency"], s["successes"]


def memory(run):
    rows = [r for r in csv.DictReader((ROOT / "results" / run / "memory.csv").open()) if int(r["llama_ws_mb"] or 0)]
    t0 = sum(int(x) * 60 ** (2 - i) for i, x in enumerate(rows[0]["time"].split(":")))
    return ([(sum(int(x) * 60 ** (2 - i) for i, x in enumerate(r["time"].split(":"))) - t0) / 60 for r in rows],
            [int(r["llama_ws_mb"]) / 1024 for r in rows])


def style(ax, title, ylabel):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=11)
    ax.set_ylabel(ylabel, color=MUTED)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.tick_params(colors=MUTED)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#c3c2b7")


fig, axes = plt.subplots(1, 3, figsize=(15, 4.4), facecolor=SURFACE)
for (run, label), color in zip(RUNS.items(), COLORS):
    pts = list(stages(run))
    x = [c for c, _ in pts]
    for ax, key in ((axes[0], "tps"), (axes[1], "ttft")):
        y = [s["throughput"]["output_tokens_per_sec"] if key == "tps" else s["latency"]["time_to_first_token"]["median"]
             for _, s in pts]
        ax.plot(x, y, color=color, linewidth=2, marker="o", markersize=8, markeredgecolor=SURFACE, label=label)
        ax.annotate(f"{y[-1]:.1f}", (x[-1], y[-1]), xytext=(8, 0), textcoords="offset points", va="center", color=INK)
    t, ws = memory(run)
    axes[2].plot(t, ws, color=color, linewidth=2, label=label)
    axes[2].annotate(f"{max(ws):.2f} GB", (t[ws.index(max(ws))], max(ws)), xytext=(6, 4), textcoords="offset points",
                     color=INK)
for ax in axes[:2]:
    ax.set_xticks([1, 2, 4])
    ax.set_xlabel("requests in flight", color=MUTED)
    ax.set_xlim(0.7, 4.6)
style(axes[0], "Output tokens/s (higher is better)", "tokens/s")
style(axes[1], "Median time to first token (lower is better)", "seconds")
style(axes[2], "llama-server resident memory during the run", "GB")
axes[2].set_xlabel("minutes since the run started", color=MUTED)
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, frameon=False, labelcolor=INK, loc="lower center", ncol=3)
fig.tight_layout(rect=(0, 0.08, 1, 1))
out = ROOT.parent.parent / "docs" / "images" / "llm_benchmark.png"
fig.savefig(out, dpi=110, facecolor=SURFACE)
print(out)
