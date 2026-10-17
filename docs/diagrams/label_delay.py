"""Histogram of the label delay (days from transaction to dispute) -> docs/images/label_delay.svg.

Run after generate.py: python docs/diagrams/label_delay.py
ponytail: hand-written SVG, no plotting library; one chart does not justify matplotlib.
"""
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "data" / "source"
OUT = ROOT / "docs" / "images" / "label_delay.svg"

disputes = pd.read_parquet(SRC / "core_banking" / "disputes.parquet", columns=["transaction_id", "reported_ts"])
txn = (ds.dataset(SRC / "core-banking-export" / "transactions", partitioning=None)
       .to_table(columns=["transaction_id", "txn_ts"], filter=ds.field("transaction_id").isin(disputes.transaction_id.tolist()))
       .to_pandas().drop_duplicates("transaction_id"))
delay = (disputes.merge(txn, on="transaction_id").pipe(lambda d: d.reported_ts - d.txn_ts).dt.total_seconds() / 86400).to_numpy()
counts, _ = np.histogram(delay, bins=np.arange(0, 61))
median, p95 = np.median(delay), np.percentile(delay, 95)

W, H, L, R, T, B = 760, 340, 56, 20, 64, 44   # canvas and plot margins
pw, ph = W - L - R, H - T - B
step = 100 if counts.max() < 1000 else 250
ymax = -(-counts.max() // step) * step
x = lambda d: L + d / 60 * pw
y = lambda c: T + ph - c / ymax * ph

svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui, sans-serif" font-size="12">',
       "<style>.bg{fill:#fcfcfb}.t1{fill:#0b0b0b}.t2{fill:#52514e}.grid{stroke:#e4e3df}.bar{fill:#2a78d6}.ref{stroke:#52514e}"
       "@media (prefers-color-scheme: dark){.bg{fill:#1a1a19}.t1{fill:#f0efec}.t2{fill:#c3c2b7}.grid{stroke:#383835}"
       ".bar{fill:#3987e5}.ref{stroke:#c3c2b7}}</style>",
       f'<rect class="bg" width="{W}" height="{H}"/>',
       f'<text class="t1" x="{L}" y="24" font-size="15" font-weight="600">Label delay: days from transaction to dispute</text>',
       f'<text class="t2" x="{L}" y="44">{len(delay):,} disputes, 1-day bins. Fraud labels are complete only after 60 days.</text>']
for c in range(0, ymax + 1, step):
    svg += [f'<line class="grid" x1="{L}" x2="{W - R}" y1="{y(c)}" y2="{y(c)}"/>',
            f'<text class="t2" x="{L - 8}" y="{y(c) + 4}" text-anchor="end">{c:,}</text>']
for d in range(0, 61, 10):
    svg.append(f'<text class="t2" x="{x(d)}" y="{H - B + 18}" text-anchor="middle">{d}</text>')
svg.append(f'<text class="t2" x="{L + pw / 2}" y="{H - 6}" text-anchor="middle">days after the transaction</text>')
bw = pw / 60 - 2   # 2px gap between bars
for d, c in enumerate(counts):
    if c:
        x0, top, r = x(d) + 1, y(c), min(4, bw / 2, (y(0) - y(c)) / 2)   # rounded top, flat on the baseline
        svg.append(f'<path class="bar" d="M{x0},{y(0)}V{top + r}Q{x0},{top} {x0 + r},{top}H{x0 + bw - r}'
                   f'Q{x0 + bw},{top} {x0 + bw},{top + r}V{y(0)}Z"><title>day {d}-{d + 1}: {c:,} disputes</title></path>')
for v, name in [(median, "median"), (p95, "p95")]:
    svg += [f'<line class="ref" x1="{x(v)}" x2="{x(v)}" y1="{T - 4}" y2="{y(0)}" stroke-dasharray="4 3"/>',
            f'<text class="t2" x="{x(v) + 6}" y="{T + 8}">{name} {v:.1f} d</text>']
svg.append("</svg>")
OUT.write_text("\n".join(svg), encoding="utf-8")
print(f"{OUT.relative_to(ROOT)}: median {median:.2f} d, p95 {p95:.2f} d, max bin {counts.max():,}")
