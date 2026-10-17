"""Ho Chi Minh City settled spend per month, by raw location name vs by normalized unit -> docs/images/hcmc_spend.svg.

Reads silver.stg_transactions through Trino (compose profile `batch` up): python docs/diagrams/hcmc_spend.py
ponytail: hand-written SVG like label_delay.py, no plotting library.
"""
import csv
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs" / "images" / "hcmc_spend.svg"
CUTOFF = "2025-07"
SQL = """SELECT date_format(txn_date, '%Y-%m'),
  sum(if(txn_city_raw = 'Ho Chi Minh', amount_vnd)) / 1e9, sum(if(txn_unit = 'Ho Chi Minh', amount_vnd)) / 1e9
FROM delta.silver.stg_transactions WHERE status = 'settled' AND txn_date < date '2026-10-01' GROUP BY 1 ORDER BY 1"""

out = subprocess.run(["docker", "compose", "-f", str(ROOT / "infra" / "docker-compose.yaml"), "exec", "-T", "trino",
                      "trino", "--output-format", "CSV", "--execute", SQL], capture_output=True, text=True, check=True).stdout
rows = [(m, float(raw), float(unit)) for m, raw, unit in csv.reader(out.splitlines())]
months = [r[0] for r in rows]

W, H, L, R, T, B = 760, 360, 56, 20, 64, 44
pw, ph = W - L - R, H - T - B
ymax = 80
x = lambda i: L + i / (len(rows) - 1) * pw
y = lambda v: T + ph - v / ymax * ph
# Direct labels at the left, where the lines are apart (after the cutoff they nearly overlap).
series = [("raw", 1, "by raw name", 22), ("unit", 2, "by unit (34 units)", -24)]

svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui, sans-serif" font-size="12">',
       "<style>.bg{fill:#fcfcfb}.t1{fill:#0b0b0b}.t2{fill:#52514e}.grid{stroke:#e4e3df}.ref{stroke:#52514e}"
       "polyline{fill:none}.raw{stroke:#eb6834}.unit{stroke:#2a78d6}.rawf{fill:#eb6834}.unitf{fill:#2a78d6}.hit{fill:transparent}"
       "@media (prefers-color-scheme: dark){.bg{fill:#1a1a19}.t1{fill:#f0efec}.t2{fill:#c3c2b7}.grid{stroke:#383835}"
       ".ref{stroke:#c3c2b7}.raw{stroke:#d95926}.unit{stroke:#3987e5}.rawf{fill:#d95926}.unitf{fill:#3987e5}}</style>",
       f'<rect class="bg" width="{W}" height="{H}"/>',
       f'<text class="t1" x="{L}" y="24" font-size="15" font-weight="600">Ho Chi Minh City: settled spend per month (billion VND)</text>',
       f'<text class="t2" x="{L}" y="44">By raw name the city "jumps" when old names are relabelled on 2025-07-01. By unit nothing changes.</text>']
for v in range(0, ymax + 1, 20):
    svg += [f'<line class="grid" x1="{L}" x2="{L + pw}" y1="{y(v)}" y2="{y(v)}"/>',
            f'<text class="t2" x="{L - 8}" y="{y(v) + 4}" text-anchor="end">{v}</text>']
for i, m in enumerate(months):
    if m.endswith("-01"):
        svg.append(f'<text class="t2" x="{x(i)}" y="{H - B + 18}" text-anchor="middle">{m[:4]}</text>')
c = x(months.index(CUTOFF))
svg += [f'<line class="ref" x1="{c}" x2="{c}" y1="{T}" y2="{y(0)}" stroke-dasharray="4 3"/>',
        f'<text class="t2" x="{c + 6}" y="{y(0) - 8}">2025-07-01: 63 provinces -> 34 units</text>']
for key, col, label, dy in series:
    pts = " ".join(f"{x(i):.1f},{y(r[col]):.1f}" for i, r in enumerate(rows))
    svg.append(f'<polyline class="{key}" points="{pts}" stroke-width="2" stroke-linejoin="round"/>')
    svg.append(f'<text class="t2" x="{x(1)}" y="{y(rows[1][col]) + dy}">{label}</text>')
for i, (m, raw, unit) in enumerate(rows):   # markers + bigger invisible hit targets with a tooltip
    tip = f"<title>{m}: {raw:.1f} by raw name, {unit:.1f} by unit</title>"
    for key, v in (("raw", raw), ("unit", unit)):
        svg.append(f'<circle class="{key}f" cx="{x(i):.1f}" cy="{y(v):.1f}" r="3"/>')
    svg.append(f'<rect class="hit" x="{x(i) - pw / len(rows) / 2:.1f}" y="{T}" width="{pw / len(rows):.1f}" height="{ph}">{tip}</rect>')
svg.append("</svg>")
OUT.write_text("\n".join(svg), encoding="utf-8")
before, after = rows[months.index(CUTOFF) - 1], rows[months.index(CUTOFF)]
print(f"{OUT.relative_to(ROOT)}: {len(rows)} months; raw {before[1]:.1f} -> {after[1]:.1f}, unit {before[2]:.1f} -> {after[2]:.1f}")
