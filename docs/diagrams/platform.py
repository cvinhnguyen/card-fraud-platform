"""Platform diagrams of the README (Layers 1-3), drawn from the tables in this file. Stdlib only.

  overview           the whole platform: every pipeline as a frame with its tasks in order, the stores between
                     them, the services and the people who use them
  system-layers-1-2  system diagram of Layers 1 + 2: every box is a deployable unit; arrows follow the data and
                     carry it as a label, numbered per flow with one colour per flow (rubric of the README diagram)
  system-layer-3     system diagram of Layer 3, same rules: agents, gateway, model servers, RAG ingest, CI/CD,
                     observability
  deployment         where every unit runs: host, Docker Desktop VM (compose profiles), kind cluster (namespaces),
                     internet

A box shows a name and a logo, never a sentence: what a part does is told by the arrows, the tasks drawn inside a
pipeline frame, and the tables under each picture. Logos: logos.json, each a data URI (Devicon, Simple Icons, the CNCF
and LF AI artwork repositories, the projects' own repositories). Coordinates are explicit: the script warns when an
arrow crosses a box or a label overlaps one, so a layout change is checked before anyone looks at the picture.

Usage: python docs/diagrams/platform.py [name ...]  ->  docs/images/platform-<name>.svg (+ .png through headless
       Chrome, if found) for every view (or only the named ones), and the arrow tables of docs/architecture.md.
"""
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "images"
LOGOS = json.loads((HERE / "logos.json").read_text())   # name -> [data URI, width / height]
USED: set = set()   # logos of the diagram being drawn: defined once in <defs>, placed with <use>
SANS = "'Segoe UI', 'Helvetica Neue', Arial, sans-serif"
MONO = "'Cascadia Mono', Consolas, 'Courier New', monospace"
INK, INK2, MUTED, LINE = "#2d3142", "#4f5d75", "#8a91a3", "#c8ccd4"
PAGE, PANEL, KIND_PANEL = "#f5f5f5", "#eceef1", "#e6ebf4"
STORE_FILL, STORE_LINE = "#eaf0f8", "#7d8fb3"   # a store: where the data rests between steps

# Flows: letter, name, who / when, colour. Normal-vision separation >= 17 (OKLab x100) between every pair; colour-blind
# readers tell flows apart by the letter in every badge.
FLOWS = {
    "S": ("Payment scoring", "end user: every card authorization, real time", "#e8590c"),
    "R": ("Streaming features", "continuous: Kafka -> Flink -> feature stores", "#1971c2"),
    "B": ("Batch data", "daily, Airflow: DP1, DP2, DP3, then the materialize pipeline", "#2f9e44"),
    "T": ("Training and deploy", "ML engineer: notebook, pipeline run, model image", "#9c36b5"),
    "M": ("Monitoring and retraining", "live Drift API + daily data drift pipeline", "#8d5524"),
}
# Layer 3 flows: data, engineer and monitoring share the colours of their Layer 1-2 counterparts; the conversation has
# its own (wine), because the overview shows it next to payment scoring (>= 16 OKLab x100 from every other flow).
FLOWS3 = {
    "A": ("Agent conversation", "support staff, every question: NGINX -> agents -> MCP tools and the LLM", "#7a1e55"),
    "K": ("Knowledge ingest", "RAG data pipeline, per release: documents, chunks, vectors", "#2f9e44"),
    "C": ("CI/CD", "developer push: Jenkins tests, builds, signs, deploys, evaluates", "#9c36b5"),
    "O": ("Observability and scaling", "continuous: metrics, traces, agent autoscaling", "#8d5524"),
}
ALL_FLOWS = {**FLOWS, **FLOWS3}


@dataclass
class Box:
    """A unit drawn as a card: tag top left, logo and name centred, then what it does (note) and the parts it holds
    (chips) under them."""
    x: float
    y: float
    w: float
    h: float
    title: str
    tag: str = ""
    icon: str = ""
    soft: bool = False   # support service: lighter, no main-flow arrows
    accent: str = ""     # novel idea: tinted card in this colour
    store: bool = False  # storage (bucket zone, database, topic): blue card, cylinder glyph
    chips: tuple = ()    # names of the parts it holds (topics, zones, policies): one pill each
    note: str = ""       # what it is or does, in plain words (wrapped to the box width)
    steps: tuple = ()    # tasks in run order (a pipeline drawn as a unit): name at the top, the tasks under it
    up: bool = False     # the tasks run bottom to top (when the first task's input arrives from below)

    def top(self, f=0.5):
        """Point on the top edge, f = fraction of the width from the left."""
        return (self.x + self.w * f, self.y)

    def bottom(self, f=0.5):
        """Point on the bottom edge, f = fraction of the width from the left."""
        return (self.x + self.w * f, self.y + self.h)

    def left(self, f=0.5):
        """Point on the left edge, f = fraction of the height from the top."""
        return (self.x, self.y + self.h * f)

    def right(self, f=0.5):
        """Point on the right edge, f = fraction of the height from the top."""
        return (self.x + self.w, self.y + self.h * f)

    def hit(self, x, y, pad=0.0):
        """Whether (x, y) lies inside the box grown by pad (negative pad = shrunk)."""
        return self.x - pad < x < self.x + self.w + pad and self.y - pad < y < self.y + self.h + pad


@dataclass
class Arrow:
    """A numbered arrow of one flow: orthogonal path, badge with flow letter + number, short label."""
    flow: str
    n: int
    label: str
    pts: list
    at: float = 0.5          # badge position, fraction of the path length
    side: str = ""           # label side of the badge: r, l, u, d (default: u on horizontal, r on vertical)
    desc: str = ""           # longer text for the README table

    @property
    def tag(self):
        """Badge text, e.g. S1."""
        return f"{self.flow}{self.n}"


def text_w(s: str, size: float, mono: bool, spacing: float = 0.0) -> float:
    """Approximate rendered width of a string (enough to place labels and check overlaps); spacing in em."""
    return len(s) * size * ((0.6 if mono else 0.56) + spacing)


def t(x, y, s, size=12.0, color=INK, anchor="middle", weight=400, mono=False, halo=None, spacing=0.0):
    """SVG text; halo = background-coloured outline so the text stays readable over lines."""
    h = f' stroke="{halo}" stroke-width="4" stroke-linejoin="round" paint-order="stroke"' if halo else ""
    ls = f' letter-spacing="{spacing}em"' if spacing else ""
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-family="{MONO if mono else SANS}" font-size="{size}" fill="{color}" '
            f'text-anchor="{anchor}" font-weight="{weight}"{ls}{h}>{escape(s)}</text>')


def icon(name, x, y, size=20, color=INK):
    """A logo from logos.json (defined once per diagram, placed by reference), or a line glyph without one."""
    if name in LOGOS:
        USED.add(name)
        return f'<use href="#lg-{name}" x="{x:.1f}" y="{y:.1f}" width="{size}" height="{size}"/>'
    glyph = {   # line icons for units without a logo
        "card": '<rect x="2" y="5" width="20" height="14" rx="2"/><path d="M2 10h20M6 15h4"/>',
        "phone": '<rect x="7" y="2" width="10" height="20" rx="2"/><path d="M11 18h2"/>',
        "person": '<circle cx="12" cy="7" r="4"/><path d="M4 22c0-4.4 3.6-8 8-8s8 3.6 8 8"/>',
        "people": '<circle cx="9" cy="8" r="3.5"/><path d="M2 21c0-3.9 3.1-7 7-7s7 3.1 7 7"/>'
                  '<circle cx="17" cy="7" r="3"/><path d="M17 13c2.8 0 5 2.2 5 5"/>',
        "gpu": '<rect x="2" y="6" width="20" height="12" rx="2"/><circle cx="9" cy="12" r="3"/><circle cx="16" cy="12" r="3"/>'
               '<path d="M6 18v3M18 18v3"/>',
        "check": '<circle cx="12" cy="12" r="9"/><path d="M8 12.5l3 3 5-6"/>',
        "bucket": '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5l2 14c.3 1.7 2.8 3 6 3s5.7-1.3 6-3l2-14"/>',
        "db": '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/>'
              '<path d="M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
        "lineage": '<circle cx="5" cy="6" r="2.5"/><circle cx="5" cy="18" r="2.5"/><circle cx="19" cy="12" r="2.5"/>'
                   '<path d="M7.5 6.5L16.5 11M7.5 17.5L16.5 13"/>',
    }[name]
    return (f'<svg x="{x}" y="{y}" width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" stroke="{color}" '
            f'stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">{glyph}</svg>')


def box_svg(b: Box) -> str:
    """SVG of one card."""
    stroke, fill = (LINE, "#fafafa") if b.soft else ("#9aa1b1", "#ffffff")
    dash = ' stroke-dasharray="4 3"' if b.soft else ""
    ink = b.accent or INK
    if b.store:
        stroke, fill = STORE_LINE, STORE_FILL
    if b.accent:
        stroke, fill = b.accent, b.accent + "14"   # 8% tint
    s = [f'<rect x="{b.x}" y="{b.y}" width="{b.w}" height="{b.h}" rx="8" fill="{fill}" stroke="{stroke}"{dash}/>']
    if b.tag:
        tw = text_w(b.tag, 9, True, 0.06) + 10   # the tag is drawn with 0.06em letter-spacing
        s.append(f'<rect x="{b.x + 8}" y="{b.y + 8}" width="{tw:.0f}" height="15" rx="3" fill="none" '
                 f'stroke="{b.accent or LINE}"/>')
        s.append(t(b.x + 13, b.y + 19, b.tag, 9, b.accent or INK2, "start", mono=True, spacing=0.06))
    rows = pill_rows(b.chips, b.w - 16)
    note = [ln for part in b.note.split("\n") for ln in wrap(part, b.w - 20, 11.5)]   # \n = a line break by meaning
    top = b.y + (22 if b.tag else 0)
    y0 = top + (b.y + b.h - top - 26 - 15 * len(note) - 26 * len(rows) - (10 if rows else 0)) / 2   # centred; pills keep 10 px
    if b.steps:
        y0 = top + 6
        s.append(chain(b.x + 14, y0 + 40, b.w - 28, b.steps, up=b.up)[0])
    ico = b.icon or ("db" if b.store else "")
    tw = text_w(b.title, 14.5, False)
    lx = b.x + (b.w - tw - (34 if ico else 0)) / 2
    if ico:
        s.append(icon(ico, lx, y0 + 1, 26, ink if not b.soft else INK2))
    s.append(t(lx + (34 if ico else 0), y0 + 19, b.title, 14.5, ink, "start", 600))
    for i, ln in enumerate(note):
        s.append(t(b.x + b.w / 2, y0 + 38 + 15 * i, ln, 11.5, INK2))
    for i, row in enumerate(rows):
        s.append(pills(row, b.x + b.w / 2, y0 + 32 + 15 * len(note) + 26 * i, "#ffffff"))
    return "\n".join(s)


def wrap(text: str, width: float, size: float) -> list:
    """Words wrapped into lines that fit the width."""
    lines, cur = [], ""
    for wd in text.split():
        if cur and text_w(cur + " " + wd, size, False) > width:
            lines.append(cur)
            cur = wd
        else:
            cur = (cur + " " + wd).strip()
    return lines + ([cur] if cur else [])


def pill_w(name: str) -> float:
    """Width of one pill."""
    return text_w(name, 11, False) + 18


def pill_rows(names, width) -> list:
    """Pills wrapped into rows that fit the width."""
    rows, cur = [], []
    for n in names:
        if cur and sum(pill_w(c) + 6 for c in cur) + pill_w(n) > width:
            rows.append(cur)
            cur = []
        cur.append(n)
    return rows + ([cur] if cur else [])


def pills(names, cx, y, fill=PAGE) -> str:
    """One row of pills centred on cx, top at y: the parts a box holds (topics, zones, policies)."""
    x = cx - (sum(pill_w(n) for n in names) + 6 * (len(names) - 1)) / 2
    s = []
    for n in names:
        w = pill_w(n)
        s.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="20" rx="10" fill="{fill}" stroke="{LINE}"/>')
        s.append(t(x + w / 2, y + 14, n, 11, INK2))
        x += w + 6
    return "\n".join(s)


SH, SG = 28, 14   # stage chip height, gap between two stages (room for the arrow)


def stage(x, y, w, name, ico="", accent="") -> str:
    """One task of a pipeline: white chip, optional logo, its name."""
    s = [f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{SH}" rx="6" fill="#ffffff" '
         f'stroke="{accent or "#9aa1b1"}"/>']
    tw = text_w(name, 12, False) + (22 if ico else 0)
    lx = x + (w - tw) / 2
    if ico:
        s.append(icon(ico, lx, y + 6, 16))
    s.append(t(lx + (22 if ico else 0), y + 18.5, name, 12, accent or INK, "start", 500))
    return "\n".join(s)


def step_arrow(p, q) -> str:
    """Short arrow from one stage to the next."""
    return (f'<path d="{path_d([p, q])}" fill="none" stroke="{INK2}" stroke-width="1.5" '
            f'marker-end="url(#ah-net)"/>')


def chain(x, y, w, steps, vertical=True, up=False) -> tuple[str, list]:
    """Tasks in run order, one under (or after) the other with an arrow between; up = the first task at the bottom.
    A list in steps = tasks that run in parallel, side by side. A step is a name or (name, logo).
    Returns (svg, [(x, y, w) of each row's chips], top to bottom)."""
    s, rows = [], []
    for i, st in enumerate(steps[::-1] if up else steps):
        group = st if isinstance(st, list) else [st]
        n = len(group)
        if vertical:
            cw = (w - 8 * (n - 1)) / n
            cells = [(x + k * (cw + 8), y + i * (SH + SG), cw) for k in range(n)]
        else:
            cells = [(x + i * (w + SG), y, w)]
        for (cx, cy, cw), g in zip(cells, group):
            name, ico = (g, "") if isinstance(g, str) else g
            s.append(stage(cx, cy, cw, name, ico))
        rows.append(cells)
    for a, b in zip(rows, rows[1:]):
        if vertical:
            for (ax, ay, aw) in a:   # every chip of a row points to the centre of each chip of the next
                for (bx, by, bw) in b:
                    if len(a) == 1 or len(b) == 1 or ax == bx:
                        p = (ax + aw / 2 if len(a) >= len(b) else bx + bw / 2, ay + SH)
                        q = (bx + bw / 2 if len(b) >= len(a) else ax + aw / 2, by)
                        s.append(step_arrow((q[0], q[1]), (p[0], p[1])) if up else step_arrow(p, q))
        else:
            (ax, ay, aw), (bx, by, bw) = a[0], b[0]
            s.append(step_arrow((ax + aw, ay + SH / 2), (bx, by + SH / 2)))
    return "\n".join(s), rows


def pframe(x, y, w, h, name, when="", accent="") -> str:
    """Dashed frame of one pipeline (or job group): its name, and what starts it, top left."""
    s = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="#ffffff" fill-opacity="0.55" '
         f'stroke="{accent or INK2}" stroke-width="1.2" stroke-dasharray="5 4"/>',
         t(x + 12, y + 18, name, 10.5, accent or INK, "start", 700, mono=True, spacing=0.06)]
    if when:
        s.append(t(x + 12, y + 33, when, 11, INK2, "start"))
    return "\n".join(s)


def actor(x, y, name, glyph="person", w=150) -> str:
    """A person or an outside system, drawn as a glyph with its name under it (no box): it uses the platform."""
    return "\n".join([icon(glyph, x + w / 2 - 20, y, 40, INK2), t(x + w / 2, y + 58, name, 13, INK, weight=600)])


def path_d(pts, r=7):
    """Orthogonal polyline with rounded corners."""
    d = [f"M{pts[0][0]:.1f},{pts[0][1]:.1f}"]
    for i in range(1, len(pts) - 1):
        (x0, y0), (x1, y1), (x2, y2) = pts[i - 1], pts[i], pts[i + 1]
        a = min(r, abs(x1 - x0) + abs(y1 - y0)) / max(1e-9, abs(x1 - x0) + abs(y1 - y0))
        b = min(r, abs(x2 - x1) + abs(y2 - y1)) / max(1e-9, abs(x2 - x1) + abs(y2 - y1))
        d.append(f"L{x1 - (x1 - x0) * a:.1f},{y1 - (y1 - y0) * a:.1f}")
        d.append(f"Q{x1:.1f},{y1:.1f} {x1 + (x2 - x1) * b:.1f},{y1 + (y2 - y1) * b:.1f}")
    d.append(f"L{pts[-1][0]:.1f},{pts[-1][1]:.1f}")
    return " ".join(d)


def point_at(pts, f):
    """Point at fraction f of the path length, and whether that segment is horizontal."""
    segs = [(a, b, abs(b[0] - a[0]) + abs(b[1] - a[1])) for a, b in zip(pts, pts[1:])]
    goal = f * sum(s[2] for s in segs)
    for a, b, ln in segs[:-1]:
        if goal <= ln:
            break
        goal -= ln
    else:
        a, b, ln = segs[-1]
    k = min(1.0, goal / ln) if ln else 0
    return (a[0] + (b[0] - a[0]) * k, a[1] + (b[1] - a[1]) * k), a[1] == b[1]


def label_box(a: Arrow):
    """Badge centre, label anchor point and the label's bounding box."""
    (bx, by), horizontal = point_at(a.pts, a.at)
    side = a.side or ("u" if horizontal else "r")
    w = text_w(a.label, 11, False)
    tx, ty, anchor = {"r": (bx + 15, by + 4, "start"), "l": (bx - 15, by + 4, "end"),
                      "u": (bx, by - 15, "middle"), "d": (bx, by + 23, "middle")}[side]
    x0 = {"start": tx, "end": tx - w, "middle": tx - w / 2}[anchor]
    return (bx, by), (tx, ty, anchor), (x0, ty - 10, x0 + w, ty + 3)


def arrow_svg(a: Arrow, halo: str) -> str:
    """SVG of one flow arrow: a halo underlay (the gap where it crosses an earlier arrow), the line, badge, label."""
    color = ALL_FLOWS[a.flow][2]
    d = path_d(a.pts)
    (bx, by), (tx, ty, anchor), _ = label_box(a)
    return "\n".join([
        f'<path d="{d}" fill="none" stroke="{halo}" stroke-width="7"/>',   # gap where it crosses an earlier arrow
        f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2" marker-end="url(#ah-{a.flow})"/>',
        f'<circle cx="{bx:.1f}" cy="{by:.1f}" r="11" fill="#ffffff" stroke="{color}" stroke-width="2"/>',
        t(bx, by + 3.6, a.tag, 9.5, INK, weight=700),
        t(tx, ty, a.label, 11, INK, anchor, halo=halo),
    ])


def check(name: str, boxes: dict, arrows: list) -> None:
    """Warn on arrows through boxes and on labels over boxes, badges or other labels."""
    for a in arrows:
        for (x0, y0), (x1, y1) in zip(a.pts, a.pts[1:]):
            steps = int(max(abs(x1 - x0), abs(y1 - y0)) / 4) + 1
            for k, b in boxes.items():
                inside = [b.hit(x0 + (x1 - x0) * i / steps, y0 + (y1 - y0) * i / steps, -2) for i in range(steps + 1)]
                if any(inside[1:-1]) and not (b.hit(*a.pts[0], 1) or b.hit(*a.pts[-1], 1)):
                    print(f"  ! {name}: {a.tag} crosses box {k}")
    rects = [(a.tag, label_box(a)[2]) for a in arrows]
    rects += [(f"{a.tag}-badge", (bx - 11, by - 11, bx + 11, by + 11)) for a in arrows for (bx, by) in [label_box(a)[0]]]
    for i, (n1, r1) in enumerate(rects):
        for k, b in boxes.items():
            if r1[0] < b.x + b.w and r1[2] > b.x and r1[1] < b.y + b.h and r1[3] > b.y:
                print(f"  ! {name}: label {n1} over box {k}")
        for n2, r2 in rects[i + 1:]:
            if n1.split("-")[0] != n2.split("-")[0] and r1[0] < r2[2] and r1[2] > r2[0] and r1[1] < r2[3] and r1[3] > r2[1]:
                print(f"  ! {name}: {n1} overlaps {n2}")


def crossings(name: str, arrows: list) -> None:
    """List every point where two arrows of the diagram cross (drawn with a gap; each one is a reading cost)."""
    segs = [(a.tag, p, q) for a in arrows for p, q in zip(a.pts, a.pts[1:])]
    for i, (n1, (ax, ay), (bx, by)) in enumerate(segs):
        for n2, (cx, cy), (dx, dy) in segs[i + 1:]:
            if n1 == n2:
                continue
            if ay == by and cx == dx and min(ax, bx) < cx < max(ax, bx) and min(cy, dy) < ay < max(cy, dy):
                print(f"  ~ {name}: {n1} crosses {n2} at ({cx:.0f}, {ay:.0f})")
            elif ax == bx and cy == dy and min(cx, dx) < ax < max(cx, dx) and min(ay, by) < cy < max(ay, by):
                print(f"  ~ {name}: {n1} crosses {n2} at ({ax:.0f}, {cy:.0f})")


def markers() -> str:
    """Arrowheads: one per flow colour and a neutral one for network paths."""
    return "".join(f'<marker id="ah-{k}" markerWidth="9" markerHeight="8" refX="8" refY="4" orient="auto" '
                   f'markerUnits="userSpaceOnUse"><path d="M0,0 L9,4 L0,8 z" fill="{v[2]}"/></marker>'
                   for k, v in ALL_FLOWS.items()) + \
        f'<marker id="ah-net" markerWidth="9" markerHeight="8" refX="8" refY="4" orient="auto" ' \
        f'markerUnits="userSpaceOnUse"><path d="M0,0 L9,4 L0,8 z" fill="{INK2}"/></marker>'


def panel(x, y, w, h, title, fill, stroke, dashed=False, note=""):
    """A titled background region (runtime, layer or group)."""
    dash = ' stroke-dasharray="6 4"' if dashed else ""
    s = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{fill}" stroke="{stroke}"{dash}/>',
         t(x + 16, y + 22, title, 11, INK2, "start", 600, mono=True, spacing=0.08)]
    if note:
        s.append(t(x + 16 + text_w(title, 11, True, 0.08) + 14, y + 22, note, 11, MUTED, "start", mono=True))
    return "\n".join(s)


def svg_doc(w, h, body, title, desc) -> str:
    """Complete SVG document with title and description for screen readers; each logo used is defined once."""
    logos = "".join(f'<symbol id="lg-{k}" viewBox="0 0 {100 * LOGOS[k][1]:.0f} 100"><image href="{LOGOS[k][0]}" '
                    f'width="{100 * LOGOS[k][1]:.0f}" height="100"/></symbol>' for k in sorted(USED))
    USED.clear()
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" role="img">\n'
            f'<title>{escape(title)}</title><desc>{escape(desc)}</desc>\n<defs>{markers()}{logos}</defs>\n'
            f'<rect width="{w}" height="{h}" fill="{PAGE}"/>\n{body}\n</svg>\n')


# the training pipeline's tasks, in run order (ml/pipelines/train_fraud_model.py: load_labels ... register)
TRAIN_STEPS = ("Load labels", "Build training set", "Check the data", "Split by time", ("Train XGBoost", "spark"),
               "Evaluate", ("Register", "mlflow"))


# ---------------------------------------------------------------- architecture
BW, BH, GX, GY = 170, 84, 64, 72
S3H = 100                                  # the storage box: name, one note line, one row of bucket chips
COLS = (296, 530, 764, 998, 1262, 1496)   # wider gap c4|c5: B1 and B7 share it
KX = 1850                                  # boxes of the kind cluster
GAPX = 242                                 # lane between the outside panel and compose: R1 runs down it


def c(i):
    """x of grid column i (1-based)."""
    return COLS[i - 1]


def r(j):
    """y of grid row j (1-based)."""
    return 128 + (j - 1) * (BH + GY)


def system_layers_1_2():
    """The system diagram of Layers 1 and 2; returns (svg, arrows) so the arrow table comes from the same data."""
    B = {
        "card": Box(28, r(1), 172, BH, "Card network", "host", "card", note="each payment, replayed\nby a script"),
        "app": Box(28, r(2), 172, BH, "Banking app", "host", "phone", note="app events, replayed\nby a script"),
        "api": Box(c(3), r(1), BW, BH, "Scoring API", "ml",
                   "fastapi", note="approves, steps up or declines each payment"),
        "redis": Box(c(3), r(2), BW, BH, "Redis", "ml", "redis", note="latest card features"),
        "pushon": Box(c(2), r(3), BW, BH, "push-online", "ml", "python", note="copies live features into Redis"),
        "fwd": Box(c(3), r(3), BW, BH, "drift-forwarder", "ml", "python", note="sends each decision to the Drift API"),
        "drift": Box(c(4), r(3), BW, BH, "Drift API", "ml", "fastapi", note="PSI of each feature\nper minute"),
        "registry": Box(c(5), r(3), BW, BH, "Docker registry", "ml",
                        "docker", note="keeps the built images"),
        "kafka": Box(c(1), r(4), c(3) + BW - c(1), BH + 26, "Kafka", "core", "kafka",
                     chips=("card_authorizations", "app_events", "card_features_rt", "card_device_seen",
                            "fraud_predictions", "ato_alerts_cep")),
        "pg": Box(c(4), r(4), BW, BH, "PostgreSQL", "core", "postgres", note="the bank's core banking data"),
        "mlflow": Box(c(5), r(4), BW, BH, "MLflow", "ml", "mlflow", note="model versions;\nchallenger, production"),
        "flink": Box(c(1), r(5), BW, BH, "Flink", "stream",
                     "flink", note="turns payments into live features"),
        "pushoff": Box(c(2), r(5), BW, BH, "push-offline", "ml",
                       "python", note="copies live features into the lakehouse"),
        "trino": Box(c(3), r(5), BW, BH, "Trino", "batch", "trino", note="SQL queries for analysts"),
        "spark": Box(c(4), r(5), BW, BH, "Spark", "batch", "spark", note="runs the batch jobs"),
        "airflow": Box(c(5), r(5), BW, BH, "Airflow", "orch",
                       "airflow", note="schedules and runs the pipelines"),
        "jupyter": Box(c(6), r(5), BW, BH, "Jupyter", "ml", "jupyter", note="notebook to explore data and train"),
        "s3": Box(c(1), r(5) + BH + GY, c(6) + BW - c(1), S3H, "SeaweedFS (S3) + Delta Lake", "core", "seaweedfs",
                  note="four buckets; the lakehouse bucket holds one folder per zone, as Delta tables",
                  chips=("lakehouse/landing", "lakehouse/bronze", "lakehouse/silver", "lakehouse/gold + feat_*",
                         "lakehouse/ml", "core-banking-export", "lakefs-data", "mlflow")),
        "kserve": Box(KX, r(1), 300, BH, "KServe model server", "ns serving", "kserve", note="serves the fraud model"),
        "kfp": Box(KX, r(4), 300, r(5) + BH + GY + S3H - r(4), "Training pipeline", "ns kubeflow", "kubeflow",
                   steps=TRAIN_STEPS, up=True),   # its input (labels + features) arrives at the bottom
    }
    sy = B["s3"].y + B["s3"].h + 26
    B["hive"] = Box(c(1), sy, 230, 72, "Hive Metastore", "batch", "hive", True, note="table catalog")
    B["lakefs"] = Box(c(1) + 242, sy, 2 * BW + GX - 242, 72, "lakeFS", "batch", "lakefs", True,
                      note="branches and commits of bronze")
    B["mldb"] = Box(c(3), sy, 2 * BW + GX, 72, "ml-db (PostgreSQL)", "ml", "postgres", True,
                    note="MLflow and Feast metadata")
    B["datahub"] = Box(c(5), sy, c(6) + BW - c(5), 72, "DataHub", "gov", "datahub", True, note="data catalog and lineage")

    s3y = B["s3"].y
    api, redis, kafka, kfp = B["api"], B["redis"], B["kafka"], B["kfp"]
    A = [
        Arrow("S", 1, "payment to score", [B["card"].right(0.32), api.left(0.32)], 0.5,
              desc="The card network's switch sends each payment authorization to POST /v1/score (card, amount, channel, device)"),
        Arrow("S", 2, "online features", [redis.top(0.35), api.bottom(0.35)],
              desc="Feast reads the card's stored features from Redis (feature service fraud_model_v1)"),
        Arrow("S", 3, "20 features", [api.right(0.3), B["kserve"].left(0.3)], 0.3,
              desc="Feature vector to the model server (KServe V1 protocol, NodePort 31080)"),
        Arrow("S", 4, "fraud score", [B["kserve"].left(0.7), api.right(0.7)], 0.7, "d",
              desc="Probability of fraud from the production XGBoost booster"),
        Arrow("S", 5, "approve / step_up / decline", [api.left(0.68), B["card"].right(0.68)], 0.5, "d",
              desc="Decision by the two thresholds; rule fallback when the model is late or down"),
        Arrow("S", 6, "decision + features", [api.bottom(0.12), (api.x + 20, 248), (c(1) + 85, 248), kafka.top(85 / kafka.w)],
              0.55, desc="Every decision is logged to Kafka topic fraud_predictions (scoring never waits on monitoring)"),
        Arrow("R", 1, "payments", [B["card"].right(0.9), (GAPX, B["card"].y + BH * 0.9), (GAPX, kafka.y + 30),
                                   kafka.left(30 / kafka.h)], 0.5, "l",
              desc="Every authorization (retries included) to Kafka topic card_authorizations (generator/producer.py replays "
                   "the generated stream in arrival order)"),
        Arrow("R", 2, "app events", [B["app"].bottom(), (B["app"].x + 86, kafka.y + 80), kafka.left(80 / kafka.h)],
              desc="The mobile banking app's events (login, password reset, new payee, transfer, biometric check) to Kafka "
                   "topic app_events, replayed the same way; about 15 % arrive after the next event of their session"),
        Arrow("R", 3, "payments", [kafka.bottom(55 / kafka.w), B["flink"].top(55 / BW)], side="l",
              desc="Flink reads card_authorizations (watermarks, dedup of network retries); ato_cep reads app_events "
                   "(account takeover patterns -> ato_alerts_cep)"),
        Arrow("R", 4, "features, alerts", [B["flink"].top(115 / BW), kafka.bottom(115 / kafka.w)],
              desc="Realtime features to card_features_rt, first (card, device) pairs to card_device_seen; ato_cep writes one "
                   "alert per matched pattern to ato_alerts_cep"),
        Arrow("R", 5, "raw events as files", [B["flink"].bottom(), (B["flink"].x + 85, s3y)],
              desc="raw_landing writes the raw authorizations as JSON lines to the S3 landing zone; DP1 (ingest to bronze) loads them into "
                   "bronze.raw_card_authorizations (Delta)"),
        Arrow("R", 6, "features", [kafka.top((c(2) + 85 - c(1)) / kafka.w), B["pushon"].bottom()],
              desc="push-online reads card_features_rt and card_device_seen"),
        Arrow("R", 7, "Feast push (online)", [B["pushon"].top(), (c(2) + 85, redis.y + 42), redis.left()], 0.75, "u",
              desc="Writes them to the Redis online store through Feast push (offsets committed after the write)"),
        Arrow("R", 8, "features", [kafka.bottom((c(2) + 85 - c(1)) / kafka.w), B["pushoff"].top()],
              desc="push-offline reads the same two topics"),
        Arrow("R", 9, "offline copy", [B["pushoff"].bottom(), (c(2) + 85, s3y)],
              desc="Appends them to gold.feat_card_auth_rt / gold.feat_card_device: the offline copy for training"),
        Arrow("B", 1, "runs the jobs", [B["airflow"].left(0.6), B["spark"].right(0.6)], side="d",
              desc="Airflow submits the Spark jobs of DP1, DP2 and DP3 (a call, no data); each pipeline ends with Great Expectations checks"),
        Arrow("B", 2, "source tables (JDBC)", [B["pg"].bottom(), B["spark"].top()], side="l",
              desc="DP1 copies the core banking tables (customers, accounts, cards, merchants, disputes)"),
        Arrow("B", 3, "export files", [(c(4) + 32, s3y), B["spark"].bottom(32 / BW)], side="l",
              desc="DP1 loads the new daily export folders from s3://core-banking-export"),
        Arrow("B", 4, "writes each zone", [B["spark"].bottom(138 / BW), (c(4) + 138, s3y)],
              desc="DP1 writes bronze, DP2 silver and gold (dbt), DP3 the feat_* tables, all Delta in s3://lakehouse; "
                   "bronze.raw_transactions goes through lakeFS: a branch per DP1 run, merged into main after the checks"),
        Arrow("B", 5, "Delta tables", [(c(3) + 85, s3y), B["trino"].bottom()],
              desc="Trino reads the Delta tables of any zone for analysts; the Hive Metastore tells it where each table "
                   "lives (bronze.raw_transactions: catalog lakefs, branch main)"),
        Arrow("B", 6, "features", [(c(5) + 40, s3y), B["airflow"].bottom(40 / BW)], side="r",
              desc="The incremental materialize pipeline (DAG feast_materialize, after DP3) reads feat_card_txn_stats_daily through "
                   "the Feast offline store"),
        Arrow("B", 7, "newest per card", [B["airflow"].left(0.22), (c(4) + BW + 14, B["airflow"].y + BH * 0.22),
                                       (c(4) + BW + 14, 266), (c(3) + 145, 266), redis.top(145 / BW)], 0.6, "u",
              desc="... and loads the newest row per card into Redis (feast materialize-incremental)"),
        Arrow("T", 1, "point-in-time features", [(c(6) + 85, s3y), B["jupyter"].bottom()],
              desc="The notebook builds the training set: labels + Feast point-in-time features"),
        Arrow("T", 2, "notebook run", [B["jupyter"].top(0.6), (c(6) + 102, B["mlflow"].y + 70), B["mlflow"].right(70 / BH)],
              0.75, "u", desc="Metrics, plots and both models of the notebook run"),
        Arrow("T", 3, "labels + features", [(B["s3"].x + B["s3"].w, s3y + 24), (kfp.x, s3y + 24)], 0.5, "u",
              desc="Steps 1 and 2 (load labels, build the training set) read the labels as of label_as_of and the point-in-time "
                   "features"),
        Arrow("T", 4, "training set snapshot", [(kfp.x, s3y + 56), (B["s3"].x + B["s3"].w, s3y + 56)], 0.5, "d",
              desc="Step 2 MERGEs the run's rows into ml/training_set_fraud_model_v1, one Delta version per run, so every model "
                   "can be traced back to the exact rows it learned from"),
        Arrow("T", 5, "metrics + candidate model", [kfp.left(0.06), (B["mlflow"].x + BW, kfp.y + kfp.h * 0.06)], 0.6, "u",
              desc="Steps 5 to 7 (train, evaluate, register) log params, data versions, metrics and the model; alias challenger "
                   "when it passes the evaluation gate"),
        Arrow("T", 6, "production model", [B["mlflow"].top(), B["registry"].bottom()],
              desc="ML engineer: fetch_model.py --alias production, docker build + push of fraud/predictor:<v>"),
        Arrow("T", 7, "image pull", [B["registry"].right(), (B["kserve"].x + 150, B["registry"].y + 42),
                                     B["kserve"].bottom()], 0.4, "u",
              desc="kubectl apply of the InferenceService; the node pulls the image from the registry"),
        Arrow("M", 1, "decisions", [kafka.top((c(3) + 85 - c(1)) / kafka.w), B["fwd"].bottom()],
              desc="The forwarder consumes the prediction log"),
        Arrow("M", 2, "decisions", [B["fwd"].right(), B["drift"].left()], side="u",
              desc="Batches to POST /v1/events"),
        Arrow("M", 3, "per-minute counts", [B["drift"].top(0.5), (c(4) + 85, redis.y + 62), redis.right(62 / BH)], 0.7, "d",
              desc="Bin counts per feature and minute in Redis db 1; GET /v1/drift returns the PSI vs the training profile"),
        Arrow("M", 4, "7 days + labels", [(c(5) + 140, s3y), B["airflow"].bottom(140 / BW)],
              desc="Data drift pipeline (DAG drift_monitoring, daily): the last 7 days of payments, their labels and point-in-time "
                   "features"),
        Arrow("M", 5, "production model", [B["mlflow"].bottom(0.25), B["airflow"].top(0.25)], side="l",
              desc="Loads the production model to score them"),
        Arrow("M", 6, "drift metrics", [B["airflow"].top(0.75), B["mlflow"].bottom(0.75)],
              desc="Logs PR-AUC, recall and PSI as a run of experiment card_fraud_monitoring"),
        Arrow("M", 7, "start training", [B["airflow"].right(0.3), (c(5) + BW + 48, B["airflow"].y + BH * 0.3),
                                      (c(5) + BW + 48, r(5) - 36), (kfp.x, r(5) - 36)], 0.82, "u",
              desc="On drift: a training pipeline run (train_fraud_model) through the Kubeflow Pipelines API, label_as_of = the "
                   "DAG's day (a call, no data)"),
    ]

    W, H = 2206, 1320
    body = [t(28, 46, "CARD FRAUD PLATFORM · SYSTEM DIAGRAM · LAYERS 1 AND 2", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "System diagram, Layers 1 and 2: deployable units and the data they exchange. Layer 3, the agents, "
                      "has its own diagram.", 22, INK, "start", 400),
            panel(14, 96, 200, r(2) + BH + 16 - 96, "OUTSIDE", "none", LINE, True),
            panel(270, 96, c(6) + BW + 26 - 270, B["datahub"].y + 72 + 16 - 96, "DOCKER COMPOSE · project fraud", PANEL, LINE,
                  note="tag = compose profile"),
            panel(KX - 30, 96, 362, kfp.y + kfp.h + 16 - 96, "KIND CLUSTER · fraud", KIND_PANEL, "#b5c0d6",
                  note="tag = namespace")]
    body += [box_svg(b) for b in B.values()]
    body += [arrow_svg(a, PANEL) for a in A]
    body.append(legend(28, B["datahub"].y + 72 + 40, FLOWS, [
        "Numbers give the order inside each flow: S1 to S6 follow one payment from the card network and back.",
        "Arrow = data moving from A to B, labelled with the data; a call without data (B1, M7) points from caller to callee.",
        "White box = one container or pod. Tag top left = where it runs: compose profile, namespace (ns) or host.",
        "Dashed boxes: support services used by many units (arrows left out).",
        "Feast is a library, not a unit: it runs inside the Scoring API, the push jobs, Airflow, Jupyter and the training pipeline's pods.",
        "Layer 3 (agents, gateway, tool servers) is on its own diagram; its tool servers read Redis and call the Scoring API (A10, A13)."]))
    check("system-layers-1-2", B, A)
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: system diagram, Layers 1 and 2",
                   "Deployable units of Layer 1 and Layer 2 and the data between them, numbered per flow."), A


def legend(x, y, flows, notes):
    """Flow legend (3 per row) and reading notes under a system diagram."""
    s = [t(x, y + 12, "FLOWS", 11, INK2, "start", 600, mono=True, spacing=0.14)]
    for i, (k, (name, who, color)) in enumerate(flows.items()):
        cx, cy = x + (i % 3) * 700, y + 40 + (i // 3) * 34
        s.append(f'<path d="M{cx},{cy} h40" stroke="{color}" stroke-width="2" marker-end="url(#ah-{k})"/>')
        s.append(f'<circle cx="{cx + 20}" cy="{cy}" r="11" fill="#fff" stroke="{color}" stroke-width="2"/>')
        s.append(t(cx + 20, cy + 3.6, k, 9.5, INK, weight=700))
        s.append(t(cx + 56, cy + 4, f"{name}", 13, INK, "start", 600))
        s.append(t(cx + 56 + text_w(name, 13, False) + 10, cy + 4, who, 11, INK2, "start"))
    s += [t(x + 1400 if len(flows) % 3 else x, y + 74 + 16 * i + (0 if len(flows) % 3 else 34), n, 11, INK2, "start")
          for i, n in enumerate(notes)]
    return "\n".join(s)


# ---------------------------------------------------------------- layer 3
K3 = (300, 540, 780, 1020, 1260)   # kind columns
C3 = (1496, 1736, 1976)            # host / compose / internet columns


def q(j):
    """y of Layer 3 grid row j (1-based); rows start lower to leave two lanes above the panels."""
    return 156 + (j - 1) * (BH + GY)


def system_layer_3():
    """The Layer 3 system diagram (agents, gateway, model servers, RAG, CI/CD, observability); returns (svg, arrows)."""
    k1, k2, k3, k4, k5 = K3
    c6, c7, c8 = C3
    B = {
        "staff": Box(28, q(1), 180, BH, "Support staff", "user", "people", note="ask questions about cards"),
        "acme": Box(28, q(2), 180, BH, "Let's Encrypt", "internet", "letsencrypt", note="issues the HTTPS certificate", soft=True),
        "groq": Box(28, q(3), 180, BH, "Groq API", "internet", note="second LLM for an A/B test (off)", soft=True),
        "nginx": Box(k1, q(1), BW, BH, "NGINX Ingress", "ns nginx-ingress", "nginx", note="HTTPS entry and login"),
        "vault": Box(k1, q(2), BW, BH, "Vault + ESO", "ns vault", "vault", note="keeps the secrets", soft=True),
        "substrate": Box(k1, q(3), BW, BH, "Agent Substrate", "ns ate-system", note="runs each chat in a sandbox", soft=True),
        "areg": Box(k1, q(4), BW, BH, "agentregistry", "ns agentregistry", note="catalog of agents and tools",
                    soft=True),
        "kagent": Box(k2, q(1), BW, BH, "kagent", "ns kagent", "kagent", note="chat UI, runs the agents"),
        "jaeger": Box(k2, q(3), BW, BH, "Jaeger", "ns monitoring", "jaeger", note="traces of each agent call"),
        "vlogs": Box(k2, q(4), BW, BH, "VictoriaLogs", "ns monitoring", "victoria", note="logs of every pod"),
        "coord": Box(k3, q(1), BW, BH, "coordinator", "ns kagent", "kagent", note="splits the question, asks the agents"),
        "card": Box(k3, q(2), BW, BH, "card-data-agent", "ns kagent", "kagent", note="looks up card data and policy"),
        "risk": Box(k3, q(3), BW, BH, "risk-agent", "ns kagent", "kagent", note="checks fraud risk and policy"),
        "keda": Box(k3, q(4), BW, BH, "KEDA", "ns keda", "keda", note="adds agent pods under load"),
        "gw": Box(k4, q(1), BW, q(3) + BH - q(1), "agentgateway", "ns agentgateway-system", "agentgateway", note="every agent call passes here",
                  chips=("A2A routes", "MCP routes", "LLM route", "API key per agent", "rate + token limits",
                         "PII masking")),
        "prom": Box(k4, q(4), BW, BH, "Prometheus", "ns monitoring", "prometheus", note="collects metrics"),
        "grafana": Box(k4, q(5), BW, BH, "Grafana", "ns monitoring", "grafana", note="dashboards"),
        "llmd": Box(k5, q(1), BW, BH, "llm-d router", "ns agentgateway-system", "llmd", note="picks a model server per prompt"),
        "rag": Box(k5, q(2), BW, BH, "policy-rag-mcp", "ns card-support", "fastapi", note="tool: searches the bank's policies"),
        "cdata": Box(k5, q(3), BW, BH, "card-data-mcp", "ns card-support",
                     "fastapi", note="tool: a card's data, policy text"),
        "fscore": Box(k5, q(4), BW, BH, "fraud-score-mcp", "ns card-support",
                      "fastapi", note="tool: fraud score of a payment"),
        "chat": Box(c6, q(1), BW, BH, "llama.cpp chat", "host", "llamacpp", note="chat model on the GPU"),
        "embed": Box(c6, q(2), BW, BH, "llama.cpp embed", "host", "llamacpp", note="turns text into vectors"),
        "vdb": Box(c6, q(3), BW, BH, "vector-db", "ml", "postgres", note="policy text and its vectors"),
        "redis": Box(c6, q(4), BW, BH, "Redis", "ml", "redis", note="latest card features"),
        "scoring": Box(c6, q(5), BW, BH, "Scoring API", "ml", "fastapi", note="fraud score and decision"),
        "github": Box(c7, q(1), BW, BH, "GitHub", "internet", "github", note="the code"),
        "langfuse": Box(c8, q(1), BW, BH, "Langfuse Cloud", "internet", "langfuse", note="prompts and eval results"),
        "jenkins": Box(c8, q(2), BW, BH, "Jenkins", "CI/CD", "jenkins", note="tests, builds and deploys"),
        "registry": Box(c7, q(3), BW, BH, "Docker registry", "ml", "docker", note="keeps the signed images"),
        "airflow": Box(c7, q(4), BW, BH, "Airflow", "orch", "airflow", note="runs the RAG data pipeline"),
        "s3": Box(c7, q(5), BW, BH, "SeaweedFS (S3)", "core", "seaweedfs", note="bank documents, chunk history"),
        "datahub": Box(c8, q(4), BW, BH, "DataHub", "gov", "datahub", note="lineage and check results"),
    }
    b = B
    fx, fy, fw, fh = k3 - 14, q(1) - 14, BW + 28, q(3) + BH + 14 - (q(1) - 14)   # frame around the three agents
    mx, my, mw, mh = k5 - 14, q(2) - 14, BW + 28, q(4) + BH + 14 - (q(2) - 14)   # frame around the three MCP servers
    lane_c5, lane_c6 = 88, 106                                                       # CI lanes above the panels
    A = [
        Arrow("A", 1, "question", [b["staff"].right(), b["nginx"].left()],
              desc="A question in the kagent chat (https://agents.<domain>, basic auth at NGINX); a support app "
                   "calls the gateway route /a2a/coordinator instead"),
        Arrow("A", 2, "question", [b["nginx"].right(), b["kagent"].left()],
              desc="NGINX passes it to kagent-ui (host agents.<domain>, streamed answers, no buffering)"),
        Arrow("A", 3, "task", [b["kagent"].right(), b["coord"].left()],
              desc="kagent's controller sends the task to the coordinator over A2A (kagent's own control path)"),
        Arrow("A", 4, "A2A + LLM", [b["coord"].right(), (k4, q(1) + 42)],
              desc="The coordinator's A2A calls to the two agents and its LLM calls, all through the gateway "
                   "(kagent proxy.url)"),
        Arrow("A", 5, "sub-task", [(k4, q(2) + 28), b["card"].right(28 / BH)],
              desc="Route agents-internal: a sub-task for card-data-agent; only the coordinator's key may call it"),
        Arrow("A", 6, "sub-task", [(k4, q(3) + 28), b["risk"].right(28 / BH)],
              desc="... and for risk-agent (risk-agent-b, the B side of its model-config A/B test, sits behind "
                   "the same route)"),
        Arrow("A", 7, "MCP + LLM", [b["card"].right(60 / BH), (k4, q(2) + 60)], side="d",
              desc="card-data-agent's tool calls (MCP, its own API key) and LLM calls through the gateway"),
        Arrow("A", 8, "MCP + LLM", [b["risk"].right(60 / BH), (k4, q(3) + 60)], side="d",
              desc="risk-agent's tool calls and LLM calls through the gateway"),
        Arrow("A", 9, "MCP", [(k4 + BW, q(2) + 108), (mx, q(2) + 108)],
              desc="Routes /mcp/card-data, /mcp/fraud-score, /mcp/policy-rag: only the key of each server's agent "
                   "sees its tools (300 calls/min per server)"),
        Arrow("A", 10, "features", [b["redis"].left(20 / BH), (k5 + BW + 20, b["redis"].y + 20),
                                    (k5 + BW + 20, q(3) + 64), (k5 + BW, q(3) + 64)], 0.6, "r",
              desc="card-data-mcp, get_card_data: the card's online features from Redis through Feast (project "
                   "card_fraud)"),
        Arrow("A", 11, "chunk", [b["vdb"].left(), (k5 + BW, b["vdb"].y + 42)],
              desc="card-data-mcp, get_policy_chunk: one policy chunk by id from pgvector (Feast project card_support_kb)"),
        Arrow("A", 12, "vector", [b["embed"].left(), (k5 + BW, b["embed"].y + 42)],
              desc="policy-rag-mcp, search_policy: the question's vector from Qwen3-Embedding-0.6B"),
        Arrow("A", 13, "score", [b["scoring"].left(), (k5 + 120, b["scoring"].y + 42), b["fscore"].bottom(120 / BW)], 0.3,
              desc="fraud-score-mcp, score_payment: the Layer 2 Scoring API answers with the fraud score and its decision"),
        Arrow("A", 14, "prompts", [(k4 + BW, q(1) + 42), b["llmd"].left()],
              desc="Route /v1: the agent's API key is checked, request and token limits applied, personal data masked, then the "
                   "InferencePool qwen3-8b"),
        Arrow("A", 15, "prompt", [b["llmd"].right(), b["chat"].left()],
              desc="The endpoint picker chooses a pod; its bridge forwards the prompt to llama.cpp on the host GPU"),
        Arrow("A", 16, "nearest chunks", [b["vdb"].left(20 / BH), (k5 + BW + 45, b["vdb"].y + 20),
                                  (k5 + BW + 45, q(2) + 64), (k5 + BW, q(2) + 64)], 0.45, "r",
              desc="policy-rag-mcp, search_policy: the chunks nearest to the question vector from pgvector, re-ranked "
                   "by cosine"),
        Arrow("K", 1, "documents", [b["s3"].top(0.3), b["airflow"].bottom(0.3)], side="l",
              desc="rag_ingest reads the 5 bank documents from s3://bank-documents"),
        Arrow("K", 2, "vectors", [b["embed"].right(), (c7 - 30, b["embed"].y + 42), (c7 - 30, b["airflow"].y + 25),
                                  b["airflow"].left(25 / BH)], 0.3,
              desc="Steps chunk (57 chunks) and embed: one 1024-dim vector per chunk from the embedding server"),
        Arrow("K", 3, "chunks", [b["airflow"].left(59 / BH), (c6 + BW + 20, b["airflow"].y + 59),
                                 (c6 + BW + 20, b["vdb"].y + 59), (c6 + BW, b["vdb"].y + 59)], 0.62, "l",
              desc="Step write: chunks + vectors into the Feast online store (pgvector)"),
        Arrow("K", 4, "chunk history", [b["airflow"].bottom(0.7), b["s3"].top(0.7)],
              desc="... and their history into the offline store s3://lakehouse/kb/policy_chunks.parquet"),
        Arrow("K", 5, "lineage", [b["airflow"].right(), b["datahub"].left()],
              desc="Step validate: 7 checks as assertions, plus lineage documents -> jobs -> feature view"),
        Arrow("C", 1, "commits", [b["github"].right(64 / BH), (c8 - 35, q(1) + 64), (c8 - 35, q(2) + 28),
                                  b["jenkins"].left(28 / BH)], 0.5, "l",
              desc="Jenkins polls GitHub main for its 5 pipelines (MCP servers, data agents, coordinator, RAG, stream jobs)"),
        Arrow("C", 2, "prompts", [b["langfuse"].bottom(0.3), b["jenkins"].top(0.3)], side="l",
              desc="Agent pipelines fetch the prompt labelled production for each agent"),
        Arrow("C", 3, "image", [b["jenkins"].bottom(14 / BW), (c8 + 14, q(3) - 36), (c7 + 134, q(3) - 36),
                                b["registry"].top(134 / BW)], 0.5,
              desc="MCP pipeline: tests (coverage gate 90 %), mutation test, image build, push, cosign signature"),
        Arrow("C", 4, "signed image", [b["registry"].top(34 / BW), (c7 + 34, q(2) - 36), (k5 + 85, q(2) - 36),
                                       (k5 + 85, my)], 0.24,
              desc="helm upgrade --atomic of each MCP server; the node pulls the signed image (Kyverno verifies it whenever "
                   "it is installed)"),
        Arrow("C", 5, "agents + prompts", [b["jenkins"].right(68 / BH), (c8 + BW + 66, q(2) + 68),
                                           (c8 + BW + 66, lane_c5), (k3 + 85, lane_c5), (k3 + 85, fy)], 0.62,
              desc="Agent pipelines: helm upgrade --atomic of the agents, the Langfuse prompts in a ConfigMap"),
        Arrow("C", 6, "eval questions", [b["jenkins"].right(28 / BH), (c8 + BW + 46, q(2) + 28),
                                         (c8 + BW + 46, lane_c6), (k4 + 85, lane_c6), b["gw"].top()], 0.76, "d",
              desc="Eval gate: the eval dataset asked to risk-agent through the gateway; below 0.6 accuracy, "
                   "helm rollback"),
        Arrow("C", 7, "scores", [b["jenkins"].top(0.7), b["langfuse"].bottom(0.7)],
              desc="Answers and their scores stored as a run of the Langfuse experiment"),
        Arrow("C", 8, "release", [b["jenkins"].bottom(0.5), (c8 + 85, q(4) - 36), (c7 + 144, q(4) - 36),
                                  b["airflow"].top(144 / BW)], 0.25,
              desc="RAG pipeline: sets Variable rag_image to the new image, triggers rag_ingest (Airflow REST API)"),
        Arrow("O", 1, "metrics", [b["gw"].bottom(), b["prom"].top()],
              desc="Gateway metrics: requests per route and backend, LLM tokens, time to first token, PII guardrail hits"),
        Arrow("O", 2, "metrics", [(mx, b["prom"].y + 42), b["prom"].right()],
              desc="MCP servers' metrics (one ServiceMonitor each): calls, failures and latency per tool"),
        Arrow("O", 3, "time series", [b["prom"].bottom(), b["grafana"].top()],
              desc="Grafana dashboards: Web API, compute, LLM telemetry, agent telemetry, A/B tests"),
        Arrow("O", 4, "requests/s", [b["prom"].left(), b["keda"].right()],
              desc="KEDA's trigger: A2A requests per second for each agent"),
        Arrow("O", 5, "replicas", [b["keda"].top(), (k3 + 85, fy + fh)],
              desc="KEDA sets each agent's replicas between 2 and 4"),
        Arrow("O", 6, "spans", [(fx, b["risk"].y + 42), b["jaeger"].right()],
              desc="Agents send spans over OTLP; the gateway's spans join the same traces (one conversation: about 20)"),
    ]

    W, H = 2240, 1136
    kind_b = q(5) + BH + 26
    body = [t(28, 46, "CARD FRAUD PLATFORM · SYSTEM DIAGRAM · LAYER 3", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "System diagram, Layer 3: the LLM and agent platform's deployable units and the data they exchange. "
                      "Layers 1 and 2 have their own diagram.", 22, INK, "start", 400),
            panel(14, 116, 208, q(3) + BH + 16 - 116, "OUTSIDE", "none", LINE, True),
            panel(k1 - 26, 116, k5 + BW + 26 - (k1 - 26), kind_b - 116, "KIND CLUSTER · fraud", KIND_PANEL, "#b5c0d6",
                  note="tag = namespace"),
            panel(c6 - 24, 116, BW + 48, q(2) + BH + 16 - 116, "HOST · GTX 1070", "none", LINE, True),
            panel(c7 - 24, 116, c8 + BW + 24 - (c7 - 24), q(1) + BH + 16 - 116, "INTERNET", "none", LINE, True)]
    # compose: an L-shaped panel (Jenkins' row on the right, the full width below the host panel)
    cx0, cx1, cy0, cy1, cyb = c6 - 24, c8 + BW + 24, q(2) - 26, q(3) - 32, kind_b
    poly = [((cx0 + cx1) / 2, cyb), (cx0, cyb), (cx0, cy1), (c7 - 24, cy1), (c7 - 24, cy0), (cx1, cy0), (cx1, cyb),
            ((cx0 + cx1) / 2, cyb)]
    body.append(f'<path d="{path_d(poly, 12)} Z" fill="{PANEL}" stroke="{LINE}"/>')
    body.append(t(cx0 + 16, cy1 + 22, "DOCKER COMPOSE · fraud", 11, INK2, "start", 600, mono=True, spacing=0.08))
    for x0, y0, w0, h0 in ((fx, fy, fw, fh), (mx, my, mw, mh)):   # dotted frames: the agents, the MCP servers
        body.append(f'<rect x="{x0}" y="{y0}" width="{w0}" height="{h0}" rx="12" fill="none" stroke="#9aa1b1" '
                    f'stroke-dasharray="2 4"/>')
    body += [box_svg(x) for x in B.values()]
    body += [arrow_svg(a, PANEL) for a in A]
    body.append(legend(28, kind_b + 24, FLOWS3, [
        "Numbers give the order inside each flow: A1 to A16 follow one question from support staff to the tools and the model.",
        "White box = one pod or container. Tag top left = where it runs: namespace (ns), compose profile, host or internet.",
        "A2A = an agent asks another agent; MCP = an agent calls a tool server; LLM = a prompt to the model.",
        "Each reply comes back on the same call and is not drawn.",
        "Dotted frames: the three agents (A5-A8, C5, O5, O6 apply to each) and the three MCP servers",
        "(one image, one Helm release each; A9, C4, O2 apply to each). Dashed boxes: support services.",
        "NGINX also serves registry, grafana, logs, traces and the Web API (api.<domain>: card data",
        "and policy chunks by id, from card-data-mcp, over HTTPS).",
        "Redis and the Scoring API are Layer 2 units, drawn again here because the tool servers call them (A10, A13)."]))
    check("system-layer-3", B, A)
    crossings("system-layer-3", A)
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: system diagram, Layer 3",
                   "Deployable units of the LLM and agent platform and the data between them, numbered per flow."), A


# ---------------------------------------------------------------- deployment
CW, CH = 262, 44   # container chip


def chip(x, y, name, image, port="", ico="", oneshot=False, w: float = CW) -> str:
    """One container or pod: name, image, published port; dashed when it runs once and exits."""
    dash = ' stroke-dasharray="4 3"' if oneshot else ""
    s = [f'<rect x="{x}" y="{y}" width="{w}" height="{CH}" rx="6" fill="#ffffff" stroke="#9aa1b1"{dash}/>']
    if ico:
        s.append(icon(ico, x + 9, y + 13, 18))
    s.append(t(x + 36, y + 19, name, 12.5, INK, "start", 600))
    s.append(t(x + 36, y + 35, image, 10, INK2, "start", mono=True))
    if port:
        s.append(t(x + w - 9, y + 19, port, 10.5, INK2, "end", mono=True))
    return "\n".join(s)


def group(x, y, title, note, chips, w=CW + 28, cols=1) -> tuple[str, float]:
    """A compose profile or a namespace: titled panel around its container chips. Returns (svg, bottom y)."""
    rows = -(-len(chips) // cols)
    h = 34 + rows * (CH + 8) + 6
    cw = (w - 28 - (cols - 1) * 12) / cols
    s = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="9" fill="#f9fafb" stroke="{LINE}"/>',
         t(x + 14, y + 22, title, 11.5, INK, "start", 700, mono=True, spacing=0.06)]
    if note:
        s.append(t(x + 14 + text_w(title, 11.5, True, 0.06) + 10, y + 22, note, 10.5, MUTED, "start", mono=True))
    for i, ch in enumerate(chips):
        cx, cy = x + 14 + (i % cols) * (cw + 12), y + 34 + (i // cols) * (CH + 8)
        s.append(chip(cx, cy, *ch, w=cw))
    return "\n".join(s), y + h


def net(pts, label, lx, ly, anchor="start", halo=PANEL, flow="") -> str:
    """An arrow with a multi-line label at (lx, ly): neutral (network path or step order), or in a flow's colour."""
    d = path_d(pts)
    color, head = (ALL_FLOWS[flow][2], flow) if flow else (INK2, "net")
    return "\n".join([f'<path d="{d}" fill="none" stroke="{halo}" stroke-width="7"/>',
                      f'<path d="{d}" fill="none" stroke="{color}" stroke-width="1.8" marker-end="url(#ah-{head})"/>',
                      *[t(lx, ly + 14 * i, ln, 11, INK, anchor, halo=halo) for i, ln in enumerate(label.split("\n")) if ln]])


def badge(x, y, tag, flow) -> str:
    """Numbered step on an arrow, as on the system diagrams; "A2+A3" = two steps the overview draws as one arrow."""
    color = ALL_FLOWS[flow][2]
    w = max(22, text_w(tag, 9.5, False, 0) + 12)
    return "\n".join([f'<rect x="{x - w / 2:.1f}" y="{y - 11:.1f}" width="{w:.1f}" height="22" rx="11" fill="#ffffff" '
                      f'stroke="{color}" stroke-width="2"/>',
                      t(x, y + 3.6, tag, 9.5, INK, weight=700)])


def deployment():
    """Host -> Docker Desktop VM -> compose profiles above the kind cluster's namespaces; internet services right."""
    W, H = 2240, 1822
    body = [t(28, 46, "CARD FRAUD PLATFORM · DEPLOYMENT · LAYERS 1 TO 3", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "Deployment: one Windows host (Docker Desktop VM: compose profiles + a kind cluster), Azure for "
                      "the IaC runs, SaaS on the internet", 22, INK, "start", 400)]
    # host tools and model servers
    body.append(panel(14, 96, 1876, 128, "HOST · Windows 11 · GTX 1070 · repo .venv + CLIs", "none", LINE, True))
    hosts = {"py": Box(250, 124, 292, 84, "Python scripts", "host", "python", note="data generator, load tests"),
             "docker": Box(560, 124, 210, 84, "docker CLI", "host", "docker", note="builds and pushes images"),
             "iac": Box(788, 124, 230, 84, "terraform · ansible", "host", "terraform", note="creates the Azure test setup"),
             "chat": Box(1036, 124, 250, 84, "llama.cpp chat", "host", "llamacpp", note="chat model on the GPU"),
             "embed": Box(1304, 124, 228, 84, "llama.cpp embed", "host", "llamacpp", note="turns text into vectors"),
             "browser": Box(1550, 124, 150, 84, "Browser", "host", "person", note="opens every UI"),
             "kind": Box(1718, 124, 158, 84, "kind · helm", "host", "kubernetes", note="creates the cluster, installs charts")}
    body += [box_svg(b) for b in hosts.values()]

    # internet services (right column, outside the VM)
    ix, iw = 1918, 294
    body.append(panel(ix - 14, 96, iw + 28, 498, "INTERNET · SaaS", "none", LINE, True))
    inet = [("GitHub", "github", "the code; Jenkins polls branch main"),
            ("Langfuse Cloud EU", "langfuse", "agent prompts, eval datasets and results"),
            ("Groq", "", "second LLM for an A/B test (off now)"),
            ("Let's Encrypt + DuckDNS", "letsencrypt", "the HTTPS certificate for NGINX")]
    for i, (name, logo, note) in enumerate(inet):
        body.append(box_svg(Box(ix, 136 + i * 112, iw, 92, name, "internet", logo, note=note)))

    # Azure: what Terraform, Ansible and kubectl create for an IaC run (docs/operations/iac.md), destroyed after it
    ay, gw = 614, iw + 8
    az = [t(ix, ay + 50 + 16 * i, ln, 11, INK2, "start", mono=True) for i, ln in enumerate(
        ("rg-cardfraud-dev · Azure for Students", "created from the PC: terraform (Azure", "API), ansible (SSH), kubectl (AKS API).",
         "Destroyed after each IaC run."))]
    g, z1 = group(ix - 4, ay + 122, "vm", "B2s_v2 · Ubuntu 24.04", [
        ("predictor", "built on the VM by Ansible", ":8080", "python"),
        ("node_exporter", "host metrics", ":9100", "")], w=gw)
    az.append(g)
    g, z2 = group(ix - 4, z1 + 14, "aks", "Free tier · 1 node B2s_v2", [
        ("card-fraud", "2 pods from ACR, ClusterIP", ":80", "kubernetes")], w=gw)
    az.append(g)
    g, z3 = group(ix - 4, z2 + 14, "acr", "Basic", [
        ("crcardfrauddev", "fraud/predictor:<v>", "", "docker")], w=gw)
    az.append(g)
    g, z4 = group(ix - 4, z3 + 14, "network", "", [
        ("vnet + subnet", "the VM's subnet", "", ""),
        ("nsg", "22, 8080, 9100: admin IP only", "", ""),
        ("public IP + NIC", "the VM's address", "", "")], w=gw)
    az.append(g)
    body.append(panel(ix - 14, ay, iw + 28, z4 + 16 - ay, "AZURE · polandcentral", "#f4f7fc", "#b5c0d6", True))
    body += az

    vy = 282
    body.append(panel(14, vy, 1876, H - vy - 104, "DOCKER DESKTOP · WSL2 VM · 8 GB RAM + 4 GB swap (.wslconfig)", PANEL, LINE))
    cx0, cy0 = 34, vy + 40
    gx = [cx0 + 20 + i * (CW + 28 + 14) for i in range(5)]
    gy = cy0 + 40
    body, compose = [], body   # groups first, the compose panel goes under them below
    g, y1 = group(gx[0], gy, "core", "always on", [
        ("postgres", "postgres:16-alpine", ":5433", "postgres"),
        ("seaweedfs", "chrislusf/seaweedfs:4.48", ":8333 S3", "seaweedfs"),
        ("kafka", "apache/kafka:3.9.0 (KRaft)", ":9092", "kafka")])
    body.append(g)
    g, y2 = group(gx[0], y1 + 14, "stream", "Flink, run it alone", [
        ("flink-jobmanager", "fraud/flink:2.2.1", ":8088", "flink"),
        ("flink-taskmanager", "6 slots, 3.5 GB", "", "flink")])
    body.append(g)
    g, _ = group(gx[0], y2 + 14, "dbt", "by hand", [("spark-thrift", "fraud/spark:4.1.3", ":10000", "spark")])
    body.append(g)
    g, _ = group(gx[1], gy, "batch", "Spark, SQL, lakeFS", [
        ("spark-master", "fraud/spark:4.1.3", ":8080", "spark"),
        ("spark-worker", "fraud/spark:4.1.3", ":8081", "spark"),
        ("spark-history", "fraud/spark:4.1.3", ":18080", "spark"),
        ("hive-metastore", "fraud/hive-metastore:3.1.3", "", "hive"),
        ("metastore-db", "postgres:16-alpine", "", "postgres"),
        ("trino", "trinodb/trino:484, 2 GB", ":8090", "trino"),
        ("lakefs", "treeverse/lakefs:1.88.0", ":8001", "lakefs")])
    body.append(g)
    g, y3 = group(gx[2], gy, "orch", "Airflow", [
        ("airflow-webserver", "fraud/airflow:2.10.5", ":8082", "airflow"),
        ("airflow-scheduler", "LocalExecutor, Feast venv", "", "airflow"),
        ("docker-proxy", "Docker API, RAG step images", "", "docker"),
        ("airflow-db", "postgres:16-alpine", "", "postgres"),
        ("airflow-init", "one-shot: db + users", "", "airflow", True)])
    body.append(g)
    g, _ = group(gx[2], y3 + 14, "gov", "DataHub", [
        ("datahub-frontend", "datahub-frontend-react:v1.5.0.6", ":9002", "datahub"),
        ("datahub-gms", "acryldata/datahub-gms:v1.5.0.6", ":8083", "datahub"),
        ("opensearch", "opensearch:2.19.3", "", "opensearch"),
        ("datahub-db", "postgres:16-alpine", "", "postgres"),
        ("datahub-system-update", "one-shot: schema upgrade", "", "datahub", True)])
    body.append(g)
    g, ml_b = group(gx[3], gy, "ml", "Layers 2-3", [
        ("ml-db", "postgres:16: mlflow, feast, registry", ":5434", "postgres"),
        ("vector-db", "pgvector 0.8.1: RAG chunks", ":5435", "postgres"),
        ("redis", "redis:7.4-alpine", ":6379", "redis"),
        ("mlflow", "fraud/mlflow:3.16.1", ":5000", "mlflow"),
        ("notebook", "fraud/ml-train (Jupyter)", ":8899", "jupyter"),
        ("push-online", "ml-train image: Kafka -> Redis", "", "python"),
        ("push-offline", "ml-train image: Kafka -> Delta", "", "python"),
        ("drift-forwarder", "ml-train image: -> Drift API", "", "python"),
        ("drift-api", "fraud/drift-api", ":8002", "fastapi"),
        ("scoring-api", "fraud/scoring-api", ":8000", "fastapi"),
        ("registry", "registry:3", ":5001", "docker")])
    body.append(g)
    g, _ = group(gx[4], gy, "CI/CD", "profile cicd · Layer 3", [
        ("jenkins", "fraud/jenkins:2.580.1", ":8095", "jenkins")])
    body.append(g)
    comp_r = gx[4] + CW + 28 + 20
    compose.append(panel(cx0, cy0, comp_r - cx0, ml_b + 20 - cy0, "COMPOSE PROJECT fraud · network fraud_default",
                         "#ffffff", LINE, note="ports published on 127.0.0.1 only"))
    body = compose + body

    # kind, below compose
    ky = ml_b + 20 + 84
    kgw = 280
    kgx = [cx0 + 20 + i * (kgw + 12) for i in range(6)]
    kg = ky + 40
    parts = []
    g, k1 = group(kgx[0], kg, "kubeflow", "KFP 2.17.2", [
        ("ml-pipeline", "KFP API", ":31888", "kubernetes"),
        ("ml-pipeline-ui", "KFP UI", ":31880", "kubernetes"),
        ("workflow-controller", "Argo: runs the DAG", "", "kubernetes"),
        ("mysql + seaweedfs", "run metadata, artifacts", "", "kubernetes"),
        ("metadata-grpc", "ML Metadata + envoy", "", "kubernetes"),
        ("cache, agents", "persistence, schedules", "", "kubernetes"),
        ("step pods", "fraud/ml-train, 1 per step", "", "python"),
        ("Spark executors", "2 pods: XGBoost workers", "", "spark")], w=2 * kgw + 12, cols=2)
    parts.append(g)
    g, k2 = group(kgx[0], k1 + 14, "serving", "", [
        ("card-fraud-predictor", "fraud/predictor:<v>", ":31080", "kubernetes")], w=kgw)
    parts.append(g)
    g, _ = group(kgx[1], k1 + 14, "kserve", "0.21", [
        ("kserve-controller", "InferenceService", "", "kubernetes")], w=kgw)
    parts.append(g)
    g, _ = group(kgx[0], k2 + 14, "cert-manager", "v1.21.1", [
        ("cert-manager", "webhook certs", "", "kubernetes")], w=kgw)
    parts.append(g)
    g, a1 = group(kgx[2], kg, "agentgateway-system", "", [
        ("agentgateway", "control plane 1.6.0", "", "kubernetes"),
        ("agentgateway-proxy", "LLM, MCP, A2A routes", ":31508", "kubernetes"),
        ("qwen3-8b-epp", "llm-d endpoint picker", "", "kubernetes"),
        ("qwen3-8b", "bridge -> host :8085", "", "kubernetes")], w=kgw)
    parts.append(g)
    g, a2 = group(kgx[2], a1 + 14, "card-support", "", [
        ("card-data-mcp", "2 pods, signed image", "", "fastapi"),
        ("fraud-score-mcp", "2 pods, signed image", "", "fastapi"),
        ("policy-rag-mcp", "2 pods, signed image", "", "fastapi")], w=kgw)
    parts.append(g)
    g, _ = group(kgx[2], a2 + 14, "nginx-ingress", "", [
        ("nginx-ingress", "F5 NIC 5.6.3", ":80 :443", "kubernetes")], w=kgw)
    parts.append(g)
    g, a3 = group(kgx[3], kg, "kagent", "0.10.3", [
        ("kagent-controller", "Agent CRDs -> pods", "", "kubernetes"),
        ("kagent-ui", "chat with the agents", "", "kubernetes"),
        ("coordinator", "2-4 pods (KEDA)", "", "kubernetes"),
        ("card-data-agent", "2-4 pods (KEDA)", "", "kubernetes"),
        ("risk-agent (+ -b)", "2-4 pods, A/B side b", "", "kubernetes"),
        ("worker pool", "Substrate gVisor worker", "", "kubernetes"),
        ("kagent-postgresql", "sessions, agent state", "", "postgres")], w=kgw)
    parts.append(g)
    g, _ = group(kgx[3], a3 + 14, "agentregistry", "0.4.0", [
        ("agentregistry", "catalog UI", ":12121", "kubernetes")], w=kgw)
    parts.append(g)
    g, m1 = group(kgx[4], kg, "monitoring", "", [
        ("prometheus", "kube-prometheus-stack", "", "kubernetes"),
        ("grafana", "5 dashboards", "", "kubernetes"),
        ("node-exporter, ksm", "CPU, RAM, disk, net", "", "kubernetes"),
        ("victoria-logs", "logs, 1 pod", "", "kubernetes"),
        ("vector", "DaemonSet: pod logs", "", "kubernetes"),
        ("jaeger", "traces (OTLP)", "", "kubernetes")], w=kgw)
    parts.append(g)
    g, _ = group(kgx[4], m1 + 14, "keda", "2.21.0", [
        ("keda-operator", "agent ScaledObjects", "", "kubernetes")], w=kgw)
    parts.append(g)
    g, s1 = group(kgx[5], kg, "ate-system", "Substrate 0.0.9", [
        ("ate-api + controller", "sandbox lifecycle", "", "kubernetes"),
        ("atelet", "DaemonSet: gVisor", "", "kubernetes"),
        ("valkey-cluster", "6 pods: state", "", "kubernetes"),
        ("rustfs", "warm snapshots", "", "kubernetes")], w=kgw)
    parts.append(g)
    g, s2 = group(kgx[5], s1 + 14, "vault", "dev mode", [
        ("vault", "secret/card-fraud/*", "", "kubernetes")], w=kgw)
    parts.append(g)
    g, kb = group(kgx[5], s2 + 14, "external-secrets", "", [
        ("external-secrets", "Vault -> Secrets", "", "kubernetes")], w=kgw)
    parts.append(g)
    kbot = max(kb, a3 + 14 + 92) + 20
    kx1 = kgx[5] + kgw + 20
    body.append(panel(cx0, ky, kx1 - cx0, kbot - ky, "KIND NODE fraud-control-plane", KIND_PANEL, "#b5c0d6",
                      note="kindest/node v1.37.0 · tag = namespace"))
    body += parts

    # network paths: host -> VM, compose <-> kind; the corridor right of compose carries the host <-> kind paths
    cb = ml_b + 20           # compose panel bottom
    chat, embed, browser, kindb = hosts["chat"], hosts["embed"], hosts["browser"], hosts["kind"]
    body += [
        net([hosts["py"].bottom(270 / 292), (hosts["py"].x + 270, cy0)], "127.0.0.1:<port>: Kafka, Postgres,\n"
            "S3, APIs, UIs", hosts["py"].x + 282, vy - 34),   # right of the VM panel's title
        net([hosts["docker"].bottom(190 / 210), (hosts["docker"].x + 190, cy0)], "push :5001", hosts["docker"].x + 202,
            vy - 34),
        # corridor between compose (right edge ~1579) and the VM edge (~1890): every path lands on the kind panel
        net([(1600, ky), (1600, 262), (chat.x + 125, 262), chat.bottom()], "", 0, 0),
        net([(1622, ky), (1622, 248), (embed.x + 114, 248), embed.bottom()],
            "pods ->\nhost.docker.internal\n:8085 chat\n:8086 embed", 1634, 700),
        net([browser.bottom(0.75), (browser.x + 112.5, 236), (1770, 236), (1770, ky)],
            "HTTPS :443 to NGINX\nagents.<domain>\nregistry.<domain>\ngrafana.<domain>\nlogs.<domain>\n"
            "traces.<domain>\napi.<domain>", 1762, 330, "end"),
        net([kindb.bottom(82 / kindb.w), (kindb.x + 82, ky)], "kind API,\nhelm", 1810, 800),
        net([(200, ky), (200, cb)], "pods -> lakehouse-s3:8333, kafka, ml-db, redis, mlflow, vector-db, scoring-api "
            "(node on fraud_default)", 212, cb + 36),
        net([(1020, cb), (1020, ky)], "scoring-api -> :31080 KServe · Airflow -> :31888 KFP\n"
            "Jenkins -> kind API (helm), :31508 (eval)", 1032, cb + 30),
        net([(1400, ky), (1400, cb)], "kind node pulls images:\nregistry:5000\n(host: localhost:5001)", 1412, cb + 24),
        # host -> Azure: along the top of the host panel, down the gap right of the VM panel
        net([hosts["iac"].top(), (hosts["iac"].x + 115, 106), (1897, 106), (1897, ay + 18), (ix - 6, ay + 18)],
            "terraform,\nansible,\nkubectl\n-> Azure", 1889, ay - 92, "end"),
    ]
    notes = ["Solid card = a container or pod that keeps running; dashed card = a one-shot job that exits; "
             ":port = published on 127.0.0.1 (compose) or a NodePort (kind); arrow = a network path, from the caller to the "
             "address it calls.",
             "8 GB does not hold every profile: run stream alone; keep trino and dbt off while orch + gov run; "
             "Layer 3 runs with core + ml + CI/CD (profile cicd) and the kind cluster.",
             "seaweedfs has the alias lakehouse-s3: inside kind, KFP's own seaweedfs Service shadows the short name.",
             "The model servers run on the host GPU (GTX 1070, Pascal: no vLLM); llm-d's InferencePool selects thin "
             "bridge pods that forward to them.",
             "Azure (docs/operations/iac.md) exists only during an IaC run: created, checked, destroyed. Its quotas (6 vCPU and 3 "
             "public IPs per region, 0 for GPUs) keep the rest of the platform on the PC."]
    body += [t(34, H - 96 + 18 * i, n, 12, INK2, "start") for i, n in enumerate(notes)]
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: deployment",
                   "Host tools and model servers, the Docker Desktop VM with the compose profiles above the kind "
                   "cluster and its namespaces, the Azure resources of the IaC runs, and the internet services the "
                   "platform uses.")


# ---------------------------------------------------------------- overview
NOVEL = "#bf4520"   # one colour for the four novel ideas


def overview():
    """The front page: every pipeline as a frame with its tasks in run order, the stores between them, the services
    and who uses them. Pipelines hand data to each other only through stores, never pipeline to pipeline."""
    W, CWD = 2166, 286
    x = [40 + i * 352 for i in range(6)]
    body, boxes = [], []

    def card(i, y, tag, title, note, ico="", accent="", w=CWD, h=84, store=False, chips=()):
        """A service, job or store in column i."""
        b = Box(x[i], y, w, h, title, tag, ico, accent=accent, store=store, note=note, chips=chips)
        boxes.append(b)
        return b

    def pipeline(x0, y0, cols, name, when, steps, h, accent=""):
        """Frame of one pipeline over `cols` columns, its tasks in run order inside (first column)."""
        body.append(pframe(x0 - 12, y0, CWD * cols + 66 * (cols - 1) + 24, h, name, when, accent))
        svg, rows = chain(x0, y0 + 46, CWD, steps)
        body.append(svg)
        return rows

    def stack(x0, y0, names):
        """Jobs that run side by side (no order between them), one chip each."""
        for i, n in enumerate(names):
            nm, ico = (n, "") if isinstance(n, str) else n
            body.append(stage(x0, y0 + i * (SH + 8), CWD, nm, ico))

    arrows, badges = [], []

    def arrow(pts, label="", lx=0.0, ly=0.0, anchor="start", halo=PANEL, flow="B", tag="", at=0.5):
        """An arrow in its flow's colour; `tag` numbers a step of one payment (S) or one question (A), the same
        number as on the system diagrams."""
        arrows.append(net(pts, label, lx, ly, anchor, halo, flow))
        if tag:
            badges.append(badge(*point_at(pts, at)[0], tag, flow))

    # ---------------------------------------------------------------- Layer 1: batch pipelines, zones, streaming
    fy0, FH = 138, 214                  # pipeline frames of the batch row
    pipeline(x[1], fy0, 1, "DP1 · DATA PIPELINE", "daily: waits for the export file",
             ["Wait for the export file", ("Copy sources into bronze", "spark"), ("Check the data", "check"),
              ("Publish (merge the branch)", "lakefs")], FH)
    pipeline(x[2], fy0, 2, "DP2 · DATA PIPELINE", "starts when DP1 succeeds",
             [("Clean into silver", "spark"), ("Check silver", "check")], FH)
    body.append(chain(x[3], fy0 + 46, CWD, [[("Build gold", "dbt"), ("Build labels", "spark")],
                                            ("Check gold", "check")])[0])
    ya, yb, xm = fy0 + 46 + SH + SG + SH / 2, fy0 + 46 + SH / 2, x[2] + CWD + 33   # check silver -> build gold
    body.append(f'<path d="{path_d([(x[2] + CWD, ya), (xm, ya), (xm, yb), (x[3], yb)])}" fill="none" stroke="{INK2}" '
                f'stroke-width="1.5" marker-end="url(#ah-net)"/>')
    pipeline(x[4], fy0, 1, "DP3 · FEATURE PIPELINE", "starts when DP2 succeeds",
             [("Compute card features", "spark"), ("Check features", "check")], FH)
    card(0, fy0 + 65, "SOURCE", "Core banking", "the bank's customers, cards and payments", "postgres")
    card(5, fy0 + 65, "SQL ON DEMAND", "Trino", "SQL on every zone, for analysts", "trino")
    arrow([(x[0] + CWD, fy0 + 107), (x[1] - 12, fy0 + 107)])
    body += [icon("person", x[5] + 20, fy0 - 18, 28, INK2), t(x[5] + 56, fy0 + 2, "Data analyst", 13, INK, "start", 600)]
    arrow([(x[5] + 34, fy0 + 14), (x[5] + 34, fy0 + 65)], "SQL queries", x[5] + 46, fy0 + 46)

    zy, ZH = fy0 + FH + 40, 86           # lakehouse zones
    zb = zy + ZH
    body += [f'<rect x="30" y="{zy - 14}" width="{W - 60}" height="{ZH + 28}" rx="10" fill="#f6f8fc" '
             f'stroke="#b5c0d6" stroke-dasharray="6 4"/>',
             t(44, zy + 22, "LAKEHOUSE", 11, INK2, "start", 700, mono=True, spacing=0.08),
             t(44, zy + 40, "S3 + Delta tables", 11.5, INK2, "start")]
    for i, (tag, name, note, acc) in enumerate([
            ("S3 lakehouse/landing/", "landing", "raw payment files from Flink", ""),
            ("NOVEL L1 #2 · lakeFS", "bronze", "exact copy of the sources;\nexport checked on a branch first", NOVEL),
            ("S3 lakehouse/silver/", "silver", "cleaned, one row per payment", ""),
            ("S3 lakehouse/gold/", "gold", "star schema and fraud labels", ""),
            ("S3 lakehouse/gold/feat_*", "features", "daily features per card", "")]):
        boxes.append(Box(x[i] + 185, zy, 268, ZH, name, tag, "", accent=acc, store=True, note=note))
    boxes.append(Box(1921, zy, 203, ZH, "Hive Metastore", "CATALOG", "hive", store=True, note="where each table lives"))
    fb = fy0 + FH
    for i in range(1, 5):                # pipeline i writes the zone right of it, the next pipeline reads it
        arrow([(x[i] + 236, fb), (x[i] + 236, zy)], "writes" if i == 1 else "", x[i] + 248, fb + 24)
    for i in range(1, 4):
        arrow([(x[i] + 402, zy), (x[i] + 402, fb)], "reads" if i == 1 else "", x[i] + 414, fb + 24)
    arrow([(x[1] + 80, zy), (x[1] + 80, fb)])                               # DP1 reads the landing files
    arrow([(1911, zy - 14), (1911, fy0 + 149)], "every zone", 1923, fb + 24)   # lakehouse -> Trino (B5)
    arrow([(2010, zy), (2010, fy0 + 149)], "table locations", 2022, fb + 24)  # catalog -> Trino

    rC = zb + 54                         # streaming row
    card(0, rC + 30, "EVENT STREAM", "Kafka", "payments and app events in\nfrom the card network and the app;\n"
         "features, alerts and decisions out", "kafka", w=250, h=100, store=True)
    body.append(pframe(x[1] - 12, rC, 2 * CWD + 66 + 24, 160, "STREAMING JOBS · FLINK",
                       "always on, seconds after each payment or app event"))
    stack(x[1], rC + 46, [("Card features per payment", "flink"), ("First device of each card", "flink"),
                          ("Raw payments to files", "flink")])
    card(2, rC + 40, "NOVEL L1 #1 · FLINK SQL", "Account takeover alerts",   # the fourth Flink job
         "password reset, new payee, transfer\nwithin 10 minutes (+ 2 more patterns),\nfound seconds after the app events",
         "flink", NOVEL, h=100)
    body.append(pframe(x[3] - 12, rC, CWD + 24, 124, "PUSH JOBS", "always on, deployed by Jenkins"))
    stack(x[3], rC + 46, [("push-online: into Redis", "python"), ("push-offline: into the lakehouse", "python")])
    pipeline(x[4], rC, 1, "INCREMENTAL MATERIALIZE PIPELINE", "starts when DP3 succeeds",
             [("Copy newest rows to Redis", "feast"), ("Check the online store", "check")], 130)
    arrow([(x[0] + 250, rC + 60), (x[1] - 12, rC + 60)], "payments,\napp events", x[0] + 295, rC + 38, "middle", flow="R")
    arrow([(x[1] - 12, rC + 100), (x[0] + 250, rC + 100)], "features,\nalerts", x[0] + 295, rC + 118, "middle", flow="R")
    arrow([(x[1] + 30, rC), (x[1] + 30, zb)], "raw files", x[1] + 42, rC - 22, flow="R")
    arrow([(x[0] + 80, rC + 130), (x[0] + 80, rC + 176), (x[3] + 20, rC + 176), (x[3] + 20, rC + 124)],
          "live features", x[2] + 143, rC + 192, "middle", flow="R")
    arrow([(x[4] + 236, zb), (x[4] + 236, rC)], "daily features", x[4] + 248, rC - 22)

    # ---------------------------------------------------------------- Feast: the hand-over between Layer 1 and 2
    ffy = rC + 160 + 56
    body.append(pframe(x[0] - 12, ffy, x[5] + CWD + 12 - (x[0] - 12), 132, "FEAST FEATURE STORE",
                       "one set of feature definitions, read by training and by scoring"))
    offline = Box(x[0], ffy + 46, x[2] + CWD - x[0], 72, "Offline store", "DELTA feat_* TABLES", "deltalake",
                  store=True, note="each card's features as they were at any past moment, for training")
    online = Box(x[3] + 120, ffy + 46, x[5] + CWD - x[3] - 120, 72, "Online store", "REDIS", "redis", store=True,
                 note="the latest features of each card, read in milliseconds, for scoring")
    boxes += [offline, online]
    fsb = ffy + 46 + 72
    arrow([(x[3] + 100, rC + 124), (x[3] + 100, ffy + 82), (x[2] + CWD, ffy + 82)], flow="R")   # push-offline
    arrow([(x[3] + 200, rC + 124), (x[3] + 200, ffy + 46)], flow="R")                         # push-online
    arrow([(x[4] + 143, rC + 130), (x[4] + 143, ffy + 46)], "newest row per card", x[4] + 155, ffy + 20)

    # ---------------------------------------------------------------- Layer 2: train, register, serve, monitor
    l2 = fsb + 44
    r2 = l2 + 46
    drift = pipeline(x[0], r2, 1, "DATA DRIFT PIPELINE", "daily",
                     [("Measure drift (last 7 days)", "check"), "PSI > 0.25 or PR-AUC down 0.05?",
                      ["Start training", "Do nothing"]], 172)
    pipeline(x[1], r2, 1, "TRAINING PIPELINE · KUBEFLOW", "on drift, or started by hand",
             [TRAIN_STEPS[0], TRAIN_STEPS[1], ("Check the data", "check"), TRAIN_STEPS[3], TRAIN_STEPS[4],
              ("Evaluate: better than production?", ""), ("Register as challenger, if better", "mlflow")], 340)
    card(2, r2, "JUPYTER", "Notebook", "explore the data, train the first model", "jupyter")
    body += [icon("person", x[3] + 130, r2 + 28, 28, INK2), t(x[3] + 166, r2 + 48, "Data scientist", 13, INK, "start", 600)]
    arrow([(x[3] + 122, r2 + 42), (x[2] + CWD, r2 + 42)], "explores, trains", x[3] + 33, r2 + 34, "middle", flow="T")
    sets = Box(x[2] + 100, r2 + 96, CWD - 100, 72, "Training sets", "S3 lakehouse/ml/", store=True,
               note="the rows of each run")
    boxes.append(sets)
    arrow([(x[2] + CWD, r2 + 67), (x[2] + 310, r2 + 67), (x[2] + 310, r2 + 290), (x[2] + CWD, r2 + 290)],   # T2
          "first model", x[2] + 322, r2 + 170, flow="T")
    sr, SH2 = r2 + 270, 100              # serving row and its card height
    card(2, sr, "MODEL REGISTRY", "MLflow",
         "each run: metrics and data version;\nchallenger = new candidate,\nproduction = the model served", "mlflow", h=SH2)
    card(3, sr, "MODEL SERVER", "KServe", "runs the production XGBoost model,\nreturns the fraud probability", "kserve",
         h=SH2)
    card(4, sr, "WEB API", "Fraud Scoring API",
         "low score: approve; middle: step up\n(OTP or biometric); high: decline;\nmodel down: a rule decides by amount",
         "fastapi", h=SH2)
    card(5, sr, "OUTSIDE", "Card network", "sends each payment,\nwaits for the decision", "card", w=200, h=SH2)
    dl = sr + SH2 + 36                    # decision log row
    card(3, dl, "KAFKA TOPIC", "Decision log", "each decision with its features,\nread by the live monitor", "kafka",
         store=True)
    card(2, dl, "LIVE MONITOR", "Drift API", "PSI of each feature per minute,\nagainst the training data", "fastapi")
    sm = sr + 50
    arrow([(x[0] + 200, fsb), (x[0] + 200, r2)], "last 7 days + labels", x[0] + 212, l2 + 4, flow="M")
    arrow([(x[1] + 143, fsb), (x[1] + 143, r2)], "labels + features", x[1] + 155, l2 + 4, flow="T")
    arrow([(x[2] + 143, fsb), (x[2] + 143, r2)], "features", x[2] + 155, l2 + 4, flow="T")
    arrow([(x[4] + 143, fsb), (x[4] + 143, sr)], "online features", x[4] + 155, l2 + 4, flow="S", tag="S2", at=0.62)
    sy3 = drift[2][0][1] + SH / 2         # the "Start training" task
    arrow([(x[0] + CWD + 12, sy3), (x[1] - 12, sy3)], "on drift", x[0] + CWD + 33, sy3 - 8, "middle", flow="M")
    s2 = r2 + 46 + SH + SG + SH / 2       # task 2: build the training set
    arrow([(x[1] + CWD + 12, s2), (x[2] + 100, s2)], flow="T")
    s7 = r2 + 46 + 6 * (SH + SG) + SH / 2  # task 7: register
    arrow([(x[1] + CWD + 12, s7), (x[2], s7)], "model", x[1] + CWD + 41, s7 - 8, "middle", flow="T")
    arrow([(x[2] + CWD, sm), (x[3], sm)], "production", x[2] + CWD + 33, sm - 8, "middle", flow="T")
    arrow([(x[4], sm - 14), (x[3] + CWD, sm - 14)], "features", x[3] + CWD + 33, sm - 30, "middle", flow="S", tag="S3")
    arrow([(x[3] + CWD, sm + 14), (x[4], sm + 14)], "score", x[3] + CWD + 33, sm + 38, "middle", flow="S", tag="S4")
    arrow([(x[5], sm - 14), (x[4] + CWD, sm - 14)], "payment", x[4] + CWD + 33, sm - 30, "middle", flow="S", tag="S1")
    arrow([(x[4] + CWD, sm + 14), (x[5], sm + 14)], "decision", x[4] + CWD + 33, sm + 38, "middle", flow="S", tag="S5")
    arrow([(x[4] + 40, sr + SH2), (x[4] + 40, dl + 42), (x[3] + CWD, dl + 42)], "decisions", x[4] + 52, sr + SH2 + 20,
          flow="S", tag="S6", at=0.2)
    arrow([(x[3], dl + 42), (x[2] + CWD, dl + 42)], flow="M")
    l2b = dl + 84 + 24

    # ---------------------------------------------------------------- Layer 3: agents, gateway, models, RAG
    l3 = l2b + 30
    r3a = l3 + 54
    card(1, r3a + 38, "MODEL SERVER · HOST GPU", "llama.cpp", "Qwen3-8B chat model and\nQwen3 embedding model", "llamacpp")
    card(2, r3a + 38, "LLM ROUTER", "llm-d", "picks a model server per prompt", "llmd")
    card(3, r3a, "GATEWAY · POLICIES APPLIED HERE", "agentgateway", "every agent call passes here", "agentgateway",
         h=160, chips=("API key per agent", "rate + token limits", "PII masking"))
    body.append(pframe(x[4] - 12, r3a, CWD + 24, 160, "AGENT TOOLS · MCP SERVERS", "one tool server per kind of data"))
    stack(x[4], r3a + 46, [("card-data: features, policy text", "fastapi"), ("fraud-score: score a payment", "fastapi"),
                           ("policy-rag: search the policies", "fastapi")])
    card(5, r3a + 38, "PGVECTOR · FEAST", "Vector store", "the bank's policies in chunks,\none vector per chunk", "postgres",
         store=True)
    r3b = r3a + 160 + 64
    card(0, r3b, "PEOPLE", "Support staff", "ask about a card or a payment", "people")
    card(1, r3b, "HTTPS ENTRY", "NGINX Ingress", "login, rate limit", "nginx")
    card(2, r3b, "AGENT · KAGENT", "Coordinator", "splits the question, asks the two\nagents, writes one answer", "kagent")
    card(3, r3b, "AGENTS · KAGENT", "Card-data + risk agents",
         "card-data looks up the card;\nrisk checks fraud score and policy", "kagent")
    card(4, r3b, "SANDBOX", "Agent Substrate", "sandboxed copies of the agents: each\nchat in its own gVisor sandbox, so\n"
         "chats cannot see each other", "", h=100)
    pipeline(x[5], r3b - 30, 1, "RAG DATA PIPELINE", "after each new RAG image",
             ["Split documents into chunks", ("Embed each chunk", "llamacpp"), ("Write to Feast", "feast"),
              ("Check the chunks", "check")], FH)
    m3a, m3b = r3a + 80, r3b + 42
    for i, lab, tg in ((0, "question", "A1"), (1, "question", "A2+A3")):
        arrow([(x[i] + CWD, m3b), (x[i + 1], m3b)], lab, x[i] + CWD + 33, m3b - 16, "middle", flow="A", tag=tg)
    arrow([(x[3] + 90, r3a + 160), (x[3] + 90, r3b)], "sub-task", x[3] + 78, r3b - 12, "end", flow="A", tag="A5+A6")
    arrow([(x[3], m3a), (x[2] + CWD, m3a)], "prompt", x[2] + CWD + 33, m3a - 16, "middle", flow="A", tag="A14")
    arrow([(x[2], m3a), (x[1] + CWD, m3a)], "prompt", x[1] + CWD + 33, m3a - 16, "middle", flow="A", tag="A15")
    arrow([(x[3] + CWD, m3a), (x[4] - 12, m3a)], "tool\ncall", x[3] + CWD + 27, m3a - 30, "middle", flow="A", tag="A9")
    arrow([(x[5], m3a), (x[4] + CWD + 12, m3a)], "policy\ntext", x[4] + CWD + 39, m3a - 30, "middle", flow="A",
          tag="A11+A16")
    arrow([(x[3] + 143, r3b), (x[3] + 143, r3a + 160)], "every agent call", x[3] + 155, r3b - 14, "start", flow="A",
          tag="A7+A8")
    arrow([(x[2] + 230, r3b), (x[2] + 230, r3b - 28), (x[3] + 20, r3b - 28), (x[3] + 20, r3a + 160)],
          "sub-tasks + prompts", x[2] + 242, r3b - 34, "start", flow="A", tag="A4", at=0.12)
    arrow([(x[1] + 60, r3a + 38), (x[1] + 60, l3 + 38), (x[4] + 40, l3 + 38), (x[4] + 40, r3a)],   # as A12 in Layer 3
          "the question's vector, for the policy search", x[1] + 72, r3a + 30, "start", flow="A", tag="A12", at=0.5)
    arrow([(x[5] + 143, r3b - 30), (x[5] + 143, r3a + 122)], "chunks + vectors", x[5] + 155, r3a + 150, flow="K")
    arrow([(x[4] + 200, sr + SH2), (x[4] + 200, r3a)], "fraud score", x[4] + 212, dl + 80, flow="A", tag="A13",
          at=0.82)
    lane = x[5] + 250
    arrow([(lane, fsb), (lane, l3 + 22), (x[4] + 250, l3 + 22), (x[4] + 250, r3a)], "card features",
          lane - 12, l3 + 14, "end", flow="A", tag="A10", at=0.93)
    l3b = r3b - 30 + FH + 20

    # ---------------------------------------------------------------- Airflow: what starts each pipeline
    ay = l3b + 30
    full = x[5] + CWD - x[0]
    body.append(f'<rect x="{x[0]}" y="{ay}" width="{full}" height="104" rx="8" fill="#ffffff" stroke="#9aa1b1"/>')
    body.append(icon("airflow", x[0] + 18, ay + 22, 30))
    body.append(t(x[0] + 58, ay + 44, "Airflow starts the pipelines", 14.5, INK, "start", 600))
    body.append(t(x[0] + 58, ay + 62, "and keeps their run history", 11.5, INK2, "start"))
    order = [("DP1 · daily", ""), ("DP2", ""), ("DP3", ""), ("Materialize", "")]
    body.append(chain(x[1], ay + 18, 150, order, vertical=False)[0])
    body.append(t(x[1], ay + 70, "each one starts when the one before succeeds, so none reads a zone that is still "
                                 "being written", 11.5, INK2, "start"))
    for i, nm in enumerate(("Data drift pipeline · daily", "DataHub metadata · daily",
                            "RAG data pipeline · after a new image")):
        body.append(stage(x[4] + 40, ay + 12 + i * 30, 2 * CWD + 66 - 56, nm))
    body.append(t(x[4] + 28, ay + 31, "on their own schedule:", 11.5, INK2, "end"))

    # ---------------------------------------------------------------- shared services, CI/CD, cloud
    sv = ay + 104 + 46
    for i, (title, note, ico) in enumerate([("SeaweedFS (S3)", "files and Delta tables", "seaweedfs"),
                                            ("PostgreSQL", "the platform's databases", "postgres"),
                                            ("DataHub", "data catalog and lineage", "datahub"),
                                            ("Prometheus + Grafana", "metrics and dashboards", "grafana"),
                                            ("VictoriaLogs + Jaeger", "logs and traces", "jaeger"),
                                            ("Vault", "keeps the secrets", "vault")]):
        card(i, sv, "", title, note, ico, h=72)
    cy = sv + 72 + 30
    cyc, H7 = cy + 40, 100               # the CI/CD row and its card height
    body.append(actor(x[0], cyc + 12, "Developer", w=110))   # the row starts with a person, so it needs its own grid
    cx7 = [x[0] + 166 + i * 322 for i in range(6)]
    for i, (tag, title, note, ico, acc) in enumerate([
            ("SAAS", "GitHub", "the code; Jenkins polls it", "github", ""),
            ("CI/CD", "Jenkins", "tests, builds, deploys", "jenkins", ""),
            ("REGISTRY", "Docker registry", "images and their signatures", "docker", ""),
            ("DEPLOY", "Deploy", "Helm, Airflow or compose", "helm", ""),
            ("NOVEL L3 #2", "Signed images only", "a tool server pod starts only if\nits image is signed by CI (cosign)",
             "kyverno", NOVEL),
            ("NOVEL L3 #1", "Eval gate", "risk-agent answers 20 test questions\nafter each data-agent release; below\n"
             "0.6 accuracy it is rolled back", "langfuse", NOVEL)]):
        boxes.append(Box(cx7[i], cyc, 266, H7, title, tag, ico, accent=acc, note=note))
    arrow([(x[0] + 112, cyc + 50), (cx7[0], cyc + 50)], "code", cx7[0] - 28, cyc + 42, "middle", flow="C")
    for i, lab in ((0, "commits"), (1, "push"), (2, "pull"), (3, "new pods"), (4, "admitted")):
        arrow([(cx7[i] + 266, cyc + 50), (cx7[i + 1], cyc + 50)], lab, cx7[i] + 294, cyc + 42, "middle", flow="C")
    zy2 = cyc + H7 + 30
    zc = zy2 + 40
    for i, (title, note, ico) in enumerate([("Groq", "second LLM for an A/B test (off)", ""),
                                            ("Langfuse Cloud EU", "prompts, eval datasets, results", "langfuse"),
                                            ("Let's Encrypt + DuckDNS", "the HTTPS certificate", "letsencrypt")]):
        card(i, zc, "SAAS", title, note, ico)
    ax, aw = x[3] - 12, x[5] + CWD - x[3] + 24
    cw2 = (aw - 28 - 12) / 2
    azure = [panel(ax, zy2 + 34, aw, 128, "AZURE · polandcentral", "#f4f7fc", "#b5c0d6", True,
                   note="built by Terraform and Ansible for the IaC test, then destroyed")]
    for i, ch in enumerate([("VNet + NSG", "network, admin IP only", "", ""),
                            ("VM B2s v2", "the fraud model server", "", "python"),
                            ("AKS, Free tier", "the same model: 2 pods", "", "kubernetes"),
                            ("ACR, Basic", "the model image", "", "docker")]):
        azure.append(chip(ax + 14 + (i % 2) * (cw2 + 12), zy2 + 66 + (i // 2) * (CH + 8), *ch, w=cw2))
    jx = cx7[1] + 54                     # over both Jenkins and Langfuse
    arrow([(jx - 40, zc), (jx - 40, cyc + H7)], "prompts", jx - 48, zy2 + 4, "end", PAGE, flow="C")
    arrow([(jx + 40, cyc + H7), (jx + 40, zc)], "eval scores", jx + 48, zy2 + 4, "start", PAGE, flow="C")
    fl = zy2 + 178 + 30                  # the key to the arrow colours
    body.append(legend(28, fl, {
        "S": ("Payment scoring", "each card payment, in milliseconds", FLOWS["S"][2]),
        "R": ("Streaming", "always on, seconds after each payment", FLOWS["R"][2]),
        "B": ("Batch data", "daily pipelines; K: the policy documents", FLOWS["B"][2]),
        "T": ("Training and release", "on drift or by an engineer; C: CI/CD", FLOWS["T"][2]),
        "M": ("Monitoring", "drift per minute and per day", FLOWS["M"][2]),
        "A": ("Agent question", "each question from support staff", FLOWS3["A"][2])}, [
        "The colour of an arrow says which path it belongs to and how often it runs.",
        "S1 to S6 follow one payment and A1 to A16 one question, in order; the same numbers are on the system diagrams "
        "and in docs/architecture.md, which gives each step in detail."]))
    H = fl + 74 + 34 + 34

    # ---------------------------------------------------------------- legend, top right
    keys, lx = [], W - 36
    for kind, label in (("novel", "novel idea"), ("arrow", "data or a request, coloured by path (key at the bottom)"), ("store", "store: data rests here"),
                        ("task", "task, in run order"), ("frame", "pipeline or job group"),
                        ("card", "service or job")):
        lx -= text_w(label, 11, False)
        keys.append(t(lx, 50, label, 11, INK2, "start"))
        lx -= 30
        if kind == "arrow":
            keys.append(net([(lx, 46), (lx + 22, 46)], "", 0, 0, halo=PAGE))
        elif kind == "frame":
            keys.append(f'<rect x="{lx}" y="38" width="22" height="16" rx="3" fill="none" stroke="{INK2}" '
                          f'stroke-dasharray="4 3"/>')
        elif kind == "task":
            keys.append(f'<rect x="{lx}" y="40" width="22" height="12" rx="3" fill="#ffffff" stroke="#9aa1b1"/>')
        else:
            st, fl = {"store": (STORE_LINE, STORE_FILL), "novel": (NOVEL, NOVEL + "14"), "card": ("#9aa1b1", "#ffffff")}[kind]
            keys.append(f'<rect x="{lx}" y="38" width="22" height="16" rx="3" fill="{fl}" stroke="{st}"/>')
        lx -= 26

    head = [t(28, 46, "CARD FRAUD PLATFORM · OVERVIEW · LAYERS 1 TO 3", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "From core banking and the card network to a fraud decision in real time, back to retraining, "
                      "and on to the agents that answer card support questions", 22, INK, "start", 400),
            *keys,
            panel(20, 100, W - 40, rC + 160 + 20 - 100, "LAYER 1 · DATA PLATFORM", PANEL, LINE,
                  note="pipelines pass data only through the lakehouse: one writes a zone, the next reads it from there"),
            panel(20, l2, W - 40, l2b - l2, "LAYER 2 · ML PLATFORM", KIND_PANEL, "#b5c0d6"),
            panel(20, l3, W - 40, l3b - l3, "LAYER 3 · LLM AND AGENT PLATFORM", PANEL, LINE),
            panel(20, sv - 34, W - 40, 72 + 48, "SHARED SERVICES", PANEL, LINE, note="used by every layer"),
            panel(20, cy, W - 40, H7 + 56, "CI/CD · JENKINS", PANEL, LINE,
                  note="a commit or a new production prompt is tested, built, signed, deployed and checked"),
            panel(20, zy2, W - 40, 178, "CLOUD", PANEL, LINE, note="SaaS + Azure"),
            t(W - 36, zy2 + 22, "everything else runs on one Windows PC: where each part runs is in the deployment picture",
              11, MUTED, "end", mono=True)]
    out = head + body + [box_svg(b) for b in boxes] + arrows + badges + azure
    return svg_doc(W, H, "\n".join(out), "Card fraud platform: overview",
                   "Layer 1's pipelines DP1, DP2 and DP3, each a frame with its tasks in run order, write a lakehouse "
                   "zone that the next one reads; streaming jobs add live features. Feast hands the features to Layer 2, "
                   "where the training pipeline trains and registers the model and the Scoring API decides each payment. "
                   "Layer 3's agents answer support staff through agentgateway with the same features, the fraud score "
                   "and the bank's policies. Airflow starts the pipelines in order; shared services, CI/CD and the "
                   "cloud are below.")


# ---------------------------------------------------------------- output
def render(name: str, svg: str, prefix: str = "platform-") -> None:
    """Write the SVG and, when Chrome or Edge is installed, a 2x PNG screenshot of it next to it."""
    path = OUT / f"{prefix}{name}.svg"
    path.write_text(svg, encoding="utf-8")
    chrome = next((p for p in (shutil.which("chrome"), shutil.which("google-chrome"), shutil.which("chromium"),
                               r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                               r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe")
                   if p and Path(p).exists()), None)
    if not chrome:
        print(f"{path.name} written (no Chrome found: PNG skipped)")
        return
    w, h = (int(v) for v in svg.split('viewBox="0 0 ')[1].split('"')[0].split())
    subprocess.run([chrome, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--window-size={w},{h}",
                    "--force-device-scale-factor=2", f"--screenshot={path.with_suffix('.png')}", path.as_uri()],
                   check=True, capture_output=True)
    print(f"{path.name} + .png written")


def table(arrows) -> str:
    """Markdown table of every numbered arrow (docs/architecture.md)."""
    rows = ["| # | Flow | Data / call |", "|---|---|---|"]
    rows += [f"| {a.tag} | {ALL_FLOWS[a.flow][0]} | {a.desc} |" for a in arrows]
    return "\n".join(rows)


if __name__ == "__main__":
    only = set(sys.argv[1:]) or {"system-layers-1-2", "system-layer-3", "deployment", "overview"}
    tables = []
    for name in ("system-layers-1-2", "system-layer-3", "deployment", "overview"):   # one Chrome at a time
        if name in only:
            out = globals()[name.replace("-", "_")]()
            svg, arrows = out if isinstance(out, tuple) else (out, None)
            render(name, svg)
            if arrows:
                tables.append(table(arrows))
    print("\n\n".join(tables))
