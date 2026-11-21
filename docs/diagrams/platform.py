"""Platform diagrams of the README (Layers 1-3), drawn from the tables in this file. Stdlib only.

  overview           the whole platform: every pipeline as a frame with its tasks in order, the stores between
                     them, the services and the people who use them
  system-layers-1-2  system diagram of Layers 1 + 2: every box is a deployable unit; arrows follow the data and
                     carry it as a label, numbered per flow with one colour per flow (rubric of the README diagram)
  system-layer-3     system diagram of Layer 3, same rules: agents, gateway, model servers, RAG ingest, CI/CD,
                     observability
  deployment         where every unit runs: host, Docker Desktop VM (compose profiles), kind cluster (namespaces),
                     internet

A box shows a name, a logo and at most a few short bullets: what a part does is told mostly by the arrows, the tasks
drawn inside a pipeline frame, and the tables under each picture. Logos: logos.json, each a data URI (Devicon, Simple Icons, the CNCF
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
    "T": ("Training and deploy", "on drift or by hand: pipeline run; Jenkins builds the model image", "#9c36b5"),
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
    """A numbered arrow of one flow: orthogonal path, badge with flow letter + number, short label (a line break in it
    starts a second line, where the gap between two boxes is narrower than the words)."""
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


PROFILES = {"core": "Core services", "stream": "Streaming", "batch": "Batch processing", "orch": "Orchestration",
            "gov": "Data governance"}   # compose profile -> the name the deployment diagram gives it
ML_STORES = {"Feast online store", "ml-db (PostgreSQL)", "Vector store"}   # profile ml is drawn as ML stores + ML apps


def tag_name(b: Box) -> str:
    """The tag as written on the card: a compose profile by its full name, a namespace or place as it is."""
    if b.tag == "ml":
        return "ML stores" if b.title in ML_STORES else "ML apps"
    return PROFILES.get(b.tag, b.tag)


def box_svg(b: Box) -> str:
    """SVG of one card."""
    tag = tag_name(b)
    stroke, fill = (LINE, "#fafafa") if b.soft else ("#9aa1b1", "#ffffff")
    dash = ' stroke-dasharray="4 3"' if b.soft else ""
    ink = b.accent or INK
    if b.store:
        stroke, fill = STORE_LINE, STORE_FILL
    if b.accent:
        stroke, fill = b.accent, b.accent + "14"   # 8% tint
    s = [f'<rect x="{b.x}" y="{b.y}" width="{b.w}" height="{b.h}" rx="8" fill="{fill}" stroke="{stroke}"{dash}/>']
    if tag:
        tw = text_w(tag, 9, True, 0.06) + 10   # the tag is drawn with 0.06em letter-spacing
        s.append(f'<rect x="{b.x + 8}" y="{b.y + 8}" width="{tw:.0f}" height="15" rx="3" fill="none" '
                 f'stroke="{b.accent or LINE}"/>')
        s.append(t(b.x + 13, b.y + 19, tag, 9, b.accent or INK2, "start", mono=True, spacing=0.06))
    rows = pill_rows(b.chips, b.w - 16)
    note = [ln for part in b.note.split("\n") for ln in wrap(part, b.w - 20, 11.5)]   # \n = a line break by meaning
    top = b.y + (22 if b.tag else 0)
    y0 = top + (b.y + b.h - top - 26 - 15 * len(note) - 26 * len(rows) - (10 if rows else 0)) / 2   # centred; pills keep 10 px
    if b.steps:
        y0 = top + 6
        s.append(chain(b.x + 14, y0 + 40, b.w - 28, b.steps, up=b.up)[0])
    ico = b.icon or ("db" if b.store else "")
    size = 14.5                                    # a long name shrinks until logo + name fit inside the card
    while text_w(b.title, size, False) * 1.04 + (34 if ico else 0) > b.w - 18 and size > 11.5:   # 600 weight: ~4% wider
        size -= 0.5
    tw = text_w(b.title, size, False) * 1.04
    lx = b.x + (b.w - tw - (34 if ico else 0)) / 2
    if ico:
        s.append(icon(ico, lx, y0 + 1, 26, ink if not b.soft else INK2))
    s.append(t(lx + (34 if ico else 0), y0 + 19, b.title, size, ink, "start", 600))
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


LH = 13   # line height of an arrow label


def label_box(a: Arrow):
    """Badge centre, label anchor point and the label's bounding box."""
    (bx, by), horizontal = point_at(a.pts, a.at)
    side = a.side or ("u" if horizontal else "r")
    lines = a.label.splitlines()
    w, more = max(text_w(ln, 11, False) for ln in lines), LH * (len(lines) - 1)
    tx, ty, anchor = {"r": (bx + 15, by + 4 - more / 2, "start"), "l": (bx - 15, by + 4 - more / 2, "end"),
                      "u": (bx, by - 15 - more, "middle"), "d": (bx, by + 23, "middle")}[side]   # ty = the first line
    x0 = {"start": tx, "end": tx - w, "middle": tx - w / 2}[anchor]
    return (bx, by), (tx, ty, anchor), (x0, ty - 10, x0 + w, ty + 3 + more)


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
        *[t(tx, ty + LH * i, ln, 11, INK, anchor, halo=halo) for i, ln in enumerate(a.label.splitlines())],
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
    for a in arrows:
        bx, by = label_box(a)[0]
        for o in arrows:
            for (x0, y0), (x1, y1) in zip(o.pts, o.pts[1:]):
                if o is not a and min(x0, x1) - 12 < bx < max(x0, x1) + 12 and min(y0, y1) - 12 < by < max(y0, y1) + 12:
                    print(f"  ! {name}: {o.tag} runs through the badge of {a.tag}")
    for a in arrows:
        x0, y0, x1, y1 = label_box(a)[2]
        for o in arrows:
            for (px, py), (qx, qy) in zip(o.pts, o.pts[1:]):
                if o is not a and min(px, qx) < x1 and max(px, qx) > x0 and min(py, qy) < y1 and max(py, qy) > y0:
                    print(f"  ! {name}: {o.tag} runs through the label of {a.tag}")
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
        f'markerUnits="userSpaceOnUse"><path d="M0,0 L9,4 L0,8 z" fill="{INK2}"/></marker>' \
        f'<marker id="ah-back" markerWidth="9" markerHeight="8" refX="1" refY="4" orient="auto" ' \
        f'markerUnits="userSpaceOnUse"><path d="M9,0 L0,4 L9,8 z" fill="{INK2}"/></marker>' + \
        "".join(f'<marker id="ah-back-{k}" markerWidth="9" markerHeight="8" refX="1" refY="4" orient="auto" '
                f'markerUnits="userSpaceOnUse"><path d="M9,0 L0,4 L9,8 z" fill="{v[2]}"/></marker>'
                for k, v in ALL_FLOWS.items())   # the answer end of a call


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
COLS = (296, 530, 764, 998, 1262, 1496)   # wider gap c4|c5: B2 and B8 share it
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
        "card": Box(28, r(1), 172, BH, "Authorization switch", "host", "card", note="the bank's, replayed\nby a script"),
        "app": Box(28, r(2), 172, BH, "Mobile banking app", "host", "phone", note="app events, replayed\nby a script"),
        "export": Box(28, r(5), 172, BH, "Nightly export job", "host", "bucket",
                      note="core banking's settled\npayments, by a script"),
        "api": Box(c(3), r(1), BW, BH, "Fraud Scoring API", "ml",
                   "fastapi", note="approves, steps up or declines each payment"),
        "redis": Box(c(3), r(2), BW, BH, "Feast online store", "ml", "redis", note="latest card features"),
        "pushon": Box(c(2), r(3), BW, BH, "push-online", "ml", "python",
                      note="always-on container:\nlive features to Redis"),   # the Python logo read as "a script"
        "drift": Box(c(4), r(3), BW, BH, "Drift API", "ml", "fastapi", note="PSI of each feature\nper minute"),
        "kafka": Box(c(1), r(4), c(3) + BW - c(1), BH + 26, "Kafka", "core", "kafka",
                     chips=("card_authorizations", "app_events", "card_features_rt", "card_device_seen",
                            "fraud_predictions", "ato_alerts_cep")),
        "pg": Box(c(4), r(4), BW, BH, "Core banking database", "core", "postgres", note="the bank's core banking data"),
        "mlflow": Box(c(5), r(4), BW, BH, "MLflow", "ml", "mlflow", note="model versions;\nchallenger, production"),
        "flink": Box(c(1), r(5), BW, BH, "Flink streaming jobs", "stream",
                     "flink", note="turns payments into live features"),
        "pushoff": Box(c(2), r(5), BW, BH, "push-offline", "ml",
                       "python", note="always-on container:\nlive features to Delta"),
        "trino": Box(c(3), r(5), BW, BH, "Trino", "batch", "trino", note="SQL queries for analysts"),
        "spark": Box(c(4), r(5), BW, BH, "Spark", "batch", "spark", note="runs the batch jobs"),
        "airflow": Box(c(5), r(5), BW, BH, "Airflow", "orch",
                       "airflow", note="schedules and runs the pipelines"),
        "jupyter": Box(c(6), r(5), BW, BH, "JupyterLab notebook", "ml", "jupyter", note="notebook to explore data and train"),
        "s3": Box(c(1), r(5) + BH + GY, c(6) + BW - c(1), S3H, "SeaweedFS (S3) + Delta Lake", "core", "seaweedfs",
                  note="four buckets; the lakehouse bucket holds one folder per zone, as Delta tables",
                  chips=("lakehouse/landing", "lakehouse/bronze", "lakehouse/silver", "lakehouse/gold + feat_*",
                         "lakehouse/ml", "core-banking-export", "lakefs-data", "mlflow")),
        "kserve": Box(KX, r(1), 300, BH, "KServe model server", "ns serving", "kserve",
                      note="serves the fraud model; its logger copies each call"),
        "broker": Box(KX, r(2), 220, BH, "Knative broker", "ns knative-eventing", "kubernetes",
                      note="routes the logger's events to the detectors"),
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
              desc="The bank's authorization switch, which receives each card payment from the card network, sends it to "
                   "POST /v1/score (card, amount, channel, device); here ml/tools/replay_scoring.py plays the switch"),
        Arrow("S", 2, "online features", [redis.top(0.35), api.bottom(0.35)],
              desc="Feast reads the card's stored features from Redis (feature service fraud_model_v1)"),
        Arrow("S", 3, "20 features", [api.right(0.3), B["kserve"].left(0.3)], 0.3,
              desc="Feature vector to the model server (KServe V1 protocol, NodePort 31080, through the pod's agent sidecar)"),
        Arrow("S", 4, "fraud score", [B["kserve"].left(0.7), api.right(0.7)], 0.7, "d",
              desc="Probability of fraud from the production XGBoost booster"),
        Arrow("S", 5, "approve / step_up / decline", [api.left(0.68), B["card"].right(0.68)], 0.5, "d",
              desc="Decision by the two thresholds; rule fallback when the model is late or down"),
        Arrow("S", 6, "decision + features", [api.bottom(0.12), (api.x + 20, 248), (c(1) + 85, 248), kafka.top(85 / kafka.w)],
              0.55, desc="Every decision is logged to Kafka topic fraud_predictions, the record of decisions (scoring never waits "
                         "on it)"),
        Arrow("R", 1, "card payments", [B["card"].right(0.9), (GAPX, B["card"].y + BH * 0.9), (GAPX, kafka.y + 30),
                                   kafka.left(30 / kafka.h)], 0.5, "l",
              desc="Every authorization (retries included) to Kafka topic card_authorizations (generator/producer.py replays "
                   "the generated stream in arrival order)"),
        Arrow("R", 2, "app events", [B["app"].bottom(), (B["app"].x + 86, kafka.y + 80), kafka.left(80 / kafka.h)],
              desc="The mobile banking app's events (login, password reset, new payee, transfer, biometric check) to Kafka "
                   "topic app_events, replayed the same way; about 15 % arrive after the next event of their session"),
        Arrow("R", 3, "payments,\napp events", [kafka.bottom(55 / kafka.w), B["flink"].top(55 / BW)], side="l",
              desc="Flink reads card_authorizations (watermarks, dedup of network retries); ato_cep reads the app events "
                   "(account takeover patterns -> ato_alerts_cep; so far on the replay topic app_events_replay)"),
        Arrow("R", 4, "live features,\ntakeover alerts", [B["flink"].top(115 / BW), kafka.bottom(115 / kafka.w)],
              desc="Realtime features to card_features_rt, first (card, device) pairs to card_device_seen; ato_cep writes one "
                   "alert per matched pattern to ato_alerts_cep"),
        Arrow("R", 5, "raw payments as JSON files", [B["flink"].bottom(), (B["flink"].x + 85, s3y)],
              desc="raw_landing writes the raw authorizations as JSON lines to the S3 landing zone; DP1 (ingest to bronze) loads them into "
                   "bronze.raw_card_authorizations (Delta)"),
        Arrow("R", 6, "card_features_rt + card_device_seen", [kafka.top((c(2) + 85 - c(1)) / kafka.w), B["pushon"].bottom()],
              desc="push-online reads card_features_rt and card_device_seen"),
        Arrow("R", 7, "live features\n(Feast push)", [B["pushon"].top(), (c(2) + 85, redis.y + 42), redis.left()], 0.75, "u",
              desc="Writes them to the Redis online store through Feast push (offsets committed after the write)"),
        Arrow("R", 8, "the same two topics", [kafka.bottom((c(2) + 85 - c(1)) / kafka.w), B["pushoff"].top()],
              desc="push-offline reads the same two topics"),
        Arrow("R", 9, "live features, appended to Delta", [B["pushoff"].bottom(), (c(2) + 85, s3y)],
              desc="Appends them to gold.feat_card_auth_rt / gold.feat_card_device: the offline copy for training"),
        Arrow("B", 1, "nightly export\n(Parquet files)", [B["export"].bottom(), (114, s3y + 50), (B["s3"].x, s3y + 50)], 0.75,
              "u", desc="Core banking's nightly export job writes the day's settled transactions as Parquet files into "
                        "s3://core-banking-export/transactions/export_date=<day>/ (here: generator/load_source.py --part "
                        "exports)"),
        Arrow("B", 2, "starts each job", [B["airflow"].left(0.6), B["spark"].right(0.6)], side="d",
              desc="Airflow submits the Spark jobs of DP1, the two DP2 pipelines and DP3 (a call, no data); each pipeline checks "
                   "what it wrote with Great Expectations, then announces the new version of its zone, which starts the next one"),
        Arrow("B", 3, "over JDBC:\ncustomers, accounts,\ncards, merchants,\ndisputes, provinces", [B["pg"].bottom(), B["spark"].top()], side="l",
              desc="DP1 copies the core banking tables over JDBC (customers, accounts, cards, merchants, disputes, province mapping)"),
        Arrow("B", 4, "nightly\nexport files", [(c(4) + 32, s3y), B["spark"].bottom(32 / BW)], side="l",
              desc="DP1 loads the new daily export folders from s3://core-banking-export"),
        Arrow("B", 5, "bronze, silver,\ngold, feat_*", [B["spark"].bottom(138 / BW), (c(4) + 138, s3y)],
              desc="DP1 writes bronze, the DP2 silver pipeline silver, the DP2 gold pipeline gold (dbt) and the labels, DP3 the "
                   "feat_* tables, all Delta in s3://lakehouse; "
                   "bronze.raw_transactions goes through lakeFS: a branch per DP1 run, merged into main after the checks"),
        Arrow("B", 6, "Delta tables\nof any zone", [(c(3) + 85, s3y), B["trino"].bottom()],
              desc="Trino reads the Delta tables of any zone for analysts; the Hive Metastore tells it where each table "
                   "lives (bronze.raw_transactions: catalog lakefs, branch main)"),
        Arrow("B", 7, "daily\nfeatures", [(c(5) + 40, s3y), B["airflow"].bottom(40 / BW)], side="r",
              desc="The incremental materialize pipeline (DAG feast_materialize, on DP3's new features) reads feat_card_txn_stats_daily through "
                   "the Feast offline store"),
        Arrow("B", 8, "newest row per card", [B["airflow"].left(0.22), (c(4) + BW + 14, B["airflow"].y + BH * 0.22),
                                       (c(4) + BW + 14, 266), (c(3) + 145, 266), redis.top(145 / BW)], 0.72, "u",
              desc="... and loads the newest row per card into Redis (feast materialize-incremental)"),
        Arrow("T", 1, "labels +\npoint-in-time features", [(c(6) + 85, s3y), B["jupyter"].bottom()],
              desc="The notebook builds the training set: labels + Feast point-in-time features"),
        Arrow("T", 2, "metrics + 2 models", [B["jupyter"].top(0.6), (c(6) + 102, B["mlflow"].y + 70), B["mlflow"].right(70 / BH)],
              0.75, "u", desc="Metrics, plots and both models of the notebook run"),
        Arrow("T", 3, "labels + features", [(B["s3"].x + B["s3"].w, s3y + 24), (kfp.x, s3y + 24)], 0.5, "u",
              desc="Steps 1 and 2 (load labels, build the training set) read the labels as of label_as_of and the point-in-time "
                   "features"),
        Arrow("T", 4, "training set snapshot", [(kfp.x, s3y + 56), (B["s3"].x + B["s3"].w, s3y + 56)], 0.5, "d",
              desc="Step 2 MERGEs the run's rows into ml/training_set_fraud_model_v1, one Delta version per run, so every model "
                   "can be traced back to the exact rows it learned from"),
        Arrow("T", 5, "metrics + candidate model", [kfp.left(0.06), (B["mlflow"].x + BW, kfp.y + kfp.h * 0.06)], 0.6, "u",
              desc="Steps 5 to 7 (train, evaluate, register) log params, data versions, metrics and the model; every model is "
                   "kept as a version, and gets alias challenger only when it passes the evaluation gate"),
        Arrow("T", 6, "production model,\nas an image", [B["mlflow"].top(), (B["mlflow"].x + BW / 2, r(3) + 42),
                                                     (B["kserve"].x + 260, r(3) + 42), B["kserve"].bottom(260 / 300)],
              0.7, "u",
              desc="Jenkins pipeline fraud-predictor: on a new alias it builds the model into the predictor image "
                   "(fraud-predictor:v<model>-<commit>), pushes it to ghcr.io, signs it and applies the InferenceService; "
                   "a challenger waits for a person to approve the A/B test"),
        Arrow("M", 1, "each request\n+ response", [B["kserve"].bottom(110 / 300), B["broker"].top()],
              desc="KServe's payload logger (the agent sidecar, mode all) posts every request and its response as CloudEvents "
                   "to the broker serving/default"),
        Arrow("M", 2, "request + response events", [B["broker"].left(), (c(4) + 130, B["broker"].y + 42), B["drift"].top(130 / BW)], 0.3, "u",
              desc="Trigger drift-detector delivers them to POST /v1/inference-events: request events count the features, "
                   "response events the score. Outlier and adversarial detectors would subscribe the same way"),
        Arrow("M", 3, "feature counts\nper minute", [B["drift"].top(0.3), (c(4) + 51, redis.y + 62), redis.right(62 / BH)], 0.7, "d",
              desc="Bin counts per feature and minute in Redis db 1; GET /v1/drift returns the PSI vs the training profile"),
        Arrow("M", 4, "last 7 days:\npayments, labels", [(c(5) + 140, s3y), B["airflow"].bottom(140 / BW)],
              desc="Data drift pipeline (DAG drift_monitoring, daily): the last 7 days of payments, their labels (the simulation's "
                   "ground truth) and point-in-time features"),
        Arrow("M", 5, "production model", [B["mlflow"].bottom(0.25), B["airflow"].top(0.25)], side="l",
              desc="Loads the production model to score them"),
        Arrow("M", 6, "PR-AUC,\nrecall, PSI", [B["airflow"].top(0.75), B["mlflow"].bottom(0.75)],
              desc="Logs PR-AUC, recall and PSI as a run of experiment card_fraud_monitoring"),
        Arrow("M", 7, "starts a training run", [B["airflow"].right(0.3), (c(5) + BW + 48, B["airflow"].y + BH * 0.3),
                                      (c(5) + BW + 48, r(5) - 36), (kfp.x, r(5) - 36)], 0.82, "u",
              desc="On drift: a training pipeline run (train_fraud_model) through the Kubeflow Pipelines API, label_as_of = the "
                   "DAG's day (a call, no data)"),
    ]

    W, H = 2206, 1320
    body = [t(28, 46, "CARD FRAUD PLATFORM · SYSTEM DIAGRAM · LAYERS 1 AND 2", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "System diagram, Layers 1 and 2: deployable units and the data they exchange. Layer 3, the agents, "
                      "has its own diagram.", 22, INK, "start", 400),
            panel(14, 96, 200, r(5) + BH + 16 - 96, "OUTSIDE", "none", LINE, True),
            panel(270, 96, c(6) + BW + 26 - 270, B["datahub"].y + 72 + 16 - 96, "DOCKER COMPOSE · project fraud", PANEL, LINE,
                  note="tag = compose profile"),
            panel(KX - 30, 96, 362, kfp.y + kfp.h + 16 - 96, "KIND CLUSTER · fraud", KIND_PANEL, "#b5c0d6",
                  note="tag = namespace")]
    body += [box_svg(b) for b in B.values()]
    body += [arrow_svg(a, PANEL) for a in A]
    body.append(legend(28, B["datahub"].y + 72 + 40, FLOWS, [
        "Numbers give the order inside each flow: S1 to S6 follow one payment from the authorization switch and back.",
        "Arrow = data moving from A to B, labelled with the data; a call without data (B2, M7) points from caller to callee.",
        "White box = one container or pod. Tag top left = where it runs: compose profile, namespace (ns) or host.",
        "Dashed boxes: support services used by many units (arrows left out).",
        "Feast is a library, not a unit: it runs inside the Scoring API, the push jobs, Airflow, Jupyter and the training pipeline's pods.",
        "Layer 3 (agents, gateway, tool servers) is on its own diagram; its tool servers read the Feast online store and "
        "call the Fraud Scoring API (A11, A14)."]))
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
# Layer 3 grid: the gateway is a bar across the middle of the cluster. Everything that calls it sits above it (the
# chat page and the agents), everything it calls sits below it (the model router and the tool servers), and what
# those read sits under the cluster (host model servers, compose stores). So every agent call is drawn going into the
# gateway first.
X3 = (300, 534, 768, 1002, 1236)   # main columns of the cluster
MCPX = (784, 1018, 1252)           # the three tool servers (policy-rag, card-data, fraud-score)
OX = (1500, 1734, 1968)            # observability and platform columns
TOP3 = 40                           # CI/CD band: top right, beside the title, clear of the lanes under it


def q(j):
    """y of Layer 3 grid row j: 1 callers, 2 the gateway, 3 what it calls, 4 host, 5-6 compose."""
    return 250 + (j - 1) * (BH + GY)


def system_layer_3():
    """The Layer 3 system diagram (agents, gateway, model servers, RAG, CI/CD, observability); returns (svg, arrows)."""
    x1, x2, x3, x4, x5 = X3
    p1, p2, p3 = MCPX
    o1, o2, o3 = OX
    GH = 100                                                   # height of the gateway bar
    gx, gr = x2, p3 + BW + 14                                  # the bar spans the chat page to the last tool server
    B = {
        "staff": Box(28, q(1), 180, BH, "Support staff", "user", "people", note="ask questions about cards"),
        "acme": Box(28, q(2), 180, BH, "Let's Encrypt", "internet", "letsencrypt", note="issues the HTTPS certificate", soft=True),
        "groq": Box(28, q(3), 180, BH, "Groq API", "internet", note="second LLM for an A/B test (off)", soft=True),
        # row 1: the callers of the gateway
        "nginx": Box(x1, q(1), BW, BH, "NGINX Ingress", "ns nginx-ingress", "nginx", note="HTTPS entry and login"),
        "ui": Box(x2, q(1), BW, BH, "Support chat", "ns kagent", note="chat page: sends the question to the gateway"),
        "coord": Box(x3, q(1), BW, BH, "Coordinator agent", "ns kagent", "kagent", note="splits the question, asks the agents"),
        "card": Box(x4, q(1), BW, BH, "card-data-agent", "ns kagent", "kagent", note="looks up card data and policy"),
        "risk": Box(x5, q(1), BW, BH, "risk-agent", "ns kagent", "kagent", note="checks fraud risk and policy"),
        # row 2: the gateway
        "kagent": Box(x1, q(2), BW, BH, "kagent", "ns kagent", "kagent", note="controller: turns each Agent into pods",
                      soft=True),
        "gw": Box(gx, q(2), gr - gx, GH, "agentgateway", "ns agentgateway-system", "agentgateway",
                  note="the first stop of every agent call: it checks the caller's key, then routes",
                  chips=("A2A routes", "MCP routes", "LLM route", "API key per agent", "rate + token limits",
                         "PII masking")),
        # row 3: what the gateway calls
        "llmd": Box(x1, q(3), BW, BH, "llm-d router", "ns agentgateway-system", "llmd", note="picks a model server per prompt"),
        "rag": Box(p1, q(3), BW, BH, "policy-rag-mcp", "ns card-support", "mcp", note="tool: searches the bank's policies"),
        "cdata": Box(p2, q(3), BW, BH, "card-data-mcp", "ns card-support", "mcp", note="tool: a card's data, policy text"),
        "fscore": Box(p3, q(3), BW, BH, "fraud-score-mcp", "ns card-support", "mcp", note="tool: fraud score of a payment"),
        # observability and platform services, right of the main path
        "keda": Box(o1, q(1), BW, BH, "KEDA", "ns keda", "keda", note="adds agent pods under load"),
        "prom": Box(o1, q(2), BW, BH, "Prometheus", "ns monitoring", "prometheus", note="collects metrics"),
        "jaeger": Box(o2, q(1), BW, BH, "Jaeger", "ns monitoring", "jaeger", note="traces of each agent call"),
        "grafana": Box(o2, q(2), BW, BH, "Grafana", "ns monitoring", "grafana", note="dashboards"),
        "vlogs": Box(o2, q(3), BW, BH, "VictoriaLogs", "ns monitoring", "victoria", note="logs of every pod"),
        "vault": Box(o3, q(1), BW, BH, "Vault + ESO", "ns vault", "vault", note="keeps the secrets", soft=True),
        "substrate": Box(o3, q(2), BW, BH, "Agent Substrate", "ns ate-system", note="runs each chat in a sandbox", soft=True),
        "areg": Box(o3, q(3), BW, BH, "agentregistry", "ns agentregistry", note="catalog of agents and tools", soft=True),
        # row 4: host; rows 5-6: compose
        "chat": Box(x1, q(4), BW, BH, "llama.cpp chat", "host", "llamacpp", note="chat model on the GPU"),
        "embed": Box(x2, q(4), BW, BH, "llama.cpp embed", "host", "llamacpp", note="turns text into vectors"),
        "datahub": Box(x1, q(5), BW, BH, "DataHub", "gov", "datahub", note="lineage and check results"),
        "airflow": Box(x2, q(5), BW, BH, "Airflow", "orch", "airflow", note="runs the RAG data pipeline"),
        "vdb": Box(p1, q(5), BW, BH, "Vector store", "ml", "postgres", note="vector-db: PostgreSQL\n+ pgvector"),
        "redis": Box(p2, q(5), BW, BH, "Feast online store", "ml", "redis", note="latest card features"),
        "scoring": Box(p3, q(5), BW, BH, "Fraud Scoring API", "ml", "fastapi", note="fraud score and decision"),
        "registry": Box(o3, q(5), BW, BH, "GHCR", "internet", "github", note="ghcr.io: the signed images"),
        "s3": Box(x2, q(6), BW, BH, "SeaweedFS (S3)", "core", "seaweedfs", note="bank documents, chunk history"),
        # CI/CD band
        "langfuse": Box(o1, TOP3, BW, BH, "Langfuse Cloud EU", "internet", "langfuse", note="prompts and eval results"),
        "jenkins": Box(o2, TOP3, BW, BH, "Jenkins", "CI/CD", "jenkins", note="tests, builds and deploys"),
        "github": Box(o3, TOP3, BW, BH, "GitHub", "internet", "github", note="the code"),
    }
    b = B
    fx, fy, fw, fh = x3 - 14, q(1) - 14, x5 + BW + 14 - (x3 - 14), BH + 28   # frame around the three agents
    mx, my, mw, mh = p1 - 14, q(3) - 14, gr - (p1 - 14), BH + 28             # frame around the three tool servers
    g_top, g_bot, r3_bot = q(2), q(2) + GH, q(3) + BH
    jb = TOP3 + BH        # bottom of the CI/CD boxes; the band ends 16 below, so the lanes start at jb + 36
    A = [
        Arrow("A", 1, "question", [b["staff"].right(), b["nginx"].left()],
              desc="A question in the support chat (https://support.<domain>, basic auth and rate limit at NGINX)"),
        Arrow("A", 2, "question", [b["nginx"].right(), b["ui"].left()],
              desc="NGINX passes it to support-chat, which serves the chat page (one A2A message/send per question)"),
        Arrow("A", 3, "question +\nAPI key", [b["ui"].bottom(), (x2 + 85, g_top)], 0.5, "l",
              desc="support-chat forwards the question to the gateway route /a2a/coordinator and adds the support-app API key, "
                   "so the key never reaches the browser. The gateway is the first hop of every question"),
        Arrow("A", 4, "question", [(x3 + 42, g_top), b["coord"].bottom(42 / BW)],
              desc="Route a2a-coordinator: the gateway checks the key (only support-app may call it), applies the rate limit "
                   "and passes the question to the coordinator over A2A"),
        Arrow("A", 5, "sub-tasks,\nprompts", [b["coord"].bottom(128 / BW), (x3 + 128, g_top)],
              desc="The coordinator's A2A calls to the two agents and its LLM calls, all through the gateway "
                   "(kagent proxy.url)"),
        Arrow("A", 6, "sub-task", [(x4 + 42, g_top), b["card"].bottom(42 / BW)],
              desc="Route card-support-data-agents-internal: a sub-task for card-data-agent; only the coordinator's key may call it"),
        Arrow("A", 7, "sub-task", [(x5 + 42, g_top), b["risk"].bottom(42 / BW)],
              desc="... and for risk-agent (risk-agent-b, the B side of its model-config A/B test, sits behind "
                   "the same route)"),
        Arrow("A", 8, "tool calls,\nprompts", [b["card"].bottom(128 / BW), (x4 + 128, g_top)],
              desc="card-data-agent's tool calls (MCP, its own API key) and LLM calls through the gateway"),
        Arrow("A", 9, "tool calls,\nprompts", [b["risk"].bottom(128 / BW), (x5 + 128, g_top)],
              desc="risk-agent's tool calls and LLM calls through the gateway"),
        Arrow("A", 10, "tool calls", [(p2 + 82, g_bot), (p2 + 82, my)],
              desc="Routes /mcp/card-data, /mcp/fraud-score, /mcp/policy-rag: only the key of each server's agent "
                   "sees its tools (300 calls/min per server)"),
        Arrow("A", 11, "card features", [b["redis"].top(132 / BW), (p2 + 132, r3_bot)], 0.5,
              desc="card-data-mcp, get_card_data: the card's online features from Redis through Feast (project "
                   "card_fraud)"),
        Arrow("A", 12, "chunk", [b["vdb"].top(153 / BW), (p1 + 153, r3_bot + 44), (p2 + 22, r3_bot + 44), (p2 + 22, r3_bot)],
              0.5, "r", desc="card-data-mcp, get_policy_chunk: one policy chunk by id from pgvector (Feast project card_support_kb)"),
        Arrow("A", 13, "question vector", [b["embed"].top(144 / BW), (x2 + 144, r3_bot + 24), (p1 + 26, r3_bot + 24),
                                            (p1 + 26, r3_bot)], 0.4, "u",
              desc="policy-rag-mcp, search_policy: the question's vector from Qwen3-Embedding-0.6B"),
        Arrow("A", 14, "payment to score", [(p3 + 78, r3_bot), b["scoring"].top(78 / BW)], 0.5,
              desc="fraud-score-mcp, score_payment: POST /v1/score with the payment the customer describes (merchant "
                   "agent-what-if, direct HTTP, not through the gateway); the Scoring API answers with the score and decision"),
        Arrow("A", 15, "prompts", [(x2 + 26, g_bot), (x2 + 26, q(3) + 42), b["llmd"].right()], 0.75, "u",
              desc="Route /v1: the agent's API key is checked, request and token limits applied, personal data masked, then the "
                   "InferencePool qwen3-8b"),
        Arrow("A", 16, "prompt", [b["llmd"].bottom(0.85), b["chat"].top(0.85)], 0.3,   # right of the host panel title
              desc="The endpoint picker chooses a pod; its bridge forwards the prompt to llama.cpp on the host GPU"),
        Arrow("A", 17, "nearest chunks", [b["vdb"].top(116 / BW), (p1 + 116, r3_bot)], 0.3, "l",
              desc="policy-rag-mcp, search_policy: the chunks nearest to the question vector from pgvector, re-ranked "
                   "by cosine"),
        Arrow("K", 1, "5 documents", [b["s3"].top(0.2), b["airflow"].bottom(0.2)], side="l",
              desc="rag_ingest reads the 5 bank documents from s3://bank-documents"),
        Arrow("K", 2, "chunk vectors", [b["embed"].bottom(), b["airflow"].top()],
              desc="Steps chunk (57 chunks) and embed: one 1024-dim vector per chunk from the embedding server"),
        Arrow("K", 3, "chunks +\nvectors", [b["airflow"].right(), b["vdb"].left()],
              desc="Step write: chunks + vectors into the Feast online store (pgvector)"),
        Arrow("K", 4, "chunk history", [b["airflow"].bottom(0.6), b["s3"].top(0.6)],
              desc="... and their history into the offline store s3://lakehouse/kb/policy_chunks.parquet"),
        Arrow("K", 5, "checks,\nlineage", [b["airflow"].left(), b["datahub"].right()], side="u",
              desc="Step validate: 7 checks as assertions, plus lineage documents -> jobs -> feature view"),
        Arrow("C", 1, "commits", [b["github"].left(), b["jenkins"].right()],
              desc="Jenkins polls GitHub main for its 11 pipelines, one per deployable unit (docs/operations/cicd.md)"),
        Arrow("C", 2, "prompts", [b["langfuse"].right(0.3), b["jenkins"].left(0.3)],
              desc="Agent pipelines fetch the prompt labelled production for each agent"),
        Arrow("C", 3, "image + signature", [b["jenkins"].bottom(0.8), (o2 + 136, jb + 60), (2180, jb + 60),
                                            (2180, q(5) + 42), b["registry"].right()],
              (60 + 2180 - o2 - 136 + q(1) + BH + 36 - jb - 60) / (60 + 2180 - o2 - 136 + q(5) + 42 - jb - 60 + 42), "l",
              desc="MCP pipeline: tests (coverage gate 90 %), mutation test, image build, push to ghcr.io, cosign signature"),
        Arrow("C", 4, "signed image", [b["registry"].top(110 / BW), (o3 + 110, r3_bot + 30), (gr - 26, r3_bot + 30),
                                       (gr - 26, my + mh)], 0.3, "u",
              desc="helm upgrade --atomic of each MCP server; the node pulls the signed image from ghcr.io (Kyverno verifies "
                   "it whenever it is installed)"),
        Arrow("C", 5, "agents + prompts", [b["jenkins"].bottom(0.2), (o2 + 34, jb + 64), (x4 + 85, jb + 64),
                                           (x4 + 85, fy)], 0.7, "d",
              desc="Agent pipelines: helm upgrade --atomic of the agents, each Agent with its Langfuse prompt inline and the "
                   "prompt version as an annotation"),
        Arrow("C", 6, "eval questions", [b["jenkins"].bottom(0.1), (o2 + 17, jb + 40), (x3 - 39, jb + 40),
                                         (x3 - 39, g_top)], 0.25, "u",
              desc="Eval gate: the eval dataset asked to risk-agent through the gateway; below 0.6 accuracy, "
                   "helm rollback"),
        Arrow("C", 7, "eval\nscores", [b["jenkins"].left(0.7), b["langfuse"].right(0.7)], side="d",
              desc="Answers and their scores stored as a run of the Langfuse experiment"),
        Arrow("C", 8, "starts rag_ingest", [b["jenkins"].bottom(0.9), (o2 + 153, jb + 36), (2200, jb + 36),
                                            (2200, q(5) + BH + 28), (x2 + 146, q(5) + BH + 28), b["airflow"].bottom(146 / BW)],
              0.62, "d", desc="RAG pipeline: sets Variable rag_image to the new image, triggers rag_ingest (Airflow REST API)"),
        Arrow("O", 1, "gateway\nmetrics", [(gr, g_top + 42), b["prom"].left(42 / BH)],
              desc="Gateway metrics: requests per route and backend, LLM tokens, time to first token, PII guardrail hits"),
        Arrow("O", 2, "tool metrics", [(gr, q(3) + 42), (o1 + 85, q(3) + 42), b["prom"].bottom()], 0.3, "d",
              desc="MCP servers' metrics (one ServiceMonitor each): calls, failures and latency per tool"),
        Arrow("O", 3, "time\nseries", [b["prom"].right(), b["grafana"].left()], side="d",
              desc="Grafana dashboards: Web API, compute, LLM telemetry, agent telemetry, A/B tests"),
        Arrow("O", 4, "agent\nrequests/s", [b["prom"].top(), b["keda"].bottom()],
              desc="KEDA's trigger: A2A requests per second for each agent"),
        Arrow("O", 5, "2 to 4\nreplicas", [b["keda"].left(), (fx + fw, q(1) + 42)], side="u",
              desc="KEDA sets each agent's replicas between 2 and 4"),
        Arrow("O", 6, "trace spans", [(fx + fw - 20, fy), (fx + fw - 20, fy - 16), (o2 + 85, fy - 16), b["jaeger"].top()],
              0.6, "u", desc="Agents send spans over OTLP; the gateway's spans join the same traces (one conversation: about 20)"),
    ]

    W = 2240
    kind_t, kind_b = q(1) - 50, r3_bot + 22
    host_t, comp_t, comp_b = q(4) - 26, q(5) - 26, q(6) + BH + 16
    H = comp_b + 290
    body = [t(28, 46, "CARD FRAUD PLATFORM · SYSTEM DIAGRAM · LAYER 3", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "System diagram, Layer 3: every agent call goes into agentgateway first; what calls it sits above "
                      "it, what it calls below.", 22, INK, "start", 400),
            panel(14, kind_t, 208, q(3) + BH + 16 - kind_t, "OUTSIDE", "none", LINE, True),
            panel(o1 - 24, TOP3 - 24, o3 + BW + 24 - (o1 - 24), BH + 40, "CI/CD", "none", LINE, True),
            panel(x1 - 30, kind_t, o3 + BW + 26 - (x1 - 30), kind_b - kind_t, "KIND CLUSTER · fraud", KIND_PANEL,
                  "#b5c0d6", note="tag = namespace"),
            panel(x1 - 30, host_t, x2 + BW + 24 - (x1 - 30), BH + 42, "HOST · GTX 1070", "none", LINE, True),
            panel(x1 - 30, comp_t, o3 - 24 - (x1 - 30), comp_b - comp_t, "DOCKER COMPOSE · fraud", PANEL, LINE),
            panel(o3 - 14, q(5) - 26, BW + 28, BH + 42, "INTERNET", "none", LINE, True)]
    for x0, y0, w0, h0 in ((fx, fy, fw, fh), (mx, my, mw, mh)):   # dotted frames: the agents, the MCP servers
        body.append(f'<rect x="{x0}" y="{y0}" width="{w0}" height="{h0}" rx="12" fill="none" stroke="#9aa1b1" '
                    f'stroke-dasharray="2 4"/>')
    body += [box_svg(x) for x in B.values()]
    body += [arrow_svg(a, PANEL) for a in A]
    body.append(legend(28, comp_b + 24, FLOWS3, [
        "Numbers give the order inside each flow: A1 to A17 follow one question from support staff to the tools and the model.",
        "Above the gateway: what calls it. Below it: what it calls. Under the cluster: the stores and model servers they read.",
        "White box = one pod or container. Tag top left = where it runs: namespace (ns), compose profile, host or internet.",
        "A sub-task goes from agent to agent (A2A), a tool call to a tool server (MCP), a prompt to the chat model (LLM route).",
        "Each reply comes back on the same call and is not drawn.",
        "Dotted frames: the three agents (A6-A9, C5, O5, O6 apply to each) and the three MCP servers",
        "(one image, one Helm release each; A10, C4, O2 apply to each). Dashed boxes: support services.",
        "NGINX also serves registry, grafana, logs, traces and the Web API (api.<domain>: card data",
        "and policy chunks by id, from card-data-mcp, over HTTPS).",
        "The Feast online store and the Fraud Scoring API are Layer 2 units, drawn again here because the tool servers "
        "call them (A11, A14)."]))
    check("system-layer-3", B, A)
    crossings("system-layer-3", A)
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: system diagram, Layer 3",
                   "Deployable units of the LLM and agent platform and the data between them, numbered per flow. The "
                   "gateway sits in the middle: the chat page and the agents call it from above, it calls the model "
                   "router and the tool servers below."), A


# ---------------------------------------------------------------- deployment
def deployment():
    """Where each part runs: the host row on top; in the Docker Desktop VM compose (left) and the kind node (right),
    one box per compose profile or namespace with its logos and what runs in it; Azure and the SaaS on the right.
    Each network path is a numbered arrow; the list under the diagram says what goes over it."""
    W = 2240
    body, arrows = [t(28, 46, "CARD FRAUD PLATFORM · DEPLOYMENT · LAYERS 1 TO 3", 12, INK2, "start", 600, mono=True,
                      spacing=0.14),
                    t(28, 76, "Deployment: one Windows host. Docker Desktop runs compose and a kind cluster; Azure only "
                              "for the IaC runs; SaaS on the internet", 22, INK, "start", 400)], []

    def unit(x, y, w, title, tag, logos, lines, kind):
        """One compose profile, namespace or resource group: its logos, name and version, then what runs in it."""
        fill, stroke = KINDS[kind]
        rows = [r for ln in lines for r in wrap(ln, w - 26, 11)]   # 11: Segoe UI at 12 px is narrower than wrap() assumes
        h = 44 + 17 * len(rows) + 6
        s = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="{fill}" stroke="{stroke}" stroke-width="1.3"/>']
        lx = x + 12
        for lg in logos:
            s.append(icon(lg, lx, y + 9, 20))
            lx += 25
        s.append(t(lx + 3, y + 25, title, 14, INK, "start", 700))
        if tag:
            tx, ms = lx + 3 + text_w(title, 14, False) * 1.08 + 14, 10.5
            while text_w(tag, ms, True) > x + w - 10 - tx and ms > 8.5:   # the tag shrinks to stay inside the unit
                ms -= 0.5
            s.append(t(tx, y + 25, tag, ms, INK2, "start", mono=True))
        s += [t(x + 14, y + 48 + 17 * i, r, 12, INK, "start") for i, r in enumerate(rows)]
        body.append("\n".join(s))
        return Box(x, y, w, h, title)

    def column(x, y, w, units):
        """Units stacked top to bottom; returns their boxes."""
        out = []
        for u in units:
            out.append(unit(x, y, w, *u))
            y = out[-1].y + out[-1].h + 12
        return out

    def num(x, y, n, label="", side="r"):
        """The number of a network path on its arrow, with a short label; the list below says the rest."""
        arrows.append(f'<circle cx="{x}" cy="{y}" r="11" fill="#ffffff" stroke="{INK2}" stroke-width="1.6"/>'
                      + t(x, y + 4, str(n), 11, INK, "middle", 700))
        if label:
            lx, ly, an = {"r": (x + 17, y + 4, "start"), "u": (x, y - 17, "middle")}[side]
            arrows.append(t(lx, ly, label, 11, INK, an, halo=PAGE if side == "r" else PANEL))

    def wire(pts):
        d = path_d(pts)
        arrows.append(f'<path d="{d}" fill="none" stroke="{PAGE}" stroke-width="7"/>'
                      f'<path d="{d}" fill="none" stroke="{INK2}" stroke-width="1.8" marker-end="url(#ah-net)"/>')

    # ---- the VM: compose on the left, the kind node on the right, a gap between them for the paths 7 to 9
    vy, top = 282, 282 + 40
    cx0, cw = 34, 360                                # compose frame left, unit width
    ccol = (cx0 + 20, cx0 + 20 + cw + 14)
    comp_r = ccol[1] + cw + 20
    kx0 = comp_r + 110
    kw = (1870 - kx0 - 40 - 28) / 3                  # kind: three columns, compose: two; both end about level
    kcol = tuple(kx0 + 20 + i * (kw + 14) for i in range(3))
    uy = top + 40                                    # first unit row, under the frame titles
    c_left = column(ccol[0], uy, cw, [
        # title: what the profile is for; tag: the profile's name, as typed in `--profile <name>`
        ("Core services", "profile core", ["postgres", "seaweedfs", "kafka"], [
            "Always on. postgres :5433 (postgres:16-alpine)", "seaweedfs :8333: the S3 lakehouse (alias lakehouse-s3)",
            "kafka :9092 (apache/kafka 3.9.0, KRaft)"], "store"),
        ("Streaming", "profile stream", ["flink"], [
            "Run it alone. flink-jobmanager :8088 (fraud/flink:2.2.1)", "flink-taskmanager: 6 slots, 3.5 GB"], "job"),
        ("Batch processing", "profile batch", ["spark", "trino", "lakefs", "hive"], [
            "spark-master :8080 (Spark 4.1.3), spark-worker :8081, history :18080",
            "hive-metastore + metastore-db (Postgres)", "trino :8090 (2 GB), lakefs :8001"], "job"),
        ("dbt", "profile dbt", ["dbt", "spark"], ["By hand. spark-thrift :10000 (fraud/spark:4.1.3)"], "job"),
        ("CI/CD", "profile cicd", ["jenkins"], ["jenkins :8095 (Jenkins 2.580.1): 11 pipelines"], "out")])
    c_right = column(ccol[1], uy, cw, [
        ("Orchestration", "profile orch", ["airflow", "docker"], [
            "airflow-webserver :8082 (Airflow 2.10.5)", "scheduler: LocalExecutor + a Feast venv",
            "docker-proxy: Docker API for the RAG steps",
            "airflow-db (Postgres); airflow-init, batch-release-init run once"], "job"),
        ("Data governance", "profile gov", ["datahub", "opensearch"], [
            "datahub-frontend :9002 (DataHub v1.5.0.6), datahub-gms :8083", "OpenSearch, datahub-db (Postgres)",
            "datahub-system-update runs once"], "store"),
        ("ML stores", "profile ml", ["postgres", "redis"], [
            "ml-db :5434: MLflow, Feast registry", "vector-db :5435: pgvector, the RAG chunks",
            "redis :6379: the Feast online store"], "store"),
        ("ML apps", "profile ml", ["fastapi", "mlflow", "jupyter"], [
            "scoring-api :8000, drift-api :8002", "mlflow :5000, notebook :8899 (Jupyter)",
            "push-online, push-offline (ml-train image)"], "svc")])
    k_left = column(kcol[0], uy, kw, [
        ("nginx-ingress", "F5 NIC 5.6.3", ["nginx"], [
            "the only way in from a browser: :80, :443", "7 hosts under *.<domain>, basic auth"], "gate"),
        ("agentgateway-system", "1.6.0", ["agentgateway", "llmd", "qwen"], [
            "proxy :31508: LLM, MCP and A2A routes", "llm-d endpoint picker (qwen3-8b-epp)",
            "qwen3-8b bridge pods → host llama.cpp"], "gate"),
        ("kagent", "0.10.3", ["kagent"], [
            "coordinator, card-data and risk agents", "2 to 4 pods each (KEDA); risk side b",
            "controller, kagent-ui, support-chat", "kagent-postgresql: sessions, state"], "svc"),
        ("agentregistry", "0.4.0", [], ["agent and MCP catalog, UI :12121"], "svc")])
    k_mid = column(kcol[1], uy, kw, [                # the agents' tools and what guards them
        ("card-support", "", ["mcp"], [
            "card-data, fraud-score, policy-rag", "2 pods each, images signed in CI"], "svc"),
        ("kyverno", "v1.19.1", ["kyverno", "cosign"], ["MCP pods start only from signed images"], "out"),
        ("ate-system", "Substrate 0.0.9", ["kubernetes"], [
            "ate-api + controller, atelet (gVisor)", "valkey-cluster (6 pods), rustfs"], "out"),
        ("vault · external-secrets", "", ["vault"], [
            "Vault (dev mode): secret/card-fraud/*", "External Secrets: Vault → Secrets"], "out"),
        ("keda · cert-manager", "", ["keda"], [
            "keda-operator 2.21.0: agent replicas", "cert-manager v1.21.1: webhook certs"], "out")])
    k_right = column(kcol[2], uy, kw, [              # Layer 2 and monitoring
        ("kubeflow", "KFP 2.17.2", ["kubeflow", "argo"], [
            "ml-pipeline API :31888, UI :31880", "Argo workflow-controller runs the DAG",
            "step pods + 2 Spark executors", "metadata-grpc; mysql + seaweedfs"], "job"),
        ("serving · kserve", "KServe 0.21", ["kserve"], [
            "card-fraud :31080 + logger; challenger :31081", "kserve-controller: InferenceService"], "svc"),
        ("knative-eventing", "1.23.1", ["kubernetes"], [
            "mt-broker: Broker serving/default", "controllers + in-memory channel"], "svc"),
        ("monitoring", "", ["prometheus", "grafana", "victoria", "jaeger"], [
            "Prometheus, node-exporter, ksm", "Grafana: 5 dashboards", "VictoriaLogs + vector: pod logs",
            "Jaeger: traces (OTLP)"], "out")])
    fb = max(b.y + b.h for b in c_left + c_right + k_left + k_mid + k_right) + 20   # both frames end together
    frames = [panel(14, vy, 1876, fb + 20 - vy, "DOCKER DESKTOP · WSL2 VM · 8 GB RAM + 4 GB swap", PANEL, LINE),
              panel(cx0, top, comp_r - cx0, fb - top, "COMPOSE PROJECT fraud", "#ffffff", LINE,
                    note="8 profiles · ports on 127.0.0.1 only"),
              panel(kx0, top, 1870 - kx0, fb - top, "KIND NODE fraud-control-plane", KIND_PANEL, "#b5c0d6",
                    note="kindest/node v1.37.0 · one box per namespace")]
    vb = fb + 20

    # ---- the host: what calls compose sits over compose, what talks to kind over kind; terraform next to Azure
    host = [panel(14, 96, 1876, 128, "HOST · Windows 11 · GTX 1070 · repo .venv + CLIs", "none", LINE, True)]
    hosts = {"py": Box(300, 124, 260, 84, "Python scripts", "", "python", note="data generator, load tests"),
             "docker": Box(576, 124, 220, 84, "docker CLI", "", "docker", note="builds and pushes images"),
             "llama": Box(kx0 - 40, 124, 240, 84, "llama.cpp", "", "llamacpp",
                          note="on the GPU:\nchat :8085, embed :8086"),
             "browser": Box(kx0 + 216, 124, 140, 84, "Browser", "", "person", note="opens every UI"),
             "kind": Box(kx0 + 372, 124, 180, 84, "kind · helm", "", "kubernetes", note="creates the cluster,\ninstalls charts"),
             "iac": Box(1692, 124, 184, 84, "terraform", "", "terraform", note="+ ansible, kubectl:\nthe Azure test setup")}
    assert hosts["kind"].x + 180 < hosts["iac"].x - 8, "host row overlaps"
    host += [box_svg(b) for b in hosts.values()]

    # ---- right column: Azure next to terraform (it exists only during an IaC run), the SaaS under it
    ix, iw = 1918, 294
    az = [t(ix, 146 + 16 * i, ln, 11, INK2, "start", mono=True) for i, ln in enumerate(
        ("rg-cardfraud-dev · Azure for Students", "created from the PC, destroyed after", "each IaC run"))]
    body += az
    a = column(ix - 4, 206, iw + 8, [
        ("vm", "B2s_v2 · Ubuntu 24.04", ["python", "ansible"], [
            "predictor :8080, built on the VM by Ansible", "node_exporter :9100"], "svc"),
        ("aks", "Free tier · 1 node", ["kubernetes"], ["card-fraud: 2 pods from ACR, ClusterIP :80"], "svc"),
        ("acr", "Basic", ["docker"], ["crcardfrauddev: fraud/predictor:<v>"], "store"),
        ("network", "", ["azure"], ["VNet + subnet, public IP + NIC", "NSG: 22, 8080, 9100 for the admin IP only"],
         "out")])
    ab = a[-1].y + a[-1].h + 16
    iy = ab + 24
    s = column(ix - 4, iy + 40, iw + 8, [
        ("GitHub + GHCR", "", ["github"], ["the code; Jenkins polls branch main",
                                            "ghcr.io: the images and their signatures"], "svc"),
        ("Langfuse Cloud EU", "", ["langfuse"], ["agent prompts, eval datasets and results"], "svc"),
        ("Groq", "", [], ["a second LLM for an A/B test (off now)"], "svc"),
        ("Let's Encrypt + DuckDNS", "", ["letsencrypt"], ["the HTTPS certificate for NGINX"], "svc")])
    sb = s[-1].y + s[-1].h + 16
    right = [panel(ix - 14, 96, iw + 28, ab - 96, "AZURE · polandcentral", "#f4f7fc", "#b5c0d6", True),
             panel(ix - 14, iy, iw + 28, sb - iy, "INTERNET · SaaS", "none", LINE, True)]

    # ---- the network paths: straight arrows, each numbered
    hb, mid = 208, (hosts["py"].y + 84 + vy) / 2      # bottom of the host boxes; where the numbers sit
    xs = {k: b.x + b.w / 2 for k, b in hosts.items()}
    xs["py"] = max(xs["py"], 520)                    # clear of the VM title
    wire([(xs["py"], hb), (xs["py"], top)])
    num(xs["py"], mid, 1, "127.0.0.1 ports")
    wire([(xs["docker"], hb), (xs["docker"], top)])
    num(xs["docker"], mid, 2, "docker API")
    wire([(xs["llama"], top), (xs["llama"], hb)])
    num(xs["llama"], mid, 3, ":8085, :8086")
    wire([(xs["browser"], hb), (xs["browser"], top)])
    num(xs["browser"], mid, 4, "HTTPS :443")
    wire([(xs["kind"], hb), (xs["kind"], top)])
    num(xs["kind"], mid, 5, "kind API")
    wire([(1876, 166), (ix - 14, 166)])
    num(1890, 166 - 26, 6)
    gx = (comp_r + kx0) / 2
    for n, y, back, label in ((7, uy + 130, True, "by name"), (8, uy + 250, False, "NodePorts")):
        wire([(kx0, y), (comp_r, y)] if back else [(comp_r, y), (kx0, y)])
        num(gx, y, n, label, "u")
    y9 = s[0].y + 30                                 # Docker Desktop <-> ghcr.io, into the GitHub + GHCR unit
    wire([(1890, y9), (ix - 14, y9)])
    num(1890, y9 - 26, 9)
    legend_y = max(vb, sb) + 40

    # ---- the key, the paths, the notes
    foot = [t(34, legend_y, "BOX COLOUR", 11, INK2, "start", 600, mono=True, spacing=0.08)]
    kx = 140
    for kind, label in (("store", "stores data"), ("job", "runs pipelines or jobs"), ("svc", "serves an API or an app"),
                        ("gate", "entry point or gateway"), ("out", "runs the platform: CI, scaling, security, monitoring")):
        fill, stroke = KINDS[kind]
        foot += [f'<rect x="{kx}" y="{legend_y - 12}" width="24" height="16" rx="4" fill="{fill}" stroke="{stroke}" '
                 f'stroke-width="1.3"/>', t(kx + 32, legend_y + 1, label, 12, INK2, "start")]
        kx += 32 + text_w(label, 12, False) + 30
    py0 = legend_y + 40
    foot.append(t(34, py0, "NETWORK PATHS", 11, INK2, "start", 600, mono=True, spacing=0.08))
    paths = [
        "Python scripts → compose on 127.0.0.1:<port>: Kafka, Postgres, S3, the APIs and the UIs",
        "docker CLI → the Docker Desktop engine: builds the images (path 9 pushes them)",
        "kind pods → llama.cpp on the host: host.docker.internal :8085 (chat), :8086 (embed)",
        "Browser → NGINX in kind, HTTPS :443: support., agents., registry., grafana., logs., traces., api.<domain>",
        "kind · helm → the kind API: creates the cluster, installs the charts",
        "terraform, ansible, kubectl → Azure: create, check and destroy the IaC test setup",
        "kind pods → compose by name (the node is on fraud_default): lakehouse-s3:8333, kafka, ml-db, redis, mlflow, "
        "vector-db, scoring-api, drift-api",
        "compose → kind NodePorts: scoring-api → :31080 KServe; Airflow → :31888 KFP; Jenkins → kind API (helm), "
        ":31508 (eval)",
        "Docker Desktop → ghcr.io, HTTPS: Jenkins pushes the images and signatures; the kind node and compose pull them"]
    for i, ln in enumerate(paths):
        x, y = (34 if i < 5 else 1130), py0 + 30 + 26 * (i % 5)
        foot += [f'<circle cx="{x + 11}" cy="{y - 4}" r="11" fill="#ffffff" stroke="{INK2}" stroke-width="1.6"/>',
                 t(x + 11, y, str(i + 1), 11, INK, "middle", 700), t(x + 30, y, ln, 12, INK, "start")]
    ny0 = py0 + 30 + 26 * 5 + 16
    notes = ["A box is one compose profile or one namespace, not one container: the lines say what runs in it and on "
             "which port. :port = published on 127.0.0.1 (compose) or a NodePort (kind).",
             "8 GB does not hold every profile: run stream alone; keep trino and dbt off while orch + gov run; "
             "Layer 3 runs with core + ml + cicd and the kind cluster.",
             "seaweedfs has the alias lakehouse-s3: inside kind, KFP's own seaweedfs Service shadows the short name.",
             "The model servers run on the host GPU (GTX 1070, Pascal: no vLLM); llm-d's InferencePool selects thin "
             "bridge pods that forward to them.",
             "Azure (docs/operations/iac.md) exists only during an IaC run: created, checked, destroyed. Its quotas (6 vCPU and 3 "
             "public IPs per region, 0 for GPUs) keep the rest of the platform on the PC."]
    foot += [t(34, ny0 + 18 * i, n, 12, INK2, "start") for i, n in enumerate(notes)]
    H = ny0 + 18 * len(notes) + 14
    out = body[:2] + host + right + frames + body[2:] + arrows + foot
    return svg_doc(W, H, "\n".join(out), "Card fraud platform: deployment",
                   "The host row with the CLIs and llama.cpp; in the Docker Desktop VM, the compose profiles on the left "
                   "and the kind cluster's namespaces on the right, one box each; the Azure resources of the IaC runs and "
                   "the internet services on the right; nine numbered network paths between them.")


# ---------------------------------------------------------------- overview
NOVEL = "#c2255c"   # the four novel ideas: raspberry, >= 15 OKLab x100 from every flow colour (dataviz validator)
# The flows of the overview's arrows (colours of FLOWS / FLOWS3). Training and release is also dashed: its purple is
# close to the streaming blue for deutan readers, so the dash tells them apart without colour.
OVERVIEW_FLOWS = {
    "S": ("Payment scoring", "each card payment, in milliseconds"),
    "R": ("Streaming", "always on, seconds after each payment"),
    "B": ("Batch data", "nightly and daily pipelines; the policy documents"),
    "T": ("Training and release", "on drift or by an engineer"),
    "M": ("Monitoring", "drift per minute and per day"),
    "A": ("Agent question", "each question from support staff"),
}
DASH = {"T": ' stroke-dasharray="7 4"'}
CTL = ' stroke-dasharray="1 5" stroke-linecap="round"'   # dotted: Airflow starts a pipeline, no data moves


def novel_tag(xr, y, text="NOVEL IDEA") -> str:
    """The novel-idea pill, right-aligned at xr: white letters on raspberry, a shape no arrow or box has."""
    w = text_w(text, 8.5, True, 0.06) + 14
    return (f'<rect x="{xr - w}" y="{y}" width="{w}" height="16" rx="8" fill="{NOVEL}"/>'
            + t(xr - w / 2, y + 11.3, text, 8.5, "#ffffff", "middle", 700, mono=True, spacing=0.06))
KINDS = {   # kind of a box on the overview -> fill, outline
    "out": ("#f1f2f4", "#8a91a3"),       # a system outside the platform
    "job": ("#edf7ee", "#5fa66c"),       # a pipeline or job: it processes data
    "svc": ("#ffffff", "#8a93a6"),       # a service: it answers requests
    "store": (STORE_FILL, STORE_LINE),   # a store: data rests here
    "stream": ("#fdf4d9", "#cf9f33"),    # the event stream
    "gate": ("#f0ebfb", "#8a74d6"),      # an entry point or gateway
    "note": ("#fbfbfe", "#9d96c9"),      # a reading aid, not a component
}


def info(x, y, w, title, tech="", bullets=(), kind="svc", ico="", novel="", pad=0) -> tuple[str, Box]:
    """A component of the overview, written the way a person would explain it: its name, the technology under it,
    then what it does as short bullets (pad = extra room at the bottom). Returns (svg, its Box: the anchor of the
    arrows)."""
    lines = [(i == 0, ln) for b in bullets for i, ln in enumerate(wrap(b, w - 46, 12.5))]
    top = 58 if tech else 42
    h = top + 17 * len(lines) + 10 + pad
    fill, stroke = KINDS[kind]
    dash = ' stroke-dasharray="6 4"' if kind == "note" else ""
    s = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="{stroke}" stroke-width="1.4"{dash}/>']
    tx = x + 16
    if ico:
        s.append(icon(ico, x + 14, y + 10, 22))
        tx = x + 44
    room = x + w - tx - 12 - (text_w(novel, 8.5, True, 0.06) + 22 if novel else 0)   # what the pill leaves
    ts = 15.5                                      # a long name or tech line shrinks until it fits the card
    while text_w(title, ts, False) * 1.08 > room and ts > 12:                       # 700 weight: ~8% wider
        ts -= 0.5
    ms = 11
    while tech and text_w(tech, ms, True) > x + w - tx - 12 and ms > 9:
        ms -= 0.5
    s.append(t(tx, y + 27, title, ts, INK, "start", 700))
    if tech:
        s.append(t(tx, y + 44, tech, ms, INK2, "start", mono=True))
    if novel:
        s.append(novel_tag(x + w - 10, y + 8, novel))
    for i, (first, ln) in enumerate(lines):
        yy = y + top + 13 + 17 * i
        if first:
            s.append(f'<circle cx="{x + 22}" cy="{yy - 4}" r="2.2" fill="{INK2}"/>')
        s.append(t(x + 32, yy, ln, 12.5, INK, "start"))
    return "\n".join(s), Box(x, y, w, h, title)


def overview():
    """The front page, for a reader who does not know the project: few boxes, each with its name, its technology and
    what it does in plain bullets; an arrow goes from the part that sends data (or a call) to the part that receives
    it, with what moves written on it. Details of every step are on the system diagrams."""
    W = 2640
    body, arrows = [], []

    def box(x, y, w, title, tech="", bullets=(), kind="svc", ico="", novel="", pad=0):
        """Draw one component and return its Box."""
        svg, b = info(x, y, w, title, tech, bullets, kind, ico, novel, pad)
        body.append(svg)
        return b

    F = ["B"]   # the flow of the arrows drawn next; f= overrides it for one arrow

    def link(pts, label="", lx=None, ly=None, anchor=None, halo=PANEL, both=False, f=None, ctl=False):
        """An arrow from sender to receiver (both = a call and its answer on one line), in the colour of its flow;
        ctl = dotted: a start signal, no data moves. Without lx/ly the label sits at the middle of the longest segment."""
        f = f or F[0]
        color = ALL_FLOWS[f][2]
        if label and lx is None:
            a, b = max(zip(pts, pts[1:]), key=lambda s: abs(s[1][0] - s[0][0]) + abs(s[1][1] - s[0][1]))
            n = label.count("\n")
            if a[1] == b[1]:
                lx, ly, anchor = (a[0] + b[0]) / 2, a[1] - 7 - 14 * n, anchor or "middle"
            else:
                lx, ly, anchor = a[0] + 8, (a[1] + b[1]) / 2 + 4 - 7 * n, anchor or "start"
        d = path_d(pts)
        back = f' marker-start="url(#ah-back-{f})"' if both else ""
        arrows.append("\n".join([
            f'<path d="{d}" fill="none" stroke="{halo}" stroke-width="7"/>',
            f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2"'
            f'{CTL if ctl else DASH.get(f, "")} marker-end="url(#ah-{f})"{back}/>',
            *[t(lx, ly + 14 * i, ln, 11.5, INK, anchor, halo=halo) for i, ln in enumerate(label.split("\n")) if ln]]))

    def dot(x, y, f=None):
        """Where one arrow branches off another."""
        arrows.append(f'<circle cx="{x}" cy="{y}" r="3.5" fill="{ALL_FLOWS[f or F[0]][2]}"/>')

    def bot(*bs):
        """Bottom edge of the lowest of these boxes: the next row starts under it."""
        return max(b.y + b.h for b in bs)

    def mid(b):
        return b.y + b.h / 2

    def flow(x, y, w, name, when, steps, halo=PANEL):
        """A pipeline drawn as its steps, top to bottom, inside a dashed frame. A step (text, artifact) names what it
        hands to the next step, written on the arrow between them. A step (question, yes, no) is a check: the
        question, then both outcomes side by side, yes on the right; no = None when a failed check only stops the
        run (one outcome, under the question). A step ("||", [texts]) is tasks that run at the same time: one row
        each, no arrow between them, joined on the right into the next step. A step text "what|engine" gets the
        engine that runs it as a tag on its right (Airflow only starts the steps). Returns (frame, step boxes)."""
        def lines_of(text, rw, kind="job"):
            """The wrapped lines of a step, its engine tag and the tag's width."""
            text, _, tag = text.partition("|")
            pw = text_w(tag, 9.5, True) + 12 if tag else 0
            return wrap(text, rw - (44 if kind == "check" else 20) - (pw + 6 if tag else 0), 12.5), tag, pw

        def rect(rx, ry, rw, text, kind="job", h=None):
            lines, tag, pw = lines_of(text, rw, kind)
            h = h or 16 + 16 * len(lines)
            if kind == "check":                    # a decision: white, with pointed ends
                pts = [(rx + 14, ry), (rx + rw - 14, ry), (rx + rw, ry + h / 2), (rx + rw - 14, ry + h), (rx + 14, ry + h),
                       (rx, ry + h / 2)]
                body.append(f'<polygon points="{" ".join(f"{a},{b}" for a, b in pts)}" fill="#ffffff" stroke="{INK2}" '
                            f'stroke-width="1.3"/>')
            else:
                fill, stroke = KINDS[kind]
                body.append(f'<rect x="{rx}" y="{ry}" width="{rw}" height="{h}" rx="7" fill="{fill}" stroke="{stroke}" '
                            f'stroke-width="1.2"/>')
            y0 = ry + h / 2 - 8 * (len(lines) - 1) + 4.5
            tx = rx + (rw - (pw + 6 if tag else 0)) / 2  # the text centres in what the tag leaves
            body.extend(t(tx, y0 + 16 * i, ln, 12.5, INK) for i, ln in enumerate(lines))
            if tag:
                px = rx + rw - (18 if kind == "check" else 7) - pw
                body.extend([f'<rect x="{px}" y="{ry + h / 2 - 8}" width="{pw}" height="16" rx="8" fill="#ffffff" '
                             f'stroke="{INK2}" stroke-width="0.8"/>', t(px + pw / 2, ry + h / 2 + 3.5, tag, 9.5, INK2, mono=True)])
            return Box(rx, ry, rw, h, text)

        frame = len(body)
        body.append("")                            # the frame, drawn under its steps once its height is known
        cy, prev, out, plab = y + 46, None, [], ""
        for st in steps:
            if isinstance(st, tuple) and st[0] == "||":   # parallel tasks: rows, a join bus on the right
                bx, rows = x + w - 18, []
                body.append(t(x + 14, cy + 8, f"these {len(st[1])} run at the same time", 11.5, INK2, "start"))
                cy += 20
                for text in st[1]:
                    rows.append(rect(x + 12, cy, w - 46, text))
                    cy = bot(rows[-1]) + 12
                jy = cy + 4                         # the bus turns under the last row, to the middle
                for r in rows:
                    arrows.append(f'<path d="M{r.x + r.w},{mid(r)} H{bx}" fill="none" stroke="{ALL_FLOWS[F[0]][2]}" '
                                  f'stroke-width="2"/>')
                arrows.append(f'<path d="M{bx},{mid(rows[0])} V{jy} H{x + w / 2}" fill="none" '
                              f'stroke="{ALL_FLOWS[F[0]][2]}" stroke-width="2"/>')
                out += rows
                prev, cy, plab = Box(x + w / 2, jy, 0, 0, ""), jy + 26, ""
            elif isinstance(st, tuple) and len(st) > 2 and st[2] is None:   # a check whose failure only stops the run
                q, yes = st[:2]
                qb = rect(x + 12, cy, w - 24, q, "check")
                if prev:
                    link([(x + w / 2, bot(prev)), (x + w / 2, qb.y)], plab, halo=halo)
                yb = rect(x + 12, bot(qb) + 28, w - 24, yes)
                link([(x + w / 2, bot(qb)), (x + w / 2, yb.y)], "yes", halo=halo)
                out += [qb, yb]
                prev, cy, plab = yb, bot(yb) + 18, ""
            elif isinstance(st, tuple) and len(st) > 2:
                q, yes, no, *left = st              # a fourth item "yes left": yes goes on the side its arrow leaves
                qb = rect(x + 12, cy, w - 24, q, "check")
                if prev:
                    link([(x + w / 2, bot(prev)), (x + w / 2, qb.y)], plab, halo=halo)
                hw = (w - 36) / 2
                hh = 16 + 16 * max(len(lines_of(s, hw)[0]) for s in (yes, no))
                oy = bot(qb) + 28
                xn, xy = (x + 24 + hw, x + 12) if left else (x + 12, x + 24 + hw)
                nb = rect(xn, oy, hw, no, "job", hh)
                yb = rect(xy, oy, hw, yes, "job", hh)
                link([(nb.x + hw / 2, bot(qb)), (nb.x + hw / 2, oy)], "no", halo=halo)
                link([(yb.x + hw / 2, bot(qb)), (yb.x + hw / 2, oy)], "yes", halo=halo)
                out += [qb, nb, yb]
                prev, cy = nb, oy + hh + 18
            else:
                text, art = st if isinstance(st, tuple) else (st, "")
                b = rect(x + 12, cy, w - 24, text)
                if prev:
                    link([(x + w / 2, bot(prev)), (x + w / 2, b.y)], plab, halo=halo)
                out.append(b)
                prev, cy, plab = b, bot(b) + (34 if art else 18), art
        fb = Box(x, y, w, cy - 6 - y, name)
        body[frame] = pframe(x, y, w, fb.h, name, when)
        return fb, out

    # ================================================================ Layer 1: the data platform
    # Airflow on top: it starts each pipeline (dotted arrow, no data); a pipeline never starts another one
    air = box(410, 236, 1840, "Airflow", "the scheduler · a dotted arrow: Airflow starts that pipeline, no data moves", [
        "On a timer: DP1 every night; the drift pipeline and the DataHub metadata every day",
        "On new data: DP2, DP3 and the materialize pipeline start when the table they read gets a new checked version",
        "On a call: after a release, Jenkins starts the RAG data pipeline, or a DP2 silver run to check the batch code"],
        "svc", "airflow")

    # ---- batch: sources -> bronze -> silver -> gold, each pipeline as its steps
    R1 = 420
    dp1, s1 = flow(410, R1, 350, "DP1 · SOURCES TO BRONZE", "Airflow runs the steps · every night", [
        ("||", ["Copy the 6 core banking tables (JDBC)|Spark",
                "Load the night's export onto lakeFS branch dp1_<day>|Spark",
                "Load Flink's raw payment files|Spark"]),
        ("Great Expectations checks pass?|GE on Spark", "lakeFS merges the branch into main; Airflow publishes bronze",
         "Stop: the export stays on its branch, to inspect")])
    db = box(40, R1, 300, "Core banking database", "PostgreSQL · another department", [
        "customers, accounts, cards", "merchants, disputes", "province mapping (63 old, 34 new)"], "out", "postgres")
    exp = box(40, bot(db) + 14, 300, "Nightly export job", "core banking · 01:00 to 03:00",
              ["Writes the day's settled card transactions as Parquet files into an S3 bucket"], "out")
    bronze = box(830, R1, 310, "Bronze", "Delta tables on S3 · lakeFS", [
        "The sources as they arrived: core banking tables, transactions, raw payments",
        "A new export reaches readers only after its checks pass (lakeFS merge)"], "store", "lakefs", "NOVEL IDEA")
    dp2s, s2 = flow(1210, R1, 320, "DP2 · BRONZE TO SILVER", "Airflow runs the steps · on a new bronze version", [
        "Remove duplicate transactions|Spark", "Map 63 old provinces to 34 units|Spark", "Write the silver tables|Spark",
        ("Checks pass? (no row lost)|GE on Spark", "Publish a new silver version|Airflow", None)])
    silver = box(1600, R1, 250, "Silver", "Delta tables on S3", [
        "stg_transactions", "stg_customers", "cleaned: one row per payment"], "store", "deltalake")
    dp2g, s3 = flow(1920, R1, 330, "DP2 · SILVER TO GOLD", "Airflow runs the steps · on a new silver version", [
        ("||", ["Build the star schema|dbt on Spark", "Build the fraud labels from disputes|Spark"]),
        ("dbt tests + Great Expectations pass?|Spark", "Publish a new gold version|Airflow", None)])
    gold = box(2320, R1, 260, "Gold", "Delta tables on S3", [
        "fact_transaction", "dim_customer, dim_card", "dim_merchant, dim_date", "label_transaction (labels)"],
        "store", "deltalake")
    # the database faces the JDBC copy; the export job sits under it, its arrow turns up into the export load at x 360
    # (left of the raw files' lane at 375, so the two never cross)
    EY = exp.y + 24
    assert R1 + 20 < mid(s1[0]) < bot(db) - 8 and mid(s1[1]) < EY, "sources do not face DP1's loads"
    link([(340, mid(s1[0])), (422, mid(s1[0]))], "6 tables")
    link([(340, EY), (360, EY), (360, mid(s1[1])), (422, mid(s1[1]))], "export\nfiles", lx=388,
         ly=mid(s1[1]) - 21, anchor="middle")   # two short lines: the gap before DP1's frame is 50 px
    RY = mid(s2[0])                                # height of the arrows into each pipeline's first step
    yes1 = s1[-1]
    link([(yes1.x + yes1.w, mid(yes1)), (795, mid(yes1)), (795, RY), (830, RY)], "checked\ntables")
    link([(1140, RY), (1222, RY)], "bronze\ntables")
    link([(s2[-1].x + s2[-1].w, mid(s2[-1])), (1565, mid(s2[-1])), (1565, RY), (1600, RY)], "cleaned\ntables",
         lx=1573, ly=bot(silver) + 17)
    link([(1850, RY), (1932, RY)], "cleaned\ntables")
    link([(s3[-1].x + s3[-1].w, mid(s3[-1])), (2285, mid(s3[-1])), (2285, RY), (2320, RY)], "star schema,\nlabels",
         lx=2293, ly=bot(gold) + 34)
    # bronze's reference tables go straight to the gold pipeline and to DP3
    LR = bot(dp1, bronze, dp2s, silver, dp2g, gold) + 22
    link([bronze.bottom(0.5), (985, LR), (2085, LR), (2085, bot(dp2g))], "bronze cards, accounts, merchants, disputes",
         lx=1540, ly=LR - 7, anchor="middle")
    # Airflow starts the three pipelines under it; DP3 (under gold) starts the same way, on a new gold version
    AB = bot(air)
    link([(585, AB), (585, R1)], "every night", ctl=True)
    link([(1370, AB), (1370, R1)], "new bronze\nversion", ctl=True)
    link([(2085, AB), (2085, R1)], "new silver\nversion", ctl=True)
    # Trino under silver, with its analyst: one arrow per zone, each up into Trino's bottom, no junction
    trino = box(1580, bot(silver) + 44, 240, "Trino", "SQL on demand", [
        "Analysts query every zone", "Catalog: Hive Metastore"], "svc", "trino")
    TM = mid(trino)
    body += [icon("person", 1853, TM - 26, 30, INK2), t(1868, TM + 24, "Data analyst", 12.5, INK, "middle", 600)]
    link([(1850, TM - 11), (1820, TM - 11)], "SQL", lx=1835, ly=TM - 18, anchor="middle")
    LY = bot(dp1, dp2s, dp2g) - 40                 # lane of the zone arrows, under DP2s and DP2g, over the bronze lane
    assert bot(dp2s, dp2g, trino) + 15 < LY, "Trino's lanes run into DP2s, DP2g or Trino"
    link([bronze.bottom(0.8), (1078, LY), (1610, LY), (1610, bot(trino))], "bronze tables", lx=1344, ly=LY - 7,
         anchor="middle")
    link([(1700, bot(silver)), (1700, trino.y)], "silver tables", lx=1708, ly=bot(silver) + 26)
    link([gold.bottom(0.3), (2398, LY), (1790, LY), (1790, bot(trino))], "gold tables", lx=2240, ly=LY - 7,
         anchor="middle")
    arrows.append(f'<rect x="2076" y="{LY - 2}" width="18" height="4" fill="{PANEL}"/>')   # gap for the hop below
    # ---- streaming and the feature store
    RL = LR + 38                                   # lane of Flink's raw payment files, up to DP1
    R2 = max(RL + 52, bot(exp) + 30)
    app = box(40, R2, 300, "Mobile banking app", "outside", ["logins, password resets", "new payees, transfers"], "out", "phone")
    card = box(40, bot(app) + 14, 300, "Authorization switch", "outside · replay_scoring.py plays it", [
        "The bank's system: card payments arrive here from the card network",
        "Asks the Scoring API, waits for the decision", "Copies each payment to Kafka"], "out", "card")
    kafka = box(410, R2, 320, "Kafka", "the event stream", [
        "in: card_authorizations, app_events", "out: card_features_rt, card_device_seen", "out: ato_alerts_cep (alerts)"],
        "stream", "kafka")
    flink = box(800, R2, 370, "Flink streaming jobs", "PyFlink · results in seconds", [
        "Card features (DataStream, always on)", "First device per card (DataStream, always on)",
        "Raw payment files for DP1 (SQL, always on)", "Account takeover alerts (SQL patterns, replay)"], "job", "flink",
        "NOVEL IDEA: THE ALERTS")
    push_off = box(1240, R2, 300, "push-offline", "container · deployed by Jenkins", [
        "Feast push: live features to Delta, for training"], "svc", "python")
    push_on = box(1240, bot(push_off) + 15, 300, "push-online", "container · deployed by Jenkins", [
        "Feast push: live features to Redis, for scoring"], "svc", "python")
    online = box(1610, R2, 270, "Feast online store", "Redis", [
        "latest features of each card", "read in milliseconds"], "store", "redis")
    offline = box(1950, R2, 290, "Feast offline store", "Delta tables gold.feat_*", [
        "each card's features at any past moment", "for training"], "store", "feast")
    dp3, s4 = flow(2310, R2, 270, "DP3 · GOLD TO FEATURES", "Airflow runs the steps · on a new gold version", [
        "Per-card statistics over 30, 90 and 180 days, written to gold.feat_*|Spark",
        ("Checks pass?|GE on Spark", "Publish the new features|Airflow", None)])
    F[0] = "R"
    link([app.right(), (410, mid(app))], "app events")
    link([card.right(), (375, mid(card)), (375, R2 + 90), (410, R2 + 90)], "payments", lx=382, ly=card.y + 14)
    link([(730, R2 + 40), (800, R2 + 40)], "payments,\napp events")
    link([(800, R2 + 82), (730, R2 + 82)], "features,\nalerts", lx=765, ly=R2 + 100, anchor="middle")
    FT = bot(kafka, flink, push_on, online, offline, dp3, card) + 25     # lane of the feature topics
    link([kafka.bottom(0.5), (570, FT), (1205, FT), (1205, mid(push_off)), (1240, mid(push_off))], "live features (topics card_features_rt, card_device_seen)",
         lx=900, ly=FT - 7, anchor="middle")
    link([(1205, mid(push_on)), (1240, mid(push_on))])
    dot(1205, mid(push_on))
    link([(840, R2), (840, RL), (375, RL), (375, mid(s1[2])), (422, mid(s1[2]))], "raw payment files", lx=608, ly=RL - 7,
         anchor="middle")
    link([(1540, mid(push_on)), (1580, mid(push_on)), (1580, R2 + 72), (1610, R2 + 72)], "live\nfeatures", lx=1588,
         ly=mid(push_on) - 22)
    link([(1500, R2), (1500, R2 - 28), (2095, R2 - 28), (2095, R2)], "live features, kept for training", lx=1790,
         ly=R2 - 35, anchor="middle")
    F[0] = "B"
    link([(1950, R2 + 82), (1880, R2 + 82)], "newest\nrows", lx=1915, ly=R2 + 100, anchor="middle")
    link([(s4[0].x, mid(s4[0])), (2240, mid(s4[0]))], "daily\nfeatures")
    yes4 = s4[-1]                                  # publishing starts the materialize pipeline: offline store -> Redis
    assert mid(yes4) > bot(offline) + 50, "DP3's publish step does not sit under the offline store"
    link([(yes4.x, mid(yes4)), (2200, mid(yes4)), (2200, bot(offline))], "starts the\nmaterialize\npipeline\n(to Redis)", lx=2208,
         ly=(mid(yes4) + bot(offline)) / 2 - 17)
    link([(2480, bot(gold)), (2480, R2)], "fact_transaction")
    link([(2085, LR), (2350, LR), (2350, R2)], "bronze disputes", lx=2358, ly=(LR + R2) / 2 + 4)
    dot(2085, LR)
    hop = f'M2077,{LY} A8,8 0 0 1 2093,{LY}'       # the gold lane hops over the bronze branch into DP2g: no junction
    arrows.append(f'<path d="{hop}" fill="none" stroke="{PANEL}" stroke-width="6"/>'
                  f'<path d="{hop}" fill="none" stroke="{ALL_FLOWS["B"][2]}" stroke-width="2"/>')
    PB = FT + 30                                   # bottom of the Layer 1 panel
    assert mid(card) > R2 + 100 and AB + 40 < R1 and R2 + 90 < bot(kafka), "layer 1 rows overlap"
    assert bot(online) > R2 + 82 and bot(push_off) < bot(online) + 60, "feature store arrows miss their boxes"
    body.insert(0, panel(20, 190, W - 40, PB - 190, "LAYER 1 · DATA PLATFORM", PANEL, LINE,
                         note="turns the bank's raw data into clean tables and features: nightly batch on top, streaming below"))

    # ================================================================ Layer 2: the ML platform
    L = PB + 60                                    # top of layers 2 and 3
    H2 = KIND_PANEL
    scoring = box(1130, L + 60, 340, "Fraud Scoring API", "FastAPI · compose", [
        "Reads the card's online features", "Asks KServe for the fraud score", "Answers approve, step up or decline",
        "A/B test, once a person approves it: a fixed share of the cards goes to the challenger model",
        "Model down: a rule on the amount decides"], "svc", "fastapi")
    kserve = box(1540, L + 60, 300, "KServe model server", "Kubernetes", [
        "Serves the production XGBoost model", "During an A/B test, also the challenger",
        "Its logger copies every request and response as events"], "svc", "kserve")
    RB = bot(scoring, kserve) + 56
    log = box(1130, RB, 340, "Decision log", "Kafka topic fraud_predictions", [
        "Written by the Scoring API after every decision", "Holds the payment, its features, the score and the decision"],
        "store", "kafka")
    knative = box(1540, RB, 300, "Knative broker", "Knative Eventing · Kubernetes", [
        "Receives the logger's events",
        "A Trigger sends them to the drift detector; outlier and adversarial detectors can be added"],
        "svc", "kubernetes")
    drift = box(1540, bot(knative) + 45, 300, "Drift API", "FastAPI · the drift detector", [
        "Drift score (PSI) of every input over the last 15 minutes", "Compared with the model's training data"],
        "svc", "fastapi")
    F[0] = "S"
    link([(1470, L + 95), (1540, L + 95)], "20 features", halo=H2)
    link([(1540, L + 140), (1470, L + 140)], "fraud score", lx=1505, ly=L + 160, anchor="middle", halo=H2)
    link([(1300, bot(scoring)), (1300, log.y)], "every decision", halo=H2)
    F[0] = "M"
    link([(1690, bot(kserve)), (1690, knative.y)], "each request\nand response", halo=H2)
    link([(1690, bot(knative)), (1690, drift.y)], "the same\nevents", halo=H2)

    # training, on the right, under the features and the labels it reads
    F[0] = "T"
    train, s5 = flow(2258, L + 24, 334, "TRAINING PIPELINE", "Kubeflow Pipelines: each step a pod · on drift, or by hand", [
        ("Load the labels as of the day (Delta time travel)", "label rows"),
        ("Join them with the features, point in time (Feast)", "training set (Delta version)"),
        ("Check the data, then split it by time (60-day gaps)", "train, validation and test files"),
        ("Train a baseline and XGBoost on Spark", "model files"),
        ("Evaluate against the baseline and the production model", "passed: true or false"),
        "Register the model in MLflow, tagged passed or not",
        ("Better than production?", "Alias challenger: deployed with no traffic; a person approves the A/B test", None)], H2)
    FY = mid(s5[1])                                # where the offline features enter the training set
    F[0] = "M"
    driftp, s6 = flow(1920, FY + 40, 290, "DRIFT PIPELINE", "Airflow runs the steps · every day", [
        "Rebuild the last 7 days with point-in-time features|Feast", "Score them with the production model|Python",
        "Measure accuracy (PR-AUC, recall) and the drift score (PSI) of every input|Python",
        "Save the drift report in MLflow|Python",
        ("PSI above 0.25 or PR-AUC down by 0.05?|Airflow", "Start a training run|KFP API", None)], H2)
    mlflow = box(1920, bot(driftp) + 40, 290, "MLflow", "experiment tracking + model registry", [
        "Experiments: params, metrics and plots of every run", "Registry: model versions, aliases challenger and production",
        "The drift reports, one run per day"], "svc", "mlflow")
    F[0] = "T"
    link([(2095, bot(offline)), (2095, FY), (2270, FY)], "point-in-time features", lx=2103, ly=L - 26, halo=PAGE)
    link([(2095, FY), (2095, driftp.y)], halo=H2, f="M")
    dot(2095, FY)
    link([gold.right(0.6), (2612, gold.y + gold.h * 0.6), (2612, mid(s5[0])), (2580, mid(s5[0]))], "fraud labels", lx=2604,
         ly=L - 26, anchor="end", halo=PAGE)
    yes6, reg = s6[-1], s5[5]
    TY = FY + 30                                   # a drift run starts the training pipeline: into its frame
    assert TY < reg.y and mid(yes6) > mid(reg) and mlflow.y + 60 < bot(mlflow), "drift and training arrows cross"
    link([(yes6.x + yes6.w, mid(yes6)), (2228, mid(yes6)), (2228, TY), (2258, TY)], halo=H2, f="M")
    assert bot(train) < mlflow.y + 62, "no room under the training pipeline for the label of its MLflow arrow"
    link([(reg.x, mid(reg)), (2244, mid(reg)), (2244, mlflow.y + 60), (2210, mlflow.y + 60)], "model version,\nmetrics",
         lx=2252, ly=mlflow.y + 80, halo=H2)
    link([(s6[3].x, mid(s6[3])), (1905, mid(s6[3])), (1905, mlflow.y + 30), (1920, mlflow.y + 30)], halo=H2, f="M")
    NY = mlflow.y + 100                            # the notebook logs its runs into MLflow on this line
    body.append(actor(1125, NY - 20, "Data scientist", "person", w=140))   # the arrow leaves the glyph's middle
    link([(1228, NY), (1300, NY)], "explores,\ntrains")
    notebook = box(1300, NY - 30, 340, "JupyterLab notebook", "compose on the platform host · :8899", [
        "Where the data is explored", "Same steps, Feast features and Spark as the training pipeline",
        "Trained and registered the first model"], "svc", "jupyter")
    assert notebook.y > bot(log, drift) + 30 and NY < bot(mlflow) - 8, "notebook does not fit under the drift API"
    # the only reader of the decision log: a script run by hand after a replay (ml/tools/drift_by_day.py)
    report = box(1130, bot(log) + 50, 340, "Per-day drift report", "Python script · run by hand after a replay", [
        "Drift (PSI) and accuracy (PR-AUC) for each replayed day",
        "Catches a fall in accuracy the Drift API cannot see"], "job", "python")
    assert bot(report) + 16 < NY - 20, "the drift report does not fit above the data scientist and the notebook"
    link([(1300, bot(log)), (1300, report.y)], "logged decisions", halo=H2, f="M")
    link([(notebook.x + notebook.w, NY), (1920, NY)], "first model, runs", lx=1780, ly=NY - 7, anchor="middle", halo=H2)

    # ================================================================ Layer 3: the LLM and agent platform
    body.append(actor(40, L + 80, "Support staff", "people", w=130))
    chat = box(220, L + 60, 340, "Support chat", "web page behind NGINX Ingress", [
        "NGINX: HTTPS, login, rate limit", "The page sends the question to the gateway with its API key"], "gate", "nginx")
    models = box(640, L + 60, 400, "Model servers", "llm-d router + llama.cpp on the host GPU", [
        "llm-d picks a model server for each prompt", "Qwen3-8B writes the answers",
        "A Qwen3 embedding model turns text into vectors"], "svc", "llamacpp")
    gw = box(40, bot(chat, models) + 56, 1000, "agentgateway",
             "the first stop of every call: from the chat, between agents, to the tools and to the chat model", [
        "Checks the API key of every caller", "Decides which agents and tools that key may use",
        "Limits requests and tokens, masks personal data"], "gate", "agentgateway")
    RC = bot(gw) + 55
    coord = box(40, RC, 310, "Coordinator agent", "kagent", [
        "Splits the question", "Asks the two data agents", "Writes one answer",
        "System prompt: the Langfuse prompt named after the agent, the version labelled production"], "svc", "kagent")
    agents = box(375, RC, 310, "Data agents", "kagent", [
        "card-data-agent: looks up the card and the policy", "risk-agent: checks the fraud score against the policy",
        "System prompts: the Langfuse prompt named after each agent, the version labelled production"], "svc", "kagent")
    mcp = box(740, RC, 310, "MCP tool servers", "three FastAPI servers", [
        "card-data-mcp: a card's features, a policy chunk",
        "fraud-score-mcp: sends a what-if payment to the Fraud Scoring API",
        "policy-rag-mcp: searches the bank's policies"], "svc", "mcp")
    RR = bot(coord, agents, mcp) + 45
    F[0] = "B"
    rag, s7 = flow(375, RR, 310, "RAG DATA PIPELINE", "Airflow: each step a container · after each release", [
        "Read the bank's 5 policy documents from S3", "Cut them into chunks: one clause, section or FAQ each",
        "Turn each chunk into a vector (embedding model server)", "Write chunks + vectors: newest to pgvector, history to the lakehouse",
        "Check the chunks"])
    vec = box(740, RR, 310, "Vector store", "PostgreSQL + pgvector", [
        "Feast online store: newest chunks and their vectors", "Finds the chunks closest to a question in milliseconds (vector index)",
        "History: a Parquet table in the lakehouse (Feast offline store)"], "store", "postgres")
    F[0] = "A"
    link([(170, L + 110), (220, L + 110)], "question", lx=195, ly=L + 102, anchor="middle")
    link([(390, bot(chat)), (390, gw.y)], "question")
    link([(840, gw.y), (840, bot(models))], "prompts")
    link([(120, bot(gw)), (120, coord.y)], "question")
    link([(250, coord.y), (250, bot(gw))], "sub-tasks,\nprompts")
    link([(455, bot(gw)), (455, agents.y)], "sub-tasks")
    link([(590, agents.y), (590, bot(gw))], "tool calls,\nprompts")
    link([(895, bot(gw)), (895, mcp.y)], "tool calls")
    wr = s7[3]
    # the write step sits under the vector store: its arrow goes right, then up into the store's bottom
    assert mid(wr) > bot(vec) + 24, "the RAG write step does not sit under the vector store"
    link([(wr.x + wr.w, mid(wr)), (895, mid(wr)), (895, bot(vec))], "chunks + vectors", f="B", lx=790, ly=mid(wr) - 7,
         anchor="middle")
    link([(895, vec.y), (895, bot(mcp))], "policy chunks")
    # fraud-score's tool score_payment calls the Scoring API (POST /v1/score) and gets the score back
    link([(1050, mcp.y + 66), (1094, mcp.y + 66), (1094, L + 140), (1130, L + 140)], "score_payment calls\nPOST /v1/score",
         lx=1118, ly=bot(scoring) + 32, halo=H2)   # inside Layer 2, in the gap under the Scoring API

    # ================================================================ between the layers
    HP = PAGE
    # the payment and its decision as two arrows, one per direction (no line with a head at each end)
    link([card.bottom(0.6), (220, L - 30), (1400, L - 30), (1400, scoring.y)], "card payment", f="S", lx=640,
         ly=L - 37, anchor="middle", halo=HP)
    link([(1340, scoring.y), (1340, L - 10), (190, L - 10), card.bottom(0.5)], "approve, step up or decline", f="S",
         lx=640, ly=L - 17, anchor="middle", halo=HP)
    link([(1660, bot(online)), (1660, L - 42), (1078, L - 42), (1078, mcp.y + 28), (1050, mcp.y + 28)], "card features",
         lx=1380, ly=L - 49, anchor="middle", halo=HP)
    link([(1840, bot(online)), (1840, L + 30), (1440, L + 30), (1440, scoring.y)], "online features", f="S", lx=1848,
         ly=(bot(online) + L) / 2, anchor="start", halo=KIND_PANEL)

    # the models behind the aliases reach KServe as images, released by Jenkins (drawn on the CI/CD diagram)
    F[0] = "T"
    link([(1920, mlflow.y + 76), (1880, mlflow.y + 76), (1880, L + 102), (1840, L + 102)],
         "the models behind\nthe aliases, released\nby Jenkins", lx=1890, ly=L + 58, anchor="start", halo=H2)
    F[0] = "A"
    LB = max(bot(drift, mlflow, train, log, notebook), bot(rag, vec)) + 25 - L     # height of the layer 2 and 3 panels
    body.insert(1, panel(1110, L, W - 1130, LB, "LAYER 2 · ML PLATFORM", KIND_PANEL, "#b5c0d6"))
    body.insert(1, panel(20, L, 1040, LB, "LAYER 3 · LLM AND AGENT PLATFORM", PANEL, LINE,
                         note="agents answer support questions about a card"))

    # ================================================================ platform services, with the tools each one uses
    sy = L + LB + 30
    SW, SX = 340, 362

    def chip_rows(items, w):
        """Lay the tool logos out in rows that fit the box: [(x offset, row, logo, name)]."""
        out, cx, row = [], 0, 0
        for ico, name in items:
            cw = 26 + text_w(name, 11.5, False)
            if cx and cx + cw > w - 32:
                cx, row = 0, row + 1
            out.append((cx, row, ico, name))
            cx += cw + 18
        return out

    services = [
        ("Runtime", "Docker Compose + kind", ["Stores, APIs, Airflow, Jenkins: compose",
                                              "KServe, Kubeflow, Layer 3: Kubernetes"],
         [("docker", "Docker"), ("kubernetes", "Kubernetes"), ("helm", "Helm")]),
        ("Observability", "metrics, logs, traces", ["Dashboards for every layer", "Traces of every agent call"],
         [("prometheus", "Prometheus"), ("grafana", "Grafana"), ("victoria", "VictoriaLogs"), ("jaeger", "Jaeger"),
          ("opentelemetry", "OpenTelemetry")]),
        ("Data catalog", "lineage and data quality", ["Lineage of every table and job", "Results of every data check"],
         [("datahub", "DataHub")]),
        ("Storage", "files and databases", ["Lakehouse files and Delta tables (S3)", "PostgreSQL for the services' own data"],
         [("seaweedfs", "SeaweedFS"), ("postgres", "PostgreSQL")]),
        ("Secrets and edge", "keys and HTTPS", ["Secrets in Vault, synced to pods", "HTTPS for the public hosts"],
         [("vault", "Vault"), ("nginx", "NGINX Ingress"), ("letsencrypt", "Let's Encrypt")]),
        ("Cloud", "built by Terraform + Ansible", ["The model server on a VM and on AKS",
                                                   "Built for the IaC test, then destroyed"],
         [("azure", "Azure"), ("terraform", "Terraform"), ("ansible", "Ansible")]),
        ("CI/CD", "11 pipelines, one per unit", ["Test, version, deploy and check every unit",
                                                  "Drawn on the CI/CD diagram"],
         [("jenkins", "Jenkins"), ("github", "GHCR")])]
    rows = max(r for *_, items in services for _, r, _, _ in chip_rows(items, SW)) + 1
    sbox = []
    for i, (title, tech, bullets, items) in enumerate(services):
        b = box(40 + i * SX, sy + 44, SW, title, tech, bullets, "svc", pad=rows * 28 + 4)
        for dx, r, ico, name in chip_rows(items, SW):
            cy0 = b.y + b.h - rows * 28 - 6 + r * 28
            body += [icon(ico, b.x + 16 + dx, cy0, 20), t(b.x + 16 + dx + 26, cy0 + 15, name, 11.5, INK2, "start")]
        sbox.append(b)
    SH = bot(*sbox) + 20 - sy
    body.insert(1, panel(20, sy, W - 40, SH, "SHARED SERVICES", PANEL, LINE, note="used by every layer, with the tools"))

    # ================================================================ the flows: what the colour of an arrow means
    ly = sy + SH + 24
    body.append(panel(20, ly, W - 40, 124, "FLOWS", PANEL, LINE, note="the colour of an arrow says which path it belongs to"))
    for i, (k, (name, when)) in enumerate(OVERVIEW_FLOWS.items()):
        fx, fy = 44 + (i % 3) * 860, ly + 60 + (i // 3) * 36
        c = ALL_FLOWS[k][2]
        body += [f'<path d="M{fx},{fy} h48" stroke="{c}" stroke-width="2.4"{DASH.get(k, "")} marker-end="url(#ah-{k})"/>',
                 t(fx + 62, fy + 4.5, name, 13.5, INK, "start", 600),
                 t(fx + 62 + text_w(name, 13.5, False) + 12, fy + 4.5, when, 12, INK2, "start")]
    H = ly + 124 + 30

    # ================================================================ title, what it does, the key
    head = [t(40, 60, "Card fraud platform: overview", 28, INK, "start", 700),
            t(40, 94, "The bank's data comes in on the left. Layer 1 turns it into clean tables and features. In Layer 2 a model scores "
                      "every card payment in real time", 15, INK2, "start"),
            t(40, 116, "and is retrained when the data drifts. In Layer 3 agents answer the support staff's questions about a card, "
                       "with the same data.", 15, INK2, "start")]
    kx = 40
    for kind, label in (("out", "outside system"), ("job", "pipeline or job"), ("svc", "service"), ("store", "store"),
                        ("stream", "event stream"), ("gate", "entry or gateway")):
        fill, stroke = KINDS[kind]
        head += [f'<rect x="{kx}" y="142" width="24" height="16" rx="4" fill="{fill}" stroke="{stroke}" stroke-width="1.4"/>',
                 t(kx + 32, 155, label, 12, INK2, "start")]
        kx += 32 + text_w(label, 12, False) + 28
    head += [f'<polygon points="{kx + 6},142 {kx + 26},142 {kx + 32},150 {kx + 26},158 {kx + 6},158 {kx},150" fill="#ffffff" '
             f'stroke="{INK2}" stroke-width="1.3"/>', t(kx + 40, 155, "check in a pipeline", 12, INK2, "start")]
    kx += 40 + text_w("check in a pipeline", 12, False) + 28
    head += [novel_tag(kx + 82, 142), t(kx + 90, 155, "novel idea", 12, INK2, "start")]
    kx += 90 + text_w("novel idea", 12, False) + 28
    head += [f'<path d="M{kx},150 h30" stroke="{ALL_FLOWS["S"][2]}" stroke-width="2" marker-end="url(#ah-S)"/>',
             t(kx + 40, 155, "from sender to receiver: the words say what moves, the colour which flow", 12,
               INK2, "start")]
    feat, _ = info(1740, 24, 860, "What the platform does", "", [
        "Scores every card payment for fraud while the customer waits: approve, step up or decline.",
        "Builds what the model needs from the bank's data: tables every night, features within seconds.",
        "Trains, registers and serves the model; watches the data for drift and retrains.",
        "Lets support staff ask an agent about a card; every agent call passes one gateway."], "note")
    out = head + [feat] + body + arrows
    return svg_doc(W, H, "\n".join(out), "Card fraud platform: overview",
                   "Layer 1: DP1 copies the core banking tables, the nightly export and Flink's raw payment files into bronze, "
                   "the export through a lakeFS branch merged only after the checks pass; the DP2 silver and gold pipelines "
                   "and DP3 each read one zone and write the next; Flink jobs turn Kafka events into live features and "
                   "alerts; Feast keeps the features offline for training and online for scoring. Layer 2: the Scoring API "
                   "decides each payment with the KServe model and logs the decision; KServe's logger feeds the Drift API "
                   "through a Knative broker; the training pipeline registers every model in MLflow. Layer 3: the support "
                   "chat, the agents, the MCP tool servers and the model servers all meet at agentgateway.")


# ---------------------------------------------------------------- CI/CD
def cicd():
    """CI/CD on its own page, so the overview keeps to the data. Top: the path every release shares. Then the two
    releases a reviewer asks about first, as flows: the model release (KServe) and the agent release (system prompts).
    Last: one row per other pipeline, grouped by layer."""
    W = 2400
    body, arrows = [], []
    T = ALL_FLOWS["T"][2]

    def box(x, y, w, title, tech="", bullets=(), kind="svc", ico="", novel=""):
        svg, b = info(x, y, w, title, tech, bullets, kind, ico, novel)
        body.append(svg)
        return b

    def link(pts, label="", lx=None, ly=None, anchor="middle", halo=PANEL):
        """A release arrow; without lx/ly the label sits on the longest segment."""
        if label and lx is None:
            a, b = max(zip(pts, pts[1:]), key=lambda g: abs(g[1][0] - g[0][0]) + abs(g[1][1] - g[0][1]))
            n = label.count("\n")
            if a[1] == b[1]:
                lx, ly, anchor = (a[0] + b[0]) / 2, a[1] - 7 - 14 * n, "middle"
            else:
                lx, ly, anchor = a[0] + 8, (a[1] + b[1]) / 2 + 4 - 7 * n, "start"
        d = path_d(pts)
        arrows.append("\n".join([
            f'<path d="{d}" fill="none" stroke="{halo}" stroke-width="7"/>',
            f'<path d="{d}" fill="none" stroke="{T}" stroke-width="2"{DASH["T"]} marker-end="url(#ah-T)"/>',
            *[t(lx, ly + 14 * i, ln, 11.5, INK, anchor, halo=halo) for i, ln in enumerate(label.split("\n")) if ln]]))

    def bot(*bs):
        return max(b.y + b.h for b in bs)

    def mid(b):
        return b.y + b.h / 2

    def step(x, y, w, h, text, check=False, ico=""):
        """One step of a release: a green box, a check (white, pointed ends), or with ico a gate (purple, its tool's
        logo on the left)."""
        if check:
            pts = [(x + 14, y), (x + w - 14, y), (x + w, y + h / 2), (x + w - 14, y + h), (x + 14, y + h), (x, y + h / 2)]
            body.append(f'<polygon points="{" ".join(f"{a},{b}" for a, b in pts)}" fill="#ffffff" stroke="{INK2}" '
                        f'stroke-width="1.3"/>')
        else:
            fill, stroke = KINDS["gate" if ico else "job"]
            body.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="{fill}" stroke="{stroke}" '
                        f'stroke-width="1.2"/>')
        pad = 34 if ico else 0                     # room for the logo
        if ico:
            body.append(icon(ico, x + 10, y + h / 2 - 11, 22))
        ls = wrap(text, w - (44 if check else 20) - pad, 12.5)
        body.extend(t(x + (w + pad) / 2, y + h / 2 - 8 * (len(ls) - 1) + 4.5 + 16 * j, ln, 12.5, INK)
                    for j, ln in enumerate(ls))
        return Box(x, y, w, h, text)

    def release(y, title, note, texts, check, no, novel="", after="", novel_at=None, gate=None):
        """One release as its steps, left to right, in a panel: texts[check] is the check; 'yes' goes on to the
        right, 'no' down to the box under it. `after` is one line under the steps. Returns the panel's bottom."""
        x0, w, gap = 40, W - 80, 46
        sy = y + 58 + (22 if novel else 0)
        sw = (w - (len(texts) - 1) * gap) / len(texts)
        gi, gico = gate or (None, "")              # gate = (index, logo): a step drawn as a gate, with its tool
        sh = 16 + 16 * max(len(wrap(x, sw - (44 if i == check else 20) - (34 if i == gi else 0), 12.5))
                           for i, x in enumerate(texts))
        st = [step(x0 + i * (sw + gap), sy, sw, sh, x, i == check, gico if i == gi else "") for i, x in enumerate(texts)]
        for a, b in zip(st, st[1:]):
            link([(a.x + a.w, mid(a)), (b.x, mid(b))], "yes" if a is st[check] else "")
        c = st[check]
        if novel:
            n = st[check if novel_at is None else novel_at]
            body.append(novel_tag(n.x + n.w - 4, sy - 22, novel))
        nh = 16 + 16 * len(wrap(no, sw - 20, 12.5))
        nb = step(c.x, bot(c) + 34, sw, nh, no)
        link([(c.x + sw / 2, bot(c)), (c.x + sw / 2, nb.y)], "no", lx=c.x + sw / 2 + 8, ly=bot(c) + 21, anchor="start")
        yb = bot(nb)
        if after:
            body.append(t(x0, yb - 6, after, 12.5, INK2, "start"))
        body.insert(0, panel(20, y, W - 40, yb + 22 - y, title, PANEL, LINE, note=note))
        return yb + 22

    # ---- title and key
    head = [t(40, 60, "Card fraud platform: CI/CD", 28, INK, "start", 700),
            t(40, 94, "Jenkins runs 11 pipelines, one per deployable unit. Each one tests a change, gives it a version, "
                      "deploys it, checks the running unit, and brings back the version", 15, INK2, "start"),
            t(40, 116, "before when the check fails. The overview shows what each unit does; this page shows how a new "
                       "version of it reaches the platform.", 15, INK2, "start")]
    kx = 40
    for kind, label in (("out", "outside system"), ("job", "pipeline or step"), ("svc", "service"),
                        ("store", "registry"), ("gate", "check after the deploy")):
        fill, stroke = KINDS[kind]
        head += [f'<rect x="{kx}" y="142" width="24" height="16" rx="4" fill="{fill}" stroke="{stroke}" stroke-width="1.4"/>',
                 t(kx + 32, 155, label, 12, INK2, "start")]
        kx += 32 + text_w(label, 12, False) + 28
    head += [f'<polygon points="{kx + 6},142 {kx + 26},142 {kx + 32},150 {kx + 26},158 {kx + 6},158 {kx},150" fill="#ffffff" '
             f'stroke="{INK2}" stroke-width="1.3"/>', t(kx + 40, 155, "check in a release", 12, INK2, "start")]
    kx += 40 + text_w("check in a release", 12, False) + 28
    head += [novel_tag(kx + 82, 142), t(kx + 90, 155, "novel idea", 12, INK2, "start")]
    kx += 90 + text_w("novel idea", 12, False) + 28
    head += [f'<path d="M{kx},150 h30" stroke="{T}" stroke-width="2"{DASH["T"]} marker-end="url(#ah-T)"/>',
             t(kx + 40, 155, "from one step of a release to the next", 12, INK2, "start")]

    # ---- 1. the path every release shares
    P1 = 190
    TY = P1 + 50
    chain = []
    for i, (title, tech, bullets, kind, ico) in enumerate([
            ("GitHub", "the code · branch main", ["Jenkins polls main every 5 minutes"], "out", "github"),
            ("Jenkins", "11 pipelines, one per unit", [
                "Runs only the pipelines whose files changed",
                "Tests the change and builds it with a version",
                "Signs the tool server and model images (cosign)"], "job", "jenkins"),
            ("GHCR", "GitHub Container Registry, ghcr.io", [
                "Every image, tagged with its version", "The signatures next to the images",
                "Code images public; the model image private"], "store", "github"),
            ("Deploy", "the tool each unit needs", [
                "kind: Helm and kubectl, for the agents, the tool servers and the model server",
                "compose: the APIs and the push jobs", "Airflow, Kubeflow, Flink: data, training and streaming"],
             "job", "helm"),
            ("Check", "after every deploy", [
                "A smoke test, a data check or the eval gate",
                "If it fails, the version before comes back"], "gate", "jenkins"),
            ("Live, with its version", "versioning and rollback", [
                "The running unit shows its version: image tag, annotation, Airflow Variable or job name",
                "MLflow, Langfuse, GHCR and Helm keep every version"], "svc", "kubernetes")]):
        chain.append(box(170 + i * 370, TY, 300, title, tech, bullets, kind, ico))
    jen = chain[1]
    body.append(actor(30, TY + 16, "Developer", "person", w=110))
    CA = TY + 40
    link([(140, CA), (170, CA)], "git\npush", lx=155, ly=CA - 22)
    for a, b, lab in zip(chain, chain[1:], ("new\ncommits", "images", "the\nimage", "new\nrelease", "passed")):
        link([(a.x + a.w, CA), (b.x, CA)], lab)
    TR = bot(*chain) + 60                          # the two other things that start a release, under Jenkins
    lf = box(330, TR, 300, "Langfuse Cloud EU", "SaaS · prompts and eval datasets", [
        "One prompt per agent, named after it; one version is labelled production",
        "Eval datasets and the score of each run"], "out", "langfuse")
    ml = box(700, TR, 300, "MLflow", "model registry", [
        "Aliases production and challenger point at model versions"], "svc", "mlflow")
    link([(590, TR), (590, bot(jen))], "new production prompt\n(checked every 15 minutes)", lx=582,
         ly=(TR + bot(jen)) / 2 - 3, anchor="end")
    link([(790, TR), (790, bot(jen))], "moved model alias\n(checked every 15 minutes)", lx=798, ly=(TR + bot(jen)) / 2 - 3,
         anchor="start")
    y = bot(lf, ml) + 24
    body.insert(0, panel(20, P1, W - 40, y - P1, "THE PATH EVERY RELEASE SHARES", PANEL, LINE,
                         note="a commit, a new production prompt or a moved model alias starts it"))

    # ---- 2. every pipeline as its steps, by layer: test, build with a version, deploy, check; no = roll back
    image = "Build the image, tagged with the commit id; push it to GHCR"
    for title, note, texts, no, *extra in [
            ("LAYER 1 · BATCH PIPELINES · Jenkins pipeline batch-pipelines",
             "starts on a commit in the DAGs, the Spark jobs, dbt or the ml code they run", [
                 "Lint, unit tests; every DAG imports in the Airflow image; dbt parse",
                 "Copy the commit's DAGs, Spark jobs and dbt into a folder named after the commit id",
                 "Move one link to that folder: Airflow and Spark switch to it together",
                 "Airflow parses it, and a DP2 silver run passes its checks?",
                 "Live. The Airflow Variable batch_release and a tag on every DAG name the commit"],
             "Move the link back to the folder before"),
            ("LAYER 1 · FLINK JOBS · Jenkins pipeline flink-jobs",
             "starts on a commit in the Flink jobs; the jobs run by hand only get the first step", [
                 "Lint; every job imports in the Flink image",
                 "Copy the commit's job files into Flink",
                 "Stop each always-on job with a savepoint; start the new code from it",
                 "It restores the savepoint and finishes a checkpoint within 3 minutes?",
                 "Live. The job's name in the Flink UI carries the commit id"],
             "Cancel it; start the old code again from the same savepoint"),
            ("LAYER 1 · PUSH JOBS · Jenkins pipeline stream-feature-jobs",
             "starts on a commit in the push jobs, the ML library or the Feast repo", [
                 "Unit tests in the ml image", image,
                 "Recreate push-online and push-offline on the new image",
                 "A test message reaches both feature stores?",
                 "Live. The containers run the image tagged with the commit id"],
             "Recreate both on the image they ran before"),
            ("LAYER 2 · MODEL RELEASE · Jenkins pipeline fraud-predictor",
             "starts when an MLflow alias moves (checked every 15 minutes), or on a commit to the model server", [
                 "Fetch the model behind the alias; test that it loads, scores and is that version",
                 "Build the image, tagged with the model version, and sign it",
                 "Apply the InferenceService: card-fraud for production, card-fraud-challenger for challenger",
                 "It answers with that version?",
                 "Live. A challenger gets traffic (10% of the cards) only after a person approves the A/B test in Jenkins"],
             "Re-apply the image it ran before",
             "", "Promote or roll back a model: move the alias production in MLflow; the next check releases that "
                 "version."),
            ("LAYER 2 · TRAINING PIPELINE · Jenkins pipeline training-pipeline",
             "starts on a commit in the training pipeline, the ML library or the Feast repo", [
                 "Unit tests: the registry, the training, the image the pipeline is pinned to", image,
                 "Compile the pipeline on that image; upload it to Kubeflow as a version named after the commit id",
                 "Every step of the uploaded version runs that image?",
                 "Live. The Airflow Variable train_release points the drift pipeline at the new version"],
             "The Variable stays on the version before"),
            ("LAYER 2 · FRAUD SCORING API · Jenkins pipeline fraud-scoring-api",
             "starts on a commit in the Fraud Scoring API", [
                 "Unit tests (90% coverage) and mutation tests", image,
                 "Recreate the container on the new image; the A/B split stays as it was",
                 "It is ready and scores a test payment with the model?",
                 "Live. The container runs the image tagged with the commit id"],
             "Recreate it on the image it ran before"),
            ("LAYER 2 · DRIFT API · Jenkins pipeline fraud-drift-api",
             "starts on a commit in the Drift API", [
                 "Unit tests (90% coverage) and mutation tests", image,
                 "Recreate the container on the new image",
                 "It is ready and names the model version it compares with?",
                 "Live. The container runs the image tagged with the commit id"],
             "Recreate it on the image it ran before"),
            ("LAYER 3 · AGENT RELEASE · Jenkins pipelines card-support-data-agents, then card-support-coordinator",
             "starts on a new production prompt in Langfuse (checked every 15 minutes), a commit, or a tool server "
             "release", [
                 "Read each agent's production prompt from Langfuse",
                 "Helm writes the prompt and its version into the Agent, in one write",
                 "Smoke test: one question to each agent, through the gateway",
                 "Eval gate (data agents): risk-agent right on 0.6 of 20 questions?",
                 "Live. The Agent's annotation names its prompt version; the coordinator release runs next"],
             "helm rollback to the version before",
             "NOVEL IDEA: EVAL GATE", "The coordinator pipeline has the same steps without the eval gate."),
            ("LAYER 3 · MCP TOOL SERVERS · Jenkins pipeline card-support-mcp",
             "starts on a commit in the tool servers", [
                 "Unit tests (90% coverage) and mutation tests",
                 "Build the image, tagged with the commit id; push it to GHCR and sign it (cosign)",
                 "Helm upgrades each tool server",
                 "Kyverno: a pod starts only if Jenkins signed its image",
                 "The card API answers, and kagent accepts each server's tools?",
                 "Live. Then the data agents' release runs, so the agents meet the new tools"],
             "helm rollback of every server it upgraded (also when Kyverno stopped a pod)",
             {"check": 4, "gate": (3, "kyverno"), "novel": "NOVEL IDEA: SIGNED IMAGES ONLY", "novel_at": 3}),
            ("LAYER 3 · RAG DATA PIPELINE · Jenkins pipeline rag-pipeline",
             "starts on a commit in the RAG steps, the DAG or the policy documents", [
                 "Unit tests of the steps; the DAG imports in the Airflow image", image,
                 "Point the Airflow Variable rag_image at the new image; run the DAG",
                 "The checks on the written chunks pass?",
                 "Live. rag_image names the image tagged with the commit id"],
             "Set rag_image back to the image before")]:
        o = extra[0] if extra and isinstance(extra[0], dict) else dict(zip(("novel", "after"), extra))
        y = release(y + 24, title, note, texts, o.get("check", 3), no, novel=o.get("novel", ""), after=o.get("after", ""),
                    novel_at=o.get("novel_at"), gate=o.get("gate"))
    H = y + 30
    return svg_doc(W, H, "\n".join(head + body + arrows), "Card fraud platform: CI/CD",
                   "Jenkins runs 11 pipelines, one per deployable unit. A commit, a new production prompt in Langfuse or a "
                   "moved alias in MLflow starts a release: GitHub, Jenkins, GitHub Container Registry, deploy, check, "
                   "live with its version; a failed check brings back the version before. Then each of the 11 "
                   "pipelines, by layer, as its steps: test, build with a version, deploy, check, and the rollback.")


# ---------------------------------------------------------------- output
def render(name: str, svg: str, prefix: str = "platform-") -> None:
    """Write the SVG and, when Chrome or Edge is installed, a 2x PNG screenshot of it next to it."""
    path = OUT / f"{prefix}{name}.svg"
    path.write_text(svg, encoding="utf-8", newline="\n")   # LF on Windows too (.gitattributes)
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
    only = set(sys.argv[1:]) or {"system-layers-1-2", "system-layer-3", "deployment", "overview", "cicd"}
    tables = []
    for name in ("system-layers-1-2", "system-layer-3", "deployment", "overview", "cicd"):   # one Chrome at a time
        if name in only:
            out = globals()[name.replace("-", "_")]()
            svg, arrows = out if isinstance(out, tuple) else (out, None)
            render(name, svg)
            if arrows:
                tables.append(table(arrows))
    print("\n\n".join(tables))
