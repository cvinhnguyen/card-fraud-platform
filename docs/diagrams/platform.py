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
        "pushon": Box(c(2), r(3), BW, BH, "push-online", "ml", "python",
                      note="always-on container:\nlive features to Redis"),   # the Python logo read as "a script"
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
                       "python", note="always-on container:\nlive features to Delta"),
        "trino": Box(c(3), r(5), BW, BH, "Trino", "batch", "trino", note="SQL queries for analysts"),
        "spark": Box(c(4), r(5), BW, BH, "Spark", "batch", "spark", note="runs the batch jobs"),
        "airflow": Box(c(5), r(5), BW, BH, "Airflow", "orch",
                       "airflow", note="schedules and runs the pipelines"),
        "jupyter": Box(c(6), r(5), BW, BH, "Jupyter", "ml", "jupyter", note="notebook to explore data and train"),
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
              desc="The card network's switch sends each payment authorization to POST /v1/score (card, amount, channel, device)"),
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
              desc="Airflow submits the Spark jobs of DP1, the two DP2 pipelines and DP3 (a call, no data); each pipeline checks "
                   "what it wrote with Great Expectations, then announces the new version of its zone, which starts the next one"),
        Arrow("B", 2, "source tables (JDBC)", [B["pg"].bottom(), B["spark"].top()], side="l",
              desc="DP1 copies the core banking tables (customers, accounts, cards, merchants, disputes)"),
        Arrow("B", 3, "export files", [(c(4) + 32, s3y), B["spark"].bottom(32 / BW)], side="l",
              desc="DP1 loads the new daily export folders from s3://core-banking-export"),
        Arrow("B", 4, "writes each zone", [B["spark"].bottom(138 / BW), (c(4) + 138, s3y)],
              desc="DP1 writes bronze, the DP2 silver pipeline silver, the DP2 gold pipeline gold (dbt) and the labels, DP3 the "
                   "feat_* tables, all Delta in s3://lakehouse; "
                   "bronze.raw_transactions goes through lakeFS: a branch per DP1 run, merged into main after the checks"),
        Arrow("B", 5, "Delta tables", [(c(3) + 85, s3y), B["trino"].bottom()],
              desc="Trino reads the Delta tables of any zone for analysts; the Hive Metastore tells it where each table "
                   "lives (bronze.raw_transactions: catalog lakefs, branch main)"),
        Arrow("B", 6, "features", [(c(5) + 40, s3y), B["airflow"].bottom(40 / BW)], side="r",
              desc="The incremental materialize pipeline (DAG feast_materialize, on DP3's new features) reads feat_card_txn_stats_daily through "
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
              desc="Steps 5 to 7 (train, evaluate, register) log params, data versions, metrics and the model; every model is "
                   "kept as a version, and gets alias challenger only when it passes the evaluation gate"),
        Arrow("T", 6, "production model", [B["mlflow"].top(), B["registry"].bottom()],
              desc="ML engineer: fetch_model.py --alias production, docker build + push of fraud/predictor:<v>"),
        Arrow("T", 7, "image pull", [B["registry"].right(), (B["kserve"].x + 260, B["registry"].y + 42),
                                     B["kserve"].bottom(260 / 300)], 0.4, "u",
              desc="kubectl apply of the InferenceService; the node pulls the image from the registry"),
        Arrow("M", 1, "calls, as events", [B["kserve"].bottom(110 / 300), B["broker"].top()],
              desc="KServe's payload logger (the agent sidecar, mode all) posts every request and its response as CloudEvents "
                   "to the broker serving/default"),
        Arrow("M", 2, "inference events", [B["broker"].left(), (c(4) + 130, B["broker"].y + 42), B["drift"].top(130 / BW)], 0.3, "u",
              desc="Trigger drift-detector delivers them to POST /v1/inference-events: request events count the features, "
                   "response events the score. Outlier and adversarial detectors would subscribe the same way"),
        Arrow("M", 3, "per-minute counts", [B["drift"].top(0.3), (c(4) + 51, redis.y + 62), redis.right(62 / BH)], 0.7, "d",
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
        "Layer 3 (agents, gateway, tool servers) is on its own diagram; its tool servers read Redis and call the Scoring API (A11, A14)."]))
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
        "ui": Box(k2, q(1), BW, BH, "support-chat", "ns kagent", note="chat page: sends the question to the gateway"),
        "kagent": Box(k2, q(2), BW, BH, "kagent", "ns kagent", "kagent", note="runs the agents; admin chat UI", soft=True),
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
        "rag": Box(k5, q(2), BW, BH, "policy-rag-mcp", "ns card-support", "mcp", note="tool: searches the bank's policies"),
        "cdata": Box(k5, q(3), BW, BH, "card-data-mcp", "ns card-support",
                     "mcp", note="tool: a card's data, policy text"),
        "fscore": Box(k5, q(4), BW, BH, "fraud-score-mcp", "ns card-support",
                      "mcp", note="tool: fraud score of a payment"),
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
              desc="A question in the support chat (https://support.<domain>, basic auth and rate limit at NGINX)"),
        Arrow("A", 2, "question", [b["nginx"].right(), b["ui"].left()],
              desc="NGINX passes it to support-chat, which serves the chat page (streamed answers, no buffering)"),
        Arrow("A", 3, "question", [b["ui"].right(), (k3 - 34, q(1) + 42), (k3 - 34, q(1) + BH + 36), (k4, q(1) + BH + 36)], 0.8,
              desc="support-chat forwards the question to the gateway route /a2a/coordinator and adds the support-app API key, "
                   "so the key never reaches the browser. The gateway is the first hop of every question"),
        Arrow("A", 4, "question", [(k4, q(1) + 26), b["coord"].right(26 / BH)],
              desc="Route a2a-coordinator: the gateway checks the key (only support-app may call it), applies the rate limit "
                   "and passes the question to the coordinator over A2A"),
        Arrow("A", 5, "A2A + LLM", [b["coord"].right(60 / BH), (k4, q(1) + 60)], side="d",
              desc="The coordinator's A2A calls to the two agents and its LLM calls, all through the gateway "
                   "(kagent proxy.url)"),
        Arrow("A", 6, "sub-task", [(k4, q(2) + 28), b["card"].right(28 / BH)],
              desc="Route agents-internal: a sub-task for card-data-agent; only the coordinator's key may call it"),
        Arrow("A", 7, "sub-task", [(k4, q(3) + 28), b["risk"].right(28 / BH)],
              desc="... and for risk-agent (risk-agent-b, the B side of its model-config A/B test, sits behind "
                   "the same route)"),
        Arrow("A", 8, "MCP + LLM", [b["card"].right(60 / BH), (k4, q(2) + 60)], side="d",
              desc="card-data-agent's tool calls (MCP, its own API key) and LLM calls through the gateway"),
        Arrow("A", 9, "MCP + LLM", [b["risk"].right(60 / BH), (k4, q(3) + 60)], side="d",
              desc="risk-agent's tool calls and LLM calls through the gateway"),
        Arrow("A", 10, "MCP", [(k4 + BW, q(2) + 108), (mx, q(2) + 108)],
              desc="Routes /mcp/card-data, /mcp/fraud-score, /mcp/policy-rag: only the key of each server's agent "
                   "sees its tools (300 calls/min per server)"),
        Arrow("A", 11, "features", [b["redis"].left(20 / BH), (k5 + BW + 20, b["redis"].y + 20),
                                    (k5 + BW + 20, q(3) + 64), (k5 + BW, q(3) + 64)], 0.6, "r",
              desc="card-data-mcp, get_card_data: the card's online features from Redis through Feast (project "
                   "card_fraud)"),
        Arrow("A", 12, "chunk", [b["vdb"].left(), (k5 + BW, b["vdb"].y + 42)],
              desc="card-data-mcp, get_policy_chunk: one policy chunk by id from pgvector (Feast project card_support_kb)"),
        Arrow("A", 13, "vector", [b["embed"].left(), (k5 + BW, b["embed"].y + 42)],
              desc="policy-rag-mcp, search_policy: the question's vector from Qwen3-Embedding-0.6B"),
        Arrow("A", 14, "score", [b["scoring"].left(), (k5 + 120, b["scoring"].y + 42), b["fscore"].bottom(120 / BW)], 0.3,
              desc="fraud-score-mcp, score_payment: the Layer 2 Scoring API answers with the fraud score and its decision"),
        Arrow("A", 15, "prompts", [(k4 + BW, q(1) + 42), b["llmd"].left()],
              desc="Route /v1: the agent's API key is checked, request and token limits applied, personal data masked, then the "
                   "InferencePool qwen3-8b"),
        Arrow("A", 16, "prompt", [b["llmd"].right(), b["chat"].left()],
              desc="The endpoint picker chooses a pod; its bridge forwards the prompt to llama.cpp on the host GPU"),
        Arrow("A", 17, "nearest chunks", [b["vdb"].left(20 / BH), (k5 + BW + 45, b["vdb"].y + 20),
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
        "Numbers give the order inside each flow: A1 to A17 follow one question from support staff to the tools and the model.",
        "White box = one pod or container. Tag top left = where it runs: namespace (ns), compose profile, host or internet.",
        "A2A = an agent asks another agent; MCP = an agent calls a tool server; LLM = a prompt to the model.",
        "Each reply comes back on the same call and is not drawn.",
        "Dotted frames: the three agents (A6-A9, C5, O5, O6 apply to each) and the three MCP servers",
        "(one image, one Helm release each; A10, C4, O2 apply to each). Dashed boxes: support services.",
        "NGINX also serves registry, grafana, logs, traces and the Web API (api.<domain>: card data",
        "and policy chunks by id, from card-data-mcp, over HTTPS).",
        "Redis and the Scoring API are Layer 2 units, drawn again here because the tool servers call them (A11, A14)."]))
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
        ("card-fraud-predictor", "model + KServe agent (logger)", ":31080", "kubernetes")], w=kgw)
    parts.append(g)
    g, kn = group(kgx[1], k1 + 14, "kserve", "0.21", [
        ("kserve-controller", "InferenceService", "", "kubernetes")], w=kgw)
    parts.append(g)
    g, _ = group(kgx[1], kn + 14, "knative-eventing", "1.23.1", [
        ("mt-broker ingress, filter", "Broker serving/default", "", "kubernetes"),
        ("eventing + imc", "controllers, in-memory channel", "", "kubernetes")], w=kgw)
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
        ("card-data-mcp", "2 pods, signed image", "", "mcp"),
        ("fraud-score-mcp", "2 pods, signed image", "", "mcp"),
        ("policy-rag-mcp", "2 pods, signed image", "", "mcp")], w=kgw)
    parts.append(g)
    g, nb = group(kgx[2], a2 + 14, "nginx-ingress", "", [
        ("nginx-ingress", "F5 NIC 5.6.3", ":80 :443", "kubernetes")], w=kgw)
    parts.append(g)
    g, a3 = group(kgx[3], kg, "kagent", "0.10.3", [
        ("kagent-controller", "Agent CRDs -> pods", "", "kubernetes"),
        ("support-chat", "chat page -> gateway", "", "nginx"),
        ("kagent-ui", "admin console of the agents", "", "kubernetes"),
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
    kbot = max(kb, a3 + 14 + 92, nb) + 20
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
            "HTTPS :443 to NGINX\nsupport.<domain>\nagents.<domain>\nregistry.<domain>\ngrafana.<domain>\nlogs.<domain>\n"
            "traces.<domain>\napi.<domain>", 1762, 316, "end"),
        net([kindb.bottom(82 / kindb.w), (kindb.x + 82, ky)], "kind API,\nhelm", 1810, 800),
        net([(200, ky), (200, cb)], "pods -> lakehouse-s3:8333, kafka, ml-db, redis, mlflow, vector-db, scoring-api, "
            "drift-api (node on fraud_default)", 212, cb + 36),
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
NOVEL = "#c2255c"   # the four novel ideas: raspberry, >= 15 OKLab x100 from every flow colour (dataviz validator)
# The flows of the overview's arrows (colours of FLOWS / FLOWS3). Training and release is also dashed: its purple is
# close to the streaming blue for deutan readers, so the dash tells them apart without colour.
OVERVIEW_FLOWS = {
    "S": ("Payment scoring", "each card payment, in milliseconds"),
    "R": ("Streaming", "always on, seconds after each payment"),
    "B": ("Batch data", "nightly and daily pipelines; the policy documents"),
    "T": ("Training and release", "on drift or by an engineer; CI/CD"),
    "M": ("Monitoring", "drift per minute and per day"),
    "A": ("Agent question", "each question from support staff"),
}
DASH = {"T": ' stroke-dasharray="7 4"'}


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
    s.append(t(tx, y + 27, title, 15.5, INK, "start", 700))
    if tech:
        s.append(t(tx, y + 44, tech, 11, INK2, "start", mono=True))
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

    def link(pts, label="", lx=None, ly=None, anchor=None, halo=PANEL, both=False, f=None):
        """An arrow from sender to receiver (both = a call and its answer on one line), in the colour of its flow.
        Without lx/ly the label sits at the middle of the longest segment."""
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
            f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2"{DASH.get(f, "")} marker-end="url(#ah-{f})"{back}/>',
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
        question, then both outcomes side by side, yes on the right. Returns (frame, step boxes)."""
        def rect(rx, ry, rw, text, kind="job", h=None):
            lines = wrap(text, rw - (44 if kind == "check" else 20), 12.5)
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
            body.extend(t(rx + rw / 2, y0 + 16 * i, ln, 12.5, INK) for i, ln in enumerate(lines))
            return Box(rx, ry, rw, h, text)

        frame = len(body)
        body.append("")                            # the frame, drawn under its steps once its height is known
        cy, prev, out, plab = y + 46, None, [], ""
        for st in steps:
            if isinstance(st, tuple) and len(st) > 2:
                q, yes, no, *left = st              # a fourth item "yes left": yes goes on the side its arrow leaves
                qb = rect(x + 12, cy, w - 24, q, "check")
                if prev:
                    link([(x + w / 2, bot(prev)), (x + w / 2, qb.y)], plab, halo=halo)
                hw = (w - 36) / 2
                hh = 16 + 16 * max(len(wrap(s, hw - 20, 12.5)) for s in (yes, no))
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
    box(410, 236, 720, "Airflow starts the pipelines", "", [
        "DP1 runs every night.",
        "Every other pipeline starts when the table it reads has a new checked version.",
        "No pipeline starts another one: DP2 depends on the bronze tables, not on DP1."], "note", "airflow")
    trino = box(1600, 236, 620, "Trino", "SQL on demand · Hive Metastore as the table catalog", [
        "Analysts run SQL on bronze, silver and gold.",
        "The Hive Metastore knows where each table's files are. It keeps its own data in PostgreSQL."], "svc", "trino")
    body += [icon("person", 2290, 262, 30, INK2), t(2328, 284, "Data analyst", 13.5, INK, "start", 600)]
    link([(2284, 290), (2220, 290)], "SQL")

    # ---- batch: sources -> bronze -> silver -> gold, each pipeline as its steps
    R1 = 420
    dp1, s1 = flow(410, R1, 350, "DP1 · SOURCES TO BRONZE", "Airflow + Spark + lakeFS · every night", [
        "Copy the 6 core banking tables (JDBC)", "Load Flink's raw payment files", "Open the lakeFS branch dp1_<day>",
        "Load the night's export onto the branch",
        ("Great Expectations checks pass on the branch?", "Merge the branch into main, publish bronze",
         "Stop: main unchanged, branch kept to inspect")])
    db = box(40, R1, 300, "Core banking database", "PostgreSQL · another department", [
        "customers, accounts, cards", "merchants, disputes", "province mapping (63 old, 34 new)"], "out", "postgres")
    exp_text = ("Nightly export job", "core banking · 01:00 to 03:00",
                ["Writes the day's settled card transactions as Parquet files into an S3 bucket"], "out")
    _, m = info(40, 0, 300, *exp_text)
    exp = box(40, mid(s1[3]) - m.h / 2, 300, *exp_text)
    bronze = box(830, R1, 310, "Bronze", "Delta on S3 · lakeFS branches of data", [
        "raw_* tables: copies of the sources", "A new export waits on its own branch",
        "Checks fail: no merge. Readers keep yesterday's data"], "store", "lakefs", "NOVEL IDEA")
    dp2s, s2 = flow(1210, R1, 320, "DP2 · BRONZE TO SILVER", "Airflow + Spark · on a new bronze version", [
        "Remove duplicate transactions", "Map 63 old provinces to 34 units", "Write the silver tables",
        ("Checks pass? (no row lost)", "Publish a new silver version", "Stop: gold does not start")])
    silver = box(1600, R1, 250, "Silver", "Delta tables on S3", [
        "stg_transactions", "stg_customers", "cleaned: one row per payment"], "store", "deltalake")
    dp2g, s3 = flow(1920, R1, 330, "DP2 · SILVER TO GOLD", "Airflow + dbt on Spark · on a new silver version", [
        "dbt builds the star schema", "Spark builds the fraud labels from disputes",
        ("dbt tests + Great Expectations pass?", "Publish a new gold version", "Stop: DP3 does not start")])
    gold = box(2320, R1, 260, "Gold", "Delta tables on S3", [
        "fact_transaction", "dim_customer, dim_card", "dim_merchant, dim_date", "label_transaction (labels)"],
        "store", "deltalake")
    assert R1 + 20 < mid(s1[0]) < bot(db) and exp.y > bot(db) + 10, "sources do not face DP1's steps"
    link([(340, mid(s1[0])), (422, mid(s1[0]))], "6 tables")
    link([(340, mid(s1[3])), (422, mid(s1[3]))], "export files")
    RY = mid(s2[0])                                # height of the arrows into each pipeline's first step
    yes1 = s1[-1]
    link([(yes1.x + yes1.w, mid(yes1)), (795, mid(yes1)), (795, RY), (830, RY)])
    link([(1140, RY), (1222, RY)])
    link([(s2[-1].x + s2[-1].w, mid(s2[-1])), (1565, mid(s2[-1])), (1565, RY), (1600, RY)])
    link([(1850, RY), (1932, RY)])
    link([(s3[-1].x + s3[-1].w, mid(s3[-1])), (2285, mid(s3[-1])), (2285, RY), (2320, RY)])
    # bronze's reference tables go straight to the gold pipeline and to DP3
    LR = bot(dp1, bronze, dp2s, silver, dp2g, gold) + 22
    link([bronze.bottom(0.5), (985, LR), (2085, LR), (2085, bot(dp2g))], "cards, accounts, merchants, disputes",
         lx=1540, ly=LR - 7, anchor="middle")
    # Trino reads every zone
    TL = R1 - 35
    link([bronze.top(0.5), (985, TL), (1725, TL), (1725, trino.y + trino.h)], "Delta files of every zone", lx=1350, ly=TL - 7,
         anchor="middle")
    link([silver.top(0.5), (1725, TL)])
    dot(1725, TL)
    link([gold.top(0.5), (2450, TL), (1790, TL), (1790, trino.y + trino.h)])

    # ---- streaming and the feature store
    RL = LR + 38                                   # lane of Flink's raw payment files, up to DP1
    R2 = max(RL + 52, bot(exp) + 30)
    app = box(40, R2, 300, "Mobile banking app", "outside", ["logins, password resets", "new payees, transfers"], "out", "phone")
    card = box(40, bot(app) + 14, 300, "Card network", "outside", ["sends every card payment", "waits for the decision"],
               "out", "card")
    kafka = box(410, R2, 320, "Kafka", "the event stream", [
        "in: card_authorizations, app_events", "out: card_features_rt, card_device_seen", "out: ato_alerts_cep (alerts)"],
        "stream", "kafka")
    flink = box(800, R2, 370, "Flink streaming jobs", "PyFlink · always on · results in seconds", [
        "Card features per payment (DataStream API)", "First device of each card (DataStream API)",
        "Raw payments to files, for DP1 (Flink SQL)", "Account takeover alerts (Flink SQL CEP)"], "job", "flink",
        "NOVEL IDEA: THE ALERTS")
    push_off = box(1240, R2, 300, "Push offline", "Python + Feast · always on", [
        "Live features to Delta, for training"], "job", "python")
    push_on = box(1240, bot(push_off) + 15, 300, "Push online", "Python + Feast · always on", [
        "Live features to Redis, for scoring"], "job", "python")
    online = box(1610, R2, 270, "Feast online store", "Redis", [
        "latest features of each card", "read in milliseconds"], "store", "redis")
    offline = box(1950, R2, 290, "Feast offline store", "Delta tables gold.feat_*", [
        "each card's features at any past moment", "for training"], "store", "feast")
    dp3, s4 = flow(2310, R2, 270, "DP3 · GOLD TO FEATURES", "Airflow + Spark · on a new gold version", [
        "Per-card statistics over 30, 90 and 180 days, written to gold.feat_*",
        ("Checks pass?", "Publish the new features", "Stop: Redis keeps the old ones", "yes left")])
    F[0] = "R"
    link([app.right(), (410, mid(app))], "app events")
    link([card.right(), (375, mid(card)), (375, R2 + 90), (410, R2 + 90)], "payments", lx=382, ly=card.y + 14)
    link([(730, R2 + 40), (800, R2 + 40)], "events")
    link([(800, R2 + 82), (730, R2 + 82)], "features,\nalerts", lx=765, ly=R2 + 100, anchor="middle")
    FT = bot(kafka, flink, push_on, online, offline, dp3, card) + 25     # lane of the feature topics
    link([kafka.bottom(0.5), (570, FT), (1205, FT), (1205, mid(push_off)), (1240, mid(push_off))], "feature topics",
         lx=900, ly=FT - 7, anchor="middle")
    link([(1205, mid(push_on)), (1240, mid(push_on))])
    dot(1205, mid(push_on))
    link([(840, R2), (840, RL), (585, RL), (585, bot(dp1))], "raw payment files", lx=712, ly=RL - 7, anchor="middle")
    link([(1540, mid(push_on)), (1580, mid(push_on)), (1580, R2 + 72), (1610, R2 + 72)])
    link([(1500, R2), (1500, R2 - 28), (2095, R2 - 28), (2095, R2)], "live features, kept for training", lx=1790,
         ly=R2 - 35, anchor="middle")
    F[0] = "B"
    link([(1950, R2 + 82), (1880, R2 + 82)], "newest\nrows", lx=1915, ly=R2 + 100, anchor="middle")
    link([(s4[0].x, mid(s4[0])), (2240, mid(s4[0]))], "features")
    yes4 = s4[-1]                                  # publishing starts the materialize pipeline: offline store -> Redis
    assert mid(yes4) > bot(offline) + 50, "DP3's publish step does not sit under the offline store"
    link([(yes4.x, mid(yes4)), (2200, mid(yes4)), (2200, bot(offline))], "starts the\nmaterialize\npipeline", lx=2208,
         ly=(mid(yes4) + bot(offline)) / 2 - 10)
    link([(2480, bot(gold)), (2480, R2)], "fact_transaction")
    link([(2085, LR), (2350, LR), (2350, R2)], "disputes", lx=2358, ly=(LR + R2) / 2 + 4)
    dot(2085, LR)
    PB = FT + 30                                   # bottom of the Layer 1 panel
    assert mid(card) > R2 + 100 and bot(trino) < TL - 10 and R2 + 90 < bot(kafka), "layer 1 rows overlap"
    assert bot(online) > R2 + 82 and bot(push_off) < bot(online) + 60, "feature store arrows miss their boxes"
    body.insert(0, panel(20, 190, W - 40, PB - 190, "LAYER 1 · DATA PLATFORM", PANEL, LINE,
                         note="turns the bank's raw data into clean tables and features: nightly batch on top, streaming below"))

    # ================================================================ Layer 2: the ML platform
    L = PB + 60                                    # top of layers 2 and 3
    H2 = KIND_PANEL
    scoring = box(1130, L + 60, 340, "Fraud Scoring API", "FastAPI", [
        "Reads the card's online features", "Asks KServe for the fraud score", "Answers approve, step up or decline",
        "Model down: a rule on the amount decides"], "svc", "fastapi")
    kserve = box(1540, L + 60, 300, "KServe model server", "Kubernetes", [
        "Serves the production XGBoost model", "Its logger copies every request and response as events"], "svc", "kserve")
    RB = bot(scoring, kserve) + 56
    log = box(1130, RB, 340, "Decision log", "Kafka topic fraud_predictions", [
        "Written by the Scoring API after every decision", "Holds the payment, its features, the score and the decision",
        "Read by the per-day drift report"], "store", "kafka")
    knative = box(1540, RB, 300, "Knative Eventing broker", "Kubernetes", [
        "Receives the logger's events",
        "A Trigger sends them to each detector: drift today; outlier and adversarial detectors can be added"],
        "svc", "kubernetes")
    drift = box(1540, bot(knative) + 45, 300, "Drift API", "FastAPI · the drift detector", [
        "Drift score (PSI) of every input over the last 15 minutes", "Compared with the model's training data"],
        "svc", "fastapi")
    F[0] = "S"
    link([(1470, L + 95), (1540, L + 95)], "features", halo=H2)
    link([(1540, L + 140), (1470, L + 140)], "score", lx=1505, ly=L + 160, anchor="middle", halo=H2)
    link([(1300, bot(scoring)), (1300, log.y)], "every decision", halo=H2)
    F[0] = "M"
    link([(1690, bot(kserve)), (1690, knative.y)], "events", halo=H2)
    link([(1690, bot(knative)), (1690, drift.y)], "Trigger", halo=H2)

    # training, on the right, under the features and the labels it reads
    F[0] = "T"
    train, s5 = flow(2258, L + 24, 334, "TRAINING PIPELINE", "Kubeflow Pipelines · on drift, or by hand", [
        ("Load the labels as of the day (Delta time travel)", "label rows"),
        ("Join them with the features, point in time (Feast)", "training set (Delta version)"),
        ("Check the data, then split it by time (60-day gaps)", "train, validation and test files"),
        ("Train a baseline and XGBoost on Spark", "model files"),
        ("Evaluate against the baseline and the production model", "passed: true or false"),
        "Register the model in MLflow, tagged passed or not",
        ("Better than production?", "Alias challenger: a reviewer can promote it", "No alias: kept to compare")], H2)
    FY = mid(s5[1])                                # where the offline features enter the training set
    F[0] = "M"
    driftp, s6 = flow(1920, FY + 40, 290, "DRIFT PIPELINE", "Airflow · every day", [
        "Rebuild the last 7 days with point-in-time features", "Score them with the production model",
        "Measure PR-AUC, recall at 1% FPR and the PSI of every input", "Save the drift report in MLflow",
        ("PSI above 0.25 or PR-AUC down by 0.05?", "Start a training run", "Keep the production model")], H2)
    mlflow = box(1920, bot(driftp) + 40, 290, "MLflow", "model registry", [
        "every run: metrics, data version, model", "aliases: challenger, production"], "svc", "mlflow")
    F[0] = "T"
    link([(2095, bot(offline)), (2095, FY), (2270, FY)], "features", lx=2103, ly=L - 26, halo=PAGE)
    link([(2095, FY), (2095, driftp.y)], halo=H2, f="M")
    dot(2095, FY)
    link([gold.right(0.6), (2612, gold.y + gold.h * 0.6), (2612, mid(s5[0])), (2580, mid(s5[0]))], "labels", lx=2604,
         ly=L - 26, anchor="end", halo=PAGE)
    yes6, reg = s6[-1], s5[5]
    TY = FY + 30                                   # a drift run starts the training pipeline: into its frame
    assert TY < reg.y and mid(yes6) > mid(reg) and mlflow.y + 60 < bot(mlflow), "drift and training arrows cross"
    link([(yes6.x + yes6.w, mid(yes6)), (2228, mid(yes6)), (2228, TY), (2258, TY)], halo=H2, f="M")
    link([(reg.x, mid(reg)), (2244, mid(reg)), (2244, mlflow.y + 60), (2210, mlflow.y + 60)], halo=H2)
    link([(s6[3].x, mid(s6[3])), (1905, mid(s6[3])), (1905, mlflow.y + 30), (1920, mlflow.y + 30)], halo=H2, f="M")
    NY = mlflow.y + 100                            # the notebook logs its runs into MLflow on this line
    body.append(actor(1125, NY - 20, "Data scientist", "person", w=140))   # the arrow leaves the glyph's middle
    link([(1228, NY), (1300, NY)])
    notebook = box(1300, NY - 30, 340, "JupyterLab notebook", "compose · the training image", [
        "Where the data is explored", "Same steps, Feast features and Spark as the training pipeline",
        "Trained and registered the first model"], "svc", "jupyter")
    assert notebook.y > bot(log, drift) + 30 and NY < bot(mlflow) - 8, "notebook does not fit under the drift API"
    link([(notebook.x + notebook.w, NY), (1920, NY)], "first model, runs", lx=1780, ly=NY - 7, anchor="middle", halo=H2)
    link([(1920, mlflow.y + 76), (1880, mlflow.y + 76), (1880, L + 102), (1840, L + 102)],
         "production model,\nbuilt into the\npredictor image", lx=1890, ly=L + 58, anchor="start", halo=H2)

    # ================================================================ Layer 3: the LLM and agent platform
    body.append(actor(40, L + 80, "Support staff", "people", w=130))
    chat = box(220, L + 60, 340, "Support chat", "web page behind NGINX Ingress", [
        "NGINX: HTTPS, login, rate limit", "The page sends the question to the gateway with its API key"], "gate", "nginx")
    models = box(640, L + 60, 400, "Model servers", "llm-d router + llama.cpp on the host GPU", [
        "llm-d picks a model server for each prompt", "Qwen3-8B writes the answers",
        "A Qwen3 embedding model turns text into vectors"], "svc", "llamacpp")
    gw = box(40, bot(chat, models) + 56, 1000, "agentgateway",
             "the first stop of every call: from the chat, between agents, to the tools and to the model", [
        "Checks the API key of every caller", "Decides which agents and tools that key may use",
        "Limits requests and tokens, masks personal data"], "gate", "agentgateway")
    RC = bot(gw) + 55
    coord = box(40, RC, 310, "Coordinator agent", "kagent", [
        "Splits the question", "Asks the two data agents", "Writes one answer"], "svc", "kagent")
    agents = box(375, RC, 310, "Data agents", "kagent", [
        "card-data agent: looks up the card and the policy", "risk agent: checks the fraud score against the policy"],
        "svc", "kagent")
    mcp = box(740, RC, 310, "MCP tool servers", "three FastAPI servers", [
        "card-data: a card's features, a policy chunk", "fraud-score: scores a payment",
        "policy-rag: searches the bank's policies"], "svc", "mcp")
    RR = bot(coord, agents, mcp) + 45
    F[0] = "B"
    rag, s7 = flow(375, RR, 310, "RAG DATA PIPELINE", "Airflow · after each release", [
        "Read 5 policy documents from S3", "Cut them into chunks: one clause, section or FAQ each",
        "Embed each chunk as a vector", "Write chunks + vectors: newest to pgvector, history to the lakehouse",
        "Check the chunks"])
    vec = box(740, RR, 310, "Vector store", "PostgreSQL + pgvector", [
        "Feast online store: newest chunks and their vectors", "Finds the chunks closest to a question in milliseconds (vector index)",
        "History: a Parquet table in the lakehouse (Feast offline store)"], "store", "postgres")
    F[0] = "A"
    link([(170, L + 110), (220, L + 110)])
    link([(390, bot(chat)), (390, gw.y)], "question")
    link([(840, gw.y), (840, bot(models))], "prompts")
    link([(120, bot(gw)), (120, coord.y)], "question")
    link([(250, coord.y), (250, bot(gw))], "sub-tasks")
    link([(455, bot(gw)), (455, agents.y)], "sub-tasks")
    link([(590, agents.y), (590, bot(gw))], "tool calls,\nprompts")
    link([(895, bot(gw)), (895, mcp.y)], "tool calls")
    wr = s7[3]
    link([(wr.x + wr.w, mid(wr)), (712, mid(wr)), (712, vec.y + 50), (740, vec.y + 50)], "chunks +\nvectors", f="B",
         lx=720, ly=bot(vec) + 30)
    link([(895, vec.y), (895, bot(mcp))], "policy chunks")
    link([(1130, L + 140), (1094, L + 140), (1094, mcp.y + 66), (1050, mcp.y + 66)], "fraud score", lx=1102, ly=mcp.y - 4,
         halo=PAGE)

    # ================================================================ between the layers
    HP = PAGE
    link([card.bottom(0.5), (190, L - 22), (1380, L - 22), (1380, scoring.y)], "payment in, decision back", f="S", lx=640,
         ly=L - 29, anchor="middle", halo=HP, both=True)
    link([(1660, bot(online)), (1660, L - 42), (1078, L - 42), (1078, mcp.y + 28), (1050, mcp.y + 28)], "card features",
         lx=1380, ly=L - 49, anchor="middle", halo=HP)
    link([(1840, bot(online)), (1840, L + 30), (1440, L + 30), (1440, scoring.y)], "online features", f="S", lx=1848,
         ly=(bot(online) + L) / 2, anchor="start", halo=KIND_PANEL)

    LB = max(bot(drift, mlflow, train, log, notebook), bot(rag, vec)) + 25 - L     # height of the layer 2 and 3 panels
    body.insert(1, panel(1110, L, W - 1130, LB, "LAYER 2 · ML PLATFORM", KIND_PANEL, "#b5c0d6"))
    body.insert(1, panel(20, L, 1040, LB, "LAYER 3 · LLM AND AGENT PLATFORM", PANEL, LINE,
                         note="agents answer support questions about a card"))

    # ================================================================ CI/CD: from a commit to a checked release
    cy = L + LB + 30
    cbox = []
    for i, (title, tech, bullets, kind, ico, nov) in enumerate([
            ("GitHub", "the code", ["Jenkins polls main every 5 minutes"], "out", "github", ""),
            ("Jenkins", "5 pipelines", ["MCP servers, data agents, coordinator, RAG pipeline, push jobs",
                                        "Tests, builds and signs each image (cosign)",
                                        "A new production prompt also starts a release"], "job", "jenkins", ""),
            ("Docker registry", "local registry", ["images and their signatures"], "store", "docker", ""),
            ("Deploy", "Helm, Airflow or compose", ["agents and tool servers: Helm", "RAG steps: a new image for Airflow",
                                                    "push jobs: recreated containers"], "job", "helm", ""),
            ("Signed images only", "Kyverno + cosign", ["A tool server pod starts only if CI signed its image"], "gate",
             "kyverno", "NOVEL IDEA"),
            ("Eval gate", "Jenkins + a Langfuse dataset", ["After each agent release: 20 test questions",
                                                          "Under 0.6 accuracy: rolled back"], "gate", "langfuse",
             "NOVEL IDEA")]):
        cbox.append(box(190 + i * 405, cy + 44, 350, title, tech, bullets, kind, ico, nov))
    jen = cbox[1]
    lf = box(jen.x, bot(*cbox) + 60, 350, "Langfuse Cloud EU", "SaaS", [
        "Agent prompts, with one marked production", "Eval datasets and their scores"], "out", "langfuse")
    body.append(actor(40, cy + 60, "Developer", "person", w=110))
    CA = cy + 44 + 40
    F[0] = "T"
    link([(150, CA), (190, CA)], "code", lx=170, ly=CA - 8, anchor="middle")
    for a, b, lab in zip(cbox, cbox[1:], ("commits", "push", "pull", "new pods", "admitted")):
        link([(a.x + a.w, CA), (b.x, CA)], lab)
    link([(jen.x + 90, lf.y), (jen.x + 90, bot(jen))], "prompts")
    link([(jen.x + 260, bot(jen)), (jen.x + 260, lf.y)], "eval scores")
    ch = bot(lf) + 20 - cy
    body.insert(1, panel(20, cy, W - 40, ch, "CI/CD · JENKINS", PANEL, LINE,
                         note="a commit or a new production prompt is tested, built, signed, deployed and checked"))

    # ================================================================ platform services, with the tools each one uses
    sy = cy + ch + 24
    SW, SX = 410, 430

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
         [("azure", "Azure"), ("terraform", "Terraform"), ("ansible", "Ansible")])]
    rows = max(r for *_, items in services for _, r, _, _ in chip_rows(items, SW)) + 1
    sbox = []
    for i, (title, tech, bullets, items) in enumerate(services):
        b = box(40 + i * SX, sy + 44, SW, title, tech, bullets, "svc", pad=rows * 28 + 4)
        for dx, r, ico, name in chip_rows(items, SW):
            cy0 = b.y + b.h - rows * 28 - 6 + r * 28
            body += [icon(ico, b.x + 16 + dx, cy0, 20), t(b.x + 16 + dx + 26, cy0 + 15, name, 11.5, INK2, "start")]
        sbox.append(b)
    SH = bot(*sbox) + 20 - sy
    body.insert(1, panel(20, sy, W - 40, SH, "PLATFORM SERVICES", PANEL, LINE, note="used by every layer, with the tools"))

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
             t(kx + 40, 155, "data or a call, from sender to receiver; its colour is its flow (key at the bottom)", 12,
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
