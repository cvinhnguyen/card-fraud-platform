"""Platform diagrams of the README (Layers 1-3), drawn from the tables in this file. Stdlib only.

  overview      the steps of the README tables: Layer 1 -> Feast -> Layer 2, Layer 3 on top of both, the retraining loop
  architecture  Layers 1 + 2: every box is a deployable unit; arrows follow the data and carry it as a label, numbered
                per flow with one colour per flow (rubric of the README diagram)
  layer3        the same rules for Layer 3: agents, gateway, model servers, RAG ingest, CI/CD, observability
  deployment    where every unit runs: host, Docker Desktop VM (compose profiles), kind cluster (namespaces), internet

Logos: Simple Icons (CC0), path data in icons.json. Coordinates are explicit: the script warns when an arrow crosses
a box or a label overlaps one, so a layout change is checked before anyone looks at the picture.

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
ICONS = json.loads((HERE / "icons.json").read_text())
SANS = "'Segoe UI', 'Helvetica Neue', Arial, sans-serif"
MONO = "'Cascadia Mono', Consolas, 'Courier New', monospace"
INK, INK2, MUTED, LINE = "#2d3142", "#4f5d75", "#8a91a3", "#c8ccd4"
PAGE, PANEL, KIND_PANEL = "#f5f5f5", "#eceef1", "#e6ebf4"

# Flows: letter, name, who / when, colour. Normal-vision separation >= 17 (OKLab x100) between every pair; colour-blind
# readers tell flows apart by the letter in every badge.
FLOWS = {
    "S": ("Payment scoring", "end user: every card authorization, real time", "#e8590c"),
    "R": ("Streaming features", "continuous: Kafka -> Flink -> feature stores", "#1971c2"),
    "B": ("Batch data", "daily, Airflow: DP1 -> DP2 -> DP3 -> Feast materialize", "#2f9e44"),
    "T": ("Training and deploy", "ML engineer: notebook, pipeline run, model image", "#9c36b5"),
    "M": ("Monitoring and retraining", "continuous drift API + daily drift DAG", "#8d5524"),
}
# Layer 3 flows: the same four colours as their Layer 1-2 counterparts (user, data, engineer, monitoring), so the
# validated pairs carry over; only this set appears in the Layer 3 diagram.
FLOWS3 = {
    "A": ("Agent conversation", "support staff, every question: NGINX -> agents -> MCP tools and the LLM", "#e8590c"),
    "K": ("Knowledge ingest", "Airflow rag_ingest, per release: documents -> chunks -> vectors", "#2f9e44"),
    "C": ("CI/CD", "developer push: Jenkins tests, builds, signs, deploys, evaluates", "#9c36b5"),
    "O": ("Observability and scaling", "continuous: metrics, traces, agent replicas", "#8d5524"),
}
ALL_FLOWS = {**FLOWS, **FLOWS3}


@dataclass
class Box:
    """A unit (or a step) drawn as a card: tag top left, logo top right, title and mono lines centred."""
    x: float
    y: float
    w: float
    h: float
    title: str
    lines: tuple = ()
    tag: str = ""
    icon: str = ""
    soft: bool = False   # support service: lighter, no main-flow arrows
    accent: str = ""     # novel idea: tinted card in this colour

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
    """A logo from icons.json, or a line glyph for units without one."""
    if name in ICONS:
        return (f'<svg x="{x}" y="{y}" width="{size}" height="{size}" viewBox="0 0 24 24">'
                f'<path d="{ICONS[name]}" fill="{color}"/></svg>')
    glyph = {   # line icons for units without a logo
        "card": '<rect x="2" y="5" width="20" height="14" rx="2"/><path d="M2 10h20M6 15h4"/>',
        "bucket": '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5l2 14c.3 1.7 2.8 3 6 3s5.7-1.3 6-3l2-14"/>',
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
    if b.accent:
        stroke, fill = b.accent, b.accent + "14"   # 8% tint
    s = [f'<rect x="{b.x}" y="{b.y}" width="{b.w}" height="{b.h}" rx="8" fill="{fill}" stroke="{stroke}"{dash}/>']
    if b.tag:
        tw = text_w(b.tag, 9, True, 0.06) + 10   # the tag is drawn with 0.06em letter-spacing
        s.append(f'<rect x="{b.x + 8}" y="{b.y + 8}" width="{tw:.0f}" height="15" rx="3" fill="none" '
                 f'stroke="{b.accent or LINE}"/>')
        s.append(t(b.x + 13, b.y + 19, b.tag, 9, b.accent or INK2, "start", mono=True, spacing=0.06))
    if b.icon:
        s.append(icon(b.icon, b.x + b.w - 28, b.y + 7, 20, ink if not b.soft else INK2))
    lines = b.lines
    ty = b.y + b.h / 2 + (4 if b.tag else 0) - 6 * len(lines) + 5
    s.append(t(b.x + b.w / 2, ty, b.title, 14.5, ink, weight=600))
    for i, ln in enumerate(lines):
        s.append(t(b.x + b.w / 2, ty + 16 + 13 * i, ln, 10.5, INK2, mono=True))
    return "\n".join(s)


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
    """Complete SVG document with title and description for screen readers."""
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" role="img">\n'
            f'<title>{escape(title)}</title><desc>{escape(desc)}</desc>\n<defs>{markers()}</defs>\n'
            f'<rect width="{w}" height="{h}" fill="{PAGE}"/>\n{body}\n</svg>\n')


# ---------------------------------------------------------------- architecture
BW, BH, GX, GY = 170, 84, 64, 72
COLS = (256, 490, 724, 958, 1222, 1456)   # wider gap c4|c5: B1 and B7 share it
KX = 1810                                  # boxes of the kind cluster


def c(i):
    """x of grid column i (1-based)."""
    return COLS[i - 1]


def r(j):
    """y of grid row j (1-based)."""
    return 128 + (j - 1) * (BH + GY)


def architecture():
    """The README system diagram; returns (svg, arrows) so the arrow table comes from the same data."""
    B = {
        "card": Box(28, r(1), 172, BH, "Card network", ("authorization switch", "replay_scoring.py, locust"), "host", "card"),
        "api": Box(c(3), r(1), BW, BH, "Scoring API", ("FastAPI, Feast client", "fraud/scoring-api :8000"), "ml", "fastapi"),
        "redis": Box(c(3), r(2), BW, BH, "Redis", ("Feast online store", "+ drift counts (db 1)"), "ml", "redis"),
        "pushon": Box(c(2), r(3), BW, BH, "push-online", ("push job A", "Kafka -> Feast push"), "ml", "python"),
        "fwd": Box(c(3), r(3), BW, BH, "drift-forwarder", ("Kafka -> Drift API", "batches of 1,000"), "ml", "python"),
        "drift": Box(c(4), r(3), BW, BH, "Drift API", ("FastAPI, PSI per", "feature, :8001"), "ml", "fastapi"),
        "registry": Box(c(5), r(3), BW, BH, "Docker registry", ("registry:3 :5001", "ml-train, predictor"), "ml", "docker"),
        "kafka": Box(c(1), r(4), c(3) + BW - c(1), BH, "Kafka",
                     ("card_authorizations · card_features_rt", "card_device_seen · fraud_predictions"), "core", "kafka"),
        "pg": Box(c(4), r(4), BW, BH, "PostgreSQL", ("core_banking", "(source) :5433"), "core", "postgres"),
        "mlflow": Box(c(5), r(4), BW, BH, "MLflow", ("tracking + registry", "card_fraud_xgb"), "ml", "mlflow"),
        "flink": Box(c(1), r(5), BW, BH, "Flink", ("rt_features", "device_seen, raw_landing"), "stream", "flink"),
        "pushoff": Box(c(2), r(5), BW, BH, "push-offline", ("push job B", "Kafka -> Delta"), "ml", "python"),
        "trino": Box(c(3), r(5), BW, BH, "Trino", ("SQL on gold", "and feat_* :8090"), "batch", "trino"),
        "spark": Box(c(4), r(5), BW, BH, "Spark", ("master + worker", "DP1 · DP2 · DP3"), "batch", "spark"),
        "airflow": Box(c(5), r(5), BW, BH, "Airflow", ("DP1-3 + dbt + GE", "materialize, drift"), "orch", "airflow"),
        "jupyter": Box(c(6), r(5), BW, BH, "Jupyter", ("fraud_model.ipynb", ":8899"), "ml", "jupyter"),
        "s3": Box(c(1), r(5) + BH + GY, c(6) + BW - c(1), 76, "SeaweedFS (S3) + Delta Lake",
                  ("s3://lakehouse: bronze · silver · gold · feat_* · ml (training sets, ground truth)   "
                   "s3://core-banking-export   s3://mlflow (model artifacts)",), "core", "bucket"),
        "kserve": Box(KX, r(1), 300, BH, "KServe predictor",
                      ("InferenceService card-fraud", "fraud/predictor:<v> NodePort 31080"), "ns serving", "kubernetes"),
        "kfp": Box(KX, r(4), 300, r(5) + BH + GY + 76 - r(4), "Kubeflow Pipelines",
                   ("train_fraud_model: one pod per step", "load_labels -> get_training_data", "-> validate_data -> time_split",
                    "-> train -> evaluate -> register", "train = Spark driver pod", "+ 2 executor pods",
                    "(distributed XGBoost)", "API :31888 · UI :31880"), "ns kubeflow", "kubernetes"),
    }
    sy = B["s3"].y + B["s3"].h + 26
    B["hive"] = Box(c(1), sy, 2 * BW + GX, 58, "Hive Metastore", ("table catalog for Spark and Trino",), "batch", "hive", True)
    B["mldb"] = Box(c(3), sy, 2 * BW + GX, 58, "ml-db (PostgreSQL)", ("MLflow backend, Feast registry",), "ml", "postgres", True)
    B["datahub"] = Box(c(5), sy, c(6) + BW - c(5), 58, "DataHub", ("lineage (OpenLineage) + GE results",), "gov", "lineage", True)

    s3y = B["s3"].y
    api, redis, kafka, kfp = B["api"], B["redis"], B["kafka"], B["kfp"]
    A = [
        Arrow("S", 1, "authorization · POST /v1/score", [B["card"].right(0.32), api.left(0.32)], 0.5,
              desc="The switch sends each authorization to the API (card, amount, channel, device)"),
        Arrow("S", 2, "online features", [redis.top(0.35), api.bottom(0.35)],
              desc="Feast reads the card's stored features from Redis (feature service fraud_model_v1)"),
        Arrow("S", 3, "20 features · V1 predict", [api.right(0.3), B["kserve"].left(0.3)], 0.3,
              desc="Feature vector to the model server (KServe V1 protocol, NodePort 31080)"),
        Arrow("S", 4, "fraud score", [B["kserve"].left(0.7), api.right(0.7)], 0.7, "d",
              desc="Probability of fraud from the production XGBoost booster"),
        Arrow("S", 5, "approve / step_up / decline", [api.left(0.68), B["card"].right(0.68)], 0.5, "d",
              desc="Decision by the two thresholds; rule fallback when the model is late or down"),
        Arrow("S", 6, "decision + features", [api.bottom(0.12), (api.x + 20, 248), (c(1) + 85, 248), kafka.top(85 / kafka.w)],
              0.55, desc="Every decision is logged to Kafka topic fraud_predictions (scoring never waits on monitoring)"),
        Arrow("R", 1, "authorizations", [B["card"].bottom(), (B["card"].x + 86, kafka.y + 42), kafka.left()], 0.45,
              desc="Every authorization (retries included) to Kafka topic card_authorizations"),
        Arrow("R", 2, "auths", [kafka.bottom(55 / kafka.w), B["flink"].top(55 / BW)], side="l",
              desc="Flink reads card_authorizations (watermarks, dedup of network retries)"),
        Arrow("R", 3, "rt features", [B["flink"].top(115 / BW), kafka.bottom(115 / kafka.w)],
              desc="Realtime features to card_features_rt, first (card, device) pairs to card_device_seen"),
        Arrow("R", 4, "raw events -> bronze", [B["flink"].bottom(), (B["flink"].x + 85, s3y)],
              desc="raw_landing writes the raw authorizations to bronze.raw_card_authorizations (Delta)"),
        Arrow("R", 5, "features", [kafka.top((c(2) + 85 - c(1)) / kafka.w), B["pushon"].bottom()],
              desc="push job A consumes card_features_rt and card_device_seen"),
        Arrow("R", 6, "Feast push (online)", [B["pushon"].top(), (c(2) + 85, redis.y + 42), redis.left()], 0.75, "u",
              desc="Writes them to the Redis online store through Feast push (offsets committed after the write)"),
        Arrow("R", 7, "features", [kafka.bottom((c(2) + 85 - c(1)) / kafka.w), B["pushoff"].top()],
              desc="push job B consumes the same two topics"),
        Arrow("R", 8, "Delta append", [B["pushoff"].bottom(), (c(2) + 85, s3y)],
              desc="Appends them to gold.feat_card_auth_rt / gold.feat_card_device: the offline copy for training"),
        Arrow("B", 1, "spark-submit", [B["airflow"].left(0.6), B["spark"].right(0.6)], side="d",
              desc="Airflow runs DP1 -> DP2 -> DP3 on the Spark cluster (call, no data); dbt and GE validate stages"),
        Arrow("B", 2, "JDBC tables", [B["pg"].bottom(), B["spark"].top()], side="l",
              desc="DP1 copies the core banking tables (customers, accounts, cards, merchants, disputes)"),
        Arrow("B", 3, "export files", [(c(4) + 32, s3y), B["spark"].bottom(32 / BW)], side="l",
              desc="DP1 loads the new daily export folders from s3://core-banking-export"),
        Arrow("B", 4, "bronze -> feat_*", [B["spark"].bottom(138 / BW), (c(4) + 138, s3y)],
              desc="Delta tables bronze -> silver -> gold (dbt) -> feat_* in s3://lakehouse"),
        Arrow("B", 5, "gold, feat_*", [(c(3) + 85, s3y), B["trino"].bottom()],
              desc="Trino reads the gold and feature tables for analysts (catalog: Hive Metastore)"),
        Arrow("B", 6, "daily stats", [(c(5) + 40, s3y), B["airflow"].bottom(40 / BW)], side="r",
              desc="DAG feast_materialize (after DP3) reads feat_card_txn_stats_daily through the Feast offline store"),
        Arrow("B", 7, "latest stats", [B["airflow"].left(0.22), (c(4) + BW + 14, B["airflow"].y + BH * 0.22),
                                       (c(4) + BW + 14, 266), (c(3) + 145, 266), redis.top(145 / BW)], 0.6, "u",
              desc="... and loads the newest row per card into Redis (feast materialize-incremental)"),
        Arrow("T", 1, "PIT features", [(c(6) + 85, s3y), B["jupyter"].bottom()],
              desc="The notebook builds the training set: labels + Feast point-in-time features"),
        Arrow("T", 2, "notebook run", [B["jupyter"].top(0.6), (c(6) + 102, B["mlflow"].y + 70), B["mlflow"].right(70 / BH)],
              0.75, "u", desc="Metrics, plots and both models of the notebook run"),
        Arrow("T", 3, "labels + PIT features", [(B["s3"].x + B["s3"].w, s3y + 24), (kfp.x, s3y + 24)], 0.5, "u",
              desc="Pipeline steps 1-2 read the labels as of label_as_of and the point-in-time features"),
        Arrow("T", 4, "training set (MERGE)", [(kfp.x, s3y + 56), (B["s3"].x + B["s3"].w, s3y + 56)], 0.5, "d",
              desc="Step 2 MERGEs the run's rows into ml/training_set_fraud_model_v1 (one Delta version per run)"),
        Arrow("T", 5, "metrics, model @challenger", [kfp.left(0.06), (B["mlflow"].x + BW, kfp.y + kfp.h * 0.06)], 0.6, "u",
              desc="Steps 5-7 log params, data versions, metrics and the booster; alias challenger when the gate passes"),
        Arrow("T", 6, "@production model", [B["mlflow"].top(), B["registry"].bottom()],
              desc="ML engineer: fetch_model.py --alias production, docker build + push of fraud/predictor:<v>"),
        Arrow("T", 7, "image pull", [B["registry"].right(), (B["kserve"].x + 150, B["registry"].y + 42),
                                     B["kserve"].bottom()], 0.4, "u",
              desc="kubectl apply of the InferenceService; the node pulls the image from the registry"),
        Arrow("M", 1, "fraud_predictions", [kafka.top((c(3) + 85 - c(1)) / kafka.w), B["fwd"].bottom()],
              desc="The forwarder consumes the prediction log"),
        Arrow("M", 2, "events", [B["fwd"].right(), B["drift"].left()], side="u",
              desc="Batches to POST /v1/events"),
        Arrow("M", 3, "per-minute counts", [B["drift"].top(0.5), (c(4) + 85, redis.y + 62), redis.right(62 / BH)], 0.7, "d",
              desc="Bin counts per feature and minute in Redis db 1; GET /v1/drift returns the PSI vs the training profile"),
        Arrow("M", 4, "7 days + truth", [(c(5) + 140, s3y), B["airflow"].bottom(140 / BW)],
              desc="DAG drift_monitoring: last 7 days of payments, ground truth, point-in-time features"),
        Arrow("M", 5, "prod model", [B["mlflow"].bottom(0.25), B["airflow"].top(0.25)], side="l",
              desc="Loads the production model to score them"),
        Arrow("M", 6, "PR-AUC, PSI", [B["airflow"].top(0.75), B["mlflow"].bottom(0.75)],
              desc="Logs PR-AUC, recall and PSI as a run of experiment card_fraud_monitoring"),
        Arrow("M", 7, "retrain run", [B["airflow"].right(0.3), (c(5) + BW + 48, B["airflow"].y + BH * 0.3),
                                      (c(5) + BW + 48, r(5) - 36), (kfp.x, r(5) - 36)], 0.82, "u",
              desc="On drift: a KFP run of train_fraud_model with label_as_of = the DAG's day (call, no data)"),
    ]

    W, H = 2166, 1250
    body = [t(28, 46, "CARD FRAUD PLATFORM · LAYER 1 + LAYER 2", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "System architecture: deployable units and the data they exchange", 22, INK, "start", 400),
            panel(14, 96, 200, 132, "OUTSIDE", "none", LINE, True),
            panel(230, 96, c(6) + BW + 26 - 230, B["datahub"].y + 58 + 16 - 96, "DOCKER COMPOSE · project fraud", PANEL, LINE,
                  note="tag = compose profile"),
            panel(KX - 30, 96, 362, kfp.y + kfp.h + 16 - 96, "KIND CLUSTER · fraud", KIND_PANEL, "#b5c0d6",
                  note="tag = namespace")]
    body += [box_svg(b) for b in B.values()]
    body += [arrow_svg(a, PANEL) for a in A]
    body.append(legend(28, B["datahub"].y + 58 + 44, FLOWS, [
        "Arrow = data moving from A to B, labelled with the data; a call without data (B1, M7) still points from "
        "caller to callee.",
        "Dashed boxes: support services used by many units (arrows left out).",
        "Feast is a library, not a unit: it runs inside Scoring API, push jobs, Airflow, Jupyter and KFP steps."]))
    check("architecture", B, A)
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: system architecture",
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


def layer3():
    """The Layer 3 system diagram (agents, gateway, model servers, RAG, CI/CD, observability); returns (svg, arrows)."""
    k1, k2, k3, k4, k5 = K3
    c6, c7, c8 = C3
    B = {
        "staff": Box(28, q(1), 180, BH, "Support staff", ("browser: kagent chat", "app: /a2a/coordinator"), "user"),
        "acme": Box(28, q(2), 180, BH, "Let's Encrypt", ("wildcard certificate,", "DNS-01 on DuckDNS"), "internet",
                    soft=True),
        "groq": Box(28, q(3), 180, BH, "Groq API", ("LLM B: qwen3.8-27b", "A/B weight 0 (off)"), "internet", soft=True),
        "nginx": Box(k1, q(1), BW, BH, "NGINX Ingress", ("HTTPS, basic auth,", "rate limit per IP"), "ns nginx-ingress"),
        "vault": Box(k1, q(2), BW, BH, "Vault + ESO", ("secret/card-fraud/*", "-> Kubernetes Secrets"), "ns vault",
                     soft=True),
        "substrate": Box(k1, q(3), BW, BH, "Agent Substrate", ("gVisor sandboxes from", "warm snapshots"),
                         "ns ate-system", soft=True),
        "areg": Box(k1, q(4), BW, BH, "agentregistry", ("catalog: 3 agents", "+ the MCP server"), "ns agentregistry",
                    soft=True),
        "kagent": Box(k2, q(1), BW, BH, "kagent", ("chat UI + controller", "Agent -> Deployment"), "ns kagent",
                      "kubernetes"),
        "jaeger": Box(k2, q(3), BW, BH, "Jaeger", ("spans over OTLP", "UI traces.<domain>"), "ns monitoring"),
        "vlogs": Box(k2, q(4), BW, BH, "VictoriaLogs", ("Vector ships every", "pod's log lines"), "ns monitoring"),
        "coord": Box(k3, q(1), BW, BH, "coordinator", ("asks the two agents", "kagent Go ADK agent"), "ns kagent"),
        "card": Box(k3, q(2), BW, BH, "card-data-agent", ("get_card_data,", "get_policy_chunk"), "ns kagent"),
        "risk": Box(k3, q(3), BW, BH, "risk-agent", ("score_payment,", "search_policy"), "ns kagent"),
        "keda": Box(k3, q(4), BW, BH, "KEDA", ("ScaledObject per", "agent: 2-4 pods"), "ns keda"),
        "gw": Box(k4, q(1), BW, q(3) + BH - q(1), "agentgateway",
                  ("proxy, one entry for", "A2A: agent to agent", "MCP: tools by API key", "LLM: /v1, PII masked",
                   "keys, rate limits"), "ns agentgateway-system"),
        "prom": Box(k4, q(4), BW, BH, "Prometheus", ("gateway, MCP, nodes,", "Scoring API"), "ns monitoring"),
        "grafana": Box(k4, q(5), BW, BH, "Grafana", ("Web API, compute, LLM,", "agent, A/B dashboards"), "ns monitoring"),
        "llmd": Box(k5, q(1), BW, BH, "llm-d router", ("InferencePool qwen3-8b", "endpoint picker"),
                    "ns agentgateway-system"),
        "mcp": Box(k5, q(2), BW, q(4) + BH - q(2), "card-support-mcp",
                   ("FastAPI + MCP server", "4 tools, REST /v1 too", "2 pods, rolling update", "helm --atomic",
                    "signed image only"), "ns card-support", "fastapi"),
        "chat": Box(c6, q(1), BW, BH, "llama.cpp chat", ("Qwen3-8B Q4, GPU", ":8085, 2 slots"), "host"),
        "embed": Box(c6, q(2), BW, BH, "llama.cpp embed", ("Qwen3-Embedding-0.6B", ":8086, 1024-dim"), "host"),
        "redis": Box(c6, q(3), BW, BH, "Redis", ("Feast card_fraud", "online store"), "ml", "redis"),
        "vdb": Box(c6, q(4), BW, BH, "vector-db", ("pgvector, Feast", "card_support_kb"), "ml", "postgres"),
        "scoring": Box(c6, q(5), BW, BH, "Scoring API", ("Layer 2: features", "+ KServe -> score"), "ml", "fastapi"),
        "github": Box(c7, q(1), BW, BH, "GitHub", ("card-fraud-platform", "branch main"), "internet"),
        "langfuse": Box(c8, q(1), BW, BH, "Langfuse Cloud", ("prompts, datasets,", "experiments (EU)"), "internet"),
        "jenkins": Box(c8, q(2), BW, BH, "Jenkins", ("5 pipelines", "polls main, :8095"), "CI/CD", "jenkins"),
        "registry": Box(c7, q(3), BW, BH, "Docker registry", ("images + cosign", "signatures, :5001"), "ml", "docker"),
        "airflow": Box(c7, q(4), BW, BH, "Airflow", ("DAG rag_ingest, steps", "in fraud/ml-train"), "orch", "airflow"),
        "s3": Box(c7, q(5), BW, BH, "SeaweedFS (S3)", ("bank-documents,", "lakehouse/kb Parquet"), "core", "bucket"),
        "datahub": Box(c8, q(4), BW, BH, "DataHub", ("lineage + 7 checks", "as assertions"), "gov", "lineage"),
    }
    b = B
    fx, fy, fw, fh = k3 - 14, q(1) - 14, BW + 28, q(3) + BH + 14 - (q(1) - 14)   # frame around the three agents
    lane_c5, lane_c6 = 88, 106                                                       # CI lanes above the panels
    A = [
        Arrow("A", 1, "question", [b["staff"].right(), b["nginx"].left()],
              desc="A question in the kagent chat (https://agents.<domain>, basic auth at NGINX); a support app "
                   "calls the gateway route /a2a/coordinator instead"),
        Arrow("A", 2, "question", [b["nginx"].right(), b["kagent"].left()],
              desc="NGINX passes it to kagent-ui (host agents.<domain>, streamed answers, no buffering)"),
        Arrow("A", 3, "task (A2A)", [b["kagent"].right(), b["coord"].left()],
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
        Arrow("A", 9, "tool calls", [(k4 + BW, q(2) + 108), (k5, q(2) + 108)],
              desc="Route /mcp: only the tools allowed to the caller's key reach the MCP server (300 calls/min)"),
        Arrow("A", 10, "features", [b["redis"].left(), (k5 + BW, b["redis"].y + 42)],
              desc="get_card_data: the card's online features from Redis through Feast (project card_fraud)"),
        Arrow("A", 11, "chunks", [b["vdb"].left(), (k5 + BW, b["vdb"].y + 42)],
              desc="get_policy_chunk / search_policy: policy chunks from pgvector (Feast project card_support_kb)"),
        Arrow("A", 12, "vector", [b["embed"].left(), (k5 + BW, b["embed"].y + 42)],
              desc="search_policy embeds the question with Qwen3-Embedding-0.6B, then searches the chunks by it"),
        Arrow("A", 13, "score", [b["scoring"].left(), (k5 + 120, b["scoring"].y + 42), b["mcp"].bottom(120 / BW)], 0.3,
              desc="score_payment: the Layer 2 Scoring API answers with the fraud score and its decision"),
        Arrow("A", 14, "prompts", [(k4 + BW, q(1) + 42), b["llmd"].left()],
              desc="Route /v1: key check, request and token limits, PII masked, then the InferencePool qwen3-8b"),
        Arrow("A", 15, "prompt", [b["llmd"].right(), b["chat"].left()],
              desc="The endpoint picker chooses a pod; its bridge forwards the prompt to llama.cpp on the host GPU"),
        Arrow("K", 1, "documents", [b["s3"].top(0.3), b["airflow"].bottom(0.3)], side="l",
              desc="rag_ingest reads the 5 bank documents from s3://bank-documents"),
        Arrow("K", 2, "vectors", [b["embed"].right(), (c7 - 30, b["embed"].y + 42), (c7 - 30, b["airflow"].y + 25),
                                  b["airflow"].left(25 / BH)], 0.75,
              desc="Steps chunk (57 chunks) and embed: one 1024-dim vector per chunk from the embedding server"),
        Arrow("K", 3, "chunks", [b["airflow"].left(59 / BH), (c6 + BW, b["vdb"].y + 59)], side="d",
              desc="Step write: chunks + vectors into the Feast online store (pgvector)"),
        Arrow("K", 4, "history", [b["airflow"].bottom(0.7), b["s3"].top(0.7)],
              desc="... and their history into the offline store s3://lakehouse/kb/policy_chunks.parquet"),
        Arrow("K", 5, "lineage", [b["airflow"].right(), b["datahub"].left()],
              desc="Step validate: 7 checks as assertions, plus lineage documents -> jobs -> feature view"),
        Arrow("C", 1, "commits", [b["github"].right(64 / BH), (c8 - 35, q(1) + 64), (c8 - 35, q(2) + 28),
                                  b["jenkins"].left(28 / BH)], 0.5, "l",
              desc="Jenkins polls GitHub main for its 5 pipelines (MCP server, data agents, coordinator, RAG, stream jobs)"),
        Arrow("C", 2, "prompts", [b["langfuse"].bottom(0.3), b["jenkins"].top(0.3)], side="l",
              desc="Agent pipelines fetch the prompt labelled production for each agent"),
        Arrow("C", 3, "image", [b["jenkins"].bottom(14 / BW), (c8 + 14, q(3) - 36), (c7 + 134, q(3) - 36),
                                b["registry"].top(134 / BW)], 0.5,
              desc="MCP pipeline: tests (coverage gate 90 %), mutation test, image build, push, cosign signature"),
        Arrow("C", 4, "signed image", [b["registry"].top(34 / BW), (c7 + 34, q(2) - 36), (k5 + 85, q(2) - 36),
                                       b["mcp"].top()], 0.24,
              desc="helm upgrade --atomic; the node pulls the signed image (Kyverno verifies it whenever it is installed)"),
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
        Arrow("O", 2, "metrics", [b["mcp"].left(354 / b["mcp"].h), b["prom"].right()],
              desc="MCP server metrics: calls, failures and latency per tool"),
        Arrow("O", 3, "series", [b["prom"].bottom(), b["grafana"].top()],
              desc="Grafana dashboards: Web API, compute, LLM telemetry, agent telemetry, A/B tests"),
        Arrow("O", 4, "req/s", [b["prom"].left(), b["keda"].right()],
              desc="KEDA's trigger: A2A requests per second for each agent"),
        Arrow("O", 5, "replicas", [b["keda"].top(), (k3 + 85, fy + fh)],
              desc="KEDA sets each agent's replicas between 2 and 4"),
        Arrow("O", 6, "spans", [(fx, b["risk"].y + 42), b["jaeger"].right()],
              desc="Agents send spans over OTLP; the gateway's spans join the same traces (one conversation: about 20)"),
    ]

    W, H = 2240, 1080
    kind_b = q(5) + BH + 26
    body = [t(28, 46, "CARD FRAUD PLATFORM · LAYER 3", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "LLM and agent platform: deployable units and the data they exchange", 22, INK, "start", 400),
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
    body.append(f'<rect x="{fx}" y="{fy}" width="{fw}" height="{fh}" rx="12" fill="none" stroke="#9aa1b1" '
                f'stroke-dasharray="2 4"/>')
    body += [box_svg(x) for x in B.values()]
    body += [arrow_svg(a, PANEL) for a in A]
    body.append(legend(28, kind_b + 24, FLOWS3, [
        "Arrow = data moving from A to B, labelled with the data; the reply to an A2A, MCP or LLM request "
        "travels back on the same call and is not drawn.",
        "Dotted frame: the three agents (A5-A8, C5, O5, O6 apply to each). Dashed boxes: support services "
        "without a flow of their own.",
        "NGINX also serves registry, grafana, logs, traces and the Web API (api.<domain>: card data, chunks, "
        "search, score over HTTPS)."]))
    check("layer3", B, A)
    crossings("layer3", A)
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: Layer 3 system architecture",
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


def net(pts, label, lx, ly, anchor="start", halo=PANEL) -> str:
    """A neutral arrow (network path or step order) with a multi-line label at (lx, ly)."""
    d = path_d(pts)
    return "\n".join([f'<path d="{d}" fill="none" stroke="{halo}" stroke-width="7"/>',
                      f'<path d="{d}" fill="none" stroke="{INK2}" stroke-width="1.8" marker-end="url(#ah-net)"/>',
                      *[t(lx, ly + 14 * i, ln, 11, INK, anchor, halo=halo) for i, ln in enumerate(label.split("\n")) if ln]])


def deployment():
    """Host -> Docker Desktop VM -> compose profiles above the kind cluster's namespaces; internet services right."""
    W, H = 2240, 1780
    body = [t(28, 46, "CARD FRAUD PLATFORM · LAYERS 1-3", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "Deployment: one Windows host (Docker Desktop VM: compose profiles + a kind cluster), Azure for "
                      "the IaC runs, SaaS on the internet", 22, INK, "start", 400)]
    # host tools and model servers
    body.append(panel(14, 96, 1876, 128, "HOST · Windows 11 · GTX 1070 · repo .venv + CLIs", "none", LINE, True))
    hosts = {"py": Box(250, 124, 292, 84, "Python scripts", ("generator, producer.py, locust,", "replay_scoring, KFP client"), "host", "python"),
             "docker": Box(560, 124, 210, 84, "docker CLI", ("build fraud/* images,", "push localhost:5001"), "host", "docker"),
             "iac": Box(788, 124, 230, 84, "terraform · ansible", ("Azure: VNet, VM, AKS, ACR;", "ansible in a container"), "host", ""),
             "chat": Box(1036, 124, 250, 84, "llama.cpp chat", ("Qwen3-8B Q4_K_M on the GPU", ":8085, 2 slots, -cram 1024"), "host", ""),
             "embed": Box(1304, 124, 228, 84, "llama.cpp embed", ("Qwen3-Embedding-0.6B", ":8086, 1024-dim"), "host", ""),
             "browser": Box(1550, 124, 150, 84, "Browser", ("UIs on 127.0.0.1,", "https://*.<domain>"), "host", ""),
             "kind": Box(1718, 124, 158, 84, "kind · helm", ("up.sh, *.sh installs,", "kubectl, forwards"), "host", "kubernetes")}
    body += [box_svg(b) for b in hosts.values()]

    # internet services (right column, outside the VM)
    ix, iw = 1918, 294
    body.append(panel(ix - 14, 96, iw + 28, 498, "INTERNET · SaaS", "none", LINE, True))
    inet = [("GitHub", ("cvinhnguyen/card-fraud-platform,", "branch main: Jenkins polls it")),
            ("Langfuse Cloud EU", ("agent prompts, eval datasets", "+ experiments (Jenkins, agents)")),
            ("Groq", ("LLM B side of the A/B test", "(weight 0 now), via agentgateway")),
            ("Let's Encrypt + DuckDNS", ("*.cvinhnguyen.duckdns.org", "DNS-01, lego in tls.sh"))]
    for i, (name, lines) in enumerate(inet):
        body.append(box_svg(Box(ix, 136 + i * 112, iw, 92, name, lines, "internet", "")))

    # Azure: what Terraform, Ansible and kubectl create for an IaC run (docs/iac.md), destroyed after it
    ay, gw = 614, iw + 8
    az = [t(ix, ay + 50 + 16 * i, ln, 11, INK2, "start", mono=True) for i, ln in enumerate(
        ("rg-cardfraud-dev · Azure for Students", "created per IaC run, then destroyed:", "terraform apply / destroy,",
         "ansible over SSH, docker push, kubectl"))]
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
        ("seaweedfs", "chrislusf/seaweedfs:4.48", ":8333 S3", "bucket"),
        ("kafka", "apache/kafka:3.9.0 (KRaft)", ":9092", "kafka")])
    body.append(g)
    g, y2 = group(gx[0], y1 + 14, "stream", "P6, run alone", [
        ("flink-jobmanager", "fraud/flink:2.2.1", ":8088", "flink"),
        ("flink-taskmanager", "6 slots, 3.5 GB", "", "flink")])
    body.append(g)
    g, _ = group(gx[0], y2 + 14, "dbt", "by hand", [("spark-thrift", "fraud/spark:4.1.3", ":10000", "spark")])
    body.append(g)
    g, _ = group(gx[1], gy, "batch", "P3-P4", [
        ("spark-master", "fraud/spark:4.1.3", ":8080", "spark"),
        ("spark-worker", "fraud/spark:4.1.3", ":8081", "spark"),
        ("spark-history", "fraud/spark:4.1.3", ":18080", "spark"),
        ("hive-metastore", "fraud/hive-metastore:3.1.3", "", "hive"),
        ("metastore-db", "postgres:16-alpine", "", "postgres"),
        ("trino", "trinodb/trino:484, 2 GB", ":8090", "trino")])
    body.append(g)
    g, y3 = group(gx[2], gy, "orch", "P7, RAG", [
        ("airflow-webserver", "fraud/airflow:2.10.5", ":8082", "airflow"),
        ("airflow-scheduler", "LocalExecutor, Feast venv", "", "airflow"),
        ("docker-proxy", "Docker API, RAG step images", "", "docker"),
        ("airflow-db", "postgres:16-alpine", "", "postgres"),
        ("airflow-init", "one-shot: db + users", "", "airflow", True)])
    body.append(g)
    g, _ = group(gx[2], y3 + 14, "gov", "P8", [
        ("datahub-frontend", "datahub-frontend-react:v1.5.0.6", ":9002", "lineage"),
        ("datahub-gms", "acryldata/datahub-gms:v1.5.0.6", ":8083", "lineage"),
        ("opensearch", "opensearch:2.19.3", "", "opensearch"),
        ("datahub-db", "postgres:16-alpine", "", "postgres"),
        ("datahub-system-update", "one-shot: schema upgrade", "", "lineage", True)])
    body.append(g)
    g, ml_b = group(gx[3], gy, "ml", "Layers 2-3", [
        ("ml-db", "postgres:16: mlflow, feast, registry", ":5434", "postgres"),
        ("vector-db", "pgvector 0.8.1: RAG chunks", ":5435", "postgres"),
        ("redis", "redis:7.4-alpine", ":6379", "redis"),
        ("mlflow", "fraud/mlflow:3.16.1", ":5000", "mlflow"),
        ("notebook", "fraud/ml-train (Jupyter)", ":8899", "jupyter"),
        ("push-online", "fraud/ml-train", "", "python"),
        ("push-offline", "fraud/ml-train", "", "python"),
        ("drift-forwarder", "fraud/ml-train", "", "python"),
        ("drift-api", "fraud/drift-api", ":8001", "fastapi"),
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
        ("card-support-mcp", "2 pods, signed image", "", "fastapi")], w=kgw)
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
        net([(1400, cb), (1400, ky)], "images pulled from\nregistry:5000\n(host: localhost:5001)", 1412, cb + 24),
        # host -> Azure: along the top of the host panel, down the gap right of the VM panel
        net([hosts["iac"].top(), (hosts["iac"].x + 115, 106), (1897, 106), (1897, ay + 18), (ix - 6, ay + 18)], "", 0, 0),
    ]
    notes = ["8 GB does not hold every profile: run stream alone; keep trino and dbt off while orch + gov run; "
             "Layer 3 runs with core + ml + CI/CD (profile cicd) and the kind cluster.",
             "seaweedfs has the alias lakehouse-s3: inside kind, KFP's own seaweedfs Service shadows the short name.",
             "The model servers run on the host GPU (GTX 1070, Pascal: no vLLM); llm-d's InferencePool selects thin "
             "bridge pods that forward to them.",
             "Azure (docs/iac.md) exists only during an IaC run: created, checked, destroyed. Its quotas (6 vCPU and 3 "
             "public IPs per region, 0 for GPUs) keep the rest of the platform on the PC."]
    body += [t(34, H - 82 + 18 * i, n, 12, INK2, "start") for i, n in enumerate(notes)]
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: deployment",
                   "Host tools and model servers, the Docker Desktop VM with the compose profiles above the kind "
                   "cluster and its namespaces, the Azure resources of the IaC runs, and the internet services the "
                   "platform uses.")


# ---------------------------------------------------------------- overview
NOVEL1, NOVEL2 = "#bf4520", "#6b7a3a"


def overview():
    """The README steps: Layer 1 writes the features, Feast hands them over, Layer 2 uses them, Layer 3 builds on both."""
    W, H, CWD = 2166, 1574, 286
    x = [40 + i * 352 for i in range(6)]

    def card(i, y, tag, title, lines, ico, accent=""):
        """A step card in column i."""
        return Box(x[i], y, CWD, 100, title, lines, tag, ico, accent=accent)

    r1a, r1b, fy, r2, r3a, r3b = 136, 270, 430, 610, 846, 1046
    B = [card(0, r1a, "SOURCE", "Core banking", ("PostgreSQL core_banking (JDBC)", "daily export files (S3)"), "postgres"),
         card(1, r1a, "01 · DP1", "Spark ingest", ("JDBC + S3A -> bronze raw_*", "Delta, schema evolution"), "spark"),
         card(2, r1a, "02 · DP2", "Spark silver", ("dedup · mergeSchema · salting", "63 -> 34 units · labels"), "spark"),
         card(3, r1a, "03 · NOVEL 1", "dbt-spark gold", ("dim_* · fact_* · SCD2 customer", "22 dbt tests"), "dbt", NOVEL1),
         card(4, r1a, "04 · DP3", "Spark features", ("feat_* tables with", "event_timestamp (PIT ready)"), "spark"),
         card(5, r1a, "05 · SQL", "Trino", ("gold + feat_* for analysts", "Hive Metastore catalog"), "trino"),
         card(0, r1b, "SOURCE", "Card network", ("authorizations -> Kafka", "card_authorizations"), "kafka"),
         card(1, r1b, "06 · STREAM", "Flink", ("watermarks · dedup · windows", "realtime features -> Kafka"), "flink"),
         Box(x[0], fy, x[5] + CWD - x[0], 76, "Feast",
             ("card_fraud: offline Delta feat_* (point-in-time joins on Spark) · online Redis · registry PostgreSQL",
              "fraud_model_v1 = 20 features for training and serving  ·  card_support_kb: policy chunks + vectors "
              "(pgvector)"),
             "09 · FEATURE STORE", ""),
         card(0, r2, "10 · NOTEBOOK", "Jupyter", ("the 7 steps one by one", "LR baseline + XGBoost"), "jupyter"),
         card(1, r2, "11 · TRAIN", "Kubeflow Pipelines", ("same 7 steps as pods (kind)", "distributed XGBoost"), "kubernetes"),
         card(2, r2, "12 · REGISTRY", "MLflow", ("runs · data versions", "@challenger · @production"), "mlflow"),
         card(3, r2, "13 · MODEL SERVER", "KServe", ("booster inside the image", "V1 predict, 1-3 replicas"), "kubernetes"),
         card(4, r2, "14 · API", "Fraud Scoring API", ("authorization -> features", "-> approve/step_up/decline"), "fastapi"),
         card(5, r2, "15 · DRIFT", "Drift API + DAG", ("PSI per feature, live", "PR-AUC on labels, daily"), "fastapi"),
         card(0, r3a, "16 · RAG", "RAG pipeline", ("bank documents -> chunks", "-> vectors -> Feast KB"), "airflow"),
         card(1, r3a, "17 · MODEL SERVER", "llama.cpp", ("Qwen3-8B chat on the GPU", "Qwen3-Embedding 0.6B"), ""),
         card(2, r3a, "18 · ROUTER", "llm-d", ("InferencePool qwen3-8b", "endpoint picker"), "kubernetes"),
         card(3, r3a, "19 · GATEWAY", "agentgateway", ("LLM · MCP · A2A routes", "keys, limits, PII masking"), ""),
         card(4, r3a, "20 · MCP SERVER", "card-support-mcp", ("4 tools: card data, chunk,", "policy search, fraud score"),
              "fastapi"),
         card(0, r3b, "USERS", "Support staff", ("kagent chat in a browser", "or an app over A2A"), ""),
         card(1, r3b, "21 · EDGE", "NGINX Ingress", ("HTTPS *.<domain>", "basic auth, rate limits"), ""),
         card(2, r3b, "22 · COORDINATOR", "Coordinator agent", ("plans the answer, asks", "the two agents (A2A)"), ""),
         card(3, r3b, "23 · AGENTS", "Card-data + risk agents", ("kagent, MCP tools", "KEDA: 2-4 replicas each"), ""),
         card(4, r3b, "24 · WARM POOL", "Agent Substrate", ("sandboxed agents restored", "from snapshots, 1 worker"), ""),
         card(5, r3b, "25 · REGISTRY", "agentregistry", ("catalog of the 3 agents", "and the MCP server"), "")]
    bars = [Box(x[0], 1196, x[5] + CWD - x[0], 56, "Airflow",
                ("DP1 -> DP2 -> DP3 -> feast_materialize, daily  ·  drift_monitoring, daily: a KFP run on drift  ·  "
                 "rag_ingest (16)  ·  validate stages (Great Expectations)",), "07 · ORCHESTRATION", "airflow"),
            Box(x[0], 1262, x[5] + CWD - x[0], 56, "SeaweedFS (S3) + Delta Lake",
                ("bronze raw_* -> silver stg_* -> gold dim_* / fact_* -> feat_*  ·  ml/ training sets (one Delta version "
                 "per run)  ·  bank-documents (RAG)  ·  MLflow artifacts",), "STORAGE", "bucket"),
            Box(x[0], 1328, x[5] + CWD - x[0], 56, "DataHub + OpenLineage",
                ("lineage sent by every Spark job and by Airflow  ·  validation results as assertions (RAG chunks "
                 "included)",), "08 · NOVEL 2", "lineage", accent=NOVEL2),
            Box(x[0], 1394, x[5] + CWD - x[0], 76, "Jenkins + Prometheus, Grafana, VictoriaLogs, Jaeger + Vault",
                ("CI/CD of the MCP server, agents (prompts from Langfuse, eval gate: L3 novel 1), RAG pipeline, stream "
                 "jobs; signed images (L3 novel 2)",
                 "metrics of the Web API, compute, LLM and agents  ·  logs of every pod  ·  traces of every agent "
                 "call  ·  secrets in Vault"), "26 · OPERATIONS", ""),
            Box(x[0], 1480, x[5] + CWD - x[0], 76, "Where it runs: one Windows PC  ·  Azure for the IaC runs  ·  SaaS",
                ("PC (16 GB RAM, GTX 1070): every box above, as compose profiles in Docker Desktop and a kind cluster",
                 "Azure, created per IaC run then destroyed: a VM (Ansible) and AKS serve the model image of 13, pulled "
                 "from ACR  ·  SaaS: GitHub, Langfuse Cloud EU, Groq, Let's Encrypt + DuckDNS"), "27 · CLOUD", "")]

    mid1, mid1b, mid2 = r1a + 50, r1b + 50, r2 + 50
    arrows = [net([(x[i] + CWD, y), (x[i + 1], y)], "", 0, 0) for y, n in ((mid1, 5), (mid1b, 1), (mid2, 5))
              for i in range(n)]
    down = [(4, r1a + 100, fy, "daily stats, materialized to Redis"), (1, r1b + 100, fy, "push jobs: realtime + device")]
    arrows += [net([(x[i] + 143, y0), (x[i] + 143, y1)], lab, x[i] + 155, y0 + 40) for i, y0, y1, lab in down]
    reads = [(0, "point-in-time"), (1, "point-in-time"), (4, "online features"), (5, "offline features")]
    arrows += [net([(x[i] + 200, fy + 76), (x[i] + 200, r2)], lab, x[i] + 212, r2 - 22) for i, lab in reads]
    loop = r2 + 100 + 30
    arrows.append(net([(x[5] + 143, r2 + 100), (x[5] + 143, loop), (x[1] + 143, loop), (x[1] + 143, r2 + 100)],
                      "drift: retraining run with the newest labels", (x[1] + x[5]) / 2 + 143, loop - 8, "middle"))
    # Layer 3: the agents' path (bottom row), the gateway's backends (top row), what it reads from Layers 1-2
    mid3a, mid3b = r3a + 50, r3b + 50
    arrows += [net([(x[i] + CWD, mid3b), (x[i + 1], mid3b)], "", 0, 0) for i in range(3)]
    arrows += [net([(x[3], mid3a), (x[2] + CWD, mid3a)], "", 0, 0), net([(x[2], mid3a), (x[1] + CWD, mid3a)], "", 0, 0),
               net([(x[3] + CWD, mid3a), (x[4], mid3a)], "", 0, 0)]
    arrows.append(net([(x[3] + 143, r3b), (x[3] + 143, r3a + 100)], "every A2A, MCP and LLM call",
                      x[3] + 155, r3a + 150))
    arrows.append(net([(x[0] + CWD, r3a + 24), (x[0] + CWD + 33, r3a + 24), (x[0] + CWD + 33, fy + 76)],
                      "chunks + vectors", x[0] + CWD + 45, 790))
    arrows.append(net([(x[4] + 143, r2 + 100), (x[4] + 143, r3a)], "fraud score", x[4] + 155, 790))
    arrows.append(net([(x[5] + CWD, fy + 38), (W - 44, fy + 38), (W - 44, mid3a), (x[4] + CWD, mid3a)],
                      "card features (Redis)\n+ policy chunks (pgvector)", W - 56, r3a + 22, "end"))

    body = [t(28, 46, "CARD FRAUD PLATFORM · OVERVIEW", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "From core banking and the card network to a fraud decision in real time, back to retraining, "
                      "and on to the agents that answer card support questions", 22, INK, "start", 400),
            panel(20, 100, W - 40, 290, "LAYER 1 · DATA PLATFORM", PANEL, LINE),
            panel(20, 526, W - 40, 244, "LAYER 2 · ML PLATFORM", KIND_PANEL, "#b5c0d6"),
            panel(20, 800, W - 40, 376, "LAYER 3 · LLM + AGENT PLATFORM", PANEL, LINE)]
    body += arrows + [box_svg(b) for b in B + bars]
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: overview",
                   "Layer 1 builds the features, Feast hands them to Layer 2, which trains, serves and monitors the "
                   "model; Layer 3's agents read the same features, the fraud score and a policy knowledge base.")


# ---------------------------------------------------------------- output
def render(name: str, svg: str) -> None:
    """Write the SVG and, when Chrome or Edge is installed, a 2x PNG screenshot of it next to it."""
    path = OUT / f"platform-{name}.svg"
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
    only = set(sys.argv[1:]) or {"architecture", "layer3", "deployment", "overview"}
    tables = []
    for name in ("architecture", "layer3", "deployment", "overview"):   # one Chrome at a time
        if name in only:
            out = globals()[name]()
            svg, arrows = out if isinstance(out, tuple) else (out, None)
            render(name, svg)
            if arrows:
                tables.append(table(arrows))
    print("\n\n".join(tables))
