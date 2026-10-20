"""Platform diagrams of the README (Layer 1 + Layer 2), drawn from the tables in this file. Stdlib only.

  overview      the 15 steps of the README tables: Layer 1 -> Feast -> Layer 2, and the retraining loop
  architecture  every box is a deployable unit; arrows follow the data and carry it as a label, numbered per flow
                with one colour per flow (rubric of the README diagram)
  deployment    where every unit runs: host, Docker Desktop VM (compose profiles), kind cluster (namespaces)

Logos: Simple Icons (CC0), path data in icons.json. Coordinates are explicit: the script warns when an arrow crosses
a box or a label overlaps one, so a layout change is checked before anyone looks at the picture.

Usage: python docs/diagrams/platform.py  ->  docs/images/platform-<name>.svg (+ .png through headless Chrome, if found)
       and the arrow table of the README on stdout.
"""
import json
import shutil
import subprocess
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


def text_w(s: str, size: float, mono: bool) -> float:
    """Approximate rendered width of a string (enough to place labels and check overlaps)."""
    return len(s) * size * (0.6 if mono else 0.56)


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
        tw = text_w(b.tag, 9, True) + 10
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
    color = FLOWS[a.flow][2]
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


def markers() -> str:
    """Arrowheads: one per flow colour and a neutral one for network paths."""
    return "".join(f'<marker id="ah-{k}" markerWidth="9" markerHeight="8" refX="8" refY="4" orient="auto" '
                   f'markerUnits="userSpaceOnUse"><path d="M0,0 L9,4 L0,8 z" fill="{v[2]}"/></marker>'
                   for k, v in FLOWS.items()) +         f'<marker id="ah-net" markerWidth="9" markerHeight="8" refX="8" refY="4" orient="auto" '         f'markerUnits="userSpaceOnUse"><path d="M0,0 L9,4 L0,8 z" fill="{INK2}"/></marker>'


def panel(x, y, w, h, title, fill, stroke, dashed=False, note=""):
    """A titled background region (runtime, layer or group)."""
    dash = ' stroke-dasharray="6 4"' if dashed else ""
    s = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{fill}" stroke="{stroke}"{dash}/>',
         t(x + 16, y + 22, title, 11, INK2, "start", 600, mono=True, spacing=0.08)]
    if note:
        s.append(t(x + 16 + text_w(title, 11, True) * 1.14 + 14, y + 22, note, 11, MUTED, "start", mono=True))
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
        Arrow("T", 5, "metrics, model @challenger", [kfp.left(0.06), (B["mlflow"].x + BW, kfp.y + kfp.h * 0.06)], 0.5, "u",
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
    body.append(legend(28, B["datahub"].y + 58 + 44))
    check("architecture", B, A)
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: system architecture",
                   "Deployable units of Layer 1 and Layer 2 and the data between them, numbered per flow."), A


def legend(x, y):
    """Flow legend and reading notes under the system diagram."""
    s = [t(x, y + 12, "FLOWS", 11, INK2, "start", 600, mono=True, spacing=0.14)]
    for i, (k, (name, who, color)) in enumerate(FLOWS.items()):
        cx, cy = x + (i % 3) * 700, y + 40 + (i // 3) * 34
        s.append(f'<path d="M{cx},{cy} h40" stroke="{color}" stroke-width="2" marker-end="url(#ah-{k})"/>')
        s.append(f'<circle cx="{cx + 20}" cy="{cy}" r="11" fill="#fff" stroke="{color}" stroke-width="2"/>')
        s.append(t(cx + 20, cy + 3.6, k, 9.5, INK, weight=700))
        s.append(t(cx + 56, cy + 4, f"{name}", 13, INK, "start", 600))
        s.append(t(cx + 56 + text_w(name, 13, False) + 10, cy + 4, who, 11, INK2, "start"))
    s.append(t(x + 1400, y + 74, "Arrow = data moving from A to B, labelled with the data; a call without data "
               "(B1, M7) still points from caller to callee.", 11, INK2, "start"))
    s.append(t(x + 1400, y + 90, "Dashed boxes: support services used by many units (arrows left out).",
               11, INK2, "start"))
    s.append(t(x + 1400, y + 106, "Feast is a library, not a unit: it runs inside Scoring API, push jobs, Airflow, "
               "Jupyter and KFP steps.", 11, INK2, "start"))
    return "\n".join(s)


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
         t(x + 14, y + 22, title, 11.5, INK, "start", 700, mono=True, spacing=0.06),
         t(x + 14 + text_w(title, 11.5, True) * 1.06 + 10, y + 22, note, 10.5, MUTED, "start", mono=True)]
    for i, ch in enumerate(chips):
        cx, cy = x + 14 + (i % cols) * (cw + 12), y + 34 + (i // cols) * (CH + 8)
        s.append(chip(cx, cy, *ch, w=cw))
    return "\n".join(s), y + h


def net(pts, label, lx, ly, anchor="start") -> str:
    """A neutral arrow (network path or step order) with a multi-line label at (lx, ly)."""
    d = path_d(pts)
    return "\n".join([f'<path d="{d}" fill="none" stroke="{PANEL}" stroke-width="7"/>',
                      f'<path d="{d}" fill="none" stroke="{INK2}" stroke-width="1.8" marker-end="url(#ah-net)"/>',
                      *[t(lx, ly + 14 * i, ln, 11, INK, anchor, halo=PANEL) for i, ln in enumerate(label.split("\n"))]])


def deployment():
    """Host -> Docker Desktop VM -> compose profiles | kind namespaces, with the network paths between them."""
    W, H = 2166, 1040
    body = [t(28, 46, "CARD FRAUD PLATFORM · LAYER 1 + LAYER 2", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "Deployment: one Windows host, one Docker Desktop VM, compose profiles + a kind cluster",
              22, INK, "start", 400)]
    # host tools
    body.append(panel(14, 96, W - 28, 128, "HOST · Windows 11 · repo .venv + CLIs", "none", LINE, True))
    # boxes right of x 480: their arrows must not cross the VM panel title
    hosts = [Box(480, 124, 330, 84, "Python scripts", ("generator, producer.py, replay_scoring.py,", "locust, ml/pipelines/ (KFP client)"), "host", "python"),
             Box(836, 124, 236, 84, "docker CLI", ("build fraud/* images,", "push localhost:5001"), "host", "docker"),
             Box(1500, 124, 330, 84, "kind · kubectl · helm", ("infra/k8s/up.sh, platform.sh,", "port-forward KFP :31888 / :31880"), "host", "kubernetes"),
             Box(1098, 124, 172, 84, "Browser", ("UIs: Airflow, MLflow,", "DataHub, Flink, KFP"), "host", "")]
    body += [box_svg(b) for b in hosts]

    vy, ph = 282, 620
    body.append(panel(14, vy, W - 28, H - vy - 14, "DOCKER DESKTOP · WSL2 VM · 8 GB RAM + 4 GB swap (.wslconfig)", PANEL, LINE))
    cx0, cy0 = 34, vy + 40
    comp_r = cx0 + 4 * (CW + 28) + 3 * 14 + 40
    body.append(panel(cx0, cy0, comp_r - cx0, ph, "COMPOSE PROJECT fraud · network fraud_default",
                      "#ffffff", LINE, note="ports published on 127.0.0.1 only"))
    gx = [cx0 + 20 + i * (CW + 28 + 14) for i in range(4)]
    gy = cy0 + 40
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
    g, y3 = group(gx[2], gy, "orch", "P7", [
        ("airflow-webserver", "fraud/airflow:2.10.5", ":8082", "airflow"),
        ("airflow-scheduler", "LocalExecutor, Feast venv", "", "airflow"),
        ("airflow-db", "postgres:16-alpine", "", "postgres"),
        ("airflow-init", "one-shot: db + users", "", "airflow", True)])
    body.append(g)
    g, _ = group(gx[2], y3 + 14, "gov", "P8", [
        ("datahub-frontend", "datahub-frontend-react:v1.5.0.6", ":9002", "lineage"),
        ("datahub-gms", "acryldata/datahub-gms:v1.5.0.6", ":8083", "lineage"),
        ("opensearch", "opensearch:2.19.3", "", ""),
        ("datahub-db", "postgres:16-alpine", "", "postgres"),
        ("datahub-system-update", "one-shot: schema upgrade", "", "lineage", True)])
    body.append(g)
    g, _ = group(gx[3], gy, "ml", "Layer 2", [
        ("ml-db", "postgres:16: mlflow + feast", ":5434", "postgres"),
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
    ml_x, ml_y = gx[3] + 14 + CW, gy + 34
    api_y, reg_y = ml_y + 8 * (CH + 8) + CH / 2, ml_y + 9 * (CH + 8) + CH / 2

    # kind
    kx, kw = 1500, W - 14 - 20 - 1500
    body.append(panel(kx, cy0, kw, ph, "KIND NODE fraud-control-plane", KIND_PANEL, "#b5c0d6",
                      note="kindest/node v1.37.0"))
    kgx, kgw = kx + 20, kw - 40
    g, k1 = group(kgx, gy, "kubeflow", "KFP 2.17.2 standalone", [
        ("ml-pipeline", "KFP API", ":31888", "kubernetes"),
        ("ml-pipeline-ui", "KFP UI", ":31880", "kubernetes"),
        ("workflow-controller", "Argo: runs the DAG", "", "kubernetes"),
        ("mysql + seaweedfs", "run metadata, artifacts", "", "kubernetes"),
        ("metadata-grpc", "ML Metadata + envoy", "", "kubernetes"),
        ("cache, agents", "persistence, schedules", "", "kubernetes"),
        ("step pods", "fraud/ml-train, 1 per step", "", "python"),
        ("Spark executors", "2 pods: XGBoost workers", "", "spark")], w=kgw, cols=2)
    body.append(g)
    g, k2 = group(kgx, k1 + 14, "serving", "", [
        ("card-fraud-predictor", "fraud/predictor:<v>, 1-3 replicas", ":31080", "kubernetes")], w=kgw)
    body.append(g)
    g, k3 = group(kgx, k2 + 14, "kserve", "0.21, Standard mode", [
        ("kserve-controller", "InferenceService -> Deployment", "", "kubernetes")], w=kgw)
    body.append(g)
    g, _ = group(kgx, k3 + 14, "cert-manager", "v1.21.1", [
        ("cert-manager", "webhook certs for KServe", "", "kubernetes")], w=kgw)
    body.append(g)

    # network paths
    body += [
        net([hosts[0].bottom(), (hosts[0].x + 165, cy0)], "127.0.0.1:<port>\nKafka, Postgres, S3, APIs",
            hosts[0].x + 177, vy - 34),
        net([hosts[1].bottom(), (hosts[1].x + 118, cy0)], "push :5001", hosts[1].x + 130, vy - 34),
        net([hosts[3].bottom(), (hosts[3].x + 86, cy0)], "UIs", hosts[3].x + 98, vy - 34),
        net([hosts[2].bottom(), (hosts[2].x + 165, cy0)], "kind API, port-forward", hosts[2].x + 177, vy - 34),
        net([(ml_x, api_y), (kx, api_y)], "scoring-api -> :31080\n(KServe predictor)", comp_r + 12, api_y - 22),
        net([(ml_x, reg_y), (kx, reg_y)], "image pulls: localhost:5001\n= registry:5000 (containerd)", comp_r + 12, reg_y + 22),
        net([(kx, gy + 60), (comp_r, gy + 60)], "pods -> lakehouse-s3:8333,\nkafka, ml-db, redis, mlflow\n(node on fraud_default)",
            comp_r + 12, gy + 16),
        net([(comp_r, gy + 150), (kx, gy + 150)], "Airflow -> :31888\n(retrain run)", comp_r + 12, gy + 128),
    ]
    notes = ["8 GB does not hold every profile: run stream alone; keep trino and dbt off while orch + gov run.",
             "seaweedfs has the alias lakehouse-s3: inside kind, KFP's own seaweedfs Service shadows the short name.",
             "Only Kubeflow Pipelines and KServe need Kubernetes; everything else stays in compose."]
    body += [t(cx0 + 20, cy0 + ph + 30 + 18 * i, n, 12, INK2, "start") for i, n in enumerate(notes)]
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: deployment",
                   "Host tools, the Docker Desktop VM with the compose profiles and the kind cluster with its namespaces.")


# ---------------------------------------------------------------- overview
NOVEL1, NOVEL2 = "#bf4520", "#6b7a3a"


def overview():
    """The 15 steps of the README tables: Layer 1 writes the features, Feast hands them over, Layer 2 uses them."""
    W, H, CWD = 2166, 1000, 286
    x = [40 + i * 352 for i in range(6)]

    def card(i, y, tag, title, lines, ico, accent=""):
        """A step card in column i."""
        return Box(x[i], y, CWD, 100, title, lines, tag, ico, accent=accent)

    r1a, r1b, fy, r2 = 136, 270, 430, 610
    B = [card(0, r1a, "SOURCE", "Core banking", ("PostgreSQL core_banking (JDBC)", "daily export files (S3)"), "postgres"),
         card(1, r1a, "01 · DP1", "Spark ingest", ("JDBC + S3A -> bronze raw_*", "Delta, schema evolution"), "spark"),
         card(2, r1a, "02 · DP2", "Spark silver", ("dedup · mergeSchema · salting", "63 -> 34 units · labels"), "spark"),
         card(3, r1a, "03 · NOVEL 1", "dbt-spark gold", ("dim_* · fact_* · SCD2 customer", "22 dbt tests"), "dbt", NOVEL1),
         card(4, r1a, "04 · DP3", "Spark features", ("feat_* tables with", "event_timestamp (PIT ready)"), "spark"),
         card(5, r1a, "05 · SQL", "Trino", ("gold + feat_* for analysts", "Hive Metastore catalog"), "trino"),
         card(0, r1b, "SOURCE", "Card network", ("authorizations -> Kafka", "card_authorizations"), "kafka"),
         card(1, r1b, "06 · STREAM", "Flink", ("watermarks · dedup · windows", "realtime features -> Kafka"), "flink"),
         Box(x[0], fy, x[5] + CWD - x[0], 76, "Feast",
             ("offline: Delta feat_* tables, point-in-time joins on Spark  ·  online: Redis, latest value per card  ·  "
              "registry: PostgreSQL",
              "feature service fraud_model_v1 = 7 daily stats + 13 on-demand features: one list for training and serving"),
             "09 · FEATURE STORE", ""),
         card(0, r2, "10 · NOTEBOOK", "Jupyter", ("the 7 steps one by one", "LR baseline + XGBoost"), "jupyter"),
         card(1, r2, "11 · TRAIN", "Kubeflow Pipelines", ("same 7 steps as pods (kind)", "distributed XGBoost"), "kubernetes"),
         card(2, r2, "12 · REGISTRY", "MLflow", ("runs · data versions", "@challenger · @production"), "mlflow"),
         card(3, r2, "13 · MODEL SERVER", "KServe", ("booster inside the image", "V1 predict, 1-3 replicas"), "kubernetes"),
         card(4, r2, "14 · API", "Fraud Scoring API", ("authorization -> features", "-> approve/step_up/decline"), "fastapi"),
         card(5, r2, "15 · DRIFT", "Drift API + DAG", ("PSI per feature, live", "PR-AUC on labels, daily"), "fastapi")]
    bars = [Box(x[0], 790, x[5] + CWD - x[0], 56, "Airflow",
                ("DP1 -> DP2 -> DP3 -> feast_materialize, daily  ·  drift_monitoring, daily: a KFP run on drift  ·  "
                 "validate stages (Great Expectations)",), "07 · ORCHESTRATION", "airflow"),
            Box(x[0], 856, x[5] + CWD - x[0], 56, "SeaweedFS (S3) + Delta Lake",
                ("bronze raw_* -> silver stg_* -> gold dim_* / fact_* -> feat_*  ·  ml/ training sets (one Delta version "
                 "per run)  ·  MLflow artifacts",), "STORAGE", "bucket"),
            Box(x[0], 922, x[5] + CWD - x[0], 56, "DataHub + OpenLineage",
                ("lineage sent by every Spark job and by Airflow  ·  validation results as assertions",),
                "08 · NOVEL 2", "lineage", accent=NOVEL2)]

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

    body = [t(28, 46, "CARD FRAUD PLATFORM · OVERVIEW", 12, INK2, "start", 600, mono=True, spacing=0.14),
            t(28, 76, "From core banking and the card network to a fraud decision in real time, and back to retraining",
              22, INK, "start", 400),
            panel(20, 100, W - 40, 290, "LAYER 1 · DATA PLATFORM", PANEL, LINE),
            panel(20, 526, W - 40, 244, "LAYER 2 · ML PLATFORM", KIND_PANEL, "#b5c0d6")]
    body += arrows + [box_svg(b) for b in B + bars]
    return svg_doc(W, H, "\n".join(body), "Card fraud platform: overview",
                   "Layer 1 builds the features, Feast hands them to Layer 2, which trains, serves and monitors the model.")


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
    rows += [f"| {a.tag} | {FLOWS[a.flow][0]} | {a.desc} |" for a in arrows]
    return "\n".join(rows)


if __name__ == "__main__":
    svg, arrows = architecture()
    render("architecture", svg)
    render("deployment", deployment())
    render("overview", overview())
    print(table(arrows))
