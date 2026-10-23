"""Entity-relationship diagrams of the lakehouse zones for docs/schema_design.md, drawn from the tables in this file.

  bronze  the 8 raw_* tables, copies of the sources: the lines are the source's keys, not constraints
  silver  stg_transactions and stg_customers (new and renamed columns marked) and the bronze tables that link them
  gold    the star schema, the label table and the feature table

A line ends in a bar on the "one" side and a crow's foot on the "many" side; a dashed line is a name lookup.
Stdlib only; fonts, colours and the PNG step come from platform.py.

Usage: python docs/diagrams/er.py [zone ...]  ->  docs/images/er-<zone>.svg (+ .png through headless Chrome)
"""
import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location("platform_diagrams", Path(__file__).with_name("platform.py"))
assert _spec and _spec.loader
P = sys.modules["platform_diagrams"] = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(P)
t, text_w, INK, INK2, MUTED, LINE = P.t, P.text_w, P.INK, P.INK2, P.MUTED, P.LINE

ZONE = {"bronze": ("#f1e4d8", "#a8754a"), "silver": ("#e8ebf0", "#7d8796"), "gold": ("#f6edd0", "#a88224")}
FLAG = {"new": "#2b8a3e", "renamed": "#5f3dc4", "changed": "#c2410c"}
HEAD, ROW, GAP_X, GAP_Y, TOP, LEFT = 32, 22, 130, 40, 96, 70


def col(name, typ="string", key="", note="", flag=""):
    """One column: name, type, PK / FK, a short note, and new / renamed / changed (silver)."""
    return name, typ, key, note, flag


META = col("_ingested_at, _source", "", "", "added by DP1")

BRONZE = {
    "raw_customers": [col("customer_id", key="PK"), col("updated_ts", "timestamp", "PK", "one row per version"),
                      col("full_name"), col("dob", "date"), col("city", note="old or new province name"),
                      col("income_band"), col("risk_tier"), col("kyc_level", "int"), META],
    "raw_accounts": [col("account_id", key="PK"), col("customer_id", key="FK"), col("account_type"),
                     col("open_ts", "timestamp"), col("status"), col("credit_limit", "bigint"), META],
    "raw_cards": [col("card_id", key="PK"), col("account_id", key="FK"), col("card_type"), col("network"),
                  col("issue_ts", "timestamp"), col("status"), col("expiry_month"), META],
    "raw_ref_province_mapping": [col("old_province_code", key="PK"), col("old_province_name", note="63 old provinces"),
                                 col("new_unit_code"), col("new_unit_name", note="34 new units"),
                                 col("effective_from", "date"), META],
    "raw_transactions": [col("transaction_id", note="not unique: 2% duplicates"), col("card_id", key="FK"),
                         col("merchant_id", key="FK"), col("terminal_id", note="NULL for ecom"),
                         col("txn_ts", "timestamp"), col("txn_city", note="old or new province name"),
                         col("amount_vnd", "bigint"), col("currency", note="VND, or USD abroad"),
                         col("channel", note="ecom, pos, atm"), col("status", note="settled, reversed"),
                         col("created_ts", "timestamp", note="later on a re-exported copy"),
                         col("auth_method", note="only from 2024-07-01"),
                         col("three_ds_version", note="only from 2024-07-01"),
                         col("export_date", "date", note="partition"), META],
    "raw_card_authorizations": [col("auth_id", note="network retries repeat it"), col("card_id", key="FK"),
                                col("merchant_id", key="FK"), col("amount_vnd", "bigint"), col("channel"),
                                col("terminal_id"), col("device_id"), col("geo_city"),
                                col("auth_ts", "timestamp", note="event time"),
                                col("received_ts", "timestamp", note="arrival time"), col("kafka_partition", "int"),
                                col("kafka_offset", "bigint"), col("auth_date", note="partition, from auth_ts"), META],
    "raw_merchants": [col("merchant_id", key="PK"), col("merchant_name"), col("mcc", "int"),
                      col("is_online", "boolean"), col("city", note="one of 34 units, NULL abroad"),
                      col("country"), META],
    "raw_disputes": [col("dispute_id", key="PK"), col("transaction_id", key="FK"),
                     col("reported_ts", "timestamp", note="1 to 60 days after the payment"), col("reason_code"),
                     col("is_confirmed_fraud", "boolean"), META],
}

SILVER = {
    "stg_customers": [col("customer_id", key="PK"), col("updated_ts", "timestamp", "PK", "one row per version"),
                      col("full_name"), col("dob", "date"), col("city_raw", note="was city", flag="renamed"),
                      col("customer_unit", note="one of 34 units", flag="new"), col("income_band"),
                      col("risk_tier"), col("kyc_level", "int"), col("_ingested_at, _source", "", "", "from bronze"),
                      col("_processed_at", "timestamp", flag="new")],
    "raw_accounts": [col("account_id", key="PK"), col("customer_id", key="FK"), col("... 4 more columns", "")],
    "raw_cards": [col("card_id", key="PK"), col("account_id", key="FK"), col("... 5 more columns", "")],
    "raw_ref_province_mapping": [col("old_province_name"), col("new_unit_name"), col("... 3 more columns", "")],
    "stg_transactions": [col("transaction_id", key="PK", note="one row per id: latest created_ts"),
                         col("card_id", key="FK"), col("merchant_id", key="FK", note="raw_merchants (bronze)"),
                         col("terminal_id"), col("txn_ts", "timestamp"),
                         col("txn_city_raw", note="was txn_city", flag="renamed"),
                         col("txn_unit", note="one of 34 units", flag="new"), col("amount_vnd", "bigint"),
                         col("currency"), col("channel"), col("status"), col("created_ts", "timestamp"),
                         col("auth_method", note="NULL -> unknown_pre_regulation", flag="changed"),
                         col("three_ds_version"), col("export_date", "date"),
                         col("txn_date", "date", note="partition", flag="new"),
                         col("_ingested_at, _source", "", "", "from bronze"),
                         col("_processed_at", "timestamp", flag="new")],
}
SILVER_ZONE = {"raw_accounts": "bronze", "raw_cards": "bronze", "raw_ref_province_mapping": "bronze"}

GOLD = {
    "dim_customer": [col("customer_sk", key="PK"), col("customer_id"), col("full_name"), col("dob", "date"),
                     col("customer_unit"), col("city_raw"), col("income_band"), col("risk_tier"),
                     col("kyc_level", "int"), col("valid_from_ts", "timestamp"),
                     col("valid_to_ts", "timestamp", note="exclusive; 9999-12-31 if current"),
                     col("is_current", "boolean")],
    "dim_date": [col("date_key", "int", "PK", "yyyyMMdd"), col("full_date", "date"), col("year", "int"),
                 col("month", "int"), col("year_month"), col("day_of_week", "int"), col("is_weekend", "boolean"),
                 col("is_after_reorg", "boolean")],
    "fact_transaction": [col("transaction_id", key="PK", note="settled transactions only"),
                         col("date_key", "int", "FK"), col("txn_ts", "timestamp"), col("card_id", key="FK"),
                         col("customer_sk", key="FK", note="version valid at txn_ts"), col("merchant_id", key="FK"),
                         col("terminal_id"), col("txn_unit", note="one of 34 units"),
                         col("txn_city_raw", note="as sent by the source"), col("channel", note="ecom, pos, atm"),
                         col("auth_method"), col("three_ds_version"), col("amount_vnd", "bigint"),
                         col("currency", note="VND, or USD abroad"), col("export_date", "date"),
                         col("txn_month", note="partition")],
    "label_transaction": [col("transaction_id", key="PK", note="reversed ones have no fact row"), col("card_id"),
                          col("customer_id"), col("event_timestamp", "timestamp"), col("amount_vnd", "bigint"),
                          col("is_fraud", "boolean"), col("fraud_reported_ts", "timestamp"),
                          col("label_as_of", "date", note="one Delta version per run"),
                          col("event_month", note="partition")],
    "dim_card": [col("card_id", key="PK"), col("account_id"), col("customer_id"), col("card_type"), col("network"),
                 col("account_type"), col("credit_limit", "bigint"), col("issue_ts", "timestamp"),
                 col("expiry_month"), col("status")],
    "dim_merchant": [col("merchant_id", key="PK"), col("merchant_name"), col("mcc", "int"), col("is_online", "boolean"),
                     col("merchant_unit", note="NULL for foreign merchants"), col("country")],
    "feat_card_txn_stats_daily": [col("card_id", key="PK"),
                                  col("event_timestamp", "timestamp", "PK", "active day L + 1, 00:00"),
                                  col("txn_count_30d", "int"), col("distinct_merchants_30d", "int"),
                                  col("avg_amount_90d", "double"), col("std_amount_90d", "double"),
                                  col("ecom_ratio_90d", "double"), col("foreign_txn_ratio_90d", "double"),
                                  col("dispute_count_180d", "int"),
                                  col("stats_ts", "timestamp", note="copy of event_timestamp"),
                                  col("created", "timestamp")],
}

# zone: tables, column layout (left to right, top to bottom), links (one table, column, many table, column, kind, side)
VIEWS = {
    "bronze": (BRONZE, [["raw_customers", "raw_accounts", "raw_cards", "raw_ref_province_mapping"],
                        ["raw_transactions", "raw_card_authorizations"], ["raw_disputes", "raw_merchants"]],
               [("raw_customers", "customer_id", "raw_accounts", "customer_id", "1n", "L"),
                ("raw_accounts", "account_id", "raw_cards", "account_id", "1n", "L"),
                ("raw_cards", "card_id", "raw_transactions", "card_id", "1n", ""),
                ("raw_cards", "card_id", "raw_card_authorizations", "card_id", "1n", ""),
                ("raw_merchants", "merchant_id", "raw_transactions", "merchant_id", "1n", ""),
                ("raw_merchants", "merchant_id", "raw_card_authorizations", "merchant_id", "1n", ""),
                ("raw_transactions", "transaction_id", "raw_disputes", "transaction_id", "1n", ""),
                ("raw_ref_province_mapping", "old_province_name", "raw_transactions", "txn_city", "lookup", ""),
                ("raw_ref_province_mapping", "old_province_name", "raw_customers", "city", "lookup", "L")],
               "Bronze: the sources copied as they are (DP1). Lines are the source's keys, not constraints: "
               "bronze keeps duplicate ids and every customer version."),
    "silver": (SILVER, [["stg_customers"], ["raw_accounts", "raw_cards", "raw_ref_province_mapping"],
                        ["stg_transactions"]],
               [("stg_customers", "customer_id", "raw_accounts", "customer_id", "1n", ""),
                ("raw_accounts", "account_id", "raw_cards", "account_id", "1n", "R"),
                ("raw_cards", "card_id", "stg_transactions", "card_id", "1n", ""),
                ("raw_ref_province_mapping", "new_unit_name", "stg_transactions", "txn_unit", "lookup", ""),
                ("raw_ref_province_mapping", "new_unit_name", "stg_customers", "customer_unit", "lookup", "")],
               "Silver: the two tables DP2 cleans; the bronze tables next to them are read as they are and link a "
               "transaction to its customer."),
    "gold": (GOLD, [["dim_customer", "dim_date"], ["fact_transaction", "label_transaction"],
                    ["dim_card", "dim_merchant"], ["feat_card_txn_stats_daily"]],
             [("dim_customer", "customer_sk", "fact_transaction", "customer_sk", "1n", ""),
              ("dim_date", "date_key", "fact_transaction", "date_key", "1n", ""),
              ("dim_card", "card_id", "fact_transaction", "card_id", "1n", ""),
              ("dim_merchant", "merchant_id", "fact_transaction", "merchant_id", "1n", ""),
              ("fact_transaction", "transaction_id", "label_transaction", "transaction_id", "11", "R"),
              ("dim_card", "card_id", "feat_card_txn_stats_daily", "card_id", "1n", "")],
             "Gold: star schema around fact_transaction (dbt), the label table and the Feast feature table."),
}


class Table:
    """A placed table: position, size, zone and the y of each column's row."""

    def __init__(self, name, rows, zone):
        self.name, self.rows, self.zone = name, rows, zone
        self.nw = max(text_w(r[0], 12, True) for r in rows)
        self.tw = max([text_w(r[1], 11, True) for r in rows] + [0])
        self.fw = max([text_w(r[3], 11, False) + (text_w(r[4], 9, True) + 16 if r[4] else 0) for r in rows] + [0])
        body = 40 + self.nw + 14 + self.tw + (14 + self.fw if self.fw else 0) + 16
        self.w = max(body, text_w(name, 13, True) + text_w(zone, 10, True, 0.08) + 44)
        self.h = HEAD + ROW * len(rows) + 6
        self.x = self.y = 0

    def row_y(self, column):
        """Vertical centre of a column's row."""
        i = next(k for k, r in enumerate(self.rows) if r[0] == column)
        return self.y + HEAD + ROW * i + ROW / 2

    def svg(self):
        fill, accent = ZONE[self.zone]
        x, y, w = self.x, self.y, self.w
        s = [f'<rect x="{x}" y="{y}" width="{w:.0f}" height="{self.h}" rx="8" fill="#ffffff" stroke="{accent}" '
             f'stroke-width="1.3"/>',
             f'<path d="M{x},{y + HEAD} V{y + 8} a8,8 0 0 1 8,-8 H{x + w - 8:.1f} a8,8 0 0 1 8,8 V{y + HEAD} Z" '
             f'fill="{fill}" stroke="{accent}" stroke-width="1.3"/>',
             t(x + 12, y + 21, self.name, 13, INK, "start", 700, mono=True),
             t(x + w - 12, y + 20, self.zone.upper(), 10, accent, "end", 700, mono=True, spacing=0.08)]
        for i, (name, typ, key, note, flag) in enumerate(self.rows):
            ry = y + HEAD + ROW * i
            if i % 2:
                s.append(f'<rect x="{x + 1}" y="{ry}" width="{w - 2:.0f}" height="{ROW}" fill="#f7f8fa"/>')
            base = ry + 15
            if key:
                kc = INK if key == "PK" else INK2
                s += [f'<rect x="{x + 10}" y="{ry + 4}" width="24" height="14" rx="3" fill="none" stroke="{kc}"/>',
                      t(x + 22, base - 1.5, key, 9, kc, "middle", 700, mono=True)]
            meta = name.startswith("_") or name.startswith("...")
            s.append(t(x + 40, base, name, 12, MUTED if meta else INK, "start", 600 if key == "PK" else 400,
                       mono=True))
            tx = x + 40 + self.nw + 14
            s.append(t(tx, base, typ, 11, MUTED, "start", mono=True))
            nx = tx + self.tw + 14
            if flag:
                fw = text_w(flag, 9, True) + 10
                s += [f'<rect x="{nx:.1f}" y="{ry + 4}" width="{fw:.1f}" height="14" rx="7" fill="{FLAG[flag]}"/>',
                      t(nx + fw / 2, base - 1.5, flag, 9, "#ffffff", "middle", 700, mono=True)]
                nx += fw + 6
            if note:
                s.append(t(nx, base, note, 11, INK2, "start"))
        return "\n".join(s)


def mark(x, y, d, kind):
    """End of a relation line at a table edge; d = +1 when the line leaves to the right, -1 to the left."""
    if kind == "many":
        return "".join(f'<path d="M{x + d * 14},{y} L{x},{y + dy}" stroke="{INK2}" stroke-width="1.4" fill="none"/>'
                       for dy in (-7, 0, 7))
    out = f'<path d="M{x + d * 8},{y - 7} V{y + 7}" stroke="{INK2}" stroke-width="1.4"/>'
    if kind == "zero_one":
        out += f'<circle cx="{x + d * 17}" cy="{y}" r="4" fill="#ffffff" stroke="{INK2}" stroke-width="1.4"/>'
    return out


def view(zone):
    tables, layout, links, desc = VIEWS[zone]
    placed, cols = {}, []
    x = LEFT
    for column in layout:
        y, width = TOP, 0
        for name in column:
            tb = Table(name, tables[name], SILVER_ZONE.get(name, zone) if zone == "silver" else zone)
            tb.x, tb.y = x, y
            placed[name] = tb
            y += tb.h + GAP_Y
            width = max(width, tb.w)
        cols.append((x, width, column))
        x += width + GAP_X
    col_of = {n: i for i, (_, _, names) in enumerate(cols) for n in names}
    # lanes: one vertical per link inside a gap, spread evenly; same-column links run outside the column's edge
    gap_links = {}
    for ln in links:
        i, j = col_of[ln[0]], col_of[ln[2]]
        if i != j:
            assert abs(i - j) == 1, f"{ln}: tables two columns apart"
            gap_links.setdefault(min(i, j), []).append(ln)
    lane = {}
    for g, lns in gap_links.items():
        gx = cols[g][0] + cols[g][1]
        lns.sort(key=lambda ln: min(placed[ln[0]].row_y(ln[1]), placed[ln[2]].row_y(ln[3])))
        for k, ln in enumerate(lns):
            lane[ln] = gx + GAP_X * (k + 1) / (len(lns) + 1)
    side_n = {}
    lines, marks = [], []
    for ln in links:
        a, ac, b, bc, kind, side = ln
        ta, tb = placed[a], placed[b]
        ya, yb = ta.row_y(ac), tb.row_y(bc)
        i, j = col_of[a], col_of[b]
        if i != j:
            (ea, da), (eb, db) = ((ta.x + ta.w, 1), (tb.x, -1)) if i < j else ((ta.x, -1), (tb.x + tb.w, 1))
            lx = lane[ln]
        else:
            cx, cw, _ = cols[i]
            k = side_n[(i, side)] = side_n.get((i, side), 0) + 1
            if side == "L":
                (ea, da), (eb, db), lx = (ta.x, -1), (tb.x, -1), cx - 16 - 12 * k
            else:
                (ea, da), (eb, db), lx = (ta.x + ta.w, 1), (tb.x + tb.w, 1), cx + cw + 16 + 12 * k
        pts = [(ea, ya), (lx, ya), (lx, yb), (eb, yb)]
        dash = ' stroke-dasharray="5 4"' if kind == "lookup" else ""
        lines.append(f'<path d="{P.path_d(pts)}" fill="none" stroke="{MUTED if kind == "lookup" else INK2}" '
                     f'stroke-width="1.4"{dash}/>')
        if kind == "1n":
            marks += [mark(ea, ya, da, "one"), mark(eb, yb, db, "many")]
        elif kind == "11":
            marks += [mark(ea, ya, da, "one"), mark(eb, yb, db, "zero_one")]
    w = int(cols[-1][0] + cols[-1][1] + LEFT)
    bottom = max(tb.y + tb.h for tb in placed.values())
    ly = bottom + 46
    legend = [mark(LEFT + 30, ly, -1, "one"), f'<path d="M{LEFT + 30},{ly} H{LEFT + 90}" stroke="{INK2}" '
              f'stroke-width="1.4"/>', mark(LEFT + 90, ly, 1, "many"),
              t(LEFT + 112, ly + 4, "one to many (bar: one, crow's foot: many)", 11, INK2, "start")]
    lx2 = LEFT + 112 + text_w("one to many (bar: one, crow's foot: many)", 11, False) + 40
    legend += [f'<path d="M{lx2},{ly} H{lx2 + 60}" stroke="{MUTED}" stroke-width="1.4" stroke-dasharray="5 4"/>',
               t(lx2 + 70, ly + 4, "name lookup through the province mapping", 11, INK2, "start")]
    lx3 = lx2 + 70 + text_w("name lookup through the province mapping", 11, False) + 40
    legend += [t(lx3, ly + 4, "PK primary key  ·  FK foreign key", 11, INK2, "start")]
    if zone == "gold":
        legend = legend[:4] + [mark(lx2, ly, -1, "one"),
                               f'<path d="M{lx2},{ly} H{lx2 + 60}" stroke="{INK2}" stroke-width="1.4"/>',
                               mark(lx2 + 60, ly, 1, "zero_one"),
                               t(lx2 + 92, ly + 4, "one to zero or one", 11, INK2, "start"), legend[-1]]
    if zone == "silver":
        fx = lx3 + text_w("PK primary key  ·  FK foreign key", 11, False) + 40
        for f, c in FLAG.items():
            fw = text_w(f, 9, True) + 10
            legend += [f'<rect x="{fx}" y="{ly - 7}" width="{fw:.1f}" height="14" rx="7" fill="{c}"/>',
                       t(fx + fw / 2, ly + 3.5, f, 9, "#ffffff", "middle", 700, mono=True)]
            fx += fw + 8
        legend.append(t(fx + 4, ly + 4, "column added or changed by DP2", 11, INK2, "start"))
    head = [t(LEFT, 42, f"{zone.upper()} ZONE · ENTITY-RELATIONSHIP", 11, INK2, "start", 700, mono=True,
              spacing=0.08), t(LEFT, 66, desc, 14, INK, "start")]
    body = "\n".join(head + lines + [tb.svg() for tb in placed.values()] + marks + legend)
    return P.svg_doc(w, int(ly + 34), body, f"{zone} zone ER diagram", desc)


if __name__ == "__main__":
    for z in sys.argv[1:] or VIEWS:
        P.render(z, view(z), "er-")
