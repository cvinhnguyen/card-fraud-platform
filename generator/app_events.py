"""Streaming source 2: mobile banking app events, one session at a time. Used by generate.py.

Domain rule: Decision 2345/QĐ-NHNN (from 2024-07-01) asks for an identity biometric check on a new
device, for a transfer >= 10M VND and when the day's total goes above 20M VND.
Fraud: account takeover (ATO) attempts of two kinds. `new_device`: password reset, login on the
fraudster's phone, mule payee, one large transfer. `remote_access` (after the cutoff): malware on the
victim's own phone, so no new-device check; transfers are split just below 10M.
Problem: ~15% of events reach Kafka after the next event of their session (parallel uploads).
"""
import numpy as np
import pandas as pd

COLS = ["session_id", "customer_id", "event_type", "device_id", "is_new_device", "event_ts",
        "amount_vnd", "payee_id", "transfer_type", "auth_method", "result", "reason"]


class Session:
    """Collects the events of one session; each event advances the clock by a random UI gap (seconds)."""

    def __init__(self, rng, sid: str, cid: str, device: str, is_new: bool, t: float):
        self.rng, self.sid, self.cid, self.device, self.is_new, self.t, self.rows = rng, sid, cid, device, is_new, t, []

    def add(self, event_type: str, gap=(2, 15), **f) -> None:
        self.t += self.rng.uniform(*gap)
        self.rows.append((self.sid, self.cid, event_type, self.device, self.is_new, self.t, f.get("amount_vnd"),
                          f.get("payee_id"), f.get("transfer_type"), f.get("auth_method"), f.get("result"), f.get("reason")))


def gen_app_day(rng, cfg: dict, day: pd.Timestamp, n: int, first_session: int, ctx: dict) -> tuple[pd.DataFrame, list]:
    """All sessions that start on `day`, in event order. Returns the events and the ATO (session_id, kind) truth."""
    a, bio = cfg["app_events"], cfg["app_events"]["biometric"]
    after = day >= pd.Timestamp(bio["cutoff"])
    is_ato = rng.random(n) < a["ato"]["share"]
    # ATO ignores the daily rhythm, so it is relatively more common at night.
    hour = np.where(is_ato, rng.integers(0, 24, n), rng.choice(24, n, p=ctx["hour_p"]))
    start = day.value / 1e9 + hour * 3600 + rng.uniform(0, 3600, n)
    eligible = int(np.searchsorted(ctx["onboard_sorted"], day.to_datetime64()))   # onboarded before the day
    cust = ctx["onboard_order"][rng.integers(0, eligible, n)]
    years = (day - pd.Timestamp(cfg["period"]["start"])).days / 365.25
    qr_share = min(0.9, a["qr_share"]["start"] * (1 + a["qr_share"]["growth_per_year"]) ** years)
    today: dict[str, list] = {}   # customer -> [successful transfer total, identity checked above the daily total]
    rows, truth = [], []

    def verify(s: Session, reason: str, pass_rate: float) -> bool:
        ok = rng.random() < pass_rate
        s.add("biometric_verify", (3, 15), result="success" if ok else "failed", reason=reason)
        return ok

    def transfer(s: Session, amount: int, payee: str, kind: str, auth: str, pass_rate: float) -> bool:
        """Decision 2345 check before the transfer; a failed check ends the attempt."""
        day_state = today.setdefault(s.cid, [0, False])
        reason = None
        if after and amount >= bio["single_transfer_vnd"]:
            reason = "amount"
        elif after and day_state[0] + amount > bio["daily_total_vnd"] and not day_state[1]:
            reason = "daily_total"
        if reason and not verify(s, reason, pass_rate):
            return False
        # ponytail: one identity check covers the rest of the day once the total is above the limit.
        day_state[1] |= reason is not None and day_state[0] + amount > bio["daily_total_vnd"]
        day_state[0] += amount
        s.add("transfer", (2, 10) if reason else (4, 18), amount_vnd=amount, payee_id=payee, transfer_type=kind, auth_method=auth)
        return True

    def legit(sid: str, c: int, t: float) -> None:
        cid, (first, count) = ctx["customer_ids"][c], (ctx["dev_first"][c], ctx["dev_count"][c])
        k = 0 if count == 1 or rng.random() < cfg["devices"]["primary_share"] else rng.integers(1, count)
        device = ctx["dev_ids"][first + k]
        s = Session(rng, sid, cid, device, ctx["last_device"].get(cid) != device, t)
        if rng.random() < a["password_reset_share"]:
            s.add("password_reset", (0, 0))
            s.add("login", (20, 90))
        else:
            for _ in range(rng.integers(1, 3) if rng.random() < a["failed_login_share"] else 0):
                s.add("failed_login", (3, 10))
            s.add("login", (2, 10))
        if after and s.is_new and not verify(s, "new_device", bio["pass_rate_legit"]):
            rows.extend(s.rows)
            return
        ctx["last_device"][cid] = device
        if rng.random() < a["change_phone_share"]:
            s.add("change_phone")
        payees = ctx["payees"].setdefault(cid, [])
        n_tr = rng.choice(list(a["transfers_per_session"]), p=list(a["transfers_per_session"].values()))
        for _ in range(n_tr):
            auth = rng.choice(list(a["transfer_auth_method"]), p=list(a["transfer_auth_method"].values()))
            if rng.random() < qr_share:   # VietQR at a shop: small amounts
                payee, kind = ctx["shops"][rng.choice(len(ctx["shops"]), p=ctx["shop_p"])], "qr_merchant"
                amount = max(10_000, round(rng.lognormal(np.log(60_000), 0.9), -3))
            else:
                kind = "p2p"
                if not payees or rng.random() < a["new_payee_share"]:
                    payee = f"pay_{rng.integers(2**32):08x}"
                    payees.append(payee)
                    s.add("add_payee", payee_id=payee)
                else:
                    payee = payees[rng.integers(len(payees))]
                amount = max(10_000, round(rng.lognormal(np.log(800_000), 1.3), -3))
            if not transfer(s, int(amount), payee, kind, auth, bio["pass_rate_legit"]):
                break
        rows.extend(s.rows)

    def ato(sid: str, c: int, t: float) -> None:
        cid, at = ctx["customer_ids"][c], a["ato"]
        remote = after and rng.random() < at["remote_share"]
        if remote:   # malware drives the victim's own phone
            s = Session(rng, sid, cid, ctx["last_device"].get(cid, ctx["dev_ids"][ctx["dev_first"][c]]), False, t)
            s.add("login", (0, 0))
        else:
            s = Session(rng, sid, cid, ctx["attackers"][rng.integers(len(ctx["attackers"]))], True, t)
            for _ in range(rng.integers(1, 4) if rng.random() < 0.5 else 0):
                s.add("failed_login", (3, 10))
            s.add("password_reset", (0, 0) if not s.rows else (3, 10))
            s.add("login", (20, 60))
            if after and not verify(s, "new_device", bio["pass_rate_ato"]):
                rows.extend(s.rows)
                truth.append((sid, "new_device"))
                return
            ctx["last_device"][cid] = s.device   # the victim's next login looks like a new device
            if rng.random() < 0.3:
                s.add("change_phone", (5, 30))   # OTPs now go to the fraudster
        mule = ctx["mules"][rng.integers(len(ctx["mules"]))]
        s.add("add_payee", (10, 40), payee_id=mule)
        if remote:   # just below the 10M check, until the 20M daily total triggers one
            for _ in range(5):
                if not transfer(s, int(rng.integers(9_000, 10_000)) * 1000, mule, "p2p", "smart_otp_pin", bio["pass_rate_ato"]):
                    break
        else:
            transfer(s, int(rng.integers(10_000, 50_000)) * 1000, mule, "p2p", "sms_otp", bio["pass_rate_ato"])
        rows.extend(s.rows)
        truth.append((sid, "remote_access" if remote else "new_device"))

    for i in np.argsort(start, kind="stable"):
        (ato if is_ato[i] else legit)(f"ses_{first_session + i:010x}", cust[i], start[i])
    df = pd.DataFrame(rows, columns=COLS).sort_values(["session_id", "event_ts"], ignore_index=True)
    # Fixed dtypes: an all-null column (e.g. `result` before the cutoff) would otherwise be written as Parquet `null`.
    text = [c for c in COLS if c not in ("is_new_device", "event_ts", "amount_vnd")]
    return df.astype({**{c: "string" for c in text}, "amount_vnd": "Int64", "is_new_device": bool}), truth


def arrival(rng, cfg: dict, df: pd.DataFrame) -> np.ndarray:
    """Problem: out-of-order events. The app uploads in parallel requests, so ~15% of events arrive after
    the next event of their session. A delayed event's neighbours are never delayed, so each one is
    overtaken exactly once and its delay stays <= max_disorder_s (a 30 s watermark covers it).
    Expects df sorted by session_id, event_ts; returns received_ts in epoch seconds."""
    a = cfg["app_events"]
    ev, n = df.event_ts.to_numpy(), len(df)
    base = rng.uniform(0.1, 1.0, n)   # normal upload latency; UI gaps are >= 2 s, so these stay in order
    gap = np.full(n, np.inf)
    same = df.session_id.to_numpy()[1:] == df.session_id.to_numpy()[:-1]
    gap[:-1] = np.where(same, ev[1:] - ev[:-1], np.inf)
    cand = np.flatnonzero(gap <= a["max_disorder_s"] - 2)
    chosen = np.zeros(n + 1, bool)   # +1: chosen[i + 1] is safe at the end
    target = round(a["out_of_order_share"] * n)
    k = 0
    for i in rng.permutation(cand):   # ponytail: greedy python loop over candidates, fine up to a few M events/day
        if k == target:
            break
        if not chosen[i - 1] and not chosen[i + 1]:
            chosen[i], k = True, k + 1
    assert k == target, f"only {k} of {target} out-of-order slots fit; UI gaps too long"
    idx = np.flatnonzero(chosen[:n])
    received = ev + base
    lo = gap[idx] + base[idx + 1] + 0.1   # strictly after the next event arrives
    received[idx] = ev[idx] + rng.uniform(lo, a["max_disorder_s"])
    return received
