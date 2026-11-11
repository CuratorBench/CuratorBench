# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy>=1.26", "pandas>=2.2", "scipy>=1.11", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""Span-level vault metrics with E4's own functions (code/analysis/e4_metrics.py)."""
import copy
import json
import os
import pathlib

import numpy as np
import pandas as pd

import context as C

import e4_metrics as E4

SUMMARY_KEYS = ("active_days", "return_days", "yield_before_fee", "yield_after_fee", "tl_headline",
                "L_p10", "L_median", "share_idle_mean")


def e4_stuck_n(path=None):
    """N of the stuck freeze of the E4 run under INPUTS, or None (depeg freeze only)."""
    path = path or (C.INPUTS / "metrics/e4/vault_daily.csv.meta.json")
    try:
        meta = json.loads(pathlib.Path(path).read_text())
    except (OSError, ValueError):
        return None
    return (meta.get("freeze_stuck") or {}).get("n_days")


class Bridge:
    """Per-process cache of E4 chain contexts and market assets."""

    def __init__(self, stuck_n="inputs"):
        self.ctx = {}
        self.assets = {}
        self.extra = {}
        self.stuck_n = e4_stuck_n() if stuck_n == "inputs" else stuck_n

    def chain(self, cid):
        """(E4 Ctx, Grid, depths) of one chain."""
        cid = int(cid)
        if cid not in self.ctx:
            ctx = E4.Ctx(pathlib.Path(C.INPUTS) / "events", [cid])
            ctx.build_families()
            ctx.build_crash()
            g = E4.Grid(E4.chain_heads()[cid])
            self.ctx[cid] = (ctx, g, ctx.scen.depths)
        return self.ctx[cid]

    def _euler(self, cid):
        key = ("euler", cid)
        if key not in self.extra:
            self.extra[key] = (E4.EulerGov(cid), E4.euler_distribution(cid))
        return self.extra[key]

    def _silo_anchors(self, cid):
        """Exact conversions of every Silo vault position on the chain, as E4's run_chain gathers them."""
        key = ("silo", cid)
        if key not in self.extra:
            _ctx, g, _d = self.chain(cid)
            out = {}
            d = E4.DATA / f"vaults/silo_v2/{cid}/positions"
            if d.exists():
                for fn in sorted(os.listdir(d)):
                    if fn.endswith(".csv"):
                        _pos, anc = E4.read_positions("silo_v2", d / fn, g)
                        for m, lst in anc.items():
                            out.setdefault(m, []).extend(lst)
            self.extra[key] = out
        return self.extra[key]

    def asset(self, proto, cid, m):
        """E4's asset for one market (None when E4 has no panel or config for it)."""
        cid, m = int(cid), m.lower()
        key = (proto, cid, m)
        if key not in self.assets:
            ctx, g, depths = self.chain(cid)
            a = None
            if proto in ("morpho", "moolah"):
                if (E4.DATA / f"panel/{proto}/{cid}/{m}.csv").exists():
                    a = E4.morpho_market_asset(ctx, proto, cid, m, g, depths)
            elif proto == "euler_v2":
                if (E4.DATA / f"panel/euler_v2/{cid}/{m}.csv").exists():
                    gov, dist = self._euler(cid)
                    a = E4.euler_vault_asset(ctx, cid, m, g, depths, gov, dist)
            elif proto == "silo_v2":
                if (cid, m) in ctx.silo_cfg:
                    a = E4.silo_market_asset(ctx, cid, m, g, depths, self._silo_anchors(cid))
            if a is not None and self.stuck_n:
                soc = self._socialized(cid) if proto == "euler_v2" else {}
                E4.stuck_freeze(ctx, a, proto, cid, m, g, self.stuck_n, soc)
            self.assets[key] = a
        return self.assets[key]

    def _socialized(self, cid):
        key = ("soc", cid)
        if key not in self.extra:
            self.extra[key] = E4.debt_socialized(cid)
        return self.extra[key]

    def freeze(self, a, proto, cid, m, aps_book):
        """Economic per-unit value from a book series, as E4 freezes the market."""
        _ctx, g, _d = self.chain(cid)
        trig = self.depeg_day(proto, cid, m)
        spells = [x for x in getattr(a, "spells", None) or [] if x.get("stuck")]
        if spells:
            dmask = np.zeros(g.n, dtype=bool)
            if trig is not None:
                dmask[max(int(trig - g.d0) + 1, 1):] = True
            smask = np.zeros(g.n, dtype=bool)
            for x in spells:
                smask[x["i0"] + 1:x["x"] + 1] = True
            if (smask & ~dmask).any():
                return E4.freeze_mask(aps_book, smask | dmask)
        if trig is not None:
            return E4.freeze_positive(aps_book, trig, g)
        return aps_book

    def depeg_day(self, proto, cid, m):
        ctx, _g, _d = self.chain(cid)
        t = ctx.scen.depeg_markets.get((proto, int(cid), m.lower()))
        return None if t is None else t // C.DAY


BRIDGE = Bridge()


def _held_asset(a, n, aps, ratio, liq):
    """Copy of E4 asset `a` with per-unit value `aps`, withdrawable liquidity `liq` and every loss term scaled by `ratio` (stre."""
    h = E4.Asset(a.key, a.kind, n)
    h.aps = aps
    h.liq = liq
    h.apy = a.apy
    h.fam = {k: v * ratio[:, None] for k, v in a.fam.items()}
    h.fam2 = {k: v * ratio[:, None] for k, v in a.fam2.items()}
    h.crash = a.crash * ratio
    h.crash2 = a.crash2 * ratio
    h.us = a.us * ratio
    for k in ("unk", "worst", "unpeg", "outside", "trackpeg", "v12same"):
        setattr(h, k, getattr(a, k))
    return h


def frame(proto, cid, days, idle, markets, usd_per_raw, key, loan_symbol="", loan_unit="",
          guard=None, bridge=BRIDGE):
    """E4's daily table of a vault given, per day end in `days` (consecutive day indexes), its idle cash and per market (positi."""
    ctx, g, depths = bridge.chain(cid)
    K = len(depths)
    n = g.n
    days = np.asarray(days, dtype=np.int64)
    i0 = int(days[0] - g.d0)
    i1 = i0 + len(days)
    if i0 < 0 or i1 > n:
        raise ValueError(f"days {days[0]}..{days[-1]} outside E4's grid {g.d0}..{g.d1}")
    sl = slice(i0, i1)
    holdings, notes = [], {"markets_outside_e4": []}
    for m, arrs in markets.items():
        pos, cfi, u = arrs[:3]
        liq_in = arrs[3] if len(arrs) > 3 else None
        pos = np.asarray(pos, dtype=float)
        if not (pos > 0).any():
            continue
        a = bridge.asset(proto, cid, m)
        if a is None:
            notes["markets_outside_e4"].append(m)
            continue
        cfi = np.asarray(cfi, dtype=float)
        u = np.asarray(u, dtype=float)
        book = np.asarray(getattr(a, "book", a.aps), dtype=float)
        rec = np.isfinite(cfi) & (cfi > 0)
        ratio = np.full(n, np.nan)
        ratio[i0:i1][rec] = cfi[rec] / book[i0:i1][rec]
        first = np.flatnonzero(np.isfinite(ratio))
        if not len(first):
            notes.setdefault("markets_without_value", []).append(m)
            continue
        ratio[:first[0]] = ratio[first[0]]
        ratio = pd.Series(ratio).ffill().values
        aps_book = book * ratio
        aps = bridge.freeze(a, proto, cid, m, aps_book)
        sh = np.zeros(n)
        sh[sl] = np.where(pos > 0, pos / aps_book[sl], 0.0)
        us_cf = np.zeros(n)
        us_cf[sl] = E4.stressed(np.where(rec, u, 0.0))
        with np.errstate(divide="ignore", invalid="ignore"):
            r = np.where(a.us > 0, us_cf / np.where(a.us > 0, a.us, 1.0), 0.0)
        r[:i0] = 1.0
        r[i1:] = 1.0
        liq = a.liq
        if liq_in is not None:
            q = np.asarray(liq_in, dtype=float)
            liq = np.array(a.liq, dtype=float)
            liq[sl] = np.where(np.isfinite(q), q, liq[sl])
        holdings.append((_held_asset(a, n, aps, r, liq), sh, None))
    idle_g = np.zeros(n)
    idle_g[sl] = np.asarray(idle, dtype=float)
    res = E4.combine(holdings, idle_g, False, n, K)
    upr = np.asarray(usd_per_raw, dtype=float)
    usd = res["V"][sl] * upr
    peak = np.nanmax(usd) if np.isfinite(usd).any() else 0.0
    floor = max(E4.MIN_USD, E4.PEAK_FRAC * peak)
    active = np.isfinite(usd) & (usd >= floor)
    zeros = np.zeros(len(days))
    d = {k: res[k][sl] for k in ("V", "W", "ret_before", "tl", "crash", "apy_gross", "worst_fam", "idle_share",
                                  "share_unk", "share_worst", "share_unpeg", "share_outside", "tl2", "crash2")}
    d.update({"ret_after": res["ret_before"][sl].copy(), "apy_net": res["apy_gross"][sl].copy(),
              "perf_fee": zeros, "mgmt_fee": zeros, "fee_assets": zeros, "active": active,
              "usd_per_raw": upr, "held_usd": zeros,
              "guard": np.zeros(len(days), dtype=bool) if guard is None else np.asarray(guard, dtype=bool),
              "first_day": int(days[0]),
              "meta": {"chain_id": int(cid), "vault": key, "protocol": proto, "kind": "autocurator",
                       "loan_unit": loan_unit, "asset_symbol": loan_symbol, "active_days": int(active.sum()),
                       "peak_usd": float(peak), "outside_share_mean": 0.0}})
    df = E4.vault_frame(int(cid), key, d, depths, {})
    notes["holdings"] = len(holdings)
    return df, notes


def summarize(df, d0, d1, meta):
    """E4's vault_summary on the rows of `df` dated d0 to d1 (day indexes); None when the vault has no active day there."""
    day = np.array([E4.day_of(x) for x in df["date"]], dtype=np.int64) if "day" not in df else df["day"].values
    sub = df[(day >= d0) & (day <= d1)]
    if sub.empty or int((sub["active"] == 1).sum()) == 0:
        return None
    depths = depths_of(df)
    row = E4.vault_summary(sub, meta, depths, {})
    return {k: row.get(k) for k in SUMMARY_KEYS}


def depths_of(df):
    """Depths of a daily table from its tl_dXXXX columns, in E4's order (the four fixed depths, then the headline)."""
    tags = [c[3:] for c in df.columns if c.startswith("tl_d") and c[4:].isdigit()]
    vals = [int(t[1:]) / 1000 for t in tags]
    fixed = [v for v in vals if round(v, 4) in E4.DEPTHS]
    head = [v for v in vals if round(v, 4) not in E4.DEPTHS]
    return tuple(sorted(fixed)) + tuple(head or [fixed[-1]])


def meta_of(row):
    """vault_summary's meta from an E4 scorecard row."""
    return {"chain_id": int(row["chain_id"]), "vault": row["vault"], "protocol": row["protocol"],
            "kind": row["kind"], "asset_symbol": row.get("asset_symbol"), "loan_unit": row.get("loan_unit"),
            "name": row.get("name")}


class HumanDaily:
    """E4's daily table (vault_daily.csv under INPUTS), one frame per vault."""

    def __init__(self, path=None, vaults=None):
        path = path or (C.INPUTS / "metrics/e4/vault_daily.csv")
        df = pd.read_csv(path, dtype={"vault": str, "protocol": str, "date": str, "curator_entity": str,
                                      "curator_confidence": str, "worst_family_headline": str,
                                      "apy_source": str, "kind": str})
        df["vault"] = df["vault"].str.lower()
        if vaults is not None:
            keys = {(p, int(c), v.lower()) for p, c, v in vaults}
            mask = [(p, int(c), v) in keys for p, c, v in zip(df["protocol"], df["chain_id"], df["vault"])]
            df = df[mask]
        dmap = {s: E4.day_of(s) for s in df["date"].unique()}
        df["day"] = df["date"].map(dmap).astype("int64")
        self.by = {(p, int(c), v): g.sort_values("day").reset_index(drop=True)
                   for (p, c, v), g in df.groupby(["protocol", "chain_id", "vault"])}

    def get(self, proto, cid, vault):
        return self.by.get((proto, int(cid), vault.lower()))

    def span(self, scorecard_row, d0, d1):
        """Span values of one human vault, or None without an active day."""
        df = self.get(scorecard_row["protocol"], scorecard_row["chain_id"], scorecard_row["vault"])
        if df is None:
            return None
        return summarize(df, d0, d1, meta_of(scorecard_row))


def copy_frame(df):
    return copy.deepcopy(df)
