# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy>=1.26", "pandas>=2.1", "scipy>=1.11", "matplotlib>=3.8", "requests>=2.31", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0"]
# ///
"""E4: daily vault metrics, vault and curator scorecards, rank agreement."""
import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import pickle
import re
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "code"))
sys.path.insert(0, str(ROOT / "code" / "data"))
import prices as e1_prices
from common import io_utils, outroot

GENERATOR = "code/analysis/e4_metrics.py"
DATA = ROOT / "data"
DERIVED = outroot.OUT_DATA
WRAPPER_DEPTH = float(os.environ.get("E4_WRAPPER_DEPEG", "0") or 0)
if not 0.0 <= WRAPPER_DEPTH <= 1.0:
    raise SystemExit(f"E4_WRAPPER_DEPEG={WRAPPER_DEPTH}: a depth in [0, 1]")
WRAPPER_DEPEG = WRAPPER_DEPTH > 0
WRAP_SUFFIX = f"_wrapper_d{round(WRAPPER_DEPTH * 1000):04d}" if WRAPPER_DEPEG else ""
OUT_REL = "metrics/e4" + WRAP_SUFFIX
OUT = DERIVED / OUT_REL
WORK = pathlib.Path(os.environ.get("E4_WORK", "/tmp/e4_metrics_work"))
DRAFTS = outroot.FIGURES / "drafts"
DAY = 86400
YEAR_S = 365 * DAY
D_START = dt.date(2024, 1, 1).toordinal() - dt.date(1970, 1, 1).toordinal()

CHAINS = {1: ("morpho", "euler_v2", "silo_v2"), 8453: ("morpho",), 4663: ("morpho",),
          42161: ("morpho", "silo_v2"), 747474: ("morpho",), 999: ("morpho",),
          56: ("moolah",), 9745: ("euler_v2",), 59144: ("euler_v2",),
          146: ("silo_v2",), 43114: ("silo_v2",)}

RHO = 0.9
ETA = 0.05
DEPTHS = (0.15, 0.30, 0.50, 1.00)
HEADLINE_DEPTH = 0.50
MIN_USD = 10_000.0
PEAK_FRAC = 0.01
MIN_ACTIVE_DAYS = 30
MAX_DAILY_RET = 0.03
MAX_OUTSIDE_SHARE = 0.05
APY_CAP = 1.0
CRASH_HORIZON_D = 7
CRASH_JUMP_D = 1
CRASH_PCT = 1.0
CRASH_MIN_OBS = 180
PRICE_END = dt.date(2026, 10, 4)
N_PERM = 10_000
N_BOOT = 2_000
SEED = 20261008
PHI = 0.25
PHI_SENS = (0.0, 0.5)
BLIND_RE = re.compile(r"a discount of (\S+) against (\S+)")
PROXY_RE = re.compile(r"a depeg of \S+ against (\S+)")
DEPEG_RE = re.compile(r"a depeg of (\S+)")

ZERO = "0x" + "0" * 40
MARKET_ADAPTERS = {"MorphoMarketV1Adapter", "MorphoMarketV1AdapterV2"}
WRAPPER_RULES = {"wsteth", "susde", "savings", "wrapped_usd", "weeth", "woeth",
                 "wsuperoeth", "wrs_eth", "xpufeth", "xsolvbtc"}
PT_RE = re.compile(r"(?:n-)?PT-(.+?)-(?:\d{1,2}[A-Z]{3}\d{4}|\d{10})(?:-\([A-Z]+\))?", re.I)
UNIT_SERIES = {
    "ETH": (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
    "BTC": (1, "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"),
    "EUR": (1, "0x1abaea1f7c830bd89acc67ec4af516284b1bc33c"),
    "X:BNB": (56, "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"),
    "X:HYPE": (999, "0x5555555555555555555555555555555555555555"),
    "X:AVAX": (43114, "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"),
    "X:S": (146, "0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38"),
}
LOAN_UNITS = ("USD", "ETH", "BTC", "EUR", "X:BNB", "X:HYPE")
UNIT_ALIASES = {"X:WHYPE": "X:HYPE", "X:WBNB": "X:BNB"}
REFERENCE_SYMBOLS = {"ETH": {"ETH", "WETH"}, "BTC": {"BTC", "WBTC"}, "EUR": {"EURC"},
                     "X:BNB": {"BNB", "WBNB"}, "X:HYPE": {"HYPE", "WHYPE"}, "X:AVAX": {"AVAX", "WAVAX"}, "X:S": {"S", "WS"}}
SELF_DEALING_CONTROLLER = (8453, "0x6708e21113922ed588bbccaa5ef756becbb2a7d1")


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def day_str(d):
    return (dt.date(1970, 1, 1) + dt.timedelta(days=int(d))).isoformat()


def day_of(datestr):
    return dt.date.fromisoformat(datestr).toordinal() - dt.date(1970, 1, 1).toordinal()


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def jsonl(path):
    with open(path) as fh:
        return [json.loads(x) for x in fh if x.strip()]


def file_stamp(path):
    """sha256 and the sidecar's written_utc of an input file."""
    side = pathlib.Path(str(path) + ".meta.json")
    written = json.loads(side.read_text()).get("written_utc") if side.exists() else None
    return {"path": str(path), "sha256": sha256(path), "written_utc": written}


def chain_heads():
    """{chain: last complete UTC day} from the E3 heads (the earliest pair head on the chain, minus the partial head day)."""
    meta = json.loads((DERIVED / "aum/vault_daily.csv.meta.json").read_text())["heads"]
    ends = {}
    for key, h in meta.items():
        cid = int(key.split(":")[1])
        d = h["timestamp"] // DAY - 1
        ends[cid] = min(ends.get(cid, d), d)
    return ends


class Grid:
    def __init__(self, end_day):
        self.d0 = D_START
        self.d1 = end_day
        self.days = np.arange(self.d0, self.d1 + 1, dtype=np.int64)
        self.n = len(self.days)


def eod_index(row_days, g):
    """Index of the last row on or before each grid day's end (-1 if none)."""
    return np.searchsorted(row_days, g.days, side="right") - 1


def ffill_on_grid(row_days, values, g, before=0.0):
    idx = eod_index(row_days, g)
    out = np.full(g.n, before, dtype=float)
    ok = idx >= 0
    if len(values):
        out[ok] = np.asarray(values, dtype=float)[idx[ok]]
    return out


def next_value(row_days, values, g):
    """Value of the first row strictly after each grid day (the last row when none follows; 0 when there is no row)."""
    if len(row_days) == 0:
        return np.zeros(g.n)
    j = np.searchsorted(row_days, g.days, side="right")
    j = np.minimum(j, len(row_days) - 1)
    return np.asarray(values, dtype=float)[j]


class TokenBook:
    """Symbol and peg unit of every token."""

    def __init__(self, events_dir):
        self.rows = {}
        for r in jsonl(DATA / "prices/tokens.jsonl"):
            self.rows[(r["chain_id"], r["token"].lower())] = r
        sup = events_dir / "tokens_supplement.jsonl"
        if sup.exists():
            for r in jsonl(sup):
                self.rows.setdefault((r["chain_id"], r["token"].lower()), r)
        self.hints = {}
        self.price_unit = {}

    def hint(self, cid, token, symbol):
        if token and symbol:
            self.hints.setdefault((cid, token.lower()), symbol)

    def symbol(self, cid, token):
        r = self.rows.get((cid, token))
        return (r or {}).get("symbol") or self.hints.get((cid, token)) or ""

    def peg(self, cid, token):
        r = self.rows.get((cid, token))
        if r and r.get("peg_family") not in (None, "unknown"):
            return r["peg_family"], r.get("reference")
        sym = self.symbol(cid, token)
        fam, ref, _rule = e1_prices.classify({"chain_id": cid, "token": token, "symbol": sym})
        return fam, ref

    def unit(self, cid, token):
        """USD, ETH, BTC, EUR, X:<reference> for other known assets, else the unit its own price tracks (infer_price_units), None if unknown."""
        fam, ref = self.peg(cid, token)
        if fam == "unknown" and (cid, token) in self.price_unit:
            return self.price_unit[(cid, token)]
        if fam == "usd_stable":
            return "USD"
        if fam == "eth":
            return "ETH"
        if fam == "btc":
            return "BTC"
        if fam == "other":
            if ref == "EUR":
                return "EUR"
            u = "X:" + (ref or self.symbol(cid, token) or token)
            return UNIT_ALIASES.get(u, u)
        return None


def underlying_symbol(sym):
    """Symbol of the asset a wrapper unwraps to, by E1's symbol rules (wstETH -> stETH, sUSDe -> USDe, sX -> X, wX -> X, PT-X-date -> X)."""
    sym = sym or ""
    m = PT_RE.fullmatch(sym)
    if m:
        return underlying_symbol(m.group(1))
    for name, pattern, _fam, ref in e1_prices.SYMBOL_RULES:
        if re.fullmatch(pattern, sym, re.I):
            if name in WRAPPER_RULES:
                return ref or sym[1:]
            return sym
    return sym


class Families:
    """Collateral families on one chain: union of E2 families (which carry
    the census's contagion links) and tokens sharing an underlying symbol."""

    def __init__(self, cid, book, e2_rows):
        self.cid = cid
        self.book = book
        self.parent = {}
        self.e2 = {}
        self.label = {}
        for r in e2_rows:
            for m in r["members"]:
                if m["chain_id"] == cid:
                    self.e2[m["token"].lower()] = (r["family_id"], r["label"])

    def _find(self, x):
        while self.parent.setdefault(x, x) != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def _union(self, a, b):
        ra, rb = self._find(a), self._find(b)
        if ra != rb:
            self.parent[ra] = rb

    def add(self, token):
        node = "tok:" + token
        self._find(node)
        und = underlying_symbol(self.book.symbol(self.cid, token)).upper() or token
        self._union(node, "sym:" + und)
        if token in self.e2:
            self._union(node, "e2:" + self.e2[token][0])

    def finish(self):
        """Label each group: the E2 family label when the group has one, else symbol:<underlying symbol>."""
        groups = defaultdict(list)
        for node in list(self.parent):
            groups[self._find(node)].append(node)
        for root, nodes in groups.items():
            e2 = sorted(n for n in nodes if n.startswith("e2:"))
            syms = sorted(n for n in nodes if n.startswith("sym:"))
            if e2:
                lab = e2[0][3:]
            else:
                lab = "symbol:" + (syms[0][4:] if syms else root)
            for n in nodes:
                if n.startswith("tok:"):
                    self.label[n[4:]] = lab

    def of(self, token):
        return self.label[token]


PEG_BAND = 0.03
PEG_MIN_DAYS = 30
PEG_MIN_SHARE = 0.95


def infer_price_units(book, tokens_by_chain, events_dir):
    """Unit of tokens E1's symbol rules leave unknown (Katana vault-bridge tokens, fund tokens)."""
    keys = set(UNIT_SERIES.values())
    todo = []
    for cid, toks in tokens_by_chain.items():
        for tok in toks:
            if book.peg(cid, tok)[0] == "unknown":
                todo.append((cid, tok))
                keys.add((cid, tok))
    if not todo:
        return
    px = load_price_series(keys, events_dir)
    ref = {"USD": None, **{u: px.get(UNIT_SERIES[u], {}) for u in ("ETH", "BTC", "EUR")}}
    for cid, tok in todo:
        own = px.get((cid, tok), {})
        best = None
        for unit, series in ref.items():
            dev = []
            for d, p in own.items():
                r = p if series is None else (p / series[d] if series.get(d) else None)
                if r is not None:
                    dev.append(abs(r - 1.0))
            if len(dev) < PEG_MIN_DAYS:
                continue
            dev = np.array(dev)
            if (dev <= PEG_BAND).mean() >= PEG_MIN_SHARE:
                score = float(np.median(dev))
                if best is None or score < best[0]:
                    best = (score, unit)
        if best:
            book.price_unit[(cid, tok)] = best[1]

def load_price_series(keys, events_dir):
    """{(chain, token): {day."""
    want = defaultdict(set)
    for cid, tok in keys:
        want[cid].add(tok)
    out = defaultdict(dict)
    for cid, toks in want.items():
        p = DATA / f"prices/{cid}.csv"
        if p.exists():
            df = pd.read_csv(p, usecols=["date", "token", "price_usd"], dtype=str)
            df = df[df.token.isin(toks) & df.price_usd.notna()]
            for tok, sub in df.groupby("token"):
                out[(cid, tok)].update({day_of(d): float(v) for d, v in zip(sub.date, sub.price_usd)})
    sup = events_dir / "prices_supplement.csv"
    if sup.exists():
        df = pd.read_csv(sup, usecols=["date", "chain_id", "token", "price_usd"], dtype=str)
        df = df[df.price_usd.notna()]
        for (cid, tok), sub in df.groupby(["chain_id", "token"]):
            key = (int(cid), tok)
            if tok in want.get(int(cid), ()):
                for d, v in zip(sub.date, sub.price_usd):
                    out[key].setdefault(day_of(d), float(v))
    return out


def ratio_returns(series_c, series_l, horizon):
    """[(day, return)] of `horizon`-day returns of p_c / p_l, calendar days, over 2024-01-01 to PRICE_END."""
    end = PRICE_END.toordinal() - dt.date(1970, 1, 1).toordinal()
    rets = []
    for d in range(D_START + horizon, end + 1):
        a = series_c(d)
        b = series_c(d - horizon)
        c = series_l(d)
        e = series_l(d - horizon)
        if None in (a, b, c, e) or b <= 0 or c <= 0 or e <= 0:
            continue
        rets.append((d, (a / c) / (b / e) - 1.0))
    return rets


def crash_depth(series_c, series_l):
    """-(1st percentile of 7-day returns of p_c / p_l); None with fewer than CRASH_MIN_OBS returns."""
    rets = ratio_returns(series_c, series_l, CRASH_HORIZON_D)
    if len(rets) < CRASH_MIN_OBS:
        return None, len(rets)
    return float(-np.percentile([r for _d, r in rets], CRASH_PCT)), len(rets)


def crash_jump(series_c, series_l):
    """Largest one-day drop of p_c / p_l and its day; None with fewer than CRASH_MIN_OBS returns."""
    rets = ratio_returns(series_c, series_l, CRASH_JUMP_D)
    if len(rets) < CRASH_MIN_OBS:
        return None, None, len(rets)
    d, r = min(rets, key=lambda x: x[1])
    return float(-r), int(d), len(rets)


class Scenarios:
    """Headline depth from E2, crash depths per (collateral unit series, loan unit)."""

    def __init__(self, events_dir):
        self.events_dir = events_dir
        ev = jsonl(events_dir / "events.jsonl")
        depths = sorted(1.0 - e["min_market_price"] for e in ev
                        if e["type"] == "depeg_loss" and e.get("min_market_price") is not None)
        self.event_depths = depths
        self.event_median_depth = float(np.median(depths)) if depths else None
        self.headline = HEADLINE_DEPTH
        self.depths = DEPTHS + (HEADLINE_DEPTH,)
        self.depeg_markets = {}
        for e in ev:
            if e["type"] != "depeg_loss":
                continue
            for m in e["affected_markets"]:
                key = (m["protocol"], m["chain_id"], m["market"].lower())
                t = m.get("first_trigger_ts") or e["onset_ts"]
                self.depeg_markets[key] = min(t, self.depeg_markets.get(key, t))
        self.prices = None
        self.crash = {}

    def series_key(self, unit, cid, token):
        if unit == "USD":
            return "USD"
        if unit in UNIT_SERIES:
            return unit
        return f"tok:{cid}:{token}"

    def prepare_crash(self, needed):
        """needed: set of (collateral series key, loan unit)."""
        keys = set(UNIT_SERIES.values())
        for sk, _lu in needed:
            if sk.startswith("tok:"):
                _, cid, tok = sk.split(":")
                keys.add((int(cid), tok))
        self.prices = load_price_series(keys, self.events_dir)

        def series(sk):
            if sk == "USD":
                return lambda d: 1.0
            if sk in UNIT_SERIES:
                s = self.prices.get(UNIT_SERIES[sk], {})
            else:
                _, cid, tok = sk.split(":")
                s = self.prices.get((int(cid), tok), {})
            return s.get

        base, _ = crash_depth(series("ETH"), series("USD"))
        base_jump, base_day, _ = crash_jump(series("ETH"), series("USD"))
        for sk, lu in sorted(needed):
            if (sk, lu) in self.crash:
                continue
            d, nobs = crash_depth(series(sk), series(lu))
            j, jday, nj = crash_jump(series(sk), series(lu))
            self.crash[(sk, lu)] = {"depth": d if d is not None else base,
                                    "n_returns": nobs, "fallback": d is None,
                                    "jump": j if j is not None else base_jump,
                                    "jump_date": day_str(jday if j is not None else base_day),
                                    "jump_n_returns": nj, "jump_fallback": j is None}


def stressed(u):
    u = np.clip(np.nan_to_num(u, nan=0.0), 0.0, 1.0)
    return u / (1.0 - RHO * (1.0 - u))


def loss_rate(lltv, delta, mode):
    """Per unit of debt at the given LLTV (scalar or array)."""
    lltv = np.asarray(lltv, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        if mode == "hard":
            r = 1.0 - (1.0 - delta) / lltv
        else:
            r = 1.0 - (1.0 - delta) * (1.0 - ETA) / lltv
    return np.where(lltv > 0, np.maximum(r, 0.0), 0.0)


def oracle_mode(otype, subtype):
    if otype in ("tracks_market", "min_of_two"):
        return "track"
    if otype == "exchange_rate" and subtype == "underlying_at_own_feed":
        return "track"
    if otype in ("exchange_rate", "fixed_price"):
        return "hard"
    return "worst"


class Asset:
    """Something a vault holds, as daily arrays."""

    def __init__(self, key, kind, n):
        self.key = key
        self.kind = kind
        self.aps = np.zeros(n)
        self.liq = np.full(n, np.inf)
        self.apy = np.zeros(n)
        self.fam = {}
        self.crash = np.zeros(n)
        self.unk = np.zeros(n)
        self.worst = np.zeros(n)
        self.unpeg = np.zeros(n)
        self.outside = np.zeros(n)
        self.trackpeg = np.zeros(n)
        self.us = np.zeros(n)
        self.v12same = np.zeros(n)
        self.fam2 = {}
        self.crash2 = np.zeros(n)


def freeze_positive(aps, from_day, g):
    """Economic assets per share."""
    i0 = int(from_day - g.d0) + 1
    if i0 >= g.n:
        return aps
    i0 = max(i0, 1)
    out = aps.copy()
    for i in range(i0, g.n):
        prev, cur = aps[i - 1], aps[i]
        if prev > 0 and cur < prev:
            out[i] = out[i - 1] * cur / prev
        else:
            out[i] = out[i - 1]
    return out


STUCK_U = 0.99
STUCK_EXITS = ("end", "write_off")
STUCK_MAX_REPAY_30D = 0.10
STUCK_N_HEADLINE = 30
STUCK_N = STUCK_N_HEADLINE
SERIES = f"series_stuck_n{STUCK_N}{WRAP_SUFFIX}"


def freeze_mask(aps, mask):
    """Economic assets per share under a boolean day mask."""
    out = aps.copy()
    started = False
    for i in range(1, len(aps)):
        prev, cur = aps[i - 1], aps[i]
        if mask[i]:
            started = True
            if prev > 0 and cur < prev:
                out[i] = out[i - 1] * cur / prev
            else:
                out[i] = out[i - 1]
        elif started:
            out[i] = out[i - 1] * cur / prev if prev > 0 else out[i - 1]
    return out


def debt_socialized(cid):
    """{Euler lending vault."""
    soc = defaultdict(list)
    for r in jsonl(DATA / f"panel/euler_v2/{cid}/losses.jsonl"):
        if r["event"] == "DebtSocialized":
            soc[r["vault"].lower()].append((int(r["timestamp"]), float(r["assets"])))
    return soc


def _grid_sum(day, vals, g):
    out = np.zeros(g.n)
    i = np.asarray(day, dtype=np.int64) - g.d0
    vals = np.asarray(vals, dtype=float)
    ok = (i >= 0) & (i < g.n)
    np.add.at(out, i[ok], vals[ok])
    return out


def market_flows(proto, cid, m, g, socialized):
    """Daily debt flows of a panel market on the grid, raw loan units."""
    if proto in ("morpho", "moolah"):
        df = pd.read_csv(DATA / f"panel/{proto}/{cid}/{m}.csv",
                         usecols=["timestamp", "kind", "total_supply_assets", "total_borrow_assets"],
                         dtype={"timestamp": np.int64, "kind": str, "total_supply_assets": float,
                                "total_borrow_assets": float})
        day = df.timestamp.values // DAY
        S, B = df.total_supply_assets.values, df.total_borrow_assets.values
        dS, dB = np.diff(S, prepend=0.0), np.diff(B, prepend=0.0)
        k = df.kind.values
        liq = k == "liquidate"
        rep = np.where(k == "repay", -dB, 0.0) + np.where(liq, dS - dB, 0.0)
        wo = np.where(liq, -dS, 0.0)
        dep = np.where((k == "supply") | (k == "withdraw"), dS, 0.0)
        return {"rep": _grid_sum(day, rep, g), "wo": _grid_sum(day, wo, g), "dep": _grid_sum(day, dep, g),
                "B": ffill_on_grid(day, B, g)}
    if proto == "euler_v2":
        df = pd.read_csv(DATA / f"panel/euler_v2/{cid}/{m}.csv",
                         usecols=["timestamp", "totalBorrows", "cash", "interestAccumulator"],
                         dtype={"timestamp": np.int64, "totalBorrows": float, "cash": float,
                                "interestAccumulator": float})
        day = df.timestamp.values // DAY
        B, C, A = df.totalBorrows.values, df.cash.values, df.interestAccumulator.values
        Bp = np.concatenate([[0.0], B[:-1]])
        Ap = np.concatenate([A[:1], A[:-1]])
        nb = B - Bp - Bp * np.where(Ap > 0, A / np.where(Ap > 0, Ap, 1.0) - 1.0, 0.0)
        ev = socialized.get(m, [])
        wo = _grid_sum([t // DAY for t, _a in ev], [x for _t, x in ev], g)
        rep = np.maximum(_grid_sum(day, np.maximum(-nb, 0.0), g) - wo, 0.0)
        dep = _grid_sum(day, np.diff(C, prepend=0.0) + nb, g) + wo
        return {"rep": rep, "wo": wo, "dep": dep, "B": ffill_on_grid(day, B, g)}
    days, rep, wo, dep, debt = [], [], [], [], []
    with open(DATA / f"panel/silo_v2/{cid}/{m}.csv", newline="") as fh:
        for r in csv.DictReader(fh):
            ev = r["event"]
            amt = float(int(r["amount"])) if r["amount"] else 0.0
            days.append(int(r["ts"]) // DAY)
            debt.append(float(int(r["debt_assets"])))
            rep.append(amt if ev == "Repay" else 0.0)
            wo.append(amt if ev == "DefaultingLiquidation" else 0.0)
            dep.append(amt if ev == "Deposit" else -amt if ev == "Withdraw" else 0.0)
    days = np.array(days, dtype=np.int64)
    return {"rep": _grid_sum(days, rep, g), "wo": _grid_sum(days, wo, g), "dep": _grid_sum(days, dep, g),
            "B": ffill_on_grid(days, debt, g)}


def pinned_spells(u, f):
    """Maximal runs of grid days with end-of-day utilization >= STUCK_U and how each ends."""
    pin = np.nan_to_num(u, nan=0.0) >= STUCK_U
    idx = np.flatnonzero(pin)
    out = []
    if not len(idx):
        return out
    cut = np.flatnonzero(np.diff(idx) > 1)
    for i0, i1 in zip(np.r_[idx[0], idx[cut + 1]], np.r_[idx[cut], idx[-1]]):
        i0, i1 = int(i0), int(i1)
        rec = {"i0": i0, "i1": i1, "days": i1 - i0 + 1, "debt_start": float(f["B"][i0]),
               "debt_last": float(f["B"][i1]), "repaid_in": float(f["rep"][i0 + 1:i1 + 1].sum()),
               "written_off_in": float(f["wo"][i0 + 1:i1 + 1].sum())}
        if i1 == len(u) - 1:
            rec.update(x=i1, exit="end", exit_repaid=0.0, exit_written_off=0.0, exit_supply=0.0)
        else:
            x = i1 + 1
            rp, w, dp = float(f["rep"][x]), float(f["wo"][x]), max(float(f["dep"][x]), 0.0)
            kind = ("write_off" if w > 0 and w >= rp and w >= dp else "repay" if rp > 0 and rp >= dp
                    else "supply" if dp > 0 else "other")
            rec.update(x=x, exit=kind, exit_repaid=rp, exit_written_off=w, exit_supply=dp)
        out.append(rec)
    return out


def stuck_freeze(ctx, a, proto, cid, m, g, n_min, socialized):
    """Record the pinned spells of market asset `a` (a.spells) and freeze its value per share over the stuck ones on top of E2'."""
    sp = pinned_spells(a.u, market_flows(proto, cid, m, g, socialized))
    a.spells = sp
    trig = ctx.scen.depeg_markets.get((proto, cid, m))
    a.depeg_day = None if trig is None else int(trig // DAY)
    a.depeg_i0 = g.n if trig is None else max(int(trig // DAY - g.d0) + 1, 1)
    if not sp:
        return
    dmask = np.zeros(g.n, dtype=bool)
    dmask[a.depeg_i0:] = True
    smask = np.zeros(g.n, dtype=bool)
    for s in sp:
        s["repaid_per_30d"] = (s["repaid_in"] / s["debt_start"] * 30.0 / s["days"]
                               if s["debt_start"] > 0 else float("inf"))
        s["stuck"] = bool(s["days"] >= n_min and s["exit"] in STUCK_EXITS
                          and s["repaid_per_30d"] < STUCK_MAX_REPAY_30D)
        win = np.zeros(g.n, dtype=bool)
        win[s["i0"] + 1:s["x"] + 1] = True
        s["window_days"] = int(win.sum())
        s["depeg_frozen_days"] = int((win & dmask).sum())
        s["new_frozen_days"] = int((win & ~dmask).sum()) if s["stuck"] else 0
        if s["stuck"]:
            smask |= win
    if (smask & ~dmask).any():
        a.aps = freeze_mask(getattr(a, "book", a.aps), smask | dmask)


def e2_depeg_tokens(events_dir):
    """(chain, token) of every member of an E2 depeg-loss collateral family, and the members' underlying symbols (upper case)."""
    fams = set()
    for e in jsonl(events_dir / "events.jsonl"):
        if e["type"] == "depeg_loss":
            fams.update(e.get("families") or [])
    toks, syms = set(), set()
    for r in jsonl(events_dir / "collateral_families.jsonl"):
        if r["family_id"] in fams:
            toks.update((int(x["chain_id"]), x["token"].lower()) for x in r["members"])
            syms.update(underlying_symbol(x.get("symbol")).upper() for x in r["members"] if x.get("symbol"))
    return toks, syms


def market_tokens(ctx, proto, cid, m, i0, g, dist):
    """(loan token, main collateral token at day index i0) of a panel market; Euler."""
    if proto in ("morpho", "moolah"):
        r = ctx.markets.get((proto, cid, m)) or {}
        return (r.get("loan_token") or "").lower(), (r.get("collateral_token") or "").lower()
    if proto == "silo_v2":
        cfg = ctx.silo_cfg.get((cid, m), {})
        pcfg = ctx.silo_cfg.get((cid, (cfg.get("paired_silo") or "").lower()), {})
        return (cfg.get("token") or "").lower(), (pcfg.get("token") or "").lower()
    loan = ctx.euler_asset.get((cid, m)) or ""
    sub = dist.get(m)
    coll = ""
    if sub is not None:
        sub = sub[sub.collateral_vault.notna() & (sub.d <= g.d0 + i0)]
        if len(sub):
            last = sub[sub.d == sub.d.max()]
            cv = last.groupby("collateral_vault").attributed.sum().idxmax().lower()
            coll = ctx.euler_asset.get((cid, cv)) or cv
    return loan.lower(), coll.lower()


def stuck_rows(ctx, cid, g, assets, holders, inside, upr, dist, e2tok):
    """Per pinned spell of every built market (`spells`) and per (spell, sample vault holding the market directly) (`exposure`)."""
    spells, expo = [], []
    usd0 = {}
    for v, hs in holders.items():
        if v not in inside:
            continue
        p = upr[v]
        for a, sh, mask in hs:
            q = np.ones(g.n) if mask is None else mask.astype(float)
            book = getattr(a, "book", a.aps)
            econ = sh * a.aps * q * p
            bval = sh * book * q * p
            for j, s in enumerate(a.spells):
                w = slice(s["i0"], s["x"] + 1)
                if not np.nansum(econ[w]) > 0:
                    continue
                wn = slice(s["i0"], max(s["i0"], min(s["x"] + 1, a.depeg_i0)))
                key = (a.key[1], a.key[2], j)
                usd0.setdefault(key, p[s["i0"]])
                expo.append({"chain_id": cid, "protocol": a.key[1], "market": a.key[2], "spell": j, "vault": v,
                             "usd_days": float(np.nansum(econ[w])), "peak_usd": float(np.nanmax(econ[w])),
                             "usd_days_outside_depeg_freeze": float(np.nansum(econ[wn])),
                             "book_over_econ_usd_at_exit": float(bval[s["x"]] - econ[s["x"]])})
    for (proto, m), a in assets.items():
        if a is None or not getattr(a, "spells", None):
            continue
        for j, s in enumerate(a.spells):
            loan, coll = market_tokens(ctx, proto, cid, m, s["i0"], g, dist)
            csym = ctx.book.symbol(cid, coll) if coll else ""
            p = usd0.get((proto, m, j), np.nan)
            spells.append({
                "chain_id": cid, "protocol": proto, "market": m, "spell": j,
                "loan_symbol": ctx.book.symbol(cid, loan) if loan else "",
                "collateral_symbol": csym, "collateral_token": coll,
                "collateral_in_e2_depeg_family": (cid, coll) in e2tok[0]
                or bool(csym and underlying_symbol(csym).upper() in e2tok[1]),
                "start": day_str(g.d0 + s["i0"]), "last_pinned": day_str(g.d0 + s["i1"]),
                "exit_day": "" if s["exit"] == "end" else day_str(g.d0 + s["x"]),
                "days": s["days"], "exit": s["exit"], "stuck": s.get("stuck", False),
                "in_depeg_set": a.depeg_day is not None,
                "depeg_trigger": "" if a.depeg_day is None else day_str(a.depeg_day),
                "window_days": s.get("window_days", 0), "depeg_frozen_days": s.get("depeg_frozen_days", 0),
                "new_frozen_days": s.get("new_frozen_days", 0),
                "debt_start_raw": s["debt_start"], "debt_start_usd": s["debt_start"] * p,
                "repaid_share_per_30d": s.get("repaid_per_30d", np.nan),
                "repaid_in_spell_raw": s["repaid_in"], "written_off_in_spell_raw": s["written_off_in"],
                "exit_repaid_raw": s["exit_repaid"], "exit_written_off_raw": s["exit_written_off"],
                "exit_supply_raw": s["exit_supply"], "usd_per_raw": p})
    return {"spells": spells, "exposure": expo}


def sees_family_v2(otype, blind_to, und):
    """Oracle-aware loss: does the oracle reprice the collateral when its family's underlying `und` falls? tracks_market and min_of_two do."""
    if otype in ("tracks_market", "min_of_two"):
        return True
    if otype != "exchange_rate" or not blind_to:
        return False
    against = {y.upper() for _x, y in BLIND_RE.findall(blind_to)}
    depegs = {x.upper() for x in DEPEG_RE.findall(blind_to)}
    return und in against and und not in depegs


def proxy_reads_reference(otype, subtype, blind_to, unit):
    """An oracle classified other/proxy_feed_other_asset whose feed prices the crash's reference asset itself (cbBTC or WBTC by."""
    if otype != "other" or subtype != "proxy_feed_other_asset" or not blind_to or not unit:
        return False
    m = PROXY_RE.search(blind_to)
    ref = unit[2:] if unit.startswith("X:") else unit
    return bool(m) and m.group(1).upper() == ref.upper()


def loss_rate_v2(lltv, delta, sees, phi):
    """Oracle-aware loss per unit of debt."""
    lltv = np.asarray(lltv, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        if sees:
            r = 1.0 - (1.0 - phi * delta) * (1.0 - ETA) / lltv
        else:
            r = 1.0 - (1.0 - delta) / lltv
    return np.where(lltv > 0, np.maximum(r, 0.0), 0.0)


class Collateral:
    """Risk of lending against one collateral token in a market of `loan`."""

    def __init__(self, ctx, cid, token, loan, otype, subtype, blind_to=None):
        self.token = token
        self.otype = otype or "missing"
        self.mode = oracle_mode(otype, subtype)
        self.none = token in (None, "", ZERO)
        loan_unit = ctx.book.unit(cid, loan)
        self.cunit = None if self.none else ctx.book.unit(cid, token)
        self.same = (not self.none) and (token == loan or
                                         ctx.families[cid].of(token) == ctx.families[cid].of(loan))
        self.unknown = (not self.none) and self.cunit is None
        self.pegged = (not self.none) and (self.unknown or self.cunit == loan_unit)
        self.family = None if self.none else ctx.families[cid].of(token)
        und = underlying_symbol(ctx.book.symbol(cid, token)).upper() if not self.none else ""
        self.sees_v2 = sees_family_v2(self.otype, blind_to, und)
        self.sees_crash_v2 = self.otype not in ("fixed_price", "other", "none", "unresolved", "missing") \
            or proxy_reads_reference(self.otype, subtype, blind_to, self.cunit)
        self.same_v1_v2 = self.pegged and self.mode == "hard" and not self.sees_v2
        self.crash_depth = self.crash_jump = None
        if not self.none and not self.pegged and loan_unit in LOAN_UNITS:
            sk = ctx.scen.series_key(self.cunit, cid, token)
            self.crash_depth = ctx.scen.crash[(sk, loan_unit)]["depth"]
            self.crash_jump = ctx.scen.crash[(sk, loan_unit)]["jump"]
        self.wrapper = (WRAPPER_DEPEG and self.crash_depth is not None
                        and self.cunit in REFERENCE_SYMBOLS
                        and und not in REFERENCE_SYMBOLS[self.cunit])

    def losses(self, us, lltv, depths):
        """(family loss n x K, crash loss n) per unit of supply, given stressed utilization (times the debt share it backs) and LLTV arrays."""
        n = len(us)
        fam = None
        crash = np.zeros(n)
        if self.none or self.same:
            return fam, crash
        if self.pegged:
            mode = "track" if self.mode == "worst" else self.mode
            fam = np.stack([us * loss_rate(lltv, dlt, mode) for dlt in depths], axis=1)
        elif self.crash_depth is not None:
            crash = us * loss_rate(lltv, self.crash_depth, "track")
        return fam, crash

    def losses_v2(self, us, lltv, depth_phi):
        """Oracle-aware: (family loss n x len(depth_phi), crash loss n) per unit of supply."""
        n = len(us)
        fam = None
        crash = np.zeros(n)
        if self.none or self.same:
            return fam, crash
        if self.pegged:
            fam = np.stack([us * loss_rate_v2(lltv, dlt, self.sees_v2, phi) for dlt, phi in depth_phi],
                           axis=1)
        elif self.wrapper:
            fam = np.stack([us * loss_rate_v2(lltv, min(dlt, WRAPPER_DEPTH), self.sees_v2, phi)
                            for dlt, phi in depth_phi], axis=1)
        if not self.pegged and self.crash_depth is not None:
            if self.sees_crash_v2:
                crash = us * loss_rate_v2(lltv, self.crash_jump, True, 1.0)
            else:
                crash = us * loss_rate_v2(lltv, self.crash_depth, False, 1.0)
        return fam, crash


def depth_phi(depths):
    """Oracle-aware columns."""
    return [(d, PHI) for d in depths] + [(depths[-1], p) for p in PHI_SENS]


def single_collateral_asset(a, coll, u, lltv, depths):
    """Fill risk fields of a market with one collateral."""
    us = stressed(u)
    a.us = us
    fam, crash = coll.losses(us, lltv, depths)
    if fam is not None:
        a.fam[coll.family] = fam
    a.crash = crash
    fam2, crash2 = coll.losses_v2(us, lltv, depth_phi(depths))
    if fam2 is not None:
        a.fam2[coll.family] = fam2
    a.crash2 = crash2
    if not coll.none and not coll.same:
        a.unk[:] = 1.0 if coll.unknown else 0.0
        a.worst[:] = 1.0 if coll.mode == "worst" else 0.0
        a.unpeg[:] = 0.0 if coll.pegged else 1.0
        a.trackpeg[:] = 1.0 if (coll.pegged and not coll.unknown and coll.otype == "tracks_market") else 0.0
        a.v12same[:] = 1.0 if coll.same_v1_v2 else 0.0
    return a


def supply_apy(borrow_rate_per_s, u, fee):
    """exp(r T) - 1 borrow APY times utilization times one minus the fee."""
    bapy = np.expm1(np.clip(borrow_rate_per_s, 0, None) * YEAR_S)
    return bapy * np.clip(np.nan_to_num(u), 0, None) * (1.0 - fee)


class Ctx:
    """Inputs shared by every chain."""

    def __init__(self, events_dir, cids):
        self.events_dir = events_dir
        self.cids = tuple(cids)
        self.book = TokenBook(events_dir)
        self.scen = Scenarios(events_dir)
        self.e2_fams = jsonl(events_dir / "collateral_families.jsonl")
        self.families = {}
        self.oracles = {}
        self.markets = {}
        self.silo_cfg = {}
        self.euler_asset = {}
        self.vault_reg = {}
        self._load_registries()

    def _load_registries(self):
        for cid in self.cids:
            for proto in CHAINS[cid]:
                for r in jsonl(DATA / f"registry/vaults/{proto}/{cid}.jsonl"):
                    self.vault_reg[(cid, r["vault"].lower())] = {**r, "protocol": proto}
                    self.book.hint(cid, r.get("asset"), None)
                if proto in ("morpho", "moolah"):
                    for r in jsonl(DATA / f"registry/markets/{proto}/{cid}.jsonl"):
                        self.markets[(proto, cid, r["market_id"].lower())] = r
                    for r in jsonl(DATA / f"registry/oracles/{proto}/{cid}.jsonl"):
                        self.oracles[(proto, cid, r["market_id"].lower())] = (r["type"], r.get("subtype"),
                                                                              r.get("blind_to"))
                        self.book.hint(cid, r.get("collateral_token"), r.get("collateral_symbol"))
                        self.book.hint(cid, r.get("loan_token"), r.get("loan_symbol"))
                elif proto == "euler_v2":
                    for r in jsonl(DATA / f"registry/markets/euler_v2/{cid}.jsonl"):
                        if r.get("type") == "vault":
                            self.euler_asset[(cid, r["vault"].lower())] = r["asset"].lower()
                    for r in jsonl(DATA / f"registry/oracles/euler_v2/{cid}.jsonl"):
                        key = ("euler_v2", cid, r["vault"].lower(), r["collateral_vault"].lower())
                        self.oracles[key] = (r["type"], r.get("subtype"), r.get("blind_to"))
                        self.book.hint(cid, r.get("collateral_token"), r.get("collateral_symbol"))
                        self.book.hint(cid, r.get("loan_token"), r.get("loan_symbol"))
                elif proto == "silo_v2":
                    for r in jsonl(DATA / f"panel/silo_v2/{cid}/configs.jsonl"):
                        self.silo_cfg[(cid, r["silo"].lower())] = r
                        self.book.hint(cid, r.get("token"), r.get("symbol"))
                    for r in jsonl(DATA / f"registry/oracles/silo_v2/{cid}.jsonl"):
                        so = r.get("solvency_oracle")
                        st = so.get("subtype") if isinstance(so, dict) else None
                        bt = so.get("blind_to") if isinstance(so, dict) else None
                        self.oracles[("silo_v2", cid, r["silo"].lower())] = (r["type"], st, bt)
                        self.book.hint(cid, r.get("token"), r.get("token_symbol"))
        for f in ("vault_daily.csv", "lending_daily.csv"):
            df = pd.read_csv(DERIVED / "aum" / f, usecols=["chain_id", "asset", "asset_symbol"],
                             dtype=str).drop_duplicates()
            for cid, tok, sym in df.itertuples(index=False):
                if int(cid) in self.cids:
                    self.book.hint(int(cid), tok, sym)

    def collateral_tokens(self, cid):
        toks = set()
        for (proto, c, _m), r in self.markets.items():
            if c == cid:
                toks.update({r["collateral_token"].lower(), r["loan_token"].lower()})
        for (c, _v), a in self.euler_asset.items():
            if c == cid:
                toks.add(a)
        for (c, _s), r in self.silo_cfg.items():
            if c == cid:
                toks.add(r["token"].lower())
        for (c, _v), r in self.vault_reg.items():
            if c == cid and r.get("asset"):
                toks.add(r["asset"].lower())
        toks.discard(ZERO)
        return toks

    def build_families(self):
        infer_price_units(self.book, {cid: self.collateral_tokens(cid) for cid in self.cids},
                          self.events_dir)
        for cid in self.cids:
            f = Families(cid, self.book, self.e2_fams)
            for tok in self.collateral_tokens(cid):
                f.add(tok)
            f.finish()
            self.families[cid] = f

    def build_crash(self):
        needed = set()
        for cid in self.cids:
            for tok in self.collateral_tokens(cid):
                u = self.book.unit(cid, tok)
                if u is None:
                    continue
                for lu in LOAN_UNITS:
                    if u != lu:
                        needed.add((self.scen.series_key(u, cid, tok), lu))
        self.scen.prepare_crash(needed)


def excluded_vaults():
    """{(chain, vault): reason}."""
    p = DERIVED / "aum/excluded_vaults.jsonl"
    if p.exists():
        out = {}
        for r in jsonl(p):
            rs = r.get("reasons") or ["owner_funded"]
            hand = [x.split(":")[0] for x in rs if not x.startswith("owner_funded")]
            out[(int(r["chain_id"]), r["vault"].lower())] = hand[0] if hand else rs[0]
        return out
    cid, ctrl = SELF_DEALING_CONTROLLER
    out = {}
    res = subprocess.run(["grep", "-i", ctrl, str(DATA / f"vaults/morpho/{cid}/roles.jsonl")],
                         capture_output=True, text=True)
    for line in res.stdout.splitlines():
        r = json.loads(line)
        if r["address"].lower() == ctrl and r["role"] in ("owner", "curator", "allocator"):
            out[(cid, r["vault"].lower())] = "self_dealing_loop"
    return out


def e3_vault_daily(cid, g):
    """{vault: dict of grid arrays} from data/aum/vault_daily.csv."""
    df = pd.read_csv(DERIVED / "aum/vault_daily.csv", dtype=str)
    df = df[df.chain_id == str(cid)]
    return _e3_arrays(df, g, "kind")


def e3_lending_daily(cid, g):
    df = pd.read_csv(DERIVED / "aum/lending_daily.csv", dtype=str)
    df = df[df.chain_id == str(cid)]
    return _e3_arrays(df, g, None)


def _e3_arrays(df, g, kind_col):
    out = {}
    for vault, sub in df.groupby("vault", sort=False):
        days = np.array([day_of(x) for x in sub.date], dtype=np.int64)
        order = np.argsort(days, kind="stable")
        sub = sub.iloc[order]
        days = days[order]
        ta = sub.total_assets.astype(float).values
        ts_col = "total_supply" if "total_supply" in sub else "total_shares"
        sup = sub[ts_col].astype(float).values
        cnt = sub.counted_assets.astype(float).values
        aum = pd.to_numeric(sub.aum_usd, errors="coerce").values
        held = pd.to_numeric(sub.held_by_vaults_usd, errors="coerce").fillna(0).values
        upr = np.where((cnt > 0) & np.isfinite(aum) & (aum > 0), aum / np.where(cnt > 0, cnt, 1), np.nan)
        upr_s = pd.Series(upr).ffill().bfill().values
        idx = eod_index(days, g)
        ok = idx >= 0
        exact = np.zeros(g.n, dtype=bool)
        pos = days - g.d0
        inr = (pos >= 0) & (pos < g.n)
        exact[pos[inr]] = True

        def take(arr, fill=0.0):
            o = np.full(g.n, fill, dtype=float)
            o[ok] = arr[idx[ok]]
            return o
        rec = {"ta": take(ta), "supply": take(sup), "usd_per_raw": take(upr_s, np.nan),
               "held_usd": np.where(exact, take(held), 0.0), "has_row": exact,
               "guard": np.zeros(g.n, dtype=bool), "first_day": int(days[0]),
               "asset": sub.asset.iloc[0].lower(), "asset_symbol": sub.asset_symbol.iloc[0]}
        gd = (sub.guard.fillna("") != "").values
        gpos = pos[gd]
        gpos = gpos[(gpos >= 0) & (gpos < g.n)]
        rec["guard"][gpos] = True
        if kind_col:
            rec["kind"] = sub[kind_col].iloc[0]
            rec["source_event"] = np.zeros(g.n, dtype=bool)
            ev = (sub.source == "accrue_interest").values
            epos = pos[ev]
            epos = epos[(epos >= 0) & (epos < g.n)]
            rec["source_event"][epos] = True
        else:
            rec["kind"] = "euler_lending"
            gov = sub.governed.isin(["1", "True", "true"]).values
            rec["governed"] = np.zeros(g.n, dtype=bool)
            gp = pos[gov]
            gp = gp[(gp >= 0) & (gp < g.n)]
            rec["governed"][gp] = True
        out[vault.lower()] = rec
    return out


def morpho_market_asset(ctx, proto, cid, mid, g, depths):
    path = DATA / f"panel/{proto}/{cid}/{mid}.csv"
    a = Asset(("mkt", proto, mid), "market", g.n)
    df = pd.read_csv(path, usecols=["timestamp", "kind", "total_supply_assets", "total_supply_shares",
                                    "total_borrow_assets", "borrow_rate", "fee"],
                     dtype={"timestamp": np.int64, "kind": str, "total_supply_assets": float,
                            "total_supply_shares": float, "total_borrow_assets": float,
                            "borrow_rate": float, "fee": float})
    day = (df.timestamp.values // DAY).astype(np.int64)
    sup = ffill_on_grid(day, df.total_supply_assets.values, g)
    shs = ffill_on_grid(day, df.total_supply_shares.values, g)
    bor = ffill_on_grid(day, df.total_borrow_assets.values, g)
    a.aps = (sup + 1.0) / (shs + 1e6)
    a.liq = np.maximum(sup - bor, 0.0)
    u = np.where(sup > 0, bor / np.where(sup > 0, sup, 1), 0.0)
    feemask = df.fee.notna().values
    fee = ffill_on_grid(day[feemask], df.fee.values[feemask] / 1e18, g)
    acc = (df.kind == "accrue").values & df.borrow_rate.notna().values
    rate = next_value(day[acc], df.borrow_rate.values[acc] / 1e18, g)
    a.apy = supply_apy(rate, u, fee)
    a.u = u
    reg = ctx.markets.get((proto, cid, mid))
    if reg is None:
        return a
    otype, sub, blind = ctx.oracles.get((proto, cid, mid), (None, None, None))
    coll = Collateral(ctx, cid, reg["collateral_token"].lower(), reg["loan_token"].lower(), otype, sub, blind)
    lltv = np.full(g.n, int(reg["lltv"]) / 1e18)
    single_collateral_asset(a, coll, u, lltv, depths)
    a.book = a.aps
    trig = ctx.scen.depeg_markets.get((proto, cid, mid))
    if trig is not None:
        a.aps = freeze_positive(a.aps, trig // DAY, g)
    return a


class EulerGov:
    """Interest fee and liquidation LTV schedules of Euler lending vaults."""

    def __init__(self, cid):
        self.fee = defaultdict(list)
        self.ltv = defaultdict(list)
        for r in jsonl(DATA / f"panel/euler_v2/{cid}/governance.jsonl"):
            v = r["vault"].lower()
            if r["event"] == "EVaultCreated":
                self.fee[v].append((r["timestamp"], 0.1))
            elif r["event"] == "GovSetInterestFee":
                self.fee[v].append((r["timestamp"], int(r["newFee"]) / 1e4))
            elif r["event"] == "GovSetLTV":
                self.ltv[(v, r["collateral"].lower())].append(r)

    def fee_on_grid(self, v, g):
        ev = sorted(self.fee.get(v, []))
        if not ev:
            return np.full(g.n, 0.1)
        return ffill_on_grid(np.array([t // DAY for t, _ in ev]), [f for _, f in ev], g, before=0.1)

    def lltv_on_grid(self, v, c, g):
        rows = sorted(self.ltv.get((v, c), []), key=lambda r: (r["block"], r["log_index"]))
        out = np.zeros(g.n)
        if not rows:
            return out
        ends = (g.days + 1) * DAY - 1
        idx = np.searchsorted(np.array([r["timestamp"] for r in rows]), ends, side="right") - 1
        for i in range(g.n):
            if idx[i] < 0:
                continue
            r = rows[idx[i]]
            tgt = r["liquidationLTV"] / 1e4
            ini = r.get("initialLiquidationLTV", 0) / 1e4
            t1, ramp = r.get("targetTimestamp", 0), r.get("rampDuration", 0)
            if ends[i] >= t1 or tgt >= ini or not ramp:
                out[i] = tgt
            else:
                frac = (t1 - ends[i]) / ramp
                out[i] = tgt + (ini - tgt) * min(max(frac, 0.0), 1.0)
        return out


def euler_distribution(cid):
    """{lending vault: DataFrame(day, totalBorrows, collateral_vault, attributed)}."""
    df = pd.read_csv(DATA / f"vaults/euler_v2/{cid}/borrow_distribution.csv",
                     usecols=["vault", "day", "totalBorrows", "collateral_vault", "attributed"],
                     dtype={"vault": str, "day": str, "totalBorrows": float,
                            "collateral_vault": str, "attributed": float})
    df["d"] = [day_of(x) for x in df.day]
    return {v.lower(): sub for v, sub in df.groupby("vault")}


def euler_vault_asset(ctx, cid, v, g, depths, gov, dist):
    """An Euler lending vault as a holding (cash + borrows, shares) with its collateral mix."""
    path = DATA / f"panel/euler_v2/{cid}/{v}.csv"
    a = Asset(("mkt", "euler_v2", v), "evault", g.n)
    df = pd.read_csv(path, usecols=["timestamp", "totalShares", "totalBorrows", "cash",
                                    "interestAccumulator", "interestRate"],
                     dtype={"timestamp": np.int64, "totalShares": float, "totalBorrows": float,
                            "cash": float, "interestAccumulator": float, "interestRate": float})
    day = (df.timestamp.values // DAY).astype(np.int64)
    cash = ffill_on_grid(day, df.cash.values, g)
    bor = ffill_on_grid(day, df.totalBorrows.values, g)
    shs = ffill_on_grid(day, df.totalShares.values, g)
    acc = ffill_on_grid(day, df.interestAccumulator.values, g, before=1e27)
    rate = ffill_on_grid(day, df.interestRate.values / 1e27, g)
    sup = cash + bor
    a.aps = (sup + 1e6) / (shs + 1e6)
    a.liq = cash
    u = np.where(sup > 0, bor / np.where(sup > 0, sup, 1), 0.0)
    fee = gov.fee_on_grid(v, g)
    a.apy = supply_apy(rate, u, fee)
    a.u, a.sup, a.bor, a.acc, a.fee, a.rate = u, sup, bor, acc, fee, rate
    loan = ctx.euler_asset.get((cid, v))
    us = stressed(u)
    a.us = us
    sub = dist.get(v)
    if sub is not None and loan:
        days_all = np.sort(sub.d.unique()).astype(np.int64)
        piv = sub[sub.collateral_vault.notna()].pivot_table(
            index="d", columns="collateral_vault", values="attributed", aggfunc="sum")
        piv = piv.reindex(days_all).fillna(0.0)
        tb = sub.drop_duplicates("d").set_index("d").totalBorrows.reindex(days_all)
        tb_g = ffill_on_grid(days_all, tb.values, g)
        for cv in piv.columns:
            att = ffill_on_grid(days_all, piv[cv].values, g)
            cv = cv.lower()
            f = np.where(tb_g > 0, att / np.where(tb_g > 0, tb_g, 1), 0.0)
            if not f.any():
                continue
            ctok = ctx.euler_asset.get((cid, cv))
            otype, st, blind = ctx.oracles.get(("euler_v2", cid, v, cv), (None, None, None))
            coll = Collateral(ctx, cid, ctok, loan, otype, st, blind)
            lltv = gov.lltv_on_grid(v, cv, g)
            fam, crash = coll.losses(us * f, lltv, depths)
            if fam is not None:
                a.fam[coll.family] = a.fam.get(coll.family, 0) + fam
            a.crash = a.crash + crash
            fam2, crash2 = coll.losses_v2(us * f, lltv, depth_phi(depths))
            if fam2 is not None:
                a.fam2[coll.family] = a.fam2.get(coll.family, 0) + fam2
            a.crash2 = a.crash2 + crash2
            if not coll.none and not coll.same:
                a.unk += f * (1.0 if coll.unknown else 0.0)
                a.worst += f * (1.0 if coll.mode == "worst" else 0.0)
                a.unpeg += f * (0.0 if coll.pegged else 1.0)
                a.trackpeg += f * (1.0 if (coll.pegged and not coll.unknown
                                           and coll.otype == "tracks_market") else 0.0)
                a.v12same += f * (1.0 if coll.same_v1_v2 else 0.0)
    a.book = a.aps
    trig = ctx.scen.depeg_markets.get(("euler_v2", cid, v))
    if trig is not None:
        a.aps = freeze_positive(a.aps, trig // DAY, g)
    return a


def silo_market_asset(ctx, cid, silo, g, depths, anchors):
    """A Silo silo: collateral assets, debt, replayed share supply corrected to exact vault conversions."""
    path = DATA / f"panel/silo_v2/{cid}/{silo}.csv"
    a = Asset(("mkt", "silo_v2", silo), "silo", g.n)
    days, C, D, S, rdays, rapr = [], [], [], [], [], []
    shares, prev_c = 0, 0
    with open(path, newline="") as fh:
        rd = csv.DictReader(fh)
        for r in rd:
            ev = r["event"]
            if ev in ("Deposit", "Withdraw"):
                amt = int(r["amount"])
                num = shares + 1000
                den = (prev_c + 1) if shares > 0 else 1
                if ev == "Deposit":
                    shares += amt * num // den
                else:
                    shares = max(0, shares - (-(-amt * num // den)))
            coll = int(r["collateral_assets"])
            prev_c = coll
            d = int(r["ts"]) // DAY
            days.append(d)
            C.append(float(coll))
            D.append(float(int(r["debt_assets"])))
            S.append(float(shares))
            if ev == "AccruedInterest" and r["borrow_apr_realized"]:
                rdays.append(d)
                rapr.append(float(r["borrow_apr_realized"]))
    days = np.array(days, dtype=np.int64)
    c = ffill_on_grid(days, C, g)
    debt = ffill_on_grid(days, D, g)
    s = ffill_on_grid(days, S, g)
    rep = (c + 1.0) / (s + 1000.0)
    fac = np.ones(g.n)
    anc = sorted(anchors.get(silo, []))
    if anc:
        by_day = {}
        for _ts, d, rate in anc:
            by_day[d] = rate
        fd = np.array(sorted(by_day), dtype=np.int64)
        fv = []
        for d in fd:
            i = int(np.searchsorted(days, d, side="right")) - 1
            if i < 0:
                fv.append(1.0)
                continue
            r0 = (C[i] + 1.0) / (S[i] + 1000.0)
            fv.append(by_day[d] / r0 if r0 > 0 else 1.0)
        fac = ffill_on_grid(fd, fv, g, before=1.0)
    a.aps = rep * fac
    a.liq = np.maximum(c - debt, 0.0)
    u = np.where(c > 0, debt / np.where(c > 0, c, 1), np.where(debt > 0, 1.0, 0.0))
    cfg = ctx.silo_cfg.get((cid, silo), {})
    fee = (int(cfg.get("dao_fee") or 0) + int(cfg.get("deployer_fee") or 0)) / 1e18
    apr = next_value(np.array(rdays, dtype=np.int64), rapr, g)
    a.apy = np.expm1(np.clip(apr, 0, None)) * np.clip(u, 0, None) * (1.0 - fee)
    a.u = u
    pair = cfg.get("paired_silo", "").lower()
    pcfg = ctx.silo_cfg.get((cid, pair))
    if pcfg:
        otype, st, blind = ctx.oracles.get(("silo_v2", cid, pair), (None, None, None))
        coll = Collateral(ctx, cid, pcfg["token"].lower(), cfg["token"].lower(), otype, st, blind)
        lltv = np.full(g.n, int(pcfg.get("lt") or 0) / 1e18)
        single_collateral_asset(a, coll, u, lltv, depths)
    a.book = a.aps
    trig = ctx.scen.depeg_markets.get(("silo_v2", cid, silo))
    if trig is not None:
        a.aps = freeze_positive(a.aps, trig // DAY, g)
    return a


def anchor_asset(key, kind, anchors, g, outside):
    """A holding outside the panel, valued at its last exact conversion."""
    a = Asset(key, kind, g.n)
    anc = sorted(anchors)
    if anc:
        a.aps = ffill_on_grid(np.array([d for _t, d, _r in anc], dtype=np.int64),
                              [r for _t, _d, r in anc], g)
    a.outside[:] = 1.0 if outside else 0.0
    return a


def read_positions(proto, path, g):
    """{holding: daily share balance} and {holding."""
    if proto in ("morpho", "moolah"):
        df = pd.read_csv(path, usecols=["timestamp", "market_id", "vault_cum_shares"],
                         dtype={"timestamp": np.int64, "market_id": str, "vault_cum_shares": float})
        col, shc = "market_id", "vault_cum_shares"
    else:
        col = "strategy" if proto == "euler_v2" else "market"
        df = pd.read_csv(path, usecols=["timestamp", col, "kind", "assets", "event_shares", "cum_shares"],
                         dtype={"timestamp": np.int64, col: str, "kind": str, "assets": float,
                                "event_shares": float, "cum_shares": float})
        shc = "cum_shares"
    out, anchors = {}, {}
    if df.empty:
        return out, anchors
    df["day"] = df.timestamp // DAY
    for m, sub in df.groupby(col, sort=False):
        last = sub.drop_duplicates("day", keep="last")
        out[m.lower()] = ffill_on_grid(last.day.values, last[shc].values, g)
        if proto not in ("morpho", "moolah"):
            an = sub[sub.kind.isin(["Deposit", "Withdraw"]) & (sub.assets > 0) & (sub.event_shares > 0)]
            if len(an):
                anchors[m.lower()] = list(zip(an.timestamp.values.tolist(), an.day.values.tolist(),
                                              (an.assets / an.event_shares).values.tolist()))
    return out, anchors


def wrapper_positions(cid, g):
    """{v2 vault: {underlying."""
    p = DATA / f"vaults/morpho/{cid}/adapter_shares.csv"
    out = defaultdict(dict)
    if not p.exists():
        return out
    df = pd.read_csv(p, usecols=["vault", "adapter", "underlying_vault", "timestamp", "cum_shares"],
                     dtype={"vault": str, "adapter": str, "underlying_vault": str,
                            "timestamp": np.int64, "cum_shares": float})
    if df.empty:
        return out
    df["day"] = df.timestamp // DAY
    for (v, ad, u), sub in df.groupby(["vault", "adapter", "underlying_vault"], sort=False):
        last = sub.drop_duplicates("day", keep="last")
        arr = ffill_on_grid(last.day.values, last.cum_shares.values, g)
        key = u.lower()
        prev = out[v.lower()].get(key)
        out[v.lower()][key] = arr if prev is None else prev + arr
    return out


def adapter_support(cid, reg_vaults):
    """{v2 vault: True when every adapter it ever registered is reconstructed}."""
    p = DATA / f"vaults/morpho/{cid}/adapters.jsonl"
    ok = defaultdict(lambda: True)
    if not p.exists():
        return ok
    for r in jsonl(p):
        v = r["vault"].lower()
        und = (r.get("underlying_vault") or "").lower()
        good = r.get("type") in MARKET_ADAPTERS or (und and und in reg_vaults)
        ok[v] = ok[v] and bool(good)
    return ok


def fee_shares(proto, cid, g):
    """{vault: daily fee shares minted} from fees.csv."""
    p = DATA / f"vaults/{proto}/{cid}/fees.csv"
    head = next(csv.reader(open(p)))
    cols = [c for c in ("feeShares", "performanceFeeShares", "managementFeeShares", "protocolFeeShares")
            if c in head]
    df = pd.read_csv(p, usecols=["vault", "timestamp"] + cols,
                     dtype={"vault": str, "timestamp": np.int64, **{c: float for c in cols}})
    df["tot"] = df[cols].fillna(0).sum(axis=1)
    df = df[df.tot > 0]
    df["i"] = df.timestamp // DAY - g.d0
    df = df[(df.i >= 0) & (df.i < g.n)]
    out = {}
    for v, sub in df.groupby("vault"):
        arr = np.zeros(g.n)
        np.add.at(arr, sub.i.values.astype(np.int64), sub.tot.values)
        out[v.lower()] = arr
    return out


def fee_schedules(proto, cid, g):
    """{vault: (performance fee array, management fee per year array)} from the fee-setting events in actions.jsonl."""
    p = DATA / f"vaults/{proto}/{cid}/actions.jsonl"
    res = subprocess.run(["grep", "-F", "-e", '"SetFee"', "-e", '"SetPerformanceFee"',
                          "-e", '"SetManagementFee"', str(p)], capture_output=True, text=True)
    perf, mgmt = defaultdict(list), defaultdict(list)
    for line in res.stdout.splitlines():
        r = json.loads(line)
        f = r.get("fields") or r
        v = r["vault"].lower()
        key = (r["block"], r["log_index"])
        if r["event"] == "SetFee":
            perf[v].append((key, r["timestamp"], int(f["newFee"]) / 1e18))
        elif r["event"] == "SetPerformanceFee":
            perf[v].append((key, r["timestamp"], int(f["newPerformanceFee"]) / 1e18))
        elif r["event"] == "SetManagementFee":
            mgmt[v].append((key, r["timestamp"], int(f["newManagementFee"]) / 1e18 * YEAR_S))
    out = {}
    for v in set(perf) | set(mgmt):
        arrs = []
        for src in (perf, mgmt):
            ev = sorted(src.get(v, []))
            if ev:
                arrs.append(ffill_on_grid(np.array([t // DAY for _k, t, _f in ev], dtype=np.int64),
                                          [f for _k, _t, f in ev], g))
            else:
                arrs.append(np.zeros(g.n))
        out[v] = tuple(arrs)
    return out


def withdraw_queues(proto, cid, g):
    """{vault: {market: day-end membership in the withdraw queue}} from SetWithdrawQueue (emitted on every queue change, includ."""
    p = DATA / f"vaults/{proto}/{cid}/actions.jsonl"
    res = subprocess.run(["grep", "-F", '"SetWithdrawQueue"', str(p)], capture_output=True, text=True)
    events = defaultdict(list)
    for line in res.stdout.splitlines():
        r = json.loads(line)
        q = (r.get("fields") or r).get("newWithdrawQueue") or []
        events[r["vault"].lower()].append(((r["block"], r["log_index"]), r["timestamp"] // DAY,
                                           {x.lower() for x in q}))
    out = {}
    for v, ev in events.items():
        ev.sort(key=lambda x: x[0])
        days = np.array([d for _k, d, _q in ev], dtype=np.int64)
        idx = eod_index(days, g)
        members = set().union(*(q for _k, _d, q in ev))
        out[v] = {m: np.array([i >= 0 and m in ev[i][2] for i in idx]) for m in members}
    return out


def held_change(sh, aps, mask):
    """Value change over each day of the shares held at the start of it."""
    dp = aps[1:] - aps[:-1]
    if mask is None:
        return sh[:-1] * dp
    q0 = mask[:-1].astype(float)
    q1 = mask[1:].astype(float)
    return (q0 * q1 * sh[:-1] * dp + q0 * (1.0 - q1) * (sh[:-1] * dp - sh[1:] * aps[1:])
            + (1.0 - q0) * q1 * sh[:-1] * aps[1:])


def combine(holdings, idle, idle_outside, n, K):
    """Daily vault series from holdings [(Asset, shares, queue mask or None)] and an idle balance."""
    V = idle.copy()
    vals = []
    for a, sh, mask in holdings:
        v = sh * (a.aps if mask is None else a.aps * mask)
        vals.append(v)
        V += v
    W = idle.copy()
    num = np.zeros(n)
    for (a, sh, mask), v in zip(holdings, vals):
        W += np.minimum(v, a.liq)
        num[1:] += held_change(sh, a.aps, mask)
    with np.errstate(divide="ignore", invalid="ignore"):
        ret = np.full(n, np.nan)
        ret[1:] = np.where(V[:-1] > 0, num[1:] / V[:-1], np.nan)
        inv = np.where(V > 0, 1.0 / V, 0.0)
    out = {"V": V, "W": W, "ret_before": ret}
    fam, fam2 = {}, {}
    crash = np.zeros(n)
    crash2 = np.zeros(n)
    apy = np.zeros(n)
    flags = {k: np.zeros(n) for k in FLAGS}
    nfam = np.zeros(n)
    for (a, _sh, _m), v in zip(holdings, vals):
        al = v * inv
        for src, dst in ((a.fam, fam), (a.fam2, fam2)):
            for lab, arr in src.items():
                if lab in dst:
                    dst[lab] += al[:, None] * arr
                else:
                    dst[lab] = al[:, None] * arr
        crash += al * a.crash
        crash2 += al * a.crash2
        apy += al * a.apy
        for k in flags:
            flags[k] += al * getattr(a, k)
    flags["outside"] += (idle * inv) if idle_outside else 0.0
    if fam:
        nfam = (np.stack([fam[x] for x in fam], axis=0)[:, :, -1] > 0).sum(axis=0)
    out.update({"fam": fam, "tl": family_max(fam, n, K), "crash": crash, "apy_gross": apy,
                "worst_fam": worst_family(fam2, n, K - 1), "idle_share": idle * inv, "nfam": nfam,
                **{"share_" + k: v for k, v in flags.items()},
                "fam2": fam2, "tl2": family_max(fam2, n, K + len(PHI_SENS)), "crash2": crash2})
    return out


def worst_family(fam, n, col):
    """Label of the family with the largest loss in column `col` each day."""
    worst = np.array([""] * n, dtype=object)
    if fam:
        labs = sorted(fam)
        stack = np.stack([fam[x][:, col] for x in labs], axis=0)
        worst = np.array(labs, dtype=object)[stack.argmax(axis=0)]
        worst[stack.max(axis=0) <= 0] = ""
    return worst


def family_max(fam, n, k):
    """Worst family per day and column (zeros without exposure)."""
    if not fam:
        return np.zeros((n, k))
    return np.stack(list(fam.values()), axis=0).max(axis=0)


FLAGS = ("unk", "worst", "unpeg", "outside", "trackpeg", "us", "v12same")


def vault_as_asset(key, res, supply, n):
    """A computed vault as a holding of another vault (look-through)."""
    a = Asset(("vault", key), "vault", n)
    with np.errstate(divide="ignore", invalid="ignore"):
        a.aps = np.where(supply > 0, res["V"] / np.where(supply > 0, supply, 1), 0.0)
    a.liq = res["W"]
    a.apy = np.nan_to_num(res["apy_net"])
    a.fam = res["fam"]
    a.crash = res["crash"]
    a.fam2 = res["fam2"]
    a.crash2 = res["crash2"]
    for k in FLAGS:
        setattr(a, k, res["share_" + k])
    return a


def toposort(deps):
    """Held vaults before their holders; a cycle leaves the back edge unresolved (valued from E3 and counted as outside)."""
    order, state = [], {}

    def visit(v):
        if state.get(v):
            return
        state[v] = 1
        for u in sorted(deps.get(v, ())):
            if u in deps:
                visit(u)
        state[v] = 2
        order.append(v)
    for v in sorted(deps):
        visit(v)
    return order


def run_chain(job):
    """All daily vault series of one chain; writes WORK/series/<cid>.pkl."""
    cid, end_day, events_dir = job[:3]
    stuck_n = job[3] if len(job) > 3 else None
    t0 = time.time()
    ctx = Ctx(pathlib.Path(events_dir), [cid])
    ctx.build_families()
    ctx.build_crash()
    g = Grid(end_day)
    depths = ctx.scen.depths
    K = len(depths)
    e3 = e3_vault_daily(cid, g)
    reg = {v: r for (c, v), r in ctx.vault_reg.items() if c == cid}
    assets = {}
    positions, anchors_by_vault = {}, {}
    fees, sched, queues = {}, {}, {}
    for proto in CHAINS[cid]:
        d = DATA / f"vaults/{proto}/{cid}/positions"
        if d.exists():
            for fn in sorted(os.listdir(d)):
                if not fn.endswith(".csv"):
                    continue
                v = fn[:-4].lower()
                pos, anc = read_positions(proto, d / fn, g)
                if pos:
                    positions[v] = (proto, pos)
                    anchors_by_vault[v] = anc
        fees.update(fee_shares(proto, cid, g))
        sched.update(fee_schedules(proto, cid, g))
        queues.update(withdraw_queues(proto, cid, g))
    wraps = wrapper_positions(cid, g) if "morpho" in CHAINS[cid] else {}
    support = adapter_support(cid, set(reg)) if "morpho" in CHAINS[cid] else {}
    all_anchors = defaultdict(list)
    for v, anc in anchors_by_vault.items():
        for m, lst in anc.items():
            all_anchors[m].extend(lst)

    lend = e3_lending_daily(cid, g) if "euler_v2" in CHAINS[cid] else {}
    gov = EulerGov(cid) if "euler_v2" in CHAINS[cid] else None
    dist = euler_distribution(cid) if "euler_v2" in CHAINS[cid] else {}
    euler_panel = {fn[:-4].lower() for fn in os.listdir(DATA / f"panel/euler_v2/{cid}")
                   if fn.startswith("0x") and fn.endswith(".csv")} if "euler_v2" in CHAINS[cid] else set()
    silo_idle = set()
    if "silo_v2" in CHAINS[cid]:
        for r in jsonl(DATA / f"vaults/silo_v2/{cid}/vaults.jsonl"):
            silo_idle.update(x.lower() for x in (r.get("idle_vaults") or []))

    socialized = debt_socialized(cid) if stuck_n and "euler_v2" in CHAINS[cid] else {}
    holders = {}

    def market(proto, m):
        if (proto, m) in assets:
            return assets[(proto, m)]
        a = None
        if proto in ("morpho", "moolah"):
            if (DATA / f"panel/{proto}/{cid}/{m}.csv").exists():
                a = morpho_market_asset(ctx, proto, cid, m, g, depths)
        elif proto == "euler_v2":
            if m in euler_panel:
                a = euler_vault_asset(ctx, cid, m, g, depths, gov, dist)
        elif proto == "silo_v2":
            if m in silo_idle:
                a = anchor_asset(("idle", m), "idle", all_anchors.get(m, []), g, outside=False)
            elif (cid, m) in ctx.silo_cfg:
                a = silo_market_asset(ctx, cid, m, g, depths, all_anchors)
        if stuck_n and a is not None and a.kind in ("market", "evault", "silo"):
            stuck_freeze(ctx, a, proto, cid, m, g, stuck_n, socialized)
        assets[(proto, m)] = a
        return a

    deps = {}
    for v, (proto, pos) in positions.items():
        deps[v] = {m for m in pos if m in reg and m != v}
    for v, ws in wraps.items():
        deps.setdefault(v, set()).update(u for u in ws if u in reg and u != v)
    order = toposort(deps)
    results = {}
    vault_assets = {}
    for v in order:
        proto = positions[v][0] if v in positions else "morpho"
        pos = positions[v][1] if v in positions else {}
        holdings = []
        vq = queues.get(v)
        for m, sh in pos.items():
            if not sh.any():
                continue
            mask = None if vq is None else vq.get(m, np.zeros(g.n, dtype=bool))
            if m in reg and m != v:
                if m in vault_assets:
                    holdings.append((vault_assets[m], sh, mask))
                elif m in e3:
                    tmp = Asset(("vault", m), "vault_unresolved", g.n)
                    s = e3[m]["supply"]
                    tmp.aps = np.where(s > 0, e3[m]["ta"] / np.where(s > 0, s, 1), 0.0)
                    tmp.outside[:] = 1.0
                    holdings.append((tmp, sh, mask))
                continue
            a = market(proto, m)
            if a is None:
                a = anchor_asset(("outside", m), "outside", all_anchors.get(m, []), g, outside=True)
            holdings.append((a, sh, mask))
        for u, sh in wraps.get(v, {}).items():
            if u in vault_assets and sh.any():
                holdings.append((vault_assets[u], sh, None))
        info = e3.get(v)
        idle = np.zeros(g.n)
        idle_outside = False
        kind = (info or {}).get("kind") or reg.get(v, {}).get("kind")
        if kind == "morpho_v2" and info is not None:
            known = np.zeros(g.n)
            for a, sh, _m in holdings:
                known += sh * a.aps
            idle = np.maximum(info["ta"] - known, 0.0)
            idle_outside = not support[v]
        res = combine(holdings, idle, idle_outside, g.n, K)
        supply = info["supply"] if info is not None else np.zeros(g.n)
        perf, mgmt = sched.get(v, (np.zeros(g.n), np.zeros(g.n)))
        fsh = fees.get(v, np.zeros(g.n))
        with np.errstate(divide="ignore", invalid="ignore"):
            fee_assets = np.where(supply > 0, fsh * res["V"] / np.where(supply > 0, supply, 1), 0.0)
            ra = res["ret_before"].copy()
            ra[1:] = ra[1:] - np.where(res["V"][:-1] > 0, fee_assets[1:] / res["V"][:-1], 0.0)
        res["ret_after"] = ra
        res["fee_assets"] = fee_assets
        res["perf_fee"] = perf
        res["mgmt_fee"] = mgmt
        res["apy_net"] = res["apy_gross"] * (1.0 - perf) - mgmt
        res["proto"] = proto if v in positions else reg.get(v, {}).get("protocol", "morpho")
        res["kind"] = kind
        res["n_holdings"] = len(holdings)
        results[v] = res
        if stuck_n:
            holders[v] = [(h, s, q) for h, s, q in holdings if getattr(h, "spells", None)]
        vault_assets[v] = vault_as_asset(v, res, supply, g.n)

    lend_results = {}
    for v, info in lend.items():
        a = market("euler_v2", v)
        if a is None:
            continue
        n = g.n
        V = a.sup.copy()
        ra = np.full(n, np.nan)
        rb = np.full(n, np.nan)
        with np.errstate(divide="ignore", invalid="ignore"):
            ra[1:] = np.where(a.aps[:-1] > 0, a.aps[1:] / a.aps[:-1] - 1.0, np.nan)
            interest = a.bor[:-1] * np.where(a.acc[:-1] > 0, a.acc[1:] / a.acc[:-1] - 1.0, 0.0)
            rb[1:] = ra[1:] + a.fee[1:] * np.where(V[:-1] > 0, interest / V[:-1], 0.0)
        fam = a.fam
        tl = family_max(fam, n, K)
        worst = worst_family(a.fam2, n, K - 1)
        nfam = (np.stack(list(fam.values()), axis=0)[:, :, -1] > 0).sum(axis=0) if fam else np.zeros(n)
        bapy = np.expm1(np.clip(a.rate, 0, None) * YEAR_S) * a.u
        lend_results[v] = {
            "V": V, "W": a.liq.copy(), "ret_before": rb, "ret_after": ra, "fam": fam, "tl": tl,
            "crash": a.crash, "apy_gross": bapy, "apy_net": bapy * (1.0 - a.fee), "worst_fam": worst,
            "idle_share": np.zeros(n), "nfam": nfam, "share_unk": a.unk, "share_worst": a.worst,
            "share_unpeg": a.unpeg, "share_outside": np.zeros(n), "share_trackpeg": a.trackpeg,
            "share_us": a.us, "share_v12same": a.v12same, "perf_fee": a.fee, "mgmt_fee": np.zeros(n),
            "fee_assets": np.zeros(n), "proto": "euler_v2", "kind": "euler_lending", "n_holdings": 1,
            "fam2": a.fam2, "tl2": family_max(a.fam2, n, K + len(PHI_SENS)), "crash2": a.crash2}
        if stuck_n and getattr(a, "spells", None):
            book = getattr(a, "book", a.aps)
            holders[v] = [(a, np.where(book > 0, a.sup / np.where(book > 0, book, 1), 0.0), None)]

    out = {"cid": cid, "grid": (g.d0, g.d1), "depths": depths, "vaults": {}, "filtered": [],
           "seconds": None}
    excl = excluded_vaults()
    for src, table, is_lend in ((results, e3, False), (lend_results, lend, True)):
        for v, res in src.items():
            info = table.get(v)
            row = decide(ctx, cid, v, res, info, is_lend, excl)
            if row["status"] == "in":
                out["vaults"][v] = pack(res, info, row, g)
            else:
                out["filtered"].append(row)
    for v, info in e3.items():
        if v not in results:
            out["filtered"].append({"chain_id": cid, "vault": v, "protocol": reg.get(v, {}).get("protocol"),
                                    "kind": info["kind"], "status": "out",
                                    "reason": excl.get((cid, v), "no_positions"),
                                    "active_days": 0, "loan_unit": ctx.book.unit(cid, info["asset"]),
                                    "asset_symbol": info["asset_symbol"]})
    if stuck_n:
        inside = set(out["vaults"])
        upr = {v: (e3.get(v) or lend.get(v))["usd_per_raw"] for v in inside}
        out["stuck"] = stuck_rows(ctx, cid, g, assets, holders, inside, upr, dist,
                                  e2_depeg_tokens(pathlib.Path(events_dir)))
    out["seconds"] = round(time.time() - t0, 1)
    out["price_units"] = {f"{c}:{t}": {"unit": u, "symbol": ctx.book.symbol(c, t)}
                          for (c, t), u in ctx.book.price_unit.items()}
    out["scenario"] = {"depths": depths, "headline": ctx.scen.headline,
                       "event_median_depth": ctx.scen.event_median_depth,
                       "event_depths": ctx.scen.event_depths, "wrapper_depeg": WRAPPER_DEPTH,
                       "crash": {f"{k[0]}|{k[1]}": v for k, v in ctx.scen.crash.items()}}
    sdir = WORK / (("series" if not stuck_n else f"series_stuck_n{stuck_n}") + WRAP_SUFFIX)
    sdir.mkdir(parents=True, exist_ok=True)
    with open(sdir / f"{cid}.pkl", "wb") as fh:
        pickle.dump(out, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return cid, len(out["vaults"]), len(out["filtered"]), out["seconds"]


def decide(ctx, cid, v, res, info, is_lend, excl):
    """Sample decision: not a self-dealing loop, pegged loan, priced, >= 30 active days, holdings reconstructed."""
    row = {"chain_id": cid, "vault": v, "protocol": res["proto"], "kind": res["kind"], "status": "out"}
    if (cid, v) in excl:
        row.update(reason=excl[(cid, v)], active_days=0, loan_unit=None,
                   asset_symbol=(info or {}).get("asset_symbol"))
        return row
    if info is None:
        row.update(reason="no_e3_row", active_days=0, loan_unit=None, asset_symbol=None)
        return row
    asset = info["asset"]
    unit = ctx.book.unit(cid, asset)
    row.update(loan_unit=unit, asset_symbol=info["asset_symbol"])
    upr = info["usd_per_raw"]
    usd = res["V"] * upr
    peak = np.nanmax(usd) if np.isfinite(usd).any() else 0.0
    floor = max(MIN_USD, PEAK_FRAC * peak)
    active = np.isfinite(usd) & (usd >= floor)
    if is_lend:
        active &= info["governed"]
    row["active_days"] = int(active.sum())
    if unit not in LOAN_UNITS:
        row["reason"] = "loan_not_pegged"
        return row
    if not np.isfinite(upr).any():
        row["reason"] = "loan_unpriced"
        return row
    if active.sum() < MIN_ACTIVE_DAYS:
        row["reason"] = "fewer_than_30_active_days"
        return row
    out_share = float(np.nanmean(res["share_outside"][active]))
    row["outside_share_mean"] = out_share
    if out_share > MAX_OUTSIDE_SHARE:
        row["reason"] = "holdings_outside_reconstruction"
        return row
    row.update(status="in", reason="", active=active, usd=usd, peak_usd=float(peak))
    return row


def pack(res, info, row, g):
    """Slice a vault's arrays to [first funded day, end] for storage."""
    V = res["V"]
    nz = np.flatnonzero(V > 0)
    i0 = int(nz[0]) if len(nz) else 0
    sl = slice(i0, g.n)
    keep = ("V", "W", "ret_before", "ret_after", "tl", "crash", "apy_gross", "apy_net", "worst_fam",
            "idle_share", "nfam", "share_unk", "share_worst", "share_unpeg", "share_outside",
            "share_trackpeg", "share_us", "share_v12same", "perf_fee", "mgmt_fee", "fee_assets", "tl2",
            "crash2")
    d = {k: res[k][sl] for k in keep}
    d["active"] = row["active"][sl]
    d["usd_per_raw"] = info["usd_per_raw"][sl]
    d["held_usd"] = info["held_usd"][sl]
    d["guard"] = info["guard"][sl]
    d["e3_ta"] = info["ta"][sl]
    d["e3_supply"] = info["supply"][sl]
    d["e3_event"] = info.get("source_event", np.zeros(g.n, dtype=bool))[sl]
    d["first_day"] = g.d0 + i0
    d["meta"] = {k: row[k] for k in ("chain_id", "vault", "protocol", "kind", "loan_unit", "asset_symbol",
                                     "active_days", "peak_usd", "outside_share_mean")}
    d["meta"]["asset"] = info["asset"]
    d["meta"]["n_holdings"] = res["n_holdings"]
    return d


def stage_series(events_dir, workers, only=None):
    ends = chain_heads()
    jobs = [(cid, ends[cid], str(events_dir)) + ((STUCK_N,) if STUCK_N else ()) for cid in sorted(CHAINS, key=lambda c: -len(CHAINS[c]))
            if only is None or cid in only]
    t0 = time.time()
    with ProcessPoolExecutor(workers) as ex:
        for cid, nin, nout, sec in ex.map(run_chain, jobs):
            print(f"chain {cid}: {nin} in sample, {nout} filtered, {sec} s", flush=True)
    print(f"series done in {time.time() - t0:.0f} s", flush=True)


def load_series():
    out = {}
    for p in sorted((WORK / SERIES).glob("*.pkl")):
        with open(p, "rb") as fh:
            out[int(p.stem)] = pickle.load(fh)
    return out


API_URL = "https://api.morpho.org/graphql"


def api_query(items):
    """One GraphQL request for up to five vaults (aliases)."""
    opts = f"{{startTimestamp: {int(dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc).timestamp())}, " \
           f"endTimestamp: {int(time.time())}, interval: DAY}}"
    parts = []
    for i, (cid, v, kind) in enumerate(items):
        if kind == "morpho_v2":
            parts.append(f'v{i}: vaultV2ByAddress(address: "{v}", chainId: {cid}) {{ address historicalState {{ '
                         f'avgApy(options: {opts}, lookbackHours: 24) {{ x y }} '
                         f'avgNetApy(options: {opts}, lookbackHours: 24) {{ x y }} }} }}')
        else:
            parts.append(f'v{i}: vaultByAddress(address: "{v}", chainId: {cid}) {{ address historicalState {{ '
                         f'apy(options: {opts}) {{ x y }} netApy(options: {opts}) {{ x y }} '
                         f'netApyWithoutRewards(options: {opts}) {{ x y }} }} }}')
    return "query { " + " ".join(parts) + " }"


def stage_api():
    import requests
    series = load_series()
    todo = []
    for cid, s in series.items():
        for v, d in s["vaults"].items():
            if d["meta"]["protocol"] == "morpho":
                todo.append((cid, v, d["meta"]["kind"]))
    cache = WORK / "api"
    cache.mkdir(parents=True, exist_ok=True)
    todo = [t for t in todo if not (cache / f"{t[0]}-{t[1]}.json").exists()]
    print(f"api: {len(todo)} vaults to fetch", flush=True)
    sess = requests.Session()

    def post(items):
        for attempt in range(6):
            try:
                return sess.post(API_URL, json={"query": api_query(items)}, timeout=120).json()
            except Exception as exc:
                err = str(exc)
                time.sleep(3 * (attempt + 1))
        return {"errors": [{"message": err}]}

    def save(item, data, errs):
        cid, v, kind = item
        (cache / f"{cid}-{v}.json").write_text(json.dumps(
            {"chain_id": cid, "vault": v, "kind": kind, "fetched_utc": utc_now(), "data": data,
             "errors": errs}))

    for i in range(0, len(todo), 5):
        items = todo[i:i + 5]
        body = post(items)
        if body.get("data") and not body.get("errors"):
            for j, item in enumerate(items):
                save(item, body["data"].get(f"v{j}"), [])
        else:
            for item in items:
                b = post([item])
                save(item, (b.get("data") or {}).get("v0"), b.get("errors") or [])
                time.sleep(0.3)
        time.sleep(0.4)
        if (i // 5) % 20 == 0:
            print(f"  {i + len(items)}/{len(todo)}", flush=True)
    (WORK / "api_query_example.graphql").write_text(api_query([(1, "0x...", "morpho_v1"),
                                                                (1, "0x...", "morpho_v2")]))


def api_series(cid, v, first_day, n):
    """{field: array over the vault's stored days} from the cached API answer."""
    p = WORK / "api" / f"{cid}-{v}.json"
    if not p.exists():
        return None
    rec = json.loads(p.read_text())
    data = rec.get("data")
    if not data or not data.get("historicalState"):
        return None
    out = {}
    for field, pts in data["historicalState"].items():
        arr = np.full(n, np.nan)
        for pt in pts or []:
            if pt.get("y") is None:
                continue
            i = pt["x"] // DAY - 1 - first_day
            if 0 <= i < n:
                arr[i] = pt["y"]
        out[field] = arr
    return out


def curator_tables():
    iv = defaultdict(list)
    for r in jsonl(DATA / "registry/vault_curators.jsonl"):
        iv[(r["chain_id"], r["vault"].lower())].append(
            (r["from_ts"] or 0, r["to_ts"] or 2 ** 62, r["entity_id"], r["confidence"]))
    names = {r["entity_id"]: r["display_name"] for r in jsonl(DATA / "registry/curators.jsonl")}
    return iv, names


def entity_by_day(intervals, days):
    """Entity and confidence holding the controlling role at each day end."""
    ent = np.array([None] * len(days), dtype=object)
    conf = np.array([None] * len(days), dtype=object)
    ends = (days + 1) * DAY - 1
    for f, t, e, c in sorted(intervals):
        m = (ends >= f) & (ends < t)
        ent[m] = e
        conf[m] = c
    return ent, conf


def annualize(rets):
    rets = np.asarray(rets, dtype=float)
    if len(rets) == 0:
        return np.nan
    return float(np.expm1(np.log1p(rets).sum() * 365.0 / len(rets)))


def depth_tag(dlt):
    return f"d{int(round(dlt * 1000)):04d}"


def vault_frame(cid, v, d, depths, iv):
    """Daily table of one sample vault (all stored days)."""
    n = len(d["V"])
    days = d["first_day"] + np.arange(n)
    meta = d["meta"]
    api = api_series(cid, v, d["first_day"], n) if meta["protocol"] == "morpho" else None
    usd = d["V"] * d["usd_per_raw"]
    net = np.maximum(usd - d["held_usd"], 0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        L = np.where(d["V"] > 0, d["W"] / d["V"], np.nan)
    prev_active = np.zeros(n, dtype=bool)
    prev_active[1:] = d["active"][:-1]
    guard_prev = np.zeros(n, dtype=bool)
    guard_prev[1:] = d["guard"][:-1]
    rb = d["ret_before"]
    used = prev_active & np.isfinite(rb) & (rb <= MAX_DAILY_RET) & ~d["guard"] & ~guard_prev
    K = len(depths)
    if api is not None and meta["kind"] == "morpho_v2":
        a_gross = np.full(n, np.nan)
        a_net = api.get("avgApy", np.full(n, np.nan))
        a_rew = api.get("avgNetApy", np.full(n, np.nan))
    elif api is not None:
        a_gross = api.get("apy", np.full(n, np.nan))
        a_net = api.get("netApyWithoutRewards", np.full(n, np.nan))
        a_rew = api.get("netApy", np.full(n, np.nan))
    else:
        a_gross = a_net = a_rew = np.full(n, np.nan)
    use_api = np.isfinite(a_net)
    apy = np.where(use_api, a_net, d["apy_net"])
    ent, conf = entity_by_day(iv.get((cid, v), []), days)
    cols = {
        "protocol": meta["protocol"], "chain_id": cid, "vault": v, "kind": meta["kind"],
        "date": [day_str(x) for x in days], "active": d["active"].astype(int),
        "value_raw": d["V"], "value_usd": usd, "net_usd": net,
        "ret_before_fee": rb, "ret_after_fee": d["ret_after"], "ret_in_yield": used.astype(int),
        "L": L,
    }
    for k, dlt in enumerate(depths):
        cols["tl_" + depth_tag(dlt)] = d["tl2"][:, k]
    cols["tl_crash"] = d["crash2"]
    cols["tl_headline"] = np.maximum(d["tl2"][:, K - 1], d["crash2"])
    for j, phi in enumerate(PHI_SENS):
        cols[phi_col(phi)] = np.maximum(d["tl2"][:, K + j], d["crash2"])
    for k, dlt in enumerate(depths):
        cols["tl_v1_" + depth_tag(dlt)] = d["tl"][:, k]
    cols["tl_v1_crash"] = d["crash"]
    cols["tl_v1_headline"] = np.maximum(d["tl"][:, K - 1], d["crash"])
    cols.update({
        "worst_family_headline": d["worst_fam"],
        "vault_apy": apy, "vault_apy_with_rewards": a_rew,
        "apy_source": np.where(use_api, "morpho_api", "rebuilt"),
        "apy_rebuilt_gross": d["apy_gross"], "apy_rebuilt_net": d["apy_net"],
        "api_apy_gross": a_gross, "api_net_apy_without_rewards": a_net, "api_net_apy_with_rewards": a_rew,
        "perf_fee": d["perf_fee"], "mgmt_fee_annual": d["mgmt_fee"],
        "share_unpegged": d["share_unpeg"], "share_unknown_unit": d["share_unk"],
        "share_worst_case_oracle": d["share_worst"], "share_outside": d["share_outside"],
        "share_idle": d["idle_share"], "curator_entity": ent, "curator_confidence": conf,
    })
    return pd.DataFrame(cols)


def vault_summary(df, meta, depths, names):
    act = df[df.active == 1]
    used = df[df.ret_in_yield == 1]
    K = len(depths)
    row = {k: meta[k] for k in ("chain_id", "vault", "protocol", "kind", "asset_symbol", "loan_unit")}
    reg = meta.get("name")
    row["name"] = reg
    row["status"] = "in"
    row["reason"] = ""
    row["active_days"] = int(len(act))
    row["first_active"] = act.date.iloc[0]
    row["last_active"] = act.date.iloc[-1]
    row["return_days"] = int(len(used))
    prev_act = df.active.shift(1, fill_value=0) == 1
    row["return_days_excluded"] = int((prev_act & (df.ret_in_yield == 0)).sum())
    row["yield_before_fee"] = annualize(used.ret_before_fee)
    row["yield_after_fee"] = annualize(used.ret_after_fee)
    row["headline_depth"] = depths[K - 1]
    for c in tl_columns(depths):
        row[c] = float(act[c].mean())
    row["L_p10"] = float(np.nanpercentile(act.L, 10))
    row["L_median"] = float(np.nanmedian(act.L))
    row["share_days_L_below_5pct"] = float((act.L < 0.05).mean())
    cap = act.vault_apy.clip(upper=APY_CAP)
    row["vault_apy"] = float(cap.median())
    row["vault_apy_mean"] = float(cap.mean())
    row["vault_apy_days_capped"] = int((act.vault_apy > APY_CAP).sum())
    row["vault_apy_with_rewards"] = float(act.vault_apy_with_rewards.clip(upper=APY_CAP).median()) \
        if act.vault_apy_with_rewards.notna().any() else np.nan
    row["apy_rebuilt_net"] = float(act.apy_rebuilt_net.clip(upper=APY_CAP).median())
    row["apy_api_coverage"] = float((act.apy_source == "morpho_api").mean())
    row["perf_fee_mean"] = float(act.perf_fee.mean())
    row["aum_mean_usd"] = float(act.value_usd.mean())
    row["aum_peak_usd"] = float(df.value_usd.max())
    row["net_aum_mean_usd"] = float(act.net_usd.mean())
    row["net_usd_days"] = float(act.net_usd.sum())
    for c in ("share_unpegged", "share_unknown_unit", "share_worst_case_oracle", "share_outside", "share_idle"):
        row[c + "_mean"] = float(act[c].mean())
    w = act.groupby(act.curator_entity.fillna("")).net_usd.sum()
    if w.sum() <= 0:
        w = act.groupby(act.curator_entity.fillna("")).size().astype(float)
    top = w.idxmax()
    row["curator_entity"] = top or ""
    row["curator_name"] = names.get(top, "") if top else ""
    row["curator_share"] = float(w.max() / w.sum()) if w.sum() > 0 else np.nan
    sub = act[act.curator_entity.fillna("") == top]
    row["curator_confidence"] = sub.curator_confidence.mode().iloc[0] if top and len(sub) else ""
    row["curator_changes"] = int(len([x for x in w.index if x]) > 1)
    return row


def phi_col(phi):
    return f"tl_headline_phi{int(round(phi * 100)):03d}"


def tl_columns(depths):
    """Tail-loss columns: oracle-aware (each depth, crash, headline, headline at PHI_SENS), then the first-pass formula under tl_v1_*."""
    return list(dict.fromkeys(["tl_" + depth_tag(x) for x in depths] + ["tl_crash", "tl_headline"]
                              + [phi_col(p) for p in PHI_SENS]
                              + ["tl_v1_" + depth_tag(x) for x in depths]
                              + ["tl_v1_crash", "tl_v1_headline"]))


def spearman(x, y):
    from scipy.stats import rankdata
    rx, ry = rankdata(x), rankdata(y)
    return float(np.corrcoef(rx, ry)[0, 1])


def perm_tests(x, y, clusters, rng, nperm=N_PERM, nboot=N_BOOT):
    """Spearman rho with a naive permutation p, a permutation p clustered by curator (curator means of y's ranks permuted acros."""
    from scipy.stats import rankdata
    x = np.asarray(x, float)
    y = np.asarray(y, float)
    rx, ry = rankdata(x), rankdata(y)
    obs = float(np.corrcoef(rx, ry)[0, 1])
    codes, uniq = pd.factorize(pd.Series(clusters))
    k = len(uniq)
    means = np.bincount(codes, weights=ry, minlength=k) / np.bincount(codes, minlength=k)
    dev = ry - means[codes]
    base = np.argsort(codes, kind="stable")
    naive = clus = 0
    for _ in range(nperm):
        if abs(np.corrcoef(rx, rng.permutation(ry))[0, 1]) >= abs(obs) - 1e-12:
            naive += 1
        order = np.lexsort((rng.random(len(codes)), codes))
        dshuf = np.empty_like(dev)
        dshuf[base] = dev[order]
        ystar = means[rng.permutation(k)][codes] + dshuf
        if abs(np.corrcoef(rx, rankdata(ystar))[0, 1]) >= abs(obs) - 1e-12:
            clus += 1
    members = [np.flatnonzero(codes == c) for c in range(k)]
    boots = []
    for _ in range(nboot):
        pick = rng.integers(0, k, k)
        idx = np.concatenate([members[c] for c in pick])
        if len(np.unique(x[idx])) > 1 and len(np.unique(y[idx])) > 1:
            boots.append(spearman(x[idx], y[idx]))
    lo, hi = np.percentile(boots, [2.5, 97.5]) if boots else (np.nan, np.nan)
    return {"rho": obs, "p_perm_naive": (naive + 1) / (nperm + 1),
            "p_perm_clustered": (clus + 1) / (nperm + 1), "ci95_cluster_bootstrap": [float(lo), float(hi)],
            "n": int(len(x)), "clusters": int(k)}


METRICS = [("vault_apy", "vault APY", 1), ("yield_after_fee", "net realized yield (after fee)", 1),
           ("tl_headline", "tail loss (headline)", -1), ("L_p10", "liquidity (10th percentile of L)", 1)]


def pareto(score_cols):
    """Non-dominated rows; every column is oriented so that larger is better."""
    X = np.asarray(score_cols, float)
    n = len(X)
    nd = np.ones(n, dtype=bool)
    for i in range(n):
        ge = (X >= X[i]).all(axis=1)
        gt = (X > X[i]).any(axis=1)
        if (ge & gt).any():
            nd[i] = False
    return nd


AUM_FLOOR = 10e6
FRONTIER_MIN_DAYS = 90
COLUMN_RENAME = ("2026-10-09: tl_* (each depth, crash, headline) hold the oracle-aware tail loss with "
                 "phi = 0.25, adopted as the paper's tail loss, and tl_headline_phi000/phi050 its phi "
                 "sensitivity; the first-pass formula moved to tl_v1_* (appendix); the columns named "
                 "tl_v2_* on 2026-10-08 no longer exist. 2026-10-09: tl_crash (and the headlines) use "
                 "the one-day crash jump for repricing oracles instead of phi times the 7-day depth; "
                 "2026-10-09: other/proxy_feed_other_asset oracles whose feed prices the crash's "
                 "reference asset itself take the repricing branch in the crash; 2026-10-09: the crash "
                 "jump is the largest one-day drop of the pair (was its 1st-percentile one-day return)")


def agreement(vs, cs, rng, tl_col, aum_floor=None):
    """Spearman matrices (vault level clustered by curator, allocator vaults only, APY with rewards, curator level), non-domina."""
    mets = [(m if m != "tl_headline" else tl_col, lab, sg) for m, lab, sg in METRICS]
    cols = [m for m, _l, _s in mets]
    out = {"tail_loss_column": tl_col, "aum_floor_usd": aum_floor}

    def matrix(df, clustered):
        res = {}
        clusters = np.where(df.curator_entity.fillna("") == "", "vault:" + df.vault, df.curator_entity)
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                a, b = cols[i], cols[j]
                cl = clusters if clustered else np.arange(len(df))
                res[f"{a}|{b}"] = perm_tests(df[a].values, df[b].values, cl, rng)
        return res

    v = vs[vs.status == "in"].dropna(subset=cols).copy()
    if aum_floor:
        v = v[v.aum_mean_usd >= aum_floor].copy()
    out["vault_level"] = {"n_vaults": int(len(v)), "pairs": matrix(v, True),
                          "note": "p_perm_clustered permutes curator means of the second metric's ranks "
                                  "across curators and the deviations within curators; vaults without "
                                  "an attributed curator are their own clusters"}
    if aum_floor is None:
        alloc = v[v.kind != "euler_lending"]
        out["vault_level_allocator_vaults_only"] = {"n_vaults": int(len(alloc)), "pairs": matrix(alloc, True)}
        rew = v.copy()
        rew["vault_apy"] = rew.vault_apy_with_rewards.where(rew.vault_apy_with_rewards.notna(), rew.vault_apy)
        out["vault_level_apy_with_rewards"] = {
            "n_vaults": int(len(rew)),
            "pairs": {k: val for k, val in matrix(rew, True).items() if k.startswith("vault_apy")}}
        c = cs.dropna(subset=cols).copy()
        c["vault"] = c.curator_entity
        out["curator_level"] = {"n_curators": int(len(c)), "pairs": matrix(c, False),
                                "note": "curators are the units; naive and clustered p coincide"}
    pool = (v.active_days >= FRONTIER_MIN_DAYS).values if aum_floor else np.ones(len(v), dtype=bool)
    nd = np.zeros(len(v), dtype=bool)
    nd[pool] = pareto(np.column_stack([v.yield_after_fee.values, -v[tl_col].values, v.L_p10.values])[pool])
    v["non_dominated"] = nd
    out["non_dominated"] = {
        "criteria": f"no other vault has yield_after_fee >= , {tl_col} <= and L_p10 >= with one strict",
        "min_active_days": FRONTIER_MIN_DAYS if aum_floor else None,
        "count": int(nd.sum()), "of": int(pool.sum()),
        "aum_share": float(v.aum_mean_usd[nd].sum() / v.aum_mean_usd[pool].sum()),
        "vaults": [{k: (None if (isinstance(r[k], float) and not np.isfinite(r[k])) else r[k])
                    for k in ("chain_id", "vault", "name", "protocol", "kind", "curator_name",
                              "yield_after_fee", tl_col, "L_p10", "vault_apy", "aum_mean_usd")}
                   for _, r in v[nd].sort_values("yield_after_fee", ascending=False).iterrows()]}
    pct = {m: (sg * v[m]).rank(pct=True) * 100.0 for m, _l, sg in mets}
    top = {}
    k = max(1, int(math.ceil(0.10 * len(v))))
    for m, _l, sg in mets:
        idx = (sg * v[m]).sort_values(ascending=False).index[:k]
        top[m] = {"n": int(k), "on_other_metrics": {}}
        for m2, _l2, _s2 in mets:
            if m2 != m:
                p = pct[m2].loc[idx]
                top[m]["on_other_metrics"][m2] = {
                    "median_percentile": float(p.median()),
                    "share_in_top_decile": float((p > 90).mean()),
                    "share_in_bottom_half": float((p <= 50).mean())}
    out["top_decile"] = top
    return out, v[["chain_id", "vault", "non_dominated"]]


def rank_agreement(vs, cs, rng, depths):
    """Rank agreement with the oracle-aware headline tail loss, on the full sample and above the AUM floor; its sensitivity to."""
    out = {"metrics": {m: {"label": lab, "better": "higher" if sg > 0 else "lower"}
                       for m, lab, sg in METRICS}}
    main, nd = agreement(vs, cs, rng, "tl_headline")
    out.update(main)
    out["aum_floor_10m"], _ = agreement(vs, cs, rng, "tl_headline", AUM_FLOOR)
    v = vs[vs.status == "in"].dropna(subset=["vault_apy", "yield_after_fee", "L_p10", "tl_headline"])
    clusters = np.where(v.curator_entity.fillna("") == "", "vault:" + v.vault, v.curator_entity)
    out["phi_sensitivity"] = {
        phi_col(p): {f"{a}|{phi_col(p)}": perm_tests(v[a].values, v[phi_col(p)].values, clusters, rng)
                     for a in ("vault_apy", "yield_after_fee", "L_p10")} for p in PHI_SENS}
    out["appendix_v1"], nd1 = agreement(vs, cs, rng, "tl_v1_headline")
    out["appendix_v1_aum_floor_10m"], _ = agreement(vs, cs, rng, "tl_v1_headline", AUM_FLOOR)
    c = cs.dropna(subset=["tl_headline", "tl_v1_headline"])
    out["v1_vs_v2"] = {
        "note": "v2 is the oracle-aware tail loss in tl_*, v1 the first-pass formula in tl_v1_*",
        "vault_level_spearman_headline": spearman(v.tl_v1_headline.values, v.tl_headline.values),
        "curator_level_spearman_headline": spearman(c.tl_v1_headline.values, c.tl_headline.values),
        "vault_level_spearman_by_depth": {
            depth_tag(d): spearman(v["tl_v1_" + depth_tag(d)].values, v["tl_" + depth_tag(d)].values)
            for d in depths}}
    nd = nd.merge(nd1.rename(columns={"non_dominated": "non_dominated_v1"}), on=["chain_id", "vault"],
                  how="outer")
    return out, nd


def frontier_2d(x, y):
    """Indices on the lower-x, higher-y Pareto frontier, sorted by x."""
    order = np.lexsort((-y, x))
    best = -np.inf
    keep = []
    for i in order:
        if y[i] > best:
            keep.append(i)
            best = y[i]
    return np.array(keep, dtype=int)


def write_csv(df, rel, meta):
    path = DERIVED / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_csv(tmp, index=False, float_format="%.10g")
    os.replace(tmp, path)
    io_utils._meta_sidecar(path, GENERATOR, {**meta, "rows": int(len(df)), "columns": list(df.columns)})


def inputs_stamp(events_dir):
    return {
        "events": file_stamp(events_dir / "events.jsonl"),
        "collateral_families": file_stamp(events_dir / "collateral_families.jsonl"),
        "e3_vault_daily": file_stamp(DERIVED / "aum/vault_daily.csv"),
        "e3_lending_daily": file_stamp(DERIVED / "aum/lending_daily.csv"),
        "vault_curators": file_stamp(DATA / "registry/vault_curators.jsonl"),
        "excluded_vaults": file_stamp(DERIVED / "aum/excluded_vaults.jsonl")
        if (DERIVED / "aum/excluded_vaults.jsonl").exists() else None,
        "tokens": file_stamp(DATA / "prices/tokens.jsonl"),
    }


def stage_outputs(events_dir):
    rng = np.random.default_rng(SEED)
    series = load_series()
    iv, names = curator_tables()
    ctx_reg = {}
    for cid, protos in CHAINS.items():
        for proto in protos:
            for r in jsonl(DATA / f"registry/vaults/{proto}/{cid}.jsonl"):
                ctx_reg[(cid, r["vault"].lower())] = r.get("name")
    depths = next(iter(series.values()))["depths"]
    scen = next(iter(series.values()))["scenario"]
    frames, vrows, frows = [], [], []
    for cid, s in sorted(series.items()):
        for v, d in sorted(s["vaults"].items()):
            d["meta"]["name"] = ctx_reg.get((cid, v))
            df = vault_frame(cid, v, d, depths, iv)
            frames.append(df)
            vrows.append(vault_summary(df, d["meta"], depths, names))
        for r in s["filtered"]:
            frows.append({"chain_id": cid, "vault": r["vault"], "protocol": r.get("protocol"),
                          "kind": r.get("kind"), "status": "out", "reason": r["reason"],
                          "active_days": r.get("active_days"), "loan_unit": r.get("loan_unit"),
                          "asset_symbol": r.get("asset_symbol"), "name": ctx_reg.get((cid, r["vault"])),
                          "outside_share_mean": r.get("outside_share_mean")})
    daily = pd.concat(frames, ignore_index=True)
    vs = pd.DataFrame(vrows)
    fs = pd.DataFrame(frows)
    peaks = e3_peaks()
    for t in (vs, fs):
        t["e3_peak_usd"] = [peaks.get((int(c), v), np.nan) for c, v in zip(t.chain_id, t.vault)]
    end_day = max(s["grid"][1] for s in series.values())
    crows = curator_rows(daily, vs, depths, names, end_day)
    cs = pd.DataFrame(crows).sort_values("aum_avg_usd", ascending=False).reset_index(drop=True)
    cs.insert(0, "rank_by_aum", np.arange(1, len(cs) + 1))
    ra, nd = rank_agreement(vs.assign(status="in"), cs, rng, depths)
    vs = vs.merge(nd, on=["chain_id", "vault"], how="left")
    ra["headline_depth"] = scen["headline"]
    ra["headline_depth_rule"] = ("fixed parameter HEADLINE_DEPTH; event_median_depth, the median "
                                 "of 1 - min_market_price over the E2 depeg-loss events, is its "
                                 "justification")
    ra["event_median_depth"] = scen.get("event_median_depth")
    ra["event_depths"] = scen.get("event_depths")
    ra["depths"] = list(dict.fromkeys(depths))
    allv = pd.concat([vs, fs], ignore_index=True)
    meta = {"inputs": inputs_stamp(events_dir), "parameters": params_meta(scen),
            "column_rename": COLUMN_RENAME}
    if STUCK_N:
        meta["freeze_stuck"] = stuck_params()
    write_csv(daily, f"{OUT_REL}/vault_daily.csv", {**meta, "what": "per sample vault and UTC day: value, "
              "returns, L(t), tail loss per depth and crash, vault APY (see data/resume/e4-metrics.md)"})
    write_csv(allv, f"{OUT_REL}/vault_scorecard.csv", {**meta, "what": "whole-period values per sample vault "
              "(status in) and one row per filtered vault (status out, reason)"})
    write_csv(cs, f"{OUT_REL}/curator_scorecard.csv", {**meta, "what": "per curator entity: its sample vaults "
              "combined each day with net USD weights, then whole-period values as for vaults (Table 4 "
              "without the E5 columns); aum_avg_usd is net USD-days over the window from 2024-01-01"})
    io_utils.write_json(f"{OUT_REL}/rank_agreement.json", ra, GENERATOR, meta)
    io_utils.write_json(f"{OUT_REL}/sample_funnel.json", sample_funnel(vs, fs), GENERATOR, meta)
    fig_data(vs, cs, depths)
    if STUCK_N:
        stuck_outputs(series, vs, cs, ra, meta)
    print(f"outputs: {len(vs)} vaults in, {len(fs)} filtered, {len(cs)} curators", flush=True)


BASE_REL = "metrics/e4_depeg_only" + WRAP_SUFFIX


def set_stuck(n):
    """Switch the process to --freeze-stuck N."""
    global STUCK_N, SERIES, OUT_REL, OUT
    n = int(n)
    STUCK_N = n or None
    SERIES = (f"series_stuck_n{n}" if n else "series") + WRAP_SUFFIX
    OUT_REL = ("metrics/e4" + WRAP_SUFFIX if n == STUCK_N_HEADLINE else
               f"metrics/e4_stuck_n{n}{WRAP_SUFFIX}" if n else BASE_REL)
    OUT = DERIVED / OUT_REL


def stuck_params():
    return {"n_days": STUCK_N, "utilization": STUCK_U, "stuck_exits": list(STUCK_EXITS),
            "max_repaid_share_per_30d": STUCK_MAX_REPAY_30D, "baseline": BASE_REL,
            "rule": f"a pinned spell (maximal run of days with end-of-day utilization >= {STUCK_U}) of at "
                    f"least {STUCK_N} days in which borrowers repay less than {STUCK_MAX_REPAY_30D:.0%} of the "
                    "debt at its start per 30 days, and that is still pinned on the grid's last day or ends "
                    "through a write-off; rises of the market's book value per share are dropped from the "
                    "day after the spell's first day through its exit day, on top of E2's depeg freeze"}


RANK_PAIRS = ("vault_apy|yield_after_fee", "vault_apy|tl_headline", "vault_apy|L_p10",
              "yield_after_fee|tl_headline", "yield_after_fee|L_p10", "tl_headline|L_p10")


def stuck_outputs(series, vs, cs, ra, meta):
    """Stuck spells, sample exposure, and the comparison with the frozen E4 run in BASE_REL."""
    sp = pd.DataFrame([r for s in series.values() for r in s.get("stuck", {}).get("spells", [])])
    ex = pd.DataFrame([r for s in series.values() for r in s.get("stuck", {}).get("exposure", [])])
    key = ["chain_id", "protocol", "market", "spell"]
    info = vs.set_index(["chain_id", "vault"])[["name", "curator_name", "kind"]]
    if len(ex):
        ex = ex.join(info, on=["chain_id", "vault"])
        agg = ex.groupby(key).agg(sample_vaults=("vault", "nunique"), sample_usd_days=("usd_days", "sum"),
                                  sample_usd_days_outside_depeg_freeze=("usd_days_outside_depeg_freeze", "sum"),
                                  max_vault_peak_usd=("peak_usd", "max"),
                                  book_over_econ_usd_at_exit=("book_over_econ_usd_at_exit", "sum"))
        sp = sp.join(agg, on=key)
        ex = ex.merge(sp[key + ["stuck", "new_frozen_days", "in_depeg_set", "start", "exit", "days"]], on=key,
                      how="left")
    for c in ("sample_vaults", "sample_usd_days", "sample_usd_days_outside_depeg_freeze", "max_vault_peak_usd",
              "book_over_econ_usd_at_exit"):
        if c not in sp:
            sp[c] = np.nan
    sp = sp.sort_values(["stuck", "sample_usd_days"], ascending=[False, False])
    write_csv(sp, f"{OUT_REL}/stuck_spells.csv", {**meta, "what": "every pinned spell (end-of-day utilization "
              ">= 0.99) of every market a vault holds: dates, length, how it ends, debt flows (raw loan units), "
              "whether it is frozen at this N, E2 depeg overlap, and the summed exposure of sample vaults "
              "holding the market directly"})
    write_csv(ex, f"{OUT_REL}/stuck_exposure.csv", {**meta, "what": "per pinned spell and sample vault holding "
              "the market directly: USD-days at economic value from the spell's first day through its exit "
              "day, the peak, and book minus economic value on the exit day"})

    base = DERIVED / BASE_REL
    if not (base / "vault_scorecard.csv").exists():
        print(f"stuck: no {BASE_REL} run to compare with; stuck_comparison.json and "
              "yield_changes.csv not written", flush=True)
        return
    cols = ["aum_mean_usd", "yield_before_fee", "yield_after_fee", "return_days", "return_days_excluded",
            "vault_apy", "tl_headline", "L_p10", "active_days"]
    old = pd.read_csv(base / "vault_scorecard.csv", low_memory=False)
    old = old[old.status == "in"]
    ids = ["chain_id", "vault", "name", "curator_name", "kind", "protocol"]
    yc = old[ids + cols].merge(vs[ids + cols], on=ids[:2], how="outer", suffixes=("_e4", "_stuck"))
    for c in ids[2:]:
        yc[c] = yc[c + "_e4"].where(yc[c + "_e4"].notna(), yc[c + "_stuck"])
        yc = yc.drop(columns=[c + "_e4", c + "_stuck"])
    yc["in_e4"] = yc.aum_mean_usd_e4.notna()
    yc["in_stuck"] = yc.aum_mean_usd_stuck.notna()
    yc["delta_yield_after_fee_pp"] = (yc.yield_after_fee_stuck - yc.yield_after_fee_e4) * 100
    if len(ex):
        st = ex[ex.stuck == True].groupby(["chain_id", "vault"]).usd_days_outside_depeg_freeze.sum().rename(
            "stuck_usd_days_outside_depeg_freeze")
        yc = yc.join(st, on=["chain_id", "vault"])
    yc = yc.reindex(yc.delta_yield_after_fee_pp.abs().sort_values(ascending=False, na_position="last").index)
    write_csv(yc, f"{OUT_REL}/yield_changes.csv", {**meta, "what": f"sample vaults of {BASE_REL} and of this run: "
              "whole-period values in each and the change in after-fee yield (percentage points per year)"})

    old_ra = json.loads((base / "rank_agreement.json").read_text())

    def pairs(src, scope):
        d = src["aum_floor_10m"]["vault_level"] if scope == "aum_floor_10m" else src.get(scope, {})
        pp = d.get("pairs", {})
        return {k: {f: pp[k].get(f) for f in ("rho", "ci95_cluster_bootstrap", "p_perm_clustered", "n")}
                for k in RANK_PAIRS if k in pp}

    def nd_names(src):
        return sorted(f"{x['chain_id']}:{x['vault']}" for x in src["aum_floor_10m"]["non_dominated"]["vaults"])

    def front(df):
        v = df[(df.aum_mean_usd >= AUM_FLOOR)].dropna(subset=["tl_headline", "yield_after_fee", "L_p10"])
        idx = frontier_2d(v.tl_headline.values, v.yield_after_fee.values)
        return sorted(f"{r.chain_id}:{r.vault}" for r in v.iloc[idx].itertuples())

    def lab(k, df):
        c, v = k.split(":")
        r = df[(df.chain_id == int(c)) & (df.vault == v)]
        if not len(r):
            return {"chain_id": int(c), "vault": v}
        r = r.iloc[0]
        return {"chain_id": int(c), "vault": v, "name": r["name"] if isinstance(r["name"], str) else None,
                "curator_name": r.curator_name if isinstance(r.curator_name, str) else None,
                "yield_after_fee_e4": None if pd.isna(r.yield_after_fee_e4) else float(r.yield_after_fee_e4),
                "yield_after_fee_stuck": None if pd.isna(r.yield_after_fee_stuck) else float(r.yield_after_fee_stuck),
                "aum_mean_usd_e4": None if pd.isna(r.aum_mean_usd_e4) else float(r.aum_mean_usd_e4)}

    nd_old, nd_new = nd_names(old_ra), nd_names(ra)
    fr_old, fr_new = front(old), front(vs)
    oc = pd.read_csv(base / "curator_scorecard.csv")
    top = oc.head(12)[["curator_entity", "curator_name", "yield_after_fee"]].merge(
        cs[["curator_entity", "yield_after_fee"]], on="curator_entity", how="left", suffixes=("_e4", "_stuck"))
    big = yc[yc.delta_yield_after_fee_pp.abs() > 0.5]
    comp = {
        "rule": stuck_params()["rule"],
        "sample": {"e4": int(yc.in_e4.sum()), "stuck": int(yc.in_stuck.sum()),
                   "left": [lab(f"{r.chain_id}:{r.vault}", yc) for r in yc[yc.in_e4 & ~yc.in_stuck].itertuples()],
                   "entered": [lab(f"{r.chain_id}:{r.vault}", yc) for r in yc[~yc.in_e4 & yc.in_stuck].itertuples()]},
        "spells": {"stuck": int(sp.stuck.sum()) if len(sp) else 0,
                   "stuck_outside_depeg_set": int((sp.stuck & ~sp.in_depeg_set).sum()) if len(sp) else 0,
                   "stuck_adding_frozen_days": int((sp.new_frozen_days > 0).sum()) if len(sp) else 0,
                   "stuck_adding_frozen_days_with_sample_exposure": int(
                       ((sp.new_frozen_days > 0) & (sp.sample_usd_days_outside_depeg_freeze > 0)).sum())
                   if len(sp) else 0,
                   "stuck_with_sample_exposure": int((sp.stuck & (sp.sample_usd_days > 0)).sum()) if len(ex) else 0},
        "vaults_yield_change_over_0p5pp": int(len(big)),
        "rank_correlations": {scope: {"e4": pairs(old_ra, scope), "stuck": pairs(ra, scope)}
                              for scope in ("vault_level", "aum_floor_10m", "curator_level")},
        "non_dominated_aum_floor_10m": {
            "e4": len(nd_old), "stuck": len(nd_new),
            "left": [lab(k, yc) for k in nd_old if k not in nd_new],
            "entered": [lab(k, yc) for k in nd_new if k not in nd_old]},
        "frontier_2d_aum_floor_10m": {
            "e4": [lab(k, yc) for k in fr_old], "stuck": [lab(k, yc) for k in fr_new],
            "left": [lab(k, yc) for k in fr_old if k not in fr_new],
            "entered": [lab(k, yc) for k in fr_new if k not in fr_old]},
        "curator_top12_yield_after_fee": [
            {"curator": r.curator_name, "e4": float(r.yield_after_fee_e4),
             "stuck": None if pd.isna(r.yield_after_fee_stuck) else float(r.yield_after_fee_stuck)}
            for r in top.itertuples()]}
    io_utils.write_json(f"{OUT_REL}/stuck_comparison.json", comp, GENERATOR, meta)
    print(f"stuck: {comp['spells']}; {len(big)} vaults move by more than 0.5 pp", flush=True)


def curator_rows(daily, vs, depths, names, end_day):
    """Curator scorecard rows."""
    owner = vs[vs.curator_entity.fillna("") != ""][["chain_id", "vault", "curator_entity"]]
    m = daily.merge(owner, on=["chain_id", "vault"], suffixes=("_day", ""))
    m = m.sort_values(["chain_id", "vault", "date"])
    m["w_prev"] = m.groupby(["chain_id", "vault"]).net_usd.shift(1)
    tl_cols = tl_columns(depths)
    window = end_day - D_START + 1

    def wmean(df, col, w):
        ok = df[col].notna() & df[w].notna() & (df[w] > 0)
        x = df[ok]
        g = (x[col] * x[w]).groupby(x.date).sum() / x[w].groupby(x.date).sum()
        return g

    rows = []
    for ent, sub in m.groupby("curator_entity"):
        vv = vs[vs.curator_entity == ent]
        act = sub[sub.active == 1]
        used = sub[sub.ret_in_yield == 1]
        by_day = act.groupby("date").net_usd.sum()
        row = {"curator_entity": ent, "curator_name": names.get(ent, ent), "vaults": int(len(vv)),
               "protocols": ";".join(sorted(vv.protocol.unique())),
               "chains": ";".join(str(x) for x in sorted(vv.chain_id.unique())),
               "kinds": ";".join(sorted(vv.kind.unique())),
               "aum_avg_usd": float(by_day.sum() / window),
               "aum_peak_usd": float(by_day.max()) if len(by_day) else 0.0,
               "aum_latest_usd": float(by_day.get(day_str(end_day), 0.0)),
               "active_days": int(len(by_day))}
        for col in ("vault_apy", "vault_apy_with_rewards"):
            ok = vv[col].notna() & (vv.net_usd_days > 0)
            row[col] = float(np.average(vv.loc[ok, col], weights=vv.loc[ok, "net_usd_days"])) \
                if ok.any() else np.nan
        row["yield_before_fee"] = annualize(wmean(used, "ret_before_fee", "w_prev").values)
        row["yield_after_fee"] = annualize(wmean(used, "ret_after_fee", "w_prev").values)
        for c in tl_cols:
            row[c] = float(wmean(act, c, "net_usd").mean())
        lc = wmean(act, "L", "net_usd")
        row["L_p10"] = float(np.nanpercentile(lc, 10)) if len(lc) else np.nan
        row["L_median"] = float(np.nanmedian(lc)) if len(lc) else np.nan
        row["perf_fee_mean"] = float(wmean(act, "perf_fee", "net_usd").mean())
        row["net_usd_days"] = float(act.net_usd.sum())
        row["vaults_with_api_apy"] = int((vv.apy_api_coverage > 0.5).sum())
        rows.append(row)
    return rows


def e3_peaks():
    """{(chain, vault): peak E3 USD value} over vault_daily and lending_daily."""
    out = {}
    for f in ("vault_daily.csv", "lending_daily.csv"):
        df = pd.read_csv(DERIVED / "aum" / f, usecols=["chain_id", "vault", "aum_usd"])
        for (c, v), x in df.groupby(["chain_id", "vault"]).aum_usd.max().items():
            out[(int(c), v.lower())] = float(x)
    return out


FUNNEL_ORDER = [
    ("owner_funded_or_self_dealing", lambda r: r.startswith("owner_funded")
     or r in ("self_dealing_loop", "protocol_owned_liquidity")),
    ("no_positions", lambda r: r == "no_positions"),
    ("no_e3_row", lambda r: r == "no_e3_row"),
    ("loan_not_pegged_or_unpriced", lambda r: r in ("loan_not_pegged", "loan_unpriced")),
    ("fewer_than_30_active_days", lambda r: r == "fewer_than_30_active_days"),
    ("holdings_outside_reconstruction", lambda r: r == "holdings_outside_reconstruction"),
]


def sample_funnel(vs, fs):
    """Sequential sample funnel over funded vaults (positive E3 USD value on at least one day, Table 2's definition), first fai."""
    t = pd.concat([vs[["chain_id", "vault", "kind", "loan_unit", "e3_peak_usd"]].assign(reason=""),
                   fs[["chain_id", "vault", "kind", "loan_unit", "e3_peak_usd", "reason"]]],
                  ignore_index=True)
    t["reason"] = t.reason.fillna("")
    funded = t[t.e3_peak_usd.fillna(0) > 0]
    out = {"definition": "funded: E3 aum_usd > 0 on at least one day (vault_daily.csv or "
                         "lending_daily.csv); steps in decide()'s order, each vault at its first "
                         "failing test",
           "enumerated": int(len(t)), "funded": int(len(funded)), "steps": [], "detail": {}}
    left = len(funded)
    for name, test in FUNNEL_ORDER:
        hit = funded.reason.map(test)
        left -= int(hit.sum())
        out["steps"].append({"step": name, "removed": int(hit.sum()), "remaining": left})
        out["detail"][name] = {str(k): int(x) for k, x in funded[hit].reason.value_counts().items()}
        if name == "loan_not_pegged_or_unpriced":
            out["detail"]["loan_not_pegged_by_unit"] = {
                str(k): int(x) for k, x in funded[hit].loan_unit.fillna("unknown").value_counts().items()}
    sample = funded[funded.reason == ""]
    out["sample"] = int(len(sample))
    if out["sample"] != left or left != len(vs):
        raise SystemExit(f"sample funnel ends at {left}, sample {out['sample']}, vs {len(vs)}")
    out["sample_by_kind"] = {str(k): int(x) for k, x in sample.kind.value_counts().items()}
    out["sample_by_loan_unit"] = {str(k): int(x) for k, x in sample.loan_unit.value_counts().items()}
    keys = set(zip(vs.chain_id.astype(int), vs.vault.str.lower()))
    tot = ins = 0.0
    by_unit = defaultdict(float)
    unit_of = {(int(c), v.lower()): (u, r) for c, v, u, r in zip(t.chain_id, t.vault, t.loan_unit, t.reason)}
    for f in ("vault_daily.csv", "lending_daily.csv"):
        cols = ["chain_id", "vault", "aum_usd", "held_by_vaults_usd"] + (
            ["excluded_reason"] if f == "vault_daily.csv" else ["governed"])
        df = pd.read_csv(DERIVED / "aum" / f, usecols=cols, low_memory=False)
        if f == "vault_daily.csv":
            df = df[df.excluded_reason.isna()]
        else:
            df = df[df.governed.astype(str) == "True"]
        df["net"] = df.aum_usd.fillna(0) - df.held_by_vaults_usd.fillna(0)
        for (c, v), x in df.groupby(["chain_id", "vault"]).net.sum().items():
            k = (int(c), v.lower())
            tot += x
            if k in keys:
                ins += x
            else:
                u, r = unit_of.get(k, (None, "not_enumerated"))
                by_unit[r if r != "loan_not_pegged" else f"loan_not_pegged:{u}"] += x
    out["curated_net_usd_days"] = {"all": tot, "sample": ins, "sample_share": ins / tot if tot else None,
                                   "outside_by_reason": {k: x / tot for k, x in
                                                         sorted(by_unit.items(), key=lambda kv: -kv[1])}}
    return out


def params_meta(scen):
    return {"rho": RHO, "eta": ETA, "depths": list(dict.fromkeys(scen["depths"])),
            "headline_depth": scen["headline"], "event_median_depth": scen.get("event_median_depth"),
            "crash_depths": scen["crash"], "min_usd": MIN_USD, "peak_fraction": PEAK_FRAC,
            "min_active_days": MIN_ACTIVE_DAYS, "max_daily_return": MAX_DAILY_RET, "apy_cap": APY_CAP,
            "max_outside_share": MAX_OUTSIDE_SHARE, "crash_horizon_days": CRASH_HORIZON_D,
            "crash_percentile": CRASH_PCT, "crash_jump_days": CRASH_JUMP_D, "n_perm": N_PERM, "n_boot": N_BOOT, "seed": SEED,
            "wrapper_depeg": WRAPPER_DEPTH, "reference_symbols": {k: sorted(v) for k, v in REFERENCE_SYMBOLS.items()}}


AXES = [("vault_apy", "Vault APY", 1), ("yield_after_fee", "Net yield", 1),
        ("tl_headline", "Safety", -1), ("L_p10", "Liquidity", 1), ("event_resilience", "Event resilience", 1)]


def fig_data(vs, cs, depths):
    rows = []
    pool = cs.copy()
    for m, lab, s in AXES:
        if m in pool:
            pool["pct_" + m] = (s * pool[m]).rank(pct=True) * 100.0
    for _, r in cs.head(12).iterrows():
        for m, lab, s in AXES:
            rows.append({"rank_by_aum": r.rank_by_aum, "curator_entity": r.curator_entity,
                         "curator_name": r.curator_name, "aum_avg_usd": r.aum_avg_usd, "axis": lab,
                         "metric": m, "value": r.get(m, np.nan),
                         "percentile": float(pool.loc[pool.curator_entity == r.curator_entity,
                                                      "pct_" + m].iloc[0]) if m in pool else np.nan})
    write_csv(pd.DataFrame(rows), f"{OUT_REL}/fig3_data.csv",
              {"what": "Fig 3: percentile of each of the 12 largest curators (by average daily net AUM) "
                       "among all attributed curators on five axes; safety from tl_headline (oracle-aware); "
                       "event resilience is left for E5",
               "n_curators_in_pool": int(len(cs)), "column_rename": COLUMN_RENAME})
    v = vs[vs.status == "in"].dropna(subset=["tl_headline", "yield_after_fee", "L_p10"]).copy()
    idx = frontier_2d(v.tl_headline.values, v.yield_after_fee.values)
    v["frontier_2d"] = False
    v.iloc[idx, v.columns.get_loc("frontier_2d")] = True
    v["label"] = ""
    lab_idx = set(idx.tolist()) | {int(np.argmin(v.tl_headline.values)), int(np.argmax(v.yield_after_fee.values))}
    for i in lab_idx:
        r = v.iloc[i]
        v.iloc[i, v.columns.get_loc("label")] = r.curator_name or (r["name"] or r.vault[:10])
    cols = ["chain_id", "vault", "name", "curator_entity", "curator_name", "protocol", "kind", "tl_headline",
            "yield_after_fee", "aum_mean_usd", "L_p10", "non_dominated", "frontier_2d", "label",
            "tl_v1_headline", "non_dominated_v1"]
    write_csv(v[cols], f"{OUT_REL}/fig4_data.csv",
              {"what": "Fig 4: one point per sample vault; non_dominated on (yield, tl_headline, liquidity), "
                       "non_dominated_v1 with tl_v1_headline (appendix), frontier_2d on (tl_headline, yield); "
                       "labels on the 2D frontier and at the two ends", "column_rename": COLUMN_RENAME})


def stage_figures():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    f3 = pd.read_csv(OUT / "fig3_data.csv")
    cur = f3.drop_duplicates("curator_entity").sort_values("rank_by_aum")
    axes_lab = [lab for _m, lab, _s in AXES]
    ang = np.linspace(0, 2 * np.pi, len(axes_lab), endpoint=False)
    fig, axs = plt.subplots(3, 4, figsize=(11, 8.6), subplot_kw={"projection": "polar"})
    for ax, (_, c) in zip(axs.flat, cur.iterrows()):
        sub = f3[f3.curator_entity == c.curator_entity].set_index("axis").reindex(axes_lab)
        vals = sub.percentile.values
        ok = np.isfinite(vals)
        a_ok = np.append(ang[ok], ang[ok][0])
        v_ok = np.append(vals[ok], vals[ok][0])
        ax.plot(np.append(ang, ang[0]), [50] * (len(ang) + 1), color="0.6", lw=0.7)
        ax.fill(a_ok, v_ok, color="#3b6ea8", alpha=0.35, lw=0)
        ax.plot(a_ok, v_ok, color="#3b6ea8", lw=1.0)
        ax.set_ylim(0, 100)
        ax.set_yticks([50, 100])
        ax.set_yticklabels([])
        ax.set_xticks(ang)
        ax.set_xticklabels([x if np.isfinite(vv) else x + "\n(E5)" for x, vv in zip(axes_lab, vals)],
                           fontsize=6.5)
        ax.set_title(c.curator_name, fontsize=9, pad=10)
    for ax in axs.flat[len(cur):]:
        ax.set_visible(False)
    fig.suptitle("Curator profiles: percentile among curators (outer = better); grey ring = median curator",
                 fontsize=10)
    fig.tight_layout()
    DRAFTS.mkdir(parents=True, exist_ok=True)
    fig.savefig(DRAFTS / "fig3_radar_draft.pdf")
    plt.close(fig)

    f4 = pd.read_csv(OUT / "fig4_data.csv")
    fig, ax = plt.subplots(figsize=(7.2, 5.0))
    markers = {"morpho": "o", "euler_v2": "s", "silo_v2": "^", "moolah": "D"}
    size = 8 + 220 * np.sqrt(f4.aum_mean_usd / f4.aum_mean_usd.max())
    sc = None
    for proto, mk in markers.items():
        m = f4.protocol == proto
        if m.any():
            sc = ax.scatter(f4.tl_headline[m] * 100, f4.yield_after_fee[m] * 100, s=size[m], c=f4.L_p10[m],
                            cmap="Blues", vmin=0, vmax=1, marker=mk, edgecolors="0.35", linewidths=0.3,
                            alpha=0.85)
    lo, hi = np.nanpercentile(f4.yield_after_fee * 100, [2, 98])
    lo, hi = min(lo, 0) - 2, hi + 3
    fr = f4[f4.frontier_2d].sort_values("tl_headline")
    ax.step(fr.tl_headline * 100, (fr.yield_after_fee * 100).clip(lo, hi), where="post", color="0.15", lw=1.0)
    shown = f4[(f4.label.notna()) & (f4.label != "") & (f4.yield_after_fee * 100 <= hi)
               & (f4.yield_after_fee * 100 >= lo)]
    for _, r in shown.iterrows():
        ax.annotate(r.label, (r.tl_headline * 100, r.yield_after_fee * 100), fontsize=6.5,
                    xytext=(3, 3), textcoords="offset points")
    out = int(((f4.yield_after_fee * 100 > hi) | (f4.yield_after_fee * 100 < lo)).sum())
    ax.set_ylim(lo, hi)
    ax.text(0.01, 0.01, f"{out} vaults outside the y-range", transform=ax.transAxes, fontsize=6.5, color="0.4")
    ax.set_xlabel("Stress tail loss at the headline scenario (%)")
    ax.set_ylabel("Net realized yield after fee (% per year)")
    if sc is not None:
        cb = fig.colorbar(sc, ax=ax, fraction=0.04, pad=0.02)
        cb.set_label("Liquidity, 10th percentile of L(t)")
    handles = [Line2D([], [], marker=mk, ls="", color="0.4", label=p) for p, mk in markers.items()
               if (f4.protocol == p).any()]
    ax.legend(handles=handles, fontsize=7, loc="lower right", frameon=False)
    fig.tight_layout()
    fig.savefig(DRAFTS / "fig4_frontier_draft.pdf")
    plt.close(fig)
    print("figures written", flush=True)


class Indep:
    """Independent recomputation of one vault-day from the raw positions and panel rows with integer arithmetic (csv module only)."""

    def __init__(self, events_dir):
        ev = jsonl(events_dir / "events.jsonl")
        self.trig = {}
        for e in ev:
            if e["type"] == "depeg_loss":
                for m in e["affected_markets"]:
                    k = (m["protocol"], m["chain_id"], m["market"].lower())
                    t = m.get("first_trigger_ts") or e["onset_ts"]
                    self.trig[k] = min(t, self.trig.get(k, t))
        self.windows = defaultdict(list)
        sp = OUT / "stuck_spells.csv"
        if STUCK_N and sp.exists():
            with open(sp, newline="") as fh:
                for r in csv.DictReader(fh):
                    if r["stuck"] == "True":
                        last = r["exit_day"] or r["last_pinned"]
                        self.windows[(r["protocol"], int(r["chain_id"]), r["market"].lower())].append(
                            (day_of(r["start"]) + 1, day_of(last)))
        self.e3 = {}
        with open(DERIVED / "aum/vault_daily.csv", newline="") as fh:
            for r in csv.DictReader(fh):
                self.e3[(int(r["chain_id"]), r["vault"].lower(), r["date"])] = (int(r["total_assets"]),
                                                                                 int(r["total_supply"]))
        self.cache = {}

    def panel_eod(self, proto, cid, m):
        """{day: (aps, liquidity)} at the end of every day with a row (exact)."""
        key = (proto, cid, m)
        if key in self.cache:
            return self.cache[key]
        out = {}
        if proto in ("morpho", "moolah"):
            with open(DATA / f"panel/{proto}/{cid}/{m}.csv", newline="") as fh:
                for r in csv.DictReader(fh):
                    sa, ss, ba = int(r["total_supply_assets"]), int(r["total_supply_shares"]), \
                        int(r["total_borrow_assets"])
                    out[int(r["timestamp"]) // DAY] = ((sa + 1) / (ss + 10 ** 6), max(sa - ba, 0))
        elif proto == "euler_v2":
            with open(DATA / f"panel/euler_v2/{cid}/{m}.csv", newline="") as fh:
                for r in csv.DictReader(fh):
                    c, b, s = int(r["cash"]), int(r["totalBorrows"]), int(r["totalShares"])
                    out[int(r["timestamp"]) // DAY] = ((c + b + 10 ** 6) / (s + 10 ** 6), c)
        self.cache[key] = out
        return out

    def at(self, eod, d):
        days = [x for x in eod if x <= d]
        return eod[max(days)] if days else (None, 0)

    def econ_aps(self, proto, cid, m, d):
        """Value per share with the freeze."""
        eod = self.panel_eod(proto, cid, m)
        trig = self.trig.get((proto, cid, m))
        wins = list(self.windows.get((proto, cid, m), []))
        if trig is not None:
            wins.append((trig // DAY + 1, math.inf))
        aps, liq = self.at(eod, d)
        if not wins or aps is None:
            return aps, liq
        first = min(a for a, _ in wins)
        if d < first:
            return aps, liq
        val = self.at(eod, first - 1)[0]
        prev = val
        for day in range(first, d + 1):
            cur = self.at(eod, day)[0]
            if prev and cur is not None:
                if any(a <= day <= b for a, b in wins):
                    if cur < prev:
                        val = val * cur / prev
                else:
                    val = val * cur / prev
            prev = cur
        return val, liq

    @staticmethod
    def change(sh0, sh1, a0, a1, q0, q1, m):
        """Value change of the shares held at the start of the day, with the withdraw-queue rule (dropped."""
        i0 = q0 is None or m in q0
        i1 = q1 is None or m in q1
        if i0 and i1:
            return sh0 * (a1 - a0)
        if i0:
            return sh0 * (a1 - a0) - sh1 * a1
        if i1:
            return sh0 * a1
        return 0.0

    def queue(self, proto, cid, v, d):
        """Withdraw queue at the end of day d (None for vaults without one)."""
        key = ("queue", proto, cid, v)
        if key not in self.cache:
            evs = []
            with open(DATA / f"vaults/{proto}/{cid}/actions.jsonl") as fh:
                for line in fh:
                    if '"SetWithdrawQueue"' in line and v in line:
                        r = json.loads(line)
                        if r["vault"].lower() == v:
                            q = (r.get("fields") or r)["newWithdrawQueue"]
                            evs.append(((r["block"], r["log_index"]), r["timestamp"] // DAY,
                                        {x.lower() for x in q}))
            self.cache[key] = sorted(evs, key=lambda x: x[0])
        evs = self.cache[key]
        if not evs:
            return None
        cur = [q for _k, day, q in evs if day <= d]
        return cur[-1] if cur else set()

    def shares(self, path, col, shcol, d):
        bal = {}
        with open(path, newline="") as fh:
            for r in csv.DictReader(fh):
                if int(r["timestamp"]) // DAY <= d:
                    bal[r[col].lower()] = int(r[shcol])
        return bal

    def vault(self, cid, proto, kind, v, d):
        """(value, withdrawable, held-value change over the day) in raw units."""
        V = W = num = 0.0
        if proto in ("morpho", "moolah"):
            path = DATA / f"vaults/{proto}/{cid}/positions/{v}.csv"
            now = self.shares(path, "market_id", "vault_cum_shares", d)
            before = self.shares(path, "market_id", "vault_cum_shares", d - 1)
            q1, q0 = self.queue(proto, cid, v, d), self.queue(proto, cid, v, d - 1)
            for m, sh in now.items():
                aps, liq = self.econ_aps(proto, cid, m, d)
                if sh and aps and (q1 is None or m in q1):
                    V += sh * aps
                    W += min(sh * aps, liq)
            for m, sh in before.items():
                a1, _ = self.econ_aps(proto, cid, m, d)
                a0, _ = self.econ_aps(proto, cid, m, d - 1)
                if sh and a1 is not None and a0 is not None:
                    num += self.change(sh, now.get(m, 0), a0, a1, q0, q1, m)
            if kind == "morpho_v2":
                wp = DATA / f"vaults/morpho/{cid}/adapter_shares.csv"
                held = defaultdict(lambda: [0, 0])
                with open(wp, newline="") as fh:
                    last = {}
                    for r in csv.DictReader(fh):
                        if r["vault"].lower() != v:
                            continue
                        t = int(r["timestamp"]) // DAY
                        for k, dd in ((0, d), (1, d - 1)):
                            if t <= dd:
                                last[(r["adapter"], r["underlying_vault"].lower(), k)] = int(r["cum_shares"])
                    for (ad, u, k), sh in last.items():
                        held[u][k] += sh
                for u, (sh1, sh0) in held.items():
                    if not (sh1 or sh0):
                        continue
                    vu1, wu1, _ = self.vault(cid, "morpho", self.kind_of(cid, u), u, d)
                    vu0, _, _ = self.vault(cid, "morpho", self.kind_of(cid, u), u, d - 1)
                    s1 = self.e3_at(cid, u, d)[1]
                    s0 = self.e3_at(cid, u, d - 1)[1]
                    p1 = vu1 / s1 if s1 else 0.0
                    p0 = vu0 / s0 if s0 else 0.0
                    V += sh1 * p1
                    W += min(sh1 * p1, wu1)
                    num += sh0 * (p1 - p0)
                ta = self.e3_at(cid, v, d)[0]
                idle = max(ta - V, 0.0)
                V += idle
                W += idle
        elif proto == "euler_v2" and kind == "euler_lending":
            a1, liq = self.econ_aps("euler_v2", cid, v, d)
            a0, _ = self.econ_aps("euler_v2", cid, v, d - 1)
            with open(DATA / f"panel/euler_v2/{cid}/{v}.csv", newline="") as fh:
                for r in csv.DictReader(fh):
                    if int(r["timestamp"]) // DAY <= d:
                        V = int(r["cash"]) + int(r["totalBorrows"])
            W = liq
            num = ("ratio", a1 / a0 - 1.0 if a0 else float("nan"))
        elif proto == "euler_v2":
            path = DATA / f"vaults/euler_v2/{cid}/positions/{v}.csv"
            now = self.shares(path, "strategy", "cum_shares", d)
            before = self.shares(path, "strategy", "cum_shares", d - 1)
            q1, q0 = self.queue("euler_v2", cid, v, d), self.queue("euler_v2", cid, v, d - 1)
            for m, sh in {**before, **now}.items():
                if sh and not (DATA / f"panel/euler_v2/{cid}/{m}.csv").exists():
                    return None
            for m, sh in now.items():
                aps, liq = self.econ_aps("euler_v2", cid, m, d)
                if sh and aps and (q1 is None or m in q1):
                    V += sh * aps
                    W += min(sh * aps, liq)
            for m, sh in before.items():
                a1, _ = self.econ_aps("euler_v2", cid, m, d)
                a0, _ = self.econ_aps("euler_v2", cid, m, d - 1)
                if sh and a1 is not None and a0 is not None:
                    num += self.change(sh, now.get(m, 0), a0, a1, q0, q1, m)
        elif proto == "silo_v2":
            path = DATA / f"vaults/silo_v2/{cid}/positions/{v}.csv"
            now = self.shares(path, "market", "cum_shares", d)
            before = self.shares(path, "market", "cum_shares", d - 1)
            q1, q0 = self.queue("silo_v2", cid, v, d), self.queue("silo_v2", cid, v, d - 1)
            for m, sh in now.items():
                got = self.silo_aps(cid, m, d)
                if got is None:
                    if sh:
                        return None
                    continue
                aps, liq = got
                if sh and aps and (q1 is None or m in q1):
                    V += sh * aps
                    W += min(sh * aps, liq)
            for m, sh in before.items():
                g1, g0 = self.silo_aps(cid, m, d), self.silo_aps(cid, m, d - 1)
                if sh and g1 and g0:
                    num += self.change(sh, now.get(m, 0), g0[0], g1[0], q0, q1, m)
        else:
            return None
        return V, W, num

    def silo_anchors(self, cid):
        """{market: [(day, assets / shares)]} from every vault's Deposit and Withdraw rows on the chain."""
        key = ("silo_anchors", cid)
        if key not in self.cache:
            out = defaultdict(list)
            d = DATA / f"vaults/silo_v2/{cid}/positions"
            for fn in os.listdir(d):
                if not fn.endswith(".csv"):
                    continue
                with open(d / fn, newline="") as fh:
                    for r in csv.DictReader(fh):
                        if r["kind"] in ("Deposit", "Withdraw") and r["assets"] and r["event_shares"]:
                            a, sh = int(r["assets"]), int(r["event_shares"])
                            if a > 0 and sh > 0:
                                out[r["market"].lower()].append((int(r["timestamp"]), a, sh))
            self.cache[key] = out
        return self.cache[key]

    def silo_rows(self, cid, m):
        """[(day, C, D, S)] after every row, share supply replayed with the verified SiloMathLib conversion (offset 1000, +1 asset)."""
        key = ("silo_rows", cid, m)
        if key not in self.cache:
            rows, S, prev_c = [], 0, 0
            with open(DATA / f"panel/silo_v2/{cid}/{m}.csv", newline="") as fh:
                for r in csv.DictReader(fh):
                    if r["event"] in ("Deposit", "Withdraw"):
                        amt = int(r["amount"])
                        num_, den = S + 1000, (prev_c + 1) if S > 0 else 1
                        S = S + amt * num_ // den if r["event"] == "Deposit" else max(0, S - (-(-amt * num_ // den)))
                    prev_c = int(r["collateral_assets"])
                    rows.append((int(r["ts"]) // DAY, prev_c, int(r["debt_assets"]), S))
            self.cache[key] = rows
        return self.cache[key]

    def silo_aps(self, cid, m, d):
        """(assets per share, liquidity) of a Silo holding at the end of day d."""
        anc = sorted(self.silo_anchors(cid).get(m, []))
        if not (DATA / f"panel/silo_v2/{cid}/{m}.csv").exists():
            last = [a / sh for t, a, sh in anc if t // DAY <= d]
            return (last[-1], float("inf")) if last else None
        rows = self.silo_rows(cid, m)

        def rep(day):
            cur = [x for x in rows if x[0] <= day]
            if not cur:
                return None, 0
            _dd, C, D, S = cur[-1]
            return (C + 1) / (S + 1000), max(C - D, 0)
        fac = 1.0
        by_day = {}
        for t, a, sh in anc:
            by_day[t // DAY] = a / sh
        days = sorted(x for x in by_day if x <= d)
        if days:
            r0 = rep(days[-1])[0]
            fac = by_day[days[-1]] / r0 if r0 else 1.0

        def aps_at(day):
            ds = sorted(x for x in by_day if x <= day)
            f = by_day[ds[-1]] / rep(ds[-1])[0] if ds and rep(ds[-1])[0] else 1.0
            return rep(day)[0] * f if rep(day)[0] is not None else None
        aps, liq = rep(d)
        if aps is None:
            return None
        aps *= fac
        trig = self.trig.get(("silo_v2", cid, m))
        if trig is not None and d > trig // DAY:
            t = trig // DAY
            val = prev = aps_at(t)
            for day in range(t + 1, d + 1):
                cur = aps_at(day)
                if prev and cur is not None and cur < prev:
                    val = val * cur / prev
                prev = cur
            aps = val
        return aps, liq

    def kind_of(self, cid, u):
        for proto in CHAINS[cid]:
            for r in jsonl(DATA / f"registry/vaults/{proto}/{cid}.jsonl"):
                if r["vault"].lower() == u:
                    return r.get("kind")
        return None

    def e3_at(self, cid, v, d):
        for dd in range(d, d - 4000, -1):
            x = self.e3.get((cid, v, day_str(dd)))
            if x is not None:
                return x
        return (0, 0)


def stage_validate(events_dir):
    rng = np.random.default_rng(SEED + 1)
    daily = pd.read_csv(OUT / "vault_daily.csv", low_memory=False)
    vs = pd.read_csv(OUT / "vault_scorecard.csv", low_memory=False)
    res = {}
    ind = Indep(events_dir)
    cand = daily[(daily.active == 1) & daily.ret_before_fee.notna()]
    picks = []
    tries = 0
    while len(picks) < 20 and tries < 200:
        tries += 1
        r = cand.iloc[int(rng.integers(len(cand)))]
        d = day_of(r.date)
        got = ind.vault(int(r.chain_id), r.protocol, r.kind, r.vault, d)
        if got is None:
            continue
        V, W, num = got
        if r.kind == "euler_lending":
            ret = num[1]
            out_ret = r.ret_after_fee
        else:
            V0 = ind.vault(int(r.chain_id), r.protocol, r.kind, r.vault, d - 1)[0]
            ret = num / V0 if V0 else np.nan
            out_ret = r.ret_before_fee
        L = W / V if V else np.nan
        picks.append({"chain_id": int(r.chain_id), "vault": r.vault, "kind": r.kind, "date": r.date,
                      "value_main": float(r.value_raw), "value_indep": float(V),
                      "ret_main": float(out_ret), "ret_indep": float(ret),
                      "L_main": float(r.L), "L_indep": float(L),
                      "value_rel_diff": abs(V - r.value_raw) / max(abs(V), 1e-30),
                      "ret_abs_diff": abs(ret - out_ret), "L_abs_diff": abs(L - r.L)})
    ok = [p for p in picks if p["value_rel_diff"] < 1e-9 and p["ret_abs_diff"] < 1e-12 and p["L_abs_diff"] < 1e-9]
    res["independent_vault_days"] = {
        "drawn": len(picks), "identical": len(ok),
        "tolerance": "value relative 1e-9, return absolute 1e-12, L absolute 1e-9 (float64 against exact integers)",
        "rows": picks}
    v1 = vs[(vs.status == "in") & (vs.kind == "morpho_v1")]
    pick = v1.sample(n=min(30, len(v1)), random_state=int(rng.integers(1 << 30)))
    e3 = pd.read_csv(DERIVED / "aum/vault_daily.csv", dtype=str)
    rows = []
    for _, r in pick.iterrows():
        dd = daily[(daily.chain_id == r.chain_id) & (daily.vault == r.vault)]
        ee = e3[(e3.chain_id == str(r.chain_id)) & (e3.vault == r.vault) & (e3.source == "accrue_interest")]
        ee = ee[(ee.date >= r.first_active) & (ee.date <= r.last_active)]
        if len(ee) < 2:
            continue
        s, e = ee.iloc[0], ee.iloc[-1]
        p0 = int(s.total_assets) / int(s.total_supply)
        p1 = int(e.total_assets) / int(e.total_supply)
        n_days = day_of(e.date) - day_of(s.date)
        seg = dd[(dd.date > s.date) & (dd.date <= e.date)]
        growth = float(np.prod(1.0 + seg.ret_after_fee.fillna(0.0)))
        y_pos = growth ** (365.0 / n_days) - 1.0 if growth > 0 else float("nan")
        y_sp = (p1 / p0) ** (365.0 / n_days) - 1.0
        ew = ee.set_index("date")
        sj = seg.set_index("date").join(ew[["total_assets"]], how="left")
        dev = (sj.value_raw / sj.total_assets.astype(float) - 1.0).abs()
        rows.append({"chain_id": int(r.chain_id), "vault": r.vault, "name": r["name"], "from": s.date,
                     "to": e.date, "days": int(n_days), "yield_positions_after_fee": y_pos,
                     "yield_share_price": y_sp, "diff_pp": (y_pos - y_sp) * 100,
                     "excluded_days_in_yield": int(r.return_days_excluded),
                     "window_days_ret_above_3pct": int((seg.ret_before_fee.abs() > MAX_DAILY_RET).sum()),
                     "window_e3_guard_days": int((e3[(e3.chain_id == str(r.chain_id)) & (e3.vault == r.vault)
                                                     & (e3.date > s.date) & (e3.date <= e.date)]
                                                  .guard.fillna("") != "").sum()),
                     "window_event_days_value_off_total_assets_1pct": int((dev > 0.01).sum()),
                     "window_inactive_days": int((seg.active == 0).sum())})
    diffs = np.array([x["diff_pp"] for x in rows], dtype=float)
    fin = diffs[np.isfinite(diffs)]
    tol = 0.25
    res["yield_vs_share_price_v1"] = {
        "vaults": len(rows), "tolerance_pp_per_year": tol,
        "within_tolerance": int((np.abs(fin) <= tol).sum()),
        "without_position_yield": int((~np.isfinite(diffs)).sum()),
        "abs_diff_pp_median": float(np.median(np.abs(fin))) if len(fin) else None,
        "abs_diff_pp_max": float(np.max(np.abs(fin))) if len(fin) else None,
        "method": "both over the same window from the first to the last active day with a vault event "
                  "(E3 share price exact on event days); position yield compounds every daily after-fee "
                  "return in the window, excluded days included",
        "rows": rows}
    mv = vs[(vs.status == "in") & (vs.kind == "morpho_v1") & (vs.apy_api_coverage > 0.5)]
    pick = mv.sample(n=min(30, len(mv)), random_state=int(rng.integers(1 << 30)))
    rows = []
    for _, r in pick.iterrows():
        dd = daily[(daily.chain_id == r.chain_id) & (daily.vault == r.vault) & (daily.active == 1)]
        best = {}
        for shift in (-1, 0, 1):
            a = dd.api_apy_gross.shift(shift).values
            b = dd.apy_rebuilt_gross.values
            m = np.isfinite(a) & np.isfinite(b)
            best[shift] = float(np.median(np.abs(a[m] - b[m]))) if m.any() else np.nan
        a = dd.api_apy_gross.values
        b = dd.apy_rebuilt_gross.values
        m = np.isfinite(a) & np.isfinite(b)
        an = dd.api_net_apy_without_rewards.values
        bn = dd.apy_rebuilt_net.values
        mn = np.isfinite(an) & np.isfinite(bn)
        rows.append({"chain_id": int(r.chain_id), "vault": r.vault, "name": r["name"], "days": int(m.sum()),
                     "median_abs_diff_gross_pp": float(np.median(np.abs(a[m] - b[m]))) * 100,
                     "p90_abs_diff_gross_pp": float(np.percentile(np.abs(a[m] - b[m]), 90)) * 100,
                     "median_abs_diff_net_pp": float(np.median(np.abs(an[mn] - bn[mn]))) * 100,
                     "mean_api_gross": float(np.mean(a[m])), "mean_rebuilt_gross": float(np.mean(b[m])),
                     "share_days_within_0p5pp": float((np.abs(a[m] - b[m]) <= 0.005).mean()),
                     "median_abs_diff_by_shift_pp": {str(k): v * 100 for k, v in best.items()}})
    med = np.array([x["median_abs_diff_gross_pp"] for x in rows])
    pooled = daily[(daily.active == 1) & daily.vault.isin(pick.vault) & daily.api_apy_gross.notna()]
    diff = (pooled.api_apy_gross - pooled.apy_rebuilt_gross).abs()
    calm = pooled.api_apy_gross <= APY_CAP
    res["apy_rebuilt_vs_morpho_api_v1"] = {
        "vaults": len(rows),
        "pooled_vault_days": int(len(pooled)),
        "pooled_share_within_0p1pp": float((diff <= 0.001).mean()),
        "pooled_share_within_0p5pp": float((diff <= 0.005).mean()),
        "pooled_days_api_above_cap": int((~calm).sum()),
        "pooled_median_abs_diff_pp_api_at_most_cap": float(diff[calm].median() * 100),
        "pooled_share_within_0p5pp_api_at_most_cap": float((diff[calm] <= 0.005).mean()),
        "per_vault_median_abs_diff_pp": {"median": float(np.median(med)), "p90": float(np.percentile(med, 90)),
                                         "max": float(med.max())} if len(med) else None,
        "whole_period_morpho_v1": whole_period_apy(vs, "morpho_v1"),
        "whole_period_morpho_v2": whole_period_apy(vs, "morpho_v2"),
        "alignment": "API point at 00:00 UTC of day D compared with the rebuilt value at the end of day D-1; "
                     "median_abs_diff_by_shift_pp shows shifts of -1, 0, +1 days",
        "rows": rows}
    tag100 = "tl_" + depth_tag(1.0)
    res["tail_loss_sanity"] = {"oracle_aware_against_first_pass": sanity_v2(),
                               "appendix_v1_tracks_market_single_family": sanity_tracks()}
    crash = vs[(vs.status == "in") & (vs.share_unpegged_mean > 0.999)]
    res["tail_loss_sanity"]["unpegged_only"] = {
        "vaults": int(len(crash)),
        "with_positive_crash_loss": int((crash.tl_crash > 0).sum()),
        "with_positive_crash_loss_v1": int((crash.tl_v1_crash > 0).sum()),
        "with_zero_depeg_loss_at_1": int((crash[tag100] < 1e-12).sum()),
        "note": "oracle-aware crash: a repricing oracle loses only the one-day move j (the largest "
                "one-day drop of the pair), so a market loses only if LLTV > (1 - j)(1 - eta)",
        "lltv_threshold_eth_usd": crash_lltv_threshold("ETH|USD"),
        "lltv_threshold_btc_usd": crash_lltv_threshold("BTC|USD"),
        "examples": crash.sort_values("aum_mean_usd", ascending=False).head(5)[
            ["chain_id", "vault", "name", "curator_name", "tl_crash", "tl_v1_crash", tag100,
             "tl_headline"]].to_dict("records")}
    io_utils.write_json(f"{OUT_REL}/validation.json", res, GENERATOR,
                        {"what": "E4 validation: independent vault-days, yield against share price, "
                                 "APY against the Morpho API, tail-loss sanity"})
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "rows"} if isinstance(v, dict) else v
                      for k, v in res.items()}, indent=1, default=str)[:4000], flush=True)


def whole_period_apy(vs, kind):
    """Scorecard vault APY from the API against the rebuilt one (both net of fee, without rewards, median of capped daily value."""
    x = vs[(vs.status == "in") & (vs.kind == kind) & (vs.apy_api_coverage > 0.5)]
    diff = (x.vault_apy - x.apy_rebuilt_net).abs() * 100
    return {"vaults": int(len(x)), "median_abs_diff_pp": float(diff.median()),
            "p90_abs_diff_pp": float(diff.quantile(0.9)), "share_within_0p25pp": float((diff <= 0.25).mean()),
            "spearman": spearman(x.vault_apy.values, x.apy_rebuilt_net.values) if len(x) > 2 else None}


def sanity_v2():
    """Oracle-aware loss against the first-pass formula on vault-days whose collateral is all pegged."""
    series = load_series()
    eq = le = 0
    eq_diff = le_viol = 0.0
    for s in series.values():
        K = len(s["depths"])
        for d in s["vaults"].values():
            base = d["active"] & (d["share_unpeg"] < 1e-12) & (d["share_outside"] < 1e-12)
            full = 1.0 - d["idle_share"]
            same = base & (np.abs(d["share_v12same"] - full) < 1e-9) & (full > 0)
            track = base & (np.abs(d["share_trackpeg"] - full) < 1e-9) & (full > 0)
            if same.any():
                eq += int(same.sum())
                eq_diff = max(eq_diff, float(np.abs(d["tl2"][same, :K] - d["tl"][same, :K]).max()))
            if track.any():
                le += int(track.sum())
                le_viol = max(le_viol, float((d["tl2"][track, :K] - d["tl"][track, :K]).max()))
    return {"no_liquidation_in_both_vault_days": eq, "max_abs_diff_v2_v1": eq_diff,
            "tracks_market_vault_days": le, "max_excess_v2_over_v1": le_viol}


def crash_lltv_threshold(pair):
    """LLTV above which a repricing oracle still loses under the crash of `pair`."""
    jump = next(iter(load_series().values()))["scenario"]["crash"][pair]["jump"]
    return (1.0 - jump) * (1.0 - ETA)


def sanity_tracks():
    """First-pass formula (appendix)."""
    series = load_series()
    rows = []
    for cid, s in series.items():
        for v, d in s["vaults"].items():
            m = (d["active"] & (np.abs(d["share_trackpeg"] - (1.0 - d["idle_share"])) < 1e-9)
                 & (d["nfam"] == 1) & (d["share_unpeg"] < 1e-12) & (d["share_outside"] < 1e-12))
            for i in np.flatnonzero(m):
                rows.append((cid, v, d["first_day"] + i, d["tl"][i, 3], d["share_us"][i]))
    rng = np.random.default_rng(SEED + 2)
    pick = [rows[i] for i in rng.choice(len(rows), size=min(20, len(rows)), replace=False)] if rows else []
    diffs = [abs(a - b) for _c, _v, _d, a, b in rows]
    return {"tracks_market_single_family": {
        "vault_days": len(rows), "max_abs_diff": float(max(diffs)) if diffs else None,
        "equal_within_1e-10": int(sum(x < 1e-10 for x in diffs)),
        "examples": [{"chain_id": c, "vault": v, "date": day_str(dd), "tl_v1_d1000": float(a),
                      "stressed_utilization_share": float(b)} for c, v, dd, a, b in pick]}}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--stage", default="all",
                    choices=["series", "api", "outputs", "figures", "validate", "all"])
    ap.add_argument("--events-dir", default=str(DERIVED / "events"),
                    help="E2 outputs to read (events.jsonl, collateral_families.jsonl, supplements)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--only", type=int, nargs="*", help="chains for the series stage")
    ap.add_argument("--freeze-stuck", type=int, metavar="N", default=None,
                    help=f"N of the stuck freeze (default {STUCK_N_HEADLINE}, the headline, all stages, "
                         "outputs in data/metrics/e4/); 0: depeg freeze only, outputs in "
                         "data/metrics/e4_depeg_only/; another N: sensitivity, outputs in "
                         "data/metrics/e4_stuck_n<N>/. Other than the headline: series, api and outputs only")
    ap.add_argument("--wrapper-depeg", type=float, metavar="D", default=0.0,
                    help="0 (default, the headline): a wrapper's depeg against the crash's reference is "
                         "not scored; D in (0, 1]: appendix sensitivity, a wrapper also depegs by "
                         "min(delta, D) (series, api and outputs only; directories with the suffix "
                         "_wrapper_d<1000 D>)")
    args = ap.parse_args()
    if not 0.0 <= args.wrapper_depeg <= 1.0:
        ap.error("--wrapper-depeg takes a depth in [0, 1]")
    if args.wrapper_depeg != WRAPPER_DEPTH:
        os.environ["E4_WRAPPER_DEPEG"] = repr(args.wrapper_depeg)
        os.execv(sys.executable, [sys.executable] + sys.argv)
    side = (args.freeze_stuck is not None and args.freeze_stuck != STUCK_N_HEADLINE) or WRAPPER_DEPEG
    if side and args.stage not in ("series", "api", "outputs", "all"):
        ap.error("a side run (--freeze-stuck other than the headline, --wrapper-depeg above 0) runs the "
                 "series, api and outputs stages only")
    if args.freeze_stuck is not None:
        set_stuck(args.freeze_stuck)
    events_dir = pathlib.Path(args.events_dir)
    t0 = time.time()
    stages = ["series", "api", "outputs", "figures", "validate"] if args.stage == "all" else [args.stage]
    if side and args.stage == "all":
        stages = ["series", "api", "outputs"]
    for st in stages:
        if st == "series":
            stage_series(events_dir, args.workers, set(args.only) if args.only else None)
        elif st == "api":
            stage_api()
        elif st == "outputs":
            stage_outputs(events_dir)
        elif st == "figures":
            stage_figures()
        elif st == "validate":
            stage_validate(events_dir)
    print(f"done in {time.time() - t0:.0f} s", flush=True)


if __name__ == "__main__":
    main()
