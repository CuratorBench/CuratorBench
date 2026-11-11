# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "pycryptodome"]
# ///
"""CuratorBench observation stream (E8)."""
import bisect
import csv
import gzip
import hashlib
import json
import math
import os
import pathlib
import sys
from collections import defaultdict

import context as C

sys.path.insert(0, str(C.ROOT / "code" / "corpus"))
import corpus as K
import leakage as L

OBS = C.DATA / "bench" / "observations"
PANEL_EXT = C.DATA / "panel_ext"
PROMPTS = pathlib.Path(__file__).resolve().parent / "prompts"
GENERATOR = "code/autocurator/observe.py"

TABLE_MIN_USD = 100_000.0
UTIL_MOVE = 0.10
UTIL_PIN = 0.99
IDLE_DROP = 0.30
BAD_DEBT_MIN_USD = 1.0
PEG_OFF = 0.02
PRICE_DROP = 0.10
TOP1_MOVE = 0.10
SUPPLY_MOVE = 0.10
ALLOCATOR_MIN_USD = 1.0
MARKET_MIN_USD = 10_000.0
UTIL_MIN_D = 30
EVENT_DAYS = 7
DOCS_PER_EVENT = 5
PRICE_LAG_D = C.PRICE_LAG_D

SNAP_COLS = ("supply_usd", "borrow_usd", "util", "util_min30", "days_pinned", "idle_usd", "borrow_apr",
             "supply_apy", "fee", "debt_shares", "top1_share", "n_borrowers")
EVENT_ORDER = ("bad_debt", "util_pin", "util_move", "idle_drop", "top1_move", "allocator_in",
               "peg_off", "price_drop", "supply_move", "new_docs")
WRAPPER_RULES = C.e2.WRAPPER_RULES + ("pendle_principal",)
DIRECT_UNITS = {"usd_stable": "USD", "eth": "ETH", "btc": "BTC"}
DIRECT_TYPES = {"usd_stable": "USD stablecoin", "eth": "ETH", "btc": "BTC"}
PROTOCOL_NAMES = {"morpho": "Morpho", "moolah": "Moolah", "euler_v2": "Euler v2", "silo_v2": "Silo v2"}
CHAIN_NAMES = {1: "Ethereum", 8453: "Base", 4663: "Robinhood", 42161: "Arbitrum", 747474: "Katana",
               999: "HyperEVM", 9745: "Plasma", 59144: "Linea", 56: "BNB Chain", 43114: "Avalanche", 146: "Sonic"}


class LookaheadError(RuntimeError):
    """A read of data after the horizon."""


def r4(x):
    return None if x is None else round(float(x), 4)


def r6(x):
    return None if x is None else round(float(x), 6)


def usd0(x):
    return None if x is None else int(round(x))


def mechanics_text(proto):
    return (PROMPTS / f"{proto}.txt").read_text().strip()


class PegTable:
    """Peg unit of a collateral token and the token its peg is read on."""

    def __init__(self):
        self.rows = {}
        for r in C.jsonl(C.DATA / "prices/tokens.jsonl"):
            self.rows[(int(r["chain_id"]), r["token"])] = r
        self.fam_unit = {}
        p = C.INPUTS / "events/collateral_families.jsonl"
        if p.exists():
            for f in C.jsonl(p):
                if f.get("reference") in ("USD", "ETH", "BTC"):
                    for m in f.get("members") or []:
                        self.fam_unit[(int(m["chain_id"]), m["token"])] = f["reference"]
        self.by_symbol = defaultdict(list)
        for (c, t), r in self.rows.items():
            if r.get("symbol") and self.direct(c, t):
                self.by_symbol[(c, r["symbol"].upper())].append(t)

    def rule(self, cid, token):
        r = self.rows.get((int(cid), token)) or {}
        return (r.get("peg_rule") or "").split(":")[-1]

    def direct(self, cid, token):
        r = self.rows.get((int(cid), token)) or {}
        if r.get("peg_family") not in DIRECT_UNITS:
            return False
        rule = self.rule(cid, token)
        parts = rule.split("/")
        return not (parts[0] in WRAPPER_RULES or parts[-1] in WRAPPER_RULES or rule.endswith("_underlying")
                    or rule == "usd0pp_now_busd0")

    def peg(self, cid, token):
        """(unit, token the peg is read on) or (None, None)."""
        cid = int(cid)
        r = self.rows.get((cid, token)) or {}
        unit = self.fam_unit.get((cid, token)) or DIRECT_UNITS.get(r.get("peg_family"))
        if unit is None:
            return None, None
        if self.direct(cid, token):
            return unit, token
        ref = (r.get("reference") or "").upper()
        cand = self.by_symbol.get((cid, ref), [])
        if len(cand) == 1 and cand[0] != token:
            return unit, cand[0]
        return None, None

    def asset_type(self, cid, token):
        """One phrase from the E1 peg rules."""
        cid = int(cid)
        r = self.rows.get((cid, token)) or {}
        if self.direct(cid, token):
            return DIRECT_TYPES[r["peg_family"]]
        unit, _ref = self.peg(cid, token)
        unit = unit or DIRECT_UNITS.get(r.get("peg_family"))
        if "pendle_principal" in self.rule(cid, token):
            return "Pendle PT" + (f" on a {unit} asset" if unit else "")
        if unit:
            return f"{unit} wrapper or yield-bearing token"
        return "other"


def issuer_of(assets, key):
    """The issuer's name in the corpus asset table, without the notes the crawl appended in parentheses or after a semicolon (written after the fact), or None."""
    name = (assets.record(key) or {}).get("issuer_name") or ""
    name = name.split(";")[0].split(" (")[0].strip()
    return name or None


class Corpus:
    """Document metadata by corpus asset key (data/corpus/documents.jsonl), with the moment each becomes visible to the cutoff."""

    def __init__(self):
        self.by_asset = defaultdict(list)
        p = C.DATA / "corpus/documents.jsonl"
        drop = K.excluded()
        if p.exists():
            for r in C.jsonl(p):
                if r["id"] in drop:
                    continue
                m = L.margin(r)
                if m is None:
                    continue
                v = max(int(r["published_ts"]), int(r.get("modified_ts") or r["published_ts"])) + m
                row = (v, r["id"], (r.get("title") or "").strip(), r["source_type"])
                for a in r["assets"]:
                    self.by_asset[a].append(row)
        for a in self.by_asset:
            self.by_asset[a].sort()
        self.vis = {a: [x[0] for x in rows] for a, rows in self.by_asset.items()}

    def cut(self, horizon):
        out = Corpus.__new__(Corpus)
        end = (horizon + 1) * C.DAY
        out.by_asset = {a: [x for x in rows if x[0] < end] for a, rows in self.by_asset.items()}
        out.vis = {a: [x[0] for x in rows] for a, rows in out.by_asset.items()}
        return out

    def arrived(self, asset, t0, t1):
        """Documents that became visible in [t0, t1)."""
        vt = self.vis.get(asset)
        if not vt:
            return []
        i, j = bisect.bisect_left(vt, t0), bisect.bisect_left(vt, t1)
        return self.by_asset[asset][i:j]


def _bad_debt_rows(proto, cid):
    """[(ts, market, raw loan units written off)] of the pair's bad-debt events (module docstring)."""
    out = []
    base = C.e2.panel_dir(proto, cid)
    if proto in ("morpho", "moolah"):
        p = base / "liquidations.csv"
        if p.exists():
            with open(p) as fh:
                for r in csv.DictReader(fh):
                    x = int(r["bad_debt_assets"] or 0)
                    if x > 0:
                        out.append((int(r["timestamp"]), r["market_id"], x))
    elif proto == "euler_v2":
        p = base / "losses.jsonl"
        if p.exists():
            for r in C.jsonl(p):
                if r.get("event") == "DebtSocialized" and int(r.get("assets") or 0) > 0:
                    out.append((int(r["timestamp"]), r["vault"], int(r["assets"])))
    else:
        p = base / "defaults.jsonl"
        if p.exists():
            for r in C.jsonl(p):
                x = int(r.get("canceled_debt") or 0)
                if x > 0:
                    out.append((int(r["ts"]), r["silo"], x))
    out.sort()
    return out


def fee_history(proto, cid, market):
    """[(ts, fee)] of a Morpho or Moolah market from its panel's create and set_fee rows (cached in the work directory by file size and mtime)."""
    src = C.e2.panel_dir(proto, cid) / f"{market}.csv"
    if not src.exists():
        return []
    st = src.stat()
    cache = C.WORK / "fees" / proto / str(cid) / f"{market}.pkl"
    if cache.exists():
        obj = C.load_pickle(cache)
        if obj["size"] == st.st_size and obj["mtime"] == st.st_mtime:
            return obj["rows"]
    rows = []
    with open(src) as fh:
        head = fh.readline().rstrip("\n").split(",")
        k_, t_, f_ = head.index("kind"), head.index("timestamp"), head.index("fee")
        for line in fh:
            if ",create," not in line and ",set_fee," not in line:
                continue
            x = line.rstrip("\n").split(",")
            if x[k_] in ("create", "set_fee") and x[f_]:
                rows.append((int(x[t_]), int(x[f_]) / 1e18))
    C.save_pickle(cache, {"size": st.st_size, "mtime": st.st_mtime, "rows": rows})
    return rows


def _read_ext(path, key_col, cols):
    """{key: {day: tuple(cols)}} of one panel_ext CSV, or None when absent."""
    if not path.exists() or not path.with_name(path.name + ".meta.json").exists():
        return None
    out = defaultdict(dict)
    with open(path) as fh:
        for r in csv.DictReader(fh):
            vals = []
            for c in cols:
                v = r.get(c)
                vals.append(float(v) if v not in (None, "") else None)
            out[r[key_col].lower()][C.e2.day_of(r["day"][:10])] = tuple(vals)
    return dict(out)


class Inputs:
    """Every source the stream of one group reads, optionally cut at a horizon day h."""

    def __init__(self, group, tokens=None, assets=None, panel=None, corpus=None, pegs=None,
                 panel_ext=PANEL_EXT, horizon=None, prices=None):
        g = self.g = group
        self.h = horizon
        self.tokens = tokens or C.Tokens()
        self.assets = assets or C.CorpusAssets()
        self.panel = panel or C.PanelDaily()
        self.pegs = pegs or PegTable()
        corpus = corpus or Corpus()
        self.corpus = corpus.cut(horizon) if horizon is not None else corpus
        self.mk = C.Markets(g.protocol, g.chain_id)
        if horizon is not None and g.protocol == "euler_v2":
            end = (horizon + 1) * C.DAY
            self.mk.ltv_hist = defaultdict(list, {v: [x for x in xs if x[0] < end]
                                                  for v, xs in self.mk.ltv_hist.items()})
            self.mk.fee_hist = {v: [x for x in xs if x[0] < end] for v, xs in self.mk.fee_hist.items()}
        self.dist = None
        if g.protocol == "euler_v2":
            dist = C.euler_debt_shares(g.chain_id)
            if horizon is not None:
                dist = {v: {d: x for d, x in days.items() if d <= horizon} for v, days in dist.items()}
            self.dist = dist
        self.loan_markets = sorted(m for m, x in self.mk.info.items()
                                   if (x.get("loan") or "").lower() == g.loan_token)
        self._series = {}
        self._maxsup = {}
        self.bad = defaultdict(list)
        for ts, m, x in _bad_debt_rows(g.protocol, g.chain_id):
            if horizon is None or ts < (horizon + 1) * C.DAY:
                self.bad[m].append((ts, x))
        self.present, self.absent = [], []
        ext = pathlib.Path(panel_ext)
        specs = {"borrowers": (ext / "borrowers" / g.protocol / f"{g.chain_id}.csv", "market",
                               ("top1_share", "n_borrowers")),
                 "token_supply": (ext / "token_supply" / f"{g.chain_id}.csv", "token", ("total_supply",)),
                 "public_allocator": (ext / "public_allocator" / g.protocol / f"{g.chain_id}.csv", "market",
                                      ("assets_in", "assets_out", "n_tx"))}
        self.ext = {}
        for name, (path, key, cols) in specs.items():
            tab = _read_ext(path, key, cols)
            if tab is None:
                self.absent.append(str(path.relative_to(C.ROOT)) if path.is_relative_to(C.ROOT) else str(path))
                continue
            self.present.append(str(path.relative_to(C.ROOT)) if path.is_relative_to(C.ROOT) else str(path))
            if horizon is not None:
                tab = {k: {d: v for d, v in days.items() if d <= horizon} for k, days in tab.items()}
            self.ext[name] = tab
        self._fees = {}
        self.prices = prices
        self._prices_cut = None


    def _day(self, d):
        if self.h is not None and d > self.h:
            raise LookaheadError(f"{self.g.key}: read of day {C.day_iso(d)} after the horizon {C.day_iso(self.h)}")
        return d

    def _ts(self, ts):
        if self.h is not None and ts >= (self.h + 1) * C.DAY:
            raise LookaheadError(f"{self.g.key}: read at {C.ts_iso(ts)} after the horizon {C.day_iso(self.h)}")
        return ts


    def need_prices(self, tokens):
        """Load USD prices for the loan token and `tokens` (cut at the horizon)."""
        wanted = {(self.g.chain_id, self.g.loan_token)} | {(self.g.chain_id, t) for t in tokens}
        for t in list(tokens):
            _u, ref = self.pegs.peg(self.g.chain_id, t)
            if ref:
                wanted.add((self.g.chain_id, ref))
        if self.prices is None or not wanted <= set(self._wanted):
            self.prices = C.Prices(self.tokens, wanted)
            self._wanted = wanted
            if self.h is not None:
                for k in list(self.prices.series):
                    self.prices.series[k] = {d: v for d, v in self.prices.series[k].items() if d <= self.h}
                self.prices.days = {k: sorted(v) for k, v in self.prices.series.items()}
    _wanted = ()

    def price_raw(self, token, d):
        """Quoted USD price (the token's own quote, carried up to 3 days, or its bridged twin's on Ethereum); no peg fallback."""
        self._day(d)
        px = self.prices.raw(self.g.chain_id, token, d)
        if px is None:
            tw = self.prices.twin.get((self.g.chain_id, token))
            if tw is not None:
                px = self.prices.raw(*tw, d)
        return px

    def unit_price(self, unit, d):
        self._day(d)
        if unit == "USD":
            return 1.0
        return self.prices.raw(*(self.prices.WETH if unit == "ETH" else self.prices.WBTC), d)

    def usd_per_unit(self, d):
        """USD per raw loan unit at day d (the loan's peg as a fallback)."""
        self._day(d)
        px = self.prices.usd(self.g.chain_id, self.g.loan_token, d)
        dec = self.tokens.decimals(self.g.chain_id, self.g.loan_token) or self.g.loan_decimals
        if px is None or dec is None:
            return None
        return px / 10 ** dec

    def series(self, m):
        if m not in self._series:
            ser = self.panel.get(self.g.protocol, self.g.chain_id, m)
            if ser is not None and self.h is not None:
                ser = C.DailySeries(ser.proto, [r for r in ser.rows if r[0] <= self.h])
            self._series[m] = ser
        return self._series[m]

    def max_supply(self, m):
        """Largest day-end supply of a market (raw units), within the horizon."""
        if m not in self._maxsup:
            ser = self.series(m)
            self._maxsup[m] = max((float(r[1]) for r in ser.rows), default=0.0) if ser else 0.0
        return self._maxsup[m]

    def state(self, m, d):
        """(supply, borrow, apr) at the end of day d (carried forward), or None."""
        self._day(d)
        ser = self.series(m)
        r = ser.at(d) if ser else None
        return None if r is None else (float(r[1]), float(r[2]), r[4] or 0.0)

    def table(self, d):
        """Markets of the loan token in the table of a decision at 00:00 UTC of day d."""
        self._day(d - 1)
        upu = self.usd_per_unit(d - PRICE_LAG_D) or self.usd_per_unit(d - 1 - PRICE_LAG_D)
        if not upu:
            return []
        out = []
        for m in self.loan_markets:
            if self.max_supply(m) * upu < TABLE_MIN_USD:
                continue
            st = self.state(m, d - 1)
            if st is not None and st[0] * upu >= TABLE_MIN_USD and not self.series(m).stuck(d - 1):
                out.append(m)
        return out

    def fee(self, m, ts):
        """Share of borrower interest that does not reach lenders, in force before ts."""
        self._ts(ts - 1)
        p = self.g.protocol
        if p in ("morpho", "moolah"):
            if m not in self._fees:
                self._fees[m] = fee_history(p, self.g.chain_id, m)
            f = 0.0
            for t, v in self._fees[m]:
                if t < ts:
                    f = v
            return f
        return self.mk.interest_fee(m, ts - 1) or 0.0

    def collaterals(self, m, ts):
        """[(token, collateral vault, lltv)] in force strictly before ts."""
        self._ts(ts - 1)
        return self.mk.collaterals(m, ts - 1)

    def families(self, m, ts):
        self._ts(ts - 1)
        return C.market_families(self.mk, self.assets, self.tokens, m, ts, self.dist)

    def bad_debt(self, m, d):
        self._day(d)
        lo, hi = d * C.DAY, (d + 1) * C.DAY
        return [x for t, x in self.bad.get(m, []) if lo <= t < hi]

    def ext_row(self, name, key, d):
        self._day(d)
        tab = self.ext.get(name)
        if tab is None:
            return None
        return (tab.get(key.lower()) or {}).get(d)

    def docs(self, key, t0, t1):
        self._ts(t1 - 1)
        return self.corpus.arrived(key, t0, t1)


def market_facts(inp, m, label, ts):
    """Static facts of a market at ts (module docstring)."""
    g = inp.g
    cols = []
    for tok, cv, lt in sorted(inp.collaterals(m, ts), key=lambda x: (x[1] or "", x[0] or "")):
        if not tok or tok == C.ZERO:
            continue
        orc = inp.mk.oracle(m, cv) or {}
        unit, ref = inp.pegs.peg(g.chain_id, tok)
        peg = None
        if unit:
            peg = {"unit": unit}
            if ref != tok:
                peg["via"] = inp.tokens.symbol(g.chain_id, ref)
        key = C.family_key(inp.assets, inp.tokens, g.chain_id, tok)
        cols.append({"token": tok, "symbol": inp.tokens.symbol(g.chain_id, tok), "key": key,
                     "issuer": issuer_of(inp.assets, key), "type": inp.pegs.asset_type(g.chain_id, tok),
                     "vault": cv, "lltv": r4(lt),
                     "oracle": {"type": orc.get("type"), "subtype": orc.get("subtype"),
                                "blind_to": orc.get("blind_to")},
                     "peg": peg})
    return {"label": label, "market": m, "loan": g.loan_symbol, "collaterals": cols}


class _Daily:
    """Day-end utilization of one market from `lo` (at least UTIL_MIN_D + 1 days before the stream's first day) to `hi`, for the trailing minimum."""

    def __init__(self, inp, m, lo, hi):
        self.lo = lo
        self.u = []
        for d in range(lo, hi + 1):
            st = inp.state(m, d)
            self.u.append(None if st is None else (min(st[1] / st[0], 10.0) if st[0] > 0 else 0.0))

    def util(self, d):
        return self.u[d - self.lo]

    def umin(self, d):
        xs = [x for x in self.u[d - UTIL_MIN_D + 1 - self.lo:d - self.lo + 1] if x is not None]
        return min(xs) if xs else None


def build_group(group, inp=None, d_first=None, d_last=None):
    """Records of one group, day by day (module docstring)."""
    inp = inp or Inputs(group)
    g = group
    d_first = g.first_day if d_first is None else d_first
    d_last = g.last_day if d_last is None else d_last
    if inp.h is not None:
        d_last = min(d_last, inp.h + 1)
    table_by_day = {}
    inp.need_prices([])
    for d in range(d_first, d_last + 1):
        table_by_day[d] = inp.table(d)
    universe = sorted({m for v in table_by_day.values() for m in v})
    toks = set()
    for m in universe:
        for tok, _cv in (inp.mk.info.get(m) or {}).get("collaterals") or []:
            if tok and tok != C.ZERO:
                toks.add(tok)
    inp.need_prices(sorted(toks))
    lo = d_first - UTIL_MIN_D - 1
    daily = {}
    labels, facts_now = {}, {}
    peg_state = {}
    records = []
    text = mechanics_text(g.protocol)

    def peg_off(tok, unit, ref, day):
        """(below the peg, price in peg units), (None, None) without a quote."""
        px, ux = inp.price_raw(ref, day), inp.unit_price(unit, day)
        if px is None or not ux:
            return None, None
        v = px / ux
        return v <= 1.0 - PEG_OFF, v

    for d in range(d_first, d_last + 1):
        ts = d * C.DAY
        table = table_by_day[d]
        for m in sorted(m for m in table if m not in labels):
            labels[m] = f"M{len(labels) + 1:03d}"
        rec = {"group": g.key, "d": d, "day": C.day_iso(d), "ts": ts, "data_through": C.ts_iso(ts - 1)}
        if not records:
            rec["static"] = {"protocol": g.protocol, "protocol_name": PROTOCOL_NAMES[g.protocol],
                             "chain_id": g.chain_id, "chain": CHAIN_NAMES.get(g.chain_id, str(g.chain_id)),
                             "loan_token": g.loan_token, "loan_symbol": g.loan_symbol,
                             "mechanics": text, "mechanics_sha256": hashlib.sha256(text.encode()).hexdigest()}
        new_facts = []
        for m, lab in sorted(labels.items(), key=lambda x: x[1]):
            if m not in facts_now or g.protocol == "euler_v2":
                f = market_facts(inp, m, lab, ts)
                if facts_now.get(m) != f:
                    facts_now[m] = f
                    new_facts.append(f)
        if new_facts:
            rec["markets"] = new_facts
        rec["table"] = sorted(labels[m] for m in table)
        in_table = set(rec["table"])
        upu = inp.usd_per_unit(d - PRICE_LAG_D) or inp.usd_per_unit(d - 1 - PRICE_LAG_D)
        snap, events = {}, []
        coll_keys = {}
        for m, lab in sorted(labels.items(), key=lambda x: x[1]):
            if m not in daily:
                daily[m] = _Daily(inp, m, lo, d_last - 1)
            for c in facts_now[m]["collaterals"]:
                coll_keys.setdefault(c["key"], {}).setdefault(c["token"], c)
            s1, s0 = inp.state(m, d - 1), inp.state(m, d - 2)
            if s1 is None:
                continue
            if s1[0] <= 0 and lab not in in_table:
                continue
            dy = daily[m]
            u1, u0 = dy.util(d - 1), dy.util(d - 2)
            fee = inp.fee(m, ts)
            idle1 = max(s1[0] - s1[1], 0.0)
            shares = None
            if g.protocol == "euler_v2":
                fams = inp.families(m, ts)
                if len(fams) > 1:
                    shares = {cv: r4(w) for _f, w, _t, cv, _lt in sorted(fams, key=lambda x: x[3] or "")}
            br = inp.ext_row("borrowers", m, d - 1)
            snap[lab] = [usd0(s1[0] * upu) if upu else None, usd0(s1[1] * upu) if upu else None, r4(u1),
                         r4(dy.umin(d - 1)), inp.series(m).pinned_days(d - 1), usd0(idle1 * upu) if upu else None,
                         r6(s1[2]),
                         r6((math.exp(min(s1[2], 50.0)) - 1.0) * min(u1 or 0.0, 1.0) * (1.0 - fee)), r4(fee), shares,
                         r4(br[0]) if br and br[0] is not None else None,
                         int(br[1]) if br and br[1] is not None else None]
            material = upu is not None and (s1[0] * upu >= MARKET_MIN_USD
                                            or (s0 is not None and s0[0] * upu >= MARKET_MIN_USD))
            if s0 is not None and material and u0 is not None and u1 is not None:
                if abs(u1 - u0) >= UTIL_MOVE:
                    events.append({"t": "util_move", "m": lab, "a": r4(u0), "b": r4(u1)})
                if (u0 < UTIL_PIN) != (u1 < UTIL_PIN):
                    events.append({"t": "util_pin", "m": lab, "a": r4(u0), "b": r4(u1),
                                   "dir": "up" if u1 >= UTIL_PIN else "down"})
                idle0 = max(s0[0] - s0[1], 0.0)
                if idle0 > 0 and idle1 <= (1.0 - IDLE_DROP) * idle0:
                    events.append({"t": "idle_drop", "m": lab, "a": usd0(idle0 * upu), "b": usd0(idle1 * upu)})
                b0 = inp.ext_row("borrowers", m, d - 2)
                if br and b0 and br[0] is not None and b0[0] is not None and abs(br[0] - b0[0]) >= TOP1_MOVE:
                    events.append({"t": "top1_move", "m": lab, "a": r4(b0[0]), "b": r4(br[0])})
            bad = inp.bad_debt(m, d - 1)
            if bad and upu and sum(bad) * upu >= BAD_DEBT_MIN_USD:
                events.append({"t": "bad_debt", "m": lab, "usd": usd0(sum(bad) * upu), "n": len(bad)})
            pa = inp.ext_row("public_allocator", m, d - 1)
            if pa and pa[0] and upu and pa[0] * upu >= ALLOCATOR_MIN_USD:
                events.append({"t": "allocator_in", "m": lab, "usd": usd0(pa[0] * upu),
                               "n": int(pa[2]) if pa[2] is not None else None})
        dp = d - PRICE_LAG_D
        csup = {}
        for key in sorted(coll_keys):
            for tok, c in sorted(coll_keys[key].items()):
                cs = supply_levels(inp, tok, d, dp)
                if cs is not None:
                    csup[tok] = cs
                if c.get("peg"):
                    unit, ref = inp.pegs.peg(g.chain_id, tok)
                    sk = (key, tok)
                    if sk not in peg_state:
                        peg_state[sk] = bool(peg_off(tok, unit, ref, dp - 1)[0])
                    off, v = peg_off(tok, unit, ref, dp)
                    if off is not None:
                        if off and not peg_state[sk]:
                            ev = {"t": "peg_off", "c": key, "tok": c["symbol"], "peg": unit, "px": r4(v),
                                  "date": C.day_iso(dp)}
                            if ref != tok:
                                ev["via"] = inp.tokens.symbol(g.chain_id, ref)
                            events.append(ev)
                        peg_state[sk] = off
                else:
                    p1, p0 = inp.price_raw(tok, dp), inp.price_raw(tok, dp - 1)
                    if p1 is not None and p0 and p1 <= (1.0 - PRICE_DROP) * p0:
                        events.append({"t": "price_drop", "c": key, "tok": c["symbol"], "a": r6(p0), "b": r6(p1),
                                       "date": C.day_iso(dp)})
                s1_, s0_ = inp.ext_row("token_supply", tok, d - 1), inp.ext_row("token_supply", tok, d - 2)
                if s1_ and s0_ and s1_[0] is not None and s0_[0] and abs(s1_[0] / s0_[0] - 1.0) >= SUPPLY_MOVE:
                    dec = inp.tokens.decimals(g.chain_id, tok)
                    sc = 10 ** dec if dec is not None else 1
                    events.append({"t": "supply_move", "c": key, "tok": c["symbol"], "a": r6(s0_[0] / sc),
                                   "b": r6(s1_[0] / sc), "units": "tokens" if dec is not None else "raw"})
            docs = inp.docs(key, ts - C.DAY, ts)
            if docs:
                types = defaultdict(int)
                for x in docs:
                    types[x[3]] += 1
                events.append({"t": "new_docs", "c": key, "n": len(docs), "types": dict(sorted(types.items())),
                               "docs": [{"id": x[1], "title": x[2][:160], "type": x[3], "time": C.ts_iso(x[0])}
                                        for x in docs[-DOCS_PER_EVENT:]]})
        rec["snapshot"] = snap
        if csup:
            rec["collateral_supply"] = csup
        rec["events"] = sorted(events, key=event_key)
        records.append(rec)
    return records


def supply_levels(inp, tok, d, dp):
    """[s1, s7, s30, usd] of a collateral token at day d (module docstring), None without a supply at the end of day d - 1."""
    rows = [inp.ext_row("token_supply", tok, d - k) for k in (1, 8, 31)]
    if not rows[0] or rows[0][0] is None:
        return None
    dec = inp.tokens.decimals(inp.g.chain_id, tok)
    sc = 10 ** dec if dec is not None else 1
    lv = [r6(r[0] / sc) if r and r[0] is not None else None for r in rows]
    px = inp.price_raw(tok, dp) if dec is not None else None
    return lv + [usd0(lv[0] * px) if px is not None else None]


def event_key(e):
    return (EVENT_ORDER.index(e["t"]), e.get("m") or "", e.get("c") or "", e.get("tok") or "")


def path_of(group_key, root=None):
    return pathlib.Path(root or OBS) / f"{group_key}.jsonl.gz"


def dumps(rec):
    return json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def write_group(group, records, inp, root=None):
    path = path_of(group.key, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=9) as fh:
        for r in records:
            fh.write(dumps(r) + "\n")
    os.replace(tmp, path)
    n_ev = sum(1 for r in records if r["events"])
    by_type = defaultdict(int)
    for r in records:
        for e in r["events"]:
            by_type[e["t"]] += 1
    meta = {"generator": GENERATOR, "group": group.key, "protocol": group.protocol, "chain_id": group.chain_id,
            "loan_token": group.loan_token, "records": len(records),
            "first_day": records[0]["day"] if records else None, "last_day": records[-1]["day"] if records else None,
            "universe": len({f["label"] for r in records for f in r.get("markets") or []}),
            "table_median": sorted(len(r["table"]) for r in records)[len(records) // 2] if records else 0,
            "table_max": max((len(r["table"]) for r in records), default=0),
            "days_with_event": n_ev, "events_by_type": dict(sorted(by_type.items())),
            "snapshot_columns": list(SNAP_COLS), "inputs_present": inp.present, "inputs_absent": inp.absent,
            "thresholds": thresholds(), "price_lag_days": PRICE_LAG_D,
            "oracle_facts": oracle_note(group), "oracle_flags": oracle_flags(group, records),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    C.write_sidecar(path, meta)
    return meta


_VALIDATION = None


def oracle_note(group):
    if group.protocol == "euler_v2":
        return ("latest-block classification, not point in time: the Euler router can be reconfigured and the "
                "data hold no router configuration history")
    return "latest-block classification of an oracle fixed at market creation"


def oracle_flags(group, records):
    """(label, market, collateral vault) of the universe's Euler pairs whose historical quotes the registry's validation could."""
    global _VALIDATION
    if group.protocol != "euler_v2":
        return []
    if _VALIDATION is None:
        p = C.DATA / "registry/oracles/validation.json"
        _VALIDATION = json.loads(p.read_text()) if p.exists() else {}
    tgt = ((_VALIDATION.get("targets") or {}).get(f"euler_v2:{group.chain_id}") or {}).get("historical") or {}
    bad = set(tgt.get("unexplained") or []) if isinstance(tgt, dict) else set()
    out = []
    for r in records:
        for f in r.get("markets") or []:
            for c in f["collaterals"]:
                k = f"{f['market']}:{c.get('vault')}"
                if k in bad and [f["label"], f["market"], c.get("vault")] not in out:
                    out.append([f["label"], f["market"], c.get("vault")])
    return out


def thresholds():
    return {"table_min_usd": TABLE_MIN_USD, "util_move": UTIL_MOVE, "util_pin": UTIL_PIN, "idle_drop": IDLE_DROP,
            "bad_debt_min_usd": BAD_DEBT_MIN_USD, "peg_off": PEG_OFF, "price_drop": PRICE_DROP,
            "top1_move": TOP1_MOVE, "supply_move": SUPPLY_MOVE, "allocator_min_usd": ALLOCATOR_MIN_USD,
            "market_min_usd": MARKET_MIN_USD, "util_min_days": UTIL_MIN_D}


def load_records(group_key, root=None):
    with gzip.open(path_of(group_key, root), "rt", encoding="utf-8") as fh:
        return [json.loads(x) for x in fh if x.strip()]


def write_index(metas=None, root=None):
    """data/bench/observations/index.json over every group's sidecar (or `metas`)."""
    root = pathlib.Path(root or OBS)
    if metas is None:
        metas = [json.loads(p.read_text()) for p in sorted(root.glob("*.jsonl.gz.meta.json"))]
    idx = {"generator": GENERATOR, "groups": len(metas), "records": sum(m["records"] for m in metas),
           "days_with_event": sum(m["days_with_event"] for m in metas),
           "inputs_absent": sorted({a for m in metas for a in m["inputs_absent"]}),
           "inputs_present": sorted({a for m in metas for a in m["inputs_present"]}),
           "thresholds": thresholds(),
           "oracle_flags": {m["group"]: m.get("oracle_flags") for m in metas if m.get("oracle_flags")},
           "by_group": {m["group"]: {k: m.get(k) for k in ("records", "days_with_event", "universe", "table_median",
                                                            "table_max", "sha256")}
                        for m in metas}}
    C.write_json(root / "index.json", idx)
    return idx


def pct(x, nd=1):
    return "n/a" if x is None else f"{100 * x:.{nd}f}%"


def money(x):
    if x is None:
        return "n/a"
    x = float(x)
    if abs(x) >= 1e9:
        return f"${x / 1e9:,.2f}B"
    if abs(x) >= 1e6:
        return f"${x / 1e6:,.2f}M"
    if abs(x) >= 1e3:
        return f"${x / 1e3:,.1f}k"
    return f"${x:,.0f}"


def change(a, b):
    """Relative change from b to a, as a signed percentage."""
    if a is None or not b:
        return "n/a"
    return f"{100 * (a / b - 1):+.1f}%"


def num(x):
    if x is None:
        return "n/a"
    x = float(x)
    if abs(x) >= 1e6:
        return f"{x / 1e6:,.2f}M"
    if abs(x) >= 1e3:
        return f"{x / 1e3:,.1f}k"
    return f"{x:,.4g}"


def oracle_text(o):
    o = o or {}
    s = f"{o.get('type')}/{o.get('subtype')}"
    if o.get("blind_to"):
        s += f" (blind to {o['blind_to']})"
    return s


class Stream:
    """The records of one group with a deterministic renderer to prompt text."""

    def __init__(self, records):
        self.records = records
        self.by_d = {r["d"]: r for r in records}
        self.static = records[0]["static"] if records else {}
        self.facts_hist = defaultdict(list)
        self.market_of, self.label_of = {}, {}
        self.first_d = {}
        for r in records:
            for f in r.get("markets") or []:
                self.facts_hist[f["label"]].append((r["d"], f))
                self.market_of[f["label"]] = f["market"]
                self.label_of[f["market"]] = f["label"]
                self.first_d.setdefault(f["label"], r["d"])
        self._mech = None

    def record(self, d):
        return self.by_d.get(d)

    def table(self, d):
        """Market ids of the table of the decision at day d."""
        r = self.by_d.get(d)
        return [self.market_of[x] for x in r["table"]] if r else []

    def facts(self, label, d):
        f = None
        for d_, x in self.facts_hist.get(label, []):
            if d_ <= d:
                f = x
        return f

    def universe(self, d):
        return sorted(lab for lab, d0 in self.first_d.items() if d0 <= d)

    def collateral_keys(self, label, d):
        """Corpus keys of the collaterals in a market's facts, sorted."""
        f = self.facts(label, d)
        return sorted({c["key"] for c in f["collaterals"]}) if f else []

    def mechanics_text(self):
        if self._mech is None:
            s = self.static
            self._mech = "\n".join([
                f"Protocol mechanics ({s.get('protocol_name')}):", s.get("mechanics", ""), "",
                f"This vault lends {s.get('loan_symbol')} ({s.get('loan_token')}) on {s.get('protocol_name')}, "
                f"{s.get('chain')} (chain {s.get('chain_id')})."]) + "\n"
        return self._mech

    def market_text(self, label, d):
        """One line of a market's static facts at day d."""
        f = self.facts(label, d)
        cols = []
        for c in f["collaterals"]:
            cols.append(f"{c['symbol']} [{c['key']}], issuer {c.get('issuer') or 'not recorded'}, "
                        f"{c.get('type') or 'other'}, LLTV {c['lltv'] if c['lltv'] is not None else 'n/a'}, "
                        f"oracle {oracle_text(c['oracle'])}")
        return f"{label} | " + ("; ".join(cols) or "no collateral")

    def facts_text(self, labels, d):
        lines = [f"Markets (label | collateral [corpus key], issuer, asset type, LLTV, oracle type/subtype); "
                 f"each lends {self.static.get('loan_symbol')}:"]
        lines += [self.market_text(lab, d) for lab in sorted(labels)]
        return "\n".join(lines) + "\n"

    def numbers_text(self, d, labels, below=()):
        r = self.by_d[d]
        lines = [f"Markets at the end of {C.day_iso(d - 1)} (label | supply | borrow | utilization | supply APY | "
                 f"idle liquidity | largest borrower's share of debt | borrowers):"]
        for lab in sorted(labels):
            row = r["snapshot"].get(lab)
            note = (f" | out of the table (supply below {money(TABLE_MIN_USD)}, or utilization at 99% or more "
                    f"for 30 days): keep or reduce only") if lab in below else ""
            if row is None:
                lines.append(f"{lab} | no state{note}")
                continue
            x = dict(zip(SNAP_COLS, row))
            nb = x["n_borrowers"]
            lines.append(f"{lab} | {money(x['supply_usd'])} | {money(x['borrow_usd'])} | {pct(x['util'])} | "
                         f"{pct(x['supply_apy'], 2)} | {money(x['idle_usd'])} | {pct(x['top1_share'], 0)} | "
                         f"{'n/a' if nb is None else nb}{note}")
        cs = r.get("collateral_supply") or {}
        rows = {}
        for lab in labels:
            f = self.facts(lab, d)
            for c in (f["collaterals"] if f else []):
                if c["token"] in cs:
                    rows[(c["symbol"], c["key"], c["token"])] = cs[c["token"]]
        if rows:
            lines.append(f"Collateral token supply on this chain at the end of {C.day_iso(d - 1)} "
                         f"(token [corpus key] | supply in tokens | in USD | 7-day change | 30-day change):")
            for (sym, key, _t), (s1, s7, s30, usd) in sorted(rows.items()):
                lines.append(f"{sym} [{key}] | {num(s1)} | {money(usd)} | {change(s1, s7)} | {change(s1, s30)}")
        return "\n".join(lines) + "\n"

    def events(self, d, labels=None, keys=None, days=EVENT_DAYS):
        """[(record day, event)] of the last `days` records up to d about the given markets or collateral keys (all when None)."""
        out = []
        for dd in range(d - days + 1, d + 1):
            r = self.by_d.get(dd)
            if not r:
                continue
            for e in r["events"]:
                if labels is None or e.get("m") in labels or (keys is not None and e.get("c") in keys):
                    out.append((dd, e))
        return out

    def events_text(self, d, labels=None, keys=None):
        """One line per event; the new documents of one day go on one line per document, with every collateral key it is about."""
        evs = self.events(d, labels, keys)
        lines = [f"Events in the last {EVENT_DAYS} days (date of the data | subject | what happened):"]
        docs = {}
        for dd, e in evs:
            if e["t"] != "new_docs":
                lines.append(event_line(dd, e))
                continue
            day = e.get("date") or C.day_iso(dd - 1)
            for x in e["docs"]:
                docs.setdefault((day, x["time"], x["id"]), (x, []))[1].append(e["c"])
            if e["n"] > len(e["docs"]):
                lines.append(f"{day} | [{e['c']}] | {e['n']} new documents, the newest {len(e['docs'])} below")
        for (day, _t, _i), (x, ks) in sorted(docs.items()):
            lines.append(f"{day} | {', '.join(f'[{k}]' for k in sorted(set(ks)))} | new document: "
                         f"\"{x['title']}\" ({x['type']}, {x['time']}, doc {x['id']})")
        if not evs:
            lines.append("none")
        return "\n".join(lines) + "\n"


def event_line(dd, e):
    day = e.get("date") or C.day_iso(dd - 1)
    t = e["t"]
    if t == "util_move":
        return f"{day} | {e['m']} | utilization {pct(e['a'])} to {pct(e['b'])}"
    if t == "util_pin":
        what = "reached 99%" if e["dir"] == "up" else "fell below 99%"
        return f"{day} | {e['m']} | utilization {what} ({pct(e['a'])} to {pct(e['b'])})"
    if t == "idle_drop":
        return f"{day} | {e['m']} | idle liquidity fell from {money(e['a'])} to {money(e['b'])}"
    if t == "bad_debt":
        return f"{day} | {e['m']} | liquidation left bad debt of {money(e['usd'])} ({e['n']} write-offs)"
    if t == "top1_move":
        return f"{day} | {e['m']} | largest borrower's share of debt {pct(e['a'], 0)} to {pct(e['b'], 0)}"
    if t == "allocator_in":
        return f"{day} | {e['m']} | public allocator moved {money(e['usd'])} into the market"
    if t == "peg_off":
        via = f" (read on {e['via']})" if e.get("via") else ""
        return (f"{day} | [{e['c']}] {e['tok']} | market price {e['px']:.4f} {e['peg']}{via}, "
                f"{100 * (1 - e['px']):.1f}% below peg")
    if t == "price_drop":
        return f"{day} | [{e['c']}] {e['tok']} | USD price fell from {num(e['a'])} to {num(e['b'])}"
    if t == "supply_move":
        return f"{day} | [{e['c']}] {e['tok']} | token supply {num(e['a'])} to {num(e['b'])}"
    if t == "new_docs":
        docs = "; ".join(f"\"{x['title']}\" ({x['type']}, {x['time']}, doc {x['id']})" for x in e["docs"])
        return f"{day} | [{e['c']}] | {e['n']} new document(s): {docs}"
    return f"{day} | {json.dumps(e, sort_keys=True)}"


def load_stream(group, root=None, build=True):
    """The group's Stream from data/bench/observations, else built in memory."""
    p = path_of(group.key, root)
    if p.exists():
        return Stream(load_records(group.key, root))
    if not build:
        raise FileNotFoundError(p)
    return Stream(build_group(group))


if __name__ == "__main__":
    print(__doc__)
