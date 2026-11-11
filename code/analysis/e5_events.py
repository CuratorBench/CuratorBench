# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "matplotlib>=3.8"]
# ///
"""E5: curators' reactions and outcomes in the E2 stress events."""
import argparse
import bisect
import csv
import datetime as dt
import json
import math
import os
import pathlib
import random
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor

sys.dont_write_bytecode = True
ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "code" / "analysis"))
import e2_events as e2
from common import inputs, io_utils, outroot, rpc

GENERATOR = "code/analysis/e5_events.py"
DATA = ROOT / "data"
DERIVED = outroot.OUT_DATA
OUT = "metrics/e5"
INPUTS = inputs.INPUTS
RECOVERY_FILE = INPUTS / "recovery_prices.json"
NOVEL_FILE = INPUTS / "event_sizing.json"
PT_FILE = INPUTS / "pt_markets.json"
CASES = ("stream", "usr", "msusd", "usd0pp", "rseth")
RECOVERY_DEFILLAMA = {"USDX": "a single pool with 0.12 USD of depth"}
RECOVERY_SAME_TOKEN = {
    (9745, "0x7884a8457f0e63e82c89a87fe48e8ba8223db069"): (
        (1, "0x5c5b196abe0d54485975d1ec29617d42d9198326"),
        "Plasma deployment of Elixir's sdeUSD (same symbol and issuer; no Plasma quote)"),
}
RECOVERY_LAST_CLOSE_D = 30
PT_SYMBOL = re.compile(r"^PT-(.+)-(\d{1,2}[A-Za-z]{3}\d{4})(?:-\(.*\))?$")
DAY = e2.DAY
YEAR = e2.YEAR
ZERO = e2.ZERO
LOOKBACK_D = 180
PATH_BEFORE_D, PATH_AFTER_D = 90, 30
EXIT_FRACTION = 0.10
LEAD_FRACTION = 0.50
MATERIAL_USD = 10_000.0
PER_DOLLAR_MIN_USD = MATERIAL_USD
L_LOCK, L_SENS = e2.L_LOCK, e2.L_SENS
ACTION_TYPES = ("realloc_out", "cap_reduction", "queue_removal")
ROLE_NAMES = {"owner", "curator", "allocator", "sentinel", "guardian", "MANAGER", "CURATOR",
              "ALLOCATOR", "BOT", "DEFAULT_ADMIN_ROLE"}
FIG5_EVENTS = 6
FIG5_LABELS = 8
SEED = 20261008


def utc(ts):
    return e2.utc(ts)


def day_str(d):
    return e2.day_str(d)


def jsonl(path):
    return e2.jsonl(path)


def fnum(x, nd=None):
    """CSV cell for a number."""
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return ""
    if nd is not None and isinstance(x, float):
        return round(x, nd)
    return x


_EVENTS = []


def load_events():
    """(events used, events excluded by E2)."""
    if not _EVENTS:
        evs = jsonl(DERIVED / "events/events.jsonl")
        used = [e for e in evs if not e.get("excluded")]
        tokens = load_tokens()
        fams = e2.Families(tokens)
        e2.census_pegs(tokens, fams)
        pstate = e2.PanelState()
        lctx = type("LossCtx", (), {})()
        lctx.tokens, lctx.fams, lctx.pstate = tokens, fams, pstate
        lctx.prices = e2.Prices(tokens, set(tokens.rows))
        rec, raw = Recovery(lctx), RawPanel()
        for e in used:
            if e["type"] == "depeg_loss":
                e["affected_markets"] = e["affected_markets"] + family_backed_markets(
                    e, fams, pstate, lctx, raw, rec)
        _EVENTS.extend([used, [e for e in evs if e.get("excluded")]])
    return _EVENTS[0], _EVENTS[1]


def family_backed_markets(e, fams, pstate, lctx, raw, rec):
    """Item 21 (2026-10-18)."""
    fam_ids = event_families(e)
    t0 = e5_onset(e)
    t0d = t0 // DAY
    have = {(m["protocol"], m["chain_id"], m["market"]) for m in e["affected_markets"]}
    out = []
    for proto, cid in e2.PAIRS:
        info = e2.market_info_cached(proto, cid)
        for m, r in sorted(info.items()):
            if (proto, cid, m) in have or not r.get("loan"):
                continue
            if fams.resolve(cid, r["loan"])["family_id"] in fam_ids:
                continue
            coll = [t for t, _cv in r.get("collaterals") or [] if t]
            hit = [t for t in coll if fams.resolve(cid, t)["family_id"] in fam_ids]
            if not hit:
                continue
            st = pstate.at(proto, cid, m, t0)
            if not st or not st["borrow"]:
                continue
            share = None
            if proto == "euler_v2":
                share = e2.family_debt_share(cid, m, t0d, fam_ids, fams)
                if not share:
                    continue
            gref = e2.guard_reference(proto, cid, m, t0, pstate, None)
            ref = gref[0] if gref else t0
            am = {"protocol": proto, "chain_id": cid, "market": m,
                  "collateral_token": hit[0], "loan_token": r["loan"],
                  "family_debt_share_at_t0": share, "first_trigger_ts": ref,
                  "first_trigger_utc": utc(ref), "hit_ids": [], "hit_kinds": [],
                  "source": "family_backed"}
            sf = market_shortfall(lctx, raw, rec, e, am)
            ps, bd = sf.get("principal_shortfall_usd") or 0.0, sf.get("bad_debt_usd") or 0.0
            if ps + bd >= MATERIAL_USD:
                am.update({"family_backed_shortfall_usd": ps, "family_backed_bad_debt_usd": bd})
                out.append(am)
    return out


def load_tokens():
    """E2's token table rebuilt from its published inputs."""
    rows = {}
    for r in jsonl(DATA / "prices/tokens.jsonl"):
        dec = r["decimals"]
        if r.get("defillama_decimals") is not None and r["defillama_decimals"] != dec:
            dec = r["defillama_decimals"]
        rows[(r["chain_id"], r["token"])] = {
            "symbol": r["symbol"], "decimals": dec, "peg_family": r["peg_family"],
            "reference": r["reference"], "peg_rule": r["peg_rule"],
            "priced_days": r["priced_days"]}
    for r in jsonl(DERIVED / "events/tokens_supplement.jsonl"):
        rows.setdefault((r["chain_id"], r["token"]), {
            k: r.get(k) for k in ("symbol", "decimals", "peg_family", "reference",
                                  "peg_rule", "priced_days")})
    return e2.Tokens(rows)


class Ctx:
    """Inputs every later stage shares, loaded once."""

    def __init__(self, work):
        self.work = work
        self.events, self.excluded = load_events()
        self.tokens = load_tokens()
        self.fams = e2.Families(self.tokens)
        e2.census_pegs(self.tokens, self.fams)
        check_families_against_e2(self.fams)
        self.prices = e2.Prices(self.tokens, set(self.tokens.rows))
        self.heads = work.load("heads.pkl")["heads"]
        self.curators, self.cur_meta = e2.load_curators()
        self.names = {r["entity_id"]: r["display_name"]
                      for r in jsonl(DATA / "registry/curators.jsonl")}
        self.pstate = e2.PanelState()
        self._series = {}

    def series(self, proto, cid):
        key = (proto, cid)
        if key not in self._series:
            self._series[key] = self.work.load(f"series/{proto}_{cid}.pkl")
        return self._series[key]


def check_families_against_e2(fams):
    """Fix 2026-10-16 (one family definition)."""
    bad = []
    for r in jsonl(DERIVED / "events/collateral_families.jsonl"):
        for m in r["members"]:
            got = fams.resolve(m["chain_id"], m["token"])["family_id"]
            if got != r["family_id"]:
                bad.append((m["chain_id"], m["token"], r["family_id"], got))
    if bad:
        raise SystemExit(f"E5 families differ from E2's collateral_families.jsonl: {bad[:5]}")


def event_families(e):
    """Fix 2026-10-16: the event's collateral families as E2 recorded them (events.jsonl "families"."""
    return frozenset(e.get("families") or [e["collateral_family"]["family_id"]])


def e5_onset(e):
    """E5's t0: E2's onset, which for a depeg-loss event is already the onset of the episode after which the collateral did not."""
    return e["onset_ts"]


def event_pairs(e):
    """{(protocol, chain): sorted affected markets} of one event."""
    out = defaultdict(set)
    for m in e["affected_markets"]:
        out[(m["protocol"], m["chain_id"])].add(m["market"])
    return {k: sorted(v) for k, v in out.items()}


def stage_heads(work):
    """Panel head of every pair (E2's panel_head) and its block timestamp."""
    heads = {}
    n0 = rpc.calls_made()
    for proto, cid in e2.PAIRS:
        blk = e2.panel_head(proto, cid)
        b = rpc.get_block(e2.CHAINS[str(cid)]["drpc"], blk)
        heads[f"{proto}:{cid}"] = {"block": blk, "ts": int(b["timestamp"], 16)}
    calls = rpc.calls_made() - n0
    work.save("heads.pkl", {"heads": heads, "rpc_calls": calls})
    return {"pairs": len(heads), "rpc_calls": calls}


def _holder_scan(job):
    """Affected markets whose id occurs in one vault's position file."""
    vault, path, needles = job
    data = pathlib.Path(path).read_bytes()
    return vault, [n.decode() for n in needles if n in data]


def _market_daily(job):
    """End-of-day state of one market."""
    proto, market, path = job
    daily, last = [], None
    for block, li, ts, supply, borrow, shares, _apr, _ev in e2.panel_rows(proto, path):
        if last is not None and last[2] // DAY != ts // DAY:
            daily.append((last[2] // DAY, last[3], last[4], last[5], last[0], last[2]))
        last = (block, li, ts, supply, borrow, shares)
    if last is not None:
        daily.append((last[2] // DAY, last[3], last[4], last[5], last[0], last[2]))
    return market, daily


def stage_series(work, workers):
    """For each pair with affected markets."""
    heads = work.load("heads.pkl")["heads"]
    events, _ = load_events()
    aff = defaultdict(set)
    for e in events:
        for pair, ms in event_pairs(e).items():
            aff[pair] |= set(ms)
    stats = {}
    for (proto, cid), ms in sorted(aff.items()):
        t1 = time.time()
        files = e2.position_files(proto, cid)
        needles = [m.encode() for m in sorted(ms)]
        with ProcessPoolExecutor(workers) as ex:
            hits = dict(ex.map(_holder_scan, [(v, str(p), needles) for v, p in files.items()],
                               chunksize=16))
        holders = sorted(v for v, h in hits.items() if h)
        with ProcessPoolExecutor(workers) as ex:
            held = dict(ex.map(e2._index_file, [(proto, v, files[v]) for v in holders],
                               chunksize=4))
        holders_by_market = {m: {v for v in holders if m in held[v]} for m in ms}
        holders = sorted(v for v in holders if held[v] & ms)
        needed = set(ms) | {m for v in holders for m in held[v]}
        panel = e2.panel_files(proto, cid)
        with ProcessPoolExecutor(workers) as ex:
            daily = dict(ex.map(_market_daily, [(proto, m, panel[m]) for m in sorted(needed)
                                                if m in panel], chunksize=4))
        anchors_all = defaultdict(list)
        if proto in ("silo_v2", "euler_v2"):
            for v in holders:
                for m, a in e2.read_positions(proto, files[v])[1].items():
                    anchors_all[m].extend(a)
        head_day = heads[f"{proto}:{cid}"]["ts"] // DAY
        mdays = {}
        for m, dl in daily.items():
            if dl:
                fac = e2.silo_factors(dl, anchors_all.get(m)) if proto == "silo_v2" else None
                mdays[m] = e2.MarketDays(proto, dl, dl[0][0], head_day, fac)
        queues = e2.queue_timelines(proto, cid, set(holders))
        work.save(f"vaultctx/{proto}_{cid}.pkl", {"mdays": mdays, "anchors": dict(anchors_all),
                                                  "head_day": head_day, "keep": set(ms),
                                                  "queues": queues})
        series = {}
        if holders:
            jobs = [(proto, cid, v, files[v]) for v in holders]
            with ProcessPoolExecutor(min(workers, len(jobs)), initializer=e2._init_vault_worker,
                                     initargs=(str(work.root), proto, cid)) as ex:
                for v, sres in ex.map(e2._vault_job, jobs, chunksize=2):
                    if sres:
                        series[v] = sres
        reg = e2.vault_registry(proto, cid)
        for v, sres in series.items():
            sres.update(reg.get(v, {"asset": None, "kind": None, "name": None}))
        if proto == "euler_v2":
            gov = e2.governor_timeline(cid)
            info = e2.market_info(proto, cid)
            for m in ms:
                md = mdays.get(m)
                if md is None:
                    continue
                n = len(md.supply)
                series[f"self:{m}"] = {
                    "vault": m, "d0": md.d0, "total": list(md.supply),
                    "wd": [min(md.avail[i], md.supply[i]) for i in range(n)],
                    "pos": {m: list(md.supply)}, "rows": {}, "outside_panel_share": 0.0,
                    "asset": info[m]["loan"], "kind": "euler_lending", "name": None,
                    "governors": gov.get(m, [])}
        work.save(f"series/{proto}_{cid}.pkl", {
            "series": series, "mdays": {m: mdays[m] for m in ms if m in mdays},
            "holders_by_market": holders_by_market, "head_day": head_day})
        stats[f"{proto}:{cid}"] = {"affected_markets": len(ms), "holders": len(holders),
                                   "markets_read": len(daily), "series": len(series),
                                   "seconds": round(time.time() - t1, 1)}
        print(f"series {proto}:{cid} {stats[f'{proto}:{cid}']}", flush=True)
    return stats


class RawPanel:
    """Panel CSV rows as dicts, for columns panel_rows does not yield (Morpho total_collateral, Euler interestAccumulator, Silo protected assets), cached per market."""

    def __init__(self):
        self.cache = {}

    def rows(self, proto, cid, market):
        key = (proto, cid, market)
        if key not in self.cache:
            p = e2.panel_dir(proto, cid) / f"{market}.csv"
            rows = []
            if p.exists():
                tcol = "ts" if proto == "silo_v2" else "timestamp"
                with open(p, newline="") as fh:
                    for r in csv.DictReader(fh):
                        r["_ts"] = int(r[tcol])
                        rows.append(r)
            self.cache[key] = ([r["_ts"] for r in rows], rows)
        return self.cache[key]

    def at(self, proto, cid, market, ts):
        tss, rows = self.rows(proto, cid, market)
        i = bisect.bisect_right(tss, ts) - 1
        return rows[i] if i >= 0 else None


class Recovery:
    """Recovery price of a collateral token at the valuation time T, in USD per whole token."""

    def __init__(self, ctx):
        self.ctx = ctx
        d = json.loads(RECOVERY_FILE.read_text())
        self.val_ts = int(d["valuation"]["ts"])
        self.val_utc = d["valuation"]["utc"]
        self.tday = self.val_ts // DAY
        self.table = {}
        rel = inputs.rel(RECOVERY_FILE)
        for key, v in d["tokens"].items():
            c = v.get("chosen") or {}
            if c.get("price") is None:
                continue
            unit = "ETH" if v.get("case") == "rseth" else "USD"
            depth = c.get("depth_usd")
            for chain, addr in v.get("deployments") or []:
                if str(chain).isdigit():
                    self.table[(int(chain), addr.lower())] = {
                        "price": float(c["price"]), "unit": unit, "symbol": v.get("symbol"),
                        "source": f"{rel} tokens['{key}'].chosen ({c.get('source')}, tier "
                                  f"{c.get('tier')}, depth_usd {depth})"}
        n = json.loads(NOVEL_FILE.read_text())
        rel = inputs.rel(NOVEL_FILE)
        for key, v in n["recovery_prices"].items():
            cid, addr = key.split(":")
            if v.get("price") is not None:
                self.table.setdefault((int(cid), addr.lower()), {
                    "price": float(v["price"]), "unit": "USD", "symbol": None,
                    "source": f"{rel} recovery_prices['{key}'] ({v.get('source')}, tier "
                              f"{v.get('tier')}, depth_usd {v.get('depth_usd')})"})
        for (cid, addr), v in list(self.table.items()):
            sym = (ctx.tokens.symbol(cid, addr) or v.get("symbol") or "").upper()
            if sym in RECOVERY_DEFILLAMA:
                px = ctx.prices.raw(cid, addr, self.tday)
                if px is not None:
                    self.table[(cid, addr)] = {
                        "price": float(px), "unit": "USD", "symbol": sym,
                        "source": f"DefiLlama close on {day_str(self.tday)}; the input table's "
                                  f"{v['price']} USD rests on {RECOVERY_DEFILLAMA[sym]} "
                                  f"({v['source']})"}
        pt = json.loads(PT_FILE.read_text())
        rel = inputs.rel(PT_FILE)
        self.pt = {}
        for x in pt["pt"]["markets"]:
            val = (x.get("blocks") or {}).get("val") or {}
            p = val.get("pt_value_usd_per_pt")
            cid = int(x["chain_id"])
            if isinstance(p, dict):
                for cv, price in p.items():
                    if price is None:
                        continue
                    self.pt[(cid, cv.lower())] = {"price": float(price), "unit": "USD",
                                                  "source": f"{rel} pt.markets uid {x['uid']} "
                                                            "val.pt_value_usd_per_pt"}
            elif p is not None and x.get("pt"):
                self.pt[(cid, x["pt"].lower())] = {"price": float(p), "unit": "USD",
                                                   "source": f"{rel} pt.markets uid {x['uid']} "
                                                             "val.pt_value_usd_per_pt"}
        self.used = {}
        self.rpc_reads = 0
        self.rpc_errors = []

    def ref(self, cid, token):
        """The input tables' own recovery price of a token, or None."""
        key = (int(cid), token.lower())
        r = self.table.get(key) or self.pt.get(key)
        return self._usd(cid, token, r) if r else None

    def usd(self, cid, token, cvault=None, since_day=None):
        """{price_usd, unit_price, unit, source} or None."""
        key = (int(cid), token.lower())
        r = self.table.get(key) or (self.pt.get((int(cid), cvault)) if cvault else None) \
            or self.pt.get(key)
        if r is None:
            r = self._derived(cid, token)
        if r is None:
            r = self._fallback(cid, token, since_day)
        return self._usd(cid, token, r) if r else None

    def _fallback(self, cid, token, since_day, depth=0):
        """Prices for collateral that none of the sources above prices at T, tried in order (they never replace a price found above)."""
        cid = int(cid)
        token = token.lower()
        same = RECOVERY_SAME_TOKEN.get((cid, token))
        if same:
            (scid, stok), why = same
            r = self.usd(scid, stok, since_day=since_day)
            if r:
                return {"price": r["unit_price"], "unit": r["unit"],
                        "source": f"{why}: recovery price of {scid}:{stok} ({r['source']})"}
        sym = self.ctx.tokens.symbol(cid, token) or ""
        mt = PT_SYMBOL.match(sym)
        if mt and depth == 0:
            try:
                maturity = e2.day_of(dt.datetime.strptime(mt.group(2).upper(), "%d%b%Y")
                                     .date().isoformat())
            except ValueError:
                maturity = None
            if maturity is not None and maturity < self.tday:
                fam = self.ctx.fams.resolve(cid, token)["family_id"]
                for (c, t) in sorted(self.ctx.tokens.rows):
                    if c != cid or t == token or \
                            (self.ctx.tokens.symbol(c, t) or "").upper() != mt.group(1).upper():
                        continue
                    if fam and self.ctx.fams.resolve(c, t)["family_id"] != fam:
                        continue
                    key = (c, t)
                    r = self.table.get(key) or self.pt.get(key) or self._derived(c, t) \
                        or self._fallback(c, t, since_day, depth + 1)
                    if r:
                        return {"price": r["price"], "unit": r["unit"],
                                "source": f"{sym} matured on {day_str(maturity)}: redeems for "
                                          f"{mt.group(1)} {t} ({r['source']})"}
        if since_day is not None:
            for d in range(self.tday, max(since_day, self.tday - RECOVERY_LAST_CLOSE_D) - 1, -1):
                px = self.ctx.prices.series.get((cid, token), {}).get(d)
                if px is not None:
                    return {"price": px, "unit": "USD",
                            "source": f"last DefiLlama close at or before T, on {day_str(d)} "
                                      "(after the market's trigger; no close at T)"}
        return None

    def _usd(self, cid, token, r):
        px = r["price"]
        if r["unit"] == "ETH":
            weth = self.ctx.prices.raw(*e2.Prices.WETH, self.tday)
            px = px * weth if weth else None
        out = {"price_usd": px, "unit_price": r["price"], "unit": r["unit"], "source": r["source"]}
        self.used[f"{cid}:{token}"] = out
        return out

    def _derived(self, cid, token):
        ctx = self.ctx
        reg = e2.registry_vaults().get((int(cid), token))
        if reg:
            under = self.table.get((int(cid), reg["asset"]))
            sp = e3_share_price(ctx, reg["protocol"], cid, token, self.tday)
            if under and sp:
                return {"price": under["price"] * sp, "unit": under["unit"],
                        "source": f"vault share {token}: E3 total assets / share supply on "
                                  f"{day_str(self.tday)} ({sp:.6f}) x recovery price of its "
                                  f"asset ({under['source']})"}
        term, tsym = ctx.fams.terminal(cid, token)
        if term and (int(cid), term) in self.table:
            under = self.table[(int(cid), term)]
            rate = self._rate(cid, token, term)
            if rate:
                return {"price": under["price"] * rate[0], "unit": under["unit"],
                        "source": f"wrapper of {tsym} {term}: recovery price of the "
                                  f"underlying ({under['source']}) x {rate[1]}"}
        px = ctx.prices.raw(cid, token, self.tday)
        if px is not None:
            return {"price": px, "unit": "USD",
                    "source": f"DefiLlama close on {day_str(self.tday)} (no input-table price)"}
        return None

    def _rate(self, cid, token, term):
        """(underlying units per wrapper unit, how)."""
        from common import evmabi
        blocks = json.loads(PT_FILE.read_text())["valuation"]["blocks"]
        dec = self.ctx.tokens.decimals(cid, token)
        tdec = self.ctx.tokens.decimals(cid, term)
        blk = blocks.get(str(cid))
        if blk and dec is not None and tdec is not None:
            try:
                ret = rpc.eth_call(e2.CHAINS[str(cid)]["drpc"], token, evmabi.encode_call(
                    "convertToAssets(uint256)", ["uint256"], [10 ** dec]), int(blk))
                self.rpc_reads += 1
                assets = evmabi.decode(["uint256"], ret)[0]
                if assets > 0:
                    return assets / 10 ** tdec, (f"convertToAssets at block {blk} (the "
                                                 f"valuation block) = {assets / 10 ** tdec:.6f}")
            except Exception as ex:
                self.rpc_errors.append(f"{cid}:{token} convertToAssets: {ex}"[:200])
        for d in range(self.tday, self.tday - 400, -1):
            a = self.ctx.prices.raw(cid, token, d)
            b = self.ctx.prices.raw(cid, term, d)
            if a and b:
                return a / b, f"DefiLlama close ratio {a / b:.6f} on {day_str(d)}"
        return None


_E3 = {}


def e3_rows(ctx):
    """E3 daily vault table, {(protocol, chain, vault)."""
    if not _E3:
        vd = defaultdict(dict)
        with open(DERIVED / "aum/vault_daily.csv", newline="") as fh:
            for r in csv.DictReader(fh):
                vd[(r["protocol"], int(r["chain_id"]), r["vault"])][e2.day_of(r["date"])] = (
                    int(r["total_assets"] or 0), int(r["total_supply"] or 0),
                    float(r["aum_usd"]) if r["aum_usd"] else None)
        ld = defaultdict(dict)
        with open(DERIVED / "aum/lending_daily.csv", newline="") as fh:
            for r in csv.DictReader(fh):
                ld[(int(r["chain_id"]), r["vault"])][e2.day_of(r["date"])] = (
                    int(r["total_assets"] or 0), float(r["aum_usd"]) if r["aum_usd"] else None,
                    r["governed"] == "1")
        excl = {(int(r["chain_id"]), r["vault"]): r["reasons"]
                for r in jsonl(DERIVED / "aum/excluded_vaults.jsonl")}
        _E3.update({"vault": vd, "lending": ld, "excluded": excl})
    return _E3


def e3_share_price(ctx, proto, cid, vault, day):
    """Assets per share (whole units) of a registry vault on a day, from E3."""
    row = e3_rows(ctx)["vault"].get((proto, int(cid), vault), {}).get(day)
    reg = e2.registry_vaults().get((int(cid), vault)) or {}
    adec = ctx.tokens.decimals(cid, reg.get("asset"))
    sdec = ctx.tokens.decimals(cid, vault) or 18
    if not row or not row[1] or adec is None:
        return None
    return (row[0] / 10 ** adec) / (row[1] / 10 ** sdec)


def family_collateral_vaults(ctx, cid, market, fam_ids):
    """Euler collateral vaults of a lending vault whose token is in one of the event's families (event_families)."""
    info = e2.market_info_cached("euler_v2", cid).get(market, {})
    return {cv: tok for tok, cv in info.get("collaterals", [])
            if tok and ctx.fams.resolve(cid, tok)["family_id"] in fam_ids}


def euler_family_collateral(ctx, raw, cid, market, cvs, ts):
    """Family collateral that indebted accounts of an Euler lending vault hold at ts."""
    base = DATA / f"vaults/euler_v2/{cid}"
    debt = {}
    with open(base / f"borrows/{market}.csv", newline="") as fh:
        for r in csv.DictReader(fh):
            if int(r["timestamp"]) > ts:
                break
            debt[r["account"]] = int(r["cum_event_assets"] or 0)
    owing = {a for a, x in debt.items() if x > 0}
    shares = {}
    p = base / f"collateral/{market}.csv"
    if p.exists():
        with open(p, newline="") as fh:
            for r in csv.DictReader(fh):
                if int(r["timestamp"]) > ts:
                    break
                shares[(r["account"], r["collateral_vault"])] = int(r["cum_shares"] or 0)
    units = defaultdict(float)
    mixed = set()
    for (a, cv), sh in shares.items():
        if a not in owing or sh <= 0:
            continue
        if cv not in cvs:
            mixed.add(a)
            continue
        st = ctx.pstate.at("euler_v2", cid, cv, ts)
        aps = e2.aps_of("euler_v2", st["supply"], st["shares"]) if st else 1.0
        units[cv] += sh * aps
    fam_accounts = {a for (a, cv), sh in shares.items() if a in owing and cv in cvs and sh > 0}
    return dict(units), len(owing), len(fam_accounts & mixed)


def bad_debt_events(ctx, proto, cid, market, fam_cvs, lo, hi):
    """[(ts, assets)] of bad debt the market wrote off against its lenders in (lo, hi]."""
    out = []
    if proto in ("morpho", "moolah"):
        with open(e2.panel_dir(proto, cid) / "liquidations.csv", newline="") as fh:
            for r in csv.DictReader(fh):
                if r["market_id"] == market and lo < int(r["timestamp"]) <= hi \
                        and int(r["bad_debt_assets"] or 0) > 0:
                    out.append((int(r["timestamp"]), int(r["bad_debt_assets"])))
    elif proto == "euler_v2":
        p = e2.panel_dir(proto, cid) / "losses.jsonl"
        rows = [r for r in jsonl(p) if r.get("vault") == market] if p.exists() else []
        fam_tx = {r["tx_hash"] for r in rows if r["event"] == "Liquidate"
                  and (r.get("collateral") or "").lower() in fam_cvs}
        out = [(r["timestamp"], int(r["assets"])) for r in rows
               if r["event"] == "DebtSocialized" and lo < r["timestamp"] <= hi
               and r["tx_hash"] in fam_tx]
    else:
        p = e2.panel_dir(proto, cid) / "defaults.jsonl"
        if p.exists():
            out = [(r["ts"], int(r["canceled_debt"])) for r in jsonl(p)
                   if r["silo"] == market and lo < r["ts"] <= hi]
    return sorted(out)


def principal_flows(rows, i_ref, i_t, accrual, col):
    """Value of column col (4 debt, 3 supply) at row i_ref plus its change on every non-accrual row up to i_t (borrows and repa."""
    d = rows[i_ref][col] if i_ref >= 0 else 0
    for i in range(max(i_ref, 0) + 1, i_t + 1):
        if rows[i][7] not in accrual:
            d += rows[i][col] - rows[i - 1][col]
    return d


def market_shortfall(ctx, raw, rec, e, am):
    """Principal shortfall of one affected market of a depeg-loss event at the valuation time T (see the module docstring)."""
    proto, cid, m = am["protocol"], am["chain_id"], am["market"]
    fam_ids = event_families(e)
    T, tday = rec.val_ts, rec.tday
    ref = am["first_trigger_ts"]
    info = e2.market_info_cached(proto, cid).get(m, {})
    loan = info.get("loan")
    ldec = ctx.tokens.decimals(cid, loan)
    lp = rec.ref(cid, loan)
    p_loan = lp["price_usd"] if lp else ctx.prices.usd(cid, loan, tday)[0]
    loan_src = lp["source"] if lp else f"DefiLlama close on {day_str(tday)}"
    loan_side = ctx.fams.resolve(cid, loan)["family_id"] in fam_ids
    tss, rows = ctx.pstate.series(proto, cid, m)
    i_t = bisect.bisect_right(tss, T) - 1
    i_r = bisect.bisect_right(tss, ref) - 1
    out = {"event_id": e["event_id"], "protocol": proto, "chain_id": cid, "market": m,
           "loan_token": loan, "loan_symbol": ctx.tokens.symbol(cid, loan),
           "loan_decimals": ldec, "loan_price_usd_T": p_loan, "loan_price_source": loan_src,
           "loan_side": loan_side,
           "reference_ts": ref, "reference_utc": utc(ref), "valuation_ts": T,
           "valuation_utc": utc(T), "state_T_row_utc": utc(rows[i_t][2]) if i_t >= 0 else None}
    if i_t < 0:
        out["unread_reason"] = "no panel row before T"
        return out
    S_T, D_T, SH_T = rows[i_t][3], rows[i_t][4], rows[i_t][5]
    fam_cvs = {}
    mixed = n_owing = None
    if proto in ("morpho", "moolah", "silo_v2"):
        accrual = {"accrue"} if proto != "silo_v2" else {"AccruedInterest"}
        D_fam_T = D_T
        D_fam_p = min(max(principal_flows(rows, i_r, i_t, accrual, 4), 0), D_T)
        S_p = max(principal_flows(rows, i_r, i_t, accrual, 3), 0)
        share = 1.0
        principal_rule = ("debt and supply at the reference + their non-accrual changes from the "
                          "panel rows")
        if proto == "silo_v2":
            ctok, csilo = info["collaterals"][0]
            st = raw.at(proto, cid, csilo, T)
            c_units = {csilo: float(int(st["collateral_assets"]) + int(st["protected_assets"]))
                       if st else 0.0}
            coll_tokens = {csilo: ctok}
            coll_rule = ("paired silo's collateral + protected assets at T (all depositors of "
                         "the family token, an upper bound on the borrowers' collateral)")
        else:
            st = raw.at(proto, cid, m, T)
            ctok = info["collaterals"][0][0]
            c_units = {m: float(int(st["total_collateral"]))}
            coll_tokens = {m: ctok}
            coll_rule = "market total_collateral at T"
    else:
        fam_cvs = family_collateral_vaults(ctx, cid, m, fam_ids)
        sh = e2.family_debt_share(cid, m, tday, fam_ids, ctx.fams)
        share = 1.0 if sh is None else sh
        D_fam_T = D_T * share
        st_t = raw.at(proto, cid, m, T)
        st_r = raw.at(proto, cid, m, ref)
        acc_t = int(st_t["interestAccumulator"])
        acc_r = int(st_r["interestAccumulator"]) if st_r else acc_t
        D_fam_p = D_fam_T * acc_r / acc_t
        S_p = S_T - (D_fam_T - D_fam_p)
        principal_rule = ("family-backed debt (borrow_distribution share) x interestAccumulator "
                          "at the reference / at T")
        c_units, n_owing, mixed = euler_family_collateral(ctx, raw, cid, m, set(fam_cvs), T)
        coll_tokens = dict(fam_cvs)
        coll_rule = ("family collateral-vault shares of accounts owing at T x the collateral "
                     "vault's assets per share at T")
    rec_value_loan = 0.0
    coll_detail = []
    missing = []
    for key, units in c_units.items():
        tok = coll_tokens[key]
        cdec = ctx.tokens.decimals(cid, tok)
        r = rec.usd(cid, tok, key if proto == "euler_v2" else None, since_day=ref // DAY)
        if r is None or r["price_usd"] is None or cdec is None or not p_loan:
            missing.append(tok)
            continue
        v = units / 10 ** cdec * r["price_usd"] / p_loan * 10 ** ldec
        rec_value_loan += v
        coll_detail.append({"collateral": key, "token": tok,
                            "symbol": ctx.tokens.symbol(cid, tok), "units": units / 10 ** cdec,
                            "recovery_price_usd": r["price_usd"], "recovery_unit_price": r["unit_price"],
                            "recovery_unit": r["unit"], "recovery_source": r["source"],
                            "value_loan_units": v / 10 ** ldec})
    ps = max(D_fam_p - rec_value_loan, 0.0) if not missing else None
    ps_mult = (max(D_fam_T - rec_value_loan, 0.0) * (D_fam_p / D_fam_T if D_fam_T > 0 else 0.0)
               if not missing else None)
    if missing:
        out["unread_reason"] = "collateral without a recovery price: " + ", ".join(missing)
    bd = bad_debt_events(ctx, proto, cid, m, set(fam_cvs), ref, T)
    scale = 10 ** ldec if ldec is not None else None
    usd = (lambda x: x / scale * p_loan) if scale and p_loan else (lambda x: None)
    out.update({
        "supply_book_T": S_T / scale if scale else None, "supply_shares_T": SH_T,
        "debt_book_T": D_T / scale if scale else None, "cash_T": (S_T - D_T) / scale if scale else None,
        "family_debt_share_T": share, "debt_family_book_T": D_fam_T / scale if scale else None,
        "debt_family_principal_T": D_fam_p / scale if scale else None,
        "principal_rule": principal_rule, "collateral_rule": coll_rule, "collateral": coll_detail,
        "collateral_missing_price": missing, "euler_owing_accounts": n_owing,
        "euler_mixed_family_accounts": mixed,
        "recoverable_collateral_loan_units": rec_value_loan / scale if scale else None,
        "principal_shortfall_raw": ps,
        "principal_shortfall": ps / scale if scale and ps is not None else None,
        "principal_shortfall_usd": usd(ps) if ps is not None else None,
        "principal_shortfall_mult_raw": ps_mult,
        "principal_shortfall_mult_usd": usd(ps_mult) if ps_mult is not None else None,
        "supply_principal_T": S_p / scale if scale else None,
        "shortfall_ratio": min(ps / S_p, 1.0) if ps is not None and S_p > 0 else None,
        "bad_debt_events": bd, "bad_debt_raw": sum(x for _, x in bd),
        "bad_debt_usd": usd(sum(x for _, x in bd)),
        "phantom_interest_usd": usd(D_fam_T - D_fam_p)})
    return out


def ref_index():
    """The input tables' per-market depositor figures, keyed (protocol, chain, market id / lending vault / loan silo)."""
    idx = defaultdict(list)
    for case in CASES:
        s = json.loads((INPUTS / f"case_summaries/{case}.json").read_text())
        for r in s["per_market"]:
            parts = r["market_uid"].split(":")
            if parts[0] != "morpho" or len(parts) < 3 or r.get("channel") != "collateral":
                continue
            c, mid = parts[1], parts[2]
            idx[("morpho", int(c), mid)].append({
                "file": f"data/inputs/case_summaries/{case}.json", "key": f"per_market[{r['market_uid']}]",
                "book_shortfall_usd": r.get("position_shortfall_usd"),
                "principal_shortfall_usd": r.get("position_shortfall_principal_est"),
                "principal_ratio": r.get("principal_ratio"),
                "written_off_usd": r.get("bad_debt_written_off_usd"),
                "final_debt_usd": r.get("final_debt_usd"), "recovery_usd": r.get("recovery_usd"),
                "recovery_price": r.get("recovery_price")})
    pt = json.loads(PT_FILE.read_text())
    silo_cfg = defaultdict(list)
    for proto, cid in e2.PAIRS:
        if proto == "silo_v2":
            for m, inf in e2.market_info_cached(proto, cid).items():
                silo_cfg[(cid, inf.get("silo_config"))].append(m)
    for case in CASES:
        adds = defaultdict(float)
        for r in pt["additions"][case]["markets"]:
            adds[r["uid"]] += r.get("depositor_usd") or 0.0
        for r in pt["family_collateral"][case]["markets"]:
            p, c, a = r["uid"].split(":")
            key = ("euler_v2", int(c), a) if p == "eulervault" else \
                  ("silo_v2", int(c), r.get("loan_silo")) if p == "silo" else None
            if key is None or key[2] is None:
                continue
            idx[key].append({
                "file": "data/inputs/pt_markets.json",
                "key": f"family_collateral.{case}.markets[{r['uid']}]",
                "book_shortfall_usd": r.get("shortfall_book_usd"),
                "principal_shortfall_usd": r.get("shortfall_principal_usd"),
                "principal_subtract_usd": r.get("shortfall_principal_subtract_usd"),
                "principal_ratio": r.get("principal_ratio"),
                "written_off_usd": r.get("written_off_onset_to_val_usd"),
                "depositor_loss_usd": r.get("depositor_loss_usd"),
                "debt_val_usd": r.get("debt_val_usd"), "collateral_val_usd": r.get("collateral_val_usd"),
                "additions_depositor_usd_all_parts": adds.get(r["uid"])})
    n = json.loads(NOVEL_FILE.read_text())
    for ev in n["events"]:
        for r in (ev["state_estimate"].get("depositors") or {}).get("markets") or []:
            parts = r["market_uid"].split(":")
            p, c = parts[0], int(parts[1])
            if p == "morpho":
                keys = [("morpho", c, parts[2])]
            elif p == "euler":
                keys = [("euler_v2", c, parts[2])]
            elif p == "silo":
                keys = [("silo_v2", c, s) for s in silo_cfg.get((c, parts[2]), [])]
            else:
                keys = []
            for key in keys:
                idx[key].append({
                    "file": "data/inputs/event_sizing.json",
                    "key": f"events[{ev['event_id']}].state_estimate.depositors.markets[{r['market_uid']}]",
                    "principal_debt_usd": r.get("principal_debt_usd"),
                    "collateral_value_usd": r.get("collateral_value_usd"),
                    "depositor_loss_usd": r.get("depositor_est_usd"),
                    "debt_at_valuation_usd": r.get("debt_at_valuation_usd")})
    return idx


def ref_scope(e):
    """Which input-table records describe this event."""
    d = json.loads(RECOVERY_FILE.read_text())
    toks = {(m["chain_id"], (m.get("collateral_token") or "").lower()) for m in e["affected_markets"]}
    cases = {v["case"] for v in d["tokens"].values()
             if any((int(c), a.lower()) in toks for c, a in v.get("deployments") or []
                    if str(c).isdigit())}
    return [f"/{c}/" for c in cases] + [f".{c}." for c in cases] + \
        [f"events[{x}]" for x in e.get("census_events") or []]


def stage_shortfall(work):
    ctx = Ctx(work)
    raw = RawPanel()
    rec = Recovery(ctx)
    idx = ref_index()
    out = {}
    for e in ctx.events:
        if e["type"] != "depeg_loss":
            continue
        scope = ref_scope(e)
        for am in e["affected_markets"]:
            r = market_shortfall(ctx, raw, rec, e, am)
            r["ref"] = [x for x in idx.get((am["protocol"], am["chain_id"], am["market"]), [])
                          if any(k in x["file"] + x["key"] for k in scope)]
            out[(e["event_id"], am["protocol"], am["chain_id"], am["market"])] = r
            print(f"shortfall {e['event_id']} {am['protocol']}:{am['chain_id']} {am['market'][:10]} "
                  f"ratio {r.get('shortfall_ratio')} ps_usd {r.get('principal_shortfall_usd')} "
                  f"bd_usd {r.get('bad_debt_usd')}", flush=True)
    work.save("shortfall.pkl", {"markets": out, "val_ts": rec.val_ts, "val_utc": rec.val_utc,
                                "recovery_used": rec.used, "rpc_reads": rec.rpc_reads,
                                "rpc_errors": rec.rpc_errors})
    return {"markets": len(out), "rpc_reads": rec.rpc_reads, "rpc_errors": rec.rpc_errors,
            "with_ref_figure": sum(1 for r in out.values() if r["ref"])}


_INFLOW = {}


def inflow_usd(proto, cid, vault, ms, t_lo, t_hi, prices, asset):
    """Sum over transactions in [t_lo, t_hi] of the positive changes of the vault's position in the markets ms (e5_inflows.pos_."""
    key = (proto, cid, vault, tuple(sorted(ms)), t_lo, t_hi)
    if key not in _INFLOW:
        import e5_inflows
        path = DATA / f"vaults/{proto}/{cid}/positions/{vault}.csv"
        tot = 0.0
        if path.exists() and ms:
            delta, _ext, meta = e5_inflows.pos_deltas(proto, path, t_lo, t_hi)
            for tx, d in delta.items():
                aff = sum(d.get(m, 0) for m in ms)
                if aff > 0:
                    tot += prices.value(cid, asset, aff, meta[tx][2] // DAY) or 0.0
        _INFLOW[key] = tot
    return _INFLOW[key]


def held_measures(ctx, s, e, proto, cid, ms, guards, weight, e3ta):
    """E5 measures of one vault with a series (it held an affected market at some time)."""
    prices = ctx.prices
    asset, kind = s["asset"], s["kind"]
    t0, end = e["onset_ts"], e["end_ts"]
    t0d = t0 // DAY
    d0, n = s["d0"], len(s["total"])
    mdays = ctx.series(proto, cid)["mdays"]
    held = [m for m in ms if m in s["pos"]]

    def ix(d):
        i = d - d0
        return i if 0 <= i < n else None

    def gf(m, d):
        md, g = mdays.get(m), guards.get(m)
        if md is None or g is None:
            return 1.0
        return e2.guard_factor(g, d * DAY + DAY - 1, md.at(d, md.aps))

    def xs(d):
        """(unweighted, family-weighted, guard cut) of the event markets at the end of day d, raw units."""
        i = ix(d)
        if i is None:
            return 0.0, 0.0, 0.0
        xu = xw = cut = 0.0
        for m in held:
            p = s["pos"][m][i]
            if p <= 0:
                continue
            g = gf(m, d)
            xu += p * g
            xw += p * g * weight(m, d)
            cut += p * (1 - g)
        return xu, xw, cut

    def value(d):
        i = ix(d)
        if i is None:
            return 0.0
        v = s["total"][i] - xs(d)[2]
        if kind == "morpho_v2":
            ta = e3ta(d)
            if ta:
                v += max(0.0, ta - s["total"][i])
        return v

    pos_t0, by_market, capped_t0 = 0.0, {}, False
    for m in held:
        st = ctx.pstate.at(proto, cid, m, t0)
        if st is None:
            continue
        md = mdays.get(m)
        fac = md.at(t0d, md.fac) if md is not None else 1.0
        aps_t0 = e2.aps_of(proto, st["supply"], st["shares"]) * fac
        g = e2.guard_factor(guards.get(m), t0, aps_t0)
        capped_t0 |= g < 1 - 1e-12
        if kind == "euler_lending":
            by_market[m] = float(st["supply"]) * g
            pos_t0 += by_market[m]
            continue
        rr = s["rows"].get(m, [])
        k = bisect.bisect_right([x[2] for x in rr], t0)
        sh = rr[k - 1][3] if k > 0 else 0
        if e2.written_off((s.get("queue") or {}).get(m), t0):
            sh = 0
        x = sh * aps_t0 * g
        if x > 0:
            by_market[m] = x
        pos_t0 += x
    xw_t0 = sum(x * weight(m, t0d) for m, x in by_market.items())
    usd = lambda x, d: prices.value(cid, asset, x, d)

    path = [(d * DAY + DAY - 1, xs(d)[1], d) for d in range(t0d - LOOKBACK_D, t0d)]
    path.append((t0, xw_t0, t0d))
    peak, peak_day = max(((v, d) for _t, v, d in path), default=(0.0, None))
    held_usd = max((usd(v, d) or 0.0 for _t, v, d in path), default=0.0)
    is_held = held_usd >= MATERIAL_USD
    exited = is_held and peak > 0 and xw_t0 < EXIT_FRACTION * peak
    group = "never_entered" if not is_held else "exited_before_t0" if exited else "present_at_t0"
    lead = None
    if exited:
        above = [i for i, (_t, v, _d) in enumerate(path) if v >= LEAD_FRACTION * peak]
        if above and above[-1] + 1 < len(path):
            lead = round((t0 - path[above[-1] + 1][0]) / DAY, 2)

    path2 = [(d * DAY + DAY - 1, xs(d)[0], d) for d in range(t0d - e2.EXPOSURE_LOOKBACK_D, t0d)]
    path2.append((t0, pos_t0, t0d))
    peak2 = max((v for _t, v, _d in path2), default=0.0)
    before2 = max(max((usd(v, d) or 0.0 for _t, v, d in path2), default=0.0),
                  usd(pos_t0, t0d) or 0.0) >= MATERIAL_USD
    after_days = [d for d in range(t0d, end // DAY + 1) if ix(d) is not None]
    after_pos_usd = max((usd(xs(d)[0], d) or 0.0 for d in after_days), default=0.0)
    after_exp_usd = max((usd(xs(d)[1], d) or 0.0 for d in after_days), default=0.0)
    reentered = False
    if exited and kind == "euler_lending":
        after_exp_raw = max((xs(d)[1] for d in after_days), default=0.0)
        reentered = after_exp_raw >= EXIT_FRACTION * peak and after_exp_usd >= MATERIAL_USD
    elif exited:
        reentered = inflow_usd(proto, cid, s["vault"], held, t0, end, prices, asset) >= MATERIAL_USD
    in_e2 = before2 or after_pos_usd >= MATERIAL_USD
    group2 = lead2 = None
    if in_e2:
        exited2 = before2 and peak2 > 0 and pos_t0 < EXIT_FRACTION * peak2
        group2 = "entered_after_t0" if not before2 else "exited_before_t0" if exited2 \
            else "present_at_t0"
        if exited2:
            above = [i for i, (_t, v, _d) in enumerate(path2) if v >= LEAD_FRACTION * peak2]
            if above and above[-1] + 1 < len(path2):
                lead2 = round((t0 - path2[above[-1] + 1][0]) / DAY, 2)

    locked = {x: 0 for x in (L_LOCK,) + L_SENS}
    min_l = None
    spell = longest = 0
    for d in range(t0d, end // DAY + 1):
        i = ix(d)
        if i is None:
            spell = 0
            continue
        x_usd = usd(xs(d)[0], d)
        tot, wd = s["total"][i], s["wd"][i]
        for m in held:
            p, g = s["pos"][m][i], gf(m, d)
            if p > 0 and g < 1:
                av = mdays[m].at(d, mdays[m].avail)
                tot += p * g - p
                wd += min(p * g, av) - min(p, av)
        if tot <= 0 or (x_usd or 0.0) < MATERIAL_USD:
            spell = 0
            continue
        lv = wd / tot
        min_l = lv if min_l is None else min(min_l, lv)
        for x in locked:
            if lv < x:
                locked[x] += 1
        spell = spell + 1 if lv < L_LOCK else 0
        longest = max(longest, spell)

    v_t0 = value(t0d - 1) - xs(t0d - 1)[0] + pos_t0 if ix(t0d - 1) is not None else pos_t0
    paths = []
    for d in range(t0d - PATH_BEFORE_D, t0d + PATH_AFTER_D + 1):
        if ix(d) is None:
            continue
        v = value(d)
        xw = xs(d)[1]
        paths.append({"day": d - t0d, "date": day_str(d),
                      "exposure_share": xw / v if v > 0 else None,
                      "exposure_usd": usd(xw, d), "value_usd": usd(v, d)})
    return {
        "group": group, "held_usd_max_180d": held_usd,
        "entered_after_t0": (not is_held and after_exp_usd >= MATERIAL_USD) or reentered,
        "value_t0_raw": v_t0, "value_t0_usd": usd(v_t0, t0d), "value_source": "series",
        "position_t0_raw": pos_t0, "position_t0_usd": usd(pos_t0, t0d),
        "position_t0_by_market": by_market,
        "exposure_t0_raw": xw_t0, "exposure_t0_usd": usd(xw_t0, t0d),
        "exposure_share_t0": xw_t0 / v_t0 if v_t0 > 0 else None,
        "peak_180d_exposure_usd": usd(peak, peak_day) if peak_day is not None else None,
        "peak_180d_day": day_str(peak_day) if peak_day is not None else None,
        "max_exposure_usd_after_t0": after_exp_usd, "exit_lead_days": lead,
        "days_locked": locked[L_LOCK], "days_locked_1pct": locked[0.01],
        "days_locked_10pct": locked[0.10], "days_locked_longest": longest, "min_L_after_t0": min_l,
        "guard_capped_at_t0": capped_t0,
        "written_off": {m: d for m, d in (s.get("written_off") or {}).items() if m in held},
        "e2_rule": {"in_e2": in_e2, "group": group2, "exit_lead_days": lead2,
                    "position_t0_usd": usd(pos_t0, t0d), "exposure_t0_usd": usd(xw_t0, t0d),
                    "days_locked": locked[L_LOCK], "days_locked_1pct": locked[0.01],
                    "days_locked_10pct": locked[0.10], "t0_utc": utc(t0)},
        "_paths": paths}


def load_roles(proto, cid):
    """{vault: addresses that ever held a curator, owner, allocator, sentinel or guardian role (Moolah."""
    out = defaultdict(set)
    p = DATA / f"vaults/{proto}/{cid}/roles.jsonl"
    if p.exists():
        for r in jsonl(p):
            if r.get("role") in ROLE_NAMES and r.get("address"):
                out[r["vault"]].add(r["address"].lower())
    return out


ACTION_EVENTS = ("SetCap", "ReallocateWithdraw", "Deallocate", "DecreaseAbsoluteCap",
                 "DecreaseRelativeCap", "SubmitMarketRemoval")


def load_actions(proto, cid, vaults):
    """{vault: [log rows]} of the curator-action events of the given vaults (actions.jsonl of the pair), sorted by block and log index."""
    p = DATA / f"vaults/{proto}/{cid}/actions.jsonl"
    out = defaultdict(list)
    if not p.exists() or not vaults:
        return out
    keys = tuple(f'"{k}"'.encode() for k in ACTION_EVENTS)
    with open(p, "rb") as fh:
        for line in fh:
            if not any(k in line for k in keys):
                continue
            r = json.loads(line)
            if r.get("vault") not in vaults or r.get("event") not in ACTION_EVENTS:
                continue
            f = r.get("fields") or r
            out[r["vault"]].append({
                "event": r["event"], "ts": r["timestamp"], "block": r["block"],
                "log_index": r["log_index"], "tx": r.get("tx_hash"),
                "market": (f.get("id") or f.get("market") or "").lower(),
                "cap": f.get("cap") if f.get("cap") is not None else
                f.get("newAbsoluteCap", f.get("newRelativeCap")),
                "assets": f.get("withdrawnAssets", f.get("assets")),
                "caller": (f.get("caller") or f.get("sender") or "").lower(),
                "ids": [x.lower() for x in f.get("ids") or []],
                "adapter": (f.get("adapter") or "").lower()})
    for v in out:
        out[v].sort(key=lambda a: (a["block"], a["log_index"]))
    return out


_V2IDS = {}


def v2_ids(cid, vault, market):
    """Vault V2 cap ids that cover a Morpho market."""
    from eth_abi import encode
    from eth_hash.auto import keccak
    key = (cid, vault, market)
    if key in _V2IDS:
        return _V2IDS[key]
    if cid not in _V2IDS:
        ad = defaultdict(set)
        p = DATA / f"vaults/morpho/{cid}/adapters.jsonl"
        if p.exists():
            for r in jsonl(p):
                if (r.get("type") or "").startswith("MorphoMarketV1Adapter"):
                    ad[(r.get("vault") or r.get("parentVault") or "").lower()].add(r["adapter"].lower())
        reg = {r["market_id"]: r for r in jsonl(DATA / f"registry/markets/morpho/{cid}.jsonl")}
        _V2IDS[cid] = (ad, reg)
    ad, reg = _V2IDS[cid]
    r = reg.get(market)
    ids, adapters = set(), ad.get(vault, set())
    if r:
        mp = (r["loan_token"], r["collateral_token"], r["oracle"], r["irm"], int(r["lltv"]))
        for a in adapters:
            ids.add("0x" + keccak(encode(["string", "address", "(address,address,address,address,uint256)"],
                                         ["this/marketParams", a, mp])).hex())
        ids.add("0x" + keccak(encode(["string", "address"],
                                     ["collateralToken", r["collateral_token"]])).hex())
    _V2IDS[key] = (ids, adapters)
    return _V2IDS[key]


def vault_actions(cid, ms, s, logs, roles, fam_cvs, gov_logs):
    """Curator actions of one vault on the event's markets."""
    vault, kind = s["vault"], s["kind"]
    msset = set(ms)
    acts = []
    holders = roles.get(vault, set())
    by_role = (lambda a: not holders or a in holders)
    if kind == "euler_lending":
        prev = {}
        for r in gov_logs.get(vault, []):
            cv = r["collateral"].lower()
            ltv = r["borrowLTV"]
            if cv in fam_cvs and cv in prev and ltv < prev[cv]:
                acts.append({"type": "cap_reduction", "ts": r["timestamp"], "market": vault,
                             "detail": f"GovSetLTV borrowLTV {prev[cv]} -> {ltv} on {cv}",
                             "amount_raw": None, "caller": None, "by_role_holder": True})
            prev[cv] = ltv
        return acts
    caps = {}
    if kind == "morpho_v2":
        info = {m: v2_ids(cid, vault, m) for m in ms}
        for a in logs:
            if a["event"] in ("DecreaseAbsoluteCap", "DecreaseRelativeCap"):
                hit = [m for m in ms if a["market"] in info[m][0]]
                for m in hit:
                    acts.append({"type": "cap_reduction", "ts": a["ts"], "market": m,
                                 "detail": f"{a['event']} id {a['market']} -> {a['cap']}",
                                 "amount_raw": None, "caller": a["caller"],
                                 "by_role_holder": by_role(a["caller"])})
            elif a["event"] == "Deallocate":
                hit = [m for m in ms if a["adapter"] in info[m][1] and set(a["ids"]) & info[m][0]]
                if hit and by_role(a["caller"]):
                    acts.append({"type": "realloc_out", "ts": a["ts"], "market": hit[0],
                                 "detail": "Deallocate", "amount_raw": int(a["assets"] or 0),
                                 "caller": a["caller"], "by_role_holder": True})
    else:
        for a in logs:
            m = a["market"]
            if a["event"] == "SetCap":
                cap = int(a["cap"] or 0)
                if m in msset and m in caps and cap < caps[m]:
                    acts.append({"type": "cap_reduction", "ts": a["ts"], "market": m,
                                 "detail": f"SetCap {caps[m]} -> {cap}", "amount_raw": None,
                                 "caller": a["caller"], "by_role_holder": by_role(a["caller"])})
                caps[m] = cap
            elif m in msset and a["event"] == "ReallocateWithdraw" and int(a["assets"] or 0) > 0:
                own = by_role(a["caller"])
                acts.append({"type": "realloc_out" if own else "realloc_out_public", "ts": a["ts"],
                             "market": m, "detail": "ReallocateWithdraw",
                             "amount_raw": int(a["assets"]), "caller": a["caller"],
                             "by_role_holder": own})
            elif m in msset and a["event"] == "SubmitMarketRemoval":
                acts.append({"type": "removal_submitted", "ts": a["ts"], "market": m,
                             "detail": "SubmitMarketRemoval", "amount_raw": None,
                             "caller": a["caller"], "by_role_holder": by_role(a["caller"])})
        for m in ms:
            for ts, _b, _li, inq in (s.get("queue") or {}).get(m) or []:
                if not inq:
                    acts.append({"type": "queue_removal", "ts": ts, "market": m,
                                 "detail": "SetWithdrawQueue without the market", "amount_raw": None,
                                 "caller": None, "by_role_holder": True})
    return sorted(acts, key=lambda a: a["ts"])


def action_summary(acts, t0, end, prices, cid, asset):
    """Counts and first/last times (days relative to t0) per action type, before t0 (180 days) and after it (to the event end)."""
    out = {}
    lo = t0 - LOOKBACK_D * DAY
    for t in ACTION_TYPES:
        b = [a for a in acts if a["type"] == t and lo <= a["ts"] < t0]
        f = [a for a in acts if a["type"] == t and t0 <= a["ts"] <= end]
        out[f"n_{t}_before"] = len(b)
        out[f"first_{t}_before_days"] = round((b[0]["ts"] - t0) / DAY, 2) if b else None
        out[f"last_{t}_before_days"] = round((b[-1]["ts"] - t0) / DAY, 2) if b else None
        out[f"n_{t}_after"] = len(f)
        out[f"first_{t}_after_days"] = round((f[0]["ts"] - t0) / DAY, 2) if f else None
    out["realloc_out_usd_before"] = sum(
        prices.value(cid, asset, a["amount_raw"], a["ts"] // DAY) or 0.0
        for a in acts if a["type"] == "realloc_out" and lo <= a["ts"] < t0)
    return out


def vault_loss(ctx, s, proto, cid, ms, sf, by_market_t0, val_ts):
    """Realized loss of one vault in a depeg-loss event, raw loan units."""
    mdays = ctx.series(proto, cid)["mdays"]
    tot = {"short": 0.0, "bad": 0.0, "pos_T": 0.0, "t0_variant": 0.0, "short_mult": 0.0,
           "short_usd": 0.0, "bad_usd": 0.0, "pos_T_usd": 0.0, "t0_variant_usd": 0.0,
           "short_mult_usd": 0.0}
    by_m = {}

    def frac(m, ts):
        if s["kind"] == "euler_lending":
            return 1.0
        rr = s["rows"].get(m, [])
        k = bisect.bisect_right([x[2] for x in rr], ts)
        sh = rr[k - 1][3] if k > 0 else 0
        st = ctx.pstate.at(proto, cid, m, ts)
        if not sh or st is None or st["supply"] <= 0:
            return 0.0
        md = mdays.get(m)
        fac = md.at(ts // DAY, md.fac) if md is not None else 1.0
        return min(sh * e2.aps_of(proto, st["supply"], st["shares"]) * fac / st["supply"], 1.0)
    for m in ms:
        r = sf.get((proto, cid, m))
        if r is None or m not in s["pos"] or r.get("principal_shortfall_raw") is None:
            continue
        f = frac(m, val_ts)
        short = f * r["principal_shortfall_raw"]
        short_mult = f * (r.get("principal_shortfall_mult_raw") or 0.0)
        bad = sum(frac(m, ts - 1) * x for ts, x in r["bad_debt_events"])
        scale = 10 ** r["loan_decimals"]
        pos_T = f * r["supply_principal_T"] * scale
        var = by_market_t0.get(m, 0.0) * (r["shortfall_ratio"] or 0.0)
        px = (r["loan_price_usd_T"] or 0.0) / scale
        by_m[m] = {"share_of_supply_T": f, "short": short, "bad": bad,
                   "usd": (short + bad) * px, "usd_mult": (short_mult + bad) * px}
        for k, x in (("short", short), ("bad", bad), ("pos_T", pos_T), ("t0_variant", var),
                     ("short_mult", short_mult)):
            tot[k] += x
            tot[f"{k}_usd"] += x * px
    return tot, by_m


def stage_rows(work):
    ctx = Ctx(work)
    e3 = e3_rows(ctx)
    sfp = work.load("shortfall.pkl")
    val_ts = sfp["val_ts"]
    rows, paths, actions, compare = [], [], [], []
    by_pair_events = defaultdict(list)
    for e in ctx.events:
        for pair in event_pairs(e):
            by_pair_events[pair].append(e)
    logs_cache = {}
    for (proto, cid), evs in sorted(by_pair_events.items()):
        vs = ctx.series(proto, cid)
        holders = set().union(*[vs["holders_by_market"].get(m, set())
                                for e in evs for m in event_pairs(e)[(proto, cid)]])
        logs_cache[(proto, cid)] = (load_actions(proto, cid, holders), load_roles(proto, cid))
        gov = defaultdict(list)
        if proto == "euler_v2":
            for r in jsonl(e2.panel_dir(proto, cid) / "governance.jsonl"):
                if r["event"] == "GovSetLTV":
                    gov[r["vault"]].append(r)
            for v in gov:
                gov[v].sort(key=lambda r: (r["block"], r["log_index"]))
        logs_cache[(proto, cid, "gov")] = gov
    reg_all = e2.registry_vaults()
    for e2_event in sorted(ctx.events, key=lambda x: x["onset_ts"]):
        t0 = e5_onset(e2_event)
        e = dict(e2_event, onset_ts=t0, onset_utc=utc(t0))
        end = e["end_ts"]
        t0d = t0 // DAY
        fam_ids = event_families(e)
        issuer = e.get("issuer_entity")
        sf_all = {k[1:]: v for k, v in sfp["markets"].items() if k[0] == e["event_id"]}
        ev_rows = []
        for (proto, cid), ms in sorted(event_pairs(e).items()):
            vs = ctx.series(proto, cid)
            guards = {m: e2.guard_reference(proto, cid, m, t0, ctx.pstate, vs["mdays"].get(m))
                      for m in ms}
            guards_e2 = guards if t0 == e2_event["onset_ts"] else {
                m: e2.guard_reference(proto, cid, m, e2_event["onset_ts"], ctx.pstate,
                                      vs["mdays"].get(m)) for m in ms}

            def weight(m, d, cid=cid, proto=proto, fam_ids=fam_ids):
                if proto != "euler_v2":
                    return 1.0
                sh = e2.family_debt_share(cid, m, d, fam_ids, ctx.fams)
                return 1.0 if sh is None else sh
            info = e2.market_info_cached(proto, cid)
            loans = {info[m]["loan"] for m in ms if m in info}
            fam_cvs = set()
            if proto == "euler_v2":
                for m in ms:
                    fam_cvs |= set(family_collateral_vaults(ctx, cid, m, fam_ids))
            logs, roles = logs_cache[(proto, cid)]
            gov = logs_cache[(proto, cid, "gov")]
            seen = set()
            cands = sorted(set().union(*[vs["holders_by_market"].get(m, set()) for m in ms]))
            if proto == "euler_v2":
                cands += [f"self:{m}" for m in ms]
            for v in cands:
                s = vs["series"].get(v)
                if s is None:
                    continue
                if s["kind"] == "euler_lending":
                    gv = s.get("governors", [])
                    if (e2.governor_at(gv, max(t0, gv[0][0]) if gv else t0) or ZERO) == ZERO:
                        continue

                def e3ta(d, s=s, proto=proto, cid=cid):
                    r = e3["vault"].get((proto, cid, s["vault"]), {}).get(d)
                    return r[0] if r else None
                h = held_measures(ctx, s, e, proto, cid, ms, guards, weight, e3ta)
                h["e2_check"] = h["e2_rule"] if guards_e2 is guards else held_measures(
                    ctx, s, e2_event, proto, cid, ms, guards_e2, weight, e3ta)["e2_rule"]
                live = h["group"] != "never_entered" or h["entered_after_t0"] or \
                    (h["value_t0_usd"] or 0.0) >= MATERIAL_USD or h["e2_check"]["in_e2"]
                if not live:
                    continue
                seen.add(s["vault"])
                acts = vault_actions(cid, ms, s, logs.get(s["vault"], []), roles, fam_cvs, gov)
                row = {"event_id": e["event_id"], "protocol": proto, "chain_id": cid,
                       "vault": s["vault"], "kind": s["kind"], "name": s.get("name"),
                       "asset": s["asset"], "has_series": True, **h,
                       **action_summary(acts, t0, end, ctx.prices, cid, s["asset"])}
                if e["type"] == "depeg_loss":
                    tot, by_m = vault_loss(ctx, s, proto, cid, ms, sf_all, h["position_t0_by_market"],
                                           val_ts)
                    row["_loss"] = (tot, by_m)
                for a in acts:
                    if not t0 - LOOKBACK_D * DAY <= a["ts"] <= end:
                        continue
                    actions.append({"event_id": e["event_id"], "protocol": proto, "chain_id": cid,
                                    "vault": s["vault"], **a, "utc": utc(a["ts"]),
                                    "days_rel_t0": round((a["ts"] - t0) / DAY, 3),
                                    "amount_usd": ctx.prices.value(cid, s["asset"], a["amount_raw"],
                                                                   a["ts"] // DAY)
                                    if a["amount_raw"] else None})
                ev_rows.append(row)
            for (c2, v), r in reg_all.items():
                if c2 != cid or r["protocol"] != proto or r["asset"] not in loans or v in seen:
                    continue
                if (cid, v) in e3["excluded"]:
                    continue
                x = e3["vault"].get((proto, cid, v), {}).get(t0d - 1)
                if not x or (x[2] or 0.0) < MATERIAL_USD:
                    continue
                ev_rows.append({"event_id": e["event_id"], "protocol": proto, "chain_id": cid,
                                "vault": v, "kind": r.get("kind"), "name": r.get("name"),
                                "asset": r["asset"], "has_series": False, "group": "never_entered",
                                "entered_after_t0": False, "value_t0_raw": x[0],
                                "value_t0_usd": x[2], "value_source": "e3", "exposure_t0_usd": 0.0,
                                "exposure_t0_raw": 0.0, "exposure_share_t0": 0.0,
                                "position_t0_usd": 0.0, "position_t0_by_market": {}, "_paths": []})
            if proto == "euler_v2":
                for (c2, v), days in e3["lending"].items():
                    if c2 != cid or v in seen or v in ms:
                        continue
                    x = days.get(t0d - 1)
                    if not x or not x[2] or (x[1] or 0.0) < MATERIAL_USD:
                        continue
                    if info.get(v, {}).get("loan") not in loans:
                        continue
                    ev_rows.append({"event_id": e["event_id"], "protocol": proto, "chain_id": cid,
                                    "vault": v, "kind": "euler_lending", "name": None,
                                    "asset": info[v]["loan"], "has_series": False,
                                    "group": "never_entered", "entered_after_t0": False,
                                    "value_t0_raw": x[0], "value_t0_usd": x[1], "value_source": "e3",
                                    "exposure_t0_usd": 0.0, "exposure_t0_raw": 0.0,
                                    "exposure_share_t0": 0.0, "position_t0_usd": 0.0,
                                    "position_t0_by_market": {}, "_paths": []})
        finish_event_rows(ctx, e, ev_rows, issuer)
        compare.extend(compare_e2(e2_event, ev_rows, t0))
        for r in ev_rows:
            r["t0_utc"], r["e2_onset_utc"] = utc(t0), e2_event["onset_utc"]
        for r in ev_rows:
            for p in r.pop("_paths"):
                if r["group"] in ("exited_before_t0", "present_at_t0"):
                    paths.append({"event_id": e["event_id"], "protocol": r["protocol"],
                                  "chain_id": r["chain_id"], "vault": r["vault"], "kind": r["kind"],
                                  "curator": r["curator"], "group": r["group"], **p})
        rows.extend(ev_rows)
        g = Counter(r["group"] for r in ev_rows)
        print(f"rows {e['event_id']} {e['type']} {dict(g)}", flush=True)
    work.save("rows.pkl", {"rows": rows, "paths": paths, "actions": actions, "compare": compare})
    return {"rows": len(rows), "paths": len(paths), "actions": len(actions),
            "groups": dict(Counter(r["group"] for r in rows))}


def finish_event_rows(ctx, e, ev_rows, issuer):
    """Curator, third-party flags, losses in USD and the Euler netting (a lending vault's exposure, value and loss net of the E."""
    t0, end = e["onset_ts"], e["end_ts"]
    t0d = t0 // DAY
    is_depeg = e["type"] == "depeg_loss"
    earn_pos, earn_loss = defaultdict(float), defaultdict(float)
    earn_loss_mult = defaultdict(float)
    for r in ev_rows:
        ent, conf = e2.curator_in_window(ctx.curators, r["chain_id"], r["vault"], t0, end)
        r["curator"], r["curator_confidence"] = ent, conf
        r["curator_name"] = ctx.names.get(ent) if ent else None
        r["issuer_curated"] = issuer is not None and ent == issuer
        ctl = e2.SELF_DEALING_VAULTS.get((r["chain_id"], r["vault"]))
        sd = None
        if r["group"] != "never_entered" and ent is None and r["kind"] != "euler_lending":
            sd = e2.self_deposited(r["protocol"], r["chain_id"], r["vault"],
                                   min(end, ctx.heads[f"{r['protocol']}:{r['chain_id']}"]["ts"]))
        r["self_deposited"] = True if ctl else sd
        r["third_party"] = not r["issuer_curated"] and not r["self_deposited"]
        if r["kind"] == "euler_earn":
            for m, x in (r.get("position_t0_by_market") or {}).items():
                earn_pos[(r["chain_id"], m)] += x
        loss = r.get("_loss")
        if loss:
            for m, x in loss[1].items():
                if r["kind"] == "euler_earn":
                    earn_loss[(r["chain_id"], m)] += x["usd"]
                    earn_loss_mult[(r["chain_id"], m)] += x["usd_mult"]
    for r in ev_rows:
        cid, asset = r["chain_id"], r["asset"]
        r["exposure_t0_usd_counted"] = r.get("exposure_t0_usd") or 0.0
        r["value_t0_usd_counted"] = r.get("value_t0_usd") or 0.0
        if r["kind"] == "euler_lending" and r["has_series"]:
            m = r["vault"]
            net = max(r["position_t0_by_market"].get(m, 0.0) - earn_pos[(cid, m)], 0.0)
            w = r["exposure_t0_raw"] / r["position_t0_raw"] if r.get("position_t0_raw") else 0.0
            r["exposure_t0_usd_counted"] = ctx.prices.value(cid, asset, net * w, t0d) or 0.0
            r["value_t0_usd_counted"] = ctx.prices.value(
                cid, asset, max(r["value_t0_raw"] - earn_pos[(cid, m)], 0.0), t0d) or 0.0
        loss = r.pop("_loss", None)
        if not is_depeg:
            r.update({k: None for k in ("loss_usd", "loss_usd_counted", "loss_per_dollar",
                                        "loss_shortfall_usd", "loss_bad_debt_usd",
                                        "position_principal_T_usd", "loss_usd_t0_position",
                                        "loss_usd_mult", "loss_usd_counted_mult")})
            continue
        if loss is None:
            r.update({"loss_usd": 0.0, "loss_usd_counted": 0.0,
                      "loss_per_dollar": 0.0 if (r.get("value_t0_usd") or 0.0) >= PER_DOLLAR_MIN_USD
                      else None,
                      "loss_shortfall_usd": 0.0, "loss_bad_debt_usd": 0.0,
                      "position_principal_T_usd": 0.0, "loss_usd_t0_position": 0.0,
                      "loss_usd_mult": 0.0, "loss_usd_counted_mult": 0.0})
            continue
        tot, by_m = loss
        raw = tot["short"] + tot["bad"]
        loss_usd = tot["short_usd"] + tot["bad_usd"]
        counted = loss_usd
        loss_mult = tot["short_mult_usd"] + tot["bad_usd"]
        counted_mult = loss_mult
        if r["kind"] == "euler_lending":
            counted = max(loss_usd - earn_loss[(cid, r["vault"])], 0.0)
            counted_mult = max(loss_mult - earn_loss_mult[(cid, r["vault"])], 0.0)
        fb = {(am["protocol"], am["chain_id"], am["market"]) for am in e["affected_markets"]
              if am.get("source") == "family_backed"}
        r["loss_usd_family_backed"] = sum(x["usd"] for m, x in by_m.items()
                                          if (r["protocol"], cid, m) in fb)
        r.update({"loss_usd": loss_usd, "loss_usd_counted": counted,
                  "loss_usd_mult": loss_mult, "loss_usd_counted_mult": counted_mult,
                  "loss_per_dollar": (raw / r["value_t0_raw"] if r.get("value_t0_raw")
                                      and (r.get("value_t0_usd") or 0.0) >= PER_DOLLAR_MIN_USD
                                      else None),
                  "loss_shortfall_usd": tot["short_usd"], "loss_bad_debt_usd": tot["bad_usd"],
                  "position_principal_T_usd": tot["pos_T_usd"],
                  "loss_usd_t0_position": tot["t0_variant_usd"],
                  "share_of_supply_T": {m: x["share_of_supply_T"] for m, x in by_m.items()},
                  "loss_usd_by_market": {m: x["usd"] for m, x in by_m.items()}})


def compare_e2(e, ev_rows, t0):
    """For every E2 exposed-vault row."""
    mine = {(r["protocol"], r["chain_id"], r["vault"]): r for r in ev_rows}
    out = []
    for v in e["exposed_vaults"]:
        r = mine.get((v["protocol"], v["chain_id"], v["vault"]))
        rec = {"event_id": e["event_id"], "protocol": v["protocol"], "chain_id": v["chain_id"],
               "vault": v["vault"], "kind": v["kind"], "found": r is not None,
               "t0_moved": t0 != e["onset_ts"],
               "e2": {k: v.get(k) for k in ("group", "exposure_t0_usd", "position_t0_usd",
                                            "exit_lead_days", "days_locked", "days_locked_1pct",
                                            "days_locked_10pct")}}
        if r is not None:
            rec["e5"] = {k: r.get(k) for k in ("group", "exposure_t0_usd", "position_t0_usd",
                                               "exit_lead_days", "days_locked", "days_locked_1pct",
                                               "days_locked_10pct", "entered_after_t0")}
            rec["e2_rule"] = r.get("e2_check")
        out.append(rec)
    return out


ROW_COLS = [
    "event_id", "event_type", "t0_utc", "e2_onset_utc", "family", "protocol", "chain_id", "vault", "kind",
    "name", "asset_symbol", "curator", "curator_name", "curator_confidence", "issuer_curated",
    "self_deposited", "third_party", "group", "entered_after_t0", "in_e2", "e2_group",
    "value_t0_usd", "value_t0_usd_counted", "value_source", "exposure_t0_usd",
    "exposure_t0_usd_counted", "exposure_share_t0", "position_t0_usd", "peak_180d_exposure_usd",
    "peak_180d_day", "max_exposure_usd_after_t0", "exit_lead_days"] + [
    f"{p}_{t}_{w}" for t in ACTION_TYPES
    for p, w in (("n", "before"), ("first", "before_days"), ("last", "before_days"),
                 ("n", "after"), ("first", "after_days"))] + [
    "realloc_out_usd_before", "days_locked", "days_locked_1pct", "days_locked_10pct",
    "days_locked_longest", "min_L_after_t0", "written_off", "guard_capped_at_t0", "loss_usd",
    "loss_usd_counted", "loss_per_dollar", "loss_shortfall_usd", "loss_bad_debt_usd",
    "position_principal_T_usd", "loss_usd_t0_position", "loss_usd_mult", "loss_usd_counted_mult", "loss_usd_family_backed",
    "inflow_usd_after_t0", "inflow_mechanism_after_t0", "extracted_usd_onbehalf"]
SHORT_COLS = [
    "event_id", "protocol", "chain_id", "market", "loan_symbol", "collateral_symbols",
    "reference_utc", "valuation_utc", "state_T_row_utc", "supply_book_T", "debt_book_T",
    "cash_T", "family_debt_share_T", "debt_family_book_T", "debt_family_principal_T",
    "phantom_interest_usd", "collateral_units", "recovery_price_usd", "recovery_source",
    "recoverable_collateral_loan_units", "principal_shortfall", "principal_shortfall_usd",
    "supply_principal_T", "shortfall_ratio", "bad_debt_usd", "market_loss_usd",
    "principal_shortfall_mult_usd", "market_loss_mult_usd", "principal_rule", "collateral_rule",
    "ref_file", "ref_key",
    "ref_principal_shortfall_usd", "ref_principal_subtract_usd", "ref_written_off_usd",
    "ref_depositor_loss_usd", "ref_market_total_usd"]


def write_csv(rel, cols, rows, meta):
    p = DERIVED / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    with open(tmp, "w", newline="") as fh:
        w = csv.DictWriter(fh, cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: fnum(r.get(k)) if not isinstance(r.get(k), (dict, list, bool))
                        else (json.dumps(r[k], sort_keys=True) if isinstance(r[k], (dict, list))
                              else r[k]) for k in cols})
    os.replace(tmp, p)
    io_utils._meta_sidecar(p, GENERATOR, {**meta, "rows": len(rows), "columns": cols})
    return p


def input_versions(work):
    """sha256 and write time of the inputs the outputs rest on."""
    out = {}
    for rel in ("events/events.jsonl", "events/collateral_families.jsonl",
                "events/tokens_supplement.jsonl", "events/prices_supplement.csv",
                "aum/vault_daily.csv", "aum/lending_daily.csv", "aum/excluded_vaults.jsonl",
                "registry/vault_curators.jsonl", "registry/curators.jsonl"):
        p = (DATA if rel.startswith("registry/") else DERIVED) / rel
        side = p.with_name(p.name + ".meta.json")
        out[rel] = {"sha256": e2.sha256(p),
                    "written_utc": json.loads(side.read_text()).get("written_utc")
                    if side.exists() else None}
    for p in (RECOVERY_FILE, NOVEL_FILE, PT_FILE):
        out[inputs.rel(p)] = {"sha256": e2.sha256(p)}
    return out


def ref_pick(r):
    """The input tables' figure for one market, as they report the depositors' loss."""
    ps = r.get("ref") or []
    pick = next((x for x in ps if "family_collateral" in x["key"]), None) or \
        next((x for x in ps if "per_market" in x["key"]), None) or (ps[0] if ps else None)
    if pick is None:
        return {}
    if "family_collateral" in pick["key"]:
        dep, total = pick.get("depositor_loss_usd"), pick.get("additions_depositor_usd_all_parts")
    elif "per_market" in pick["key"]:
        dep = (pick.get("principal_shortfall_usd") or 0.0) + (pick.get("written_off_usd") or 0.0)
        total = dep
    else:
        dep = total = pick.get("depositor_loss_usd")
    return {"ref_file": pick["file"], "ref_key": pick["key"],
            "ref_principal_shortfall_usd": pick.get("principal_shortfall_usd"),
            "ref_principal_subtract_usd": pick.get("principal_subtract_usd"),
            "ref_written_off_usd": pick.get("written_off_usd"),
            "ref_depositor_loss_usd": dep, "ref_market_total_usd": total}


def shortfall_rows(sfp):
    out = []
    for r in sfp["markets"].values():
        c = r.get("collateral") or []
        out.append({**r, **ref_pick(r),
                    "collateral_symbols": ",".join(x["symbol"] or x["token"] for x in c),
                    "collateral_units": sum(x["units"] for x in c) if c else None,
                    "recovery_price_usd": c[0]["recovery_price_usd"] if c else None,
                    "recovery_source": c[0]["recovery_source"] if c else None,
                    "market_loss_usd": (r.get("principal_shortfall_usd") or 0.0) +
                    (r.get("bad_debt_usd") or 0.0),
                    "market_loss_mult_usd": (r.get("principal_shortfall_mult_usd") or 0.0) +
                    (r.get("bad_debt_usd") or 0.0)})
    return out


def curator_summary(rows):
    """Per curator entity, over third-party rows."""
    per = defaultdict(lambda: defaultdict(list))
    loss_all, loss_late, names = defaultdict(float), defaultdict(float), {}
    loss_mult = defaultdict(float)
    for r in rows:
        if not r["third_party"]:
            continue
        cur = r["curator"] or "unattributed"
        names.setdefault(cur, r["curator_name"] or cur)
        if r["event_type"] == "depeg_loss":
            loss_all[cur] += r["loss_usd_counted"] or 0.0
            loss_mult[cur] += r.get("loss_usd_counted_mult") or 0.0
            if r["group"] == "never_entered" or r.get("entered_after_t0"):
                loss_late[cur] += r["loss_usd_counted"] or 0.0
        if r["group"] != "never_entered":
            per[cur][r["event_id"]].append(r)
    out = []
    for cur in sorted(set(per) | {c for c, x in loss_all.items() if x > 0}):
        evs = per.get(cur, {})
        status = {k: ("present_at_t0" if any(x["group"] == "present_at_t0" for x in v)
                      else "exited_before_t0") for k, v in evs.items()}
        flat = [x for v in evs.values() for x in v]
        dep = [x for x in flat if x["event_type"] == "depeg_loss"]
        loss = loss_all[cur]
        expo = sum(x["exposure_t0_usd_counted"] or 0.0 for x in dep)
        val = sum(x["value_t0_usd_counted"] or 0.0 for x in dep)
        big = [x for x in dep if (x.get("value_t0_usd") or 0.0) >= PER_DOLLAR_MIN_USD]
        loss_pd = loss - sum(x["loss_usd_counted"] or 0.0 for x in dep
                             if (x.get("value_t0_usd") or 0.0) < PER_DOLLAR_MIN_USD)
        expo_pd = sum(x["exposure_t0_usd_counted"] or 0.0 for x in big)
        val_pd = sum(x["value_t0_usd_counted"] or 0.0 for x in big)
        leads = [x["exit_lead_days"] for x in flat if x["exit_lead_days"] is not None]
        locks = [x["days_locked"] for x in flat if x["group"] == "present_at_t0"]
        out.append({
            "curator": cur, "curator_name": names[cur],
            "vault_rows": len(flat), "vaults": len({(x["chain_id"], x["vault"]) for x in flat}),
            "events_entered": len(evs),
            "events_exited_before_onset": sum(1 for s in status.values() if s == "exited_before_t0"),
            "events_present_at_t0": sum(1 for s in status.values() if s == "present_at_t0"),
            "depeg_events_entered": len({x["event_id"] for x in dep}),
            "exposure_t0_usd_depeg": expo, "value_t0_usd_depeg": val,
            "loss_usd": loss, "loss_usd_mult": loss_mult[cur],
            "loss_usd_entered_after_t0": loss_late[cur],
            "loss_per_dollar_exposure": loss_pd / expo_pd if expo_pd > 0 else None,
            "loss_per_dollar_value": loss_pd / val_pd if val_pd > 0 else None,
            "median_exit_lead_days": statistics.median(leads) if leads else None,
            "n_exit_lead": len(leads),
            "median_days_locked": statistics.median(locks) if locks else None,
            "n_days_locked": len(locks),
            "events": ";".join(f"{k}:{'P' if s == 'present_at_t0' else 'X'}"
                               for k, s in sorted(status.items()))})
    out.sort(key=lambda x: (-x["events_entered"], -x["loss_usd"], x["curator"]))
    return out


E4_DIR = [DERIVED / "metrics/e4"]


def curator_aum_rank():
    """E4's curator ranking by average AUM (curator_scorecard.csv, rank_by_aum), for the 12-largest view, and the file's provenance."""
    p = E4_DIR[0] / "curator_scorecard.csv"
    if not p.exists():
        return {}, None
    with open(p, newline="") as fh:
        rows = list(csv.DictReader(fh))
    side = p.with_name(p.name + ".meta.json")
    prov = {"path": str(p), "sha256": e2.sha256(p),
            "written_utc": json.loads(side.read_text()).get("written_utc") if side.exists() else None}
    return {r["curator_entity"]: int(r["rank_by_aum"]) for r in rows}, prov


def fig5_lines(ctx, rows, paths):
    """One line per curator in each of the FIG5_EVENTS largest events (E2's third-party exposure rank)."""
    rank = sorted(ctx.events, key=lambda e: -(e.get("exposure_third_party_usd_rank") or 0))
    top = [e["event_id"] for e in rank[:FIG5_EVENTS]]
    sel = {(r["event_id"], r["protocol"], r["chain_id"], r["vault"]): r for r in rows
           if r["event_id"] in top and r["third_party"] and r["kind"] != "euler_lending"
           and r["group"] in ("exited_before_t0", "present_at_t0")}
    agg = defaultdict(lambda: [0.0, 0.0])
    grp = defaultdict(set)
    for p in paths:
        r = sel.get((p["event_id"], p["protocol"], p["chain_id"], p["vault"]))
        if r is None or p["value_usd"] is None or p["value_usd"] < MATERIAL_USD:
            continue
        label = r["curator_name"] or "unattributed vaults"
        k = (p["event_id"], label, p["day"])
        agg[k][0] += p["exposure_usd"] or 0.0
        agg[k][1] += p["value_usd"]
        grp[(p["event_id"], label)].add(r["group"])
    out = []
    for (ev, label, day), (x, v) in sorted(agg.items()):
        g = "present_at_t0" if "present_at_t0" in grp[(ev, label)] else "exited_before_t0"
        out.append({"event_id": ev, "curator": label, "day": day,
                    "exposure_share": x / v if v > 0 else None, "exposure_usd": x, "value_usd": v,
                    "group": g})
    return top, out


def stage_outputs(work):
    ctx = Ctx(work)
    rp = work.load("rows.pkl")
    sfp = work.load("shortfall.pkl")
    rows, paths, actions = rp["rows"], rp["paths"], rp["actions"]
    evs = {e["event_id"]: e for e in ctx.events}
    e2g = {(e["event_id"], v["protocol"], v["chain_id"], v["vault"]): v["group"]
           for e in ctx.events for v in e["exposed_vaults"]}
    for r in rows:
        e = evs[r["event_id"]]
        r.update({"event_type": e["type"],
                  "family": e["collateral_family"]["label"],
                  "asset_symbol": ctx.tokens.symbol(r["chain_id"], r["asset"]),
                  "in_e2": (r["event_id"], r["protocol"], r["chain_id"], r["vault"]) in e2g,
                  "e2_group": e2g.get((r["event_id"], r["protocol"], r["chain_id"], r["vault"]))})
    rows.sort(key=lambda r: (evs[r["event_id"]]["onset_ts"], r["event_id"], r["group"],
                             -(r.get("exposure_t0_usd") or 0.0), r["chain_id"], r["vault"]))
    import e5_inflows
    inf, _files = e5_inflows.compute(rows, evs)
    inf = {(x["event_id"], x["protocol"], x["chain_id"], x["vault"]): x for x in inf}
    for r in rows:
        if r["event_type"] != "depeg_loss" or r["kind"] == "euler_lending":
            r.update(inflow_usd_after_t0=None, inflow_mechanism_after_t0=None,
                     extracted_usd_onbehalf=None)
            continue
        x = inf.get((r["event_id"], r["protocol"], r["chain_id"], r["vault"]))
        r.update(inflow_usd_after_t0=x["inflow_usd_total"] if x else 0.0,
                 inflow_mechanism_after_t0=x["mechanism_main"] if x else "",
                 extracted_usd_onbehalf=x["extracted_usd_onbehalf_tx"] if x else 0.0)
    versions = input_versions(work)
    heads = work.load("heads.pkl")
    common = {"inputs": versions, "panel_heads": heads["heads"],
              "parameters": {"lookback_days": LOOKBACK_D, "exit_fraction": EXIT_FRACTION,
                             "lead_fraction": LEAD_FRACTION, "material_usd": MATERIAL_USD,
                             "path_days": [-PATH_BEFORE_D, PATH_AFTER_D],
                             "L_thresholds": [L_LOCK, *L_SENS], "valuation_ts": sfp["val_ts"],
                             "valuation_utc": sfp["val_utc"]}}
    write_csv(f"{OUT}/event_vault.csv", ROW_COLS, rows, {
        **common,
        "what": "one row per (E2 event, curated vault on the same protocol and chain whose asset "
                "is the loan asset of an affected market) that was live at t0 or held the "
                "markets; groups, exposure, exit lead time, curator actions, days locked, "
                "realized loss (see data/resume/e5-events.md)",
        "units": "USD at the day's DefiLlama close (stables carried at 1 when unquoted, E2's "
                 "rule); losses are loan units times the loan token's price at the valuation "
                 "time (the recovery price when the loan token is itself depegged, "
                 "else the DefiLlama close); loss_per_dollar is in the vault's own asset units; "
                 "days are UTC days; action times in days relative to t0 (negative before)",
        "t0": "E2's onset; for a depeg-loss event the onset of the episode after which the "
              "collateral did not recover (e2_onset_utc repeats it)",
        "days_locked": "count of day ends from t0's day to the event end with L(t) < 5% while "
                       "the vault holds >= MATERIAL_USD in the markets, not necessarily "
                       "consecutive (E2's rule); days_locked_longest is the longest run of "
                       "consecutive such day ends",
        "loss_per_dollar": f"None when the vault was worth less than {PER_DOLLAR_MIN_USD:,.0f} USD "
                           "at t0 (left out of every per-dollar ratio; the dollar loss stays in "
                           "every total)",
        "entered_after_t0": "never entered and exposure after t0 reached MATERIAL_USD, or exited "
                            "before t0 and transactions between t0 and the event end raised its "
                            "position in the event's markets by at least MATERIAL_USD in sum "
                            "(re-entry; Euler lending vaults: exposure back to 10 percent of the "
                            "180-day peak and MATERIAL_USD)",
        "loss_usd_family_backed": "part of loss_usd from markets E2 did not hit whose debt "
                                  "the event's families backed at t0 (family_backed_markets)",
        "inflow_usd_after_t0": "e5_inflows.py: positive changes of the vault's position in the "
                               "event's markets in transactions within 30 days after t0, USD; 0 "
                               "below MATERIAL_USD; depeg-loss rows other than Euler lending "
                               "vaults only",
        "inflow_mechanism_after_t0": "largest component of that inflow: allocator, "
                                     "public_allocator, supply_queue, external_onbehalf, other",
        "extracted_usd_onbehalf": "vault withdrawals less deposits in the transactions where a "
                                  "third party supplied into the event's markets on the vault's "
                                  "behalf (donation attacks): cash taken out of the vault that "
                                  "the point-in-time loss does not see; not added to loss_usd",
        "groups": dict(Counter(r["group"] for r in rows))})
    write_csv(f"{OUT}/exposure_paths.csv",
              ["event_id", "protocol", "chain_id", "vault", "kind", "curator", "group", "day",
               "date", "exposure_share", "exposure_usd", "value_usd"], paths, {
                  **common,
                  "what": "per (event, vault in the exited or present group, day from t0-90 to "
                          "t0+30): share of vault value in the event's markets (guarded, family "
                          "weighted) and vault value, at UTC day ends; day 0 is t0's day end"})
    daily = {}
    for a in actions:
        d = a["ts"] // DAY
        t0d = e5_onset(evs[a["event_id"]]) // DAY
        k = (a["event_id"], a["protocol"], a["chain_id"], a["vault"], a["type"], d)
        x = daily.setdefault(k, {"event_id": k[0], "protocol": k[1], "chain_id": k[2],
                                 "vault": k[3], "type": k[4], "date": day_str(d),
                                 "day_rel_t0": d - t0d, "n": 0, "amount_usd": 0.0,
                                 "first_utc": a["utc"], "markets": set(), "details": set()})
        x["n"] += 1
        x["amount_usd"] += a["amount_usd"] or 0.0
        x["markets"].add(a["market"])
        if a["type"] != "realloc_out":
            x["details"].add(a["detail"])
    act_rows = []
    for k in sorted(daily):
        x = daily[k]
        act_rows.append({**x, "markets": sorted(x["markets"]), "details": sorted(x["details"])})
    write_csv(f"{OUT}/curator_actions.csv",
              ["event_id", "protocol", "chain_id", "vault", "type", "date", "day_rel_t0", "n",
               "amount_usd", "first_utc", "markets", "details"], act_rows, {
                  **common,
                  "what": "curator actions on an event's markets from the vaults' logs, per "
                          "(event, vault, type, UTC day), from 180 days before t0 to the event "
                          "end; realloc_out_public are reallocations by callers without a role "
                          "(the public allocator)",
                  "actions": dict(Counter(a["type"] for a in actions))})
    srows = shortfall_rows(sfp)
    write_csv(f"{OUT}/market_shortfall.csv", SHORT_COLS, srows, {
        **common,
        "what": "principal shortfall of every affected market of a depeg-loss event at the "
                "valuation time, with the input tables' figure for the same market",
        "definition": "principal shortfall = max(principal debt - collateral x recovery price, "
                      "0); shortfall ratio = principal shortfall / (book supply less the "
                      "interest accrued after the market's onset); the state rule of "
                      "data/inputs/hit_rules.md",
        "definition_mult": "principal_shortfall_mult_usd = max(book debt - collateral x recovery "
                           "price, 0) x principal debt / book debt, the input tables' "
                           "multiplicative estimate; market_loss_mult_usd "
                           "adds the written-off bad debt",
        "recovery_used": sfp["recovery_used"]})
    cs = curator_summary(rows)
    rank, e4_prov = curator_aum_rank()
    for c in cs:
        c["e4_aum_rank"] = rank.get(c["curator"])
    write_csv(f"{OUT}/curator_summary.csv",
              ["curator", "curator_name", "e4_aum_rank", "vault_rows", "vaults", "events_entered",
               "events_exited_before_onset", "events_present_at_t0", "depeg_events_entered",
               "exposure_t0_usd_depeg", "value_t0_usd_depeg", "loss_usd", "loss_usd_mult",
               "loss_usd_entered_after_t0", "loss_per_dollar_exposure", "loss_per_dollar_value",
               "median_exit_lead_days", "n_exit_lead", "median_days_locked", "n_days_locked",
               "events"], cs, {
                  **common,
                  "what": "per curator entity over third-party rows of event_vault.csv "
                          "(unattributed vaults pooled); e4_aum_rank is E4's rank_by_aum",
                  "e4_curator_scorecard": e4_prov})
    top, lines = fig5_lines(ctx, rows, paths)
    write_csv(f"{OUT}/fig5_data.csv", ["event_id", "curator", "day", "exposure_share",
                                       "exposure_usd", "value_usd", "group"], lines, {
                  **common, "events": top,
                  "what": "Fig 5 lines: the six largest events by E2's third-party exposure rank "
                          "(Table 3), one line per curator"})
    work.save("outputs.pkl", {"rows": rows, "fig5": (top, lines), "summary": cs})
    return {"event_vault": len(rows), "paths": len(paths), "actions": len(actions),
            "curators": len(cs), "fig5_events": top}


INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
C_PRESENT, C_EXITED = "#2a78d6", "#eb6834"
SEQ = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7", "#3987e5",
       "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b"]


def _plt():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.family": "sans-serif", "font.size": 7, "axes.edgecolor": MUTED,
                         "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
                         "axes.linewidth": 0.6, "pdf.fonttype": 42})
    return plt


def spread_labels(ys, gap, lo, hi):
    """Label heights at least `gap` apart inside [lo, hi], as close to the line ends as that allows (the gap shrinks when the labels do not fit)."""
    if not ys:
        return []
    gap = min(gap, (hi - lo) / max(len(ys) - 1, 1))
    order = sorted(range(len(ys)), key=lambda i: ys[i])
    pos = []
    for k, i in enumerate(order):
        pos.append(max(ys[i], lo, pos[-1] + gap if k else lo))
    pos[-1] = min(pos[-1], hi)
    for k in range(len(pos) - 2, -1, -1):
        pos[k] = min(pos[k], pos[k + 1] - gap)
    out = [0.0] * len(ys)
    for k, i in enumerate(order):
        out[i] = pos[k]
    return out


def stage_figures(work):
    ctx = Ctx(work)
    o = work.load("outputs.pkl")
    plt = _plt()
    figdir = DERIVED / OUT / "figures"
    figdir.mkdir(parents=True, exist_ok=True)
    evs = {e["event_id"]: e for e in ctx.events}
    top, lines = o["fig5"]
    fig, axes = plt.subplots(2, 3, figsize=(7.2, 5.0), sharex=True)
    by_ev = defaultdict(lambda: defaultdict(list))
    for r in lines:
        if r["exposure_share"] is not None:
            by_ev[r["event_id"]][(r["curator"], r["group"])].append((r["day"], r["exposure_share"]))
    peak_usd = defaultdict(float)
    for r in lines:
        peak_usd[(r["event_id"], r["curator"])] = max(peak_usd[(r["event_id"], r["curator"])],
                                                      r["exposure_usd"] or 0.0)
    short = {"morpho": "Morpho", "euler_v2": "Euler", "silo_v2": "Silo", "moolah": "Moolah"}
    for ax, ev in zip(axes.flat, top):
        e = evs[ev]
        ax.axvline(0, color=MUTED, lw=0.6, ls="--")
        ax.grid(axis="y", color=GRID, lw=0.5)
        ax.set_axisbelow(True)
        big = sorted(by_ev[ev], key=lambda k: -peak_usd[(ev, k[0])])[:FIG5_LABELS]
        ends = []
        for (cur, g), pts in sorted(by_ev[ev].items(), key=lambda kv: kv[0] in big):
            pts.sort()
            xs, ys = [p[0] for p in pts], [min(p[1], 1.0) for p in pts]
            labelled = (cur, g) in big
            ax.plot(xs, ys, color=C_PRESENT if g == "present_at_t0" else C_EXITED,
                    lw=1.1 if labelled else 0.6, alpha=1.0 if labelled else 0.45)
            if labelled:
                ends.append((ys[-1], xs[-1], cur))
        top_y = max([0.05] + [y for pts in by_ev[ev].values() for _, y in pts])
        ax.set_ylim(0, min(1.05, top_y * 1.15))
        ymax = ax.get_ylim()[1]
        lab = spread_labels([y for y, _x, _c in ends], 0.075 * ymax, 0.02 * ymax, 0.98 * ymax)
        for (_y, _x, cur), ly in zip(ends, lab):
            ax.text(PATH_AFTER_D + 3, ly, cur[:20], fontsize=5, color=INK2, va="center")
        ax.set_xlim(-PATH_BEFORE_D, PATH_AFTER_D + 58)
        ax.set_xticks([-90, -60, -30, 0, 30])
        n_more = len(by_ev[ev]) - len(ends)
        ax.set_title(f"{e['collateral_family']['label']} {utc(e5_onset(e))[:10]}\n"
                     f"{', '.join(short[p] for p in e['protocols'])}"
                     + (f"; {n_more} smaller lines unlabeled" if n_more > 0 else ""),
                     fontsize=6.3, color=INK, loc="left")
        for sp in ("top", "right"):
            ax.spines[sp].set_visible(False)
    for ax in axes[-1]:
        ax.set_xlabel("days from t0")
    for ax in axes[:, 0]:
        ax.set_ylabel("share of vault value\nin the event's markets")
    from matplotlib.lines import Line2D
    fig.legend([Line2D([], [], color=C_PRESENT, lw=1.2), Line2D([], [], color=C_EXITED, lw=1.2)],
               ["present at t0", "exited before t0"], loc="lower center", ncol=2, frameon=False,
               fontsize=7)
    fig.tight_layout(rect=(0, 0.04, 1, 1), w_pad=1.2, h_pad=1.0)
    fig.savefig(figdir / "fig5_exposure_paths.pdf")
    plt.close(fig)

    rows = [r for r in o["rows"] if r["third_party"] and r["group"] != "never_entered"]
    cell = defaultdict(list)
    for r in rows:
        cell[(r["curator_name"] or "unattributed", r["event_id"])].append(r)
    curs = Counter(c for c, _ in cell)
    cur_order = sorted(curs, key=lambda c: (-curs[c], c))
    ev_order = sorted({ev for _, ev in cell}, key=lambda ev: evs[ev]["onset_ts"])
    import numpy as np
    from matplotlib.colors import ListedColormap
    cmap = ListedColormap(SEQ)
    vals = np.full((len(cur_order), len(ev_order)), np.nan)
    fig, ax = plt.subplots(figsize=(max(7.0, 0.16 * len(ev_order) + 2.0),
                                    max(4.0, 0.15 * len(cur_order) + 1.2)))
    for (cur, ev), rs in cell.items():
        i, j = cur_order.index(cur), ev_order.index(ev)
        if evs[ev]["type"] == "depeg_loss":
            loss = sum(x["loss_usd_counted"] or 0.0 for x in rs)
            val = sum(x["value_t0_usd_counted"] or 0.0 for x in rs)
            vals[i, j] = loss / val if val > 0 else 0.0
        else:
            ax.add_patch(plt.Rectangle((j - 0.5, i - 0.5), 1, 1, color="#f0efec", lw=0))
        if all(x["group"] == "exited_before_t0" for x in rs):
            leads = [x["exit_lead_days"] for x in rs if x["exit_lead_days"] is not None]
            txt = f"{statistics.median(leads):.0f}" if leads else "x"
        else:
            txt = "·"
        dark = not np.isnan(vals[i, j]) and vals[i, j] > 0.25
        ax.text(j, i, txt, ha="center", va="center", fontsize=4.5,
                color="#ffffff" if dark else INK)
    vmax = max(0.05, float(np.nanmax(vals)) if np.isfinite(vals).any() else 0.05)
    xs = np.arange(len(ev_order) + 1) - 0.5
    ys = np.arange(len(cur_order) + 1) - 0.5
    im = ax.pcolormesh(xs, ys, np.ma.masked_invalid(vals), cmap=cmap, vmin=0, vmax=vmax,
                       edgecolors="#ffffff", linewidth=0.4)
    ax.set_xticks(range(len(ev_order)))
    ax.set_xticklabels([f"{evs[ev]['collateral_family']['label'][:10]} {utc(e5_onset(evs[ev]))[:7]}"
                        for ev in ev_order], rotation=90, fontsize=4.5)
    for t, ev in zip(ax.get_xticklabels(), ev_order):
        t.set_color(INK if evs[ev]["type"] == "depeg_loss" else MUTED)
    ax.set_yticks(range(len(cur_order)))
    ax.set_yticklabels(cur_order, fontsize=4.5)
    ax.set_xlim(-0.5, len(ev_order) - 0.5)
    ax.set_ylim(len(cur_order) - 0.5, -0.5)
    cb = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01)
    cb.set_label("realized loss per dollar of vault value at t0", fontsize=6)
    cb.ax.tick_params(labelsize=5)
    ax.set_title("Curators by event (third-party vaults). Number: median exit lead time in days "
                 "(exited before t0); dot: present at t0; grey cell: lock-up (no loss); blank: "
                 "never entered; dark labels: depeg-loss events",
                 fontsize=6, color=INK, loc="left")
    fig.tight_layout()
    fig.savefig(figdir / "figA_event_matrix.pdf")
    plt.close(fig)
    for name, what in (("fig5_exposure_paths.pdf", "Fig 5: exposure paths in the six largest "
                        "events, one line per curator"),
                       ("figA_event_matrix.pdf", "appendix: curator x event matrix")):
        io_utils._meta_sidecar(figdir / name, GENERATOR, {
            "what": what, "data": f"data/{OUT}/fig5_data.csv" if name.startswith("fig5")
            else f"data/{OUT}/event_vault.csv"})
    return {"fig5_events": top, "matrix": [len(cur_order), len(ev_order)]}


class Indep:
    """Independent re-derivation of one row's group, exposure at t0 and exit lead time from the raw position, panel, actions, d."""

    def __init__(self, ctx):
        self.ctx = ctx
        self.panels, self.pos, self.px, self.anchor_cache = {}, {}, {}, {}

    def panel(self, proto, cid, m):
        import pandas as pd
        key = (proto, cid, m)
        if key in self.panels:
            return self.panels[key]
        df = pd.read_csv(e2.panel_dir(proto, cid) / f"{m}.csv", dtype=str, keep_default_na=False)
        t = df["ts" if proto == "silo_v2" else "timestamp"].astype("int64").to_numpy()
        if proto in ("morpho", "moolah"):
            sup = [int(x) for x in df["total_supply_assets"]]
            bor = [int(x) for x in df["total_borrow_assets"]]
            sh = [int(x) for x in df["total_supply_shares"]]
            rate = [int(x) * YEAR / 1e18 if x else None for x in df["borrow_rate"]]
        elif proto == "euler_v2":
            bor = [int(x) for x in df["totalBorrows"]]
            sup = [int(c) + b for c, b in zip(df["cash"], bor)]
            sh = [int(x) for x in df["totalShares"]]
            rate = [int(x) * YEAR / 1e27 for x in df["interestRate"]]
        else:
            sup = [int(x) for x in df["collateral_assets"]]
            bor = [int(x) for x in df["debt_assets"]]
            rate = [float(x) if x else None for x in df["borrow_apr_realized"]]
            sh, tot, prev = [], 0, 0
            for ev, amt, c in zip(df["event"], df["amount"], sup):
                if ev in ("Deposit", "Withdraw"):
                    a = int(amt)
                    num, den = (a * 1000, 1) if tot == 0 else (a * (tot + 1000), prev + 1)
                    tot = tot + num // den if ev == "Deposit" else max(tot - (-(-num // den)), 0)
                prev = c
                sh.append(tot)
        self.panels[key] = (t, sup, bor, sh, rate)
        return self.panels[key]

    def aps(self, proto, s, sh):
        if proto in ("morpho", "moolah"):
            return (s + 1) / (sh + 10 ** 6)
        if proto == "euler_v2":
            return (s + 10 ** 6) / (sh + 10 ** 6)
        return (s + 1) / (sh + 1000) if sh > 0 else 1 / 1000

    def state(self, proto, cid, m, ts):
        t, sup, bor, sh, rate = self.panel(proto, cid, m)
        i = int(__import__("numpy").searchsorted(t, ts, side="right")) - 1
        return i

    def silo_factor(self, cid, m, d):
        """exact / replayed assets per share at the last day with an exact vault conversion (Deposit or Withdraw with assets and sh."""
        key = (cid, m)
        if key not in self.anchor_cache:
            import pandas as pd
            byday = {}
            anchors = []
            for p in e2.position_files("silo_v2", cid).values():
                if m.encode() not in pathlib.Path(p).read_bytes():
                    continue
                df = pd.read_csv(p, dtype=str, keep_default_na=False)
                df = df[(df["market"] == m) & df["kind"].isin(["Deposit", "Withdraw"])
                        & (df["assets"] != "") & (df["event_shares"] != "")]
                for b, li, ts, a, s in zip(df["block"], df["log_index"], df["timestamp"],
                                           df["assets"], df["event_shares"]):
                    if int(a) > 0 and int(s) > 0:
                        anchors.append((int(b), int(li), int(ts), int(a) / int(s)))
            t, sup, bor, sh, rate = self.panel("silo_v2", cid, m)
            for _b, _li, ts, r in sorted(anchors):
                byday[ts // DAY] = r
            fac = {}
            import numpy as np
            for day, r in byday.items():
                i = int(np.searchsorted(t, day * DAY + DAY - 1, side="right")) - 1
                if i >= 0:
                    rep = self.aps("silo_v2", sup[i], sh[i])
                    if rep > 0:
                        fac[day] = r / rep
            self.anchor_cache[key] = (sorted(fac), fac)
        days, fac = self.anchor_cache[key]
        j = bisect.bisect_right(days, d) - 1
        return fac[days[j]] if j >= 0 else 1.0

    def aps_at(self, proto, cid, m, ts, fac_day):
        i = self.state(proto, cid, m, ts)
        if i < 0:
            return None, None
        t, sup, bor, sh, rate = self.panel(proto, cid, m)
        f = self.silo_factor(cid, m, fac_day) if proto == "silo_v2" else 1.0
        return self.aps(proto, sup[i], sh[i]) * f, i

    def guard(self, proto, cid, m, t0):
        t, sup, bor, sh, rate = self.panel(proto, cid, m)
        i = self.state(proto, cid, m, t0)
        if i < 0:
            return None

        def pinned(k):
            return (bor[k] / sup[k] if sup[k] > 0 else (math.inf if bor[k] > 0 else 0.0)) >= 0.99
        j = i
        if pinned(i):
            while j > 0 and pinned(j - 1):
                j -= 1
        ref = int(t[j]) if pinned(i) else t0
        f = self.silo_factor(cid, m, ref // DAY) if proto == "silo_v2" else 1.0
        a = self.aps(proto, sup[j], sh[j]) * f
        r = next((rate[k] for k in range(j, -1, -1) if rate[k] is not None), None)
        r = 1.0 if r is None else min(max(r, 0.0), 1.0)
        return ref, a, r

    def capped(self, g, ts, aps_now):
        if g is None or ts < g[0] or aps_now <= 0:
            return 1.0
        return min(1.0, g[1] * (1 + g[2] * (ts - g[0]) / YEAR) / aps_now)

    def shares(self, proto, cid, vault):
        import pandas as pd
        key = (proto, cid, vault)
        if key not in self.pos:
            df = pd.read_csv(DATA / f"vaults/{proto}/{cid}/positions/{vault}.csv", dtype=str,
                             keep_default_na=False)
            col = e2.POS_MARKET_COL[proto]
            val = "vault_cum_shares" if proto in ("morpho", "moolah") else "cum_shares"
            df["_b"] = df["block"].astype("int64")
            df["_l"] = df["log_index"].astype("int64")
            df = df.sort_values(["_b", "_l"], kind="stable")
            out = defaultdict(list)
            for m, ts, v in zip(df[col], df["timestamp"].astype("int64"), df[val]):
                out[m].append((int(ts), int(v)))
            self.pos[key] = out
        return self.pos[key]

    def queue_out(self, proto, cid, vault, m, ts):
        """True when the market is out of the vault's withdraw queue at ts (queue rebuilt from SetCap and SetWithdrawQueue)."""
        key = ("q", proto, cid, vault)
        if key not in self.pos:
            evs = []
            p = DATA / f"vaults/{proto}/{cid}/actions.jsonl"
            needle = vault.encode()
            with open(p, "rb") as fh:
                for line in fh:
                    if needle in line and (b"SetWithdrawQueue" in line or b'"SetCap"' in line):
                        r = json.loads(line)
                        if r["vault"] == vault and r["event"] in ("SetWithdrawQueue", "SetCap"):
                            evs.append(r)
            evs.sort(key=lambda r: (r["block"], r["log_index"]))
            self.pos[key] = evs
        q, seen = set(), False
        for r in self.pos[key]:
            if r["timestamp"] > ts:
                break
            f = r.get("fields") or r
            if r["event"] == "SetWithdrawQueue":
                q = {x.lower() for x in f.get("newWithdrawQueue") or []}
            elif int(f.get("cap") or 0) > 0:
                q.add((f.get("id") or f.get("market") or "").lower())
            seen = seen or m in q
        return seen and m not in q

    def usd(self, cid, token, raw, d):
        key = cid
        if key not in self.px:
            series = defaultdict(dict)
            for f in (DATA / f"prices/{cid}.csv", DERIVED / "events/prices_supplement.csv"):
                if f.exists():
                    with open(f) as fh:
                        for r in csv.DictReader(fh):
                            if r.get("chain_id") not in (None, str(cid)) or not r["price_usd"]:
                                continue
                            series[r["token"]][e2.day_of(r["date"])] = float(r["price_usd"])
            self.px[key] = series
        s = self.px[key].get(token, {})
        px = next((s[d - k] for k in range(4) if d - k in s), None)
        row = self.ctx.tokens.get(cid, token) or {}
        if px is None and s and row.get("peg_family") == "usd_stable":
            px = 1.0
        dec = row.get("decimals")
        return None if px is None or dec is None else raw / 10 ** dec * px

    def weight(self, cid, m, d, fam_ids):
        """Share of the lending vault's attributed debt backed by family collateral on the latest distribution day at or before d."""
        key = ("w", cid, m)
        if key not in self.pos:
            rows = defaultdict(dict)
            with open(DATA / f"vaults/euler_v2/{cid}/borrow_distribution.csv") as fh:
                for r in csv.DictReader(fh):
                    if r["vault"] == m:
                        day = rows[e2.day_of(r["day"])]
                        if r["collateral_vault"] and int(r["attributed"]) > 0:
                            day[r["collateral_vault"]] = int(r["attributed"])
            self.pos[key] = (sorted(rows), rows)
        days, rows = self.pos[key]
        j = bisect.bisect_right(days, d) - 1
        if j < 0:
            return 1.0
        att = rows[days[j]]
        if not att:
            return 0.0
        fam = family_collateral_vaults(self.ctx, cid, m, fam_ids)
        return sum(a for cv, a in att.items() if cv in fam) / sum(att.values())

    def row(self, e, r):
        """(group, exposure_t0_usd, exit_lead_days) of one row."""
        proto, cid, vault, kind = r["protocol"], r["chain_id"], r["vault"], r["kind"]
        t0 = e5_onset(e)
        t0d = t0 // DAY
        fam_ids = event_families(e)
        ms = event_pairs(e)[(proto, cid)]
        info = e2.market_info_cached(proto, cid)
        asset = info[vault]["loan"] if kind == "euler_lending" else \
            e2.registry_vaults()[(cid, vault)]["asset"]
        guards = {m: self.guard(proto, cid, m, t0) for m in ms}
        sh = {} if kind == "euler_lending" else self.shares(proto, cid, vault)
        held = [vault] if kind == "euler_lending" else [m for m in ms if m in sh]

        def w(m, d):
            return self.weight(cid, m, d, fam_ids) if proto == "euler_v2" else 1.0

        def x_at(ts, d):
            tot = 0.0
            for m in held:
                aps, i = self.aps_at(proto, cid, m, ts, d)
                if aps is None:
                    continue
                if kind == "euler_lending":
                    amount = self.panel(proto, cid, m)[1][i]
                else:
                    rows_m = sh[m]
                    k = bisect.bisect_right([x[0] for x in rows_m], ts)
                    s = rows_m[k - 1][1] if k > 0 else 0
                    if s <= 0 or self.queue_out(proto, cid, vault, m, ts):
                        continue
                    amount = s * aps
                g = self.capped(guards[m], ts, aps)
                tot += amount * g * w(m, d)
            return tot
        path = [(d * DAY + DAY - 1, x_at(d * DAY + DAY - 1, d), d)
                for d in range(t0d - LOOKBACK_D, t0d)]
        xt0 = x_at(t0, t0d)
        path.append((t0, xt0, t0d))
        peak = max(v for _t, v, _d in path)
        held_usd = max(self.usd(cid, asset, v, d) or 0.0 for _t, v, d in path)
        if held_usd < MATERIAL_USD:
            return "never_entered", self.usd(cid, asset, xt0, t0d), None
        if not (peak > 0 and xt0 < EXIT_FRACTION * peak):
            return "present_at_t0", self.usd(cid, asset, xt0, t0d), None
        above = [i for i, (_t, v, _d) in enumerate(path) if v >= LEAD_FRACTION * peak]
        lead = round((t0 - path[above[-1] + 1][0]) / DAY, 2) if above and above[-1] + 1 < len(path) \
            else None
        return "exited_before_t0", self.usd(cid, asset, xt0, t0d), lead


def archive_check(ctx, sfp):
    """Morpho and Moolah market() at the valuation block per chain against the panel state at T (the panel's last row at or before the block's time)."""
    from common import evmabi, multicall
    pt = json.loads(PT_FILE.read_text())
    blocks = {int(c): b for c, b in pt["valuation"]["blocks"].items()}
    dep = json.loads((ROOT / "code/config/deployments.json").read_text())["protocols"]
    todo = defaultdict(list)
    for ev, proto, cid, m in sfp["markets"]:
        if proto in ("morpho", "moolah") and cid in blocks:
            todo[(proto, cid)].append((ev, m))
    out, n0 = [], rpc.calls_made()
    for (proto, cid), items in sorted(todo.items()):
        core = dep[proto]["chains"][str(cid)][proto]
        chain = e2.CHAINS[str(cid)]["drpc"]
        blk = rpc.get_block(chain, blocks[cid])
        bts = int(blk["timestamp"], 16)
        mk = sorted({m for _ev, m in items})
        calls = [(core, evmabi.encode_call("market(bytes32)", ["bytes32"], [bytes.fromhex(m[2:])]))
                 for m in mk]
        res = multicall.aggregate3(chain, calls, block=blocks[cid])
        for m, (ok, ret) in zip(mk, res):
            st = ctx.pstate.at(proto, cid, m, bts)
            row = {"protocol": proto, "chain_id": cid, "market": m, "block": blocks[cid],
                   "block_utc": utc(bts), "ok": ok}
            if ok and ret not in ("0x", ""):
                v = evmabi.decode(["uint128"] * 6, ret)
                row.update({"chain_supply": v[0], "chain_borrow": v[2], "chain_shares": v[1],
                            "panel_supply": st["supply"] if st else None,
                            "panel_borrow": st["borrow"] if st else None,
                            "panel_shares": st["shares"] if st else None})
                row["identical"] = bool(st) and (v[0], v[1], v[2]) == (st["supply"], st["shares"],
                                                                       st["borrow"])
            out.append(row)
    return out, rpc.calls_made() - n0


def stage_validate(work, n=20):
    ctx = Ctx(work)
    o = work.load("outputs.pkl")
    rp = work.load("rows.pkl")
    sfp = work.load("shortfall.pkl")
    rows = o["rows"]
    evs = {e["event_id"]: e for e in ctx.events}
    rng = random.Random(SEED)
    held = [r for r in rows if r["group"] in ("exited_before_t0", "present_at_t0")]
    nev_s = [r for r in rows if r["group"] == "never_entered" and r["has_series"]]
    nev_e3 = [r for r in rows if r["group"] == "never_entered" and not r["has_series"]]
    sample = rng.sample(held, min(14, len(held))) + rng.sample(nev_s, min(3, len(nev_s)))
    sample += rng.sample(nev_e3, n - len(sample))
    ind = Indep(ctx)
    checks = []
    for r in sample:
        e = evs[r["event_id"]]
        if r["has_series"]:
            g, x, lead = ind.row(e, r)
            ok = (g == r["group"] and abs((x or 0.0) - (r["exposure_t0_usd"] or 0.0))
                  <= 1e-9 * max(1.0, abs(x or 0.0)) and lead == r.get("exit_lead_days"))
            how = "positions file + panel CSVs + actions queue + distribution + prices (pandas)"
        else:
            ms = event_pairs(e)[(r["protocol"], r["chain_id"])]
            p = DATA / f"vaults/{r['protocol']}/{r['chain_id']}/positions/{r['vault']}.csv"
            holds = p.exists() and any(m.encode() in p.read_bytes() for m in ms)
            import pandas as pd
            src = "lending_daily.csv" if r["kind"] == "euler_lending" else "vault_daily.csv"
            df = pd.read_csv(DERIVED / f"aum/{src}", usecols=["chain_id", "vault", "date", "aum_usd"],
                             dtype={"vault": str, "date": str})
            day = day_str(e5_onset(e) // DAY - 1)
            v = df[(df["chain_id"] == r["chain_id"]) & (df["vault"] == r["vault"]) &
                   (df["date"] == day)]["aum_usd"]
            val = float(v.iloc[0]) if len(v) else None
            g, x, lead = ("never_entered" if not holds else "holds the markets"), 0.0, None
            ok = not holds and val is not None and val >= MATERIAL_USD and \
                abs(val - r["value_t0_usd"]) < 1e-6
            how = f"no affected market id in the position file; E3 {src} value {val} on {day}"
        checks.append({"event_id": r["event_id"], "protocol": r["protocol"],
                       "chain_id": r["chain_id"], "vault": r["vault"], "kind": r["kind"],
                       "ours": {"group": r["group"], "exposure_t0_usd": r["exposure_t0_usd"],
                                "exit_lead_days": r.get("exit_lead_days")},
                       "independent": {"group": g, "exposure_t0_usd": x, "exit_lead_days": lead},
                       "identical": bool(ok), "how": how})
    comp = rp["compare"]
    cons = {"e2_rows": len(comp), "found": sum(1 for c in comp if c["found"]),
            "missing": [c for c in comp if not c["found"]]}
    expo_eq = locked_eq = rule_eq = 0
    diffs, group_changes = [], Counter()
    for c in comp:
        if not c["found"]:
            continue
        a, b, rule = c["e2"], c["e5"], c["e2_rule"]
        xe = abs((a["exposure_t0_usd"] or 0.0) - (rule["exposure_t0_usd"] or 0.0)) <= \
            1e-6 * max(1.0, abs(a["exposure_t0_usd"] or 0.0))
        le = all(a[k] == rule[k] for k in ("days_locked", "days_locked_1pct", "days_locked_10pct"))
        re_ = rule["group"] == a["group"] and rule["exit_lead_days"] == a["exit_lead_days"]
        expo_eq += xe
        locked_eq += le
        rule_eq += re_
        if not (xe and le and re_):
            diffs.append(c)
        if b["group"] != a["group"]:
            why = ("E5 dates the event from its depeg episode" if c["t0_moved"] else
                   "E5 has no entered-after group: no exposure in the 180 days before t0"
                   if a["group"] == "entered_after_t0" else
                   "family-weighted exposure below 10k USD (Euler debt not backed by the family)"
                   if b["group"] == "never_entered" else
                   "peak over 180 days instead of 90, or family weighting")
            group_changes[(a["group"], b["group"], why)] += 1
    cons.update({"exposure_t0_equal": expo_eq, "days_locked_equal": locked_eq,
                 "e2_rule_group_and_lead_equal": rule_eq, "differences": diffs,
                 "group_changes": [{"e2": k[0], "e5": k[1], "reason": k[2], "rows": v}
                                   for k, v in sorted(group_changes.items())],
                 "lead_changes": [{"event_id": c["event_id"], "vault": c["vault"],
                                   "e2": c["e2"]["exit_lead_days"], "e5": c["e5"]["exit_lead_days"]}
                                  for c in comp if c["found"] and c["e2"]["group"] == c["e5"]["group"]
                                  == "exited_before_t0" and c["e2"]["exit_lead_days"]
                                  != c["e5"]["exit_lead_days"]]})
    stream = next(e for e in ctx.events if e["collateral_family"]["label"].upper() == "XUSD"
                  and e["type"] == "depeg_loss")
    mk = [r for (ev, *_k), r in sfp["markets"].items() if ev == stream["event_id"]]
    srows = [r for r in rows if r["event_id"] == stream["event_id"]]
    by_market_vaults = defaultdict(float)
    for r in srows:
        if r["kind"] == "euler_lending":
            continue
        for m, x in (r.get("loss_usd_by_market") or {}).items():
            by_market_vaults[m] += x
    markets = []
    for r in mk:
        pp = ref_pick(r)
        own_total = (r.get("principal_shortfall_usd") or 0.0) + (r.get("bad_debt_usd") or 0.0)
        lend = next((x for x in srows if x["kind"] == "euler_lending" and x["vault"] == r["market"]),
                    None)
        markets.append({
            "protocol": r["protocol"], "chain_id": r["chain_id"], "market": r["market"],
            "loan": r["loan_symbol"], "reference_utc": r["reference_utc"],
            "shortfall_ratio": r.get("shortfall_ratio"),
            "principal_shortfall_usd": r.get("principal_shortfall_usd"),
            "bad_debt_usd": r.get("bad_debt_usd"), "market_loss_usd": own_total,
            "recovery": [{k: c[k] for k in ("symbol", "units", "recovery_price_usd",
                                             "recovery_source")} for c in r.get("collateral") or []],
            "curated_allocator_vaults_loss_usd": by_market_vaults.get(r["market"], 0.0),
            "lending_vault_direct_depositors_loss_usd": lend["loss_usd_counted"] if lend else None,
            **pp,
            "difference_to_ref_usd": own_total - (pp.get("ref_market_total_usd") or 0.0)
            if pp else None})
    ref_sum = sum(m.get("ref_market_total_usd") or 0.0 for m in markets)
    own_sum = sum(m["market_loss_usd"] for m in markets)
    vault_sum = sum(r["loss_usd_counted"] or 0.0 for r in srows)
    third = sum(r["loss_usd_counted"] or 0.0 for r in srows if r["third_party"])
    arch, calls = archive_check(ctx, sfp)
    val = {
        "exposure_rows": {"n": len(checks), "identical": sum(c["identical"] for c in checks),
                          "rows": checks},
        "stream_realized_loss": {
            "event_id": stream["event_id"], "valuation_utc": sfp["val_utc"], "markets": markets,
            "market_loss_usd_sum": own_sum, "ref_sum_same_markets_usd": ref_sum,
            "vault_rows_loss_usd_counted_sum": vault_sum,
            "third_party_vault_rows_loss_usd_counted_sum": third,
            "ref_figure": "ref_market_total_usd: the depositor loss the input tables give for "
                            "the market (R23 additions over its parts: family coin and PT "
                            "collateral; Morpho: principal shortfall + bad debt written off)",
            "note": "market_loss_usd is the loss of all lenders of the market; the vault rows sum "
                    "the curated vaults' shares (Euler lending vaults net of the listed Earn "
                    "vaults, i.e. their direct depositors); the rest is uncurated lenders of the "
                    "markets"},
        "e2_consistency": cons,
        "archive_market_state_at_T": {"rows": arch, "rpc_calls": calls,
                                      "identical": sum(1 for a in arch if a.get("identical"))}}
    io_utils.write_json(f"{OUT}/validation.json", val, GENERATOR, {
        "what": "independent re-derivations: 20 random rows, the Stream xUSD realized loss against "
                "the input tables, consistency with E2, Morpho and Moolah market state at T",
        "rpc_calls": calls, "seed": SEED})
    return {"rows_identical": f"{sum(c['identical'] for c in checks)}/{len(checks)}",
            "e2_found": f"{cons['found']}/{cons['e2_rows']}", "exposure_equal": expo_eq,
            "locked_equal": locked_eq, "e2_rule_equal": rule_eq,
            "stream_market_loss": own_sum, "stream_ref": ref_sum,
            "archive_identical": f"{val['archive_market_state_at_T']['identical']}/{len(arch)}",
            "rpc_calls": calls}


STAGES = ("heads", "series", "shortfall", "rows", "outputs", "figures", "validate")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--stage", choices=STAGES + ("all",), default="all")
    ap.add_argument("--work", default=os.environ.get("E5_WORK", "/tmp/e5_work"))
    ap.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 4))
    ap.add_argument("--e4-dir", default=str(DERIVED / "metrics/e4"),
                    help="E4 outputs to take the curator AUM rank from (curator_scorecard.csv)")
    a = ap.parse_args()
    E4_DIR[0] = pathlib.Path(a.e4_dir)
    work = e2.Work(a.work)
    stages = STAGES if a.stage == "all" else (a.stage,)
    log = work.load("run_log.pkl") if work.has("run_log.pkl") else []
    for st in stages:
        t1 = time.time()
        if st == "heads":
            res = stage_heads(work)
        elif st == "series":
            res = stage_series(work, a.workers)
        elif st == "shortfall":
            res = stage_shortfall(work)
        elif st == "rows":
            res = stage_rows(work)
        elif st == "outputs":
            res = stage_outputs(work)
        elif st == "figures":
            res = stage_figures(work)
        else:
            res = stage_validate(work)
        log.append({"stage": st, "seconds": round(time.time() - t1, 1), "result": res,
                    "rpc_calls_process": rpc.calls_made(), "utc": e2.now_utc()})
        work.save("run_log.pkl", log)
        print(json.dumps({"stage": st, "seconds": round(time.time() - t1, 1), "result": res},
                         default=str), flush=True)


if __name__ == "__main__":
    main()
