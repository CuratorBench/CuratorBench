# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26"]
# ///
"""E2: the stress-event set over the 14 protocol-chain pairs."""
import argparse
import bisect
import csv
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import pickle
import random
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import requests

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "code"))
sys.path.insert(0, str(ROOT / "code" / "data"))
from common import evmabi, inputs, io_utils, multicall, outroot, rpc
import prices as peg_rules

GENERATOR = "code/analysis/e2_events.py"
DATA = ROOT / "data"
OUT = "events"
DERIVED = outroot.OUT_DATA
INPUTS = inputs.INPUTS
CENSUS_EVENTS = INPUTS / "hit_events.jsonl"
CENSUS_UNIVERSE = INPUTS / "token_universe.jsonl"
CENSUS_S1_TOKENS = INPUTS / "token_wrappers.jsonl"
CENSUS_RULES = INPUTS / "hit_rules.md"
CENSUS_RULE_VERSION = ("hit_rules.md R19 (2026-09-24): hit = market x "
                       "pinned window with a depeg window and I1 in "
                       "[window start - 30 d, window end]; R4 filter 0.97; "
                       "7-day family merge")
CHAINS = json.loads((ROOT / "code/config/chains.json").read_text())

PAIRS = [("morpho", 1), ("morpho", 8453), ("morpho", 4663),
         ("morpho", 42161), ("morpho", 747474), ("morpho", 999),
         ("euler_v2", 1), ("euler_v2", 9745), ("euler_v2", 59144),
         ("moolah", 56),
         ("silo_v2", 43114), ("silo_v2", 146), ("silo_v2", 1),
         ("silo_v2", 42161)]
PRICED_CHAINS = {1, 8453, 56, 9745, 146, 42161, 43114}

DAY = 86400
YEAR = 365 * DAY
THRESHOLDS = (0.98, 0.99, 0.995)
DURS_H = (48, 72, 96)
MAIN = (0.99, 72)
MIN_PEAK_USD = 1_000_000.0
L_LOCK = 0.05
L_SENS = (0.01, 0.10)
MATERIAL_USD = 10_000.0
MERGE_GAP = 7 * DAY
MERGE_HORIZON = 30 * DAY
EPISODE_GAP = 180 * DAY
SELF_DEPOSIT_SHARE = 0.9
MAIN_TEXT_ROWS = 15
SELF_DEALING = {
    (8453, "0x6708e21113922ed588bbccaa5ef756becbb2a7d1"): {
        "source": "data/resume/king-rss.md",
        "vaults": {"0xf80c0529bd94c773844e459853cd91b9263dd525",
                   "0x61bfd6f7df1f72427f472144d043c25d742d145e",
                   "0xfdd5a1d4823411809d6ac735991b3a015e5aaab5",
                   "0x0d96ba80502eb8a08a6d3bd4680134b20c229532",
                   "0xc91f3bc556001ef7acfcb869ec0fc29ac780c35c",
                   "0x35a00f116536c13a63273513990e4e496a15ddb2",
                   "0xb96bcffbb458581a3af7fed3150b7cd4b233a7b9"},
        "markets": {"0x41c08085ddcfd1dc1c5eb82d7dc031593d1a1a831958380e8b60469c45bf7d88":
                    "RSS (elephanToken), Morpho Base, market 0x41c0…7d88, 2026-08-20: "
                    "flash-loan-funded self-loop; depositor, borrower, market creator, oracle "
                    "deployer and collateral holder are one controller; no third-party funds; "
                    "excluded."}},
}
SELF_DEALING_VAULTS = {(key[0], v): key for key, info in SELF_DEALING.items() for v in info["vaults"]}
GUARD_MAX_APR = 1.0
ISSUERS = {
    "BUSD0": ("usual", "USD0++ / bUSD0, Usual's bond token"),
    "USD0++": ("usual", "Usual's bond token"),
    "USD0": ("usual", "Usual's stablecoin"),
    "USUAL": ("usual", "Usual's governance token"),
    "USUALX": ("usual", "staked USUAL"),
    "SLISBNB": ("lista-dao", "slisBNB, Lista DAO's liquid staked BNB (census row synclub-staked-bnb)"),
    "LISUSD": ("lista-dao", "lisUSD, Lista DAO's stablecoin"),
    "YNRWAX": ("yieldnest", "YieldNest RWA vault token"),
    "YNETHX": ("yieldnest", "YieldNest ETH vault token"),
    "YNETH": ("yieldnest", "YieldNest restaked ETH"),
    "ENZOBTC": ("lorenzo-protocol", "Lorenzo wrapped BTC"),
    "WELL": ("moonwell", "Moonwell's governance token"),
    "WNLP-USDT": ("native", "Wrapped NLP: USDT, Native's liquidity-pool token"),
    "RUSD": ("reservoir-labs", "Reservoir stablecoin"),
    "SRUSD": ("reservoir-labs", "staked rUSD"),
    "WSRUSD": ("reservoir-labs", "wrapped staked rUSD"),
    "IUSD": ("infinifi", "InfiniFi stablecoin"),
    "SIUSD": ("infinifi", "staked iUSD"),
    "FEUSD": ("felix", "Felix stablecoin"),
}
EXPOSURE_LOOKBACK_D = 90
EXIT_FRACTION = 0.10
LEAD_FRACTION = 0.50
I1_RATIO = 1.08
DEPEG_LINE = 0.97
PRICED_LOSS_MIN_USD = 100_000.0
RECOVERY_LINE = 0.99
RECOVERY_DAYS = 14
RECOVERY_SENS = (7, 30)
R19_LOOKBACK = 30 * DAY
CENSUS_PAD = 3 * DAY
MOOLAH_GLOBAL_CAP_APR = 0.30
I3_FRACTION = 0.5
MORPHO_CAP_APR = 8.0
FALLBACK_I3_APR = 1.0
MAX_PRICE_CARRY_D = 3
ZERO = "0x" + "0" * 40
SEED = 20261008


def now_utc():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def utc(ts):
    if ts is None:
        return None
    return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime(
        "%Y-%m-%d %H:%M")


def day_str(d):
    return dt.datetime.fromtimestamp(int(d) * DAY, dt.timezone.utc).date().isoformat()


def day_of(datestr):
    return int(dt.datetime.fromisoformat(datestr).replace(
        tzinfo=dt.timezone.utc).timestamp()) // DAY


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def jsonl(path):
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


class Work:
    """Pickled intermediate results of each stage, outside the repository."""

    def __init__(self, root):
        self.root = pathlib.Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, name):
        return self.root / name

    def save(self, name, obj):
        p = self.path(name)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        with open(tmp, "wb") as fh:
            pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, p)

    def load(self, name):
        with open(self.path(name), "rb") as fh:
            return pickle.load(fh)

    def has(self, name):
        return self.path(name).exists()


def panel_dir(proto, cid):
    return DATA / "panel" / proto / str(cid)


def panel_files(proto, cid):
    """{market id: csv path}."""
    d = panel_dir(proto, cid)
    return {fn[:-4]: d / fn for fn in sorted(os.listdir(d))
            if fn.startswith("0x") and fn.endswith(".csv")}


def panel_rows(proto, path):
    """Yield (block, log_index, ts, supply, borrow, shares, apr, event) for every state row of one market, in file order."""
    with open(path, newline="") as fh:
        rd = csv.reader(fh)
        head = next(rd)
        ix = {k: i for i, k in enumerate(head)}
        if proto in ("morpho", "moolah"):
            b_, l_, t_ = ix["block"], ix["log_index"], ix["timestamp"]
            s_, sh_, bo_, r_ = (ix["total_supply_assets"], ix["total_supply_shares"],
                                ix["total_borrow_assets"], ix["borrow_rate"])
            for row in rd:
                rate = row[r_]
                yield (int(row[b_]), int(row[l_]), int(row[t_]), int(row[s_]),
                       int(row[bo_]), int(row[sh_]),
                       int(rate) * YEAR / 1e18 if rate else None, row[ix["kind"]])
        elif proto == "euler_v2":
            b_, l_, t_ = ix["block"], ix["log_index"], ix["timestamp"]
            c_, bo_, sh_, r_ = ix["cash"], ix["totalBorrows"], ix["totalShares"], ix["interestRate"]
            for row in rd:
                borrow = int(row[bo_])
                yield (int(row[b_]), int(row[l_]), int(row[t_]), int(row[c_]) + borrow,
                       borrow, int(row[sh_]), int(row[r_]) * YEAR / 1e27, "status")
        else:
            b_, l_, t_ = ix["block"], ix["log_index"], ix["ts"]
            e_, a_, c_, d_, r_ = (ix["event"], ix["amount"], ix["collateral_assets"],
                                  ix["debt_assets"], ix["borrow_apr_realized"])
            shares, prev_c = 0, 0
            for row in rd:
                ev = row[e_]
                if ev in ("Deposit", "Withdraw"):
                    amt = int(row[a_])
                    num = shares + 1000
                    den = (prev_c + 1) if shares > 0 else 1
                    if ev == "Deposit":
                        shares += amt * num // den
                    else:
                        shares = max(0, shares - (-(-amt * num // den)))
                coll = int(row[c_])
                prev_c = coll
                rate = row[r_]
                yield (int(row[b_]), int(row[l_]), int(row[t_]), coll, int(row[d_]),
                       shares, float(rate) if rate else None, ev)


def util_of(supply, borrow):
    if supply > 0:
        return borrow / supply
    return math.inf if borrow > 0 else 0.0


def aps_of(proto, supply, shares):
    """Assets per share with each protocol's virtual offsets."""
    if proto in ("morpho", "moolah"):
        return (supply + 1) / (shares + 10 ** 6)
    if proto == "euler_v2":
        return (supply + 10 ** 6) / (shares + 10 ** 6)
    return (supply + 1) / (shares + 1000) if shares > 0 else 1 / 1000


def liquidity_of(supply, borrow):
    return max(supply - borrow, 0)


def _scan_market(job):
    """Runs at every threshold (>= 48 h, or open at the head), end-of-day state and rate windows for one market file."""
    proto, cid, market, path, i3_rule = job
    runs = {t: [] for t in THRESHOLDS}
    open_run = {t: None for t in THRESHOLDS}
    daily = []
    rate_windows, rate_open = [], None
    n = 0
    first_ts = last_ts = None
    last = None
    max_apr = 0.0
    for block, li, ts, supply, borrow, shares, apr, _ev in panel_rows(proto, path):
        n += 1
        if first_ts is None:
            first_ts = ts
        d = ts // DAY
        if last is not None and last[2] // DAY != d:
            daily.append((last[2] // DAY, last[3], last[4], last[5], last[0], last[2]))
        last = (block, li, ts, supply, borrow, shares)
        last_ts = ts
        u = util_of(supply, borrow)
        for t in THRESHOLDS:
            r = open_run[t]
            if u >= t:
                if r is None:
                    open_run[t] = r = {
                        "start_block": block, "start_li": li, "start_ts": ts,
                        "supply_start": supply, "borrow_start": borrow,
                        "shares_start": shares, "min_util": u, "max_util": u,
                        "peak_supply": supply, "peak_ts": ts, "day_max": {d: supply}}
                else:
                    r["min_util"] = min(r["min_util"], u)
                    r["max_util"] = max(r["max_util"], u)
                    if supply > r["peak_supply"]:
                        r["peak_supply"], r["peak_ts"] = supply, ts
                    if supply > r["day_max"].get(d, -1):
                        r["day_max"][d] = supply
            elif r is not None:
                r.update(end_block=block, end_li=li, end_ts=ts, ongoing=False)
                if ts - r["start_ts"] >= min(DURS_H) * 3600:
                    runs[t].append(r)
                open_run[t] = None
        if apr is not None:
            max_apr = max(max_apr, apr)
            thr = i3_rule(ts) if i3_rule else None
            if thr is not None and apr >= thr:
                if rate_open is None:
                    rate_open = {"start_ts": ts, "start_block": block, "peak_apr": apr,
                                 "threshold_apr": thr}
                rate_open["peak_apr"] = max(rate_open["peak_apr"], apr)
                rate_open["last_ts"] = ts
            elif rate_open is not None:
                rate_open.update(end_ts=ts, end_block=block)
                rate_windows.append(rate_open)
                rate_open = None
    if last is not None:
        daily.append((last[2] // DAY, last[3], last[4], last[5], last[0], last[2]))
    for t in THRESHOLDS:
        r = open_run[t]
        if r is not None:
            r.update(end_block=None, end_li=None, end_ts=None, ongoing=True)
            runs[t].append(r)
    if rate_open is not None:
        rate_open.update(end_ts=None, end_block=None)
        rate_windows.append(rate_open)
    return market, {"rows": n, "first_ts": first_ts, "last_ts": last_ts,
                    "runs": runs, "daily": daily, "rate_windows": rate_windows,
                    "max_apr": max_apr,
                    "last": last}


class I3Rule:
    """Census I3 threshold, APR >= I3_FRACTION x the rate-model cap."""

    def __init__(self, proto, caps=None):
        self.proto = proto
        self.caps = caps or []

    def __call__(self, ts):
        if self.proto == "morpho":
            return I3_FRACTION * MORPHO_CAP_APR
        if self.proto == "moolah":
            cap = MOOLAH_GLOBAL_CAP_APR
            for t, c in self.caps:
                if t <= ts:
                    cap = c if c > 0 else MOOLAH_GLOBAL_CAP_APR
            return I3_FRACTION * cap
        return FALLBACK_I3_APR


def moolah_caps():
    caps = defaultdict(list)
    with open(panel_dir("moolah", 56) / "irm_params.csv") as fh:
        for r in csv.DictReader(fh):
            if r["param"] == "rate_cap" and r["market_id"]:
                caps[r["market_id"]].append(
                    (int(r["timestamp"]), int(r["new"]) * YEAR / 1e18))
    return {m: sorted(v) for m, v in caps.items()}


def panel_head(proto, cid):
    """Last block covered by the panel (its scan bound is exclusive)."""
    d = panel_dir(proto, cid)
    if proto in ("morpho", "moolah"):
        return json.loads((d / "_checkpoint.json").read_text())["to_block"] - 1
    if proto == "euler_v2":
        return json.loads((d / "checkpoint.json").read_text())["to_block"] - 1
    return json.loads((d / "_checkpoint.json").read_text())["head"] - 1


def stage_scan(work, workers):
    t0 = time.time()
    heads = {}
    for proto, cid in PAIRS:
        blk = panel_head(proto, cid)
        b = rpc.get_block(CHAINS[str(cid)]["drpc"], blk)
        heads[f"{proto}:{cid}"] = {"block": blk, "ts": int(b["timestamp"], 16)}
    work.save("heads.pkl", heads)
    caps = moolah_caps()
    summary = {}
    for proto, cid in PAIRS:
        files = panel_files(proto, cid)
        jobs = [(proto, cid, m, p, I3Rule(proto, caps.get(m)))
                for m, p in files.items()]
        out = {}
        with ProcessPoolExecutor(workers) as ex:
            for market, res in ex.map(_scan_market, jobs, chunksize=8):
                out[market] = res
        work.save(f"scan/{proto}_{cid}.pkl", out)
        summary[f"{proto}:{cid}"] = {
            "markets": len(out), "rows": sum(r["rows"] for r in out.values()),
            "runs_ge48h": {str(t): sum(len(r["runs"][t]) for r in out.values())
                           for t in THRESHOLDS}}
        print(f"scan {proto}:{cid} {summary[f'{proto}:{cid}']}", flush=True)
    summary["seconds"] = round(time.time() - t0, 1)
    work.save("scan_summary.pkl", summary)
    return summary


def market_info(proto, cid):
    """{market: {loan, collaterals [(collateral_token, collateral_vault)], oracle rows}} from the E1 registries."""
    info = {}
    if proto in ("morpho", "moolah"):
        for r in jsonl(DATA / f"registry/markets/{proto}/{cid}.jsonl"):
            info[r["market_id"]] = {"loan": r["loan_token"], "created_block": r["created_block"],
                                    "collaterals": [(r["collateral_token"], None)],
                                    "lltv": r.get("lltv"), "oracle_address": r.get("oracle")}
        orc = DATA / f"registry/oracles/{proto}/{cid}.jsonl"
        if orc.exists():
            for r in jsonl(orc):
                if r["market_id"] in info:
                    info[r["market_id"]]["oracle"] = {
                        k: r.get(k) for k in ("type", "subtype", "blind_to")}
    elif proto == "euler_v2":
        rows = jsonl(DATA / f"registry/markets/euler_v2/{cid}.jsonl")
        asset = {r["vault"]: r["asset"] for r in rows if r["type"] == "vault"}
        coll = defaultdict(dict)
        for r in rows:
            if r["type"] == "ltv":
                coll[r["vault"]][r["collateral_vault"]] = asset.get(r["collateral_vault"])
        for v, a in asset.items():
            info[v] = {"loan": a, "collaterals": [(t, cv) for cv, t in coll[v].items()],
                       "oracle_by_collateral": {}}
        orc = DATA / f"registry/oracles/euler_v2/{cid}.jsonl"
        if orc.exists():
            for r in jsonl(orc):
                if r["vault"] in info:
                    info[r["vault"]]["oracle_by_collateral"][r["collateral_vault"]] = {
                        k: r.get(k) for k in ("type", "subtype", "blind_to")}
    else:
        cfg = {r["silo"]: r for r in jsonl(panel_dir(proto, cid) / "configs.jsonl")}
        orc = {}
        p = DATA / f"registry/oracles/silo_v2/{cid}.jsonl"
        if p.exists():
            for r in jsonl(p):
                so = r.get("solvency_oracle") or {}
                orc[r["silo"]] = {"type": so.get("type", r.get("type")),
                                  "subtype": so.get("subtype"),
                                  "blind_to": so.get("blind_to")}
        for s, r in cfg.items():
            pair = cfg.get(r["paired_silo"], {})
            info[s] = {"loan": r["token"], "decimals": r.get("decimals"),
                       "symbol": r.get("symbol"),
                       "collaterals": [(pair.get("token"), r["paired_silo"])],
                       "silo_config": r["silo_config"],
                       "oracle": orc.get(r["paired_silo"])}
    return info


def euler_distribution(cid):
    """{lending vault: {day."""
    out = defaultdict(lambda: defaultdict(dict))
    p = DATA / f"vaults/euler_v2/{cid}/borrow_distribution.csv"
    with open(p) as fh:
        for r in csv.DictReader(fh):
            day = out[r["vault"]][day_of(r["day"])]
            if r["collateral_vault"] and int(r["attributed"]) > 0:
                day[r["collateral_vault"]] = int(r["attributed"])
    return out


def dominant_collateral(info_row, dist, day):
    """Collateral of a market at a day."""
    cols = info_row["collaterals"]
    if len(cols) == 1:
        return cols[0], "single collateral"
    if dist:
        days = [d for d in dist if d <= day and dist[d]]
        if days:
            att = dist[max(days)]
            cv = max(att, key=att.get)
            tok = dict((v, t) for t, v in cols).get(cv)
            share = att[cv] / sum(att.values())
            return (tok, cv), f"largest attributed debt on {day_str(max(days))} ({share:.0%})"
    return (None, None), "no attribution"


class Tokens:
    """Symbol, decimals and peg family of every token the stage touches."""

    def __init__(self, rows):
        self.rows = rows

    def get(self, cid, token):
        return self.rows.get((int(cid), token))

    def symbol(self, cid, token):
        r = self.get(cid, token)
        return (r or {}).get("symbol")

    def decimals(self, cid, token):
        r = self.get(cid, token)
        return (r or {}).get("decimals")


def collect_tokens(work, needed):
    """Build Tokens for `needed` {(cid, token)}; RPC only for gaps."""
    rows = {}
    for r in jsonl(DATA / "prices/tokens.jsonl"):
        dec, note = r["decimals"], None
        if r.get("defillama_decimals") is not None and r["defillama_decimals"] != dec:
            dec, note = r["defillama_decimals"], f"decimals() {r['decimals']}, DefiLlama {dec}"
        rows[(r["chain_id"], r["token"])] = {
            "symbol": r["symbol"], "decimals": dec, "decimals_note": note,
            "peg_family": r["peg_family"], "reference": r["reference"],
            "peg_rule": r["peg_rule"], "priced_days": r["priced_days"],
            "source": "data/prices/tokens.jsonl"}
    local_sym, local_dec = {}, {}
    for proto, cid in PAIRS:
        if proto == "silo_v2":
            for r in jsonl(panel_dir(proto, cid) / "configs.jsonl"):
                local_sym[(cid, r["token"])] = r.get("symbol")
                local_dec[(cid, r["token"])] = r.get("decimals")
        if proto == "euler_v2":
            meta = json.loads((DATA / f"vaults/euler_v2/{cid}/asset_metadata.json").read_text())
            for v in meta.values():
                local_dec[(cid, v["asset"])] = v["decimals"]
        orc = DATA / f"registry/oracles/{proto}/{cid}.jsonl"
        if orc.exists() and proto != "silo_v2":
            for r in jsonl(orc):
                for k in ("loan", "collateral"):
                    if r.get(f"{k}_token") and r.get(f"{k}_symbol"):
                        local_sym.setdefault((cid, r[f"{k}_token"]), r[f"{k}_symbol"])
    gaps = [(cid, t) for cid, t in needed if (cid, t) not in rows and t != ZERO]
    by_chain = defaultdict(list)
    for cid, t in gaps:
        by_chain[cid].append(t)
    heads = work.load("heads.pkl")
    head_of = {cid: h["block"] for k, h in heads.items() for cid in [int(k.split(":")[1])]}
    rpc_reads = {}
    for cid, toks in by_chain.items():
        need = [t for t in toks if local_sym.get((cid, t)) is None or local_dec.get((cid, t)) is None]
        if not need:
            continue
        chain = CHAINS[str(cid)]["drpc"]
        syms = multicall.try_view(chain, need, "symbol()", ["string"], head_of[cid])
        decs = multicall.try_view(chain, need, "decimals()", ["uint8"], head_of[cid])
        for t, s, d in zip(need, syms, decs):
            rpc_reads[(cid, t)] = (s[0] if s else None, d[0] if d else None)
    for cid, t in gaps:
        s = local_sym.get((cid, t)) or rpc_reads.get((cid, t), (None, None))[0]
        d = local_dec.get((cid, t))
        if d is None:
            d = rpc_reads.get((cid, t), (None, None))[1]
        fam, ref, rule = peg_rules.classify({"chain_id": cid, "token": t, "symbol": s or ""})
        rows[(cid, t)] = {"symbol": s, "decimals": d, "peg_family": fam, "reference": ref,
                          "peg_rule": rule, "priced_days": None,
                          "source": "local registries" if (cid, t) not in rpc_reads
                          else "symbol()/decimals() at the panel head"}
    return Tokens(rows)


class Llama:
    """DefiLlama chart endpoint, at most four request starts per second."""

    API = "https://coins.llama.fi"

    def __init__(self):
        self.requests = 0
        self.next_start = 0.0

    def chart(self, coins, start, span):
        for attempt in range(6):
            time.sleep(max(0.0, self.next_start - time.monotonic()))
            self.next_start = time.monotonic() + 0.26
            self.requests += 1
            try:
                resp = requests.get(f"{self.API}/chart/{','.join(coins)}", timeout=60,
                                    params={"start": start, "span": span, "period": "1d",
                                            "searchWidth": "12h"})
                if resp.ok:
                    return resp.json().get("coins", {})
                if resp.status_code not in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"DefiLlama HTTP {resp.status_code}")
            except (requests.RequestException, ValueError):
                pass
            time.sleep(2 ** attempt)
        raise RuntimeError("DefiLlama retry budget exhausted")


PRICE_START = dt.date(2024, 1, 1)


def fetch_supplement(tokens, keys, end_day):
    """Daily closes (nearest quote to 23:59:59 UTC within 12 h, the E1 convention) for tokens data/prices/ lacks."""
    llama = Llama()
    first = int(dt.datetime.combine(PRICE_START, dt.time(23, 59, 59),
                                    dt.timezone.utc).timestamp())
    n_days = end_day - first // DAY + 1
    out = []
    keys = sorted(keys)
    for i, key in enumerate(keys):
        coins = {f"{CHAINS[str(key[0])]['defillama']}:{key[1]}": key}
        best = {}
        for off in range(0, n_days, 500):
            span = min(500, n_days - off)
            start = first + off * DAY
            got = llama.chart(list(coins), start, span)
            for coin, obj in got.items():
                if coin not in coins:
                    continue
                for pt in obj.get("prices", []):
                    ts, px = pt.get("timestamp"), pt.get("price")
                    if ts is None or not isinstance(px, (int, float)) or px < 0:
                        continue
                    k = math.floor((ts - start) / DAY + 0.5)
                    target = start + k * DAY
                    if not 0 <= k < span or abs(ts - target) > 43200:
                        continue
                    key = (coins[coin], target // DAY)
                    if key not in best or abs(ts - target) < abs(best[key][1] - target):
                        best[key] = (px, ts, pt.get("confidence"))
        for ((cid, t), d), (px, ts, conf) in sorted(best.items()):
            out.append({"date": day_str(d), "chain_id": cid, "token": t,
                        "symbol": tokens.symbol(cid, t), "price_usd": px,
                        "quote_timestamp": ts, "confidence": conf})
        if (i + 1) % 25 == 0 or i + 1 == len(keys):
            print(f"prices supplement {i + 1}/{len(keys)}", flush=True)
    return out, llama.requests


def census_pegs(tokens, fams):
    """Fill the peg family of tokens E1's rules leave unknown with the peg unit of their census family (apxUSD, wM and the like."""
    unit_to_family = {"USD": "usd_stable", "ETH": "eth", "BTC": "btc"}
    n = 0
    for (cid, tok), row in tokens.rows.items():
        if row.get("peg_family") not in (None, "unknown"):
            continue
        fam = fams.resolve(cid, tok)
        if fam.get("census_uid") and unit_to_family.get(fam.get("peg")):
            row["peg_family"] = unit_to_family[fam["peg"]]
            row["peg_rule"] = f"census family {fam['census_uid']} ({fam['rule'][:2].strip()})"
            n += 1
    return n


class Prices:
    """USD per whole token on a UTC day."""

    WETH = (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
    WBTC = (1, "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599")

    def __init__(self, tokens, wanted):
        self.tokens = tokens
        self.series = defaultdict(dict)
        wanted = set(wanted) | {self.WETH, self.WBTC}
        by_chain = defaultdict(set)
        for cid, t in wanted:
            by_chain[cid].add(t)
        for cid, toks in by_chain.items():
            p = DATA / f"prices/{cid}.csv"
            if not p.exists():
                continue
            with open(p) as fh:
                for r in csv.DictReader(fh):
                    if r["token"] in toks and r["price_usd"]:
                        self.series[(cid, r["token"])][day_of(r["date"])] = float(r["price_usd"])
        sup = DERIVED / OUT / "prices_supplement.csv"
        if sup.exists():
            with open(sup) as fh:
                for r in csv.DictReader(fh):
                    key = (int(r["chain_id"]), r["token"])
                    if key in wanted:
                        self.series[key][day_of(r["date"])] = float(r["price_usd"])
        self.sorted_days = {k: sorted(v) for k, v in self.series.items()}

    def quoted(self, cid, token):
        return bool(self.series.get((int(cid), token)))

    def raw(self, cid, token, day):
        key = (int(cid), token)
        s = self.series.get(key)
        if not s:
            return None
        if day in s:
            return s[day]
        days = self.sorted_days[key]
        i = bisect.bisect_right(days, day) - 1
        if i >= 0 and day - days[i] <= MAX_PRICE_CARRY_D:
            return s[days[i]]
        return None

    def usd(self, cid, token, day):
        """(price, source) or (None, reason)."""
        px = self.raw(cid, token, day)
        if px is not None:
            return px, "defillama"
        row = self.tokens.get(cid, token) or {}
        fam = row.get("peg_family")
        if not self.quoted(cid, token):
            return None, "never quoted"
        if fam == "usd_stable":
            return 1.0, "peg_usd"
        if fam == "eth":
            px = self.raw(*self.WETH, day)
            return (px, "peg_eth") if px else (None, "no eth reference")
        if fam == "btc":
            px = self.raw(*self.WBTC, day)
            return (px, "peg_btc") if px else (None, "no btc reference")
        return None, "unpriced"

    def value(self, cid, token, raw_amount, day):
        dec = self.tokens.decimals(cid, token)
        px, _ = self.usd(cid, token, day)
        if px is None or dec is None:
            return None
        return raw_amount / 10 ** dec * px

    def in_unit(self, cid, token, day, unit):
        """Market price of a token in a peg unit (USD, ETH or BTC)."""
        px = self.raw(cid, token, day)
        if px is None or unit not in ("USD", "ETH", "BTC"):
            return None
        if unit == "USD":
            return px
        r = self.raw(*(self.WETH if unit == "ETH" else self.WBTC), day)
        return px / r if r else None


def peg_reference(row):
    """Peg unit of a token: USD, ETH, BTC or None."""
    if not row:
        return None
    fam = row.get("peg_family")
    return {"usd_stable": "USD", "eth": "ETH", "btc": "BTC"}.get(fam)


def scan_tokens(work):
    """Every (cid, token) the later stages may price or classify."""
    needed = set()
    for proto, cid in PAIRS:
        scan = work.load(f"scan/{proto}_{cid}.pkl")
        info = market_info(proto, cid)
        for m, res in scan.items():
            if not any(res["runs"][t] for t in THRESHOLDS) and not res["rate_windows"]:
                continue
            row = info.get(m)
            if not row:
                continue
            needed.add((cid, row["loan"]))
            for tok, _ in row["collaterals"]:
                if tok:
                    needed.add((cid, tok))
        for r in jsonl(DATA / f"registry/vaults/{proto}/{cid}.jsonl"):
            needed.add((cid, r["asset"]))
    return needed


def stage_prices(work):
    needed = scan_tokens(work)
    tokens = collect_tokens(work, needed)
    have = {(r["chain_id"], r["token"]) for r in jsonl(DATA / "prices/tokens.jsonl")}
    missing = sorted(k for k in needed if k not in have and k[1] != ZERO)
    heads = work.load("heads.pkl")
    end_day = max(h["ts"] for h in heads.values()) // DAY
    p = DERIVED / OUT / "prices_supplement.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    request_key = hashlib.sha256(json.dumps([missing, end_day]).encode()).hexdigest()
    side = p.with_name(p.name + ".meta.json")
    if p.exists() and side.exists() and \
            json.loads(side.read_text()).get("request_sha256") == request_key:
        with open(p) as fh:
            rows = [dict(r, chain_id=int(r["chain_id"])) for r in csv.DictReader(fh)]
        n_req = 0
    else:
        rows, n_req = fetch_supplement(tokens, missing, end_day)
    cols = ["date", "chain_id", "token", "symbol", "price_usd", "quote_timestamp", "confidence"]
    tmp = p.with_name(p.name + ".tmp")
    with open(tmp, "w", newline="") as fh:
        w = csv.DictWriter(fh, cols)
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, p)
    quoted = {(r["chain_id"], r["token"]) for r in rows}
    io_utils._meta_sidecar(p, GENERATOR, {
        "what": "DefiLlama daily USD closes for tokens of the E2 scan that "
                "data/prices/ does not cover (chains 4663, 747474, 999, 59144 and "
                "Morpho tokens on 42161, plus a few others)",
        "date_semantics": "UTC day, nearest quote to 23:59:59 within 12 h (as data/prices/)",
        "source": "https://coins.llama.fi/chart/{chain}:{token}",
        "first_date": PRICE_START.isoformat(), "last_date": day_str(end_day),
        "tokens_requested": len(missing), "tokens_quoted": len(quoted),
        "rows": len(rows), "defillama_requests": n_req, "request_sha256": request_key})
    meta_rows = [{"chain_id": cid, "token": t, **tokens.get(cid, t)}
                 for cid, t in sorted(needed) if tokens.get(cid, t)]
    work.save("tokens.pkl", tokens.rows)
    io_utils.write_jsonl(f"{OUT}/tokens_supplement.jsonl",
                         [r for r in meta_rows if r.get("source") != "data/prices/tokens.jsonl"],
                         GENERATOR, {
                             "what": "symbol, decimals and E1 peg family of tokens E2 uses "
                                     "that data/prices/tokens.jsonl does not list",
                             "peg_rules": "code/data/prices.py classify (address then symbol rules)"})
    return {"tokens_needed": len(needed), "supplement_requested": len(missing),
            "supplement_quoted": len(quoted), "defillama_requests": n_req}


WRAPPER_RULES = ("wsteth", "susde", "savings", "wrapped_usd", "weeth", "woeth",
                 "wsuperoeth", "wrs_eth", "xpufeth", "xsolvbtc", "usd0pp")

MERGE_RULES = ("issuer_lineage", "same_issuer")
if "E2_MERGE_RULES" in os.environ:
    MERGE_RULES = tuple(x for x in os.environ["E2_MERGE_RULES"].split(",") if x)

LINEAGE_BASE = {
    "defillama:2": "USDC",
    "defillama:1": "USDT",
    "coingecko:usdt0": "USDT0",
    "coingecko:weth": "WETH",
    "coingecko:wrapped-bitcoin": "WBTC",
    "coingecko:vaultbridge-bridged-usdc-katana": "vbUSDC (Katana bridged USDC)",
}
LINEAGE_TABLE = {
    "coingecko:wrapped-steth": ("coingecko:staked-ether", "Lido wstETH wraps stETH"),
    "coingecko:wrapped-rseth": ("coingecko:kelp-dao-restaked-eth", "Kelp wrsETH wraps rsETH"),
    "coingecko:wrapped-eeth": ("coingecko:ether-fi-staked-eth", "ether.fi weETH wraps eETH"),
    "coingecko:arbitrum-bridged-wrapped-eeth": ("coingecko:ether-fi-staked-eth",
                                                "bridged ether.fi weETH, which wraps eETH"),
    "coingecko:usd0-liquid-bond": ("defillama:195", "Usual USD0++ / bUSD0 is locked USD0"),
    "s2:bUSD0": ("defillama:195", "Usual USD0++ / bUSD0 is locked USD0"),
}
FAMILY_ISSUERS = {
    "census:defillama:197": ("resolv", "USR, Resolv's stablecoin"),
    "census:s2:RLP": ("resolv", "RLP, Resolv's liquidity pool token that backs USR"),
    "symbol:MHYPER": ("midas", "mHYPER, a Midas tokenized strategy"),
    "symbol:MHYPERETH": ("midas", "mHyperETH, a Midas tokenized strategy"),
    "symbol:VBETH": ("agglayer-vault-bridge", "vbETH, Vault Bridge ETH on Katana"),
    "symbol:VBWBTC": ("agglayer-vault-bridge", "vbWBTC, Vault Bridge WBTC on Katana"),
}


class Families:
    """Collateral family of a token, decided by the first rule that applies:

    0. the token is the hit token of a census lending event: that event's
       family (census s2 identity, R16);
    1. the census price universe (s9/universe.jsonl) lists the contract:
       that row, an audit or out-of-scope row joining the in-scope row with
       the same name and symbol (census R9(3)(a));
    2. the census unwrap chain (s1/tokens/tokens.jsonl) leads to a contract
       rule 1 resolves: that row (wrappers join their underlying, R9(3)(b));
       this also wins over rule 1 when rule 1 finds only an audit row and
       the underlying's row is in scope (Sonic smsUSD -> Main Street msUSD);
    3. the same address is a census row's contract on another chain and the
       symbols agree (same-address bridged deployments): that row;
    4. a Pendle PT whose underlying symbol names exactly one token on the
       same chain takes that token's family; otherwise the underlying
       symbol after the E1 wrapper rules of code/data/prices.py
       (wstETH -> stETH, sX -> X, PT-X-date -> X), as
       "symbol:<SYMBOL>"; a symbol two census issuers share is marked
       ambiguous instead of joined.
    The peg unit is the census row's, else the E1 peg family.

    Issuer lineage (when "issuer_lineage" is in MERGE_RULES): a family that
    is a census row which wraps, stakes or is a receipt of another census
    row's issuer asset takes that row's family, followed to the end of the
    chain (wstUSR -> USR, sUSDe -> USDe, wstETH -> stETH, wrsETH -> rsETH).
    The link is the census unwrap chain at row level, or the reviewed
    LINEAGE_TABLE; a link to a base asset (LINEAGE_BASE) is not followed."""

    def __init__(self, tokens):
        self.tokens = tokens
        self.s1 = {}
        for r in jsonl(CENSUS_S1_TOKENS):
            self.s1[(int(r["chain_id"]), r["token"].lower())] = r
        self.rows = {r["uid"]: r for r in jsonl(CENSUS_UNIVERSE)}
        by_addr = defaultdict(list)
        for r in self.rows.values():
            for c, a in (r.get("addresses") or {}).items():
                if str(c).isdigit():
                    by_addr[(int(c), a.lower())].append(r["uid"])
        self.by_addr = by_addr
        inscope = defaultdict(list)
        for r in self.rows.values():
            if r.get("in_scope"):
                inscope[((r.get("name") or "").lower(), (r.get("symbol") or "").lower())].append(r["uid"])
        self.inscope_by_name = inscope
        sym_issuers = defaultdict(set)
        for r in self.rows.values():
            if r.get("in_scope"):
                sym_issuers[(r.get("symbol") or "").upper()].add(r["uid"])
        self.sym_issuers = sym_issuers
        self.hit_family = {}
        for e in jsonl(CENSUS_EVENTS):
            for h in e["hits"]:
                self.hit_family[(int(h["chain_id"]), h["token"].lower())] = (
                    e["family_uid"], e["family_key"], e["family"])
        self.cache = {}
        self.lineage = self._lineage() if "issuer_lineage" in MERGE_RULES else {}

    def _lineage(self):
        """{census row: (underlying census row, evidence)}."""
        links = {}
        for uid, r in sorted(self.rows.items()):
            for c, a in sorted((r.get("addresses") or {}).items()):
                if not str(c).isdigit():
                    continue
                t = self.s1.get((int(c), a.lower()))
                if not t or not t.get("terminal_token") or t["terminal_token"].lower() == a.lower():
                    continue
                for u in self.by_addr.get((int(c), t["terminal_token"].lower()), []):
                    if u != uid and self.rows[u].get("in_scope") and uid not in links:
                        links[uid] = (u, f"census unwrap {t.get('symbol')} {a} -> "
                                         f"{t.get('terminal_symbol')} {t['terminal_token'].lower()} "
                                         f"on chain {c}")
        for uid, (u, why) in LINEAGE_TABLE.items():
            links[uid] = (u, f"reviewed table: {why}")
        return {k: v for k, v in links.items() if v[0] not in LINEAGE_BASE}

    def lift(self, fam):
        """The family after issuer lineage, with the steps in its rule."""
        seen = set()
        while fam.get("census_uid") in self.lineage and fam["census_uid"] not in seen:
            seen.add(fam["census_uid"])
            u, why = self.lineage[fam["census_uid"]]
            nxt = self._census_family(u, fam["rule"] + f"; issuer lineage {fam['family_id']} -> "
                                                      f"census:{u} ({why})")
            nxt["lineage_from"] = fam.get("lineage_from", []) + [fam["family_id"]]
            fam = nxt
        return fam

    def _row_choice(self, uids, cid, token):
        """In-scope row first; an audit-row contract joins the one in-scope row whose name and symbol equal the contract's on-chain."""
        rows = [self.rows[u] for u in uids if u in self.rows]
        for r in rows:
            if r.get("in_scope"):
                return r["uid"], None
        t = self.s1.get((int(cid), token.lower())) or {}
        key = ((t.get("name") or "").strip().lower(), (t.get("symbol") or "").strip().lower())
        joined = self.inscope_by_name.get(key, [])
        if rows and any(r.get("audit_only") for r in rows) and len(joined) == 1:
            return joined[0], f"audit row {rows[0]['uid']} joins {joined[0]} by on-chain name and symbol"
        return (rows[0]["uid"], None) if rows else (None, None)

    def _census_family(self, uid, rule):
        r = self.rows.get(uid, {})
        unit = (r.get("peg_unit") or "").upper() or None
        return {"family_id": f"census:{uid}", "label": r.get("family_key") or r.get("symbol") or uid,
                "census_uid": uid, "peg": unit, "rule": rule}

    def terminal(self, cid, token):
        r = self.s1.get((int(cid), token.lower()))
        if r and r.get("terminal_token") and r["terminal_token"].lower() != token.lower():
            return r["terminal_token"].lower(), r.get("terminal_symbol")
        return None, None

    def resolve(self, cid, token):
        key = (int(cid), (token or "").lower())
        if key in self.cache:
            return self.cache[key]
        self.cache[key] = fam = self.lift(self._resolve(*key))
        return fam

    @staticmethod
    def issuer(fam):
        """(issuer, source) of a family for the same-issuer rule."""
        lab = (fam.get("label") or "").upper()
        if lab in ISSUERS:
            return ISSUERS[lab][0], f"ISSUERS[{lab}]"
        if fam.get("family_id") in FAMILY_ISSUERS:
            return FAMILY_ISSUERS[fam["family_id"]][0], f"FAMILY_ISSUERS[{fam['family_id']}]"
        return None, None

    def _resolve(self, cid, token):
        if not token or token == ZERO:
            return {"family_id": None, "label": None, "census_uid": None, "peg": None,
                    "rule": "no collateral"}
        sym = self.tokens.symbol(cid, token)
        if (cid, token) in self.hit_family:
            uid, fkey, cls = self.hit_family[(cid, token)]
            unit = {"usd_stable": "USD", "eth_pegged": "ETH", "btc_pegged": "BTC"}.get(cls)
            r = self.rows.get(uid)
            if r and r.get("peg_unit"):
                unit = r["peg_unit"].upper()
            return {"family_id": f"census:{uid}", "label": fkey, "census_uid": uid,
                    "peg": unit, "rule": f"0: hit token of a census lending event ({uid})"}
        uid, note = self._row_choice(self.by_addr.get((cid, token), []), cid, token)
        term, tsym = self.terminal(cid, token)
        tuid, tnote = (self._row_choice(self.by_addr.get((cid, term), []), cid, term)
                       if term else (None, None))
        in_scope = bool(uid and self.rows[uid].get("in_scope"))
        if uid and (in_scope or not (tuid and self.rows[tuid].get("in_scope"))):
            return self._census_family(uid, "1: census price universe row" + (f"; {note}" if note else ""))
        if tuid:
            return self._census_family(
                tuid, f"2: census unwrap to {tsym} {term}, then its universe row"
                + (f" (over audit row {uid})" if uid else "") + (f"; {tnote}" if tnote else ""))
        same = [u for (c2, a2), us in self._same_address(token) for u in us if c2 != cid]
        for u in same:
            r = self.rows[u]
            if sym and (r.get("symbol") or "").lower() == sym.lower():
                return self._census_family(u, f"3: same address and symbol as census row {u} on another chain")
        base = tsym or sym or token
        row = self.tokens.get(cid, token) or {}
        pt = re.fullmatch(r"(?:n-)?PT-(.+?)-(?:\d{1,2}[A-Z]{3}\d{4}|\d{10})(?:-\([A-Z]+\))?", sym or "", re.I)
        if pt:
            same = [t for (c, t), r in self.tokens.rows.items()
                    if c == cid and t != token and r.get("symbol") == pt.group(1)]
            if len(same) == 1:
                fam = dict(self.resolve(cid, same[0]))
                fam["rule"] = f"4: Pendle PT of {pt.group(1)} {same[0]} on the same chain; " + fam["rule"]
                return fam
        rule_name = (row.get("peg_rule") or "").split(":")[-1].split("/")[0]
        if not tsym and rule_name in WRAPPER_RULES + ("pendle_principal",) and row.get("reference"):
            base = row["reference"]
        for _ in range(3):
            fam, ref, rule = peg_rules.classify({"chain_id": cid, "token": "", "symbol": base})
            name = rule.split(":")[-1].split("/")[0]
            if name in WRAPPER_RULES + ("pendle_principal",) and ref and ref.upper() != base.upper():
                base = ref
            else:
                break
        upper = base.upper()
        unit = peg_reference(row) or peg_reference({"peg_family": peg_rules.classify(
            {"chain_id": cid, "token": "", "symbol": base})[0]})
        issuers = self.sym_issuers.get(upper, set())
        if len(issuers) > 1:
            return {"family_id": f"symbol:{upper} (issuer not identified)", "label": base,
                    "census_uid": None, "peg": unit,
                    "rule": f"4: symbol {base}, which census rows of several issuers share"}
        ours = {str(c) for _, c in PAIRS}
        if len(issuers) == 1:
            u = next(iter(issuers))
            if ours & set((self.rows[u].get("addresses") or {})):
                return self._census_family(
                    u, f"4: underlying symbol {base} equals census row {u}, which has a "
                       "contract on one of our chains (issuer of this contract not verified)")
        return {"family_id": f"symbol:{upper}", "label": base, "census_uid": None, "peg": unit,
                "rule": "4: underlying symbol" + (f" via census unwrap ({tsym})" if tsym else "")}

    def _same_address(self, token):
        if not hasattr(self, "_addr_index"):
            idx = defaultdict(list)
            for (c, a), uids in self.by_addr.items():
                idx[a].append(((c, a), uids))
            self._addr_index = idx
        return self._addr_index.get(token.lower(), [])


def family_price(prices, fams, cid, token, day):
    """Depeg evidence price of a collateral in its family's peg unit."""
    fam = fams.resolve(cid, token)
    term, _ = fams.terminal(cid, token)
    src = term if term and prices.quoted(cid, term) else token
    return prices.in_unit(cid, src, day, fam["peg"])


def census_provenance():
    meta = json.loads(CENSUS_EVENTS.with_name(CENSUS_EVENTS.name + ".meta.json").read_text())
    return {"file": inputs.rel(CENSUS_EVENTS),
            "sha256": sha256(CENSUS_EVENTS), "generator": meta.get("generator"),
            "written_utc": meta.get("written_utc"), "rule_version": CENSUS_RULE_VERSION,
            "rules_file": inputs.rel(CENSUS_RULES),
            "rules_sha256": sha256(CENSUS_RULES), "n_events_in_file": None}


def import_census():
    """Census hits on our 14 pairs, mapped to our market ids."""
    prov = census_provenance()
    events = jsonl(CENSUS_EVENTS)
    prov["n_events_in_file"] = len(events)
    pairs = set(PAIRS)
    silo_cfg = {}
    for proto, cid in PAIRS:
        if proto == "silo_v2":
            for r in jsonl(panel_dir(proto, cid) / "configs.jsonl"):
                silo_cfg.setdefault((cid, r["silo_config"]), []).append(r)
    hits, out_of_scope = [], Counter()
    for e in events:
        for i, h in enumerate(e["hits"]):
            proto, cid = h["protocol"], int(h["chain_id"])
            if (proto, cid) not in pairs:
                out_of_scope[f"{proto}:{cid}"] += 1
                continue
            parts = h["market_uid"].split(":")
            coll_vault = None
            if proto == "morpho":
                market = parts[2]
            elif proto == "euler_v2":
                market, coll_vault = parts[2], parts[3]
            else:
                rows = silo_cfg.get((cid, parts[2]), [])
                tok = {r["silo"]: r["token"] for r in rows}
                lend = [r["silo"] for r in rows if tok.get(r["paired_silo"]) == h["token"]]
                if len(lend) != 1:
                    out_of_scope[f"unmapped silo config {h['market_uid']}"] += 1
                    continue
                market = lend[0]
            r19 = h.get("r19") or {}
            hits.append({
                "hit_id": f"census:{e['event_id']}:{i}", "kind": "depeg_loss",
                "source": "depeg_census", "protocol": proto, "chain_id": cid,
                "market": market, "collateral_token": h["token"],
                "collateral_vault": coll_vault, "census_market_uid": h["market_uid"],
                "census_event_id": e["event_id"], "census_family_uid": e["family_uid"],
                "census_family_key": e["family_key"], "census_family_class": e["family"],
                "i1_mode": h.get("i1_mode"), "oracle_class": h.get("oracle_class"),
                "i2": h.get("i2"), "i3": h.get("i3"),
                "window_from_ts": r19.get("window_from_ts"), "window_to_ts": r19.get("window_to_ts"),
                "onset_ts": h["from_ts"], "end_ts": h["to_ts"],
                "min_price_peg": e.get("min_collateral_price"),
                "min_price_source": e.get("min_collateral_price_source"),
                "hit_min_reading": r19.get("min_reading"),
                "provenance": {"file": prov["file"], "row_id": f"{e['event_id']}/hits[{i}]",
                               "rule_version": CENSUS_RULE_VERSION}})
    return hits, dict(out_of_scope), prov


def moolah_scan(work, prices, fams):
    """Moolah depeg hits by the census rules on our panel."""
    proto, cid = "moolah", 56
    scan = work.load(f"scan/{proto}_{cid}.pkl")
    head = work.load("heads.pkl")[f"{proto}:{cid}"]
    info = market_info(proto, cid)
    cands = []
    for m, res in scan.items():
        row = info.get(m)
        if not row:
            continue
        coll = row["collaterals"][0][0]
        if coll in (None, ZERO) or fams.resolve(cid, coll)["peg"] not in ("USD", "ETH", "BTC"):
            continue
        for r in res["runs"][0.99]:
            end = r["end_ts"] if not r["ongoing"] else head["ts"]
            if end - r["start_ts"] >= 72 * 3600:
                cands.append({"market": m, "indicator": "I2", "start_ts": r["start_ts"],
                              "end_ts": end, "ongoing": r["ongoing"],
                              "start_block": r["start_block"]})
        for w in res["rate_windows"]:
            end = w["end_ts"] if w["end_ts"] is not None else head["ts"]
            cands.append({"market": m, "indicator": "I3", "start_ts": w["start_ts"],
                          "end_ts": end, "ongoing": w["end_ts"] is None,
                          "start_block": w["start_block"], "peak_apr": w["peak_apr"],
                          "threshold_apr": w["threshold_apr"]})
    block_at = {}
    for res in scan.values():
        for d, _s, _b, _sh, blk, _ts in res["daily"]:
            if blk > block_at.get(d, -1):
                block_at[d] = blk
    created = {m: (scan[m]["daily"][0][0] if scan[m]["daily"] else None) for m in scan}
    want = defaultdict(set)
    for c in cands:
        for d in range((c["start_ts"] - R19_LOOKBACK) // DAY, c["end_ts"] // DAY + 1):
            if created.get(c["market"]) is not None and d >= created[c["market"]] and d in block_at:
                want[d].add(c["market"])
    sel = evmabi.selector("peek(address)")
    samples = {}
    calls_before = rpc.calls_made()
    chain = CHAINS[str(cid)]["drpc"]
    request_key = hashlib.sha256(json.dumps(
        sorted((d, block_at[d], sorted(ms)) for d, ms in want.items())).encode()).hexdigest()
    prev = DERIVED / OUT / "moolah_oracle_samples.csv"
    prev_meta = prev.with_name(prev.name + ".meta.json")
    if prev.exists() and prev_meta.exists():
        with open(prev) as fh:
            for r in csv.DictReader(fh):
                m, d = r["market"], day_of(r["date"])
                if m in want.get(d, ()) and int(r["block"]) == block_at[d]:
                    samples[(m, d)] = (
                        int(r["peek_collateral"]) if r["peek_collateral"] else None,
                        int(r["peek_loan"]) if r["peek_loan"] else None)
        want = {d: {m for m in ms if (m, d) not in samples} for d, ms in want.items()}
        want = {d: ms for d, ms in want.items() if ms}

    def read_day(d):
        mk = sorted(want[d])
        calls = []
        for m in mk:
            row = info[m]
            for tok in (row["collaterals"][0][0], row["loan"]):
                calls.append((row["oracle_address"],
                              sel + evmabi.encode_call("x(address)", ["address"], [tok])[10:]))
        res = multicall.aggregate3(chain, calls, block_at[d])
        out = {}
        for j, m in enumerate(mk):
            vals = []
            for ok, ret in res[2 * j:2 * j + 2]:
                vals.append(evmabi.decode(["uint256"], ret)[0] if ok and len(ret) >= 66 else None)
            out[m] = vals
        return d, out

    with ThreadPoolExecutor(6) as ex:
        for d, out in ex.map(read_day, sorted(want)):
            for m, (pc, pl) in out.items():
                samples[(m, d)] = (pc, pl)
    n_rpc = rpc.calls_made() - calls_before
    rows = []
    ratio = {}
    for (m, d), (pc, pl) in sorted(samples.items()):
        row = info[m]
        coll, loan = row["collaterals"][0][0], row["loan"]
        mc, ml = prices.raw(cid, coll, d), prices.raw(cid, loan, d)
        r = None
        if pc and pl and mc and ml:
            r = (pc / pl) / (mc / ml)
            ratio[(m, d)] = r
        rows.append({"date": day_str(d), "block": block_at[d], "market": m,
                     "oracle": row["oracle_address"], "collateral": coll, "loan": loan,
                     "peek_collateral": pc, "peek_loan": pl,
                     "llama_collateral_usd": mc, "llama_loan_usd": ml,
                     "accounting_over_market": r})
    hits = []
    for c in cands:
        m = c["market"]
        row = info[m]
        coll = row["collaterals"][0][0]
        lo, hi = (c["start_ts"] - R19_LOOKBACK) // DAY, c["end_ts"] // DAY
        days = range(lo, hi + 1)
        i1_days = [d for d in days if ratio.get((m, d), 0) >= I1_RATIO]
        i1_run = any(d + 1 in i1_days for d in i1_days)
        peg = [(d, family_price(prices, fams, cid, coll, d)) for d in days]
        below = [d for d, p in peg if p is not None and p <= DEPEG_LINE]
        c["i1_days"], c["i1_24h"] = len(i1_days), i1_run
        c["depeg_days"] = len(below)
        c["min_price_peg"] = min((p for _, p in peg if p is not None), default=None)
        c["max_ratio"] = max((ratio.get((m, d), 0) for d in days), default=None)
        if not (i1_run and below):
            continue
        onset = max(c["start_ts"], below[0] * DAY + DAY - 1)
        hits.append({
            "hit_id": f"moolah:{m}:{c['indicator']}:{c['start_block']}", "kind": "depeg_loss",
            "source": "moolah_scan", "protocol": proto, "chain_id": cid, "market": m,
            "collateral_token": coll, "collateral_vault": None,
            "indicator": c["indicator"], "window_from_ts": c["start_ts"],
            "window_to_ts": c["end_ts"], "onset_ts": onset,
            "end_ts": c["end_ts"], "ongoing": c["ongoing"],
            "min_price_peg": c["min_price_peg"], "min_price_source": "DefiLlama daily close / peg",
            "max_accounting_over_market": c["max_ratio"],
            "i1_days": c["i1_days"], "depeg_days": c["depeg_days"]})
    return hits, cands, rows, n_rpc, len({d for _m, d in samples}), request_key


def stage_depeg(work):
    tokens = Tokens(work.load("tokens.pkl"))
    fams = Families(tokens)
    census_pegs(tokens, fams)
    prices = Prices(tokens, set(tokens.rows))
    census_hits, out_scope, prov = import_census()
    moolah_hits, cands, samples, n_rpc, n_days, request_key = moolah_scan(work, prices, fams)
    p = DERIVED / OUT / "moolah_oracle_samples.csv"
    cols = list(samples[0]) if samples else ["date"]
    tmp = p.with_name(p.name + ".tmp")
    with open(tmp, "w", newline="") as fh:
        w = csv.DictWriter(fh, cols)
        w.writeheader()
        w.writerows(samples)
    os.replace(tmp, p)
    io_utils._meta_sidecar(p, GENERATOR, {
        "what": "Moolah I1 evidence: the market oracle's peek(collateral) and peek(loan) "
                "(USD, 8 decimals) at each day's last panel block, against DefiLlama "
                "daily closes; accounting_over_market = (peek_c / peek_l) / (llama_c / llama_l)",
        "rows": len(samples), "days": n_days, "rpc_requests_this_run": n_rpc,
        "candidates": len(cands), "request_sha256": request_key,
        "reuse": "a rerun reads every (market, day) sample with the same block from this "
                 "file and reads only the missing ones from the chain"})
    work.save("depeg.pkl", {"census": census_hits, "moolah": moolah_hits,
                            "moolah_candidates": cands, "provenance": prov,
                            "out_of_scope": out_scope, "moolah_rpc": n_rpc})
    return {"census_hits_in_scope": len(census_hits), "census_out_of_scope": out_scope,
            "moolah_candidates": len(cands), "moolah_days_sampled": n_days,
            "moolah_rpc": n_rpc, "moolah_hits": len(moolah_hits)}


POS_MARKET_COL = {"morpho": "market_id", "moolah": "market_id",
                  "euler_v2": "strategy", "silo_v2": "market"}


def vault_registry(proto, cid):
    out = {}
    for r in jsonl(DATA / f"registry/vaults/{proto}/{cid}.jsonl"):
        out[r["vault"]] = {"asset": r["asset"], "kind": r.get("kind"), "name": r.get("name")}
    return out


def position_files(proto, cid):
    d = DATA / f"vaults/{proto}/{cid}/positions"
    if not d.exists():
        return {}
    return {fn[:-4]: d / fn for fn in os.listdir(d) if fn.endswith(".csv")}


def _index_file(job):
    proto, vault, path = job
    col = POS_MARKET_COL[proto]
    with open(path, newline="") as fh:
        rd = csv.reader(fh)
        head = next(rd)
        i = head.index(col)
        return vault, {row[i] for row in rd}


def position_index(proto, cid, workers):
    """{market: set(vault)} over every position file of the pair."""
    files = position_files(proto, cid)
    idx = defaultdict(set)
    with ProcessPoolExecutor(workers) as ex:
        for vault, markets in ex.map(_index_file, [(proto, v, p) for v, p in files.items()],
                                     chunksize=4):
            for m in markets:
                idx[m].add(vault)
    return idx


def read_positions(proto, path):
    """{market: [(block, log_index, ts, shares after the row)]} with one entry per row, and {market."""
    rows, anchors = defaultdict(list), defaultdict(list)
    with open(path, newline="") as fh:
        rd = csv.DictReader(fh)
        for r in rd:
            b, li, ts = int(r["block"]), int(r["log_index"]), int(r["timestamp"])
            m = r[POS_MARKET_COL[proto]]
            if proto in ("morpho", "moolah"):
                rows[m].append((b, li, ts, int(r["vault_cum_shares"])))
                continue
            rows[m].append((b, li, ts, int(r["cum_shares"])))
            if r["kind"] in ("Deposit", "Withdraw") and r["assets"] and r["event_shares"]:
                a, sh = int(r["assets"]), int(r["event_shares"])
                if a > 0 and sh > 0:
                    anchors[m].append((b, li, ts, a, sh))
    return rows, anchors


def queue_timelines(proto, cid, vaults):
    """{vault: {market: [(ts, block, log_index, in_queue)]}} for vaults with a withdraw queue (the MetaMorpho design of Morpho V1, Moolah, Silo and Euler Earn vaults)."""
    p = DATA / f"vaults/{proto}/{cid}/actions.jsonl"
    rows = defaultdict(list)
    if not p.exists():
        return {}
    with open(p, "rb") as fh:
        for line in fh:
            if b"SetWithdrawQueue" not in line and b'"SetCap"' not in line:
                continue
            r = json.loads(line)
            if r["vault"] not in vaults or r["event"] not in ("SetWithdrawQueue", "SetCap"):
                continue
            rows[r["vault"]].append((r["block"], r["log_index"], r["timestamp"], r["event"],
                                     r.get("fields") or r))
    out = {}
    for v, evs in rows.items():
        evs.sort(key=lambda x: (x[0], x[1]))
        queue, tl = set(), defaultdict(list)
        for b, li, ts, ev, f in evs:
            before = set(queue)
            if ev == "SetWithdrawQueue":
                queue = {x.lower() for x in f.get("newWithdrawQueue") or []}
            else:
                m = (f.get("id") or f.get("market") or "").lower()
                if m and int(f.get("cap") or 0) > 0:
                    queue.add(m)
            for m in before ^ queue:
                tl[m].append((ts, b, li, m in queue))
        out[v] = dict(tl)
    return out


def written_off(timeline, ts):
    """True when the market is out of the vault's withdraw queue at ts."""
    if not timeline:
        return False
    state = None
    for t, _b, _l, inq in timeline:
        if t <= ts:
            state = inq
        else:
            break
    return state is False


class MarketDays:
    """End-of-day state of one market as dense arrays over [d0, d1]."""

    def __init__(self, proto, daily, d0, d1, factors=None):
        n = d1 - d0 + 1
        self.d0 = d0
        self.supply = [0.0] * n
        self.avail = [0.0] * n
        self.aps = [0.0] * n
        self.fac = [1.0] * n
        j, cur = 0, None
        fac_days = sorted(factors) if factors else []
        k, fac = 0, 1.0
        for i in range(n):
            d = d0 + i
            while j < len(daily) and daily[j][0] <= d:
                cur = daily[j]
                j += 1
            while k < len(fac_days) and fac_days[k] <= d:
                fac = factors[fac_days[k]]
                k += 1
            self.fac[i] = fac
            if cur is None:
                self.aps[i] = aps_of(proto, 0, 0)
                continue
            _, sup, bor, sh = cur[:4]
            self.supply[i] = float(sup)
            self.avail[i] = float(liquidity_of(sup, bor))
            self.aps[i] = aps_of(proto, sup, sh) * fac

    def at(self, d, arr):
        i = d - self.d0
        if i < 0:
            return 0.0
        return arr[min(i, len(arr) - 1)]


def silo_factors(daily, anchors):
    """{day: exact / replayed assets per share} at the last vault anchor of each day."""
    if not anchors:
        return {}
    by_day = {}
    for _b, _li, ts, a, sh in sorted(anchors):
        by_day[ts // DAY] = a / sh
    days = [x[0] for x in daily]
    out = {}
    for d, exact in by_day.items():
        j = bisect.bisect_right(days, d) - 1
        if j < 0:
            continue
        _, sup, _bor, sh = daily[j][:4]
        rep = aps_of("silo_v2", sup, sh)
        if rep > 0:
            out[d] = exact / rep
    return out


def anchor_aps(anchors, d):
    """Assets per share of a market outside the panel."""
    best = None
    for b, li, ts, a, sh in anchors:
        if ts // DAY <= d and (best is None or (b, li) > best[0]):
            best = ((b, li), a / sh)
    return best[1] if best else None


def build_vault_series(proto, cid, vault, path, mdays, anchors_all, head_day, keep, queues):
    """Daily series of one vault."""
    rows, anchors = read_positions(proto, path)
    first = min((r[0][2] for r in rows.values() if r), default=None)
    if first is None:
        return None
    d0 = first // DAY
    n = head_day - d0 + 1
    total = [0.0] * n
    wd = [0.0] * n
    pos_keep = {}
    outside = [0.0] * n
    wo_first = {}
    for m, rr in rows.items():
        md = mdays.get(m)
        tl = (queues or {}).get(m)
        anc = anchors_all.get(m, []) or anchors.get(m, [])
        eod = []
        cur, j = 0, 0
        for i in range(n):
            d = d0 + i
            while j < len(rr) and rr[j][2] // DAY <= d:
                cur = rr[j][3]
                j += 1
            eod.append(cur)
        arr = [0.0] * n
        for i, sh in enumerate(eod):
            if sh <= 0:
                continue
            d = d0 + i
            if tl and written_off(tl, d * DAY + DAY - 1):
                wo_first.setdefault(m, d)
                continue
            if md is not None:
                a = sh * md.at(d, md.aps)
                arr[i] = a
                total[i] += a
                wd[i] += min(a, md.at(d, md.avail))
            else:
                rate = anchor_aps(anc, d)
                if rate is None:
                    continue
                a = sh * rate
                arr[i] = a
                total[i] += a
                wd[i] += a
                outside[i] += a
        if m in keep:
            pos_keep[m] = arr
    share_out = max((o / t for o, t in zip(outside, total) if t > 0), default=0.0)
    return {"vault": vault, "d0": d0, "total": total, "wd": wd, "pos": pos_keep,
            "rows": {m: rows[m] for m in keep if m in rows},
            "queue": {m: (queues or {}).get(m) for m in keep if m in rows},
            "written_off": {m: day_str(d) for m, d in wo_first.items()},
            "outside_panel_share": share_out}


_W = {}


def _init_vault_worker(work_root, proto, cid):
    w = Work(work_root)
    _W.clear()
    _W.update(w.load(f"vaultctx/{proto}_{cid}.pkl"))


def _vault_job(job):
    proto, cid, vault, path = job
    return vault, build_vault_series(proto, cid, vault, path, _W["mdays"], _W["anchors"],
                                     _W["head_day"], _W["keep"], _W["queues"].get(vault))


def governor_timeline(cid):
    """{lending vault: [(ts, governor)]} from EVaultCreated (creator) and GovSetGovernorAdmin."""
    out = defaultdict(list)
    for r in jsonl(panel_dir("euler_v2", cid) / "governance.jsonl"):
        if r["event"] == "EVaultCreated":
            out[r["vault"]].append((r["timestamp"], r["block"], r["log_index"], r["creator"]))
        elif r["event"] == "GovSetGovernorAdmin":
            out[r["vault"]].append((r["timestamp"], r["block"], r["log_index"],
                                    r["newGovernorAdmin"]))
    return {v: sorted(x) for v, x in out.items()}


def governor_at(timeline, ts):
    g = None
    for t, _b, _l, who in timeline:
        if t <= ts:
            g = who
    return g


def run_end(run, head_ts):
    return run["end_ts"] if not run["ongoing"] else head_ts


def run_days(run, head_ts):
    """UTC days whose end (23:59:59) falls inside the run."""
    a, b = run["start_ts"], run_end(run, head_ts)
    return [d for d in range(a // DAY, b // DAY + 1) if a <= d * DAY + DAY - 1 <= b]


def peak_supply_usd(run, prices, cid, loan, head_ts):
    """Largest supply x price over the run's days (supply carried across days without rows), and the day it occurred."""
    best, best_day = None, None
    cur = run["supply_start"]
    for d in range(run["start_ts"] // DAY, run_end(run, head_ts) // DAY + 1):
        if d in run["day_max"]:
            day_sup = run["day_max"][d]
            v = prices.value(cid, loan, max(day_sup, cur), d)
            cur = day_sup
        else:
            v = prices.value(cid, loan, cur, d)
        if v is not None and (best is None or v > best):
            best, best_day = v, d
    return best, best_day


def pinned_share(daily, day):
    """Share of the 180 day ends before `day` (those after the market's first row) with end-of-day utilization >= 99%."""
    days = [x[0] for x in daily]
    n = hit = 0
    for d in range(day - 180, day):
        j = bisect.bisect_right(days, d) - 1
        if j < 0:
            continue
        n += 1
        if util_of(daily[j][1], daily[j][2]) >= MAIN[0]:
            hit += 1
    return round(hit / n, 4) if n else None


def stage_lockups(work, workers):
    tokens = Tokens(work.load("tokens.pkl"))
    fams = Families(tokens)
    census_pegs(tokens, fams)
    prices = Prices(tokens, set(tokens.rows))
    heads = work.load("heads.pkl")
    depeg = work.load("depeg.pkl")
    depeg_markets = defaultdict(set)
    for h in depeg["census"] + depeg["moolah"]:
        depeg_markets[(h["protocol"], h["chain_id"])].add(h["market"])
    all_runs = []
    vault_series = {}
    stats = {}
    for proto, cid in PAIRS:
        key = f"{proto}:{cid}"
        head = heads[key]
        scan = work.load(f"scan/{proto}_{cid}.pkl")
        info = market_info(proto, cid)
        dist = euler_distribution(cid) if proto == "euler_v2" else {}
        n_unpriced = 0
        runs = []
        for m, res in scan.items():
            row = info.get(m)
            if not row:
                continue
            for t in THRESHOLDS:
                for r in res["runs"][t]:
                    dur = run_end(r, head["ts"]) - r["start_ts"]
                    if dur < min(DURS_H) * 3600:
                        continue
                    peak, peak_day = peak_supply_usd(r, prices, cid, row["loan"], head["ts"])
                    if peak is None:
                        n_unpriced += 1
                        continue
                    if peak < MIN_PEAK_USD:
                        continue
                    (ctok, cvault), crule = dominant_collateral(
                        row, dist.get(m), r["start_ts"] // DAY)
                    orc = row.get("oracle")
                    if proto == "euler_v2":
                        orc = row["oracle_by_collateral"].get(cvault)
                    runs.append({
                        "pinned_share_prior_180d": pinned_share(res["daily"], r["start_ts"] // DAY),
                        "opened_in_creation_block": r["start_block"] == row.get("created_block"),
                        "protocol": proto, "chain_id": cid, "market": m, "threshold": t,
                        "run": r, "duration_h": dur / 3600, "peak_supply_usd": peak,
                        "peak_day": peak_day, "loan_token": row["loan"],
                        "collateral_token": ctok, "collateral_vault": cvault,
                        "collateral_rule": crule, "oracle": orc or {},
                        "family": fams.resolve(cid, ctok) if ctok else
                        {"family_id": f"unattributed:{proto}:{cid}:{m}", "label": None,
                         "census_uid": None, "peg": None, "rule": crule}})
        stats[key] = {"runs_ge48h_ge1m": len(runs), "runs_unpriced": n_unpriced}
        keep = {r["market"] for r in runs} | depeg_markets[(proto, cid)]
        idx = position_index(proto, cid, workers)
        holders = sorted({v for m in keep for v in idx.get(m, ())})
        files = position_files(proto, cid)
        needed_markets = {m for m, vs_ in idx.items() if vs_ & set(holders)}
        anchors_all = defaultdict(list)
        if proto in ("silo_v2", "euler_v2"):
            for v in holders:
                for m, a in read_positions(proto, files[v])[1].items():
                    anchors_all[m].extend(a)
        head_day = head["ts"] // DAY
        mdays = {}
        for m in needed_markets | keep:
            if m in scan and scan[m]["daily"]:
                daily = scan[m]["daily"]
                fac = silo_factors(daily, anchors_all.get(m)) if proto == "silo_v2" else None
                mdays[m] = MarketDays(proto, daily, daily[0][0], head_day, fac)
        queues = queue_timelines(proto, cid, set(holders))
        work.save(f"vaultctx/{proto}_{cid}.pkl", {"mdays": mdays, "anchors": dict(anchors_all),
                                                  "head_day": head_day, "keep": keep,
                                                  "queues": queues})
        series = {}
        if holders:
            jobs = [(proto, cid, v, files[v]) for v in holders]
            with ProcessPoolExecutor(min(workers, len(jobs)), initializer=_init_vault_worker,
                                     initargs=(str(work.root), proto, cid)) as ex:
                for v, sres in ex.map(_vault_job, jobs, chunksize=2):
                    if sres:
                        series[v] = sres
        reg = vault_registry(proto, cid)
        for v, sres in series.items():
            sres.update(reg.get(v, {"asset": None, "kind": None, "name": None}))
        if proto == "euler_v2":
            gov = governor_timeline(cid)
            for m in keep:
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
        vault_series[key] = {"series": series, "index": {m: idx.get(m, set()) for m in keep},
                             "mdays": mdays}
        stats[key].update({"vaults_with_series": len(series), "markets_of_interest": len(keep)})
        all_runs.extend(runs)
        print(f"lockups {key} {stats[key]}", flush=True)
    work.save("vault_series.pkl", vault_series)
    evaluated = []
    for r in all_runs:
        key = f"{r['protocol']}:{r['chain_id']}"
        evaluated.append(step_two(r, vault_series[key], prices, heads[key]["ts"]))
    work.save("runs.pkl", evaluated)
    main_rows, vault_rows = write_lockups(evaluated, tokens, heads)
    stats["main_runs"] = len(main_rows)
    stats["main_kept"] = sum(1 for x in main_rows if x["kept"])
    stats["vault_rows"] = len(vault_rows)
    return stats


def step_two(r, vs, prices, head_ts):
    """Vaults the run pushes below L(t) = 5%, with their minimum L(t), days below each threshold, and position at the run's start."""
    proto, cid, m = r["protocol"], r["chain_id"], r["market"]
    run = r["run"]
    days = run_days(run, head_ts)
    md = vs["mdays"].get(m)
    holders = set(vs["index"].get(m, set()))
    if proto == "euler_v2":
        holders.add(f"self:{m}")
    out = []
    for v in sorted(holders):
        s = vs["series"].get(v)
        if s is None or m not in s["pos"]:
            continue
        if s["kind"] == "euler_lending" and \
                (governor_at(s.get("governors", []), run["start_ts"]) or ZERO) == ZERO:
            continue
        asset = s["asset"]
        min_l, below, pushed, first_push, max_usd = None, {x: 0 for x in (L_LOCK,) + L_SENS}, False, None, 0.0
        for d in days:
            i = d - s["d0"]
            if i < 0 or i >= len(s["total"]):
                continue
            pos = s["pos"][m][i]
            tot = s["total"][i]
            if pos <= 0 or tot <= 0:
                continue
            usd = prices.value(cid, asset, pos, d)
            if usd is None or usd < MATERIAL_USD:
                continue
            max_usd = max(max_usd, usd)
            lv = s["wd"][i] / tot
            avail = md.at(d, md.avail) if md else pos
            lcf = (s["wd"][i] - min(pos, avail) + pos) / tot
            min_l = lv if min_l is None else min(min_l, lv)
            for x in below:
                if lv < x:
                    below[x] += 1
            if lv < L_LOCK and lcf >= L_LOCK and not pushed:
                pushed, first_push = True, d
        if min_l is None:
            continue
        sd = run["start_ts"] // DAY
        if s["kind"] == "euler_lending":
            pos_start = float(run["supply_start"])
        else:
            rr = s["rows"].get(m, [])
            k = bisect.bisect_left([(x[0], x[1]) for x in rr], (run["start_block"], run["start_li"]))
            sh = rr[k - 1][3] if k > 0 else 0
            if written_off((s.get("queue") or {}).get(m), run["start_ts"]):
                sh = 0
            fac = md.at(sd, md.fac) if md is not None else 1.0
            pos_start = sh * aps_of(proto, run["supply_start"], run["shares_start"]) * fac
        ip = sd - 1 - s["d0"]
        share_prev = None
        if 0 <= ip < len(s["total"]) and s["total"][ip] > 0:
            share_prev = s["pos"][m][ip] / s["total"][ip]
        out.append({"vault": s["vault"], "series_key": v, "kind": s["kind"], "name": s.get("name"),
                    "asset": asset, "min_L": min_l, "days_below": {str(k): n for k, n in below.items()},
                    "pushed": pushed, "first_push_day": day_str(first_push) if first_push else None,
                    "position_at_start": pos_start,
                    "position_at_start_usd": prices.value(cid, asset, pos_start, sd),
                    "share_of_vault_prev_day": share_prev, "max_position_usd_in_run": max_usd,
                    "outside_panel_share": s["outside_panel_share"]})
    r = dict(r)
    r["vaults"] = out
    r["kept"] = any(x["pushed"] for x in out)
    return r


def run_id(r):
    run = r["run"]
    return f"{r['protocol']}:{r['chain_id']}:{r['market']}:{run['start_block']}:{run['start_li']}"


def finite(x):
    return None if x is None or (isinstance(x, float) and not math.isfinite(x)) else x


def is_main(r, head_ts):
    return r["threshold"] == MAIN[0] and \
        run_end(r["run"], head_ts) - r["run"]["start_ts"] >= MAIN[1] * 3600


def lockup_market_row(r, tokens, prices, head_ts):
    run = r["run"]
    cid = r["chain_id"]
    sd = run["start_ts"] // DAY
    return {
        "run_id": run_id(r), "protocol": r["protocol"], "chain_id": cid, "market": r["market"],
        "loan_token": r["loan_token"], "loan_symbol": tokens.symbol(cid, r["loan_token"]),
        "collateral_token": r["collateral_token"],
        "collateral_symbol": tokens.symbol(cid, r["collateral_token"]) if r["collateral_token"] else None,
        "collateral_vault": r["collateral_vault"], "collateral_rule": r["collateral_rule"],
        "family_id": r["family"]["family_id"], "family_label": r["family"]["label"],
        "start_utc": utc(run["start_ts"]), "start_ts": run["start_ts"],
        "start_block": run["start_block"],
        "end_utc": utc(run["end_ts"]), "end_ts": run["end_ts"], "end_block": run["end_block"],
        "ongoing_at_head": run["ongoing"], "head_ts": head_ts if run["ongoing"] else None,
        "duration_h": round(r["duration_h"], 2),
        "peak_supply_usd": r["peak_supply_usd"], "peak_supply_day": day_str(r["peak_day"]),
        "supply_at_start_usd": prices.value(cid, r["loan_token"], run["supply_start"], sd),
        "min_utilization": finite(run["min_util"]), "max_utilization": finite(run["max_util"]),
        "debt_exceeds_supply": run["max_util"] > 1.0,
        "oracle_type": r["oracle"].get("type"), "oracle_subtype": r["oracle"].get("subtype"),
        "blind_to": r["oracle"].get("blind_to"),
        "opened_in_creation_block": r["opened_in_creation_block"],
        "pinned_share_prior_180d": r["pinned_share_prior_180d"],
        "excluded": "self_dealing_loop" if r["kept"] and self_dealing_run(r) else None,
        "kept": r["kept"], "n_vaults_material": len(r["vaults"]),
        "n_vaults_pushed": sum(1 for v in r["vaults"] if v["pushed"]),
        "min_L_over_vaults": min((v["min_L"] for v in r["vaults"]), default=None)}


def write_lockups(evaluated, tokens, heads):
    prices = Prices(tokens, set(tokens.rows))
    curators, cur_meta = load_curators()
    main_rows, vault_rows = [], []
    for r in sorted(evaluated, key=lambda x: (x["protocol"], x["chain_id"], x["run"]["start_ts"], x["market"])):
        head_ts = heads[f"{r['protocol']}:{r['chain_id']}"]["ts"]
        if not is_main(r, head_ts):
            continue
        row = lockup_market_row(r, tokens, prices, head_ts)
        main_rows.append(row)
        if not r["kept"]:
            continue
        for v in r["vaults"]:
            ent, conf = curator_at(curators, r["chain_id"], v["vault"], r["run"]["start_ts"])
            ctl = SELF_DEALING_VAULTS.get((r["chain_id"], v["vault"]))
            vault_rows.append({
                "self_dealing_controller": ctl[1] if ctl else None,
                "curator": ent, "curator_confidence": conf,
                "run_id": row["run_id"], "protocol": r["protocol"], "chain_id": r["chain_id"],
                "market": r["market"], "vault": v["vault"], "vault_kind": v["kind"],
                "vault_name": v["name"], "asset": v["asset"],
                "asset_symbol": tokens.symbol(r["chain_id"], v["asset"]) if v["asset"] else None,
                "pushed_below_5pct": v["pushed"], "first_push_day": v["first_push_day"],
                "min_L": v["min_L"], "days_L_below": v["days_below"],
                "position_at_start_raw": f"{v['position_at_start']:.0f}",
                "position_at_start_usd": v["position_at_start_usd"],
                "share_of_vault_prev_day": v["share_of_vault_prev_day"],
                "max_position_usd_in_run": v["max_position_usd_in_run"],
                "outside_panel_share": v["outside_panel_share"]})
    common = {"step_one": f"utilization >= {MAIN[0]} for >= {MAIN[1]} h, peak supply >= "
                          f"{MIN_PEAK_USD:,.0f} USD", "panel": "data/panel/<protocol>/<chain>/",
              "prices": "data/prices/<chain>.csv and data/events/prices_supplement.csv"}
    io_utils.write_jsonl(f"{OUT}/lockups_market.jsonl", main_rows, GENERATOR, {
        **common,
        "what": "every step-one lock-up run on the 14 pairs, with the step-two outcome (kept)",
        "utilization": "Morpho/Moolah totalBorrowAssets / totalSupplyAssets; Euler totalBorrows / "
                       "(cash + totalBorrows); Silo debt / collateral (protected excluded)",
        "run": "starts at the first panel row with utilization >= threshold, ends at the first "
               "later row below it; open at the head = ongoing_at_head",
        "peak_supply_usd": "max over the run's days of supply x the loan token's daily close",
        "collateral": "Euler: the collateral vault with the most attributed debt at the run start "
                      "(data/vaults/euler_v2/*/borrow_distribution.csv); Silo: the paired silo's token",
        "oracle": "data/registry/oracles/ (Silo: the paired silo's solvency oracle)",
        "kept": "step two: some curated vault holding >= 10k USD in the market had L(t) < 5% on a "
                "day end inside the run while L(t) with this market liquid would be >= 5%",
        "opened_in_creation_block": "the run starts in the block that created the market (one-block "
                                    "supply-and-borrow loops such as the 720 Base cbBTC/GMORPHO "
                                    "markets)",
        "pinned_share_prior_180d": "share of the 180 day ends before the run with end-of-day "
                                   "utilization >= 99% (markets pinned by design score high)"})
    io_utils.write_jsonl(f"{OUT}/lockups_vault.jsonl", vault_rows, GENERATOR, {
        **common,
        "what": "step two: every curated vault with >= 10k USD in the market of a kept run",
        "L": "daily at UTC day end: sum over the vault's markets of min(position assets, market "
             "supply - borrow) / sum of position assets; positions are share balances "
             "(data/vaults/) times the panel's assets per share",
        "vault kinds": "morpho_v1, morpho_v2 (market positions through adapters), moolah_vault, "
                       "silo_vault, euler_earn, euler_lending (a governed Euler lending vault, whose "
                       "depositors hold the market itself; L = cash / (cash + totalBorrows))",
        "position_at_start": "vault shares just before the run's first row x the market's assets "
                             "per share at that row; share_of_vault_prev_day is the market's share "
                             "of the vault at the previous day end",
        "outside_panel_share": "largest share of the vault in holdings the panel does not cover "
                               "(Silo idle or nested vaults, non-EVault Earn strategies), valued at "
                               "their last exact conversion and counted as withdrawable",
        "curator": "entity_id of the data/registry/vault_curators.jsonl interval covering the "
                   "run start (null: unattributed or no registry)", "curator_source": cur_meta})
    return main_rows, vault_rows


KNOWN = [
    {"case": "USD0++ 2025-01", "month": "2025-01",
     "families": {"census:s2:bUSD0", "census:coingecko:usd0-liquid-bond", "census:defillama:195"}},
    {"case": "Stream xUSD 2025-11", "month": "2025-11",
     "families": {"census:coingecko:staked-stream-usd"}},
    {"case": "deUSD 2025-11", "month": "2025-11", "families": {"census:defillama:210"}},
    {"case": "USDX 2025-11", "month": "2025-11", "families": {"census:defillama:214"}},
    {"case": "USR 2026-03", "month": "2026-03",
     "families": {"census:defillama:197"}, "related": {"census:s2:RLP", "census:s2:wstUSR"}},
    {"case": "rsETH 2026-04", "month": "2026-04",
     "families": {"census:coingecko:kelp-dao-restaked-eth", "census:coingecko:wrapped-rseth"}},
    {"case": "msUSD 2026-06", "month": "2026-06",
     "families": {"census:defillama:297"}, "related": {"census:s2:AVLT"}},
]


class PanelState:
    """Exact market state at a timestamp, read from the panel CSV (the last row at or before it), cached per market."""

    def __init__(self):
        self.cache = {}

    def series(self, proto, cid, market):
        """(timestamps, rows) of one market's panel."""
        key = (proto, cid, market)
        if key not in self.cache:
            p = panel_dir(proto, cid) / f"{market}.csv"
            rows = list(panel_rows(proto, p)) if p.exists() else []
            self.cache[key] = ([r[2] for r in rows], rows)
        return self.cache[key]

    def at(self, proto, cid, market, ts):
        tss, rows = self.series(proto, cid, market)
        i = bisect.bisect_right(tss, ts) - 1
        if i < 0:
            return None
        _b, _l, _t, supply, borrow, shares, _apr, _ev = rows[i]
        return {"supply": supply, "borrow": borrow, "shares": shares, "ts": rows[i][2]}


def self_dealing_run(r):
    """The controller of a run whose every pushed vault belongs to one listed self-dealing controller, else None."""
    ctls = {SELF_DEALING_VAULTS.get((r["chain_id"], v["vault"])) for v in r["vaults"] if v["pushed"]}
    return next(iter(ctls)) if len(ctls) == 1 and None not in ctls else None


def event_exclusion(hs):
    """(reason, description) when every hit of an event is a lock-up kept only by a self-dealing controller's vaults, else (None, None)."""
    ctls = {self_dealing_run(h["run"]) if h["kind"] == "lockup" else None for h in hs}
    if len(ctls) != 1 or None in ctls:
        return None, None
    ctl = next(iter(ctls))
    info = SELF_DEALING[ctl]
    desc = next((info["markets"][h["market"]] for h in hs if h["market"] in info["markets"]),
                f"self-dealing loop of controller {ctl[1]} ({info['source']}); excluded.")
    return "self_dealing_loop", desc


def lockup_hits(runs, heads, t, dur_h):
    out = []
    for r in runs:
        if r["threshold"] != t or not r["kept"]:
            continue
        head_ts = heads[f"{r['protocol']}:{r['chain_id']}"]["ts"]
        run = r["run"]
        if run_end(run, head_ts) - run["start_ts"] < dur_h * 3600:
            continue
        out.append({"hit_id": f"lockup:{run_id(r)}", "kind": "lockup", "source": "lockup_rule",
                    "protocol": r["protocol"], "chain_id": r["chain_id"], "market": r["market"],
                    "collateral_token": r["collateral_token"], "family": r["family"],
                    "onset_ts": run["start_ts"], "end_ts": run_end(run, head_ts),
                    "ongoing": run["ongoing"], "open": run["ongoing"], "run": r})
    return out


def merge_hits(hits, horizon=MERGE_HORIZON):
    """Same family, windows overlapping or less than 7 days apart."""
    groups = defaultdict(list)
    for h in hits:
        fid = h["family"]["family_id"] or f"alone:{h['hit_id']}"
        groups[fid].append(h)

    def live_end(h):
        if horizon is None or not h.get("open"):
            return h["end_ts"]
        return min(h["end_ts"], h["onset_ts"] + horizon)
    events = []
    for hs in groups.values():
        hs.sort(key=lambda h: (h["onset_ts"], h["hit_id"]))
        cur, cur_end = [], None
        for h in hs:
            if cur and h["onset_ts"] <= cur_end + MERGE_GAP:
                cur.append(h)
                cur_end = max(cur_end, live_end(h))
            else:
                if cur:
                    events.append(cur)
                cur, cur_end = [h], live_end(h)
        if cur:
            events.append(cur)
    if "same_issuer" in MERGE_RULES:
        events = join_same_issuer(events, live_end)
    events.sort(key=lambda hs: (min(h["onset_ts"] for h in hs), hs[0]["hit_id"]))
    return events


def join_same_issuer(groups, live_end):
    """The same-issuer rule of merge_hits over its single-family groups."""
    parent = list(range(len(groups)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i
    info = []
    for g in groups:
        fam = g[0]["family"]
        iss = Families.issuer(fam)[0] if fam["family_id"] else None
        info.append((iss, fam["family_id"], [h for h in g if h["kind"] == "depeg_loss"]))
    for i in range(len(groups)):
        for j in range(i + 1, len(groups)):
            (ia, fa, da), (ib, fb, db) = info[i], info[j]
            if not ia or ia != ib or fa == fb or not da or not db:
                continue
            if any(x["onset_ts"] <= live_end(y) + MERGE_GAP and y["onset_ts"] <= live_end(x) + MERGE_GAP
                   for x in da for y in db):
                parent[find(i)] = find(j)
    out = defaultdict(list)
    for i, g in enumerate(groups):
        out[find(i)].extend(g)
    return [sorted(g, key=lambda h: (h["onset_ts"], h["hit_id"])) for g in out.values()]


def merge_episodes(episodes, gap=EPISODE_GAP):
    """Repeated pins form one event."""
    parent = list(range(len(episodes)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i
    by_key = defaultdict(list)
    for i, ep in enumerate(episodes):
        for h in ep:
            if h["kind"] != "lockup":
                continue
            by_key[("market", h["protocol"], h["chain_id"], h["market"])].append((h["onset_ts"], i))
            by_key[("family", h["protocol"], h["chain_id"], h["family"]["family_id"])].append(
                (h["onset_ts"], i))
    for lst in by_key.values():
        lst.sort()
        for (a, i), (b, j) in zip(lst, lst[1:]):
            if b - a <= gap:
                parent[find(i)] = find(j)
    groups = defaultdict(list)
    for i, ep in enumerate(episodes):
        groups[find(i)].append(ep)
    out = [sorted(g, key=lambda ep: (min(h["onset_ts"] for h in ep), ep[0]["hit_id"]))
           for g in groups.values()]
    out.sort(key=lambda g: (min(h["onset_ts"] for h in g[0]), g[0][0]["hit_id"]))
    return out


def hit_min_price(h, prices, fams, heads):
    """(minimum collateral price in peg units over the hit's window, source)."""
    if h["kind"] != "lockup":
        return h.get("min_price_peg"), h.get("min_price_source")
    cid, tok = h["chain_id"], h["collateral_token"]
    if not tok:
        return None, None
    vals = [family_price(prices, fams, cid, tok, d)
            for d in range(h["onset_ts"] // DAY, h["end_ts"] // DAY + 1)]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, "no DefiLlama close in the run"
    return min(vals), "DefiLlama daily close in peg units (wrappers through their unwrap terminal)"


_PRICED = {}


def priced_loss(hs, tokens, prices, fams, pstate):
    """Priced principal loss of the markets of one event without a depeg-loss hit (module docstring, "Priced principal loss")."""
    sys.modules.setdefault("e2_events", sys.modules[__name__])
    import e5_events as e5
    if "ctx" not in _PRICED:
        lctx = type("PricedCtx", (), {})()
        lctx.tokens, lctx.prices, lctx.fams, lctx.pstate = tokens, prices, fams, pstate
        _PRICED.update(ctx=lctx, raw=e5.RawPanel(), rec=e5.Recovery(lctx), memo={})
    rec = _PRICED["rec"]
    fam_ids = sorted({h["family"]["family_id"] for h in hs if h["family"]["family_id"]})
    own = defaultdict(lambda: None)
    for h in hs:
        k = (h["protocol"], h["chain_id"], h["market"])
        own[k] = h["onset_ts"] if own[k] is None else min(own[k], h["onset_ts"])
    key = (tuple(fam_ids), tuple(sorted(own.items())))
    if key in _PRICED["memo"]:
        return _PRICED["memo"][key]
    out = {"valuation_utc": rec.val_utc, "threshold_usd": PRICED_LOSS_MIN_USD,
           "rule": "sum over the event's markets of max(principal debt - collateral x recovery "
                   "price, 0) + bad debt written off, from each market's first trigger to T "
                   "(e5_events.market_shortfall)",
           "principal_shortfall_usd": 0.0, "bad_debt_usd": 0.0, "unpriced_markets": [],
           "markets": []}
    if min(own.values()) >= rec.val_ts:
        out.update(selected=False, loss_usd=None, not_valued="event starts after T")
        _PRICED["memo"][key] = out
        return out
    ev = {"event_id": "priced", "families": fam_ids, "affected_markets": []}
    for (proto, cid, m), ts in sorted(own.items()):
        if ts >= rec.val_ts:
            out["unpriced_markets"].append(f"{proto}:{cid}:{m} (trigger after T)")
            continue
        r = e5.market_shortfall(_PRICED["ctx"], _PRICED["raw"], rec, ev,
                                {"protocol": proto, "chain_id": cid, "market": m,
                                 "first_trigger_ts": ts})
        ps, bd = r.get("principal_shortfall_usd"), r.get("bad_debt_usd") or 0.0
        if ps is None:
            out["unpriced_markets"].append(f"{proto}:{cid}:{m} ({r.get('unread_reason')})")
        else:
            out["principal_shortfall_usd"] += ps
        out["bad_debt_usd"] += bd
        out["markets"].append({
            "protocol": proto, "chain_id": cid, "market": m, "reference_utc": utc(ts),
            "principal_shortfall_usd": ps, "bad_debt_usd": bd,
            "debt_family_principal_T": r.get("debt_family_principal_T"),
            "collateral": [{k: c[k] for k in ("symbol", "units", "recovery_price_usd",
                                               "recovery_source")} for c in r.get("collateral", [])]})
    out["loss_usd"] = out["principal_shortfall_usd"] + out["bad_debt_usd"]
    out["selected"] = out["loss_usd"] >= PRICED_LOSS_MIN_USD
    _PRICED["memo"][key] = out
    return out


def trigger_hits(ep, prices, fams, heads):
    """Hits of an episode that are price triggers."""
    out = []
    for h in ep:
        v = hit_min_price(h, prices, fams, heads)[0]
        if h["kind"] == "depeg_loss" or (v is not None and v < DEPEG_LINE):
            out.append(h)
    return out


def price_trigger_episode(episodes, prices, fams, heads):
    """First price-trigger episode (trigger_hits), else None."""
    return next((ep for ep in episodes if trigger_hits(ep, prices, fams, heads)), None)


def recovery_day(ep, until_day, prices, fams, heads, n=RECOVERY_DAYS):
    """Day on which the collateral of a price-trigger episode had recovered (module docstring, "Depeg-loss onset")."""
    toks = sorted({(h["chain_id"], h["collateral_token"]) for h in trigger_hits(ep, prices, fams, heads)
                   if h["collateral_token"]})
    dipped, run = False, 0
    for d in range(min(h["onset_ts"] for h in ep) // DAY, until_day):
        vals = [v for v in (family_price(prices, fams, cid, t, d) for cid, t in toks) if v is not None]
        if not vals:
            run = 0
            continue
        v = min(vals)
        if v < DEPEG_LINE:
            dipped, run = True, 0
        elif dipped and v >= RECOVERY_LINE:
            run += 1
            if run >= n:
                return d
        else:
            run = 0
    return None


def split_recovered(episodes, prices, fams, heads, n=RECOVERY_DAYS):
    """(episodes of the depeg loss, episodes of the recovered dip, day the collateral had recovered) of one depeg-loss event."""
    trig = [i for i, ep in enumerate(episodes) if trigger_hits(ep, prices, fams, heads)]
    cut = None
    for a, b in zip(trig, trig[1:]):
        rd = recovery_day(episodes[a], min(h["onset_ts"] for h in episodes[b]) // DAY,
                          prices, fams, heads, n)
        if rd is not None:
            cut = (b, rd)
    if cut is None:
        return episodes, [], None
    b, rd = cut
    dip = [ep for i, ep in enumerate(episodes) if i < b and min(h["onset_ts"] for h in ep) // DAY <= rd]
    return [ep for ep in episodes if not any(ep is x for x in dip)], dip, rd


def split_events(merged, ctx, n=RECOVERY_DAYS):
    """The event list after split_recovered."""
    prices, fams, heads = ctx[1:4]
    out, dips = [], []
    for k, eps in enumerate(merged, 1):
        split = None
        if event_loss_rule(eps, ctx)[0]:
            keep, dip, rd = split_recovered(eps, prices, fams, heads, n)
            if dip and event_loss_rule(keep, ctx)[0]:
                eps = keep
                split = {"recovered_dips": []}
                if any(h["kind"] == "lockup" for ep in dip for h in ep):
                    dips.append((k, dip, rd, split))
                else:
                    split["recovered_dips"].append(_dip_row(None, dip, rd, n))
        out.append((k, eps, split))
    dips.sort(key=lambda x: (min(h["onset_ts"] for ep in x[1] for h in ep), x[0]))
    for j, (k, dip, rd, split) in enumerate(dips, len(merged) + 1):
        split["recovered_dips"].append(_dip_row(f"E2-{j:03d}", dip, rd, n))
        out.append((j, dip, {"recovered_dip_of": f"E2-{k:03d}", "recovered_utc": day_str(rd),
                             "recovery_days": n}))
    return out


def _dip_row(event_id, dip, rd, n):
    """recovered_dips entry of a depeg loss."""
    return {"event_id": event_id, "onset_utc": utc(min(h["onset_ts"] for ep in dip for h in ep)),
            "episodes": [episode_row(ep) for ep in dip], "recovered_utc": day_str(rd),
            "recovery_days": n, "dropped": None if event_id else "no lock-up hit"}


def guard_reference(proto, cid, market, t0, pstate, md):
    """Reference point of the accrual guard for one market of an event."""
    tss, rows = pstate.series(proto, cid, market)
    i = bisect.bisect_right(tss, t0) - 1
    if i < 0:
        return None
    pinned = util_of(rows[i][3], rows[i][4]) >= MAIN[0]
    j = i
    if pinned:
        while j > 0 and util_of(rows[j - 1][3], rows[j - 1][4]) >= MAIN[0]:
            j -= 1
    ref_ts = rows[j][2] if pinned else t0
    fac = md.at(ref_ts // DAY, md.fac) if md is not None else 1.0
    aps_ref = aps_of(proto, rows[j][3], rows[j][5]) * fac
    rate = next((rows[k][6] for k in range(j, -1, -1) if rows[k][6] is not None), None)
    r = GUARD_MAX_APR if rate is None else min(max(rate, 0.0), GUARD_MAX_APR)
    return ref_ts, aps_ref, r, pinned


def guard_factor(ref, ts, aps_now):
    """Multiplier (<= 1) that caps a position valued at aps_now at time ts."""
    if ref is None or ts < ref[0] or aps_now <= 0:
        return 1.0
    return min(1.0, ref[1] * (1 + ref[2] * (ts - ref[0]) / YEAR) / aps_now)


def vault_exposure(s, markets, t0, end_ts, cid, proto, prices, pstate, mdays, weight, guards):
    """Exposure of one vault to an event's markets on one chain."""
    asset = s["asset"]
    t0d = t0 // DAY
    d0 = s["d0"]
    n = len(s["total"])

    held = [m for m in markets if m in s["pos"]]

    def gf(m, d):
        md = mdays.get(m)
        if md is None or guards.get(m) is None:
            return 1.0
        return guard_factor(guards[m], d * DAY + DAY - 1, md.at(d, md.aps))

    def x_at(i, d=None):
        """Guarded position in the event's markets at the end of day d0 + i."""
        d = d0 + i if d is None else d
        return sum(s["pos"][m][i] * gf(m, d) for m in held) if 0 <= i < n else 0.0

    def xw_at(i, d):
        return sum(s["pos"][m][i] * weight(m, d) * gf(m, d) for m in held) if 0 <= i < n else 0.0

    def capped(i, d):
        return 0 <= i < n and any(s["pos"][m][i] > 0 and gf(m, d) < 1 - 1e-12 for m in held)
    pos_t0 = 0.0
    by_market = {}
    capped_t0 = False
    for m in markets:
        if m not in s["pos"]:
            continue
        st = pstate.at(proto, cid, m, t0)
        if st is None:
            continue
        md = mdays.get(m)
        fac = md.at(t0d, md.fac) if md is not None else 1.0
        aps_t0 = aps_of(proto, st["supply"], st["shares"]) * fac
        g = guard_factor(guards.get(m), t0, aps_t0)
        if g < 1 - 1e-12:
            capped_t0 = True
        if s["kind"] == "euler_lending":
            by_market[m] = float(st["supply"]) * g
            pos_t0 += by_market[m]
            continue
        rr = s["rows"].get(m, [])
        k = bisect.bisect_right([x[2] for x in rr], t0)
        sh = rr[k - 1][3] if k > 0 else 0
        if written_off((s.get("queue") or {}).get(m), t0):
            sh = 0
        x = sh * aps_t0 * g
        if x > 0:
            by_market[m] = x
        pos_t0 += x
    exp_t0 = sum(x * weight(m, t0d) for m, x in by_market.items())
    window = range(t0d - EXPOSURE_LOOKBACK_D, t0d)
    path = [(d * DAY + DAY - 1, x_at(d - d0), d) for d in window]
    path.append((t0, pos_t0, t0d))
    peak_assets, peak_day = max(((v, d) for _t, v, d in path), default=(0.0, None))
    peak_usd = max((prices.value(cid, asset, v, d) or 0.0 for _t, v, d in path), default=0.0)
    pos_t0_usd = prices.value(cid, asset, pos_t0, t0d)
    exp_t0_usd = prices.value(cid, asset, exp_t0, t0d)
    exp_peak_usd = max([prices.value(cid, asset, xw_at(d - d0, d), d) or 0.0
                        for d in window] + [exp_t0_usd or 0.0])
    after_days = [d for d in range(t0d, end_ts // DAY + 1) if 0 <= d - d0 < n]
    after_usd = max((prices.value(cid, asset, x_at(d - d0), d) or 0.0 for d in after_days),
                    default=0.0)
    exp_after_usd = max((prices.value(cid, asset, xw_at(d - d0, d), d) or 0.0 for d in after_days),
                        default=0.0)
    before = max(peak_usd, pos_t0_usd or 0.0) >= MATERIAL_USD
    if not before and after_usd < MATERIAL_USD:
        return None
    exited = before and peak_assets > 0 and pos_t0 < EXIT_FRACTION * peak_assets
    lead = None
    if exited:
        above = [i for i, (_t, v, _d) in enumerate(path) if v >= LEAD_FRACTION * peak_assets]
        first_below = path[above[-1] + 1][0] if above and above[-1] + 1 < len(path) else None
        if first_below is not None:
            lead = round((t0 - first_below) / DAY, 2)
    locked = {x: 0 for x in (L_LOCK,) + L_SENS}
    min_l = None
    daily_w, daily_usd = {}, {}
    for d in window:
        if 0 <= d - d0 < n:
            daily_usd[d] = prices.value(cid, asset, x_at(d - d0), d)
    for d in range(t0d, end_ts // DAY + 1):
        i = d - d0
        if not 0 <= i < n:
            continue
        daily_w[d] = {m: s["pos"][m][i] * weight(m, d) * gf(m, d) for m in held if s["pos"][m][i] > 0}
        daily_usd[d] = prices.value(cid, asset, x_at(i), d)
        tot, wd = s["total"][i], s["wd"][i]
        for m in held:
            p, g = s["pos"][m][i], gf(m, d)
            if p > 0 and g < 1:
                av = mdays[m].at(d, mdays[m].avail)
                tot += p * g - p
                wd += min(p * g, av) - min(p, av)
        if tot <= 0 or (daily_usd[d] or 0.0) < MATERIAL_USD:
            continue
        lv = wd / tot
        min_l = lv if min_l is None else min(min_l, lv)
        for x in locked:
            if lv < x:
                locked[x] += 1
    ip = t0d - 1 - d0
    share_prev = x_at(ip) / s["total"][ip] if 0 <= ip < n and s["total"][ip] > 0 else None
    return {"vault": s["vault"], "kind": s["kind"], "name": s.get("name"), "asset": asset,
            "position_t0_raw": f"{pos_t0:.0f}", "position_t0_usd": pos_t0_usd,
            "share_of_vault_prev_day": share_prev,
            "peak_90d_usd": peak_usd, "peak_90d_day": day_str(peak_day) if peak_day else None,
            "group": ("entered_after_t0" if not before else
                      "exited_before_t0" if exited else "present_at_t0"),
            "max_position_usd_after_t0": after_usd,
            "position_t0_by_market": {m: f"{x:.0f}" for m, x in sorted(by_market.items())},
            "exposure_t0_usd": exp_t0_usd, "peak_90d_exposure_usd": exp_peak_usd,
            "max_exposure_usd_after_t0": exp_after_usd,
            "guard_capped_at_t0": capped_t0,
            "written_off": {m: d for m, d in (s.get("written_off") or {}).items() if m in held},
            "guard_capped_days": sum(1 for d in list(window) + after_days if capped(d - d0, d)),
            "_daily_w": daily_w, "_daily_usd": daily_usd,
            "exit_lead_days": lead, "days_locked": locked[L_LOCK],
            "days_locked_1pct": locked[0.01], "days_locked_10pct": locked[0.10],
            "min_L_after_t0": min_l}


_SHARE_DAYS = {}
_CV_TOK = {}


def family_debt_share(cid, market, day, fam_ids, fams):
    """Share of an Euler lending vault's attributed debt that collateral of the families `fam_ids` backs, on the latest borrow-distribution day at or before `day`."""
    dist = euler_dist_cached(cid).get(market)
    if not dist:
        return None
    key = (cid, market)
    if key not in _SHARE_DAYS:
        _SHARE_DAYS[key] = sorted(dist)
        _CV_TOK[key] = {v: t for t, v in market_info_cached("euler_v2", cid)
                        .get(market, {}).get("collaterals", [])}
    days = _SHARE_DAYS[key]
    i = bisect.bisect_right(days, day) - 1
    if i < 0:
        return None
    att = dist[days[i]]
    if not att:
        return 0.0
    fam_debt = sum(a for cv, a in att.items()
                   if fams.resolve(cid, _CV_TOK[key].get(cv) or ZERO)["family_id"] in fam_ids)
    return fam_debt / sum(att.values())


_VAULTS = {}


def registry_vaults():
    """{(chain_id, vault): registry row} over the 14 pairs."""
    if not _VAULTS:
        for proto, cid in PAIRS:
            for r in jsonl(DATA / f"registry/vaults/{proto}/{cid}.jsonl"):
                _VAULTS[(cid, r["vault"])] = dict(r, protocol=proto)
    return _VAULTS


def issuer_of(fam, markets, t0, curators):
    """(entity, rule) of the curator-registry entity that issues the event's collateral family."""
    lab = (fam.get("label") or "").upper()
    if lab in ISSUERS:
        return ISSUERS[lab][0], f"issuer table: {ISSUERS[lab][1]}"
    toks = {(m["chain_id"], m.get("collateral_token")) for m in markets}
    if len(toks) == 1:
        cid, tok = next(iter(toks))
        if tok and (cid, tok) in registry_vaults():
            ent, _ = curator_at(curators, cid, tok, t0)
            if ent:
                return ent, f"the collateral {tok} is a curated vault of {ent}"
    return None, None


MANAGER_ROLES = {"owner", "curator", "DEFAULT_ADMIN_ROLE", "MANAGER", "CURATOR"}
_ROLES = {}


def role_holders(proto, cid, vault):
    """Every address that ever held the vault's owner or curator role (Moolah."""
    if (proto, cid) not in _ROLES:
        acc = defaultdict(set)
        p = DATA / f"vaults/{proto}/{cid}/roles.jsonl"
        if p.exists():
            for r in jsonl(p):
                if r.get("role") in MANAGER_ROLES and r.get("address"):
                    acc[r["vault"]].add(r["address"].lower())
        for (c, v), r in registry_vaults().items():
            if c == cid and r["protocol"] == proto:
                for k in ("caller", "initial_owner", "curator", "manager"):
                    if r.get(k):
                        acc[v].add(r[k].lower())
        _ROLES[(proto, cid)] = acc
    return _ROLES[(proto, cid)].get(vault, set())


def self_deposited(proto, cid, vault, ts):
    """True when one address holds >= 90% of the vault's net deposited shares up to ts (flows/, share transfers not followed) a."""
    p = DATA / f"vaults/{proto}/{cid}/flows/{vault}.csv"
    if not p.exists():
        return None
    net = Counter()
    with open(p) as fh:
        for r in csv.DictReader(fh):
            if int(r["timestamp"]) > ts:
                break
            sh = int(r["shares"] or 0)
            net[r["owner"].lower()] += sh if r["kind"].lower() == "deposit" else -sh
    held = {a: v for a, v in net.items() if v > 0}
    if not held:
        return False
    top, v = max(held.items(), key=lambda x: x[1])
    return v / sum(held.values()) >= SELF_DEPOSIT_SHARE and top in role_holders(proto, cid, vault)


def load_curators():
    """Attribution intervals from data/registry/vault_curators.jsonl when that file exists."""
    p = DATA / "registry/vault_curators.jsonl"
    if not p.exists():
        return None, None
    out = defaultdict(list)
    for r in jsonl(p):
        out[(int(r["chain_id"]), r["vault"].lower())].append(
            (r.get("from_ts") or 0, r.get("to_ts"), r.get("entity_id"), r.get("confidence")))
    meta = json.loads(p.with_name(p.name + ".meta.json").read_text()) \
        if p.with_name(p.name + ".meta.json").exists() else {}
    return dict(out), {"file": "data/registry/vault_curators.jsonl", "sha256": sha256(p),
                       "rows": sum(len(v) for v in out.values()),
                       "written_utc": meta.get("written_utc"), "generator": meta.get("generator")}


def curator_in_window(curators, cid, vault, t0, end):
    """Curator at t0, or for a vault attributed only later, at the first attributed interval (else the first interval) that opens before the event ends."""
    ent, conf = curator_at(curators, cid, vault, t0)
    if conf != "no interval" or curators is None:
        return ent, conf
    later = sorted((iv for iv in curators.get((int(cid), vault.lower()), []) if t0 < iv[0] <= end),
                   key=lambda iv: (iv[2] is None, iv[0]))
    return (later[0][2], later[0][3]) if later else (None, "no interval")


def curator_at(curators, cid, vault, ts):
    """(entity_id, confidence) of the attribution interval covering ts."""
    if curators is None:
        return None, None
    for a, b, ent, conf in curators.get((int(cid), vault.lower()), []):
        if a <= ts and (b is None or ts < b):
            return ent, conf
    return None, "no interval"


GUARDED_DAILY = {}


def episode_row(ep):
    """Summary of one episode (a group merge_hits made) inside an event."""
    kinds = {h["kind"] for h in ep}
    return {"onset_utc": utc(min(h["onset_ts"] for h in ep)),
            "onset_ts": min(h["onset_ts"] for h in ep),
            "end_utc": utc(max(h["end_ts"] for h in ep)), "end_ts": max(h["end_ts"] for h in ep),
            "type": "depeg_loss" if "depeg_loss" in kinds else "lockup",
            "family_id": ep[0]["family"]["family_id"],
            "families": sorted({h["family"]["family_id"] or "" for h in ep}),
            "markets": sorted({f"{h['protocol']}:{h['chain_id']}:{h['market']}" for h in ep}),
            "hit_ids": [h["hit_id"] for h in ep],
            "census_events": sorted({h["census_event_id"] for h in ep if h.get("census_event_id")})}


def event_loss_rule(episodes, ctx):
    """(loss_rule, priced_loss) of one event."""
    tokens, prices, fams, _heads, _vs, pstate, _cur = ctx
    hs = [h for ep in episodes for h in ep]
    if any(h["kind"] == "depeg_loss" for h in hs):
        return "depeg_hit", None
    pl = priced_loss(hs, tokens, prices, fams, pstate)
    return ("priced_shortfall" if pl["selected"] else None), pl


def build_event(k, episodes, ctx, split=None):
    tokens, prices, fams, heads, vseries, pstate, curators = ctx
    hs = sorted((h for ep in episodes for h in ep), key=lambda h: (h["onset_ts"], h["hit_id"]))
    split = split or {}
    loss_rule, priced = (None, None) if split.get("recovered_dip_of") else \
        event_loss_rule(episodes, ctx)
    first_any = hs[0]
    dep_ep = None
    if loss_rule:
        dep_ep = price_trigger_episode(episodes, prices, fams, heads) or episodes[0]
    first = min(dep_ep, key=lambda h: (h["onset_ts"], h["hit_id"])) if dep_ep else first_any
    t0 = first["onset_ts"]
    earlier = [ep for ep in episodes if dep_ep is not None and ep is not dep_ep
               and min(h["onset_ts"] for h in ep) < t0]
    end = max(h["end_ts"] for h in hs)
    ongoing = any(h.get("ongoing") for h in hs)
    fam = next((h for h in hs if h["kind"] == "depeg_loss"), hs[0])["family"]
    fam_ids = {h["family"]["family_id"] for h in hs}
    kinds = {h["kind"] for h in hs}
    by_market = defaultdict(list)
    for h in hs:
        by_market[(h["protocol"], h["chain_id"], h["market"])].append(h)
    markets = []
    guards = defaultdict(dict)
    supply_t0 = supply_t0_whole = 0.0
    for (proto, cid, m), mh in sorted(by_market.items()):
        info = market_info_cached(proto, cid).get(m, {})
        loan = info.get("loan")
        st = pstate.at(proto, cid, m, t0)
        own = min(x["onset_ts"] for x in mh)
        st_own = pstate.at(proto, cid, m, own)
        usd_t0 = prices.value(cid, loan, st["supply"], t0 // DAY) if st and loan else None
        usd_own = prices.value(cid, loan, st_own["supply"], own // DAY) if st_own and loan else None
        orc = next((x["run"]["oracle"] for x in mh if x["kind"] == "lockup"), None)
        if orc is None:
            orc = info.get("oracle") or {}
            if proto == "euler_v2":
                cv = next((x.get("collateral_vault") for x in mh if x.get("collateral_vault")), None)
                orc = info.get("oracle_by_collateral", {}).get(cv, {})
        debt_share = family_debt_share(cid, m, t0 // DAY, fam_ids, fams) \
            if proto == "euler_v2" else None
        gref = guard_reference(proto, cid, m, t0, pstate,
                               vseries[f"{proto}:{cid}"]["mdays"].get(m))
        guards[(proto, cid)][m] = gref
        supply_t0_whole += usd_t0 or 0.0
        supply_t0 += (usd_t0 or 0.0) * (debt_share if debt_share is not None else 1.0)
        markets.append({
            "guard_reference_utc": utc(gref[0]) if gref else None,
            "guard_rate_apr": gref[2] if gref else None,
            "pinned_at_t0": gref[3] if gref else None,
            "family_debt_share_at_t0": debt_share,
            "protocol": proto, "chain_id": cid, "market": m, "loan_token": loan,
            "loan_symbol": tokens.symbol(cid, loan) if loan else None,
            "collateral_token": mh[0]["collateral_token"],
            "collateral_symbol": tokens.symbol(cid, mh[0]["collateral_token"])
            if mh[0]["collateral_token"] else None,
            "first_trigger_utc": utc(own), "first_trigger_ts": own,
            "supply_usd_at_t0": usd_t0, "supply_usd_at_own_trigger": usd_own,
            "hit_kinds": sorted({x["kind"] for x in mh}), "hit_ids": [x["hit_id"] for x in mh],
            "oracle_type": orc.get("type"), "oracle_subtype": orc.get("subtype"),
            "blind_to": orc.get("blind_to")})
    by_src = defaultdict(list)
    for h in hs:
        v, src = hit_min_price(h, prices, fams, heads)
        if v is not None:
            by_src[h["source"]].append((v, src))
    pick = by_src.get("depeg_census") or by_src.get("moolah_scan") or by_src.get("lockup_rule") or []
    minp = min(pick, default=(None, None), key=lambda x: x[0])
    min_by_source = {k: min(x[0] for x in v) for k, v in by_src.items()}
    vaults = []
    by_pair = defaultdict(set)
    for proto, cid, m in by_market:
        by_pair[(proto, cid)].add(m)
    for (proto, cid), ms in sorted(by_pair.items()):
        vs = vseries[f"{proto}:{cid}"]
        holders = set()
        for m in ms:
            holders |= set(vs["index"].get(m, set()))
            if proto == "euler_v2":
                holders.add(f"self:{m}")
        for v in sorted(holders):
            sser = vs["series"].get(v)
            if sser is None:
                continue
            if sser["kind"] == "euler_lending":
                gov = sser.get("governors", [])
                if (governor_at(gov, max(t0, gov[0][0]) if gov else t0) or ZERO) == ZERO:
                    continue

            def weight(m, d, cid=cid, proto=proto):
                if proto != "euler_v2":
                    return 1.0
                sh = family_debt_share(cid, m, d, fam_ids, fams)
                return 1.0 if sh is None else sh
            ex = vault_exposure(sser, sorted(ms), t0, end, cid, proto, prices, pstate,
                                vs["mdays"], weight, guards[(proto, cid)])
            if ex is None:
                continue
            ent, conf = curator_in_window(curators, cid, ex["vault"], t0, end)
            ex.update({"protocol": proto, "chain_id": cid, "curator": ent,
                       "curator_confidence": conf, "_weight": weight})
            vaults.append(ex)
    present = [v for v in vaults if v["group"] == "present_at_t0" and
               (v["position_t0_usd"] or 0) >= MATERIAL_USD]
    issuer, issuer_rule = issuer_of(fam, markets, t0, curators)
    excl = event_exclusion(hs)
    t0d = t0 // DAY
    earn_in = defaultdict(float)
    for v in vaults:
        if v["kind"] == "euler_earn":
            for m, x in v["position_t0_by_market"].items():
                earn_in[(v["chain_id"], m)] += float(x)
    third_party = 0.0
    for v in vaults:
        weight = v.pop("_weight")
        v["issuer_curated"] = issuer is not None and v["curator"] == issuer
        ctl = SELF_DEALING_VAULTS.get((v["chain_id"], v["vault"]))
        sd = self_deposited(v["protocol"], v["chain_id"], v["vault"],
                            min(end, heads[f"{v['protocol']}:{v['chain_id']}"]["ts"])) \
            if v["curator"] is None and v["kind"] != "euler_lending" else None
        v["self_deposited"] = True if ctl else sd
        v["self_dealing_controller"] = ctl[1] if ctl else None
        v["third_party"] = not v["issuer_curated"] and not v["self_deposited"]
        counted = v["exposure_t0_usd"] or 0.0
        if v["kind"] == "euler_lending":
            m = v["vault"]
            net = max(float(v["position_t0_by_market"].get(m, 0)) - earn_in[(v["chain_id"], m)], 0.0)
            counted = prices.value(v["chain_id"], v["asset"], net * weight(m, t0d), t0d) or 0.0
        v["exposure_t0_usd_counted"] = counted
        if v["third_party"]:
            third_party += counted
    exposed = [v for v in vaults if max(v["exposure_t0_usd"] or 0.0, v["peak_90d_exposure_usd"],
                                        v["max_exposure_usd_after_t0"]) >= MATERIAL_USD]
    earn_daily = defaultdict(lambda: defaultdict(float))
    for v in vaults:
        if v["kind"] == "euler_earn":
            for d, by_m in v["_daily_w"].items():
                for m, x in by_m.items():
                    earn_daily[(v["chain_id"], m)][d] += x
    daily_tp = defaultdict(float)
    for v in vaults:
        if not v["third_party"]:
            continue
        for d, by_m in v["_daily_w"].items():
            if v["kind"] == "euler_lending":
                raw = max(by_m.get(v["vault"], 0.0) - earn_daily[(v["chain_id"], v["vault"])][d], 0.0)
            else:
                raw = sum(by_m.values())
            daily_tp[d] += prices.value(v["chain_id"], v["asset"], raw, d) or 0.0
    tp_peak_day = max(daily_tp, key=lambda d: (daily_tp[d], -d)) if daily_tp else None
    tp_peak = daily_tp[tp_peak_day] if tp_peak_day is not None else 0.0
    for v in vaults:
        v.pop("_daily_w")
        GUARDED_DAILY[(f"E2-{k:03d}", v["protocol"], v["chain_id"], v["vault"], v["kind"])] = \
            v.pop("_daily_usd")
    return {
        "event_id": f"E2-{k:03d}", "type": "depeg_loss" if loss_rule else "lockup",
        "loss_rule": loss_rule, "priced_loss": priced,
        "hit_kinds": sorted(kinds), "onset_utc": utc(t0), "onset_ts": t0,
        "first_trigger": {"hit_id": first["hit_id"], "kind": first["kind"],
                          "protocol": first["protocol"], "chain_id": first["chain_id"],
                          "market": first["market"]},
        "first_trigger_any": {"hit_id": first_any["hit_id"], "kind": first_any["kind"],
                              "onset_utc": utc(first_any["onset_ts"]), "onset_ts": first_any["onset_ts"],
                              "protocol": first_any["protocol"], "chain_id": first_any["chain_id"],
                              "market": first_any["market"]},
        "earlier_lockup_episodes": [episode_row(ep) for ep in earlier],
        "recovered_dips": split.get("recovered_dips", []),
        "recovered_dip_of": split.get("recovered_dip_of"),
        "recovered_utc": split.get("recovered_utc"),
        "end_utc": utc(end), "end_ts": end, "ongoing_at_head": ongoing,
        "census_window_open_at_census_head": any(
            h["source"] == "depeg_census" and h["end_ts"] >= ctx_census_head() - DAY for h in hs),
        "protocols": sorted({h["protocol"] for h in hs}),
        "chains": sorted({h["chain_id"] for h in hs}),
        "collateral_family": {"family_id": fam["family_id"], "label": fam["label"],
                              "census_uid": fam["census_uid"], "peg": fam["peg"]},
        "affected_markets": markets, "n_markets": len(markets),
        "max_pinned_share_prior_180d": max((h["run"]["pinned_share_prior_180d"] or 0.0
                                            for h in hs if h["kind"] == "lockup"), default=None),
        "min_market_price": minp[0], "min_market_price_source": minp[1],
        "min_market_price_by_hit_source": min_by_source,
        "affected_supply_usd_at_t0": supply_t0,
        "affected_supply_usd_at_t0_whole_markets": supply_t0_whole,
        "affected_supply_usd_at_own_trigger": sum(x["supply_usd_at_own_trigger"] or 0.0 for x in markets),
        "excluded": excl[0], "excluded_description": excl[1],
        "issuer_entity": issuer, "issuer_rule": issuer_rule,
        "exposure_third_party_usd_at_t0": third_party,
        "exposure_third_party_usd_peak_after_t0": tp_peak,
        "exposure_third_party_peak_date": day_str(tp_peak_day) if tp_peak_day is not None else None,
        "exposure_third_party_usd_rank": max(third_party, tp_peak),
        "guard_capped_vault_days": sum(v["guard_capped_days"] for v in vaults),
        "self_curated": bool(exposed) and all(v["issuer_curated"] for v in exposed),
        "n_vaults_exposed_to_family": len(exposed),
        "families": sorted({h["family"]["family_id"] or "" for h in hs}),
        "episodes": [episode_row(ep) for ep in episodes], "n_episodes": len(episodes),
        "exposed_vaults": vaults, "n_vaults_present_at_t0": len(present),
        "n_vaults_exited_before_t0": sum(1 for v in vaults if v["group"] == "exited_before_t0"),
        "n_vaults_entered_after_t0": sum(1 for v in vaults if v["group"] == "entered_after_t0"),
        "curators_present_at_t0": None if curators is None else sorted(
            {v["curator"] for v in present if v["curator"]}),
        "census_events": sorted({h["census_event_id"] for h in hs if h.get("census_event_id")}),
        "census_rows": [h["provenance"]["row_id"] for h in hs if h.get("provenance")],
        "lockup_runs": [run_id(h["run"]) for h in hs if h["kind"] == "lockup"],
        "moolah_hits": [h["hit_id"] for h in hs if h["source"] == "moolah_scan"]}


_MI = {}
_ED = {}


def euler_dist_cached(cid):
    if cid not in _ED:
        _ED[cid] = euler_distribution(cid)
    return _ED[cid]


def market_info_cached(proto, cid):
    if (proto, cid) not in _MI:
        _MI[(proto, cid)] = market_info(proto, cid)
    return _MI[(proto, cid)]


_CENSUS_HEAD = []


def ctx_census_head():
    """Latest window end in the census table (its data head)."""
    if not _CENSUS_HEAD:
        _CENSUS_HEAD.append(max(h["to_ts"] for e in jsonl(CENSUS_EVENTS) for h in e["hits"]))
    return _CENSUS_HEAD[0]


def depeg_hits(depeg, fams):
    """Depeg hits with their family; a census window that runs to the census head, or a Moolah window open at our head, is open."""
    out = []
    for h in depeg["census"] + depeg["moolah"]:
        h = dict(h)
        h["family"] = fams.resolve(h["chain_id"], h["collateral_token"])
        h["open"] = bool(h.get("ongoing")) if h["source"] == "moolah_scan" else \
            h["end_ts"] >= ctx_census_head() - DAY
        out.append(h)
    return out


def stage_events(work):
    _WORK[:] = [work]
    tokens = Tokens(work.load("tokens.pkl"))
    fams = Families(tokens)
    census_pegs(tokens, fams)
    prices = Prices(tokens, set(tokens.rows))
    heads = work.load("heads.pkl")
    depeg = work.load("depeg.pkl")
    runs = work.load("runs.pkl")
    vseries = work.load("vault_series.pkl")
    curators, cur_meta = load_curators()
    pstate = PanelState()
    dh = depeg_hits(depeg, fams)
    ctx = (tokens, prices, fams, heads, vseries, pstate, curators)

    def counts(episodes):
        evs = split_events(merge_episodes(episodes), ctx)
        types = Counter("depeg_loss" if not (sp or {}).get("recovered_dip_of")
                        and event_loss_rule(e, ctx)[0] else "lockup" for _k, e, sp in evs)
        n_excl = sum(1 for _k, e, _sp in evs if event_exclusion([h for ep in e for h in ep])[0])
        return {"episodes": len(episodes), "events": len(evs), "events_after_exclusions": len(evs) - n_excl,
                "depeg_loss_events": types["depeg_loss"], "lockup_events": types["lockup"]}
    sens = []
    for t in THRESHOLDS:
        for dur in DURS_H:
            lh = lockup_hits(runs, heads, t, dur)
            step_one = sum(1 for r in runs if r["threshold"] == t and
                           run_end(r["run"], heads[f"{r['protocol']}:{r['chain_id']}"]["ts"])
                           - r["run"]["start_ts"] >= dur * 3600)
            sens.append({"utilization_threshold": t, "duration_h": dur, "step_one_runs": step_one,
                         "kept_runs": len(lh), **counts(merge_hits(dh + lh)),
                         "main": (t, dur) == MAIN})
    lh = lockup_hits(runs, heads, *MAIN)
    episodes = merge_hits(dh + lh)
    sens.append({"utilization_threshold": MAIN[0], "duration_h": MAIN[1],
                 "merge": "census rule without the 30-day horizon for open windows",
                 "kept_runs": len(lh), **counts(merge_hits(dh + lh, horizon=None)), "main": False})
    merged = merge_episodes(episodes)
    t0s = {}
    for n in sorted(RECOVERY_SENS + (RECOVERY_DAYS,)):
        for k, eps, sp in split_events(merged, ctx, n):
            if not (sp or {}).get("recovered_dip_of") and event_loss_rule(eps, ctx)[0]:
                ep = price_trigger_episode(eps, prices, fams, heads) or eps[0]
                t0s.setdefault(f"E2-{k:03d}", {})[f"{n}d"] = utc(min(h["onset_ts"] for h in ep))
    for k, eps in enumerate(merged, 1):
        if event_loss_rule(eps, ctx)[0]:
            ep = price_trigger_episode(eps, prices, fams, heads) or eps[0]
            t0s[f"E2-{k:03d}"]["no_split"] = utc(min(h["onset_ts"] for h in ep))
    rec_sens = {"recovery_line": RECOVERY_LINE, "main_days": RECOVERY_DAYS,
                "t0_by_recovery_days": t0s,
                "changed": {eid: v for eid, v in t0s.items() if len(set(v.values())) > 1}}
    GUARDED_DAILY.clear()
    events = [build_event(k, eps, ctx, sp) for k, eps, sp in split_events(merged, ctx)]
    work.save("guarded_daily.pkl", dict(GUARDED_DAILY))
    write_events(events, dh, lh, sens, tokens, fams, depeg, cur_meta, rec_sens)
    return {"events": len(events), "episodes": len(episodes),
            "recovered_dips": [(e["event_id"], e["recovered_dip_of"], e["onset_utc"][:10])
                               for e in events if e["recovered_dip_of"]],
            "t0_changed_by_recovery_days": rec_sens["changed"],
            "by_type": dict(Counter(e["type"] for e in events)),
            "by_loss_rule": dict(Counter(e["loss_rule"] or "none" for e in events)),
            "priced_shortfall_events": [(e["event_id"], e["collateral_family"]["label"],
                                         round(e["priced_loss"]["loss_usd"]))
                                        for e in events if e["loss_rule"] == "priced_shortfall"],
            "self_curated": sum(e["self_curated"] for e in events),
            "excluded": sum(1 for e in events if e["excluded"]),
            "guard_capped_vault_days": sum(e["guard_capped_vault_days"] for e in events),
            "depeg_hits": len(dh), "lockup_hits": len(lh),
            "curators_joined": cur_meta is not None}


def month_bounds(month):
    a = dt.datetime.fromisoformat(month + "-01").replace(tzinfo=dt.timezone.utc)
    b = (a + dt.timedelta(days=32)).replace(day=1)
    return int(a.timestamp()), int(b.timestamp()) - 1


def diagnose_miss(case, fams, heads, tokens, prices, runs_eval):
    """Why no event of the case's family overlaps its month."""
    lo, hi = month_bounds(case["month"])
    kept = {(run_id(r), r["threshold"]): r["kept"] for r in runs_eval}
    out = []
    for proto, cid in PAIRS:
        scan = work_global().load(f"scan/{proto}_{cid}.pkl")
        info = market_info_cached(proto, cid)
        head_ts = heads[f"{proto}:{cid}"]["ts"]
        for m, res in scan.items():
            row = info.get(m)
            if not row:
                continue
            fids = {fams.resolve(cid, t)["family_id"] for t, _ in row["collaterals"] if t}
            if not fids & case["families"]:
                continue
            for t in THRESHOLDS:
                for r in res["runs"][t]:
                    end = run_end(r, head_ts)
                    if end < lo - 30 * DAY or r["start_ts"] > hi + 30 * DAY:
                        continue
                    peak, _ = peak_supply_usd(r, prices, cid, row["loan"], head_ts)
                    rid = f"{proto}:{cid}:{m}:{r['start_block']}:{r['start_li']}"
                    out.append({"protocol": proto, "chain_id": cid, "market": m,
                                "loan_symbol": tokens.symbol(cid, row["loan"]),
                                "threshold": t, "start_utc": utc(r["start_ts"]),
                                "end_utc": utc(r["end_ts"]), "duration_h": round((end - r["start_ts"]) / 3600, 1),
                                "peak_supply_usd": peak, "step_two_kept": kept.get((rid, t))})
    return out


_WORK = []


def work_global():
    return _WORK[0]


def write_events(events, dh, lh, sens, tokens, fams, depeg, cur_meta, rec_sens):
    prices = Prices(tokens, set(tokens.rows))
    heads = work_global().load("heads.pkl")
    runs_eval = work_global().load("runs.pkl")
    prov = depeg["provenance"]
    io_utils.write_jsonl(f"{OUT}/depeg_events.jsonl", sorted(dh, key=lambda h: (h["onset_ts"], h["hit_id"])),
                         GENERATOR, {
        "what": "depeg-loss hits on the 14 pairs: the census table imported hit by hit "
                "(Morpho, Euler, Silo) and the Moolah scan by the same rules",
        "census_provenance": prov, "census_hits_out_of_scope": depeg["out_of_scope"],
        "market_mapping": "morpho:<cid>:<id> -> id; euler:<cid>:<lending>:<collateral> -> lending "
                          "vault; silo:<cid>:<silo config> -> the silo of that config whose paired "
                          "silo holds the family token",
        "moolah_rules": "I1: oracle peek(collateral)/peek(loan) over the DefiLlama close ratio >= 1.08 "
                        "on two consecutive day ends; I2: utilization >= 99% for >= 72 h; I3: borrow "
                        "APR >= 50% of the market's rate cap (30% APR IRM maximum where none is set); "
                        "depeg: a daily close <= 0.97 of the family peg (wrappers through their unwrap "
                        "terminal); all within [window start - 30 d, window end] (R19); onset = "
                        "max(window start, first depeg close); I4 not scanned (as in the census)",
        "moolah_rpc_requests_last_run": depeg["moolah_rpc"],
        "family": "see collateral_families.jsonl"})
    rows = []
    for e in events:
        rows.append(e)
    io_utils.write_jsonl(f"{OUT}/events.jsonl", rows, GENERATOR, {
        "what": "E2 stress-event set: depeg-loss hits and kept lock-up runs merged by collateral "
                "family (overlap or gap < 7 days, across chains; a window still open at the data "
                "head counts for the merge up to 30 days after its onset) into episodes; episodes "
                "with lock-up runs on the same market, or the same family, chain and protocol, "
                "whose onsets are within 180 days of each other form one event (episodes lists "
                "them); merge_rules lists the rules on top of the census family identity",
        "merge_rules": {"on": list(MERGE_RULES),
                        "issuer_lineage": "a wrapper, staked version or receipt of an issuer asset "
                                          "is in that asset's family (census unwrap chain at row "
                                          "level or the reviewed LINEAGE_TABLE; not through the "
                                          "base assets of LINEAGE_BASE)",
                        "same_issuer": "families of one issuer (ISSUERS by label, else "
                                       "FAMILY_ISSUERS) join when each has a depeg-loss hit and "
                                       "those depeg-loss windows overlap or are < 7 days apart",
                        "type": "depeg_loss when any hit is a depeg loss (loss_rule depeg_hit) or "
                                "the event's priced principal loss is at least "
                                f"{PRICED_LOSS_MIN_USD:,.0f} USD (loss_rule priced_shortfall), "
                                "else lockup"},
        "priced_loss": "for an event without a depeg-loss hit: summed over its markets, the "
                       "principal shortfall at the recovery price at T plus the bad "
                       "debt written off, from each market's first trigger (E5's market_shortfall); "
                       "a market whose collateral has no recovery price is listed in "
                       "unpriced_markets and adds nothing; selected when loss_usd >= threshold_usd",
        "collateral_family": "the family of the first depeg-loss hit, else of the first hit; "
                             "families lists all, and Euler debt shares count all of them",
        "third_party": "exposure_third_party_usd_at_t0 sums exposure_t0_usd_counted over vaults "
                       "with third_party true: curator not the issuer_entity and not "
                       "self_deposited (unattributed, one address holding >= 90% of net deposits "
                       "that also holds the owner or curator role); exposure weights each market "
                       "by family_debt_share (Euler lending vaults, 1 elsewhere) and an Euler "
                       "lending vault counts net of the Earn vaults listed in the same event",
        "excluded": "self_dealing_loop when every hit is a lock-up kept only by vaults of a "
                    "self-dealing controller listed in the script (data/resume/king-rss.md); the "
                    "event stays here and leaves Table 3; those vaults count as self-deposited",
        "guard": "a pinned market keeps accruing interest nobody pays; from a reference point (t0, "
                 "or the first row of the pinned run for a market already pinned at t0) every "
                 "position in the event's markets is valued at min(assets per share, assets per "
                 "share at the reference x (1 + r x years since)), r = the borrow APR at the "
                 "reference, at most 100%; guard_capped_days counts the vault-days the cap bound "
                 "(lookback and post-t0 day ends); L(t) for days_locked uses the capped values",
        "exposure_third_party_usd_peak_after_t0": "peak over day ends from t0's day to the event end "
                                                  "of the daily sum of the guarded, family-weighted "
                                                  "third-party positions (Euler lending vaults net of "
                                                  "the listed Earn vaults that day)",
        "self_curated": "true when at least one vault reaches 10k USD of exposure to the family "
                        "(at t0, in the 90 days before or after t0) and every such vault is curated "
                        "by the issuer_entity (for an Euler lending vault, its governor)",
        "issuer": "ISSUERS table in the script (registry entities that issue a collateral), or "
                  "the curator of a collateral that is itself a curated vault",
        "onset": "t0: for a lock-up event the earliest trigger among its hits (lock-up run "
                 "start); for a depeg-loss event the earliest trigger (census hit date, Moolah hit "
                 "date, or a lock-up run start) in the episode after which the collateral did not "
                 "recover: its first price-trigger episode (a depeg-loss hit, or a close below "
                 "0.97 of peg) once recovered dips are split off, else its first trigger; every "
                 "*_t0 field, exposure and vault group is read at t0",
        "first_trigger_any": "the earliest trigger among all the event's hits",
        "earlier_lockup_episodes": "lock-up episodes of a depeg-loss event that start before t0",
        "recovered_dips": "episodes split off a depeg-loss event because its collateral recovered "
                          f"after them ({RECOVERY_DAYS} consecutive closes at or above "
                          f"{RECOVERY_LINE} of peg after a close below 0.97, before the next "
                          "price-trigger episode): event_id of the lock-up event they form (null "
                          "when dropped for holding no lock-up hit) and recovered_utc, the day "
                          "the run completed",
        "recovered_dip_of": "on a split-off event: the depeg loss it was split from; its number "
                            "follows the merged events so that the others keep theirs",
        "end": "the latest hit end; lock-ups open at the panel head end at the head "
               "(ongoing_at_head); census hits end at their window end, which for windows open "
               "at the census head (2026-08-20) is that head",
        "affected_supply_usd_at_t0": "sum over affected markets of supply at the last panel row "
                                     "at or before t0, times the loan token's close on t0's day; an "
                                     "Euler lending vault serves many collaterals, so it counts with "
                                     "family_debt_share_at_t0, the share of its attributed debt "
                                     "(borrow_distribution.csv, latest day at or before t0) backed by "
                                     "the event's family; affected_supply_usd_at_t0_whole_markets "
                                     "counts every market whole",
        "min_market_price": "peg units; the census event minimum when the event has census hits "
                            "(depth-checked pools), else the Moolah scan's or the lock-up runs' "
                            "DefiLlama closes of the collateral in its family peg (wrappers through "
                            "the unwrap terminal); min_market_price_by_hit_source keeps all three",
        "exposed_vaults": "curated vaults with >= 10k USD in the event's markets (same chain) at t0 "
                          "or at some day end in the 90 days before; position_t0 is exact (shares "
                          "at t0 x assets per share at the last panel row); group exited_before_t0 "
                          "when position_t0 < 10% of the 90-day peak, entered_after_t0 when the vault "
                          "reached 10k USD in the markets only after t0; exit_lead_days = t0 minus the "
                          "first sample after the last one at >= 50% of the peak (E5); days_locked "
                          "counts day ends from t0's day to the event end (or the chain's own panel "
                          "head if earlier) with L(t) < 5% while the vault still holds >= 10k USD "
                          "in the markets (1% and 10% alongside)",
        "curator": "joined from data/registry/vault_curators.jsonl" if cur_meta else
                   "null: data/registry/vault_curators.jsonl did not exist when this ran",
        "curator_source": cur_meta,
        "parameters": {"utilization": MAIN[0], "duration_h": MAIN[1], "min_peak_usd": MIN_PEAK_USD,
                       "L_lock": L_LOCK, "material_usd": MATERIAL_USD, "merge_gap_days": 7,
                       "merge_horizon_days": MERGE_HORIZON // DAY,
                       "episode_gap_days": EPISODE_GAP // DAY,
                       "self_deposit_share": SELF_DEPOSIT_SHARE}})
    fam_rows = {}
    for h in dh + lh:
        f = h["family"]
        fid = f["family_id"] or f"alone:{h['hit_id']}"
        r = fam_rows.setdefault(fid, {"family_id": fid, "label": f["label"],
                                      "census_uid": f["census_uid"], "reference": f["peg"],
                                      "members": {}, "n_hits": 0})
        r["n_hits"] += 1
        tok = h["collateral_token"]
        if tok:
            key = (h["chain_id"], tok)
            t = tokens.get(*key) or {}
            fr = fams.resolve(*key)
            r["members"][key] = {"chain_id": h["chain_id"], "token": tok, "symbol": t.get("symbol"),
                                 "rule": fr["rule"], "lineage_from": fr.get("lineage_from", []),
                                 "e1_peg_family": t.get("peg_family"),
                                 "e1_peg_rule": t.get("peg_rule")}
    ev_of = defaultdict(list)
    for e in events:
        for fid in e["families"]:
            ev_of[fid].append(e["event_id"])
    fam_out = []
    for fid, r in sorted(fam_rows.items()):
        r["members"] = sorted(r["members"].values(), key=lambda x: (x["chain_id"], x["token"]))
        r["events"] = ev_of.get(fid, [])
        fam_out.append(r)
    io_utils.write_jsonl(f"{OUT}/collateral_families.jsonl", fam_out, GENERATOR, {
        "what": "collateral family of every collateral token in a main-variant hit; events lists "
                "every event with a hit of the family",
        "rules": Families.__doc__,
        "merge_rules": list(MERGE_RULES),
        "lineage_base": LINEAGE_BASE,
        "lineage_table": {k: list(v) for k, v in LINEAGE_TABLE.items()},
        "family_issuers": {k: list(v) for k, v in FAMILY_ISSUERS.items()},
        "lineage_from": "per member: the families issuer lineage lifted the token through, in order",
        "sources": {"census_universe": inputs.rel(CENSUS_UNIVERSE),
                    "census_unwrap": inputs.rel(CENSUS_S1_TOKENS),
                    "e1_peg_rules": "code/data/prices.py classify; data/prices/tokens.jsonl"},
        "census_universe_sha256": sha256(CENSUS_UNIVERSE),
        "census_unwrap_sha256": sha256(CENSUS_S1_TOKENS)})

    cols = ["event_id", "date", "first_trigger", "collateral", "type", "protocols", "chains", "markets",
            "episodes", "exposure_third_party_usd", "exposure_third_party_usd_peak_after_t0",
            "exposure_third_party_peak_date", "affected_supply_usd", "min_market_price",
            "exposed_vaults", "exposed_curators", "self_curated", "main_text_rank"]
    t3 = []
    for e in events:
        if e["excluded"]:
            continue
        t3.append({"event_id": e["event_id"], "date": e["onset_utc"][:10],
                   "first_trigger": e["first_trigger_any"]["onset_utc"][:10],
                   "collateral": e["collateral_family"]["label"], "type": e["type"],
                   "protocols": ";".join(e["protocols"]),
                   "chains": ";".join(str(c) for c in e["chains"]), "markets": e["n_markets"],
                   "episodes": e["n_episodes"],
                   "exposure_third_party_usd": round(e["exposure_third_party_usd_at_t0"]),
                   "exposure_third_party_usd_peak_after_t0": round(e["exposure_third_party_usd_peak_after_t0"]),
                   "exposure_third_party_peak_date": e["exposure_third_party_peak_date"],
                   "affected_supply_usd": round(e["affected_supply_usd_at_t0"]),
                   "min_market_price": None if e["min_market_price"] is None else round(e["min_market_price"], 4),
                   "exposed_vaults": e["n_vaults_present_at_t0"],
                   "exposed_curators": None if e["curators_present_at_t0"] is None
                   else len(e["curators_present_at_t0"]),
                   "self_curated": e["self_curated"], "main_text_rank": None})
    t3.sort(key=lambda r: (-max(r["exposure_third_party_usd"], r["exposure_third_party_usd_peak_after_t0"]),
                           -r["affected_supply_usd"], r["event_id"]))
    rank = 0
    for r in t3:
        if not r["self_curated"] and rank < MAIN_TEXT_ROWS:
            rank += 1
            r["main_text_rank"] = rank
    t3_meta = {
        "columns": {"date": "UTC day of t0: for a depeg-loss event the onset of the episode after "
                            "which the collateral did not recover, for a lock-up the first trigger",
                    "first_trigger": "UTC day of the earliest trigger of any hit "
                                     "(first_trigger_any); earlier than date only for depeg-loss "
                                     "events with a lock-up episode before the loss",
                    "collateral": "collateral family label",
                    "markets": "affected markets", "episodes": "episodes merged into the event",
                    "exposure_third_party_usd": "positions at t0 of curated vaults whose curator "
                                                "is not the collateral's issuer and that are not "
                                                "self-deposited, each market weighted by the share "
                                                "of its debt backed by the family (Euler lending "
                                                "vaults net of the Earn vaults that supply them)",
                    "exposure_third_party_usd_peak_after_t0": "peak over day ends from t0's day to "
                                                              "the event end of the daily sum of the "
                                                              "same positions, guarded against "
                                                              "phantom accrual",
                    "exposure_third_party_peak_date": "day of that peak",
                    "affected_supply_usd": "supply at t0 in USD; an Euler lending vault weighted by "
                                           "the share of its debt backed by the event's family",
                    "min_market_price": "minimum collateral price in peg units",
                    "exposed_vaults": "curated vaults with >= 10k USD in the markets at t0",
                    "exposed_curators": "distinct curator entities of those vaults "
                                        "(blank: no curator registry)",
                    "self_curated": "every vault exposed to the family is curated by its issuer",
                    "main_text_rank": "rank among events that are not self-curated (first "
                                      f"{MAIN_TEXT_ROWS})"}}
    for name, rows_, what in (
            ("table3.csv", t3, "Table 3, every event (appendix), sorted by the larger of third-party "
                               "curated exposure at t0 and its peak after t0"),
            ("table3_main.csv", [r for r in t3 if r["main_text_rank"]],
             f"Table 3, main text: the {MAIN_TEXT_ROWS} events that are not self-curated with "
             "the largest third-party curated exposure (larger of t0 and the peak after t0)")):
        p = DERIVED / OUT / name
        with open(p.with_name(p.name + ".tmp"), "w", newline="") as fh:
            w = csv.DictWriter(fh, cols)
            w.writeheader()
            w.writerows(rows_)
        os.replace(p.with_name(p.name + ".tmp"), p)
        io_utils._meta_sidecar(p, GENERATOR, {
            "what": what, **t3_meta, "rows": len(rows_),
            "excluded_events": [{"event_id": e["event_id"], "reason": e["excluded"],
                                 "description": e["excluded_description"]}
                                for e in events if e["excluded"]]})
    checks = []

    def episodes_in(e, families, lo, hi):
        return [ep for ep in e["episodes"]
                if set(ep["families"]) & families and lo - MERGE_GAP <= ep["onset_ts"] <= hi]
    for case in KNOWN:
        lo, hi = month_bounds(case["month"])
        match = [e for e in events if episodes_in(e, case["families"], lo, hi)]
        rel = [e for e in events if episodes_in(e, case.get("related", set()), lo, hi)]
        overlap = [e for e in events if set(e["families"]) & case["families"]
                   and e not in match and e["onset_ts"] <= hi and e["end_ts"] >= lo]
        row = {"case": case["case"], "families": sorted(case["families"]),
               "recovered": bool(match),
               "events": [{"event_id": e["event_id"], "type": e["type"], "onset_utc": e["onset_utc"],
                           "end_utc": e["end_utc"], "protocols": e["protocols"], "chains": e["chains"],
                           "n_episodes": e["n_episodes"],
                           "matching_episodes": episodes_in(e, case["families"], lo, hi),
                           "hit_kinds": e["hit_kinds"], "census_events": e["census_events"]}
                          for e in match],
               "related_events": [e["event_id"] for e in rel],
               "earlier_events_still_open_in_month": [
                   {"event_id": e["event_id"], "onset_utc": e["onset_utc"], "type": e["type"]}
                   for e in overlap]}
        if not match:
            row["diagnosis_runs"] = diagnose_miss(case, fams, heads, tokens, prices, runs_eval)
        checks.append(row)
    io_utils.write_json(f"{OUT}/selfcheck.json", {"cases": checks,
                                                 "recovered": sum(c["recovered"] for c in checks),
                                                 "of": len(checks)}, GENERATOR, {
        "what": "known events the rules must recover (docs/experiment.md E2)",
        "match": "an event with an episode of the case's collateral family whose onset falls in "
                 "the case's month or the 7 days before it; related_events: same rule for the "
                 "case's sibling families (RLP and wstUSR for USR, AVLT for msUSD)"})
    io_utils.write_json(f"{OUT}/sensitivity.json", {"variants": sens, "recovery_days": rec_sens},
                        GENERATOR, {
        "what": "event counts by lock-up utilization threshold and duration; depeg-loss hits are "
                "the same in every variant; min peak supply 1M USD, L < 5% and the merge rules "
                "fixed; episodes before and events after the 180-day episode merge and the "
                "recovery split; the last row merges the main variant without the 30-day horizon",
        "recovery_days": "t0 of every depeg-loss event of the main variant when a recovered dip "
                         "needs 7, 14 (main) or 30 consecutive closes at or above recovery_line of "
                         "peg, and without the split (no_split); changed lists the events whose t0 "
                         "differs between any two"})


class VPanel:
    """One market's panel CSV as a pandas frame with Python-int supply, borrow and share columns."""

    def __init__(self, proto, path):
        import pandas as pd
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        self.proto = proto
        tcol = "ts" if proto == "silo_v2" else "timestamp"
        df["t"] = df[tcol].astype("int64")
        df["b"] = df["block"].astype("int64")
        df["li"] = df["log_index"].astype("int64")
        if proto in ("morpho", "moolah"):
            sup = [int(x) for x in df["total_supply_assets"]]
            bor = [int(x) for x in df["total_borrow_assets"]]
            sh = [int(x) for x in df["total_supply_shares"]]
        elif proto == "euler_v2":
            bor = [int(x) for x in df["totalBorrows"]]
            sup = [int(c) + b for c, b in zip(df["cash"], bor)]
            sh = [int(x) for x in df["totalShares"]]
        else:
            sup = [int(x) for x in df["collateral_assets"]]
            bor = [int(x) for x in df["debt_assets"]]
            sh, total, prev = [], 0, 0
            for ev, amt, c in zip(df["event"], df["amount"], sup):
                if ev in ("Deposit", "Withdraw"):
                    a = int(amt)
                    if total == 0:
                        num, den = a * 1000, 1
                    else:
                        num, den = a * (total + 1000), prev + 1
                    total = total + num // den if ev == "Deposit" else max(total - (-(-num // den)), 0)
                prev = c
                sh.append(total)
        df["S"] = pd.Series(sup, index=df.index, dtype=object)
        df["B"] = pd.Series(bor, index=df.index, dtype=object)
        df["SH"] = pd.Series(sh, index=df.index, dtype=object)
        self.df = df

    def runs(self, thr):
        """[(start row, end row or None, max supply)] of maximal runs with borrow / supply >= thr (supply 0 with debt counts as pinned)."""
        import numpy as np
        u = np.array([b / s if s > 0 else (np.inf if b > 0 else 0.0)
                      for s, b in zip(self.df["S"], self.df["B"])])
        pinned = u >= thr
        grp = np.cumsum(np.r_[True, pinned[1:] != pinned[:-1]])
        out = []
        for g in np.unique(grp[pinned]):
            idx = np.nonzero(grp == g)[0]
            i0, i1 = idx[0], idx[-1]
            end = i1 + 1 if i1 + 1 < len(self.df) else None
            out.append((i0, end, max(self.df["S"].iloc[i0:i1 + 1])))
        return out

    def row_at(self, ts):
        """Index of the last row at or before ts, or None."""
        import numpy as np
        i = int(np.searchsorted(self.df["t"].to_numpy(), ts, side="right")) - 1
        return i if i >= 0 else None

    def rate_apr(self):
        if self.proto in ("morpho", "moolah"):
            return [int(x) * YEAR / 1e18 if x else None for x in self.df["borrow_rate"]]
        if self.proto == "euler_v2":
            return [int(x) * YEAR / 1e27 for x in self.df["interestRate"]]
        return [float(x) if x else None for x in self.df["borrow_apr_realized"]]


class VPrices:
    """Daily closes read straight from data/prices/<chain>.csv and the supplement, with the documented rules."""

    def __init__(self, cid, tokens_rows):
        self.p = defaultdict(dict)
        for f in (DATA / f"prices/{cid}.csv", DERIVED / OUT / "prices_supplement.csv"):
            if not f.exists():
                continue
            with open(f) as fh:
                for r in csv.DictReader(fh):
                    if r.get("chain_id") not in (None, str(cid)):
                        continue
                    if r["price_usd"]:
                        self.p[r["token"]][day_of(r["date"])] = float(r["price_usd"])
        self.cid = cid
        self.rows = tokens_rows

    def usd(self, token, d):
        s = self.p.get(token, {})
        for k in range(0, MAX_PRICE_CARRY_D + 1):
            if d - k in s:
                return s[d - k]
        row = self.rows.get((self.cid, token), {})
        if s and row.get("peg_family") == "usd_stable":
            return 1.0
        return None


def v_rederive_runs(work, n=20):
    rows = jsonl(DERIVED / OUT / "lockups_market.jsonl")
    rng = random.Random(SEED)
    sample = rng.sample(rows, min(n, len(rows)))
    heads = work.load("heads.pkl")
    out = []
    for r in sample:
        proto, cid, m = r["protocol"], r["chain_id"], r["market"]
        vp = VPanel(proto, panel_dir(proto, cid) / f"{m}.csv")
        df = vp.df
        match = None
        for i0, end, smax in vp.runs(MAIN[0]):
            if int(df["b"].iloc[i0]) == r["start_block"] and \
                    int(df["t"].iloc[i0]) == r["start_ts"]:
                match = (i0, end, smax)
        if match is None:
            out.append({"run_id": r["run_id"], "ok": False, "why": "no run starting at that row"})
            continue
        i0, end, smax = match
        end_ts = int(df["t"].iloc[end]) if end is not None else None
        end_block = int(df["b"].iloc[end]) if end is not None else None
        head_ts = heads[f"{proto}:{cid}"]["ts"]
        dur = ((end_ts or head_ts) - r["start_ts"]) / 3600
        ok = end_ts == r["end_ts"] and end_block == r["end_block"] and \
            (end is None) == r["ongoing_at_head"] and abs(dur - r["duration_h"]) < 0.01
        out.append({"run_id": r["run_id"], "ok": ok, "start_utc": r["start_utc"],
                    "end_utc_pipeline": r["end_utc"], "end_utc_independent": utc(end_ts),
                    "duration_h_independent": round(dur, 2), "max_supply_raw_in_run": str(smax)})
    return {"sampled": len(sample), "identical_start_and_end": sum(x["ok"] for x in out),
            "seed": SEED, "runs": out}


def v_census_against_panel(work):
    depeg = work.load("depeg.pkl")
    heads = work.load("heads.pkl")
    out = []
    for h in depeg["census"] + depeg["moolah"]:
        proto, cid, m = h["protocol"], h["chain_id"], h["market"]
        p = panel_dir(proto, cid) / f"{m}.csv"
        if not p.exists():
            out.append({"hit_id": h["hit_id"], "panel": False})
            continue
        vp = VPanel(proto, p)
        df = vp.df
        head_ts = heads[f"{proto}:{cid}"]["ts"]
        pad = CENSUS_PAD if h["source"] == "depeg_census" else 0
        w0 = (h.get("window_from_ts") or h["onset_ts"]) + pad
        w1 = (h.get("window_to_ts") or h["end_ts"]) - pad
        pinned = []
        for i0, end, _ in vp.runs(MAIN[0]):
            a = int(df["t"].iloc[i0])
            b = int(df["t"].iloc[end]) if end is not None else head_ts
            if b - a >= MAIN[1] * 3600 and a <= w1 and b >= w0 - DAY:
                pinned.append((a, b))
        thr = (I3_FRACTION * MORPHO_CAP_APR if proto == "morpho" else
               FALLBACK_I3_APR if proto in ("euler_v2", "silo_v2") else None)
        rate_hits = []
        if thr is not None:
            for t, apr in zip(df["t"], vp.rate_apr()):
                if apr is not None and apr >= thr and w0 - R19_LOOKBACK <= t <= w1:
                    rate_hits.append(int(t))
        first = min([a for a, _ in pinned] + rate_hits[:1], default=None)
        out.append({
            "hit_id": h["hit_id"], "protocol": proto, "chain_id": cid, "market": m,
            "source": h["source"],
            "window_start_unpadded_utc": utc(w0), "census_hit_utc": utc(h["onset_ts"]),
            "i2_in_our_panel": bool(pinned), "i3_in_our_panel": bool(rate_hits),
            "i3_threshold_apr": thr,
            "our_first_trigger_utc": utc(first),
            "hours_ours_minus_window_start": None if first is None else round((first - w0) / 3600, 2),
            "census_flags": {"i2": h.get("i2"), "i3": h.get("i3")}})
    n = len(out)
    shown = sum(1 for x in out if x.get("i2_in_our_panel") or x.get("i3_in_our_panel"))
    diffs = sorted(abs(x["hours_ours_minus_window_start"]) for x in out
                   if x.get("hours_ours_minus_window_start") is not None
                   and x.get("source") == "depeg_census")
    return {"hits": n, "with_indicator_in_our_panel": shown,
            "first_trigger_abs_diff_hours_census_hits": {
                "median": diffs[len(diffs) // 2] if diffs else None,
                "within_1h": sum(1 for d in diffs if d <= 1),
                "within_24h": sum(1 for d in diffs if d <= 24), "max": diffs[-1] if diffs else None},
            "i4": "not checked: the census did not scan I4 and the panel has no positions to test it",
            "rows": out}


_VQ = {}


def v_queue_states(proto, cid, vaults):
    """{vault: {market: [(ts, in_queue)]}} replayed from actions.jsonl with a plain list queue (a positive cap appends, a new w."""
    key = (proto, cid)
    have = _VQ.setdefault(key, {})
    missing = set(vaults) - set(have)
    if not missing:
        return have
    p = DATA / f"vaults/{proto}/{cid}/actions.jsonl"
    evs = defaultdict(list)
    if p.exists():
        needles = [v.encode() for v in missing]
        with open(p, "rb") as fh:
            for line in fh:
                if not (b"SetWithdrawQueue" in line or b'"SetCap"' in line):
                    continue
                if not any(nd in line for nd in needles):
                    continue
                r = json.loads(line)
                if r["vault"] in missing and r["event"] in ("SetWithdrawQueue", "SetCap"):
                    evs[r["vault"]].append(r)
    for v in missing:
        queue, states = [], defaultdict(list)
        for r in sorted(evs.get(v, []), key=lambda r: (r["block"], r["log_index"])):
            f = r.get("fields") or r
            old = list(queue)
            if r["event"] == "SetWithdrawQueue":
                queue = [x.lower() for x in f.get("newWithdrawQueue") or []]
            elif int(f.get("cap") or 0) > 0:
                mk = (f.get("id") or f.get("market") or "").lower()
                if mk and mk not in queue:
                    queue.append(mk)
            for mk in set(old) | set(queue):
                if (mk in old) != (mk in queue):
                    states[mk].append((r["timestamp"], mk in queue))
        have[v] = dict(states)
    return have


def v_exposure(work, n=10, present_only=False):
    """Recompute exposure rows from the positions files and panel CSVs; present_only draws from vaults holding >= 10k USD at t0."""
    import pandas as pd
    events = jsonl(DERIVED / OUT / "events.jsonl")
    tok = Tokens(work.load("tokens.pkl"))
    census_pegs(tok, Families(tok))
    tokens_rows = tok.rows
    heads = work.load("heads.pkl")
    pool = [(e, v) for e in events for v in e["exposed_vaults"]
            if not present_only or (v["group"] == "present_at_t0" and
                                    (v["position_t0_usd"] or 0) >= MATERIAL_USD)]
    rng = random.Random(SEED + (2 if present_only else 1))
    sample = rng.sample(pool, min(n, len(pool)))
    panels, vfiles = {}, {}

    def panel(proto, cid, m):
        k = (proto, cid, m)
        if k not in panels:
            p = panel_dir(proto, cid) / f"{m}.csv"
            panels[k] = VPanel(proto, p) if p.exists() else None
        return panels[k]

    def vfile(proto, cid, v):
        k = (proto, cid, v)
        if k not in vfiles:
            p = DATA / f"vaults/{proto}/{cid}/positions/{v}.csv"
            vfiles[k] = pd.read_csv(p, dtype=str, keep_default_na=False) if p.exists() else None
        return vfiles[k]

    anchor_cache = {}

    def anchors(proto, cid, m):
        """Exact conversions in market m from every vault file of the chain."""
        if proto not in ("silo_v2", "euler_v2"):
            return []
        if (proto, cid) not in anchor_cache:
            col = POS_MARKET_COL[proto]
            acc = defaultdict(list)
            for v in os.listdir(DATA / f"vaults/{proto}/{cid}/positions"):
                if not v.endswith(".csv"):
                    continue
                df = vfile(proto, cid, v[:-4])
                sub = df[df["kind"].isin(["Deposit", "Withdraw"]) &
                         (df["assets"] != "") & (df["event_shares"] != "")]
                for mm, t, a, sh in zip(sub[col], sub["timestamp"], sub["assets"], sub["event_shares"]):
                    if int(a) > 0 and int(sh) > 0:
                        acc[mm].append((int(t), int(a) / int(sh)))
            anchor_cache[(proto, cid)] = {k: sorted(x) for k, x in acc.items()}
        return anchor_cache[(proto, cid)].get(m, [])

    def rate_at(proto, cid, m, ts):
        """Assets per share of market m at ts (state of the last row)."""
        vp = panel(proto, cid, m)
        if vp is None:
            anc = [r for t, r in anchors(proto, cid, m) if t // DAY <= ts // DAY]
            return (anc[-1] if anc else None), None
        i = vp.row_at(ts)
        if i is None:
            return 0.0, 0.0
        return rate_row(proto, cid, m, i, ts // DAY)

    def rate_row(proto, cid, m, i, day):
        """Assets per share and liquidity of market m at row i, Silo anchored at the last vault conversion on or before `day`."""
        vp = panel(proto, cid, m)
        ts = day * DAY
        S, B, SH = vp.df["S"].iloc[i], vp.df["B"].iloc[i], vp.df["SH"].iloc[i]
        if proto in ("morpho", "moolah"):
            r = (S + 1) / (SH + 10 ** 6)
        elif proto == "euler_v2":
            r = (S + 10 ** 6) / (SH + 10 ** 6)
        else:
            r = (S + 1) / (SH + 1000) if SH > 0 else 1 / 1000
            anc = [(t, x) for t, x in anchors(proto, cid, m) if t // DAY <= ts // DAY]
            if anc:
                ad = anc[-1][0] // DAY
                j = vp.row_at(ad * DAY + DAY - 1)
                S2, SH2 = vp.df["S"].iloc[j], vp.df["SH"].iloc[j]
                rep = (S2 + 1) / (SH2 + 1000) if SH2 > 0 else 1 / 1000
                r *= anc[-1][1] / rep
        return r, float(max(S - B, 0))

    rate_cache = {}

    def vguard(proto, cid, m, t0):
        """Accrual guard reference re-derived from the panel CSV."""
        vp = panel(proto, cid, m)
        if vp is None:
            return None
        i = vp.row_at(t0)
        if i is None:
            return None
        S, B = vp.df["S"], vp.df["B"]

        def pinned(k):
            sk, bk = S.iloc[k], B.iloc[k]
            return (bk / sk if sk > 0 else (math.inf if bk > 0 else 0.0)) >= MAIN[0]
        pin = pinned(i)
        j = i
        while pin and j > 0 and pinned(j - 1):
            j -= 1
        ref_ts = int(vp.df["t"].iloc[j]) if pin else t0
        aps_ref, _ = rate_row(proto, cid, m, j, ref_ts // DAY)
        if (proto, cid, m) not in rate_cache:
            rate_cache[(proto, cid, m)] = vp.rate_apr()
        rates = rate_cache[(proto, cid, m)]
        rate = next((rates[k] for k in range(j, -1, -1) if rates[k] is not None), None)
        return ref_ts, aps_ref, GUARD_MAX_APR if rate is None else min(max(rate, 0.0), GUARD_MAX_APR)

    def capped(g, aps_now, ts):
        if g is None or ts < g[0] or not aps_now:
            return 1.0
        return min(1.0, g[1] * (1 + g[2] * (ts - g[0]) / YEAR) / aps_now)

    out = []
    for e, v in sample:
        proto, cid = v["protocol"], v["chain_id"]
        ms = [x["market"] for x in e["affected_markets"]
              if x["protocol"] == proto and x["chain_id"] == cid]
        t0, t0d = e["onset_ts"], e["onset_ts"] // DAY
        vg = {m: vguard(proto, cid, m, t0) for m in ms}
        vpz = VPrices(cid, tokens_rows)
        asset = v["asset"]
        dec = (tokens_rows.get((cid, asset)) or {}).get("decimals")
        if v["kind"] == "euler_lending":
            def pos_markets(ts, proto=proto, cid=cid, v=v, vg=vg):
                vp = panel(proto, cid, v["vault"])
                i = vp.row_at(ts)
                if i is None:
                    return {v["vault"]: (0.0, 0.0)}
                S_, B_, SH_ = vp.df["S"].iloc[i], vp.df["B"].iloc[i], vp.df["SH"].iloc[i]
                g = capped(vg.get(v["vault"]), (S_ + 10 ** 6) / (SH_ + 10 ** 6), ts)
                return {v["vault"]: (float(S_) * g, float(max(S_ - B_, 0)))}
        else:
            df = vfile(proto, cid, v["vault"])
            col = POS_MARKET_COL[proto]
            shcol = "vault_cum_shares" if proto in ("morpho", "moolah") else "cum_shares"
            per_m = {}
            for m, sub in df.groupby(col, sort=False):
                per_m[m] = (sub["timestamp"].astype("int64").to_numpy(),
                            [int(x) for x in sub[shcol]])
            vq = v_queue_states(proto, cid, [v["vault"]]).get(v["vault"], {})

            def out_of_queue(m, ts, vq=vq):
                past = [inq for t, inq in vq.get(m, []) if t <= ts]
                return bool(past) and not past[-1]

            def pos_markets(ts, proto=proto, cid=cid, per_m=per_m, vg=vg, out_of_queue=out_of_queue):
                import numpy as np
                res = {}
                for m, (tsa, sha) in per_m.items():
                    k = int(np.searchsorted(tsa, ts, side="right")) - 1
                    if k < 0:
                        continue
                    sh = sha[k]
                    if sh <= 0 or out_of_queue(m, ts):
                        continue
                    r, av = rate_at(proto, cid, m, ts)
                    if r is None:
                        continue
                    x = sh * r * capped(vg.get(m), r, ts)
                    res[m] = (x, av if av is not None else x)
                return res
        p0 = pos_markets(t0)
        x0 = sum(p0[m][0] for m in ms if m in p0)
        usd0 = x0 / 10 ** dec * vpz.usd(asset, t0d) if dec is not None and vpz.usd(asset, t0d) else None
        share = {x["market"]: x["family_debt_share_at_t0"] for x in e["affected_markets"]
                 if x["protocol"] == proto and x["chain_id"] == cid}
        xw0 = sum(p0[m][0] * (1.0 if share.get(m) is None else share[m]) for m in ms if m in p0)
        expo0 = xw0 / 10 ** dec * vpz.usd(asset, t0d) if dec is not None and vpz.usd(asset, t0d) else None
        path = []
        for d in range(t0d - EXPOSURE_LOOKBACK_D, t0d):
            pm = pos_markets(d * DAY + DAY - 1)
            path.append((d * DAY + DAY - 1, sum(pm[m][0] for m in ms if m in pm), d))
        path.append((t0, x0, t0d))
        peak = max(path, key=lambda z: z[1])
        peak_usd = max(z[1] / 10 ** dec * (vpz.usd(asset, z[2]) or 0.0) for z in path)
        before = max(peak_usd, usd0 or 0.0) >= MATERIAL_USD
        exited = before and peak[1] > 0 and x0 < EXIT_FRACTION * peak[1]
        lead = None
        if exited:
            above = [i for i, z in enumerate(path) if z[1] >= LEAD_FRACTION * peak[1]]
            if above and above[-1] + 1 < len(path):
                lead = round((t0 - path[above[-1] + 1][0]) / DAY, 2)
        locked = 0
        last_day = min(e["end_ts"], heads[f"{proto}:{cid}"]["ts"]) // DAY
        for d in range(t0d, last_day + 1):
            pm = pos_markets(d * DAY + DAY - 1)
            tot = sum(x for x, _ in pm.values())
            xe = sum(pm[m][0] for m in ms if m in pm)
            px = vpz.usd(asset, d)
            if tot <= 0 or px is None or xe / 10 ** dec * px < MATERIAL_USD:
                continue
            if sum(min(x, av) for x, av in pm.values()) / tot < L_LOCK:
                locked += 1
        got = {"position_t0_raw": f"{x0:.0f}", "position_t0_usd": usd0, "peak_90d_usd": peak_usd,
               "exposure_t0_usd": expo0,
               "group": ("entered_after_t0" if not before else
                         "exited_before_t0" if exited else "present_at_t0"),
               "exit_lead_days": lead, "days_locked": locked}
        cmp = {}
        for k, val in got.items():
            ref = v[k]
            if k == "position_t0_raw":
                rel = abs(float(val) - float(ref)) / max(abs(float(ref)), 1.0)
                cmp[k] = {"pipeline": ref, "independent": val, "rel_diff": rel, "ok": bool(rel < 1e-9)}
            elif isinstance(ref, float) and val is not None:
                val = float(val)
                rel = abs(val - ref) / max(abs(ref), 1e-12)
                cmp[k] = {"pipeline": ref, "independent": val, "rel_diff": rel, "ok": bool(rel < 1e-9)}
            else:
                cmp[k] = {"pipeline": ref, "independent": val, "ok": bool(val == ref)}
        out.append({"event_id": e["event_id"], "vault": v["vault"], "kind": v["kind"],
                    "protocol": proto, "chain_id": cid, "fields": cmp,
                    "ok": bool(all(c["ok"] for c in cmp.values()))})
    return {"sampled": len(out), "identical": sum(x["ok"] for x in out),
            "seed": SEED + (2 if present_only else 1), "pool": len(pool),
            "kinds": dict(Counter(f"{x['protocol']}:{x['kind']}" for x in out)),
            "tolerance": "relative 1e-9 on floats (summation order), exact on group and lead time; "
                         "exposure_t0_usd takes the Euler family debt share from the event row",
            "rows": out}


def v_silo_replay(work):
    """Replayed Silo share price against every vault Deposit/Withdraw anchor (assets / shares of the event, state of the previous row)."""
    out = {}
    for proto, cid in PAIRS:
        if proto != "silo_v2":
            continue
        errs, cache, anchor_pts = [], {}, defaultdict(list)
        for fn in os.listdir(DATA / f"vaults/{proto}/{cid}/positions"):
            if not fn.endswith(".csv"):
                continue
            with open(DATA / f"vaults/{proto}/{cid}/positions/{fn}") as fh:
                rows = [r for r in csv.DictReader(fh) if r["kind"] in ("Deposit", "Withdraw")
                        and r["assets"] and r["event_shares"] and int(r["event_shares"]) > 0]
            by_m = defaultdict(list)
            for r in rows:
                by_m[r["market"]].append(r)
            for m, rr in by_m.items():
                p = panel_dir(proto, cid) / f"{m}.csv"
                if not p.exists():
                    continue
                if m not in cache:
                    vp = VPanel(proto, p)
                    cache[m] = (vp, list(zip(vp.df["b"], vp.df["li"])))
                vp, keys = cache[m]
                for r in rr:
                    i = bisect.bisect_left(keys, (int(r["block"]), int(r["log_index"]))) - 1
                    if i < 0:
                        continue
                    S, SH = vp.df["S"].iloc[i], vp.df["SH"].iloc[i]
                    rep = (S + 1) / (SH + 1000) if SH > 0 else 1 / 1000
                    exact = int(r["assets"]) / int(r["event_shares"])
                    errs.append(abs(rep / exact - 1))
                    anchor_pts[m].append((int(r["block"]), int(r["log_index"]),
                                          int(r["timestamp"]) // DAY, exact, rep))
        after = []
        for pts in anchor_pts.values():
            pts.sort()
            fac, fac_day = None, None
            for _b, _li, d, exact, rep in pts:
                if fac is not None and fac_day < d:
                    after.append(abs(rep * fac / exact - 1))
                if fac_day is None or d >= fac_day:
                    fac, fac_day = exact / rep, d
        after.sort()
        errs.sort()
        out[str(cid)] = {"anchors_scored_after_factor": len(after),
                         "after_factor_median_rel_err": after[len(after) // 2] if after else None,
                         "after_factor_p99_rel_err": after[int(len(after) * 0.99)] if after else None,
                         "after_factor_max_rel_err": after[-1] if after else None,
                         "anchors": len(errs), "median_rel_err": errs[len(errs) // 2] if errs else None,
                         "p99_rel_err": errs[int(len(errs) * 0.99)] if errs else None,
                         "share_within_1e-6": sum(1 for x in errs if x <= 1e-6) / len(errs) if errs else None,
                         "max_rel_err": errs[-1] if errs else None}
    return {"note": "the replay drifts only where a silo emptied to dust; positions use the replay "
                    "times the drift factor of the latest anchor (silo_factors)", "by_chain": out}


def v_guard_against_e3(work):
    """Every guarded daily value of a vault in an event's markets against the vault's own daily book value from E3 (data/aum/vault_daily.csv; Euler lending vaults."""
    gd = work.load("guarded_daily.pkl")
    want = defaultdict(set)
    for (_e, _p, cid, vault, _kind), days in gd.items():
        want[(str(cid), vault)] |= set(days)
    e3 = {}
    for name in ("vault_daily.csv", "lending_daily.csv"):
        p = DERIVED / "aum" / name
        if not p.exists():
            continue
        with open(p) as fh:
            for r in csv.DictReader(fh):
                key = (r["chain_id"], r["vault"])
                if key in want and r["aum_usd"]:
                    d = day_of(r["date"])
                    if d in want[key]:
                        e3[(key, d)] = float(r["aum_usd"])
    compared = over = over_material = 0
    worst = []
    missing = 0
    for (eid, proto, cid, vault, kind), days in gd.items():
        for d, usd in days.items():
            if not usd:
                continue
            ref = e3.get(((str(cid), vault), d))
            if not ref:
                missing += 1
                continue
            compared += 1
            ratio = usd / ref
            if ratio > 10:
                over += 1
                if usd >= MATERIAL_USD:
                    over_material += 1
                worst.append({"event_id": eid, "protocol": proto, "chain_id": cid, "vault": vault,
                              "kind": kind, "date": day_str(d), "guarded_usd": usd, "e3_aum_usd": ref,
                              "ratio": ratio})
    worst.sort(key=lambda x: -x["ratio"])
    return {"vault_days_compared": compared, "vault_days_without_e3_value": missing,
            "above_10x": over, "above_10x_and_above_10k_usd": over_material,
            "max_ratio_above_10x": worst[0]["ratio"] if worst else None, "worst": worst[:10],
            "files": ["data/aum/vault_daily.csv", "data/aum/lending_daily.csv"]}


def stage_validate(work):
    sc = json.loads((DERIVED / OUT / "selfcheck.json").read_text())
    res = {"runs_rederived": v_rederive_runs(work),
           "census_hits_against_panel": v_census_against_panel(work),
           "selfcheck": {"recovered": sc["recovered"], "of": sc["of"],
                         "misses": [c["case"] for c in sc["cases"] if not c["recovered"]]},
           "exposure_recomputed": v_exposure(work),
           "exposure_recomputed_present_at_t0": v_exposure(work, present_only=True),
           "silo_share_replay": v_silo_replay(work),
           "guard_against_e3": v_guard_against_e3(work)}
    io_utils.write_json(f"{OUT}/validation.json", res, GENERATOR, {
        "what": "independent checks of the E2 outputs (pandas over the CSVs; no shared code with "
                "the pipeline's readers, run logic, prices or vault series)"})
    return {"runs": f"{res['runs_rederived']['identical_start_and_end']}/{res['runs_rederived']['sampled']}",
            "census": f"{res['census_hits_against_panel']['with_indicator_in_our_panel']}/"
                      f"{res['census_hits_against_panel']['hits']}",
            "selfcheck": f"{sc['recovered']}/{sc['of']}",
            "exposure": f"{res['exposure_recomputed']['identical']}/{res['exposure_recomputed']['sampled']}",
            "exposure_present": f"{res['exposure_recomputed_present_at_t0']['identical']}/"
                                f"{res['exposure_recomputed_present_at_t0']['sampled']}",
            "guard_above_10x_e3": f"{res['guard_against_e3']['above_10x']}/"
                                  f"{res['guard_against_e3']['vault_days_compared']}"}


STAGES = ("scan", "prices", "depeg", "lockups", "events", "validate")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--stage", default="all", choices=STAGES + ("all",))
    ap.add_argument("--work", default=os.environ.get("E2_WORK", "/tmp/e2_events_work"),
                    help="directory for intermediate files (outside the repository)")
    ap.add_argument("--workers", type=int, default=12)
    args = ap.parse_args()
    work = Work(args.work)
    stages = STAGES if args.stage == "all" else (args.stage,)
    log = work.load("run_log.pkl") if work.has("run_log.pkl") else []
    for st in stages:
        t0, c0 = time.time(), rpc.calls_made()
        if st == "scan":
            res = stage_scan(work, args.workers)
        elif st == "prices":
            res = stage_prices(work)
        elif st == "depeg":
            res = stage_depeg(work)
        elif st == "lockups":
            res = stage_lockups(work, args.workers)
        elif st == "events":
            res = stage_events(work)
        else:
            res = stage_validate(work)
        entry = {"stage": st, "seconds": round(time.time() - t0, 1),
                 "rpc_requests": rpc.calls_made() - c0, "finished_utc": now_utc(),
                 "result": res}
        log.append(entry)
        work.save("run_log.pkl", log)
        print(json.dumps(entry, default=str)[:2000], flush=True)


if __name__ == "__main__":
    main()
