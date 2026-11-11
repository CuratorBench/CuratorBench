# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31",
#                 "hypersync==1.2.0"]
# ///
"""E3: daily curated AUM per vault, per protocol and per curator."""
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
import statistics
import sys
import tempfile
import time
import types
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor

import requests

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "code"))
sys.path.insert(0, str(ROOT / "code" / "data"))
from common import blocktime, evmabi, io_utils, multicall, outroot, rpc
from prices import classify
from eth_utils import keccak

DATA = ROOT / "data"
OUT = "aum"
DERIVED = outroot.OUT_DATA
GENERATOR = "code/analysis/e3_aum.py"
CACHE = pathlib.Path(os.environ.get(
    "E3_AUM_CACHE", pathlib.Path(tempfile.gettempdir()) / "e3_aum"))
CHAINS = json.loads((ROOT / "code" / "config" / "chains.json").read_text())
SCOPE = [("morpho", 1), ("morpho", 8453), ("morpho", 4663),
         ("morpho", 42161), ("morpho", 747474), ("morpho", 999),
         ("moolah", 56), ("euler_v2", 1), ("euler_v2", 9745),
         ("euler_v2", 59144), ("silo_v2", 1), ("silo_v2", 42161),
         ("silo_v2", 43114), ("silo_v2", 146)]
DAY = 86400
MAX_JUMP = 0.10
JUMP_WINDOW = 30
DUST_SUPPLY = 1e-6
CARRY_DAYS = 3
EVK_VIRTUAL = 10 ** 6
FEE_COLUMNS = ("feeShares", "performanceFeeShares", "managementFeeShares",
               "protocolFeeShares")
REFERENCE = {"eth_ref": (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
             "btc_ref": (1, "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599")}
LLAMA = "https://coins.llama.fi/chart/"
LLAMA_SEARCH = 12 * 3600
ZERO = "0x" + "0" * 40
BURNS = {ZERO, "0x" + "0" * 39 + "1", "0x000000000000000000000000000000000000dead"}
OWNER_FUNDED_SHARE = 0.9
REVIEW_BALANCE = 0.9
TRANSFER_TOPIC = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
MANAGER_ROLES = {"owner", "curator", "DEFAULT_ADMIN_ROLE", "MANAGER",
                 "CURATOR"}
_LOOP = "self_dealing_loop:data/resume/king-rss.md"
_POL = "protocol_owned_liquidity:data/resume/excluded-vaults.md"
HAND_EXCLUSIONS = {
    (8453, "0xf80c0529bd94c773844e459853cd91b9263dd525"):
        ("King RSS USDC Vault", _LOOP),
    (8453, "0x61bfd6f7df1f72427f472144d043c25d742d145e"):
        ("King Elepan USDC Vault", _LOOP),
    (8453, "0xfdd5a1d4823411809d6ac735991b3a015e5aaab5"):
        ("King Elepan WETH Vault", _LOOP),
    (8453, "0x0d96ba80502eb8a08a6d3bd4680134b20c229532"):
        ("Kingdom Elepan USDC", _LOOP),
    (8453, "0xc91f3bc556001ef7acfcb869ec0fc29ac780c35c"):
        ("King Synth eUSD USDC Vault", _LOOP),
    (8453, "0x35a00f116536c13a63273513990e4e496a15ddb2"):
        ("unnamed Vault V2", _LOOP),
    (8453, "0xb96bcffbb458581a3af7fed3150b7cd4b233a7b9"):
        ("unnamed Vault V2", _LOOP),
    (999, "0x4346c98e690c17efbb999ae8e1da96b089be320b"):
        ("Gauntlent rUSDC Vault (Relend Network rUSDC)", _POL),
    (42161, "0x37997418ef10f0db0b38a411877de1e17e0b2058"): ("MS-sUSDC", _POL),
    (42161, "0xbe1a0690aa440ccae131b05679aef197f843527a"): ("MS-sUSDe", _POL)}
GROUP = {"morpho_v1": "morpho", "morpho_v2": "morpho",
         "moolah_vault": "moolah", "silo_vault": "silo_v2",
         "euler_earn": "euler_v2_earn", "euler_lending": "euler_v2_lending"}


def day_str(day):
    """ISO date of a day number (days since 1970-01-01, UTC)."""
    return (dt.date(1970, 1, 1) + dt.timedelta(days=day)).isoformat()


def jsonl(path):
    """Rows of a JSON-lines file."""
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


def save(name, obj):
    """Pickle an intermediate result into CACHE, atomically."""
    CACHE.mkdir(parents=True, exist_ok=True)
    tmp = CACHE / (name + ".tmp")
    with open(tmp, "wb") as fh:
        pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, CACHE / name)


def load(name):
    """An intermediate result saved by save()."""
    with open(CACHE / name, "rb") as fh:
        return pickle.load(fh)


def target(cid):
    """dRPC slug of a chain id."""
    return CHAINS[str(cid)]["drpc"]


def scan_head(protocol, cid):
    """(last block, its timestamp) of the vault layer's scan."""
    d = DATA / "vaults" / protocol / str(cid)
    if protocol == "euler_v2":
        h = json.loads((d / "head.json").read_text())
        return h["block"], h["timestamp"]
    if protocol == "silo_v2":
        block = json.loads((d / "scan.json").read_text())["head_block"]
    else:
        block = json.loads(
            (d / "_vault_checkpoint.json").read_text())["to_block"] - 1
    return block, int(rpc.get_block(target(cid), block)["timestamp"], 16)


def read_accruals(path):
    """Per vault and day: the position and newTotalAssets of the day's last AccrueInterest, and the fee shares minted that day."""
    last = defaultdict(dict)
    fees = defaultdict(lambda: defaultdict(int))
    with open(path, newline="") as fh:
        rd = csv.reader(fh)
        ix = {c: i for i, c in enumerate(next(rd))}
        iv, ib, il = ix["vault"], ix["block"], ix["log_index"]
        it, ia = ix["timestamp"], ix["newTotalAssets"]
        fcols = [ix[c] for c in FEE_COLUMNS if c in ix]
        for row in rd:
            v, day = row[iv], int(row[it]) // DAY
            pos = (int(row[ib]), int(row[il]))
            cur = last[v].get(day)
            if cur is None or pos > cur[0]:
                last[v][day] = (pos, int(row[ia]))
            for i in fcols:
                if row[i] not in ("", "0"):
                    fees[v][day] += int(row[i])
    return last, fees


def read_flows(path):
    """[(position, day, +1 deposit or -1 withdraw, assets, shares)]."""
    out = []
    with open(path, newline="") as fh:
        rd = csv.reader(fh)
        ix = {c: i for i, c in enumerate(next(rd))}
        for row in rd:
            kind = row[ix["kind"]].lower()
            if kind not in ("deposit", "withdraw"):
                raise ValueError(f"{path}: flow kind {kind}")
            out.append(((int(row[ix["block"]]), int(row[ix["log_index"]])),
                        int(row[ix["timestamp"]]) // DAY,
                        1 if kind == "deposit" else -1,
                        int(row[ix["assets"]]), int(row[ix["shares"]])))
    return out


def replay_vault(accrual, fees, flows, head_day):
    """Daily total assets, share supply and fill length of one vault from its first deposit day to head_day; None if it never had a deposit."""
    deposits = [day for _, day, sign, _, _ in flows if sign > 0]
    if not deposits:
        return None
    first = min(deposits)
    by_day = defaultdict(list)
    for pos, day, sign, assets, shares in flows:
        by_day[day].append((pos, sign, assets, shares))
    start = min([first, *accrual, *fees])
    ta = supply = 0
    last_event = start
    out = {"start": first, "ta": [], "supply": [], "fill": [], "flow": [],
           "low": [], "after_accrual_days": 0}
    for day in range(start, head_day + 1):
        flows_today = sorted(by_day.get(day, ()))
        if day in accrual:
            pos, ta = accrual[day]
            after = [f for f in flows_today if f[0] > pos]
            out["after_accrual_days"] += bool(after) and day >= first
        else:
            after = flows_today
        for _, sign, assets, _ in after:
            ta = ta + assets if sign > 0 else max(ta - assets, 0)
        low = None
        for _, sign, _, shares in flows_today:
            supply += sign * shares
            low = supply if low is None else min(low, supply)
        supply += fees.get(day, 0)
        low = supply if low is None else min(low, supply)
        if day in accrual or flows_today:
            last_event = day
        if day >= first:
            out["ta"].append(ta)
            out["supply"].append(supply)
            out["fill"].append(day - last_event)
            out["flow"].append(sum(sign * assets
                                   for _, sign, assets, _ in flows_today))
            out["low"].append(low)
    return out


def read_balances(path, key_of, balance_col="cum_shares"):
    """{key: [(day, end-of-day balance)]} from a running-balance CSV; key_of maps a row dict to a key or None (row ignored)."""
    last = defaultdict(dict)
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            key = key_of(row)
            if key is None:
                continue
            day = int(row["timestamp"]) // DAY
            pos = (int(row["block"]), int(row["log_index"]))
            cur = last[key].get(day)
            if cur is None or pos > cur[0]:
                last[key][day] = (pos, int(row[balance_col]))
    return {k: sorted((d, b) for d, (_, b) in v.items())
            for k, v in last.items()}


def holdings(protocol, cid, registry):
    """Vault-held shares of other vaults."""
    d = DATA / "vaults" / protocol / str(cid)
    out = []
    if protocol in ("morpho", "moolah") and (d / "adapter_shares.csv").exists():
        for (v, a, u), series in read_balances(
                d / "adapter_shares.csv",
                lambda r: (r["vault"], r["adapter"], r["underlying_vault"])
        ).items():
            out.append((v, a, u, "wrapper_ledger", series))
    if protocol == "silo_v2":
        for v in registry:
            f = d / "positions" / f"{v}.csv"
            if f.exists():
                for (m,), series in read_balances(
                        f, lambda r: (r["market"],) if r["market"] in registry
                        else None).items():
                    out.append((v, "", m, "silo_market", series))
    if protocol == "euler_v2":
        for v in registry:
            f = d / "positions" / f"{v}.csv"
            if f.exists():
                for (s,), series in read_balances(
                        f, lambda r: (r["strategy"],)).items():
                    out.append((v, "", s, "earn_strategy", series))
    return out


def lending_series(cid, head_day):
    """Euler lending vaults that set a collateral LTV at least once (vault_aum.py's set)."""
    reg = jsonl(DATA / "registry" / "markets" / "euler_v2" / f"{cid}.jsonl")
    lends = {r["vault"] for r in reg if r.get("type") == "ltv"
             and (r["borrow_ltv"] or r["liquidation_ltv"])}
    asset = {r["vault"]: r["asset"] for r in reg if r.get("type") == "vault"}
    panel = DATA / "panel" / "euler_v2" / str(cid)
    gov = defaultdict(list)
    for r in jsonl(panel / "governance.jsonl"):
        if r["event"] == "EVaultCreated":
            gov[r["vault"]].append(((r["block"], r["log_index"]),
                                    r["timestamp"] // DAY, r["creator"]))
        elif r["event"] == "GovSetGovernorAdmin":
            gov[r["vault"]].append(((r["block"], r["log_index"]),
                                    r["timestamp"] // DAY,
                                    r["newGovernorAdmin"]))
    out = {}
    for v in sorted(lends):
        f = panel / f"{v}.csv"
        if not f.exists():
            continue
        last, low_of = {}, {}
        with open(f, newline="") as fh:
            for row in csv.DictReader(fh):
                day = int(row["timestamp"]) // DAY
                pos = (int(row["block"]), int(row["log_index"]))
                sh = int(row["totalShares"])
                low_of[day] = min(sh, low_of.get(day, sh))
                if day not in last or pos > last[day][0]:
                    last[day] = (pos, int(row["cash"]),
                                 int(row["totalBorrows"]), sh)
        funded = [d for d, x in last.items() if x[1] + x[2] > 0]
        if not funded:
            continue
        first = min(funded)
        changes = sorted(gov[v])
        s = {"asset": asset[v], "start": first, "cash": [], "borrows": [],
             "shares": [], "low": [], "fill": [], "governor": []}
        cash = borrows = shares = 0
        governor, k, last_event = ZERO, 0, min(last)
        for day in range(min(last), head_day + 1):
            if day in last:
                _, cash, borrows, shares = last[day]
                last_event = day
            while k < len(changes) and changes[k][1] <= day:
                governor = changes[k][2]
                k += 1
            if day >= first:
                s["cash"].append(cash)
                s["borrows"].append(borrows)
                s["shares"].append(shares)
                s["low"].append(low_of.get(day, shares))
                s["fill"].append(day - last_event)
                s["governor"].append(governor)
        out[v] = s
    return out


def series_pair(pair):
    """Everything the later stages need from one protocol-chain pair."""
    protocol, cid = pair
    t0, calls0 = time.time(), rpc.calls_made()
    head_block, head_ts = scan_head(protocol, cid)
    head_day = head_ts // DAY
    d = DATA / "vaults" / protocol / str(cid)
    registry = {r["vault"]: r for r in jsonl(
        DATA / "registry" / "vaults" / protocol / f"{cid}.jsonl")}
    accrual, fees = read_accruals(d / "fees.csv")
    vaults, no_accrual = {}, []
    for v, r in registry.items():
        f = d / "flows" / f"{v}.csv"
        if not f.exists():
            continue
        flows = read_flows(f)
        s = replay_vault(accrual.get(v, {}), fees.get(v, {}), flows,
                         head_day)
        if s is None:
            continue
        if v not in accrual:
            no_accrual.append(v)
        s.update(kind=r["kind"], asset=r["asset"], name=r.get("name"))
        vaults[v] = s
    out = {"protocol": protocol, "chain_id": cid, "head_block": head_block,
           "head_ts": head_ts, "head_day": head_day, "vaults": vaults,
           "vaults_with_deposits_without_accrual": no_accrual,
           "holdings": holdings(protocol, cid, registry),
           "lending": lending_series(cid, head_day)
           if protocol == "euler_v2" else {},
           "rpc_calls": rpc.calls_made() - calls0}
    save(f"series-{protocol}-{cid}.pkl", out)
    print(f"series {protocol}:{cid} {len(vaults)} vaults, "
          f"{len(out['lending'])} lending, {len(out['holdings'])} holdings, "
          f"{time.time() - t0:.0f}s", flush=True)
    return pair, out["rpc_calls"]


def stage_series(only):
    """series_pair over the in-scope pairs, four processes."""
    pairs = [p for p in SCOPE if not only or f"{p[0]}:{p[1]}" in only]
    pairs.sort(key=lambda p: -sum(
        f.stat().st_size for f in (DATA / "vaults" / p[0] / str(p[1])).glob(
            "fees.csv")))
    calls = 0
    with ProcessPoolExecutor(max_workers=4) as ex:
        for _, n in ex.map(series_pair, pairs):
            calls += n
    return calls


def all_series():
    """The saved series of every in-scope pair."""
    return [load(f"series-{p}-{c}.pkl") for p, c in SCOPE]


def token_table():
    """data/prices/tokens.jsonl keyed by (chain_id, token)."""
    rows = {}
    for t in jsonl(DATA / "prices" / "tokens.jsonl"):
        rows[(t["chain_id"], t["token"])] = t
    return rows


def monthly_meta():
    """Asset symbol and decimals as data/registry/vault_aum.jsonl read them."""
    out = {}
    for r in jsonl(DATA / "registry" / "vault_aum.jsonl"):
        if r.get("asset"):
            out[(r["chain_id"], r["asset"])] = (
                r["asset_symbol"], r["price_decimals"] or r["asset_decimals"])
    return out


def chain_meta(cid, assets):
    """symbol() and decimals() at the head for assets known to neither data/prices nor vault_aum.jsonl."""
    sym = multicall.try_view(target(cid), assets, "symbol()", ["string"])
    dec = multicall.try_view(target(cid), assets, "decimals()", ["uint8"])
    return {a: (s[0] if s else None, d[0] if d else None)
            for a, s, d in zip(assets, sym, dec)}


def price_rule(family, reference):
    """Price rule of an asset from its peg family and reference."""
    if family == "usd_stable" and reference == "USD":
        return "usd_peg"
    if family == "eth" and reference == "ETH":
        return "eth_ref"
    if family == "btc" and reference == "BTC":
        return "btc_ref"
    return "own"


def last_complete_day():
    """The last UTC day whose DefiLlama quote can no longer change."""
    return int((time.time() - LLAMA_SEARCH) // DAY) - 1


def llama_chart(coin, first_day, span):
    """{day: (price, decimals)} for span days from first_day."""
    path = CACHE / "llama" / f"{coin.replace(':', '-')}-{first_day}-{span}.json"
    body = json.loads(path.read_text()) if path.exists() else None
    complete = (first_day + span) * DAY + LLAMA_SEARCH
    if body is not None and body.get("_fetched_at",
                                      path.stat().st_mtime) < complete:
        body = None
    if body is None:
        start = first_day * DAY + DAY - 1
        for attempt in range(6):
            try:
                resp = requests.get(LLAMA + coin, timeout=60, params={
                    "start": start, "span": span, "period": "1d",
                    "searchWidth": "12h"})
                if resp.status_code == 429:
                    time.sleep(5 * (attempt + 1))
                    continue
                resp.raise_for_status()
                body = resp.json()
                break
            except (requests.RequestException, ValueError):
                time.sleep(3 * (attempt + 1))
        else:
            raise RuntimeError(f"DefiLlama failed for {coin}")
        body["_fetched_at"] = time.time()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(body))
        time.sleep(0.3)
    obj = body.get("coins", {}).get(coin, {})
    out = {}
    for p in obj.get("prices", []):
        day = (p["timestamp"] - (DAY - 1) + DAY // 2) // DAY
        target_ts = day * DAY + DAY - 1
        if (first_day <= day < first_day + span
                and abs(p["timestamp"] - target_ts) <= DAY // 2
                and isinstance(p.get("price"), (int, float))
                and math.isfinite(p["price"]) and p["price"] >= 0):
            prev = out.get(day)
            if prev is None or abs(p["timestamp"] - target_ts) < prev[2]:
                out[day] = (p["price"], obj.get("decimals"),
                            abs(p["timestamp"] - target_ts))
    return {d: (p, dec) for d, (p, dec, _) in out.items()}


def stage_prices():
    """Asset metadata, price rule and daily own USD price of every asset a counted vault holds, plus the WETH and WBTC reference prices."""
    series = all_series()
    need = {}
    for s in series:
        cid, head = s["chain_id"], s["head_day"]
        for v in list(s["vaults"].values()) + list(s["lending"].values()):
            key = (cid, v["asset"])
            lo, hi = need.get(key, (v["start"], head))
            need[key] = (min(lo, v["start"]), max(hi, head))
    tokens, monthly = token_table(), monthly_meta()
    unknown = defaultdict(list)
    for cid, a in need:
        if (cid, a) not in tokens and (cid, a) not in monthly:
            unknown[cid].append(a)
    onchain = {(cid, a): m for cid, assets in unknown.items()
               for a, m in chain_meta(cid, sorted(assets)).items()}
    meta = {}
    for key in need:
        cid, a = key
        if key in tokens:
            t = tokens[key]
            symbol = t["symbol"]
            decimals = t["defillama_decimals"] or t["decimals"]
            family, reference = t["peg_family"], t["reference"]
            src = "data/prices/tokens.jsonl"
        else:
            symbol, decimals = monthly.get(key) or onchain.get(key)
            family, reference, _ = classify(
                {"chain_id": cid, "token": a, "symbol": symbol})
            src = ("data/registry/vault_aum.jsonl" if key in monthly
                   else "chain")
        meta[key] = {"symbol": symbol, "decimals": decimals,
                     "peg_family": family, "reference": reference,
                     "rule": price_rule(family, reference), "meta_source": src}
    want = dict(need)
    lo = min(lo for lo, _ in need.values())
    hi = max(hi for _, hi in need.values())
    for ref in REFERENCE.values():
        want[ref] = (min(lo, want.get(ref, (lo, hi))[0]), hi)
    prices, covered = load_price_files(want)
    windows = 0
    cap = last_complete_day()
    for key, (lo, hi) in sorted(want.items()):
        cid, a = key
        days = covered.get(key, set())
        missing = [d for d in range(lo, min(hi, cap) + 1) if d not in days]
        runs = []
        for d in missing:
            if runs and d == runs[-1][1] + 1 and d - runs[-1][0] < 400:
                runs[-1][1] = d
            else:
                runs.append([d, d])
        for a0, a1 in runs:
            chart = llama_chart(f"{CHAINS[str(cid)]['defillama']}:{a}",
                                a0, a1 - a0 + 1)
            windows += 1
            for d, (p, dec) in chart.items():
                prices.setdefault(key, {})[d] = p
                if key in meta and dec is not None and \
                        meta[key]["meta_source"] != "data/prices/tokens.jsonl":
                    meta[key]["decimals"] = dec
    save("prices.pkl", {"meta": meta, "prices": prices, "need": need,
                        "chart_windows": windows,
                        "fetch_cap": day_str(cap),
                        "rpc_calls": rpc.calls_made()})
    print(f"prices: {len(meta)} assets, rules "
          f"{dict(Counter(m['rule'] for m in meta.values()))}, "
          f"{windows} DefiLlama chart windows", flush=True)
    return rpc.calls_made()


def load_price_files(want):
    """Daily prices from data/prices/<chain>.csv for the wanted tokens, and the days each file has a row for (a blank price is."""
    prices, covered = {}, defaultdict(set)
    by_chain = defaultdict(set)
    for cid, a in want:
        by_chain[cid].add(a)
    for cid, toks in by_chain.items():
        path = DATA / "prices" / f"{cid}.csv"
        if not path.exists():
            continue
        with open(path, newline="") as fh:
            for row in csv.DictReader(fh):
                if row["token"] not in toks:
                    continue
                day = dt.date.fromisoformat(row["date"]).toordinal() - \
                    dt.date(1970, 1, 1).toordinal()
                covered[(cid, row["token"])].add(day)
                if row["price_usd"]:
                    prices.setdefault((cid, row["token"]), {})[day] = \
                        float(row["price_usd"])
    return prices, covered


def guard(values, supply, flows=None, low=None):
    """vault_aum.py's share-price guard on a daily grid."""
    counted, flags, acc_i, acc_p = [], [], [], []
    floor = DUST_SUPPLY * max(supply, default=0)
    for i, (x, s) in enumerate(zip(values, supply)):
        if x <= 0 or s <= floor:
            counted.append(x)
            flags.append(False)
            if s <= floor:
                acc_i, acc_p = [], []
            continue
        p = x / s
        if low is not None and low[i] <= floor:
            acc_i, acc_p = [i], [p]
            counted.append(x)
            flags.append(False)
            continue
        j = bisect.bisect_right(acc_i, i - JUMP_WINDOW) - 1
        ref = acc_p[max(j, 0)] if acc_p else None
        if ref is not None and p > ref * (1 + MAX_JUMP):
            if flows is not None:
                counted.append(max((counted[i - 1] if i else x) + flows[i],
                                   0))
            else:
                counted.append(int(s * acc_p[-1]))
            flags.append(True)
        else:
            acc_i.append(i)
            acc_p.append(p)
            counted.append(x)
            flags.append(False)
    return counted, flags


class Book:
    """All counted vaults (curated vaults and Euler lending vaults) with their daily counted assets, share price and USD value."""

    def __init__(self, series, price_data):
        self.meta, self.prices = price_data["meta"], price_data["prices"]
        self.head = {(s["protocol"], s["chain_id"]): s["head_day"]
                     for s in series}
        self.head_block = {(s["protocol"], s["chain_id"]): s["head_block"]
                           for s in series}
        self.v = {}
        for s in series:
            cid = s["chain_id"]
            for addr, x in s["vaults"].items():
                counted, flags = guard(x["ta"], x["supply"], x["flow"],
                                       x.get("low"))
                self.v[(cid, addr)] = {
                    "protocol": s["protocol"], "kind": x["kind"],
                    "group": GROUP[x["kind"]], "asset": x["asset"],
                    "name": x["name"], "start": x["start"], "ta": x["ta"],
                    "supply": x["supply"], "fill": x["fill"],
                    "counted": counted, "guard": flags, "governed": None}
            for addr, x in s["lending"].items():
                ta = [c + b for c, b in zip(x["cash"], x["borrows"])]
                counted, flags = guard(ta, x["shares"], low=x.get("low"))
                self.v[(cid, addr)] = {
                    "protocol": "euler_v2", "kind": "euler_lending",
                    "group": "euler_v2_lending", "asset": x["asset"],
                    "name": None, "start": x["start"], "ta": ta,
                    "cash": x["cash"], "borrows": x["borrows"],
                    "supply": x["shares"], "fill": x["fill"],
                    "counted": counted, "guard": flags,
                    "governor": x["governor"],
                    "governed": [g != ZERO for g in x["governor"]]}
        self.price_all()

    def idx(self, key, day):
        """Index of a day in a vault's series, or None outside it."""
        v = self.v[key]
        i = day - v["start"]
        return i if 0 <= i < len(v["ta"]) else None

    def per_share(self, key, i):
        """Held-vault assets (raw) per share (raw) on day index i."""
        v = self.v[key]
        if v["kind"] == "euler_lending":
            return (v["counted"][i] + EVK_VIRTUAL) / (v["supply"][i]
                                                      + EVK_VIRTUAL)
        return v["counted"][i] / v["supply"][i] if v["supply"][i] > 0 else 0

    def asset_price(self, cid, asset, rule, day):
        """(USD per whole unit, rule applied) of an asset on a day."""
        if rule == "usd_peg":
            return 1.0, rule
        key = REFERENCE.get(rule, (cid, asset))
        quotes = self.prices.get(key, {})
        for back in range(CARRY_DAYS + 1):
            if day - back in quotes:
                return quotes[day - back], rule if back == 0 else \
                    rule + "_carried"
        return None, "unpriced"

    def price_all(self):
        """USD per raw unit of each vault's asset, per day; vaults whose asset is a counted vault's share go through that vault."""
        wrapped = []
        for (cid, addr), v in self.v.items():
            if (cid, v["asset"]) in self.v:
                wrapped.append((cid, addr))
                continue
            m = self.meta[(cid, v["asset"])]
            v["rule"], v["symbol"], v["decimals"] = (
                m["rule"], m["symbol"], m["decimals"])
            v["unit_usd"], v["day_rule"] = [], []
            for i in range(len(v["ta"])):
                p, used = self.asset_price(cid, v["asset"], m["rule"],
                                           v["start"] + i)
                if m["decimals"] is None:
                    p, used = None, "unpriced"
                v["unit_usd"].append(None if p is None
                                     else p / 10 ** m["decimals"])
                v["day_rule"].append(used)
        for cid, addr in sorted(wrapped):
            v, u = self.v[(cid, addr)], self.v[(cid, self.v[(cid, addr)]
                                                ["asset"])]
            m = self.meta.get((cid, v["asset"]), {})
            v["rule"], v["symbol"] = "via_vault", m.get("symbol")
            v["decimals"] = m.get("decimals")
            v["unit_usd"], v["day_rule"] = [], []
            for i in range(len(v["ta"])):
                j = self.idx((cid, v["asset"]), v["start"] + i)
                if j is None or u.get("unit_usd") is None or \
                        u["unit_usd"][j] is None:
                    v["unit_usd"].append(None)
                    v["day_rule"].append("unpriced")
                else:
                    v["unit_usd"].append(self.per_share((cid, v["asset"]), j)
                                         * u["unit_usd"][j])
                    v["day_rule"].append("via_vault")
        for v in self.v.values():
            v["usd"] = [None if p is None else c * p
                        for c, p in zip(v["counted"], v["unit_usd"])]
            v["in_gross_all"] = [
                u is not None and u > 0
                and (v["governed"] is None or v["governed"][i])
                for i, u in enumerate(v["usd"])]
            v["in_gross"] = list(v["in_gross_all"])
            v["excluded"] = None
            v["held_by"] = [0.0] * len(v["ta"])

    def exclude(self, reasons):
        """Take excluded vaults out of the gross sum on every day; in_gross_all keeps the sum without the exclusion."""
        for key, why in reasons.items():
            v = self.v[key]
            v["excluded"] = why
            v["in_gross"] = [False] * len(v["ta"])

    def nesting(self, series):
        """Daily holdings of counted vaults by counted vaults, valued at the held vault's assets per share."""
        rows = []
        for s in series:
            cid = s["chain_id"]
            for holder, via, held, channel, points in s["holdings"]:
                if (cid, held) not in self.v or (cid, holder) not in self.v:
                    continue
                rows.extend(self._value(cid, holder, via, held, channel,
                                        points))
        for (cid, addr), v in self.v.items():
            if v["rule"] != "via_vault":
                continue
            u = self.v[(cid, v["asset"])]
            for i, x in enumerate(v["counted"]):
                day = v["start"] + i
                j = self.idx((cid, v["asset"]), day)
                if j is None or x <= 0:
                    continue
                assets = x * self.per_share((cid, v["asset"]), j)
                usd = None if u["unit_usd"][j] is None else \
                    assets * u["unit_usd"][j]
                rows.append(self._row(day, cid, addr, "", v["asset"],
                                      "asset_wrap", x, assets, usd))
        return rows

    def _value(self, cid, holder, via, held, channel, points):
        """Rows of one holding: its end-of-day balance carried forward from the first balance to the chain's head."""
        out = []
        u = self.v[(cid, held)]
        days = [d for d, _ in points]
        lo = max(points[0][0], u["start"])
        end = self.head[(self.v[(cid, holder)]["protocol"], cid)]
        for day in range(lo, end + 1):
            k = bisect.bisect_right(days, day) - 1
            if k < 0 or points[k][1] <= 0:
                continue
            j = self.idx((cid, held), day)
            if j is None:
                continue
            shares = points[k][1]
            assets = shares * self.per_share((cid, held), j)
            usd = None if u["unit_usd"][j] is None else \
                assets * u["unit_usd"][j]
            out.append(self._row(day, cid, holder, via, held, channel,
                                 shares, assets, usd))
        return out

    def _row(self, day, cid, holder, via, held, channel, shares, assets,
             usd):
        """One nesting row; subtracted only when holder and held vault are both in the gross sum that day (and, for the counterfact."""
        h, u = self.v[(cid, holder)], self.v[(cid, held)]
        i, j = self.idx((cid, holder), day), self.idx((cid, held), day)
        both = usd is not None and i is not None and j is not None
        sub = both and h["in_gross"][i] and u["in_gross"][j]
        if sub:
            u["held_by"][j] += usd
        return {"day": day, "chain_id": cid, "holder": holder,
                "holder_group": h["group"], "via": via, "held": held,
                "held_group": u["group"], "channel": channel,
                "shares": shares, "assets": int(assets),
                "usd": usd, "subtracted": sub,
                "subtracted_without_exclusion": both
                and h["in_gross_all"][i] and u["in_gross_all"][j]}


def role_holders(protocol, cid):
    """{vault: every address that ever held its owner or curator role (Moolah."""
    acc = defaultdict(set)
    path = DATA / "vaults" / protocol / str(cid) / "roles.jsonl"
    if path.exists():
        for r in jsonl(path):
            if r.get("role") in MANAGER_ROLES and r.get("address"):
                acc[r["vault"]].add(r["address"].lower())
    for r in jsonl(DATA / "registry" / "vaults" / protocol / f"{cid}.jsonl"):
        for k in ("caller", "initial_owner"):
            if r.get(k):
                acc[r["vault"]].add(r[k].lower())
    return acc


def net_shares(path, until_ts):
    """{owner: shares deposited minus withdrawn up to until_ts} with a positive balance, from a vault's flows file (share transfers are not followed)."""
    net = Counter()
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            if int(r["timestamp"]) <= until_ts:
                sh = int(r["shares"] or 0)
                net[r["owner"].lower()] += \
                    sh if r["kind"].lower() == "deposit" else -sh
    return {a: x for a, x in net.items() if x > 0}


def create_addresses(sender, nonces):
    """Addresses of the contracts sender created with CREATE at nonces 0..nonces-1."""
    raw = bytes.fromhex(sender[2:])
    out = set()
    for n in range(nonces):
        if n == 0:
            enc = b"\x80"
        elif n < 0x80:
            enc = bytes([n])
        else:
            b = n.to_bytes((n.bit_length() + 7) // 8, "big")
            enc = bytes([0x80 + len(b)]) + b
        payload = b"\x94" + raw + enc
        out.add("0x" + keccak(bytes([0xc0 + len(payload)]) + payload)[12:]
                .hex())
    return out


def owner_funded(book, curators):
    """Vaults whose money is their manager's own."""
    holders, cands = {}, {}
    for (cid, addr), v in book.v.items():
        if v["kind"] == "euler_lending":
            continue
        iv = curators.get((cid, addr), []) if curators is not None else []
        if any(e != "unattributed" for _, _, e, _ in iv):
            continue
        vals = v["usd"] if any(v["usd"]) else v["counted"]
        i = max(range(len(vals)), key=lambda k: vals[k] or 0)
        end = (v["start"] + i) * DAY + DAY - 1
        held = net_shares(DATA / "vaults" / v["protocol"] / str(cid) /
                          "flows" / f"{addr}.csv", end)
        if not held:
            continue
        top, x = max(held.items(), key=lambda kv: kv[1])
        share = x / sum(held.values())
        if share < OWNER_FUNDED_SHARE:
            continue
        pair = (v["protocol"], cid)
        if pair not in holders:
            holders[pair] = role_holders(*pair)
        cands[(cid, addr)] = {
            "pair": pair, "top_depositor": top, "top_share": share,
            "top_net_shares": x, "evaluated_end": end,
            "evaluated_on": day_str(v["start"] + i),
            "role_holders": sorted(holders[pair].get(addr, ()))}
    out = {k: dict(c, link="direct") for k, c in cands.items()
           if c["top_depositor"] in c["role_holders"]}
    rest = {k: c for k, c in cands.items() if k not in out}
    by_pair = defaultdict(set)
    for c in rest.values():
        by_pair[c["pair"]].update(c["role_holders"])
    cpath = CACHE / "owner_funded_reads.json"
    cache = (json.loads(cpath.read_text()) if cpath.exists()
             else {"owners": {}, "nonce": {}})
    safe_owners = {}
    sel = evmabi.selector("getOwners()")
    for pair, addrs in by_pair.items():
        block = book.head_block[pair]
        todo = sorted(a for a in addrs
                      if f"{pair[1]}:{a}:{block}" not in cache["owners"])
        res = read_calls(pair[1], [(a, sel) for a in todo], block) \
            if todo else []
        for a, (ok, ret) in zip(todo, res):
            try:
                owners = evmabi.decode(["address[]"], ret)[0] if ok else ()
            except Exception:
                owners = ()
            cache["owners"][f"{pair[1]}:{a}:{block}"] = sorted(
                o.lower() for o in owners)
        for a in addrs:
            safe_owners[(pair[1], a)] = set(
                cache["owners"][f"{pair[1]}:{a}:{block}"])
    created = {}
    for key, c in rest.items():
        top, cid = c["top_depositor"], key[0]
        signed = [a for a in c["role_holders"]
                  if top in safe_owners.get((cid, a), ())]
        if signed:
            out[key] = dict(c, link="safe_signer", via=signed)
            continue
        if (cid, top) not in created:
            block = book.head_block[c["pair"]]
            k = f"{cid}:{top}:{block}"
            if k not in cache["nonce"]:
                cache["nonce"][k] = int(retry(
                    rpc.call, target(cid), "eth_getTransactionCount",
                    [top, hex(block)]), 16)
            created[(cid, top)] = create_addresses(top, cache["nonce"][k])
        made = [a for a in c["role_holders"] if a in created[(cid, top)]]
        if made:
            out[key] = dict(c, link="created_contract", via=made)
    cpath.parent.mkdir(parents=True, exist_ok=True)
    cpath.write_text(json.dumps(cache, sort_keys=True))
    for c in out.values():
        c.pop("pair")
    return out


def share_transfers(cid, vaults, from_block, to_block):
    """{vault: [(timestamp, block, log_index, from, to, value)]} from the share Transfer logs of the given vaults on one chain,."""
    from common import hypersync_client as hs
    vaults = sorted(vaults)
    tag = hashlib.sha256(f"{cid}:{to_block}:{','.join(vaults)}".encode()) \
        .hexdigest()[:16]
    path = CACHE / "share_transfers" / f"{cid}-{tag}.json"
    if path.exists():
        logs = json.loads(path.read_text())
    else:
        t0 = time.time()
        raw, _ = hs.scan_logs(CHAINS[str(cid)]["hypersync"], addresses=vaults,
                              topics=[[TRANSFER_TOPIC]], from_block=from_block,
                              to_block=to_block)
        print(f"share transfers chain {cid}: {len(vaults)} vaults, {len(raw)} "
              f"logs, {time.time() - t0:.0f}s", flush=True)
        if any(lg["timestamp"] is None for lg in raw):
            raise RuntimeError(f"HyperSync returned a log without a block "
                               f"timestamp on chain {cid}")
        logs = [[lg["timestamp"], lg["block_number"], lg["log_index"],
                 lg["address"].lower(), "0x" + lg["topics"][1][-40:].lower(),
                 "0x" + lg["topics"][2][-40:].lower(), str(int(lg["data"], 16))]
                for lg in raw if len(lg["topics"]) == 3 and lg["data"]
                not in (None, "0x")]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(logs))
    out = defaultdict(list)
    for ts, block, idx, addr, src, dst, value in sorted(
            logs, key=lambda r: (r[1], r[2])):
        out[addr].append((ts, block, idx, src, dst, int(value)))
    return out


def share_screen(book, rule):
    """Share balances behind each rule candidate at the end of its evaluation day, from the vault's share Transfer logs."""
    by_chain = defaultdict(set)
    for (cid, addr) in rule:
        by_chain[cid].add(addr)
    created = {}
    for (protocol, cid) in book.head_block:
        for r in jsonl(DATA / "registry" / "vaults" / protocol / f"{cid}.jsonl"):
            created[(cid, r["vault"].lower())] = r["created_block"]
    out = {}
    for cid, addrs in sorted(by_chain.items()):
        head = max(b for (p, c), b in book.head_block.items() if c == cid)
        logs = share_transfers(cid, addrs, min(created[(cid, a)] for a in addrs),
                               head + 1)
        for addr in sorted(addrs):
            c = rule[(cid, addr)]
            top, end = c["top_depositor"], c["evaluated_end"]
            bal, sent, minted, n = Counter(), Counter(), 0, 0
            for ts, _, _, src, dst, value in logs.get(addr, []):
                if ts > end:
                    break
                n += 1
                bal[src] -= value
                bal[dst] += value
                if src == top and dst not in BURNS:
                    sent[dst] += value
                if src == ZERO and dst == top:
                    minted += value
            for b in BURNS:
                bal.pop(b, None)
            supply = sum(x for x in bal.values() if x > 0)
            if top in BURNS or not n or not minted or not bal:
                out[(cid, addr)] = {"flag": False, "transfer_logs": n,
                                    "note": "top depositor is a burn address or "
                                    "has no share mint in the Transfer logs; "
                                    "screen not applied"}
                continue
            holder, hx = max(bal.items(), key=lambda kv: (kv[1], kv[0]))
            ratio = bal[top] / c["top_net_shares"]
            out[(cid, addr)] = {
                "flag": ratio < REVIEW_BALANCE and sum(sent.values()) > 0,
                "transfer_logs": n,
                "top_balance_shares": str(bal[top]),
                "top_net_deposited_shares": str(c["top_net_shares"]),
                "top_balance_over_net_deposited": round(ratio, 6),
                "top_transferred_out_shares": str(sum(sent.values())),
                "top_transfer_recipients": [
                    {"address": a, "shares": str(x),
                     "share_of_supply": round(x / supply, 6) if supply else None}
                    for a, x in sorted(sent.items(),
                                       key=lambda kv: (-kv[1], kv[0]))[:5]],
                "largest_holder": holder,
                "largest_holder_share_of_supply":
                    round(hx / supply, 6) if supply else None}
    return out


def exclusions(book, curators):
    """({(chain_id, vault): evidence} of every excluded vault, the same for every vault sent to review)."""
    rule = owner_funded(book, curators)
    screen = share_screen(book, rule)
    out, review = {}, {}
    for key in set(rule) | {k for k in HAND_EXCLUSIONS if k in book.v}:
        ev = dict(rule.get(key, {}))
        for k in ("evaluated_end", "top_net_shares"):
            ev.pop(k, None)
        if key in screen:
            ev["share_screen"] = screen[key]
        if key not in HAND_EXCLUSIONS and screen[key]["flag"]:
            review[key] = ev
            continue
        reasons = []
        if key in rule:
            reasons.append(f"owner_funded:{rule[key]['link']}")
        if key in HAND_EXCLUSIONS:
            reasons.append(HAND_EXCLUSIONS[key][1])
        out[key] = dict(ev, reasons=reasons)
    return out, review


def write_csv(relpath, header, rows):
    """Write rows under data/ (or $CB_OUT) atomically; returns the path and row count."""
    path = DERIVED / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    n = 0
    with open(tmp, "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        for r in rows:
            w.writerow(r)
            n += 1
    os.replace(tmp, path)
    return path, n


def fmt(x, nd=2):
    """Fixed-point text, blank for None."""
    return "" if x is None else f"{x:.{nd}f}"


def vault_rows(book):
    """Rows of vault_daily.csv."""
    for (cid, addr), v in sorted(book.v.items(),
                                 key=lambda kv: (kv[1]["protocol"], kv[0])):
        if v["kind"] == "euler_lending":
            continue
        for i, ta in enumerate(v["ta"]):
            u = v["unit_usd"][i]
            yield [v["protocol"], cid, addr, v["kind"],
                   day_str(v["start"] + i), ta, v["supply"][i],
                   v["counted"][i], v["asset"], v["symbol"] or "",
                   "" if u is None else f"{u * 10 ** (v['decimals'] or 0):.8g}",
                   v["day_rule"][i], fmt(v["usd"][i]), fmt(v["held_by"][i]),
                   "accrue_interest" if v["fill"][i] == 0 else "forward_fill",
                   v["fill"][i], "share_price_jump" if v["guard"][i] else "",
                   v["excluded"] or ""]


def lending_rows(book):
    """Rows of lending_daily.csv."""
    for (cid, addr), v in sorted(book.v.items()):
        if v["kind"] != "euler_lending":
            continue
        for i, ta in enumerate(v["ta"]):
            u = v["unit_usd"][i]
            yield ["euler_v2", cid, addr, day_str(v["start"] + i),
                   v["cash"][i], v["borrows"][i], ta, v["supply"][i],
                   v["counted"][i], v["asset"], v["symbol"] or "",
                   "" if u is None else f"{u * 10 ** (v['decimals'] or 0):.8g}",
                   v["day_rule"][i], fmt(v["usd"][i]), fmt(v["held_by"][i]),
                   v["governor"][i],
                   int(v["governed"][i]),
                   "vault_status" if v["fill"][i] == 0 else "forward_fill",
                   v["fill"][i], "share_price_jump" if v["guard"][i] else ""]


VAULT_HEADER = ["protocol", "chain_id", "vault", "kind", "date",
                "total_assets", "total_supply", "counted_assets", "asset",
                "asset_symbol", "price_usd", "price_rule", "aum_usd",
                "held_by_vaults_usd", "source", "fill_days", "guard",
                "excluded_reason"]
LENDING_HEADER = ["protocol", "chain_id", "vault", "date", "cash",
                  "total_borrows", "total_assets", "total_shares",
                  "counted_assets", "asset", "asset_symbol", "price_usd",
                  "price_rule", "aum_usd", "held_by_vaults_usd", "governor",
                  "governed", "source", "fill_days", "guard"]
NEST_HEADER = ["date", "chain_id", "holder", "holder_group", "via", "held",
               "held_group", "channel", "shares", "held_assets", "usd",
               "subtracted"]

ROWS = {"morpho": {"morpho"}, "moolah": {"moolah"}, "silo_v2": {"silo_v2"},
        "euler_v2": {"euler_v2_earn", "euler_v2_lending"},
        "euler_v2_earn": {"euler_v2_earn"},
        "euler_v2_lending": {"euler_v2_lending"},
        "all": set(GROUP.values())}


def row_end(book):
    """Last day of each by_protocol row."""
    heads = defaultdict(list)
    for (cid, _), v in book.v.items():
        heads[v["group"]].append(book.head[(v["protocol"], cid)])
    return {name: min(h for g in groups for h in heads[g])
            for name, groups in ROWS.items()}


def by_protocol(book, nest):
    """Daily gross, nested and net AUM of every row in ROWS."""
    end = row_end(book)
    gross, gross_all = defaultdict(float), defaultdict(float)
    vaults = defaultdict(int)
    unpriced = defaultdict(int)
    nested, nested_all = defaultdict(float), defaultdict(float)
    for v in book.v.values():
        for i, usd in enumerate(v["usd"]):
            day = v["start"] + i
            for name, groups in ROWS.items():
                if v["group"] not in groups:
                    continue
                if v["in_gross_all"][i]:
                    gross_all[(name, day)] += usd
                if v["in_gross"][i]:
                    gross[(name, day)] += usd
                    vaults[(name, day)] += 1
                elif usd is None and v["counted"][i] > 0 and (
                        v["governed"] is None or v["governed"][i]) \
                        and not v["excluded"]:
                    unpriced[(name, day)] += 1
    for r in nest:
        for name, groups in ROWS.items():
            if r["holder_group"] in groups and r["held_group"] in groups:
                if r["subtracted"]:
                    nested[(name, r["day"])] += r["usd"]
                if r["subtracted_without_exclusion"]:
                    nested_all[(name, r["day"])] += r["usd"]
    keys = sorted((k for k in set(gross_all) | set(unpriced)
                   if k[1] <= end[k[0]]),
                  key=lambda k: (list(ROWS).index(k[0]), k[1]))
    return [{"protocol": n, "day": d, "gross": gross[(n, d)],
             "nested": nested[(n, d)],
             "aum": gross[(n, d)] - nested[(n, d)],
             "excluded": gross_all[(n, d)] - nested_all[(n, d)]
             - gross[(n, d)] + nested[(n, d)],
             "vaults": vaults[(n, d)], "unpriced": unpriced[(n, d)]}
            for n, d in keys]


CURATORS = DATA / "registry" / "vault_curators.jsonl"
CURATOR_COLUMNS = ["morpho", "moolah", "silo_v2", "euler_v2_earn",
                   "euler_v2_lending"]


def curator_intervals():
    """{(chain_id, vault): [(from_ts, to_ts, entity, confidence)]} from the attribution intervals of data/registry/vault_curato."""
    if not CURATORS.exists():
        return None
    out = defaultdict(list)
    for r in jsonl(CURATORS):
        out[(int(r["chain_id"]), r["vault"].lower())].append(
            (r["from_ts"], math.inf if r["to_ts"] is None else r["to_ts"],
             r["entity_id"] or "unattributed", r["confidence"]))
    for v in out.values():
        v.sort()
    return out


def entity_at(intervals, ts):
    """Entity and confidence of the interval covering ts."""
    for a, b, entity, confidence in intervals:
        if a <= ts < b:
            return entity, confidence
    return "unattributed", "no_interval"


def by_curator(book, curators):
    """Net AUM (aum_usd minus held_by_vaults_usd, so money held through another counted vault is credited to the outer vault) p."""
    acc = defaultdict(lambda: defaultdict(float))
    count = defaultdict(int)
    conf = defaultdict(float)
    end = min(book.head.values())
    for (cid, addr), v in book.v.items():
        iv = curators.get((cid, addr), []) if curators is not None else None
        for i, usd in enumerate(v["usd"]):
            day = v["start"] + i
            if not v["in_gross"][i]:
                continue
            net = usd - v["held_by"][i]
            if iv is None:
                key = (v["protocol"], cid, addr)
            elif day > end:
                continue
            else:
                key, c = entity_at(iv, day * DAY + DAY - 1)
                conf[(day, c)] += net
            acc[(key, day)][v["group"]] += net
            count[(key, day)] += 1
    return acc, count, conf


def stage_outputs():
    """Price, aggregate and write every file under data/aum/."""
    series, price_data = all_series(), load("prices.pkl")
    book = Book(series, price_data)
    curators = curator_intervals()
    excluded, review = exclusions(book, curators)
    book.exclude({k: ";".join(x["reasons"]) for k, x in excluded.items()})
    nest = book.nesting(series)
    inputs = {"vault_layer": "data/vaults/<protocol>/<chain>/{fees.csv,"
              "flows/,adapter_shares.csv,positions/}",
              "lending": "data/panel/euler_v2/<chain>/<vault>.csv, "
              "governance.jsonl", "prices": "data/prices/, DefiLlama chart",
              "registry": "data/registry/vaults/, markets/euler_v2/"}
    heads = {f"{s['protocol']}:{s['chain_id']}": {
        "block": s["head_block"], "timestamp": s["head_ts"],
        "date": day_str(s["head_day"])} for s in series}
    common = {"scope": [f"{p}:{c}" for p, c in SCOPE], "heads": heads,
              "inputs": inputs, "max_jump": MAX_JUMP, "dust_supply": DUST_SUPPLY,
              "jump_window_days": JUMP_WINDOW}
    path, n = write_csv(f"{OUT}/vault_daily.csv", VAULT_HEADER,
                        vault_rows(book))
    io_utils._meta_sidecar(path, GENERATOR, {
        **common, "rows": n, "what": "Daily total assets and USD AUM per "
        "curated vault (Morpho V1 and V2, Moolah, Silo vaults, Euler Earn)",
        "columns": {
            "total_assets": "raw asset units at the end of the UTC day: the "
            "day's last AccrueInterest newTotalAssets plus later same-day "
            "deposits minus withdrawals; carried forward on days without a "
            "vault event",
            "total_supply": "share supply replayed from flows and fee shares",
            "counted_assets": "total_assets, or supply times the last "
            "accepted share price on guard days",
            "price_usd": "USD per whole asset unit",
            "price_rule": "usd_peg, eth_ref, btc_ref, own, via_vault, "
            "unpriced; _carried when the last quote of up to 3 days earlier "
            "fills a DefiLlama gap",
            "aum_usd": "counted_assets in USD; blank when unpriced",
            "held_by_vaults_usd": "part of aum_usd held by other counted "
            "vaults that day (subtracted in the aggregates)",
            "source": "accrue_interest (a vault event that day) or "
            "forward_fill", "fill_days": "days since the last vault event",
            "guard": "share_price_jump when counted_assets is the book "
            "value",
            "excluded_reason": "set on every row of a vault left out of the "
            "aggregates (owner_funded:<link> by rule; self_dealing_loop or "
            "protocol_owned_liquidity by hand); see excluded_vaults.jsonl"}})

    def vault_row(key, x):
        v = book.v[key]
        vals = [(u, i) for i, u in enumerate(v["usd"]) if u is not None]
        u, i = max(vals) if vals else (None, None)
        return {
            "protocol": v["protocol"], "chain_id": key[0], "vault": key[1],
            "kind": v["kind"], "name": v["name"],
            "top_depositor": x.get("top_depositor"),
            "top_share_of_net_deposited_shares": x.get("top_share"),
            "evaluated_on": x.get("evaluated_on"),
            "role_link": x.get("link"), "role_link_via": x.get("via"),
            "share_screen": x.get("share_screen"),
            "first_day": day_str(v["start"]),
            "peak_book_usd": None if u is None else round(u, 2),
            "peak_date": None if i is None else day_str(v["start"] + i),
            "last_day": day_str(v["start"] + len(v["ta"]) - 1),
            "last_day_usd": None if v["usd"][-1] is None
            else round(v["usd"][-1], 2)}
    rule_text = (
        "owner_funded: no attribution interval in "
        "data/registry/vault_curators.jsonl names an entity; one address "
        "holds >= 90% of the net deposited shares (flows/) at the end of "
        "the vault's peak day; that address, a Safe it signs (getOwners at "
        "the scan head) or a contract it created (CREATE address of one of "
        "its nonces) holds or held the owner or curator role "
        "(roles.jsonl, registry creator and initial owner)")
    screen_text = (
        "share_screen: the vault's share Transfer logs (HyperSync, up to the "
        "scan head) at the end of the evaluation day; a vault whose top "
        "depositor holds < 90% of its net deposited shares and has "
        "transferred shares to other addresses is not excluded but listed "
        "in review_vaults.jsonl, unless a hand reason excludes it")
    rows = [dict(vault_row(key, x), reasons=x["reasons"])
            for key, x in sorted(excluded.items())]
    io_utils.write_jsonl(f"{OUT}/excluded_vaults.jsonl", rows, GENERATOR, {
        **common, "what": "Owner-funded vaults left out of every aggregate "
        "from their creation, with the reason and their peak book value",
        "rule": rule_text, "screen": screen_text,
        "self_dealing_loop": "by hand: the Base vaults of controller "
        "0x6708e21113922ed588bbccaa5ef756becbb2a7d1 named in "
        "data/resume/king-rss.md",
        "protocol_owned_liquidity": "by hand: vaults whose shares the screen "
        "finds in the protocol's own contract (Relend Timelock, MUX "
        "Mux3POL); data/resume/excluded-vaults.md"})
    rows = [dict(vault_row(key, x), decision="counted; owner_funded rule "
                 "matched but the top depositor transferred its shares out")
            for key, x in sorted(review.items())]
    io_utils.write_jsonl(f"{OUT}/review_vaults.jsonl", rows, GENERATOR, {
        **common, "what": "Vaults the owner_funded rule matches whose top "
        "depositor passed its shares on: counted in every aggregate and "
        "listed here with the transfer evidence for a hand decision",
        "rule": rule_text, "screen": screen_text})
    path, n = write_csv(f"{OUT}/lending_daily.csv", LENDING_HEADER,
                        lending_rows(book))
    io_utils._meta_sidecar(path, GENERATOR, {
        **common, "rows": n, "what": "Daily cash plus totalBorrows and USD "
        "AUM per Euler lending vault that set a collateral LTV at least once",
        "columns": {"total_assets": "cash + total_borrows at the day's last "
                    "VaultStatus, carried forward",
                    "governed": "1 when the governor at the end of the day is "
                    "nonzero; only governed days enter the aggregates",
                    "held_by_vaults_usd": "Euler Earn (or other counted "
                    "vault) deposits in this vault that day"}})
    path, n = write_csv(f"{OUT}/nesting_daily.csv", NEST_HEADER, (
        [day_str(r["day"]), r["chain_id"], r["holder"], r["holder_group"],
         r["via"], r["held"], r["held_group"], r["channel"], r["shares"],
         r["assets"], fmt(r["usd"]), int(r["subtracted"])]
        for r in sorted(nest, key=lambda r: (r["day"], r["chain_id"],
                                             r["holder"], r["held"]))))
    io_utils._meta_sidecar(path, GENERATOR, {
        **common, "rows": n, "what": "Daily vault-on-vault holdings between "
        "counted vaults, valued at the held vault's assets per share",
        "channels": {"wrapper_ledger": "Vault V2 adapter balance of a vault "
                     "share (adapter_shares.csv)", "silo_market": "Silo vault "
                     "holding another Silo vault as a market",
                     "earn_strategy": "Euler Earn strategy shares",
                     "asset_wrap": "vault whose asset is the held vault's "
                     "share (its whole counted assets)"},
        "subtracted": "1 when holder and held vault are both in the gross "
        "sum that day"})
    prot = by_protocol(book, nest)
    path, n = write_csv(f"{OUT}/by_protocol_daily.csv", [
        "protocol", "date", "aum_usd", "gross_usd", "nested_usd", "vaults",
        "unpriced_vaults", "excluded_usd"], (
            [r["protocol"], day_str(r["day"]), fmt(r["aum"]),
             fmt(r["gross"]), fmt(r["nested"]), r["vaults"], r["unpriced"],
             fmt(r["excluded"])] for r in prot))
    io_utils._meta_sidecar(path, GENERATOR, {
        **common, "rows": n, "what": "Daily curated AUM per protocol, net of "
        "vault-on-vault holdings",
        "rows_definition": {
            "morpho, moolah, silo_v2": "the protocol's curated vaults",
            "euler_v2_earn": "Euler Earn vaults",
            "euler_v2_lending": "governed Euler lending vaults",
            "euler_v2": "Earn plus governed lending, Earn deposits in those "
            "lending vaults subtracted",
            "all": "every row above together, all vault-on-vault holdings "
            "between counted vaults subtracted (cross-protocol included)"},
        "columns": {"gross_usd": "sum of aum_usd", "nested_usd": "holdings "
                    "whose holder and held vault both belong to the row",
                    "vaults": "vaults with positive priced AUM that day",
                    "unpriced_vaults": "vaults with assets but no price that "
                    "day (left out)",
                    "excluded_usd": "net AUM the row would have without the "
                    "vault exclusion (excluded_vaults.jsonl) minus aum_usd"}})
    acc, count, conf = by_curator(book, curators)
    if curators is not None:
        header = ["entity", "date", "aum_usd", "vaults"] + [
            f"{g}_usd" for g in CURATOR_COLUMNS]
        rows = ([k, day_str(d), fmt(sum(x.values())), count[(k, d)]]
                + [fmt(x.get(g, 0.0)) for g in CURATOR_COLUMNS]
                for (k, d), x in sorted(acc.items()))
        registry = {"path": "data/registry/vault_curators.jsonl",
                    "sha256": hashlib.sha256(CURATORS.read_bytes())
                    .hexdigest(), "mtime_utc": dt.datetime.fromtimestamp(
                        CURATORS.stat().st_mtime, dt.timezone.utc)
                    .isoformat(timespec="seconds"),
                    "join": "the attribution interval covering the end of "
                    "each UTC day (from_ts <= t < to_ts)"}
    else:
        header = ["protocol", "chain_id", "vault", "date", "aum_usd"]
        rows = ([*k, day_str(d), fmt(sum(x.values()))]
                for (k, d), x in sorted(acc.items()))
        registry = "absent at build time: keyed by vault"
    path, n = write_csv(f"{OUT}/by_curator_daily.csv", header, rows)
    io_utils._meta_sidecar(path, GENERATOR, {
        **common, "rows": n, "what": "Daily net AUM per curator entity "
        "(aum_usd minus held_by_vaults_usd: money held through another "
        "counted vault is credited to the outer vault), with its split by "
        "protocol group; up to the earliest scan head",
        "curator_registry": registry})
    summary = summarize(book, prot, nest, price_data, series)
    if curators is not None:
        summary["curators"] = curator_summary(acc, conf, prot)
    io_utils.write_json(f"{OUT}/summary.json", summary, GENERATOR,
                        {**common, "what": "Peak and latest AUM, coverage, "
                         "fill, guard and nesting statistics"})
    save("book.pkl", {"v": book.v, "head": book.head,
                      "head_block": book.head_block})
    print(json.dumps(summary["protocols"], indent=1)[:3000], flush=True)


def curator_summary(acc, conf, prot):
    """Attribution coverage at the overall peak and latest day, and the largest entities by peak net AUM."""
    rows = [r for r in prot if r["protocol"] == "all"]
    days = {"peak": max(rows, key=lambda r: r["aum"])["day"],
            "latest": rows[-1]["day"]}
    out = {"coverage": {}}
    for name, day in days.items():
        tot = sum(x for (d, _), x in conf.items() if d == day)
        out["coverage"][name] = {"date": day_str(day), "net_usd": round(tot),
                                 "share_by_confidence": {
                                     c: x / tot for (d, c), x in conf.items()
                                     if d == day}}
    series = defaultdict(dict)
    for (entity, day), x in acc.items():
        series[entity][day] = sum(x.values())
    top = []
    for entity, s in series.items():
        if entity == "unattributed":
            continue
        d = max(s, key=s.get)
        top.append({"entity": entity, "peak_usd": round(s[d]),
                    "peak_date": day_str(d),
                    "latest_usd": round(s.get(days["latest"], 0.0))})
    out["entities"] = len(top)
    out["top_by_peak"] = sorted(top, key=lambda r: -r["peak_usd"])[:20]
    return out


def summarize(book, prot, nest, price_data, series):
    """summary.json: peaks, latest values, coverage, fill, guard, nesting and pricing sensitivity."""
    out = {"protocols": {}, "pairs": {}, "fill": {}, "guard": {},
           "nesting": {}, "prices": {}, "unpriced_assets": []}
    by = defaultdict(list)
    for r in prot:
        by[r["protocol"]].append(r)
    for name, rows in by.items():
        peak = max(rows, key=lambda r: r["aum"])
        last = rows[-1]
        out["protocols"][name] = {
            "peak_usd": round(peak["aum"]), "peak_date": day_str(peak["day"]),
            "latest_usd": round(last["aum"]),
            "latest_date": day_str(last["day"]),
            "nested_at_peak_usd": round(peak["nested"]),
            "nested_latest_usd": round(last["nested"]),
            "max_nested_usd": round(max(r["nested"] for r in rows))}
    for s in series:
        key = f"{s['protocol']}:{s['chain_id']}"
        out["pairs"][key] = {
            "head_date": day_str(s["head_day"]),
            "vaults_with_deposits": len(s["vaults"]),
            "lending_vaults": len(s["lending"]),
            "vaults_with_deposits_without_accrual":
                s["vaults_with_deposits_without_accrual"],
            "days_with_flows_after_last_accrual":
                sum(v["after_accrual_days"] for v in s["vaults"].values())}
    cover = defaultdict(lambda: Counter())
    fill = defaultdict(list)
    for (cid, addr), v in book.v.items():
        g = v["group"]
        for i, x in enumerate(v["counted"]):
            if x <= 0:
                continue
            c = cover[g]
            c["vault_days_with_assets"] += 1
            c["priced"] += v["usd"][i] is not None
            c["guard_days"] += v["guard"][i]
            if v["usd"][i] is not None:
                fill[g].append((v["fill"][i], v["usd"][i]))
    out["prices"]["vault_days"] = {g: dict(c) | {
        "share_priced": c["priced"] / c["vault_days_with_assets"]}
        for g, c in cover.items()}
    total = Counter()
    for c in cover.values():
        total.update(c)
    out["prices"]["vault_days"]["all"] = dict(total) | {
        "share_priced": total["priced"] / total["vault_days_with_assets"]}
    for g, pts in list(fill.items()) + [("all", [p for v in fill.values()
                                                  for p in v])]:
        f = [p[0] for p in pts]
        w = sum(p[1] for p in pts) or 1
        out["fill"][g] = {
            "vault_days": len(f), "share_fill_0": f.count(0) / len(f),
            "share_fill_le_1": sum(x <= 1 for x in f) / len(f),
            "share_fill_le_7": sum(x <= 7 for x in f) / len(f),
            "share_fill_gt_30": sum(x > 30 for x in f) / len(f),
            "max_fill": max(f), "median_fill": statistics.median(f),
            "usd_weighted_share_fill_0": sum(p[1] for p in pts
                                             if p[0] == 0) / w,
            "usd_weighted_share_fill_le_1": sum(p[1] for p in pts
                                                if p[0] <= 1) / w,
            "usd_weighted_share_fill_gt_7": sum(p[1] for p in pts
                                                if p[0] > 7) / w}
    peaks = []
    for (cid, addr), v in book.v.items():
        vals = [(u, i) for i, u in enumerate(v["usd"]) if u is not None]
        if not vals or v["excluded"]:
            continue
        u, i = max(vals)
        latest = v["usd"][-1]
        peaks.append({"protocol": v["protocol"], "chain_id": cid,
                      "vault": addr, "kind": v["kind"], "name": v["name"],
                      "asset_symbol": v["symbol"], "peak_usd": round(u),
                      "peak_date": day_str(v["start"] + i),
                      "latest_usd": None if latest is None else round(latest),
                      "guard_days": sum(v["guard"])})
    peaks.sort(key=lambda r: -r["peak_usd"])
    out["top_vaults_by_peak"] = peaks[:20]
    guarded = []
    for (cid, addr), v in book.v.items():
        days = [i for i, g in enumerate(v["guard"]) if g and v["usd"][i]]
        if not days:
            continue
        i = max(days, key=lambda k: v["usd"][k])
        removed = max((v["ta"][k] - v["counted"][k]) * v["unit_usd"][k]
                      for k in days)
        guarded.append({"protocol": v["protocol"], "chain_id": cid,
                        "vault": addr, "kind": v["kind"], "name": v["name"],
                        "excluded": v["excluded"], "guard_days": len(days),
                        "max_counted_usd_on_guard_days": round(v["usd"][i]),
                        "date": day_str(v["start"] + i),
                        "max_usd_removed": round(removed)})
    out["guard"] = {"vaults": len(guarded), "largest": sorted(
        guarded, key=lambda r: -r["max_counted_usd_on_guard_days"])[:15]}
    for g in ("eth_ref", "btc_ref", "usd_peg", "own", "via_vault"):
        out["prices"].setdefault("vaults_by_rule", {})[g] = sum(
            1 for v in book.v.values() if v["rule"] == g)
    out["prices"]["vault_days_by_rule"] = dict(Counter(
        r for v in book.v.values() for r, x in zip(v["day_rule"], v["counted"])
        if x > 0))
    unpriced = defaultdict(lambda: {"vault_days": 0, "vaults": set(),
                                    "max_units": 0.0})
    for (cid, addr), v in book.v.items():
        for i, x in enumerate(v["counted"]):
            if x > 0 and v["usd"][i] is None:
                u = unpriced[(cid, v["asset"], v["symbol"], v["rule"])]
                u["vault_days"] += 1
                u["vaults"].add(addr)
                if v["decimals"] is not None:
                    u["max_units"] = max(u["max_units"],
                                         x / 10 ** v["decimals"])
    out["unpriced_assets"] = sorted(
        ({"chain_id": k[0], "asset": k[1], "symbol": k[2], "rule": k[3],
          "vault_days": u["vault_days"], "vaults": len(u["vaults"]),
          "max_whole_units_in_one_vault": round(u["max_units"], 2)}
         for k, u in unpriced.items()), key=lambda r: -r["vault_days"])
    chan = defaultdict(lambda: defaultdict(float))
    for r in nest:
        if r["subtracted"]:
            chan[r["channel"]][r["day"]] += r["usd"]
    for c, days in chan.items():
        peak = max(days, key=days.get)
        last = max(days)
        out["nesting"][c] = {"holding_days": sum(
            1 for r in nest if r["subtracted"] and r["channel"] == c),
            "max_usd": round(days[peak]), "max_date": day_str(peak),
            "latest_usd": round(days[last]), "latest_date": day_str(last)}
    out["exclusion"] = exclusion_summary(book, by)
    out["review"] = review_summary(book)
    out["pricing_sensitivity"] = sensitivity(book)
    out["defillama_chart_windows"] = price_data["chart_windows"]
    return out


def exclusion_summary(book, rows_by_protocol):
    """Excluded vaults by reason, the largest by peak book value, and the net AUM each by_protocol row loses on its latest day, on its peak day and at most."""
    ex = [(key, v) for key, v in book.v.items() if v["excluded"]]
    largest = []
    for (cid, addr), v in ex:
        vals = [(u, i) for i, u in enumerate(v["usd"]) if u is not None]
        if vals:
            u, i = max(vals)
            largest.append({"chain_id": cid, "vault": addr, "name": v["name"],
                            "reasons": v["excluded"],
                            "peak_book_usd": round(u),
                            "peak_date": day_str(v["start"] + i)})
    removed = {}
    for name, rows in rows_by_protocol.items():
        peak = max(rows, key=lambda r: r["aum"])
        top = max(rows, key=lambda r: r["excluded"])
        removed[name] = {
            "latest": {"date": day_str(rows[-1]["day"]),
                       "usd": round(rows[-1]["excluded"])},
            "on_peak_day": {"date": day_str(peak["day"]),
                            "usd": round(peak["excluded"])},
            "max": {"date": day_str(top["day"]), "usd": round(top["excluded"])}}
    return {"vaults": len(ex),
            "by_reason": dict(Counter(v["excluded"] for _, v in ex)),
            "by_protocol": dict(Counter(v["protocol"] for _, v in ex)),
            "largest_by_peak": sorted(largest,
                                      key=lambda r: -r["peak_book_usd"])[:20],
            "removed_net_usd": removed}


def review_summary(book):
    """Vaults the share-transfer screen sent to review (counted)."""
    rows = [r for r in jsonl(DERIVED / OUT / "review_vaults.jsonl")] \
        if (DERIVED / OUT / "review_vaults.jsonl").exists() else []
    return {"vaults": len(rows),
            "largest_by_peak": sorted(
                ({"chain_id": r["chain_id"], "vault": r["vault"],
                  "name": r["name"], "peak_book_usd": r["peak_book_usd"],
                  "last_day_usd": r["last_day_usd"],
                  "largest_holder": (r["share_screen"] or {}).get(
                      "largest_holder")} for r in rows),
                key=lambda r: -(r["peak_book_usd"] or 0))[:20]}


def sensitivity(book):
    """What the peg and reference rules change against each asset's own DefiLlama price, summed over vault-days in the gross sum."""
    eff = {"usd_peg_below_0.97": defaultdict(float),
           "eth_ref": defaultdict(float), "btc_ref": defaultdict(float)}
    days = Counter()
    for (cid, _), v in book.v.items():
        if v["rule"] not in ("usd_peg", "eth_ref", "btc_ref"):
            continue
        own = book.prices.get((cid, v["asset"]), {})
        for i, usd in enumerate(v["usd"]):
            day = v["start"] + i
            if not v["in_gross"][i]:
                continue
            q = own.get(day)
            if q is None:
                days[v["rule"] + "_without_own_price"] += 1
                continue
            if v["rule"] == "usd_peg":
                if q < 0.97:
                    eff["usd_peg_below_0.97"][day] += usd * (1 - q)
                    days["usd_peg_below_0.97"] += 1
            else:
                ref = book.prices.get(REFERENCE[v["rule"]], {}).get(day)
                if ref:
                    eff[v["rule"]][day] += usd * (q / ref - 1)
    out = {"vault_days": dict(days)}
    for k, series in eff.items():
        if series:
            d = max(series, key=lambda x: abs(series[x]))
            out[k] = {"max_abs_usd": round(series[d]), "date": day_str(d),
                      "meaning": "counted minus own-price value"
                      if k.startswith("usd") else
                      "own-price value minus counted value"}
    return out


STATE_GAPS = ("metadata is not found", "header not found",
              "missing trie node")


def retry(fn, *args, **kwargs):
    """Call fn, retrying archive answers that another dRPC upstream serves (Linea nodes intermittently answer 'metadata is not found')."""
    for attempt in range(6):
        try:
            return fn(*args, **kwargs)
        except RuntimeError as exc:
            if attempt == 5 or not any(g in str(exc) for g in STATE_GAPS):
                raise
            time.sleep(2 * (attempt + 1))


def clock_for(clocks, cid):
    """One blocktime.BlockClock per chain, created on first use."""
    if cid not in clocks:
        clocks[cid] = retry(blocktime.BlockClock, target(cid))
    return clocks[cid]


def block_ts(target_, number):
    """Timestamp of a block; None when the node has no such block yet."""
    b = rpc.get_block(target_, number)
    return None if b is None else int(b["timestamp"], 16)


def last_block(clock, ts):
    """The last block with timestamp <= ts."""
    if ts >= clock.head[0]:
        return clock.head[1]
    lo = clock.block_at_verified(ts, tol_secs=120)
    if block_ts(clock.target, lo) > ts:
        raise RuntimeError(f"blocktime returned a block after {ts}")
    step = 1
    while True:
        hi = min(lo + step, clock.head[1])
        t = block_ts(clock.target, hi)
        if t is None or t > ts:
            break
        if hi == clock.head[1]:
            return hi
        lo, step = hi, step * 2
    while hi - lo > 1:
        mid = (lo + hi) // 2
        t = block_ts(clock.target, mid)
        if t is not None and t <= ts:
            lo = mid
        else:
            hi = mid
    return lo


def check_monthly(book):
    """Month-start totalAssets of data/registry/vault_aum.jsonl against the end-of-previous-day value here."""
    scope = {(p, c) for p, c in SCOPE}
    diffs, off, absent, extra = [], [], Counter(), 0
    seen = set()
    for r in jsonl(DATA / "registry" / "vault_aum.jsonl"):
        if (r["protocol"], r["chain_id"]) not in scope:
            continue
        key = (r["chain_id"], r["vault"])
        readings = {int(t): int(x) for t, x in r["total_assets"].items()}
        readings.update({int(t): int(x) for t, x in
                         r["share_price_jump"].items()})
        readings.update({int(t): int(x) for t, x in r["over_supply"].items()})
        for ts, theirs in sorted(readings.items()):
            day = ts // DAY - 1
            seen.add((key, day))
            v = book.v.get(key)
            i = None if v is None else day - v["start"]
            if v is None or i < 0 or i >= len(v["ta"]):
                why = ("not in the vault layer with deposits" if v is None
                       else "before first deposit" if i < 0
                       else "after scan head")
                absent[why] += 1
                off.append({"chain_id": key[0], "vault": key[1],
                            "kind": r["kind"], "month": day_str(day + 1),
                            "monthly": str(theirs), "daily": None,
                            "reason": why})
                continue
            mine = v["ta"][i]
            rel = (mine - theirs) / theirs if theirs else math.inf
            diffs.append(rel)
            if abs(rel) > 0.01:
                off.append({"chain_id": key[0], "vault": key[1],
                            "kind": r["kind"], "month": day_str(day + 1),
                            "monthly": str(theirs), "daily": str(mine),
                            "rel_diff": rel, "fill_days": v["fill"][i],
                            "monthly_usd": r["usd"].get(str(ts)),
                            "reason": None})
    for key, v in book.v.items():
        for i, x in enumerate(v["ta"]):
            day = v["start"] + i
            if x > 0 and (dt.date(1970, 1, 1) + dt.timedelta(day + 1)).day \
                    == 1 and (key, day) not in seen:
                extra += 1
    return {"compared": len(diffs), "absent": dict(absent),
            "abs_rel_diff": distribution([abs(x) for x in diffs]),
            "within_1e-6": sum(abs(x) <= 1e-6 for x in diffs),
            "within_1e-4": sum(abs(x) <= 1e-4 for x in diffs),
            "within_1pct": sum(abs(x) <= 0.01 for x in diffs),
            "nonzero_here_absent_monthly": extra}, off


def distribution(xs):
    """Count, min, quantiles and max of a list."""
    if not xs:
        return {"n": 0}
    s = sorted(xs)

    def q(p):
        return s[min(len(s) - 1, int(p * (len(s) - 1) + 0.5))]
    return {"n": len(s), "min": s[0], "p50": q(0.5), "p90": q(0.9),
            "p99": q(0.99), "max": s[-1]}


def explain_monthly(off, clocks):
    """Reason for every vault-month off by more than 1 percent, from totalAssets() reread at the last block of the day before t."""
    sel = evmabi.selector("totalAssets()")
    groups = defaultdict(list)
    for o in off:
        if o["reason"] is None:
            groups[(o["chain_id"], o["month"])].append(o)
    path = CACHE / "monthly_reread.json"
    done = json.loads(path.read_text()) if path.exists() else {}
    for (cid, month), cases in sorted(groups.items()):
        key = f"{cid}:{month}"
        vaults = sorted({o["vault"] for o in cases})
        if key not in done or set(done[key]["values"]) != set(vaults):
            ts = int(dt.datetime.fromisoformat(month + "T00:00:00+00:00")
                     .timestamp())
            b = retry(last_block, clock_for(clocks, cid), ts - 1)
            res = read_calls(cid, [(v, sel) for v in vaults], b)
            done[key] = {"block": b, "values": {
                v: str(int(ret, 16)) if ok and ret not in ("0x", "") else None
                for v, (ok, ret) in zip(vaults, res)}}
            path.write_text(json.dumps(done))
            print(f"monthly {cid} {month}: {len(cases)} cases at block {b}, "
                  f"RPC {rpc.calls_made()}", flush=True)
        b = done[key]["block"]
        for o in cases:
            chain = done[key]["values"][o["vault"]]
            chain = None if chain is None else int(chain)
            mine, theirs = int(o["daily"]), int(o["monthly"])
            o["day_end_block"] = b
            o["chain_at_day_end"] = None if chain is None else str(chain)
            if chain is None:
                o["reason"] = "unreadable on chain"
                continue
            o["rel_diff_vs_chain"] = (mine - chain) / chain if chain else None
            if mine == chain:
                o["reason"] = ("daily value equals totalAssets() at the day's "
                               "last block; the monthly reading comes from an "
                               "earlier block with a different state")
            elif abs(chain - theirs) > 0.01 * theirs:
                o["reason"] = ("totalAssets() at the day's last block differs "
                               "from the monthly reading: state changed after "
                               "the monthly reading's block")
            elif mine < chain and o["fill_days"] > 0:
                o["reason"] = ("forward-filled: interest accrued since the "
                               "last vault event")
            elif mine < chain:
                o["reason"] = ("totalAssets() rose after the day's last vault "
                               "event (market interest)")
            else:
                o["reason"] = ("totalAssets() fell after the last vault event "
                               "without a vault event (a loss realized in a "
                               "market; Vault V2 reports min(realAssets, "
                               "capped growth))")
    return off


def check_chain(book, n, seed, clocks, lending, weighted=False):
    """totalAssets() at the last block of random vault-days with assets (the scan head's block on a chain's last day), from vau."""
    rng = random.Random(seed)
    pool = [(k, i) for k, v in book.v.items()
            if (v["kind"] == "euler_lending") == lending
            for i, x in enumerate(v["ta"]) if x > 0]
    if weighted:
        w = [book.v[k]["usd"][i] or 0 for k, i in pool]
        picked = []
        while len(picked) < n:
            p = rng.choices(pool, weights=w)[0]
            if p not in picked:
                picked.append(p)
    else:
        picked = rng.sample(pool, n)
    sel = evmabi.selector("totalAssets()")
    path = CACHE / "chain_sample.json"
    done = json.loads(path.read_text()) if path.exists() else {}
    out = []
    for key, i in picked:
        cid, addr = key
        v = book.v[key]
        day = v["start"] + i
        tag = f"{cid}:{addr}:{day}"
        if tag not in done:
            key_head = (v["protocol"], cid)
            b = book.head_block[key_head] if day == book.head[key_head] \
                else retry(last_block, clock_for(clocks, cid),
                           day * DAY + DAY - 1)
            ok, ret = retry(multicall.aggregate3, target(cid),
                            [(addr, sel)], block=b)[0]
            done[tag] = {"block": b, "chain": str(int(ret, 16))
                         if ok and ret not in ("0x", "") else None}
            path.write_text(json.dumps(done))
        b = done[tag]["block"]
        chain = None if done[tag]["chain"] is None else int(done[tag]["chain"])
        mine = v["ta"][i]
        out.append({"chain_id": cid, "vault": addr, "kind": v["kind"],
                    "date": day_str(day), "block": b, "daily": str(mine),
                    "chain": None if chain is None else str(chain),
                    "rel_diff": None if not chain else (mine - chain) / chain,
                    "fill_days": v["fill"][i],
                    "usd": v["usd"][i]})
    return out


def read_calls(cid, calls, block, chunk=50):
    """aggregate3 in batches of chunk, halving a batch whose eth_call fails (one call that burns the gas cap fails the whole ba."""
    out = []
    for i in range(0, len(calls), chunk):
        part = calls[i:i + chunk]
        try:
            out.extend(retry(multicall.aggregate3, target(cid), part,
                             block=block, chunk=len(part)))
        except RuntimeError:
            if len(part) == 1:
                out.append((False, "0x"))
                continue
            h = len(part) // 2
            out.extend(read_calls(cid, part[:h], block, h))
            out.extend(read_calls(cid, part[h:], block, len(part) - h))
    return out


def check_head(book):
    """totalAssets() of every counted vault with assets on its last day, read at its scan head (the block that day's value describes)."""
    sel = evmabi.selector("totalAssets()")
    by_pair = defaultdict(list)
    for (cid, addr), v in book.v.items():
        if v["ta"][-1] > 0:
            by_pair[(v["protocol"], cid)].append(addr)
    rows = []
    for (protocol, cid), vaults in sorted(by_pair.items()):
        b = book.head_block[(protocol, cid)]
        res = read_calls(cid, [(a, sel) for a in vaults], b)
        for a, (ok, ret) in zip(vaults, res):
            v = book.v[(cid, a)]
            chain = int(ret, 16) if ok and ret not in ("0x", "") else None
            unit = v["unit_usd"][-1]
            rows.append({
                "protocol": protocol, "chain_id": cid, "vault": a,
                "kind": v["kind"], "block": b, "daily": str(v["ta"][-1]),
                "chain": None if chain is None else str(chain),
                "rel_diff": (v["ta"][-1] - chain) / chain if chain else None,
                "usd_diff": None if chain is None or unit is None
                else (v["ta"][-1] - chain) * unit,
                "fill_days": v["fill"][-1], "guard": v["guard"][-1]})
    def side(r):
        """guarded: the day counts at book value, so the gap does not enter AUM; above."""
        mine, chain = int(r["daily"]), int(r["chain"])
        if r["guard"]:
            return "guarded"
        if mine > chain:
            return "above"
        return "below_interest" if chain <= 1.1 * mine else "below_phantom"
    groups = defaultdict(list)
    for r in rows:
        if r["chain"] is not None and r["daily"] != r["chain"]:
            groups[side(r)].append(r)
    out = {"vaults": len(rows),
           "unreadable": sum(r["chain"] is None for r in rows),
           "exact": sum(r["daily"] == r["chain"] for r in rows),
           "abs_rel_diff": distribution([abs(r["rel_diff"]) for r in rows
                                         if r["rel_diff"] is not None]),
           "within_1e-4": sum(r["rel_diff"] is not None
                              and abs(r["rel_diff"]) <= 1e-4 for r in rows),
           "within_1pct": sum(r["rel_diff"] is not None
                              and abs(r["rel_diff"]) <= 0.01 for r in rows)}
    for name, rs in groups.items():
        known = [r for r in rs if r["usd_diff"] is not None]
        out[name] = {"vaults": len(rs),
                     "usd_daily_minus_chain": sum(r["usd_diff"]
                                                  for r in known),
                     "over_10k_usd": sorted(
                         (r for r in known if abs(r["usd_diff"]) > 1e4),
                         key=lambda r: -abs(r["usd_diff"]))}
    out["unreadable_vaults"] = [r for r in rows if r["chain"] is None]
    return out


def check_nesting(book, days_n, seed, clocks):
    """For random days between the first day the Morpho wrapper subtraction reaches $1M and the day before the earliest Morpho scan head."""
    nest = defaultdict(float)
    with open(DERIVED / OUT / "nesting_daily.csv", newline="") as fh:
        for r in csv.DictReader(fh):
            if (r["channel"] == "wrapper_ledger" and r["subtracted"] == "1"
                    and r["holder_group"] == r["held_group"] == "morpho"):
                nest[(int(r["chain_id"]), r["date"])] += float(r["usd"])
    rng = random.Random(seed)
    chains = [c for p, c in SCOPE if p == "morpho"]
    epoch = dt.date(1970, 1, 1).toordinal()
    total = defaultdict(float)
    for (_, d), x in nest.items():
        total[dt.date.fromisoformat(d).toordinal() - epoch] += x
    lo = min(d for d, x in total.items() if x >= 1e6)
    hi = min(book.head[("morpho", c)] for c in chains) - 1
    days = sorted(rng.sample(range(lo, hi + 1), days_n))
    out = []
    for cid in chains:
        ledger = defaultdict(list)
        with open(DATA / "vaults" / "morpho" / str(cid) /
                  "adapter_shares.csv", newline="") as fh:
            for r in csv.DictReader(fh):
                ledger[(r["vault"], r["adapter"], r["underlying_vault"])
                       ].append(((int(r["block"]), int(r["log_index"])),
                                 int(r["timestamp"]), int(r["cum_shares"])))
        clock = clock_for(clocks, cid)
        for day in days:
            end = day * DAY + DAY - 1
            rebuilt, held_raw = 0.0, []
            for (pv, ad, u), rows in ledger.items():
                hk, uk = (cid, pv), (cid, u)
                if hk not in book.v or uk not in book.v or \
                        book.v[uk]["group"] != "morpho":
                    continue
                past = [r for r in rows if r[1] <= end]
                if not past or max(past)[2] <= 0:
                    continue
                bal = max(past)[2]
                hv, uv = book.v[hk], book.v[uk]
                i, j = day - hv["start"], day - uv["start"]
                if not (0 <= i < len(hv["ta"]) and 0 <= j < len(uv["ta"])):
                    continue
                if not (hv["in_gross"][i] and uv["in_gross"][j]):
                    continue
                price = uv["counted"][j] / uv["supply"][j]
                rebuilt += bal * price * uv["unit_usd"][j]
                held_raw.append((ad, u, bal, uv["unit_usd"][j]))
            if not held_raw:
                out.append({"chain_id": cid, "date": day_str(day),
                            "holdings": 0, "subtracted_usd":
                            nest.get((cid, day_str(day)), 0.0)})
                continue
            b = retry(last_block, clock, end)
            calls = [(u, evmabi.encode_call("balanceOf(address)",
                                            ["address"], [ad]))
                     for ad, u, _, _ in held_raw]
            res = retry(multicall.aggregate3, target(cid), calls, block=b)
            bals = [int(r, 16) if ok and r not in ("0x", "") else None
                    for ok, r in res]
            conv = retry(multicall.aggregate3, target(cid), [
                (u, evmabi.encode_call("convertToAssets(uint256)",
                                       ["uint256"], [x or 0]))
                for (_, u, _, _), x in zip(held_raw, bals)], block=b)
            onchain = sum(int(r, 16) * h[3] for (ok, r), h in zip(conv,
                                                                   held_raw)
                          if ok and r not in ("0x", ""))
            bal_match = sum(x == h[2] for x, h in zip(bals, held_raw))
            sub = nest.get((cid, day_str(day)), 0.0)
            out.append({"chain_id": cid, "date": day_str(day), "block": b,
                        "holdings": len(held_raw),
                        "ledger_balance_equals_chain": bal_match,
                        "subtracted_usd": sub, "rebuilt_from_ledger_usd":
                        rebuilt, "rel_diff_rebuilt":
                        (sub - rebuilt) / rebuilt if rebuilt else None,
                        "onchain_convertToAssets_usd": onchain,
                        "rel_diff_onchain":
                        (sub - onchain) / onchain if onchain else None})
            print(f"nesting {cid} {day_str(day)}: subtracted {sub:,.0f} "
                  f"ledger {rebuilt:,.0f} chain {onchain:,.0f} "
                  f"({bal_match}/{len(held_raw)} balances)", flush=True)
    return out


def stage_validate(n_chain, n_days, seed):
    """validation.json: monthly file, archive samples, every vault at the scan head, wrapper nesting."""
    book = types.SimpleNamespace(**load("book.pkl"))
    clocks = {}
    t0 = rpc.calls_made()
    monthly, off = check_monthly(book)
    off = explain_monthly(off, clocks)
    reasons = Counter(o["reason"] for o in off)
    chain = check_chain(book, n_chain, seed, clocks, lending=False)
    lend = check_chain(book, n_chain // 5, seed + 2, clocks, lending=True)
    heavy = check_chain(book, n_chain // 2, seed + 3, clocks, lending=False,
                        weighted=True)
    rel = [abs(c["rel_diff"]) for c in chain if c["rel_diff"] is not None]
    head = check_head(book)
    nesting = check_nesting(book, n_days, seed + 1, clocks)
    result = {
        "monthly": {**monthly, "off_by_more_than_1pct": len(off),
                    "reasons": dict(reasons), "cases": off},
        "chain_sample": {
            "n": len(chain), "seed": seed,
            "abs_rel_diff": distribution(rel),
            "abs_rel_diff_fill_0": distribution(
                [abs(c["rel_diff"]) for c in chain
                 if c["rel_diff"] is not None and c["fill_days"] == 0]),
            "abs_rel_diff_fill_gt_0": distribution(
                [abs(c["rel_diff"]) for c in chain
                 if c["rel_diff"] is not None and c["fill_days"] > 0]),
            "exact": sum(c["daily"] == c["chain"] for c in chain),
            "within_1e-6": sum(x <= 1e-6 for x in rel),
            "within_1e-4": sum(x <= 1e-4 for x in rel),
            "within_1pct": sum(x <= 0.01 for x in rel),
            "unreadable": sum(c["chain"] is None for c in chain),
            "points": chain},
        "chain_sample_usd_weighted": {
            "n": len(heavy), "seed": seed + 3,
            "abs_rel_diff": distribution([abs(c["rel_diff"]) for c in heavy
                                          if c["rel_diff"] is not None]),
            "within_1e-4": sum(abs(c["rel_diff"]) <= 1e-4 for c in heavy
                               if c["rel_diff"] is not None),
            "within_1e-3": sum(abs(c["rel_diff"]) <= 1e-3 for c in heavy
                               if c["rel_diff"] is not None),
            "points": heavy},
        "chain_sample_lending": {
            "n": len(lend), "seed": seed + 2,
            "abs_rel_diff": distribution([abs(c["rel_diff"]) for c in lend
                                          if c["rel_diff"] is not None]),
            "points": lend},
        "head": head,
        "nesting": {"days": n_days, "seed": seed + 1, "checks": nesting},
        "rpc_calls": rpc.calls_made() - t0}
    io_utils.write_json(f"{OUT}/validation.json", result, GENERATOR, {
        "what": "E3 AUM validation: monthly file, archive totalAssets, "
        "wrapper nesting", "rpc_calls": result["rpc_calls"]})
    print(json.dumps({k: v for k, v in result["monthly"].items()
                      if k != "cases"}, indent=1), flush=True)
    print(json.dumps({k: v for k, v in result["chain_sample"].items()
                      if k != "points"}, indent=1), flush=True)
    return result["rpc_calls"]


def main():
    """Run the requested stages and report RPC calls."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", default="all", choices=[
        "series", "prices", "outputs", "validate", "all"])
    ap.add_argument("--only", nargs="*", help="protocol:chain pairs "
                    "(series stage)")
    ap.add_argument("--chain-points", type=int, default=100)
    ap.add_argument("--nesting-days", type=int, default=10)
    ap.add_argument("--seed", type=int, default=3003)
    args = ap.parse_args()
    t0 = time.time()
    calls = 0
    if args.stage in ("series", "all"):
        calls += stage_series(args.only)
    if args.stage in ("prices", "all"):
        calls += stage_prices()
    if args.stage in ("outputs", "all"):
        before = rpc.calls_made()
        stage_outputs()
        calls += rpc.calls_made() - before
    if args.stage in ("validate", "all"):
        calls += stage_validate(args.chain_points, args.nesting_days,
                                args.seed)
    print(f"done in {time.time() - t0:.0f}s, RPC calls {calls}", flush=True)


if __name__ == "__main__":
    main()
