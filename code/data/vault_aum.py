# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""E1, step 2: curated AUM of every vault on a monthly grid, in USD, and the chain selection it implies."""
import argparse
import calendar
import datetime as dt
import json
import pathlib
import re
import sys
import time
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import requests

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common import (blocktime, evmabi, io_utils, logs,
                    multicall, rpc)

START = (2024, 1)
THRESHOLD_USD = 100e6
FUNDED_USD = 10e3
MAX_JUMP = 0.10
OVER_SUPPLY = 10
FEEDS = {
    "ETH": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
    "BTC": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
    "BNB": "0x14e613AC84a31f709eadbdF89C6CC390fDc9540A",
}
EUR_USD = 1.10
CLASS_RULES = [
    ("BTC", re.compile(r"btc", re.I)),
    ("ETH", re.compile(r"eth", re.I)),
    ("BNB", re.compile(r"bnb", re.I)),
    ("EUR", re.compile(r"eur", re.I)),
    ("USD", re.compile(r"usd|dai|gho|frax|honey|usr\b|^m$|^wm$", re.I)),
]
PARTS = "registry/vault_aum_parts"
LLAMA = "https://coins.llama.fi/batchHistorical"
LLAMA_NOW = "https://coins.llama.fi/prices/current/"
SEL_TOTAL = evmabi.selector("totalAssets()")
SEL_SUPPLY = evmabi.selector("totalSupply()")
SEL_BALANCE = "balanceOf(address)"
SEL_CONVERT = "convertToAssets(uint256)"


def month_grid():
    y, m = START
    now = dt.datetime.now(dt.timezone.utc)
    out = []
    while (y, m) <= (now.year, now.month):
        out.append(calendar.timegm((y, m, 1, 0, 0, 0)))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def month(ts):
    return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime(
        "%Y-%m")


def asset_class(symbol):
    for name, pat in CLASS_RULES:
        if symbol and pat.search(symbol):
            return name
    return None


def load_vaults():
    """Registry vaults plus the Euler lending vaults that lend."""
    by_chain = defaultdict(list)
    reg = io_utils.DATA / "registry"
    for f in sorted((reg / "vaults").glob("*/*.jsonl")):
        for line in open(f):
            r = json.loads(line)
            by_chain[str(r["chain_id"])].append(r)
    for f in sorted((reg / "markets" / "euler_v2").glob("*.jsonl")):
        rows = [json.loads(line) for line in open(f)]
        lends = {r["vault"] for r in rows if r.get("type") == "ltv"
                 and (r["borrow_ltv"] or r["liquidation_ltv"])}
        for r in rows:
            if r.get("type") == "vault" and r["vault"] in lends:
                by_chain[str(r["chain_id"])].append({
                    "protocol": "euler_v2", "chain_id": r["chain_id"],
                    "kind": "euler_lending", "vault": r["vault"],
                    "asset": r["asset"], "name": None,
                    "official_factory": True, "found_by": "factory_event",
                    "created_block": r["created_block"],
                    "created_ts": r["created_ts"]})
    return by_chain


def read(target, calls, block, chunk=50, retry=True):
    """aggregate3 that halves a batch when the whole eth_call fails, and rereads failed entries once in small batches."""
    out = []
    for i in range(0, len(calls), chunk):
        part = calls[i:i + chunk]
        try:
            out.extend(multicall.aggregate3(target, part, block=block,
                                            chunk=len(part)))
        except RuntimeError:
            if len(part) == 1:
                out.append((False, "0x"))
                continue
            h = len(part) // 2
            out.extend(read(target, part[:h], block, h, retry=False))
            out.extend(read(target, part[h:], block, len(part) - h,
                            retry=False))
    bad = [i for i, (ok, _) in enumerate(out) if not ok]
    if retry and bad and len(calls) > 10:
        again = read(target, [calls[i] for i in bad], block, 10, retry=False)
        for i, r in zip(bad, again):
            out[i] = r
    return out


def uint(ok_ret):
    ok, ret = ok_ret
    return int(ret[:66], 16) if ok and ret not in ("0x", "") else None


def symbol_prices(grid):
    """ETH, BTC, BNB in USD on each grid date, from mainnet Chainlink."""
    clock = blocktime.BlockClock("ethereum")
    out = {}
    for ts in grid:
        res = multicall.aggregate3(
            "ethereum", [(a, evmabi.selector("latestAnswer()"))
                         for a in FEEDS.values()],
            block=clock.block_at_verified(ts))
        out[ts] = {k: (int(ret, 16) / 1e8 if ok and ret != "0x" else None)
                   for k, (ok, ret) in zip(FEEDS, res)}
        out[ts].update(USD=1.0, EUR=EUR_USD)
    return out


def llama_prices(chain_key, wanted, batch=10):
    """{(asset, ts): (price, confidence)} from DefiLlama for the {asset."""
    out = {}
    items = sorted(wanted.items())
    for i in range(0, len(items), batch):
        coins = {f"{chain_key}:{a}": sorted(tss)
                 for a, tss in items[i:i + batch]}
        q = urllib.parse.quote(json.dumps(coins))
        for key, v in _get(f"{LLAMA}?coins={q}&searchWidth=12h").items():
            a = key.split(":", 1)[1].lower()
            asked = coins.get(f"{chain_key}:{a}", [])
            for p in v.get("prices", []):
                ts = min(asked, key=lambda t: abs(t - p["timestamp"]),
                         default=None)
                if ts is not None and abs(ts - p["timestamp"]) <= 12 * 3600:
                    out[(a, ts)] = (p["price"], p.get("confidence"))
    return out


def _get(url):
    for attempt in range(5):
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            return resp.json().get("coins", {})
        except (requests.RequestException, ValueError):
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"DefiLlama failed: {url[:120]}")


def llama_decimals(chain_key, assets, batch=40):
    """{asset: decimals} that DefiLlama's prices refer to."""
    out = {}
    for i in range(0, len(assets), batch):
        q = ",".join(f"{chain_key}:{a}" for a in assets[i:i + batch])
        for key, v in _get(f"{LLAMA_NOW}{q}").items():
            if v.get("decimals") is not None:
                out[key.split(":", 1)[1].lower()] = int(v["decimals"])
    return out


def holders(target, vaults, counted):
    """[(holder_address, holder_vault, held_vault)]."""
    pairs = []
    v2 = [v["vault"] for v in vaults if v["kind"] == "morpho_v2"]
    queue = [v["vault"] for v in vaults
             if v["kind"] in ("euler_earn", "silo_vault")]
    if v2:
        n = multicall.try_view(target, v2, "adaptersLength()", ["uint256"])
        calls = [(v, evmabi.encode_call("adapters(uint256)", ["uint256"], [i]))
                 for v, k in zip(v2, n) if k for i in range(min(k[0], 50))]
        adapters = [(v, evmabi.addr(ret)) for (v, _), (ok, ret)
                    in zip(calls, read(target, calls, "latest")) if ok
                    and len(ret) >= 66]
        held = multicall.try_view(target, [a for _, a in adapters],
                                  "morphoVaultV1()", ["address"])
        pairs += [(a, v, h[0].lower()) for (v, a), h in zip(adapters, held)
                  if h and h[0].lower() in counted]
    if queue:
        n = multicall.try_view(target, queue, "withdrawQueueLength()",
                               ["uint256"])
        calls = [(v, evmabi.encode_call("withdrawQueue(uint256)",
                                        ["uint256"], [i]))
                 for v, k in zip(queue, n) if k for i in range(min(k[0], 50))]
        for (v, _), (ok, ret) in zip(calls, read(target, calls, "latest")):
            if ok and len(ret) >= 66 and evmabi.addr(ret) in counted:
                pairs.append((v, v, evmabi.addr(ret)))
    return pairs


def run_chain(cid, vaults, grid, sym_px, chains):
    ccfg = chains[cid]
    target = logs.rpc_target(ccfg)
    clock = blocktime.BlockClock(target)
    by_addr = {v["vault"]: v for v in vaults}
    assets = sorted({v["asset"] for v in vaults if v.get("asset")})
    sym = multicall.try_view(target, assets, "symbol()", ["string"])
    dec = multicall.try_view(target, assets, "decimals()", ["uint8"])
    meta = {a: {"symbol": s[0] if s else None,
                "decimals": d[0] if d else None}
            for a, s, d in zip(assets, sym, dec)}
    unnamed = [v["vault"] for v in vaults if not v.get("name")]
    for a, n in zip(unnamed, multicall.try_view(target, unnamed, "name()",
                                                ["string"])):
        by_addr[a]["name"] = n[0] if n else None

    first_ts = min((v.get("created_ts") or 0) for v in vaults)
    blocks = {ts: clock.block_at_verified(ts) for ts in grid
              if ts >= clock.anchors[0][0] and ts + 31 * 86400 >= first_ts}
    pairs = holders(target, vaults, set(by_addr))

    def sample(ts_block):
        """totalAssets of live vaults, totalSupply of their assets, and assets held per holder pair."""
        ts, block = ts_block
        live = [v["vault"] for v in vaults if v["created_block"] <= block]
        total = read(target, [(a, SEL_TOTAL) for a in live], block)
        shares = read(target, [(a, SEL_SUPPLY) for a in live], block)
        toks = sorted({by_addr[a]["asset"] for a in live
                       if by_addr[a].get("asset")})
        supply = dict(zip(toks, map(uint, read(
            target, [(t, SEL_SUPPLY) for t in toks], block))))
        bal = read(target, [(h, evmabi.encode_call(SEL_BALANCE, ["address"],
                                                   [a]))
                            for a, _, h in pairs], block)
        nz = [(p, uint(b)) for p, b in zip(pairs, bal) if uint(b)]
        conv = read(target, [(h, evmabi.encode_call(SEL_CONVERT, ["uint256"],
                                                    [s]))
                             for (_, _, h), s in nz], block)
        return ts, supply, zip(live, map(uint, total), map(uint, shares)), [
            (hv, h, uint(c)) for ((_, hv, h), _), c in zip(nz, conv)]

    raw = defaultdict(dict)
    shares = defaultdict(dict)
    supply = {}
    held_raw = defaultdict(dict)
    with ThreadPoolExecutor(max_workers=4) as ex:
        for ts, sup, total, held in ex.map(sample, blocks.items()):
            supply.update({(t, ts): x for t, x in sup.items()})
            for a, x, sh in total:
                if x:
                    raw[a][ts], shares[a][ts] = x, sh
            for hv, h, x in held:
                if x:
                    held_raw[(hv, h)][ts] = held_raw[(hv, h)].get(ts, 0) + x

    jumped = defaultdict(dict)
    over = defaultdict(dict)
    for a, series in raw.items():
        ref = None
        for ts in sorted(series):
            sh = shares[a].get(ts)
            if sh and ref and series[ts] > sh * ref * (1 + MAX_JUMP):
                jumped[a][ts] = series[ts]
                series[ts] = int(sh * ref)
            elif sh:
                ref = series[ts] / sh
            cap = supply.get((by_addr[a].get("asset"), ts))
            if cap and series[ts] > OVER_SUPPLY * cap:
                over[a][ts] = series.pop(ts)

    wanted = defaultdict(set)
    for a, series in raw.items():
        for ts, x in series.items():
            if x and by_addr[a].get("asset"):
                wanted[by_addr[a]["asset"]].add(ts)
    llama, ldec = {}, {}
    if ccfg.get("defillama"):
        llama = llama_prices(ccfg["defillama"], wanted)
        ldec = llama_decimals(ccfg["defillama"], sorted(wanted))
    for a, m in meta.items():
        m["price_decimals"] = ldec.get(a, m["decimals"])

    def usd(asset, ts, amount):
        m = meta.get(asset) or {}
        p = llama.get((asset, ts))
        if amount is None or m.get("price_decimals") is None or p is None:
            return None
        return amount / 10 ** m["price_decimals"] * p[0]

    rows = {}
    for v in vaults:
        a, asset = v["vault"], v.get("asset")
        m = meta.get(asset, {"symbol": None, "decimals": None,
                             "price_decimals": None})
        cls = asset_class(m["symbol"])
        priced, by_symbol, conf = {}, {}, {}
        for ts, x in raw[a].items():
            if m["price_decimals"] is None:
                continue
            u = usd(asset, ts, x)
            if u is not None:
                priced[ts] = u
                conf[ts] = llama[(asset, ts)][1]
            if cls and sym_px[ts].get(cls):
                by_symbol[ts] = (x / 10 ** m["price_decimals"]
                                 * sym_px[ts][cls])
        rows[a] = {
            "protocol": v["protocol"], "chain_id": int(cid),
            "kind": v["kind"], "vault": a, "name": v.get("name"),
            "official_factory": v.get("official_factory"),
            "found_by": v.get("found_by"), "asset": asset,
            "asset_symbol": m["symbol"], "asset_decimals": m["decimals"],
            "price_decimals": m["price_decimals"],
            "asset_class": cls,
            "asset_is_vault": asset if asset in by_addr else None,
            "total_assets": {str(k): str(x) for k, x in raw[a].items()},
            "over_supply": {str(k): str(x) for k, x in over[a].items()},
            "share_price_jump": {str(k): str(x)
                                 for k, x in jumped[a].items()},
            "usd": {str(k): round(x, 2) for k, x in priced.items()},
            "usd_by_symbol": {str(k): round(x, 2)
                              for k, x in by_symbol.items()},
            "price_confidence_min": min(conf.values()) if conf else None,
            "held_by_vaults_usd": {},
        }
    for (hv, h), series in held_raw.items():
        dst = rows[h]["held_by_vaults_usd"].setdefault(
            by_addr[hv]["protocol"], {})
        for ts, x in series.items():
            if ts not in raw[h]:
                continue
            if ts in jumped[h]:
                x = x * raw[h][ts] // jumped[h][ts]
            u = usd(by_addr[h]["asset"], ts, x)
            if u:
                dst[str(ts)] = round(dst.get(str(ts), 0) + u, 2)
    for r in rows.values():
        if r["asset_is_vault"]:
            dst = rows[r["asset_is_vault"]]["held_by_vaults_usd"].setdefault(
                r["protocol"], {})
            for ts, u in r["usd"].items():
                dst[ts] = round(dst.get(ts, 0) + u, 2)
    return list(rows.values())


def aggregate(rows, grid):
    """Per (protocol, chain)."""
    agg = {}
    for r in rows:
        key = f"{r['protocol']}:{r['chain_id']}"
        a = agg.setdefault(key, {"protocol": r["protocol"],
                                 "chain_id": r["chain_id"], "vaults": 0,
                                 "funded_vaults": 0, "unpriced_vaults": 0,
                                 "over_supply_vaults": 0,
                                 "share_price_jump_vaults": 0,
                                 "kinds": defaultdict(int),
                                 "gross": defaultdict(float),
                                 "nested": defaultdict(float),
                                 "cross_nested": defaultdict(float),
                                 "symbol_only": defaultdict(float)})
        a["vaults"] += 1
        a["kinds"][r["kind"]] += 1
        a["funded_vaults"] += max(r["usd"].values(), default=0) >= FUNDED_USD
        a["over_supply_vaults"] += bool(r["over_supply"])
        a["share_price_jump_vaults"] += bool(r["share_price_jump"])
        a["unpriced_vaults"] += bool(
            [t for t, x in r["total_assets"].items()
             if int(x) and t not in r["usd"]])
        for ts, u in r["usd"].items():
            a["gross"][ts] += u
        for ts, u in r["usd_by_symbol"].items():
            if ts not in r["usd"]:
                a["symbol_only"][ts] += u
        for proto, series in r["held_by_vaults_usd"].items():
            dst = a["nested"] if proto == r["protocol"] else a["cross_nested"]
            for ts, u in series.items():
                dst[ts] += u
    out = {}
    for key, a in agg.items():
        net = {str(t): a["gross"].get(str(t), 0) - a["nested"].get(str(t), 0)
               for t in grid}
        peak = max(net, key=net.get)
        latest = str(grid[-1])
        positive = net[peak] > 0
        out[key] = {
            "protocol": a["protocol"], "chain_id": a["chain_id"],
            "vaults": a["vaults"], "funded_vaults": a["funded_vaults"],
            "unpriced_vaults": a["unpriced_vaults"],
            "over_supply_vaults": a["over_supply_vaults"],
            "share_price_jump_vaults": a["share_price_jump_vaults"],
            "kinds": dict(a["kinds"]),
            "peak_usd": round(net[peak]),
            "peak_month": month(peak) if positive else None,
            "latest_usd": round(net[latest]),
            "gross_peak_usd": round(max(a["gross"].values(), default=0)),
            "nested_at_peak_usd": round(a["nested"].get(peak, 0)),
            "cross_protocol_nested_at_peak_usd":
                round(a["cross_nested"].get(peak, 0)),
            "symbol_only_at_peak_usd": round(a["symbol_only"].get(peak, 0)),
            "by_month": {month(ts): {
                "gross": round(a["gross"].get(str(ts), 0)),
                "nested": round(a["nested"].get(str(ts), 0)),
                "net": round(net[str(ts)]),
                "symbol_only": round(a["symbol_only"].get(str(ts), 0))}
                for ts in grid},
        }
    return out


def not_measured(chains, measured):
    """Deployments with no registry data."""
    reg = json.loads((HERE.parent / "config" / "deployments.json")
                     .read_text())["protocols"]
    out = []
    for proto, p in reg.items():
        for cid in p["chains"]:
            c = chains.get(cid, {})
            if f"{proto}:{cid}" not in measured and not c.get("hypersync"):
                out.append({"protocol": proto, "chain_id": int(cid),
                            "chain": c.get("name"),
                            "reason": "no HyperSync index: not enumerated"})
    return out


def checkpoint(relpath, grid, fresh):
    """Rows of a finished part, or None when it must be (re)computed."""
    path = io_utils.DATA / relpath
    side = path.with_suffix(path.suffix + ".meta.json")
    if fresh or not path.exists() or not side.exists():
        return None
    if json.loads(side.read_text()).get("grid") != grid:
        return None
    return io_utils.read_jsonl(relpath)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only")
    ap.add_argument("--fresh", action="store_true",
                    help="ignore checkpoints in " + PARTS)
    args = ap.parse_args()
    chains = logs.load_chains()
    by_chain = load_vaults()
    if args.only:
        by_chain = {args.only: by_chain[args.only]}
    grid = month_grid()
    cached = checkpoint(f"{PARTS}/symbol_prices.jsonl", grid, args.fresh)
    if cached:
        sym_px = {r["ts"]: r["px"] for r in cached}
    else:
        sym_px = symbol_prices(grid)
        io_utils.write_jsonl(f"{PARTS}/symbol_prices.jsonl",
                             [{"ts": k, "px": v} for k, v in sym_px.items()],
                             generator="code/data/vault_aum.py",
                             meta={"grid": grid})
    print(f"{len(grid)} monthly dates, {sum(map(len, by_chain.values()))} "
          f"vaults on {len(by_chain)} chains", flush=True)

    failed = []

    def job(item):
        cid, vaults = item
        t0 = time.time()
        part = f"{PARTS}/{cid}.jsonl"
        done = checkpoint(part, grid, args.fresh)
        if done is not None:
            print(f"chain {cid:>7} from checkpoint", flush=True)
            return done
        try:
            rows = run_chain(cid, vaults, grid, sym_px, chains)
            io_utils.write_jsonl(part, rows,
                                 generator="code/data/vault_aum.py",
                                 meta={"grid": grid})
            print(f"chain {cid:>7} {chains[cid]['name']:<12} "
                  f"{len(vaults):>5} vaults {time.time() - t0:6.1f}s",
                  flush=True)
            return rows
        except Exception as exc:
            failed.append(cid)
            print(f"chain {cid} FAILED: {str(exc)[:300]}", flush=True)
            return []

    all_rows = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for rows in ex.map(job, sorted(by_chain.items(),
                                       key=lambda kv: -len(kv[1]))):
            all_rows.extend(rows)
    out = aggregate(all_rows, grid)
    print(f"RPC calls made: {rpc.calls_made()}", flush=True)

    for key, v in sorted(out.items(), key=lambda kv: -kv[1]["peak_usd"]):
        print(f"{key:<20} peak ${v['peak_usd'] / 1e6:9,.1f}M "
              f"({v['peak_month']}) latest ${v['latest_usd'] / 1e6:9,.1f}M "
              f"nested ${v['nested_at_peak_usd'] / 1e6:7,.1f}M "
              f"symbol-only ${v['symbol_only_at_peak_usd'] / 1e6:7,.1f}M "
              f"vaults {v['vaults']}", flush=True)
    if args.only or failed:
        print(f"not writing outputs (only={args.only}, failed={failed})")
        return
    meta = {"grid": grid, "eur_usd": EUR_USD, "price_source": LLAMA,
            "symbol_prices": {str(k): v for k, v in sym_px.items()}}
    io_utils.write_jsonl("registry/vault_aum.jsonl", all_rows,
                         generator="code/data/vault_aum.py", meta=meta)
    io_utils.write_json("registry/chain_aum.json", out,
                        generator="code/data/vault_aum.py",
                        meta={"grid": grid})
    pairs = sorted(({k: v[k] for k in (
        "protocol", "chain_id", "peak_usd", "peak_month", "latest_usd",
        "vaults", "funded_vaults", "gross_peak_usd", "nested_at_peak_usd",
        "symbol_only_at_peak_usd", "unpriced_vaults")}
        | {"chain": chains[str(v["chain_id"])]["name"],
           "selected": v["peak_usd"] >= THRESHOLD_USD}
        for v in out.values()), key=lambda r: -r["peak_usd"])
    io_utils.write_json(
        "registry/chain_selection.json",
        {"rule": "net curated AUM at a month start peaked at or above "
                 "threshold_usd (docs/experiment.md, E1)",
         "threshold_usd": THRESHOLD_USD, "pairs": pairs,
         "not_measured": not_measured(chains, out)},
        generator="code/data/vault_aum.py", meta={"grid": grid})


if __name__ == "__main__":
    main()
