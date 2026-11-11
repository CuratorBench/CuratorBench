# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""E1, Silo v2 market layer."""
import argparse
import csv
import io
import json
import os
import pathlib
import random
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common import evmabi, hypersync_client, io_utils, logs, multicall, rpc

GEN = "code/data/panel_silo.py"
YEAR = 365 * 86400
WAD = 10 ** 18

CONFIG_T = ("(uint256,uint256,address,address,address,address,address,"
            "address,address,address,uint256,uint256,uint256,uint256,"
            "uint256,address,bool)")
CONFIG_KEYS = ["dao_fee", "deployer_fee", "silo", "token",
               "protected_share_token", "collateral_share_token",
               "debt_share_token", "solvency_oracle", "max_ltv_oracle",
               "interest_rate_model", "max_ltv", "lt",
               "liquidation_target_ltv", "liquidation_fee", "flashloan_fee",
               "hook_receiver", "call_before_quote"]
LEGACY_T = ("(uint256,uint256,address,address,address,address,address,"
            "address,address,address,uint256,uint256,uint256,uint256,"
            "address,bool)")
LEGACY_KEYS = [k for k in CONFIG_KEYS if k != "liquidation_target_ltv"]

EV = {
    "Deposit": "Deposit(address,address,uint256,uint256)",
    "DepositProtected": "DepositProtected(address,address,uint256,uint256)",
    "Withdraw": "Withdraw(address,address,address,uint256,uint256)",
    "WithdrawProtected":
        "WithdrawProtected(address,address,address,uint256,uint256)",
    "Borrow": "Borrow(address,address,address,uint256,uint256)",
    "Repay": "Repay(address,address,uint256,uint256)",
    "AccruedInterest": "AccruedInterest(uint256)",
    "DefaultingLiquidation": "DefaultingLiquidation(uint256,uint256)",
}
T_LIQ = evmabi.topic0(
    "LiquidationCall(address,address,address,uint256,uint256,bool)")
TOPIC = {evmabi.topic0(s): n for n, s in EV.items()}
CSV_COLS = ["block", "ts", "log_index", "tx", "event", "amount",
            "collateral_assets", "protected_assets", "debt_assets",
            "utilization", "borrow_apr_realized"]
N_WINDOWS = 40


def out_dir(cid):
    return io_utils.DATA / f"panel/silo_v2/{cid}"


def rel(cid, name):
    return f"panel/silo_v2/{cid}/{name}"


def chain_cfg(cid):
    return logs.load_chains()[str(cid)]


def registry(cid):
    return io_utils.read_jsonl(f"registry/markets/silo_v2/{cid}.jsonl")


def build_configs(cid):
    path = out_dir(cid) / "configs.jsonl"
    if path.exists():
        return {r["silo"]: r for r in io_utils.read_jsonl(rel(cid, "configs.jsonl"))}
    target = logs.rpc_target(chain_cfg(cid))
    head = rpc.block_number(target)
    silos = registry(cid)
    calls = [(r["silo_config"], evmabi.encode_call(
        "getConfig(address)", ["address"], [r["silo"]])) for r in silos]
    calls += [(r["silo_config"], evmabi.selector("getSilos()")) for r in silos]
    res = multicall.aggregate3(target, calls, block=head)
    n = len(silos)
    tokens = sorted({r["token"] for r in silos})
    meta_calls = [(t, evmabi.selector(f)) for t in tokens
                  for f in ("symbol()", "decimals()")]
    tres = multicall.aggregate3(target, meta_calls, block=head)
    tok = {}
    for i, t in enumerate(tokens):
        (ok_s, rs), (ok_d, rd) = tres[2 * i], tres[2 * i + 1]
        sym = None
        if ok_s and len(rs) > 2:
            try:
                sym = evmabi.decode(["string"], rs)[0]
            except Exception:
                sym = bytes.fromhex(rs[2:66]).rstrip(b"\0").decode(
                    "utf-8", "replace")
        dec = int(rd, 16) if ok_d and len(rd) > 2 else None
        tok[t] = (sym, dec)
    rows = []
    for i, r in enumerate(silos):
        row = {"silo": r["silo"], "silo_config": r["silo_config"],
               "factory": r.get("factory"), "created_block": r["created_block"],
               "created_ts": r["created_ts"], "chain_id": int(cid)}
        ok, ret = res[i]
        keys, vals = CONFIG_KEYS, None
        try:
            vals = evmabi.decode([CONFIG_T], ret)[0] if ok else None
            row["config_layout"] = "verified17"
        except Exception:
            if ok and len(ret) == 2 + 64 * 16:
                keys = LEGACY_KEYS
                vals = evmabi.decode([LEGACY_T], ret)[0]
                row["config_layout"] = "legacy16_inferred"
        if vals is None:
            row["config_raw"] = ret
        else:
            for k, v in zip(keys, vals):
                row[k] = v.lower() if isinstance(v, str) else v
        ok2, ret2 = res[n + i]
        if ok2 and len(ret2) > 2:
            s0, s1 = (a.lower() for a in evmabi.decode(["address", "address"], ret2))
            row["silo0"], row["silo1"] = s0, s1
            row["paired_silo"] = s1 if r["silo"] == s0 else s0
        row["token"] = r["token"]
        row["symbol"], row["decimals"] = tok[r["token"]]
        rows.append(row)
    io_utils.write_jsonl(rel(cid, "configs.jsonl"), rows, GEN,
                         {"chain_id": int(cid), "read_at_block": head,
                          "source": "SiloConfig.getConfig(silo), getSilos(); "
                                    "token symbol()/decimals()"})
    return {r["silo"]: r for r in rows}


class Ckpt:
    """Replay state plus the byte length of every output file, written atomically after each block window."""

    def __init__(self, cid):
        self.path = out_dir(cid) / "_checkpoint.json"
        self.d = json.loads(self.path.read_text()) if self.path.exists() else None

    def save(self, d):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(d))
        os.replace(tmp, self.path)
        self.d = d


def _truncate_to(cid, sizes):
    """Cut every output file back to its checkpointed length; files the checkpoint does not know are emptied."""
    d = out_dir(cid)
    for p in list(d.glob("0x*.csv")) + [d / "liquidations.jsonl",
                                        d / "defaults.jsonl"]:
        if not p.exists():
            continue
        want = sizes.get(p.name, 0)
        if p.stat().st_size != want:
            with p.open("r+b") as fh:
                fh.truncate(want)


def estimate(ccfg, addrs, lo, head, k=5):
    """Event count from k evenly spaced slices of 1% of the range."""
    span = head - lo
    width = max(1, span // 100)
    sel = [(addrs, [list(TOPIC) + [T_LIQ]])]
    n = 0
    for i in range(k):
        a = lo + (span - width) * i // (k - 1)
        n += len(hypersync_client.scan_logs_multi(
            ccfg["hypersync"], sel, a, a + width))
    return int(n * span / (k * width))


def replay(cid, cfgs, workers):
    ccfg = chain_cfg(cid)
    if not ccfg.get("hypersync"):
        raise RuntimeError(f"chain {cid}: no HyperSync index")
    d = out_dir(cid)
    d.mkdir(parents=True, exist_ok=True)
    ck = Ckpt(cid)
    silos = sorted(cfgs)
    hooks = sorted({c["hook_receiver"] for c in cfgs.values()
                    if c.get("hook_receiver")})
    fee = {s: (cfgs[s].get("dao_fee", 0) or 0) + (cfgs[s].get("deployer_fee", 0) or 0)
           for s in silos}
    if ck.d is None:
        lo = min(c["created_block"] for c in cfgs.values())
        head = hypersync_client.get_height(ccfg["hypersync"])
        est = estimate(ccfg, silos + hooks, lo, head)
        print(f"[{cid}] {len(silos)} silos, {len(hooks)} hook receivers, "
              f"blocks {lo}..{head}; estimated events ~{est:,}; "
              f"estimated CSV size ~{est * 140 / 1e6:.0f} MB; "
              f"RPC calls for replay: 0", flush=True)
        st = {s: [0, 0, 0, None] for s in silos}
        ck.save({"head": head, "lo": lo, "next_block": lo, "state": st,
                 "sizes": {}, "events": {},
                 "n_liq": 0, "estimated_events": est})
    c = ck.d
    _truncate_to(cid, c["sizes"])
    head, st = c["head"], c["state"]
    ev_count = Counter(c["events"])
    edges = [c["lo"] + (head - c["lo"]) * k // N_WINDOWS
             for k in range(N_WINDOWS + 1)]
    silo_set, hook_set = set(silos), set(hooks)
    topics = [list(TOPIC) + [T_LIQ]]
    n_liq = c["n_liq"]
    keep = silo_set | hook_set

    def fetch(a, b):
        for attempt in range(6):
            got = []
            try:
                hypersync_client.scan_logs_stream(
                    ccfg["hypersync"], addresses=sorted(keep), topics=topics,
                    from_block=a, to_block=b,
                    on_page=lambda pg: got.extend(
                        lg for lg in pg if lg["address"].lower() in keep))
                return got
            except Exception as exc:
                msg = str(exc).lower()
                if attempt == 5 or not ("429" in msg or "too many" in msg
                                        or "rate limit" in msg):
                    raise
                pause = 15 * 2 ** attempt
                print(f"[{cid}] HyperSync throttled on {a}..{b}; "
                      f"retry in {pause}s", flush=True)
                time.sleep(pause)

    todo = [(max(a, c["next_block"]), b) for a, b in zip(edges, edges[1:])
            if b > c["next_block"]]
    pool = ThreadPoolExecutor(max_workers=workers)
    futs = [pool.submit(fetch, a, b) for a, b in todo]
    for (a, b), fut in zip(todo, futs):
        t0 = time.time()
        rows = defaultdict(list)
        liqs, defaults = [], []

        def on_page(page):
            nonlocal n_liq
            for lg in page:
                addr = lg["address"].lower()
                t0_ = lg["topics"][0]
                if addr in hook_set:
                    if t0_ == T_LIQ:
                        rep, wcol, recv = evmabi.decode(
                            ["uint256", "uint256", "bool"], lg["data"])
                        liqs.append({
                            "block": lg["block_number"], "ts": lg["timestamp"],
                            "log_index": lg["log_index"], "tx": lg["tx_hash"],
                            "hook": addr,
                            "liquidator": evmabi.addr(lg["topics"][1]),
                            "debt_silo": evmabi.addr(lg["topics"][2]),
                            "borrower": evmabi.addr(lg["topics"][3]),
                            "repay_debt_assets": str(rep),
                            "withdraw_collateral": str(wcol),
                            "receive_stoken": recv})
                        n_liq += 1
                    if addr not in silo_set:
                        continue
                if addr not in silo_set or t0_ not in TOPIC:
                    continue
                name = TOPIC[t0_]
                s = st[addr]
                apr = ""
                if name == "AccruedInterest":
                    x = int(lg["data"], 16)
                    if s[2] > 0 and s[3] and lg["timestamp"] > s[3]:
                        apr = f"{x / s[2] / (lg['timestamp'] - s[3]) * YEAR:.6g}"
                    s[2] += x
                    s[0] += x - x * fee[addr] // WAD
                    amt = x
                elif name == "DefaultingLiquidation":
                    cancelled, amt = evmabi.decode(["uint256", "uint256"],
                                                   lg["data"])
                    s[0] -= amt
                    defaults.append({
                        "block": lg["block_number"], "ts": lg["timestamp"],
                        "log_index": lg["log_index"], "tx": lg["tx_hash"],
                        "silo": addr, "canceled_debt": str(cancelled),
                        "deducted_from_collateral": str(amt)})
                else:
                    amt = int(lg["data"][2:66], 16)
                    if name == "Deposit":
                        s[0] += amt
                    elif name == "Withdraw":
                        s[0] -= amt
                    elif name == "DepositProtected":
                        s[1] += amt
                    elif name == "WithdrawProtected":
                        s[1] -= amt
                    elif name == "Borrow":
                        s[2] += amt
                    elif name == "Repay":
                        s[2] -= amt
                s[3] = lg["timestamp"]
                ev_count[name] += 1
                util = f"{s[2] / s[0]:.8g}" if s[0] > 0 else ""
                rows[addr].append([lg["block_number"], lg["timestamp"],
                                   lg["log_index"], lg["tx_hash"], name, amt,
                                   s[0], s[1], s[2], util, apr])

        on_page(fut.result())
        sizes = dict(c["sizes"])
        for addr, rr in rows.items():
            p = d / f"{addr}.csv"
            buf = io.StringIO()
            w = csv.writer(buf)
            if sizes.get(p.name, 0) == 0:
                w.writerow(CSV_COLS)
            w.writerows(rr)
            with p.open("a") as fh:
                fh.write(buf.getvalue())
            sizes[p.name] = p.stat().st_size
        for name, recs in (("liquidations.jsonl", liqs),
                           ("defaults.jsonl", defaults)):
            if recs:
                p = d / name
                with p.open("a") as fh:
                    for r in recs:
                        fh.write(json.dumps(r, sort_keys=True) + "\n")
                sizes[p.name] = p.stat().st_size
        c = {**c, "next_block": b, "state": st, "sizes": sizes,
             "events": dict(ev_count),
             "n_liq": n_liq}
        ck.save(c)
        print(f"[{cid}] blocks {a}..{b} ({edges.index(b)}/{N_WINDOWS}) "
              f"{sum(len(v) for v in rows.values())} silo events, "
              f"{len(liqs)} liquidations, {time.time() - t0:.0f}s", flush=True)
    pool.shutdown()
    return c


def finish_replay(cid, c):
    """Sidecars for the replay outputs, once the checkpoint reaches head."""
    meta = {"chain_id": int(cid), "scanned_from_block": c["lo"],
            "scanned_to_block_exclusive": c["head"], "source": "hypersync",
            "method": "exact event replay of Silo storage totals"}
    d = out_dir(cid)
    for p in d.glob("0x*.csv"):
        io_utils._meta_sidecar(p, GEN, meta)
    lp = d / "liquidations.jsonl"
    lp.touch()
    io_utils._meta_sidecar(lp, GEN, {**meta, "rows": c["n_liq"],
                                     "event": "LiquidationCall from hook receivers"})
    dp = d / "defaults.jsonl"
    dp.touch()
    io_utils._meta_sidecar(dp, GEN, {
        **meta, "rows": c["events"].get("DefaultingLiquidation", 0),
        "event": "DefaultingLiquidation from silos (SiloHookV2 bad-debt write-off)"})
    io_utils.write_json(rel(cid, "event_counts.json"),
                        {"silo_events": c["events"],
                         "liquidation_calls": c["n_liq"]}, GEN, meta)


def _totals_calls(silo):
    return [(silo, evmabi.encode_call("getTotalAssetsStorage(uint8)",
                                      ["uint8"], [k])) for k in (1, 0, 2)]


def _read_totals(target, silos, block):
    calls = [c for s in silos for c in _totals_calls(s)]
    res = multicall.aggregate3(target, calls, block=block)
    out = {}
    for i, s in enumerate(silos):
        part = res[3 * i:3 * i + 3]
        out[s] = [int(r, 16) if ok and len(r) > 2 else None for ok, r in part]
    return out


def _cmp(mine, chain):
    if None in chain:
        return "unreadable", None
    diffs = [abs(m - x) for m, x in zip(mine, chain)]
    rel_ = max((df / x if x else (0 if df == 0 else 1.0))
               for df, x in zip(diffs, chain))
    return ("exact" if max(diffs) == 0 else "diff"), {"abs": diffs, "rel": rel_}


def load_series(cid, silo):
    p = out_dir(cid) / f"{silo}.csv"
    if not p.exists():
        return []
    with p.open() as fh:
        return [(int(r["block"]), int(r["log_index"]), int(r["collateral_assets"]),
                 int(r["protected_assets"]), int(r["debt_assets"]),
                 int(r["ts"]), r["utilization"])
                for r in csv.DictReader(fh)]


def state_at_block(series, block):
    """State after the last event at or before `block`."""
    last = None
    for row in series:
        if row[0] > block:
            break
        last = row
    return [last[2], last[3], last[4]] if last else [0, 0, 0]


def validate(cid, cfgs, c, n_sample=200, seed=7):
    vpath = out_dir(cid) / "validation.json"
    v = json.loads(vpath.read_text()) if vpath.exists() else {}
    target = logs.rpc_target(chain_cfg(cid))
    calls0 = rpc.calls_made()
    head_block = c["head"] - 1
    silos = sorted(cfgs)
    if "final" not in v:
        chain = _read_totals(target, silos, head_block)
        per, counts, worst = {}, Counter(), []
        for s in silos:
            mine = c["state"][s][:3]
            status, info = _cmp(mine, chain[s])
            counts[status] += 1
            per[s] = {"status": status, "mine": [str(x) for x in mine],
                      "chain": [str(x) if x is not None else None for x in chain[s]],
                      **({"rel": info["rel"], "abs": [str(x) for x in info["abs"]]}
                         if info else {})}
            if status == "diff":
                worst.append((info["rel"], s))
        worst.sort(reverse=True)
        v["final"] = {"block": head_block, "counts": dict(counts),
                      "max_rel_diff": worst[0][0] if worst else 0.0,
                      "worst": [{"silo": s, "rel": r} for r, s in worst[:15]],
                      "per_silo": per}
        _save_validation(cid, v, c)
        print(f"[{cid}] final-state check at {head_block}: {dict(counts)}, "
              f"max rel diff {v['final']['max_rel_diff']:.3g}", flush=True)
    if "sample" not in v:
        rng = random.Random(seed)
        series = {s: load_series(cid, s) for s in silos}
        active = [s for s in silos if series[s]]
        points = []
        for k in range(n_sample):
            s = rng.choice(active)
            ser = series[s]
            if k % 2 == 0:
                b = rng.choice(ser)[0]
            else:
                b = rng.randint(ser[0][0], head_block)
            points.append((s, b))
        res = []
        counts = Counter()
        for s, b in points:
            chain = [None, None, None]
            for attempt in range(3):
                try:
                    chain = _read_totals(target, [s], b)[s]
                    break
                except RuntimeError:
                    time.sleep(2 * (attempt + 1))
            mine = state_at_block(series[s], b)
            status, info = _cmp(mine, chain)
            counts[status] += 1
            res.append({"silo": s, "block": b, "status": status,
                        "mine": [str(x) for x in mine],
                        "chain": [str(x) if x is not None else None for x in chain],
                        **({"rel": info["rel"]} if info else {})})
        rels = [r["rel"] for r in res if r.get("rel") is not None]
        v["sample"] = {"n": len(res), "seed": seed, "counts": dict(counts),
                       "max_rel_diff": max(rels) if rels else None,
                       "n_rel_below_1e-9": sum(1 for x in rels if x < 1e-9),
                       "points": res}
        _save_validation(cid, v, c)
        print(f"[{cid}] sampled archive check: {dict(counts)}, max rel diff "
              f"{v['sample']['max_rel_diff']}", flush=True)
    if "crosscheck" not in v:
        v["crosscheck"] = crosscheck_check(cid, silos)
        _save_validation(cid, v, c)
        print(f"[{cid}] crosscheck cross-check: "
              f"{ {k: x for k, x in v['crosscheck'].items() if k != 'rows'} }",
              flush=True)
    v["rpc_calls_this_run"] = v.get("rpc_calls_this_run", 0) + rpc.calls_made() - calls0
    _save_validation(cid, v, c)
    return v


CROSSCHECK = (pathlib.Path(os.environ["CB_INPUTS"]).resolve() if os.environ.get("CB_INPUTS")
           else HERE.parents[1] / "data" / "inputs") / "utilization_check" / "silo_v2"


def crosscheck_check(cid, silos):
    """The independent scan sampled utilizationData() daily, then at 4h around high-utilization days."""
    p = CROSSCHECK / f"{cid}.jsonl"
    if not p.exists() or p.stat().st_size == 0:
        return {"available": False}
    rows = [json.loads(x) for x in p.read_text().splitlines() if x.strip()]
    written = json.loads(p.with_suffix(".jsonl.meta.json").read_text())["written_utc"]
    cutoff = int(datetime.fromisoformat(written).timestamp())
    out = []
    n_runs = n_runs_ok = n_peak_ok = n_cmp = 0
    for r in rows:
        silo = r["market_uid"].split(":")[-1].lower()
        if silo not in silos:
            continue
        ser = load_series(cid, silo)
        if not ser:
            continue
        utils = [(x[5], float(x[6])) for x in ser if x[6] not in ("", None)]
        n_cmp += 1
        peak_mine = max((u for t, u in utils if t <= cutoff), default=0.0)
        peak_ok = peak_mine + 1e-6 >= min(r["peak_util"], 1.0)
        n_peak_ok += peak_ok
        run_res = []
        for run in r.get("util_runs", []):
            n_runs += 1
            before = [u for t, u in utils if t <= run["from_ts"]]
            inside = [u for t, u in utils if run["from_ts"] < t <= run["to_ts"]]
            seen = ([before[-1]] if before else []) + inside
            ok = bool(seen) and max(seen) >= 0.95 - 1e-6
            n_runs_ok += ok
            run_res.append({"from_ts": run["from_ts"], "to_ts": run["to_ts"],
                            "replay_max_util": max(seen) if seen else None,
                            "ok": ok})
        out.append({"silo": silo, "crosscheck_peak_util": r["peak_util"],
                    "replay_peak_util": peak_mine, "peak_ok": peak_ok,
                    "runs": run_res})
    return {"available": True, "crosscheck_written_utc": written,
            "silos_compared": n_cmp,
            "peak_consistent": n_peak_ok, "runs": n_runs,
            "runs_recovered": n_runs_ok, "rows": out}


def _save_validation(cid, v, c):
    io_utils.write_json(rel(cid, "validation.json"), v, GEN,
                        {"chain_id": int(cid),
                         "scanned_to_block_exclusive": c["head"]})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("chain_id")
    ap.add_argument("--stage", default="all",
                    choices=["configs", "replay", "validate", "all"])
    ap.add_argument("--sample", type=int, default=200)
    ap.add_argument("--workers", type=int, default=2,
                    help="block windows fetched concurrently")
    args = ap.parse_args()
    cid = args.chain_id
    t0, calls0 = time.time(), rpc.calls_made()
    cfgs = build_configs(cid)
    print(f"[{cid}] configs: {len(cfgs)} silos; RPC calls so far "
          f"{rpc.calls_made() - calls0}", flush=True)
    if args.stage == "configs":
        return
    c = replay(cid, cfgs, args.workers)
    if c["next_block"] >= c["head"]:
        finish_replay(cid, c)
    if args.stage in ("validate", "all") and c["next_block"] >= c["head"]:
        print(f"[{cid}] validation: ~{1 + args.sample} RPC calls", flush=True)
        validate(cid, cfgs, c, n_sample=args.sample)
    print(f"[{cid}] done in {time.time() - t0:.0f}s, RPC calls "
          f"{rpc.calls_made() - calls0}", flush=True)


if __name__ == "__main__":
    main()
