# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""E1, Euler v2 market layer."""
import argparse
import json
import os
import pathlib
import random
import sys
import time
from collections import Counter, defaultdict

import eth_abi
import requests

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common import (env, evmabi, hypersync_client, io_utils, logs,
                    multicall, rpc)

GEN = "code/data/panel_euler.py"
T_PROXY = evmabi.topic0("ProxyCreated(address,bool,address,bytes)")
STATUS_COLS = ["totalShares", "totalBorrows", "accumulatedFees", "cash",
               "interestAccumulator", "interestRate"]
LOSS_EVENTS = ("Liquidate", "DebtSocialized")
CONFIRMATIONS = 100
INIT_DEFAULTS = {"interestFee": 1000, "hookedOps": (1 << 15) - 1}
ZERO = "0x" + "0" * 40


STATE_GAPS = ("metadata is not found", "header not found", "missing trie node")


def retry_state_gap(fn, *args, **kwargs):
    """Call fn, retrying up to five times while the provider reports a state gap."""
    for attempt in range(6):
        try:
            return fn(*args, **kwargs)
        except RuntimeError as exc:
            if attempt == 5 or not any(s in str(exc) for s in STATE_GAPS):
                raise
            time.sleep(1 + attempt)


def _etherscan_abi(cid, address):
    r = requests.get("https://api.etherscan.io/v2/api", timeout=60, params={
        "chainid": cid, "module": "contract", "action": "getabi",
        "address": address, "apikey": env.require("ETHERSCAN_API_KEY")})
    body = r.json()
    if body.get("status") != "1":
        raise RuntimeError(f"etherscan getabi {address} on {cid}: "
                           f"{body.get('result')}")
    return json.loads(body["result"])


def _sig(ev):
    return f"{ev['name']}({','.join(i['type'] for i in ev['inputs'])})"


def load_abi(cid, ccfg, entry, out_dir):
    """Event ABI of the EVault implementation(s), cached in out_dir."""
    path = out_dir / "evault_abi.json"
    if path.exists():
        return json.loads(path.read_text())["events"]
    raw, _ = logs.scan(ccfg, addresses=[entry["evault_factory"]],
                       topics=[[T_PROXY]])
    impls = {evmabi.decode(["bool", "address", "bytes"], lg["data"])[1].lower()
             for lg in raw}
    cur = rpc.eth_call(logs.rpc_target(ccfg), entry["evault_factory"],
                       evmabi.selector("implementation()"))
    impls.add(evmabi.addr(cur))
    events = None
    for impl in sorted(impls):
        evs = sorted((e for e in _etherscan_abi(cid, impl)
                      if e["type"] == "event"), key=_sig)
        evs = [{"name": e["name"], "inputs": [
            {"name": i["name"], "type": i["type"], "indexed": i["indexed"]}
            for i in e["inputs"]]} for e in evs]
        if events is not None and evs != events:
            raise RuntimeError(f"implementation {impl} has a different event "
                               "ABI; decode per implementation")
        events = evs
        time.sleep(0.3)
    io_utils.write_json(str(path.relative_to(io_utils.DATA)),
                        {"events": events}, GEN,
                        {"chain_id": int(cid), "implementations": sorted(impls),
                         "source": "Etherscan v2 getabi (verified source)"})
    return events


def decoders(events):
    """topic0 -> (name, inputs) for the events this panel keeps."""
    keep = {"VaultStatus", "EVaultCreated", *LOSS_EVENTS}
    out = {}
    for e in events:
        if e["name"] in keep or e["name"].startswith("Gov"):
            out[evmabi.topic0(_sig(e))] = (e["name"], e["inputs"])
    names = {n for n, _ in out.values()}
    missing = keep - names
    if missing:
        raise RuntimeError(f"ABI lacks {missing}")
    status = next(i for n, i in out.values() if n == "VaultStatus")
    if [i["name"] for i in status] != STATUS_COLS + ["timestamp"]:
        raise RuntimeError(f"unexpected VaultStatus layout {status}")
    return out


def decode(lg, inputs):
    topics = lg["topics"][1:]
    data_types = [i["type"] for i in inputs if not i["indexed"]]
    data = evmabi.decode(data_types, lg["data"]) if data_types else ()
    out, ti, di = {}, 0, 0
    for i in inputs:
        if i["indexed"]:
            v = topics[ti]
            ti += 1
            v = evmabi.addr(v) if i["type"] == "address" else int(v, 16)
        else:
            v = data[di]
            di += 1
            if i["type"] == "address":
                v = v.lower()
        out[i["name"]] = v
    return out


class Writer:
    """Appends one page at a time and checkpoints after each page."""

    def __init__(self, out_dir, vaults, dec):
        self.dir, self.vaults, self.dec = out_dir, vaults, dec
        self.ck_path = out_dir / "checkpoint.json"
        self.n = Counter()
        self.t0 = self.last_print = time.time()

    def csv_path(self, v):
        return self.dir / f"{v}.csv"

    def on_page(self, page):
        status, side = defaultdict(list), defaultdict(list)
        hi = page[-1]["block_number"]
        for lg in page:
            hi = max(hi, lg["block_number"])
            v = lg["address"].lower()
            name, inputs = self.dec[lg["topics"][0]]
            self.n[f"{name}:all"] += 1
            if v not in self.vaults:
                continue
            self.n[name] += 1
            ts = lg["timestamp"]
            if ts is None:
                raise RuntimeError(f"no timestamp for block {lg['block_number']}")
            f = decode(lg, inputs)
            if name == "VaultStatus":
                if f["timestamp"] != ts:
                    raise RuntimeError(f"VaultStatus timestamp {f['timestamp']}"
                                       f" != block timestamp {ts}")
                status[v].append(",".join(map(str, [
                    lg["block_number"], lg["log_index"], ts,
                    *(f[c] for c in STATUS_COLS)])) + "\n")
                continue
            row = {"vault": v, "event": name, "block": lg["block_number"],
                   "log_index": lg["log_index"], "timestamp": ts,
                   "tx_hash": lg["tx_hash"], **f}
            kind = "losses" if name in LOSS_EVENTS else "governance"
            side[kind].append(json.dumps(row, sort_keys=True) + "\n")
        for v, lines in status.items():
            with self.csv_path(v).open("a") as fh:
                fh.writelines(lines)
        for kind, lines in side.items():
            with (self.dir / f"{kind}.jsonl").open("a") as fh:
                fh.writelines(lines)
        ck = json.loads(self.ck_path.read_text())
        ck["next_block"] = hi
        _atomic_json(self.ck_path, ck)
        if time.time() - self.last_print > 60:
            self.last_print = time.time()
            print(f"  block {hi:,}  status {self.n['VaultStatus']:,}  "
                  f"gov {sum(v for k, v in self.n.items() if k.startswith('Gov')):,}  "
                  f"{time.time() - self.t0:.0f}s", flush=True)


def _atomic_json(path, obj):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def _block_of(line, is_csv):
    if is_csv:
        return int(line.split(",", 1)[0])
    return json.loads(line)["block"]


def truncate(path, cut, is_csv):
    """Drop rows with block >= cut and any partial last line."""
    if not path.exists() or path.stat().st_size == 0:
        return 0
    with path.open("rb") as fh:
        fh.seek(max(0, path.stat().st_size - 4096))
        tail = fh.read().decode()
    lines = tail.split("\n")
    last = lines[-2] if tail.endswith("\n") and len(lines) > 1 else None
    header = is_csv and last is not None and last.startswith("block")
    if last is not None and (header or _block_of(last, is_csv) < cut):
        return 0
    kept, dropped = [], 0
    with path.open() as fh:
        for i, line in enumerate(fh):
            if not line.endswith("\n"):
                dropped += 1
            elif is_csv and i == 0:
                kept.append(line)
            elif _block_of(line, is_csv) < cut:
                kept.append(line)
            else:
                dropped += 1
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("".join(kept))
    os.replace(tmp, path)
    return dropped


def scan(ccfg, out_dir, vaults, dec, start, to_block):
    ck_path = out_dir / "checkpoint.json"
    vset_key = f"{len(vaults)}:{min(vaults)}:{max(vaults)}"
    if ck_path.exists():
        ck = json.loads(ck_path.read_text())
        if ck["vault_set"] != vset_key:
            raise RuntimeError("registry vault set changed since this scan "
                               "began; move the panel directory and rerun")
        if ck["complete"]:
            print(f"scan complete to {ck['to_block']:,}", flush=True)
            return ck
        cut = ck["next_block"]
        dropped = sum(truncate(out_dir / f"{v}.csv", cut, True)
                      for v in vaults)
        for kind in ("governance", "losses"):
            dropped += truncate(out_dir / f"{kind}.jsonl", cut, False)
        print(f"resuming at block {cut:,} (cut {dropped} rows past it)",
              flush=True)
    else:
        ck = {"from_block": start, "to_block": to_block, "next_block": start,
              "complete": False, "vault_set": vset_key}
        header = "block,log_index,timestamp," + ",".join(STATUS_COLS) + "\n"
        for v in vaults:
            (out_dir / f"{v}.csv").write_text(header)
        for kind in ("governance", "losses"):
            (out_dir / f"{kind}.jsonl").write_text("")
        _atomic_json(ck_path, ck)
    w = Writer(out_dir, vaults, dec)
    hypersync_client.scan_logs_stream(
        ccfg["hypersync"], topics=[sorted(dec)], from_block=ck["next_block"],
        to_block=ck["to_block"], on_page=w.on_page)
    ck = json.loads(ck_path.read_text())
    ck.update(complete=True, next_block=ck["to_block"])
    _atomic_json(ck_path, ck)
    print(f"scan done; this segment: {dict(w.n)}", flush=True)
    return ck


def estimate(ccfg, vaults, dec, start, to_block, windows=8, frac=0.01):
    """Extrapolate event counts and CSV size from evenly spaced windows."""
    span = to_block - start
    width = max(1, int(span * frac / windows))
    n, csv_bytes = Counter(), 0
    for k in range(windows):
        lo = start + k * span // windows
        page, _ = hypersync_client.scan_logs(
            ccfg["hypersync"], topics=[sorted(dec)], from_block=lo,
            to_block=min(lo + width, to_block))
        for lg in page:
            name, inputs = dec[lg["topics"][0]]
            n[f"{name}:all"] += 1
            if lg["address"].lower() in vaults:
                n[name] += 1
                if name == "VaultStatus":
                    f = decode(lg, inputs)
                    csv_bytes += len(",".join(map(str, [
                        lg["block_number"], lg["log_index"], lg["timestamp"],
                        *(f[c] for c in STATUS_COLS)]))) + 1
    scale = span / (width * windows)
    est = {k: round(v * scale) for k, v in sorted(n.items())}
    est["csv_mb"] = round(csv_bytes * scale / 1e6, 1)
    est["sample_fraction"] = round(1 / scale, 3)
    return est


def read_status(path):
    with path.open() as fh:
        next(fh)
        for line in fh:
            p = line.rstrip("\n").split(",")
            yield [int(x) for x in p]


def summarize(out_dir, vault_rows):
    """vaults.jsonl: one row per vault with series extent, final state, loss totals and the event-derived governance state at the scan head."""
    gov = defaultdict(list)
    with (out_dir / "governance.jsonl").open() as fh:
        for line in fh:
            r = json.loads(line)
            gov[r["vault"]].append(r)
    loss = defaultdict(Counter)
    with (out_dir / "losses.jsonl").open() as fh:
        for line in fh:
            r = json.loads(line)
            c = loss[r["vault"]]
            c[r["event"]] += 1
            if r["event"] == "Liquidate":
                c["repay_assets"] += r["repayAssets"]
            else:
                c["socialized_assets"] += r["assets"]
    rows = []
    for vr in vault_rows:
        v = vr["vault"]
        n, first, last, max_b, max_u = 0, None, None, 0, 0.0
        for p in read_status(out_dir / f"{v}.csv"):
            n += 1
            first = first or p[0]
            last = p
            b, cash = p[4], p[6]
            max_b = max(max_b, b)
            if b + cash:
                max_u = max(max_u, b / (b + cash))
        st = gov_state(gov[v])
        rows.append({
            "vault": v, "asset": vr["asset"], "created_block": vr["created_block"],
            "status_rows": n, "first_block": first,
            "last_block": last[0] if last else None,
            "last": dict(zip(STATUS_COLS, map(str, last[3:]))) if last else None,
            "ever_borrowed": max_b > 0, "max_total_borrows": str(max_b),
            "max_utilization": round(max_u, 6),
            "gov_events": len(gov[v]),
            "liquidations": loss[v]["Liquidate"],
            "liquidation_repay_assets": str(loss[v]["repay_assets"]),
            "socializations": loss[v]["DebtSocialized"],
            "socialized_assets": str(loss[v]["socialized_assets"]),
            "governor_admin": st.get("governorAdmin"),
            "creator": st.get("creator"),
        })
    return rows


def gov_state(rows, upto=None):
    """Governance state implied by events with (block, log_index) < upto."""
    st = {"ltv": {}}
    for r in rows:
        if upto is not None and (r["block"], r["log_index"]) >= upto:
            break
        e = r["event"]
        if e == "EVaultCreated":
            st.update(creator=r["creator"], governorAdmin=r["creator"],
                      interestFee=INIT_DEFAULTS["interestFee"],
                      hookTarget=ZERO,
                      hookedOps=INIT_DEFAULTS["hookedOps"])
        elif e == "GovSetGovernorAdmin":
            st["governorAdmin"] = r["newGovernorAdmin"]
        elif e == "GovSetFeeReceiver":
            st["feeReceiver"] = r["newFeeReceiver"]
        elif e == "GovSetInterestFee":
            st["interestFee"] = r["newFee"]
        elif e == "GovSetInterestRateModel":
            st["interestRateModel"] = r["newInterestRateModel"]
        elif e == "GovSetCaps":
            st["caps"] = (r["newSupplyCap"], r["newBorrowCap"])
        elif e == "GovSetHookConfig":
            st.update(hookTarget=r["newHookTarget"], hookedOps=r["newHookedOps"])
        elif e == "GovSetConfigFlags":
            st["configFlags"] = r["newConfigFlags"]
        elif e == "GovSetMaxLiquidationDiscount":
            st["maxLiquidationDiscount"] = r["newDiscount"]
        elif e == "GovSetLiquidationCoolOffTime":
            st["liquidationCoolOffTime"] = r["newCoolOffTime"]
        elif e == "GovSetLTV":
            st["ltv"][r["collateral"]] = (
                r["borrowLTV"], r["liquidationLTV"], r["initialLiquidationLTV"],
                r["targetTimestamp"], r["rampDuration"])
    return st


VIEWS = [
    ("cash()", ["uint256"], "cash"),
    ("totalBorrows()", ["uint256"], "totalBorrows"),
    ("totalSupply()", ["uint256"], "totalShares"),
    ("accumulatedFees()", ["uint256"], "accumulatedFees"),
    ("interestAccumulator()", ["uint256"], "interestAccumulator"),
    ("interestRate()", ["uint256"], "interestRate"),
    ("governorAdmin()", ["address"], "governorAdmin"),
    ("feeReceiver()", ["address"], "feeReceiver"),
    ("interestFee()", ["uint16"], "interestFee"),
    ("interestRateModel()", ["address"], "interestRateModel"),
    ("caps()", ["uint16", "uint16"], "caps"),
    ("hookConfig()", ["address", "uint32"], "hookConfig"),
    ("configFlags()", ["uint32"], "configFlags"),
    ("maxLiquidationDiscount()", ["uint16"], "maxLiquidationDiscount"),
    ("liquidationCoolOffTime()", ["uint16"], "liquidationCoolOffTime"),
]
GOV_KEYS = ["governorAdmin", "feeReceiver", "interestFee", "interestRateModel",
            "caps", "hookConfig", "configFlags", "maxLiquidationDiscount",
            "liquidationCoolOffTime"]
GOV_UNSET = {"feeReceiver": ZERO, "interestRateModel": ZERO, "caps": (0, 0),
             "configFlags": 0, "maxLiquidationDiscount": 0,
             "liquidationCoolOffTime": 0}


def read_views(target, vaults, block):
    calls = [(v, evmabi.selector(sig)) for v in vaults for sig, _, _ in VIEWS]
    res = retry_state_gap(multicall.aggregate3, target, calls, block=block)
    out, k = {}, 0
    for v in vaults:
        d = {}
        for _sig_, types, key in VIEWS:
            ok, ret = res[k]
            k += 1
            if not ok or ret in ("0x", ""):
                d[key] = None
                continue
            val = evmabi.decode(types, ret)
            val = tuple(x.lower() if isinstance(x, str) else x for x in val)
            d[key] = val[0] if len(val) == 1 else val
        out[v] = d
    return out


def compare_gov(st, chain):
    """List of governance keys where events and chain disagree."""
    bad = []
    for key in GOV_KEYS:
        if key == "hookConfig":
            ev = (st.get("hookTarget"), st.get("hookedOps"))
        else:
            ev = st.get(key, GOV_UNSET.get(key))
        if chain.get(key) != ev:
            bad.append({"key": key, "events": ev, "chain": chain.get(key)})
    return bad


def rpow(x, n, base):
    """RPow.rpow from the EVault source (rounding square-and-multiply)."""
    if x == 0:
        return base if n == 0 else 0
    z = base if n % 2 == 0 else x
    half = base // 2
    n //= 2
    while n:
        x = (x * x + half) // base
        if n % 2:
            z = (z * x + half) // base
        n //= 2
    return z


def accrue(last, dt, fee):
    """Borrows and shares after dt seconds at the last event's rate, from the event's (rounded-up) borrows."""
    b, s, cash = last["totalBorrows"], last["totalShares"], last["cash"]
    acc, rate = last["interestAccumulator"], last["interestRate"]
    new_acc = acc * rpow(rate + 10**27, dt, 10**27) // 10**27
    new_b = b * new_acc // acc
    fee_assets = (new_b - b) * fee // 10**4
    if fee_assets:
        total = cash + new_b
        s = total * s // (total - fee_assets)
    return new_b, s


def validate(ccfg, out_dir, vault_rows, ck, n_sample, crosscheck_dir):
    path = out_dir / "validation.json"
    head = ck["to_block"] - 1
    val = json.loads(path.read_text()) if path.exists() else {}
    if val.get("head_block") != head:
        val = {"head_block": head, "stages": {}}
    target = logs.rpc_target(ccfg)
    calls0 = rpc.calls_made()
    gov = defaultdict(list)
    with (out_dir / "governance.jsonl").open() as fh:
        for line in fh:
            r = json.loads(line)
            gov[r["vault"]].append(r)
    last = {}
    for vr in vault_rows:
        p = None
        for p in read_status(out_dir / f"{vr['vault']}.csv"):
            pass
        last[vr["vault"]] = p
    vaults = [vr["vault"] for vr in vault_rows]

    def save():
        val["rpc_calls_total"] = val.get("rpc_calls_total", 0) + (
            rpc.calls_made() - save.calls)
        save.calls = rpc.calls_made()
        io_utils.write_json(str(path.relative_to(io_utils.DATA)), val, GEN,
                            {"chain_id": ck.get("chain_id"), "head_block": head})
    save.calls = calls0

    if "head" not in val["stages"]:
        ts_head = int(retry_state_gap(rpc.get_block, target, head)["timestamp"], 16)
        chain = read_views(target, vaults, head)
        res = Counter()
        fails, max_rel = [], {"totalBorrows": 0.0, "totalShares": 0.0}
        created = {vr["vault"]: vr["created_block"] for vr in vault_rows}
        for v in vaults:
            c, p = chain[v], last[v]
            if created[v] > head:
                res["created_after_head"] += 1
                continue
            if c["cash"] is None:
                res["unreadable"] += 1
                fails.append({"vault": v, "why": "getter reverted"})
                continue
            if p is None:
                ok = c["cash"] == c["totalBorrows"] == c["totalShares"] == 0
                res["no_events_zero_state" if ok else "no_events_nonzero"] += 1
                continue
            ev = dict(zip(STATUS_COLS, p[3:]))
            res["cash_exact" if c["cash"] == ev["cash"] else "cash_diff"] += 1
            if c["cash"] != ev["cash"]:
                fails.append({"vault": v, "key": "cash", "events": ev["cash"],
                              "chain": c["cash"]})
            pb, ps = accrue(ev, ts_head - p[2], c["interestFee"] or 0)
            for key, pred in (("totalBorrows", pb), ("totalShares", ps)):
                got = c[key]
                if got == ev[key]:
                    res[f"{key}_exact"] += 1
                    continue
                rel = abs(got - pred) / max(got, 1)
                max_rel[key] = max(max_rel[key], rel)
                if abs(got - pred) <= 2 or rel <= 1e-9:
                    res[f"{key}_accrued_match"] += 1
                else:
                    res[f"{key}_diff"] += 1
                    fails.append({"vault": v, "key": key, "events": ev[key],
                                  "predicted": pred, "chain": got})
            bad = compare_gov(gov_state(gov[v]), c)
            res["gov_match" if not bad else "gov_diff"] += 1
            if bad:
                fails.append({"vault": v, "gov": bad})
        pairs = [(v, col, t) for v in vaults
                 for col, t in gov_state(gov[v])["ltv"].items()]
        sel = evmabi.selector("LTVFull(address)")
        out = retry_state_gap(multicall.aggregate3, target, [
            (v, sel + eth_abi.encode(["address"], [col]).hex())
            for v, col, _ in pairs], block=head) if pairs else []
        for (v, col, t), (ok, ret) in zip(pairs, out):
            got = evmabi.decode(["uint16", "uint16", "uint16", "uint48",
                                 "uint32"], ret) if ok else None
            if got == t:
                res["ltv_match"] += 1
            else:
                res["ltv_diff"] += 1
                fails.append({"vault": v, "collateral": col, "events": t,
                              "chain": got})
        val["stages"]["head"] = {
            "block": head, "block_timestamp": ts_head, "counts": dict(res),
            "max_rel_err_after_accrual": max_rel, "failures": fails[:50],
            "method": "getters at the scan head; cash must equal the last "
                      "VaultStatus; totalBorrows and totalSupply are equal "
                      "when nothing accrued, otherwise compared with the "
                      "last event accrued to the head timestamp at its "
                      "stored rate (EVault initVaultCache logic), match "
                      "if within 2 wei or 1e-9 relative; governance getters "
                      "against state replayed from governance.jsonl; "
                      "LTVFull for every collateral ever set"}
        save()
        print(f"validation head: {dict(res)}", flush=True)

    if "last_event" not in val["stages"]:
        res, fails = Counter(), []
        by_block = defaultdict(list)
        for v, p in last.items():
            if p is not None:
                by_block[p[0]].append(v)
        for blk, vs in sorted(by_block.items()):
            chain = read_views(target, vs, blk)
            for v in vs:
                ev = dict(zip(STATUS_COLS, last[v][3:]))
                bad = [k for k in STATUS_COLS if chain[v][k] != ev[k]]
                res["exact" if not bad else "diff"] += 1
                if bad:
                    fails.append({"vault": v, "block": blk, "keys": bad})
        val["stages"]["last_event"] = {
            "counts": dict(res), "failures": fails[:50],
            "method": "the six VaultStatus fields read by getters at the "
                      "block of each vault's last VaultStatus (no time has "
                      "passed, so no accrual): exact equality"}
        save()
        print(f"validation last_event: {dict(res)}", flush=True)

    if "sample" not in val["stages"]:
        rng = random.Random(20261002)
        cand = []
        for v in vaults:
            prev = None
            for p in read_status(out_dir / f"{v}.csv"):
                if prev is not None and p[0] != prev[0]:
                    cand.append((v, prev))
                prev = p
            if prev is not None:
                cand.append((v, prev))
        pts = rng.sample(cand, min(n_sample, len(cand)))
        res, fails = Counter(), []
        for v, p in pts:
            c = read_views(target, [v], p[0])[v]
            ev = dict(zip(STATUS_COLS, p[3:]))
            bad = [k for k in STATUS_COLS if c[k] != ev[k]]
            gbad = compare_gov(gov_state(gov[v], upto=(p[0] + 1, 0)), c)
            res["state_exact" if not bad else "state_diff"] += 1
            res["gov_match" if not gbad else "gov_diff"] += 1
            if bad or gbad:
                fails.append({"vault": v, "block": p[0], "keys": bad,
                              "gov": gbad})
        val["stages"]["sample"] = {
            "points": len(pts), "candidates": len(cand), "seed": 20261002,
            "counts": dict(res), "failures": fails[:50],
            "method": "random (vault, block) where the row is the vault's "
                      "last VaultStatus in that block; six fields exact at "
                      "that block, and governance getters against state "
                      "replayed from events up to the end of that block"}
        save()
        print(f"validation sample: {dict(res)}", flush=True)

    sib = crosscheck_dir / f"{ck['chain_id']}.jsonl"
    if "crosscheck" not in val["stages"] and sib.exists():
        meta = json.loads(sib.with_suffix(".jsonl.meta.json").read_text())
        frm = meta["coverage"]["from_block"]
        upto = meta["coverage"]["scanned_to_block"]
        res, fails = Counter(), []
        with sib.open() as fh:
            for line in fh:
                r = json.loads(line)
                v = r["vault"]
                if v not in last:
                    res["not_in_registry"] += 1
                    continue
                n, final = 0, None
                for p in read_status(out_dir / f"{v}.csv"):
                    if p[0] >= upto:
                        break
                    n += p[0] >= frm
                    final = p
                liqs = sum(1 for g in _losses(out_dir, v)
                           if g["event"] == "Liquidate"
                           and frm <= g["block"] < upto)
                ok = (final is not None and str(final[4]) == r["final_borrows"]
                      and str(final[6]) == r["final_cash"])
                res["final_state_match" if ok else "final_state_diff"] += 1
                res["n_events_match" if n == r["n_events"] else "n_events_diff"] += 1
                res["n_liq_match" if liqs == r["n_liquidations"] else "n_liq_diff"] += 1
                if not ok or n != r["n_events"] or liqs != r["n_liquidations"]:
                    fails.append({"vault": v, "ours": [n, liqs, final and str(final[4]), final and str(final[6])],
                                  "crosscheck": [r["n_events"], r["n_liquidations"], r["final_borrows"], r["final_cash"]]})
        val["stages"]["crosscheck"] = {
            "file": "data/inputs/utilization_check/euler_v2/" + sib.name, "crosscheck_from_block": frm,
            "crosscheck_scanned_to_block": upto,
            "counts": dict(res), "failures": fails[:50],
            "method": "for each vault in the independent Euler scan "
                      "(VaultStatus and Liquidate by vault address): final "
                      "borrows and cash before its scan head, and the "
                      "VaultStatus and Liquidate counts within its scan "
                      "range, must equal ours"}
        save()
        print(f"validation crosscheck: {dict(res)}", flush=True)
    return val


def _losses(out_dir, v, _cache={}):
    if out_dir not in _cache:
        d = defaultdict(list)
        with (out_dir / "losses.jsonl").open() as fh:
            for line in fh:
                r = json.loads(line)
                d[r["vault"]].append(r)
        _cache[out_dir] = d
    return _cache[out_dir][v]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("chain_id")
    ap.add_argument("--estimate-only", action="store_true")
    ap.add_argument("--no-validate", action="store_true")
    ap.add_argument("--sample", type=int, default=300)
    ap.add_argument("--crosscheck", default=str((
        pathlib.Path(os.environ["CB_INPUTS"]).resolve() if os.environ.get("CB_INPUTS")
        else HERE.parents[1] / "data" / "inputs") / "utilization_check" / "euler_v2"))
    args = ap.parse_args()
    cid = args.chain_id
    ccfg = logs.load_chains()[cid]
    entry = json.loads((HERE.parent / "config/deployments.json").read_text()
                       )["protocols"]["euler_v2"]["chains"][cid]
    reg = io_utils.read_jsonl(f"registry/markets/euler_v2/{cid}.jsonl")
    vault_rows = sorted((r for r in reg if r["type"] == "vault"),
                        key=lambda r: r["vault"])
    vaults = {r["vault"] for r in vault_rows}
    start = min(r["created_block"] for r in vault_rows)
    out_dir = io_utils.DATA / f"panel/euler_v2/{cid}"
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    dec = decoders(load_abi(cid, ccfg, entry, out_dir))
    print(f"chain {cid}: {len(vaults)} vaults, {len(dec)} event types, "
          f"from block {start:,}", flush=True)
    ck_path = out_dir / "checkpoint.json"
    if ck_path.exists():
        to_block = json.loads(ck_path.read_text())["to_block"]
    else:
        to_block = hypersync_client.get_height(ccfg["hypersync"]) - CONFIRMATIONS
        print("estimate:", json.dumps(estimate(ccfg, vaults, dec, start,
                                               to_block)), flush=True)
        if args.estimate_only:
            return
    ck = scan(ccfg, out_dir, vaults, dec, start, to_block)
    ck["chain_id"] = int(cid)

    rows = summarize(out_dir, vault_rows)
    counts = Counter({"VaultStatus": sum(r["status_rows"] for r in rows)})
    for kind in ("governance", "losses"):
        with (out_dir / f"{kind}.jsonl").open() as fh:
            counts.update(json.loads(line)["event"] for line in fh)
    meta = {"chain_id": int(cid), "chain": ccfg["name"],
            "from_block": ck["from_block"], "scanned_to_block": ck["to_block"],
            "registry": f"registry/markets/euler_v2/{cid}.jsonl",
            "events_in_set": dict(sorted(counts.items()))}
    io_utils.write_jsonl(f"panel/euler_v2/{cid}/vaults.jsonl", rows, GEN, {
        **meta, "series_files": "<vault>.csv, one per registry vault",
        "series_columns": ["block", "log_index", "timestamp", *STATUS_COLS],
        "series_units": "raw uint256 as emitted; interestRate is ray (1e27) "
                         "per second; interestAccumulator starts at 1e27"})
    for kind in ("governance", "losses"):
        n = sum(c for e, c in counts.items() if e != "VaultStatus"
                and (e in LOSS_EVENTS) == (kind == "losses"))
        io_utils._meta_sidecar(out_dir / f"{kind}.jsonl", GEN, {
            **meta, "rows": n,
            "init_defaults_without_events": INIT_DEFAULTS})
    size = sum(f.stat().st_size for f in out_dir.iterdir())
    print(f"outputs: {size / 1e6:.1f} MB in {out_dir}", flush=True)

    if not args.no_validate:
        validate(ccfg, out_dir, vault_rows, ck, args.sample,
                 pathlib.Path(args.crosscheck))
    print(f"done in {time.time() - t0:.0f}s, rpc calls this run "
          f"{rpc.calls_made()}", flush=True)


if __name__ == "__main__":
    main()
