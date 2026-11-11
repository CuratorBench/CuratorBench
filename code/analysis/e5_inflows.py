# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "matplotlib>=3.8"]
# ///
"""E5 addendum: the mechanism behind every rise in a curated vault's exposure to a depeg-loss event's markets after t0."""
import csv
import json
import os
import pathlib
import sys
import time
from collections import defaultdict

sys.dont_write_bytecode = True
ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "code" / "analysis"))
import e5_events as e5
from e5_events import e2
from common import io_utils, rpc

GENERATOR = "code/analysis/e5_inflows.py"
DATA = ROOT / "data"
DERIVED = e5.DERIVED
OUT = "metrics/e5/post_onset_inflows.csv"
WINDOW_D = 30
MATERIAL_USD = e5.MATERIAL_USD
DAY = e2.DAY
PUBLIC_ALLOCATOR = {
    ("morpho", 1): "0xfd32fa2ca22c76dd6e550706ad913fc6ce91c75d",
    ("morpho", 8453): "0xa090dd1a701408df1d4d0b85b716c87565f90467",
    ("morpho", 999): "0x517505be22d9068687334e69ae7a02fc77edf4fc",
    ("morpho", 42161): "0x769583af5e9d03589f159ebec31cc2c23e8c355e",
    ("morpho", 747474): "0x39eb6da5e88194c82b13491df2e8b3e213ed2412",
}
FORCE = {("E2-026", "0xa9c251f8304b1b3fc2b9e8fcae78d94eff82ac66")}
COMPONENTS = ("allocator", "public_allocator", "supply_queue", "external_onbehalf", "other")
COLS = ["event_id", "family", "protocol", "chain_id", "vault", "kind", "name", "curator",
        "curator_name", "group", "t0_utc", "exposure_t0_usd", "max_exposure_usd_after_t0",
        "inflow_usd_total"] + [f"inflow_usd_{c}" for c in COMPONENTS] + [
        "mechanism_main", "outflow_usd_total", "n_tx", "n_tx_by_mechanism", "first_tx_time",
        "example_tx", "example_tx_usd", "example_tx_mechanism", "realloc_callers",
        "extracted_usd_onbehalf_tx", "loss_usd"]


def pos_deltas(proto, path, t_lo, t_hi):
    """{tx: {market: change of the vault's assets}}, {tx."""
    col = e2.POS_MARKET_COL[proto]
    delta = defaultdict(lambda: defaultdict(int))
    ext = defaultdict(lambda: defaultdict(int))
    meta = {}
    if proto in ("morpho", "moolah"):
        with open(path, newline="") as fh:
            for r in csv.DictReader(fh):
                ts = int(r["timestamp"])
                if not t_lo <= ts <= t_hi:
                    continue
                tx = r["tx_hash"]
                meta.setdefault(tx, (int(r["block"]), int(r["log_index"]), ts))
                a = int(r["assets"] or 0)
                delta[tx][r[col]] += a if r["kind"] == "supply" else -a
        return delta, ext, meta
    aps = {}
    sh = defaultdict(lambda: defaultdict(int))
    vault = path.stem.lower()
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            m, kind, ts = r[col], r["kind"], int(r["timestamp"])
            a = int(r["assets"]) if r["assets"] else 0
            es = int(r["event_shares"]) if r["event_shares"] else 0
            if kind in ("Deposit", "Withdraw") and a > 0 and es > 0:
                aps[m] = a / es
            if not t_lo <= ts <= t_hi:
                continue
            tx = r["tx_hash"]
            meta.setdefault(tx, (int(r["block"]), int(r["log_index"]), ts))
            if kind == "Deposit":
                delta[tx][m] += a
                sh[tx][m] -= es
                if proto == "silo_v2" and (r.get("sender") or "").lower() != vault:
                    ext[tx][m] += a
            elif kind == "Withdraw":
                delta[tx][m] -= a
                sh[tx][m] += es
            else:
                sh[tx][m] += int(r["shares_delta"] or 0)
                sh[tx].setdefault(("aps", m), aps.get(m))
    for tx, d in sh.items():
        for m, s in list(d.items()):
            if isinstance(m, tuple) or s == 0:
                continue
            p = d.get(("aps", m)) or aps.get(m)
            if p:
                delta[tx][m] += int(round(s * p))
    return delta, ext, meta


_HELD = {}


def held_markets(proto, path):
    """Markets that ever appear in a vault's positions file."""
    if path not in _HELD:
        col = e2.POS_MARKET_COL[proto]
        with open(path, newline="") as fh:
            _HELD[path] = {r[col] for r in csv.DictReader(fh)}
    return _HELD[path]


def vault_flows(proto, cid, vault, t_lo, t_hi):
    """{tx: (deposits, withdrawals, depositor senders)} of the vault."""
    out = {}
    p = DATA / f"vaults/{proto}/{cid}/flows/{vault}.csv"
    if not p.exists():
        return out
    with open(p, newline="") as fh:
        for r in csv.DictReader(fh):
            ts = int(r["timestamp"])
            if not t_lo <= ts <= t_hi:
                continue
            d, w, s = out.get(r["tx_hash"], (0, 0, set()))
            a = int(r["assets"] or 0)
            if r["kind"].lower() == "deposit":
                d += a
                s.add((r["sender"] or "").lower())
            elif r["kind"].lower() == "withdraw":
                w += a
            out[r["tx_hash"]] = (d, w, s)
    return out


def load_reallocs(proto, cid, windows):
    """{(vault, tx): [(event, market, assets, caller, adapter, ids)]} of the ReallocateSupply and Allocate logs of the given vaults in their windows."""
    p = DATA / f"vaults/{proto}/{cid}/actions.jsonl"
    out = defaultdict(list)
    if not p.exists() or not windows:
        return out
    vkeys = [v.encode() for v in windows]
    with open(p, "rb") as fh:
        for line in fh:
            if b'"ReallocateSupply"' not in line and b'"Allocate"' not in line:
                continue
            if not any(k in line for k in vkeys):
                continue
            r = json.loads(line)
            v = r.get("vault")
            if v not in windows or r.get("event") not in ("ReallocateSupply", "Allocate"):
                continue
            lo, hi = windows[v]
            if not lo <= r["timestamp"] <= hi:
                continue
            f = r.get("fields") or r
            if r["event"] == "ReallocateSupply":
                out[(v, r["tx_hash"])].append((
                    "ReallocateSupply", (f.get("id") or f.get("market") or "").lower(),
                    int(f.get("suppliedAssets") or 0), (f.get("caller") or "").lower(), "", ()))
            else:
                out[(v, r["tx_hash"])].append((
                    "Allocate", "", int(f.get("assets") or 0), (f.get("sender") or "").lower(),
                    (f.get("adapter") or "").lower(), tuple(x.lower() for x in f.get("ids") or [])))
    return out


def classify_tx(proto, cid, kind, vault, ms, d, ext_direct, flow, reallocs):
    """Split one inflow tx's change in the event markets into COMPONENTS."""
    aff = sum(d.get(m, 0) for m in ms)
    dep, wd, senders = flow or (0, 0, set())
    pa = PUBLIC_ALLOCATOR.get((proto, cid))
    parts = dict.fromkeys(COMPONENTS, 0.0)
    rl = defaultdict(float)
    callers = set()
    for ev, m, a, caller, adapter, ids in reallocs:
        if ev == "ReallocateSupply":
            if m not in ms:
                continue
            rl["public_allocator" if caller == pa else "allocator"] += a
        else:
            hit = [x for x in ms if adapter in e5.v2_ids(cid, vault, x)[1]
                   and set(ids) & e5.v2_ids(cid, vault, x)[0]]
            if not hit:
                continue
            rl["supply_queue" if caller in senders else "allocator"] += a
        callers.add(caller)
    tot = sum(rl.values())
    scale = min(1.0, aff / tot) if tot > 0 else 0.0
    for k, a in rl.items():
        parts[k] += a * scale
    rem = aff - tot * scale
    if rem > 0:
        if kind == "morpho_v2":
            ext = rem
        else:
            residual = sum(d.values()) - (dep - wd)
            ext = min(rem, max(residual, sum(ext_direct.get(m, 0) for m in ms), 0))
        parts["external_onbehalf"] += ext
        rem -= ext
    if rem > 0:
        sq = min(rem, dep)
        parts["supply_queue"] += sq
        rem -= sq
    parts["other"] += max(rem, 0)
    return aff, parts, callers


def compute(all_rows, events):
    """Inflow rows (COLS) for E5 rows (dicts as in event_vault.csv) of depeg-loss events, not Euler lending vaults, and the input files read."""
    rows = [r for r in all_rows
            if r["event_type"] == "depeg_loss" and r["kind"] != "euler_lending"]
    tokens = e5.load_tokens()
    cands = []
    wanted = set()
    for r in rows:
        e = events[r["event_id"]]
        proto, cid, vault = r["protocol"], int(r["chain_id"]), r["vault"]
        ms = set(e5.event_pairs(e).get((proto, cid), []))
        t0 = e5.e5_onset(e)
        lo, hi = t0, t0 + WINDOW_D * DAY
        p = DATA / f"vaults/{proto}/{cid}/positions/{vault}.csv"
        if not p.exists() or not ms:
            continue
        delta, ext, meta = pos_deltas(proto, p, lo, hi)
        inflow_tx = [tx for tx, d in delta.items() if sum(d.get(m, 0) for m in ms) > 0]
        if not inflow_tx and (r["event_id"], vault) not in FORCE:
            continue
        info = e2.market_info_cached(proto, cid)
        held = [m for m in ms if m in held_markets(proto, p)]
        loans = {info[m]["loan"] for m in held if m in info}
        if len(loans) != 1:
            raise SystemExit(f"{r['event_id']} {vault}: loan tokens {loans}")
        loan = loans.pop()
        ms = {m for m in ms if info.get(m, {}).get("loan") == loan}
        inflow_tx = [tx for tx in inflow_tx if sum(delta[tx].get(m, 0) for m in ms) > 0]
        wanted.add((cid, loan))
        cands.append(dict(row=r, e=e, proto=proto, cid=cid, vault=vault, ms=ms, t0=t0,
                          lo=lo, hi=hi, delta=delta, ext=ext, meta=meta, inflow_tx=inflow_tx,
                          loan=loan, path=p))
    prices = e2.Prices(tokens, wanted)

    def usd(c, raw, ts):
        v = prices.value(c["cid"], c["loan"], raw, ts // DAY)
        return 0.0 if v is None else v

    keep = []
    for c in cands:
        tot = sum(usd(c, sum(c["delta"][tx].get(m, 0) for m in c["ms"]), c["meta"][tx][2])
                  for tx in c["inflow_tx"])
        if tot >= MATERIAL_USD or (c["row"]["event_id"], c["vault"]) in FORCE:
            keep.append(c)
    by_pair = defaultdict(dict)
    for c in keep:
        w = by_pair[(c["proto"], c["cid"])].get(c["vault"])
        by_pair[(c["proto"], c["cid"])][c["vault"]] = (
            (min(w[0], c["lo"]), max(w[1], c["hi"])) if w else (c["lo"], c["hi"]))
    reallocs, files = {}, set()
    for (proto, cid), windows in sorted(by_pair.items()):
        reallocs[(proto, cid)] = load_reallocs(proto, cid, windows)
        files.add(DATA / f"vaults/{proto}/{cid}/actions.jsonl")
    out = []
    for c in keep:
        r, proto, cid, vault, ms = c["row"], c["proto"], c["cid"], c["vault"], c["ms"]
        flows = vault_flows(proto, cid, vault, c["lo"], c["hi"])
        files |= {c["path"], DATA / f"vaults/{proto}/{cid}/flows/{vault}.csv"}
        sums = dict.fromkeys(COMPONENTS, 0.0)
        ntx = defaultdict(int)
        callers = set()
        best = None
        outflow = extracted = 0.0
        for tx, d in c["delta"].items():
            ts = c["meta"][tx][2]
            aff = sum(d.get(m, 0) for m in ms)
            if aff < 0:
                outflow += usd(c, -aff, ts)
            if aff <= 0:
                continue
            _, parts, cl = classify_tx(proto, cid, r["kind"], vault, ms, d, c["ext"].get(tx, {}),
                                       flows.get(tx), reallocs[(proto, cid)].get((vault, tx), []))
            callers |= cl
            px = usd(c, aff, ts) / aff
            if parts["external_onbehalf"] > 0:
                dep, wd, _s = flows.get(tx) or (0, 0, set())
                extracted += max(wd - dep, 0) * px
            pu = {k: v * px for k, v in parts.items()}
            for k, v in pu.items():
                sums[k] += v
            main_k = max(pu, key=pu.get)
            ntx[main_k] += 1
            if best is None or aff * px > best[1]:
                best = (tx, aff * px, main_k)
        total = sum(sums.values())
        first = min((c["meta"][tx] for tx in c["inflow_tx"]), default=None)
        out.append({
            "event_id": r["event_id"], "family": r["family"], "protocol": proto,
            "chain_id": cid, "vault": vault, "kind": r["kind"], "name": r["name"],
            "curator": r["curator"], "curator_name": r["curator_name"], "group": r["group"],
            "t0_utc": e2.utc(c["t0"]), "exposure_t0_usd": r["exposure_t0_usd"],
            "max_exposure_usd_after_t0": r["max_exposure_usd_after_t0"],
            "inflow_usd_total": round(total, 2),
            **{f"inflow_usd_{k}": round(v, 2) for k, v in sums.items()},
            "mechanism_main": max(sums, key=sums.get) if total > 0 else "",
            "outflow_usd_total": round(outflow, 2), "n_tx": sum(ntx.values()),
            "n_tx_by_mechanism": json.dumps(dict(sorted(ntx.items()))),
            "first_tx_time": e2.utc(first[2]) if first else "",
            "example_tx": best[0] if best else "", "example_tx_usd": round(best[1], 2) if best else "",
            "example_tx_mechanism": best[2] if best else "",
            "realloc_callers": ";".join(sorted(callers)),
            "extracted_usd_onbehalf_tx": round(extracted, 2), "loss_usd": r["loss_usd"]})
    out.sort(key=lambda x: (x["event_id"], -x["inflow_usd_total"]))
    return out, files


def main():
    t_start = time.time()
    ev_path = DERIVED / "metrics/e5/event_vault.csv"
    events = {e["event_id"]: e for e in e5.load_events()[0]}
    with open(ev_path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    out, files = compute(rows, events)
    path = DERIVED / OUT
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", newline="") as fh:
        w = csv.DictWriter(fh, COLS)
        w.writeheader()
        w.writerows(out)
    os.replace(tmp, path)
    inputs = {}
    for p in [ev_path, DERIVED / "events/events.jsonl"] + sorted(files):
        if p.exists():
            side = p.with_name(p.name + ".meta.json")
            rel = str(p.relative_to(DERIVED if str(p).startswith(str(DERIVED)) else DATA))
            inputs[rel] = {"sha256": e2.sha256(p),
                           "written_utc": json.loads(side.read_text()).get("written_utc")
                           if side.exists() else None}
    io_utils._meta_sidecar(path, GENERATOR, {
        "what": "one row per (depeg-loss event, curated vault in E5's event_vault.csv, not an "
                "Euler lending vault) whose position in the event's markets rose in transactions "
                f"within {WINDOW_D} days after E5's t0 by at least {MATERIAL_USD:,.0f} USD in sum, "
                "split by the mechanism of each rise",
        "rules": {
            "inflow": "per tx, change of the vault's assets in the event's markets summed over "
                      "them (Morpho/Moolah supply - withdraw, Vault V2 via its adapters; Silo/Euler "
                      "Earn Deposit - Withdraw plus non-mint share transfers at the last "
                      "conversion); a tx with a positive change is an inflow tx",
            "public_allocator": "ReallocateSupply into an event market with caller = the "
                                "chain's Morpho Public Allocator",
            "allocator": "ReallocateSupply into an event market by any other caller; Vault V2 "
                         "Allocate on an adapter covering an event market by a sender that did "
                         "not deposit in the same tx",
            "supply_queue": "Vault V2 Allocate whose sender deposited in the same tx; else the "
                            "remainder up to the vault's deposits in the tx (supply-queue routing, "
                            "including same-tx deposit/withdraw loops)",
            "external_onbehalf": "residual = change of all the vault's positions in the tx - "
                                 "(vault deposits - withdrawals), if positive (MetaMorpho forks "
                                 "hold no idle cash); Silo: market Deposit with owner = vault and "
                                 "sender != vault; Vault V2: adapter increase no Allocate covers",
            "other": "the remainder",
            "extracted_usd_onbehalf_tx": "vault withdrawals less deposits (floored at 0) in the "
                                         "transactions with an external_onbehalf part: the cash "
                                         "the third party took out of the vault in the same "
                                         "transaction (donation attacks), USD at the inflow's "
                                         "price; not part of E5's loss",
            "order": "reallocations first (logged amounts, scaled pro rata to the tx change), "
                     "then external_onbehalf, then supply_queue, then other",
            "mechanism_main": "the largest component in USD",
            "usd": "vault asset at the DefiLlama close of the tx day (E2 Prices)",
            "floor": f"inflow_usd_total >= {MATERIAL_USD:,.0f}, except FORCE rows",
        },
        "parameters": {"window_days": WINDOW_D, "material_usd": MATERIAL_USD,
                       "public_allocator": {f"{p}:{c}": a for (p, c), a in
                                            PUBLIC_ALLOCATOR.items()},
                       "force": sorted(f"{a}:{b}" for a, b in FORCE)},
        "inputs": inputs, "rows": len(out), "columns": COLS,
        "units": "USD; times UTC", "rpc_calls": rpc.calls_made(),
    })
    print(json.dumps({"rows": len(out), "seconds": round(time.time() - t_start, 1),
                      "rpc_calls": rpc.calls_made()}))
    for x in out:
        print(x["event_id"], str(x["name"])[:28], x["chain_id"], x["curator"], x["mechanism_main"],
              round(x["inflow_usd_total"]), {k: round(x[f"inflow_usd_{k}"]) for k in COMPONENTS
                                             if x[f"inflow_usd_{k}"]}, x["n_tx"],
              str(x["loss_usd"])[:10], x["extracted_usd_onbehalf_tx"])


if __name__ == "__main__":
    main()
