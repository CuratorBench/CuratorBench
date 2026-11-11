# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy>=1.26", "pandas>=2.1", "scipy>=1.11", "matplotlib>=3.8", "statsmodels>=0.14", "requests>=2.31", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "hypersync==1.2.0"]
# ///
"""E6: curator incentives."""
import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import math
import os
import pathlib
import pickle
import subprocess
import sys
import time
from collections import Counter, defaultdict

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "code"))
from common import evmabi, io_utils, multicall, outroot, rpc

GENERATOR = "code/analysis/e6_incentives.py"
DATA = ROOT / "data"
DERIVED = outroot.OUT_DATA
OUT = pathlib.Path(os.environ.get("E6_OUT", DERIVED / "metrics/e6"))
WORK = pathlib.Path(os.environ.get("E6_WORK", "/tmp/e6_incentives_work"))
E4_DIR = pathlib.Path(os.environ.get("E6_E4_DIR", DERIVED / "metrics/e4"))
E5_EVENT_VAULT = DERIVED / "metrics/e5/event_vault.csv"
EVENTS_DIR = DERIVED / "events"
DAY = 86400
YEAR_S = 365 * DAY
SEED = 20261008
N_PERM = 10_000
ZERO = "0x" + "0" * 40

PAIRS = (("morpho", 1), ("morpho", 8453), ("morpho", 4663), ("morpho", 42161), ("morpho", 747474),
         ("morpho", 999), ("moolah", 56), ("euler_v2", 1), ("euler_v2", 9745), ("euler_v2", 59144),
         ("silo_v2", 1), ("silo_v2", 42161), ("silo_v2", 43114), ("silo_v2", 146))
EULER_CHAINS = (1, 9745, 59144)
EVK_DEFAULT_INTEREST_FEE = 0.1
EVK_VIRTUAL = 10 ** 6
FEE_EVENTS = ("SetFee", "SetFeeRecipient", "SetTimelock", "SetPerformanceFee", "SetManagementFee",
              "SetPerformanceFeeRecipient", "SetManagementFeeRecipient", "IncreaseTimelock",
              "DecreaseTimelock")
V2_CAP_SELECTOR = evmabi.selector("increaseAbsoluteCap(bytes,uint256)")
V2_FEE_SELECTOR = evmabi.selector("setPerformanceFee(uint256)")

CLASS_THRESHOLD = 0.9
MIN_CLASS_N = 10
WITHIN_CLASSES = ("stable", "mixed", "eth", "btc")
WINDOW_FEE_CHANGE = 60
MIN_WINDOW_ACTIVE = 30
REPUTATION_DAYS = 90
MIN_SEGMENT_USD = 1e6
APY_CAP = 1.0
MAX_DAILY_PPS = 0.03
TL_MAIN = ("tl_d0150", "tl_d0300", "tl_d0500", "tl_d1000", "tl_headline")
TL_V1 = tuple(c.replace("tl_", "tl_v1_", 1) for c in TL_MAIN)
METRICS = (("yield_after_fee", "after-fee yield"), ("tl_d0150", "tail loss 0.15"),
           ("tl_d0300", "tail loss 0.30"), ("tl_d0500", "tail loss 0.50"),
           ("tl_d1000", "tail loss 1.00"), ("tl_headline", "tail loss headline"),
           ("L_p10", "liquidity p10"), ("L_median", "liquidity median"),
           ("loss_per_dollar", "event loss per dollar"))


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


def stamp(path):
    """sha256 and the sidecar's written_utc of an input file."""
    path = pathlib.Path(path)
    side = pathlib.Path(str(path) + ".meta.json")
    written = json.loads(side.read_text()).get("written_utc") if side.exists() else None
    return {"path": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
            "sha256": sha256(path), "written_utc": written}


def e4_stamps():
    """Stamps of E4's scorecard and daily table, taken when a stage starts reading them."""
    return {"e4_vault_scorecard": stamp(E4_DIR / "vault_scorecard.csv"),
            "e4_vault_daily": stamp(E4_DIR / "vault_daily.csv")}


def check_e4_unchanged(before):
    """Stop when E4 rewrote its outputs while a stage was reading them."""
    now = e4_stamps()
    if any(now[k]["sha256"] != before[k]["sha256"] for k in before):
        raise RuntimeError("E4 outputs changed during this stage; rerun it on a stable E4 version")


def to_int(x):
    """Exact integer from a CSV cell (empty or NaN is 0)."""
    if x is None or (isinstance(x, float) and math.isnan(x)) or x == "":
        return 0
    return int(x)


def wpickle(name, obj):
    WORK.mkdir(parents=True, exist_ok=True)
    tmp = WORK / (name + ".tmp")
    with open(tmp, "wb") as fh:
        pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, WORK / name)


def rpickle(name):
    with open(WORK / name, "rb") as fh:
        return pickle.load(fh)


def write_csv(df, name, meta):
    """CSV under data/metrics/e6 with a sidecar (generator, time, rows)."""
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)
    io_utils._meta_sidecar(path, GENERATOR, {**meta, "rows": int(len(df)), "columns": list(df.columns)})
    return path


def write_json(obj, name, meta):
    """JSON under the output directory with a sidecar."""
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True, default=float) + "\n")
    os.replace(tmp, path)
    io_utils._meta_sidecar(path, GENERATOR, meta)
    return path


def grep_lines(path, needles):
    """Lines of a large JSONL file that contain any of the quoted needles."""
    args = ["grep", "-F"]
    for n in needles:
        args += ["-e", f'"{n}"']
    res = subprocess.run(args + [str(path)], capture_output=True, text=True,
                         env={**os.environ, "LC_ALL": "C"})
    return res.stdout.splitlines()


def load_e4():
    """E4 module, loaded by path for its token unit rules (TokenBook, infer_price_units) so collateral units match E4's."""
    spec = importlib.util.spec_from_file_location("e4_metrics", ROOT / "code/analysis/e4_metrics.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def drpc_slug(cid):
    return json.loads((ROOT / "code/config/chains.json").read_text())[str(cid)]["drpc"]


def hypersync_slug(cid):
    return json.loads((ROOT / "code/config/chains.json").read_text())[str(cid)]["hypersync"]


def e3_heads():
    return json.loads((DERIVED / "aum/vault_daily.csv.meta.json").read_text())["heads"]


def registry_vaults():
    """{(chain, vault): registry row with protocol} for allocator vaults."""
    out = {}
    for proto, cid in PAIRS:
        for r in jsonl(DATA / f"registry/vaults/{proto}/{cid}.jsonl"):
            out[(cid, r["vault"].lower())] = {**r, "protocol": proto}
    return out


def curator_intervals():
    """{(chain, vault): sorted [(from_ts, to_ts or inf, from_block, to_block or inf, entity, confidence)]} from data/registry/vault_curators.jsonl."""
    out = defaultdict(list)
    for r in jsonl(DATA / "registry/vault_curators.jsonl"):
        out[(r["chain_id"], r["vault"].lower())].append(
            (r["from_ts"], r["to_ts"] if r["to_ts"] is not None else math.inf,
             r["from_block"], r["to_block"] if r["to_block"] is not None else math.inf,
             r["entity_id"], r["confidence"]))
    for k in out:
        out[k].sort()
    return out


def entity_at_block(iv, block):
    for _fts, _tts, fb, tb, ent, conf in iv:
        if fb <= block < tb:
            return ent, conf
    return None, None


def entity_on_days(iv, days):
    """Entity holding the vault at the end of each UTC day (array of objects)."""
    ends = (np.asarray(days, dtype=np.int64) + 1) * DAY - 1
    ent = np.full(len(ends), None, dtype=object)
    for fts, tts, _fb, _tb, e, _c in iv:
        ent[(ends >= fts) & (ends < tts)] = e
    return ent


def curator_names():
    return {r["entity_id"]: r.get("display_name") or r["entity_id"]
            for r in jsonl(DATA / "registry/curators.jsonl")}


def excluded_vaults():
    """{(chain, vault): reasons} of vaults E3 leaves out (owner-funded)."""
    return {(r["chain_id"], r["vault"].lower()): ";".join(r["reasons"])
            for r in jsonl(DERIVED / "aum/excluded_vaults.jsonl")}


def e3_prices():
    """Per vault-day pricing from E3."""
    cols = ["protocol", "chain_id", "vault", "kind", "date", "total_assets", "total_supply",
            "counted_assets", "asset", "asset_symbol", "price_usd", "aum_usd", "held_by_vaults_usd",
            "source", "guard", "excluded_reason"]
    df = pd.read_csv(DERIVED / "aum/vault_daily.csv", usecols=cols,
                     dtype={"vault": str, "total_assets": str, "total_supply": str,
                            "counted_assets": str, "asset": str, "asset_symbol": str, "guard": str,
                            "excluded_reason": str, "kind": str, "protocol": str})
    lend = pd.read_csv(DERIVED / "aum/lending_daily.csv",
                       usecols=["protocol", "chain_id", "vault", "date", "total_assets", "total_shares",
                                "counted_assets", "asset", "asset_symbol", "price_usd", "aum_usd",
                                "held_by_vaults_usd", "source", "guard", "governor", "governed"],
                       dtype={"vault": str, "total_assets": str, "total_shares": str,
                              "counted_assets": str, "asset": str, "asset_symbol": str, "guard": str,
                              "governor": str})
    lend = lend.rename(columns={"total_shares": "total_supply"})
    lend["kind"] = "euler_lending"
    lend["excluded_reason"] = np.nan
    df = pd.concat([df, lend], ignore_index=True)
    df["vault"] = df.vault.str.lower()
    df["day"] = (pd.to_datetime(df.date) - pd.Timestamp("1970-01-01")).dt.days.astype(np.int64)
    ca = df.counted_assets.astype(float)
    ok = (df.aum_usd >= 100) & (df.price_usd > 0) & (ca > 0)
    dec = np.round(np.log10(ca[ok] * df.price_usd[ok] / df.aum_usd[ok]))
    mode = dec.groupby([df.chain_id[ok], df.vault[ok]]).agg(lambda s: s.value_counts().index[0])
    df = df.join(mode.rename("decimals"), on=["chain_id", "vault"])
    df["usd_per_raw"] = df.price_usd / np.power(10.0, df.decimals)
    df["net_usd"] = (df.aum_usd - df.held_by_vaults_usd.fillna(0)).clip(lower=0)
    df = df.sort_values(["chain_id", "vault", "day"], kind="stable").reset_index(drop=True)
    ev = df[df.source != "forward_fill"]
    pps = ev.total_assets.astype(float) / ev.total_supply.astype(float).where(ev.total_supply.astype(float) > 0)
    same = (ev.chain_id.values[1:] == ev.chain_id.values[:-1]) & (ev.vault.values[1:] == ev.vault.values[:-1])
    gap = np.diff(ev.day.values).astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        g = np.where(same, (pps.values[1:] / pps.values[:-1]) ** (1.0 / np.maximum(gap, 1)) - 1.0, np.nan)
    df["pps_daily_growth"] = np.nan
    df.loc[ev.index[1:], "pps_daily_growth"] = g
    df["phantom"] = (df.guard.notna() & (df.guard != "")) | (df.pps_daily_growth > MAX_DAILY_PPS)
    return df


def fee_changes(proto, cid):
    """{vault: sorted [(block, log_index, ts, field, value, event)]} from the vault's own fee and timelock events."""
    out = defaultdict(list)
    for line in grep_lines(DATA / f"vaults/{proto}/{cid}/actions.jsonl", FEE_EVENTS):
        r = json.loads(line)
        ev = r["event"]
        if ev not in FEE_EVENTS:
            continue
        v = r["vault"].lower()
        if r.get("emitter") and r["emitter"].lower() != v:
            continue
        f = r.get("fields") or r
        if ev == "SetFee":
            items = [("perf_fee", int(f["newFee"]) / 1e18)]
        elif ev == "SetPerformanceFee":
            items = [("perf_fee", int(f["newPerformanceFee"]) / 1e18)]
        elif ev == "SetManagementFee":
            items = [("mgmt_fee_annual", int(f["newManagementFee"]) / 1e18 * YEAR_S)]
        elif ev in ("SetFeeRecipient", "SetPerformanceFeeRecipient"):
            items = [("fee_recipient", (f.get("newFeeRecipient") or f.get("newPerformanceFeeRecipient")).lower())]
        elif ev == "SetManagementFeeRecipient":
            items = [("mgmt_fee_recipient", f["newManagementFeeRecipient"].lower())]
        elif ev == "SetTimelock":
            items = [("timelock_s", int(f["newTimelock"]))]
        else:
            items = [("tl:" + f["selector"].lower(), int(f["newDuration"]))]
        for field, val in items:
            out[v].append((r["block"], r["log_index"], r["timestamp"], field, val, ev))
    for v in out:
        out[v].sort(key=lambda x: (x[0], x[1]))
    return out


def euler_lending_changes(cid):
    """{vault: sorted changes} from the EVK governance log."""
    out = defaultdict(list)
    for r in jsonl(DATA / f"panel/euler_v2/{cid}/governance.jsonl"):
        v = r["vault"].lower()
        key = (r["block"], r["log_index"], r["timestamp"])
        if r["event"] == "EVaultCreated":
            out[v].append((*key, "perf_fee", EVK_DEFAULT_INTEREST_FEE, "EVaultCreated"))
        elif r["event"] == "GovSetInterestFee":
            out[v].append((*key, "perf_fee", int(r["newFee"]) / 1e4, "GovSetInterestFee"))
        elif r["event"] == "GovSetFeeReceiver":
            out[v].append((*key, "fee_recipient", r["newFeeReceiver"].lower(), "GovSetFeeReceiver"))
    for v in out:
        out[v].sort(key=lambda x: (x[0], x[1]))
    return out


def history_rows(cid, proto, kind, vault, changes, reg):
    """Fee state intervals of one vault."""
    is_v2 = kind == "morpho_v2"
    tl0 = (reg or {}).get("initial_timelock")
    state = {"perf_fee": 0.0, "mgmt_fee_annual": 0.0, "fee_recipient": ZERO,
             "mgmt_fee_recipient": ZERO if is_v2 else None,
             "timelock_s": 0 if is_v2 else (int(tl0) if tl0 is not None and kind != "euler_lending" else None)}
    tls = {}
    rows = []
    start = ((reg or {}).get("created_block"), 0, (reg or {}).get("created_ts"), "created")
    if changes and (start[0] is None or changes[0][0] < start[0]):
        start = (changes[0][0], changes[0][1], changes[0][2], changes[0][5])

    def snapshot(b, li, ts, ev):
        s = dict(state)
        if is_v2:
            s["timelock_s"] = tls.get(V2_CAP_SELECTOR, 0)
            s["timelock_fee_s"] = tls.get(V2_FEE_SELECTOR, 0)
            s["timelock_max_s"] = max(tls.values()) if tls else 0
        else:
            s["timelock_fee_s"] = 0 if s["timelock_s"] is not None else None
            s["timelock_max_s"] = s["timelock_s"]
        return {"chain_id": cid, "protocol": proto, "kind": kind, "vault": vault, "from_block": b,
                "from_log_index": li, "from_ts": ts, "event": ev, **s}

    rows.append(snapshot(*start))
    for b, li, ts, field, val, ev in changes:
        if field.startswith("tl:"):
            tls[field[3:]] = val
        else:
            state[field] = val
        snap = snapshot(b, li, ts, ev)
        same = all(snap[k] == rows[-1][k] for k in snap if k not in ("from_block", "from_log_index",
                                                                     "from_ts", "event"))
        if not same:
            rows.append(snap)
    for i, r in enumerate(rows):
        nxt = rows[i + 1] if i + 1 < len(rows) else None
        r["to_block"] = nxt["from_block"] if nxt else None
        r["to_log_index"] = nxt["from_log_index"] if nxt else None
        r["to_ts"] = nxt["from_ts"] if nxt else None
        r["from_date"] = day_str(r["from_ts"] // DAY) if r["from_ts"] is not None else None
    return rows


def stage_history():
    """fee_history.csv for every curated vault of E3 (allocator vaults with deposits and governed Euler lending vaults)."""
    reg = registry_vaults()
    e3 = pd.read_csv(DERIVED / "aum/vault_daily.csv", usecols=["protocol", "chain_id", "vault", "kind"],
                     dtype=str).drop_duplicates()
    lend = pd.read_csv(DERIVED / "aum/lending_daily.csv", usecols=["chain_id", "vault"], dtype=str).drop_duplicates()
    rows, counts = [], Counter()
    for proto, cid in PAIRS:
        ch = fee_changes(proto, cid)
        counts.update({f"{proto}:{cid}:{x[5]}": 1 for v in ch for x in ch[v]})
        sub = e3[(e3.protocol == proto) & (e3.chain_id == str(cid))]
        for v, kind in zip(sub.vault.str.lower(), sub.kind):
            rows += history_rows(cid, proto, kind, v, ch.get(v, []), reg.get((cid, v)))
    lreg = {}
    for cid in EULER_CHAINS:
        for r in jsonl(DATA / f"registry/markets/euler_v2/{cid}.jsonl"):
            if r.get("type") == "vault":
                lreg[(cid, r["vault"].lower())] = r
        ch = euler_lending_changes(cid)
        counts.update({f"euler_lending:{cid}:{x[5]}": 1 for v in ch for x in ch[v]})
        for v in lend[lend.chain_id == str(cid)].vault.str.lower():
            rows += history_rows(cid, "euler_v2", "euler_lending", v, ch.get(v, []), lreg.get((cid, v)))
    df = pd.DataFrame(rows)
    cols = ["chain_id", "protocol", "kind", "vault", "from_block", "from_log_index", "from_ts", "from_date",
            "to_block", "to_log_index", "to_ts", "event", "perf_fee", "mgmt_fee_annual", "fee_recipient",
            "mgmt_fee_recipient", "timelock_s", "timelock_fee_s", "timelock_max_s"]
    df = df[cols].sort_values(["chain_id", "vault", "from_block", "from_log_index"], kind="stable")
    write_csv(df, "fee_history.csv", {
        "what": "per vault, the fee state in force over [from, to): performance fee (Euler lending: "
                "interest fee), Vault V2 management fee per year, fee recipients, timelock",
        "source": "actions.jsonl SetFee, SetPerformanceFee, SetManagementFee, SetFeeRecipient, "
                  "SetPerformanceFeeRecipient, SetManagementFeeRecipient, SetTimelock, Increase/DecreaseTimelock; "
                  "Euler lending: panel governance.jsonl EVaultCreated, GovSetInterestFee, GovSetFeeReceiver",
        "conventions": {
            "initial_state": "fee 0 and recipient zero at creation (MetaMorpho, Moolah, Silo, Euler Earn, "
                             "Vault V2); Euler lending interest fee 0.1 (EVK DEFAULT_INTEREST_FEE) and "
                             "receiver zero",
            "timelock_s": "V1, Silo, Euler Earn: the vault timelock (registry initial_timelock, then "
                          "SetTimelock); Vault V2: the timelock of increaseAbsoluteCap; Moolah and Euler "
                          "lending: none",
            "timelock_fee_s": "timelock on fee changes: 0 where setFee is immediate (V1, Silo, Euler Earn); "
                              "Vault V2 setPerformanceFee timelock",
            "timelock_max_s": "Vault V2: the largest per-function timelock"},
        "event_counts": dict(sorted(counts.items())),
        "inputs": {"vault_curators": stamp(DATA / "registry/vault_curators.jsonl"),
                   "e3_vault_daily": stamp(DERIVED / "aum/vault_daily.csv")}})
    print(f"fee_history.csv: {len(df)} rows, {df.groupby(['chain_id', 'vault']).ngroups} vaults")


def load_history():
    df = pd.read_csv(OUT / "fee_history.csv", dtype={"vault": str, "fee_recipient": str,
                                                     "mgmt_fee_recipient": str})
    out = defaultdict(list)
    for r in df.itertuples(index=False):
        out[(int(r.chain_id), r.vault)].append(r)
    return out


def fee_on_days(hist, days):
    """Performance fee in force at the end of each UTC day."""
    ends = (np.asarray(days, dtype=np.int64) + 1) * DAY - 1
    fee = np.zeros(len(ends))
    for r in hist:
        if pd.isna(r.from_ts):
            continue
        to = r.to_ts if not pd.isna(r.to_ts) else math.inf
        fee[(ends >= r.from_ts) & (ends < to)] = r.perf_fee
    return fee


def key_of(block, log_index):
    return np.asarray(block, dtype=np.int64) * 100_000 + np.asarray(log_index, dtype=np.int64)


def accrual_rows(proto, cid):
    """Accruals of fees.csv that mint fee shares, all share and asset columns kept as exact integer strings."""
    path = DATA / f"vaults/{proto}/{cid}/fees.csv"
    head = list(pd.read_csv(path, nrows=0).columns)
    share_cols = [c for c in ("feeShares", "performanceFeeShares", "managementFeeShares", "protocolFeeShares")
                  if c in head]
    use = ["vault", "block", "log_index", "timestamp", "newTotalAssets"] + share_cols
    if "fee_recipient" in head:
        use.append("fee_recipient")
    parts = []
    for ch in pd.read_csv(path, usecols=use, dtype=str, chunksize=2_000_000):
        nz = np.zeros(len(ch), dtype=bool)
        for c in share_cols:
            s = ch[c].fillna("0")
            nz |= (s != "0") & (s != "")
        parts.append(ch[nz])
    df = pd.concat(parts, ignore_index=True)
    for c in ("feeShares", "performanceFeeShares", "managementFeeShares", "protocolFeeShares"):
        if c not in df:
            df[c] = "0"
        df[c] = df[c].fillna("0").replace("", "0")
    df["vault"] = df.vault.str.lower()
    df["block"] = df.block.astype(np.int64)
    df["log_index"] = df.log_index.astype(np.int64)
    df["timestamp"] = df.timestamp.astype(np.int64)
    df["perf_shares"] = [str(to_int(a) + to_int(b)) for a, b in zip(df.feeShares, df.performanceFeeShares)]
    df["mgmt_shares"] = df.managementFeeShares
    df["protocol_shares"] = df.protocolFeeShares
    return df.drop(columns=["feeShares", "performanceFeeShares", "managementFeeShares", "protocolFeeShares"])


def flow_deltas(proto, cid, vault):
    """(keys, signed share deltas as Python ints) of a vault's deposits and withdrawals."""
    p = DATA / f"vaults/{proto}/{cid}/flows/{vault}.csv"
    if not p.exists():
        return np.zeros(0, dtype=np.int64), []
    f = pd.read_csv(p, usecols=["block", "log_index", "kind", "shares"], dtype=str)
    if f.empty:
        return np.zeros(0, dtype=np.int64), []
    kind = f.kind.str.lower().values
    if not set(kind) <= {"deposit", "withdraw"}:
        raise ValueError(f"{p}: unexpected flow kinds {set(kind)}")
    sign = np.where(kind == "deposit", 1, -1)
    return key_of(f.block.astype(np.int64), f.log_index.astype(np.int64)), \
        [int(s) * int(g) for s, g in zip(f.shares, sign)]


def supply_before(fee_keys, fee_deltas, flow_keys, flow_deltas_):
    """Share supply just before each fee accrual."""
    keys = np.concatenate([np.asarray(flow_keys, dtype=np.int64), np.asarray(fee_keys, dtype=np.int64)])
    deltas = np.empty(len(keys), dtype=object)
    deltas[:len(flow_keys)] = flow_deltas_
    deltas[len(flow_keys):] = fee_deltas
    order = np.argsort(keys, kind="stable")
    after = np.cumsum(deltas[order])
    before = after - deltas[order]
    out = np.empty(len(keys), dtype=object)
    out[order] = before
    return out[len(flow_keys):]


def recipient_lookup(hist_rows, field):
    """(keys, values) of the recipient field over a vault's fee history."""
    ks, vs = [], []
    for r in hist_rows:
        if pd.isna(r.from_block):
            continue
        ks.append(int(r.from_block) * 100_000 + int(r.from_log_index))
        vs.append(getattr(r, field))
    return np.asarray(ks, dtype=np.int64), vs


def at_keys(lookup, keys, default):
    ks, vs = lookup
    if len(ks) == 0:
        return [default] * len(keys)
    idx = np.searchsorted(ks, keys, side="right") - 1
    return [vs[i] if i >= 0 else default for i in idx]


def allocator_income(proto, cid, hist, iv):
    """Every fee-minting accrual of one pair, valued in raw asset units."""
    t0 = time.time()
    df = accrual_rows(proto, cid)
    out = []
    for v, sub in df.groupby("vault", sort=True):
        sub = sub.sort_values(["block", "log_index"], kind="stable")
        keys = key_of(sub.block.values, sub.log_index.values)
        perf = [int(x) for x in sub.perf_shares]
        mgmt = [int(x) for x in sub.mgmt_shares]
        prot = [int(x) for x in sub.protocol_shares]
        tot = [a + b + c for a, b, c in zip(perf, mgmt, prot)]
        fk, fd = flow_deltas(proto, cid, v)
        before = supply_before(keys, tot, fk, fd)
        nta = [int(x) for x in sub.newTotalAssets]
        assets = [t * a / (s + t) if s + t > 0 else 0.0 for t, a, s in zip(tot, nta, before)]
        h = hist.get((cid, v), [])
        if "fee_recipient" in sub:
            rec = list(sub.fee_recipient.str.lower())
        else:
            rec = at_keys(recipient_lookup(h, "fee_recipient"), keys, ZERO)
        mrec = at_keys(recipient_lookup(h, "mgmt_fee_recipient"), keys, None)
        ents = [entity_at_block(iv.get((cid, v), []), b)[0] for b in sub.block]
        for i in range(len(sub)):
            share_cur = (perf[i] + mgmt[i]) / tot[i] if tot[i] else 0.0
            out.append((cid, proto, v, int(sub.block.iat[i]), int(sub.log_index.iat[i]),
                        int(sub.timestamp.iat[i]), str(nta[i]), str(perf[i]), str(mgmt[i]), str(prot[i]),
                        str(before[i]), assets[i], assets[i] * share_cur, rec[i], mrec[i], ents[i]))
    print(f"  {proto}:{cid}: {len(out)} fee accruals in {df.vault.nunique()} vaults, {time.time() - t0:.0f}s",
          flush=True)
    return out


def protocol_fee_shares(cid, vaults, block):
    """Protocol share of each lending vault's interest fees at the scan head."""
    slug = drpc_slug(cid)
    vaults = list(vaults)
    cfg = multicall.try_view(slug, vaults, "protocolConfigAddress()", ["address"], block=block)
    calls = [(c[0], evmabi.encode_call("protocolFeeConfig(address)", ["address"], [v]))
             for v, c in zip(vaults, cfg) if c is not None]
    res = multicall.aggregate3(slug, calls, block=block)
    out, j = {}, 0
    for v, c in zip(vaults, cfg):
        if c is None:
            out[v] = None
            continue
        ok, ret = res[j]
        j += 1
        out[v] = min(evmabi.decode(["address", "uint16"], ret)[1] / 1e4, 0.5) if ok and ret not in ("0x", "") else None
    return out


def lending_income(cid, hist, iv, pfs):
    """Interest fees of governed Euler lending vaults."""
    lend = pd.read_csv(DERIVED / "aum/lending_daily.csv", usecols=["chain_id", "vault"], dtype=str)
    vaults = sorted(set(lend[lend.chain_id == str(cid)].vault.str.lower()))
    out = []
    for v in vaults:
        p = DATA / f"panel/euler_v2/{cid}/{v}.csv"
        if not p.exists():
            continue
        f = pd.read_csv(p, dtype=str)
        acc = [int(x) for x in f.accumulatedFees]
        prev = [0] + acc[:-1]
        inc = [a - b if a >= b else a for a, b in zip(acc, prev)]
        keep = [i for i, x in enumerate(inc) if x > 0]
        if not keep:
            continue
        sub = f.iloc[keep]
        keys = key_of(sub.block.astype(np.int64), sub.log_index.astype(np.int64))
        rec = at_keys(recipient_lookup(hist.get((cid, v), []), "fee_recipient"), keys, ZERO)
        share = pfs.get(v)
        for j, i in enumerate(keep):
            num = int(f.cash.iat[i]) + int(f.totalBorrows.iat[i]) + EVK_VIRTUAL
            den = int(f.totalShares.iat[i]) + EVK_VIRTUAL
            a = inc[i] * num / den
            gov = 0.0 if rec[j] in (None, ZERO) or share is None else a * (1.0 - share)
            b = int(f.block.iat[i])
            out.append((cid, "euler_v2", v, b, int(f.log_index.iat[i]), int(f.timestamp.iat[i]),
                        str(num - EVK_VIRTUAL), str(inc[i]), "0", "0", str(den - EVK_VIRTUAL), a, gov, rec[j], None,
                        entity_at_block(iv.get((cid, v), []), b)[0]))
    print(f"  euler_lending:{cid}: {len(out)} fee increments in {len(vaults)} vaults", flush=True)
    return out


ACCRUAL_COLS = ["chain_id", "protocol", "vault", "block", "log_index", "timestamp", "new_total_assets",
                "perf_shares", "mgmt_shares", "protocol_shares", "supply_before", "fee_assets_total",
                "fee_assets_curator", "fee_recipient", "mgmt_fee_recipient", "curator_entity"]


def depeg_windows(events):
    """{(chain, vault): [(t0 day, end day)]} of third-party vaults present at t0 in a depeg-loss event."""
    out = defaultdict(list)
    for e in events:
        if e["type"] != "depeg_loss" or e.get("excluded"):
            continue
        for x in e["exposed_vaults"]:
            if x["group"] == "present_at_t0" and x.get("third_party"):
                out[(x["chain_id"], x["vault"].lower())].append((e["onset_ts"] // DAY, e["end_ts"] // DAY))
    return out


def stage_income(reuse=False):
    """Value every fee accrual; write fee_income.csv (vault-day), the vault and curator totals and the fee income of exposed vaults before each E2 event."""
    if reuse and (WORK / "accruals.pkl").exists():
        pf = rpickle("protocol_fee_share.pkl")
        write_income_outputs(rpickle("accruals.pkl"), pf["values"], pf["rpc_calls"])
        return
    hist = load_history()
    iv = curator_intervals()
    heads = e3_heads()
    rows = []
    pfs_all = {}
    rpc_before = rpc.calls_made()
    for proto, cid in PAIRS:
        rows += allocator_income(proto, cid, hist, iv)
    lend = pd.read_csv(DERIVED / "aum/lending_daily.csv", usecols=["chain_id", "vault"], dtype=str)
    for cid in EULER_CHAINS:
        vaults = sorted(set(lend[lend.chain_id == str(cid)].vault.str.lower()))
        pfs = protocol_fee_shares(cid, vaults, heads[f"euler_v2:{cid}"]["block"])
        pfs_all.update({(cid, v): s for v, s in pfs.items()})
        rows += lending_income(cid, hist, iv, pfs)
    acc = pd.DataFrame(rows, columns=ACCRUAL_COLS)
    acc["day"] = acc.timestamp // DAY
    wpickle("accruals.pkl", acc)
    wpickle("protocol_fee_share.pkl", {"values": pfs_all, "rpc_calls": rpc.calls_made() - rpc_before})
    print(f"accruals: {len(acc)} rows; RPC calls {rpc.calls_made() - rpc_before}", flush=True)
    write_income_outputs(acc, pfs_all, rpc.calls_made() - rpc_before)


def kind_map():
    e3 = pd.read_csv(DERIVED / "aum/vault_daily.csv", usecols=["chain_id", "vault", "kind", "asset_symbol"],
                     dtype={"vault": str, "kind": str, "asset_symbol": str}).drop_duplicates(["chain_id", "vault"])
    out = {(int(c), v.lower()): (k, s) for c, v, k, s in e3.itertuples(index=False)}
    lend = pd.read_csv(DERIVED / "aum/lending_daily.csv", usecols=["chain_id", "vault", "asset_symbol"],
                       dtype={"vault": str, "asset_symbol": str}).drop_duplicates(["chain_id", "vault"])
    out.update({(int(c), v.lower()): ("euler_lending", s) for c, v, s in lend.itertuples(index=False)})
    return out


def write_income_outputs(acc, pfs_all, rpc_calls):
    px = e3_prices()
    excl = excluded_vaults()
    kinds = kind_map()
    events = jsonl(EVENTS_DIR / "events.jsonl")
    dep = depeg_windows(events)
    key = px.set_index(["chain_id", "vault", "day"])[["usd_per_raw", "phantom", "asset_symbol"]]
    acc = acc.join(key, on=["chain_id", "vault", "day"])
    acc["phantom"] = acc.phantom.fillna(False).astype(bool)
    usd_tot = acc.fee_assets_total * acc.usd_per_raw
    usd_cur = acc.fee_assets_curator * acc.usd_per_raw
    acc["fee_usd_total"] = np.where(acc.phantom, 0.0, usd_tot)
    acc["fee_usd_curator"] = np.where(acc.phantom, 0.0, usd_cur)
    acc["fee_usd_curator_phantom"] = np.where(acc.phantom, usd_cur, 0.0)
    acc["priced"] = acc.usd_per_raw.notna()
    acc["excluded"] = [excl.get((c, v)) for c, v in zip(acc.chain_id, acc.vault)]
    acc["kind"] = [kinds.get((c, v), (None, None))[0] for c, v in zip(acc.chain_id, acc.vault)]
    post = np.zeros(len(acc), dtype=bool)
    for (c, v), idx in acc.groupby(["chain_id", "vault"]).indices.items():
        for d0, d1 in dep.get((c, v), ()):
            dd = acc.day.values[idx]
            post[idx[(dd >= d0) & (dd <= d1)]] = True
    acc["post_depeg_exposed"] = post
    wpickle("accruals_valued.pkl", acc)

    g = acc.groupby(["chain_id", "protocol", "kind", "vault", "day"], sort=True)
    daily = g.agg(accruals=("block", "size"), fee_assets_total=("fee_assets_total", "sum"),
                  fee_assets_curator=("fee_assets_curator", "sum"), fee_usd_total=("fee_usd_total", "sum"),
                  fee_usd_curator=("fee_usd_curator", "sum"),
                  fee_usd_curator_phantom=("fee_usd_curator_phantom", "sum"), priced=("priced", "all"),
                  phantom=("phantom", "any"), post_depeg_exposed=("post_depeg_exposed", "any"),
                  curator_entity=("curator_entity", "last"), fee_recipient=("fee_recipient", "last"),
                  excluded=("excluded", "last")).reset_index()
    sums = g[["perf_shares", "mgmt_shares", "protocol_shares"]].agg(lambda s: str(sum(int(x) for x in s)))
    daily = daily.merge(sums.reset_index(), on=["chain_id", "protocol", "kind", "vault", "day"])
    daily.loc[~daily.priced, ["fee_usd_total", "fee_usd_curator", "fee_usd_curator_phantom"]] = np.nan
    daily.insert(4, "date", [day_str(d) for d in daily.day])
    daily = daily.drop(columns=["day"])
    meta_common = {
        "inputs": {"e3_vault_daily": stamp(DERIVED / "aum/vault_daily.csv"),
                   "e3_lending_daily": stamp(DERIVED / "aum/lending_daily.csv"),
                   "vault_curators": stamp(DATA / "registry/vault_curators.jsonl"),
                   "e2_events": stamp(EVENTS_DIR / "events.jsonl"),
                   "fee_history": stamp(OUT / "fee_history.csv")},
        "valuation": "fee shares x newTotalAssets / (supply before the accrual + all shares minted in it); "
                     "supply replayed from flows/ deposit and withdraw shares and fees.csv fee shares; USD at "
                     "the vault's E3 price of that UTC day (peg family or own price) over 10^decimals. Euler "
                     "lending: accumulatedFees increments x (cash + totalBorrows + 1e6) / (totalShares + 1e6).",
        "phantom": "accruals on E3 guard days or on event days whose share price grew more than "
                   f"{MAX_DAILY_PPS} per day since the vault's previous event day compound on unrecoverable "
                   "debt; their fee shares are reported as fee_usd_curator_phantom and left out of the "
                   "income columns",
        "curator_share": "V1, V2 (performance and management), Moolah, Silo, Euler Earn feeShares go to "
                         "curator-set recipients; AmpleEarn protocolFeeShares go to the protocol; Euler lending "
                         "governor receives 1 - protocolFeeShare() (read at the E3 head block) when a fee "
                         "receiver is set, else nothing",
        "dRPC_calls": rpc_calls}
    write_csv(daily, "fee_income.csv", {
        "what": "fee income per vault and UTC day (only days with a fee-minting accrual)", **meta_common,
        "columns_note": {"fee_usd_total": "all fee shares minted (what depositors paid)",
                         "fee_usd_curator": "the curator's part",
                         "post_depeg_exposed": "day inside a depeg-loss event in which the vault was present "
                                               "at t0 (fees partly on unrecoverable debt)",
                         "excluded": "E3 exclusion reason (owner-funded vaults); such vaults leave every total"}})

    good = daily[daily.excluded.isna()]
    by_vault = good.groupby(["chain_id", "protocol", "kind", "vault"]).agg(
        fee_usd_total=("fee_usd_total", "sum"), fee_usd_curator=("fee_usd_curator", "sum"),
        fee_days=("date", "size"), first_fee_date=("date", "min"), last_fee_date=("date", "max"),
        unpriced_days=("priced", lambda s: int((~s).sum())),
        curator_entity=("curator_entity", lambda s: s.dropna().value_counts().index[0] if s.notna().any() else None)
    ).reset_index()
    gv = good.groupby(["chain_id", "vault"]).fee_usd_curator_phantom.sum()
    pd_ = good[good.post_depeg_exposed].groupby(["chain_id", "vault"]).fee_usd_curator.sum()
    by_vault["fee_usd_curator_phantom"] = [gv.get((c, v), 0.0) for c, v in zip(by_vault.chain_id, by_vault.vault)]
    by_vault["fee_usd_post_depeg_exposed"] = [pd_.get((c, v), 0.0) for c, v in zip(by_vault.chain_id, by_vault.vault)]
    by_vault["protocol_fee_share"] = [pfs_all.get((c, v)) for c, v in zip(by_vault.chain_id, by_vault.vault)]
    by_vault = by_vault.sort_values("fee_usd_curator", ascending=False)
    write_csv(by_vault, "fee_income_by_vault.csv", {
        "what": "whole-period fee income per vault (E3's owner-funded exclusions left out); curator_entity is "
                "the entity at most of the vault's fee days", **meta_common})

    names = curator_names()
    cur = good.dropna(subset=["curator_entity"])
    proto_grp = np.where(cur.kind == "euler_lending", "euler_v2_lending",
                         np.where(cur.kind == "euler_earn", "euler_v2_earn", cur.protocol))
    piv = cur.assign(group=proto_grp).pivot_table(index="curator_entity", columns="group",
                                                  values="fee_usd_curator", aggfunc="sum", fill_value=0.0)
    by_cur = cur.groupby("curator_entity").agg(fee_usd_curator=("fee_usd_curator", "sum"),
                                               fee_usd_total=("fee_usd_total", "sum"),
                                               vaults=("vault", "nunique")).join(piv.add_prefix("fee_usd_"))
    by_cur = by_cur.reset_index().rename(columns={"curator_entity": "entity"})
    by_cur.insert(1, "name", [names.get(e, e) for e in by_cur.entity])
    by_cur = by_cur.sort_values("fee_usd_curator", ascending=False)
    unattr = good[good.curator_entity.isna()]
    write_csv(by_cur, "fee_income_by_curator.csv", {
        "what": "whole-period fee income per curator entity (the entity holding the vault at each accrual "
                "block), split by protocol group", **meta_common,
        "unattributed_fee_usd_curator": float(unattr.fee_usd_curator.sum())})

    ev_rows = []
    for e in events:
        if e.get("excluded"):
            continue
        d0 = e["onset_ts"] // DAY
        for x in e["exposed_vaults"]:
            c, v = x["chain_id"], x["vault"].lower()
            m = (good.chain_id == c) & (good.vault == v)
            dd = good[m]
            days = dd.date.map(day_of)
            win = dd[(days >= d0 - 365) & (days < d0)]
            ev_rows.append({"event_id": e["event_id"], "type": e["type"], "t0": e["onset_utc"][:10],
                            "chain_id": c, "vault": v, "kind": x["kind"], "group": x["group"],
                            "third_party": x.get("third_party"), "curator_entity": x.get("curator"),
                            "exposure_t0_usd": x.get("exposure_t0_usd"),
                            "fee_usd_curator_365d": float(win.fee_usd_curator.sum()),
                            "fee_usd_total_365d": float(win.fee_usd_total.sum())})
    write_csv(pd.DataFrame(ev_rows), "fee_income_events.csv", {
        "what": "fee income of every exposed vault of every E2 event (not excluded) in the 365 days before t0",
        **meta_common})
    tot = good.groupby(np.where(good.kind == "euler_lending", "euler_v2_lending",
                                np.where(good.kind == "euler_earn", "euler_v2_earn", good.protocol)))
    print(tot[["fee_usd_curator", "fee_usd_total"]].sum())


D0 = dt.date(2024, 1, 1).toordinal() - dt.date(1970, 1, 1).toordinal()
D1 = dt.date(2026, 10, 8).toordinal() - dt.date(1970, 1, 1).toordinal()
NDAYS = D1 - D0 + 1
UNIT_KEYS = ("USD", "ETH", "BTC", "EUR", "other", "unknown", "none", "outside")


def ffill_days(days, values, before=0.0):
    """Day-end value on the grid D0..D1 from rows sorted by time (last row of each day carried forward; `before` until the first row)."""
    idx = np.searchsorted(np.asarray(days, dtype=np.int64), np.arange(D0, D1 + 1), side="right") - 1
    out = np.full(NDAYS, before, dtype=float)
    ok = idx >= 0
    if len(values):
        out[ok] = np.asarray(values, dtype=float)[idx[ok]]
    return out


def unit_key(unit):
    if unit in ("USD", "ETH", "BTC", "EUR"):
        return unit
    return "unknown" if unit is None else "other"


def e4_daily(cols):
    """Columns of E4's daily vault table (sample vaults only)."""
    df = pd.read_csv(E4_DIR / "vault_daily.csv", usecols=["chain_id", "vault", "date"] + list(cols),
                     dtype={"vault": str})
    df["day"] = (pd.to_datetime(df.date) - pd.Timestamp("1970-01-01")).dt.days.astype(np.int64)
    return df


def e4_sample():
    sc = pd.read_csv(E4_DIR / "vault_scorecard.csv", dtype={"vault": str, "curator_entity": str})
    return sc[sc.status == "in"].copy()


class ChainComposition:
    """Daily value of a chain's vaults by collateral unit, in raw loan units."""

    def __init__(self, cid, ctx, e3px):
        self.cid = cid
        self.ctx = ctx
        self.protos = [p for p, c in PAIRS if c == cid]
        self.pos = {}
        self.prices = defaultdict(list)
        self.wraps = defaultdict(lambda: defaultdict(list))
        self.e3 = {v: g for v, g in e3px[e3px.chain_id == cid].groupby("vault")}
        self.reg = {v for (c, v) in ctx.vault_reg if c == cid}
        self.silo_idle = set()
        self.memo = {}
        self.evault = {}
        self._read()

    def _read(self):
        cid = self.cid
        for proto in self.protos:
            d = DATA / f"vaults/{proto}/{cid}/positions"
            if not d.exists():
                continue
            for fn in sorted(os.listdir(d)):
                if not fn.endswith(".csv"):
                    continue
                v = fn[:-4].lower()
                if proto in ("morpho", "moolah"):
                    f = pd.read_csv(d / fn, usecols=["timestamp", "market_id", "assets", "shares_delta",
                                                      "vault_cum_shares"],
                                    dtype={"market_id": str, "assets": float, "shares_delta": float,
                                           "vault_cum_shares": float})
                    f = f.rename(columns={"market_id": "market", "vault_cum_shares": "cum"})
                    pr = f[(f.shares_delta != 0) & (f.assets > 0)]
                    pr = pr.assign(p=pr.assets / pr.shares_delta.abs())
                else:
                    col = "strategy" if proto == "euler_v2" else "market"
                    f = pd.read_csv(d / fn, usecols=["timestamp", col, "kind", "assets", "event_shares",
                                                      "cum_shares"],
                                    dtype={col: str, "kind": str, "assets": float, "event_shares": float,
                                           "cum_shares": float})
                    f = f.rename(columns={col: "market", "cum_shares": "cum"})
                    pr = f[f.kind.isin(["Deposit", "Withdraw"]) & (f.event_shares > 0) & (f.assets > 0)]
                    pr = pr.assign(p=pr.assets / pr.event_shares)
                if f.empty:
                    continue
                f["market"] = f.market.str.lower()
                f["day"] = f.timestamp // DAY
                hold = {}
                for m, g in f.groupby("market", sort=False):
                    hold[m] = (g.day.values, g.cum.values)
                self.pos[v] = (proto, hold)
                for m, g in pr.groupby(pr.market.str.lower(), sort=False):
                    self.prices[m].append((g.timestamp.values, g.p.values))
            if proto == "morpho" and (DATA / f"vaults/morpho/{cid}/adapter_shares.csv").exists():
                w = pd.read_csv(DATA / f"vaults/morpho/{cid}/adapter_shares.csv",
                                usecols=["vault", "adapter", "underlying_vault", "timestamp", "cum_shares"],
                                dtype={"vault": str, "adapter": str, "underlying_vault": str, "cum_shares": float})
                for (v, a, u), g in w.groupby(["vault", "adapter", "underlying_vault"]):
                    self.wraps[v.lower()][u.lower()].append((g.timestamp.values // DAY, g.cum_shares.values))
            if proto == "silo_v2":
                for r in jsonl(DATA / f"vaults/silo_v2/{cid}/vaults.jsonl"):
                    self.silo_idle.update(x.lower() for x in (r.get("idle_vaults") or []))
        if cid in EULER_CHAINS:
            dist = pd.read_csv(DATA / f"vaults/euler_v2/{cid}/borrow_distribution.csv",
                               usecols=["vault", "day", "totalBorrows", "collateral_vault", "attributed",
                                        "unattributed"],
                               dtype={"vault": str, "collateral_vault": str, "totalBorrows": float,
                                      "attributed": float, "unattributed": float})
            dist["d"] = (pd.to_datetime(dist.day) - pd.Timestamp("1970-01-01")).dt.days.astype(np.int64)
            for v, g in dist.groupby("vault"):
                self.evault[v.lower()] = g
        self.market_price = {}
        for m, parts in self.prices.items():
            ts = np.concatenate([p[0] for p in parts])
            pv = np.concatenate([p[1] for p in parts])
            o = np.argsort(ts, kind="stable")
            ts, pv = ts[o], pv[o]
            self.market_price[m] = ffill_days(ts // DAY, pv, before=pv[0])

    def evault_fractions(self, v):
        """{unit: daily fraction} of an EVault's lending by collateral unit; an EVault of the panel that never had borrows is all none."""
        if ("evault", v) in self.memo:
            return self.memo[("evault", v)]
        g = self.evault.get(v)
        out = {k: np.zeros(NDAYS) for k in UNIT_KEYS}
        if g is None:
            known = (DATA / f"panel/euler_v2/{self.cid}/{v}.csv").exists()
            out["none" if known else "outside"][:] = 1.0
            self.memo[("evault", v)] = out
            return out
        tb = g.groupby("d").totalBorrows.first()
        tbd = ffill_days(tb.index.values, tb.values)
        un = g.groupby("d").unattributed.sum()
        und = ffill_days(un.index.values, un.values)
        att = g[g.collateral_vault.notna() & (g.collateral_vault != "")]
        acc = np.zeros(NDAYS)
        days_with_rows = np.zeros(NDAYS, dtype=bool)
        days_with_rows[np.clip(g.d.values - D0, 0, NDAYS - 1)] = True
        for cv, h in att.groupby(att.collateral_vault.str.lower()):
            tok = self.ctx.euler_asset.get((self.cid, cv))
            u = unit_key(self.ctx.book.unit(self.cid, tok)) if tok else "unknown"
            s = h.groupby("d").attributed.sum()
            arr = np.zeros(NDAYS)
            i = s.index.values - D0
            ok = (i >= 0) & (i < NDAYS)
            arr[i[ok]] = s.values[ok]
            last = np.maximum.accumulate(np.where(days_with_rows, np.arange(NDAYS), -1))
            arr = np.where(last >= 0, arr[np.maximum(last, 0)], 0.0)
            out[u] += arr
            acc += arr
        out["unknown"] += und
        with np.errstate(divide="ignore", invalid="ignore"):
            for k in UNIT_KEYS:
                out[k] = np.where(tbd > 0, out[k] / tbd, 0.0)
        out["none"] = np.where(tbd > 0, 0.0, 1.0)
        self.memo[("evault", v)] = out
        return out

    def held_price(self, u):
        g = self.e3.get(u)
        if g is None:
            return None
        ta = g.total_assets.astype(float).values
        ts = g.total_supply.astype(float).values
        with np.errstate(divide="ignore", invalid="ignore"):
            p = np.where(ts > 0, ta / np.where(ts > 0, ts, 1), np.nan)
        ok = np.isfinite(p)
        return ffill_days(g.day.values[ok], p[ok])

    def market_unit(self, proto, m):
        if proto in ("morpho", "moolah"):
            r = self.ctx.markets.get((proto, self.cid, m))
            if r is None:
                return "outside"
            tok = r["collateral_token"].lower()
            return "none" if tok == ZERO else unit_key(self.ctx.book.unit(self.cid, tok))
        if proto == "silo_v2":
            if m in self.silo_idle:
                return "none"
            cfg = self.ctx.silo_cfg.get((self.cid, m))
            if cfg is None:
                return "outside"
            pair = self.ctx.silo_cfg.get((self.cid, cfg["paired_silo"].lower()))
            return unit_key(self.ctx.book.unit(self.cid, pair["token"].lower())) if pair else "outside"
        return "outside"

    def composition(self, v, stack=()):
        """{unit: daily value in raw loan units} of vault v."""
        if v in self.memo:
            return self.memo[v]
        out = {k: np.zeros(NDAYS) for k in UNIT_KEYS}
        if v in stack:
            return out
        stack = stack + (v,)
        proto, hold = self.pos.get(v, (None, {}))
        for m, (days, cum) in hold.items():
            sh = ffill_days(days, cum)
            if not sh.any():
                continue
            if m in self.reg and m != v:
                self._add_lookthrough(out, m, sh, stack)
                continue
            price = self.market_price.get(m)
            val = sh * (price if price is not None else 1.0)
            if proto == "euler_v2":
                for k, f in self.evault_fractions(m).items():
                    out[k] += val * f
            else:
                out[self.market_unit(proto, m)] += val
        for u, parts in self.wraps.get(v, {}).items():
            sh = np.zeros(NDAYS)
            for days, cum in parts:
                sh += ffill_days(days, cum)
            if sh.any():
                self._add_lookthrough(out, u, sh, stack)
        self.memo[v] = out
        return out

    def _add_lookthrough(self, out, u, shares, stack):
        price = self.held_price(u)
        if price is None:
            out["outside"] += shares
            return
        val = shares * price
        inner = self.composition(u, stack)
        tot = sum(inner.values())
        with np.errstate(divide="ignore", invalid="ignore"):
            for k in UNIT_KEYS:
                out[k] += np.where(tot > 0, val * inner[k] / np.where(tot > 0, tot, 1), 0.0)
        out["outside"] += np.where(tot > 0, 0.0, val)


def classify(shares, loan_unit):
    """Collateral class from mean collateral-unit shares; collateral of unknown unit takes the loan's unit (E4's worse case)."""
    s = dict(shares)
    if loan_unit in s:
        s[loan_unit] = s[loan_unit] + s.get("unknown", 0.0)
    else:
        s["other"] = s.get("other", 0.0) + s.get("unknown", 0.0)
    s.pop("unknown", None)
    tot = sum(s.values())
    if tot <= 0:
        return "none"
    best = max(s, key=s.get)
    if s[best] / tot < CLASS_THRESHOLD:
        return "mixed"
    return {"USD": "stable", "ETH": "eth", "BTC": "btc"}.get(best, "other")


def stage_classes():
    """classes.csv: collateral class of every E4 sample vault."""
    e4_in = e4_stamps()
    e4 = load_e4()
    ctx = e4.Ctx(EVENTS_DIR, tuple(e4.CHAINS))
    e4.infer_price_units(ctx.book, {cid: ctx.collateral_tokens(cid) for cid in ctx.cids}, ctx.events_dir)
    sample = e4_sample()
    act = e4_daily(["active"])
    act = act[act.active == 1]
    active_days = {(c, v.lower()): g.day.values for (c, v), g in act.groupby(["chain_id", "vault"])}
    px = e3_prices()
    rows = []
    for cid in sorted(sample.chain_id.unique()):
        t0 = time.time()
        comp = ChainComposition(int(cid), ctx, px)
        for r in sample[sample.chain_id == cid].itertuples(index=False):
            v = r.vault.lower()
            if r.kind == "euler_lending":
                fr = comp.evault_fractions(v)
                daily = {k: fr[k] for k in UNIT_KEYS}
            else:
                daily = comp.composition(v)
            d = active_days.get((cid, v), np.zeros(0, dtype=np.int64)) - D0
            d = d[(d >= 0) & (d < NDAYS)]
            tot = sum(daily[k][d] for k in UNIT_KEYS)
            coll = sum(daily[k][d] for k in UNIT_KEYS if k not in ("none", "outside"))
            okc = coll > 0
            oktot = tot > 0
            shares = {k: float(np.mean(daily[k][d][okc] / coll[okc])) if okc.any() else 0.0
                      for k in UNIT_KEYS if k not in ("none", "outside")}
            loan = r.loan_unit
            unpeg = sum(daily[k][d] for k in ("USD", "ETH", "BTC", "EUR", "other") if k != loan)
            rows.append({
                "chain_id": cid, "vault": v, "protocol": r.protocol, "kind": r.kind, "loan_unit": loan,
                "curator_entity": r.curator_entity,
                **{f"share_{k}": shares[k] for k in shares},
                "share_none_of_value": float(np.mean(daily["none"][d][oktot] / tot[oktot])) if oktot.any() else np.nan,
                "share_outside_of_value": float(np.mean(daily["outside"][d][oktot] / tot[oktot])) if oktot.any() else np.nan,
                "share_unpegged_of_value": float(np.mean(unpeg[oktot] / tot[oktot])) if oktot.any() else np.nan,
                "e4_share_unpegged_mean": r.share_unpegged_mean,
                "active_days": int(len(d)), "collateral_days": int(okc.sum()),
                "class": classify(shares, loan) if okc.any() else "none"})
        print(f"  chain {cid}: {int((sample.chain_id == cid).sum())} vaults, {time.time() - t0:.0f}s", flush=True)
    df = pd.DataFrame(rows)
    df["class_loan"] = df.loan_unit + ":" + df["class"]
    corr = df[df.loan_unit == "USD"][["share_unpegged_of_value", "e4_share_unpegged_mean"]].corr(method="spearman")
    check_e4_unchanged(e4_in)
    write_csv(df, "classes.csv", {
        "what": "collateral class of every E4 sample vault from its position-weighted collateral units over "
                "E4 active days",
        "method": ChainComposition.__doc__.strip() + " Shares are means over active days of the day's "
                  "collateral value by unit over the day's collateralized value. Class: the dominant unit, "
                  f"unknown units counted as the loan's unit, when it holds at least {CLASS_THRESHOLD} "
                  "(stable for USD, eth, btc, other for EUR and other assets), else mixed; none when the "
                  "vault never held collateralized markets on an active day.",
        "class_counts": df["class"].value_counts().to_dict(),
        "class_loan_counts": df.class_loan.value_counts().to_dict(),
        "check_vs_e4_share_unpegged_spearman_usd_vaults": float(corr.iloc[0, 1]),
        "inputs": {**e4_in,
                   "e3_vault_daily": stamp(DERIVED / "aum/vault_daily.csv")}})
    print(df["class"].value_counts().to_string())
    print(df.class_loan.value_counts().to_string())
    print("spearman vs E4 share_unpegged (USD vaults):", float(corr.iloc[0, 1]))


def e5_losses():
    """E5 realized losses per (event, vault) when data/metrics/e5/event_vault.csv exists."""
    if not E5_EVENT_VAULT.exists():
        return None
    df = pd.read_csv(E5_EVENT_VAULT, dtype={"vault": str})
    cols = {c.lower(): c for c in df.columns}

    def pick(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None
    lpd = pick("loss_per_dollar", "realized_loss_per_dollar", "loss_per_usd")
    lusd = pick("loss_usd", "realized_loss_usd", "loss")
    if lpd is None and lusd is None:
        raise ValueError(f"{E5_EVENT_VAULT}: no loss column among {list(df.columns)}")
    out = pd.DataFrame({"event_id": df[pick("event_id", "event")], "chain_id": df[pick("chain_id", "chain")],
                        "vault": df[pick("vault")].str.lower(),
                        "loss_per_dollar": df[lpd] if lpd else np.nan,
                        "loss_usd": df[lusd] if lusd else np.nan})
    cnt = pick("loss_usd_counted")
    out["loss_usd_counted"] = df[cnt] if cnt else out.loss_usd
    cur = pick("curator_entity", "curator")
    if cur:
        out["curator_entity"] = df[cur].where(df[cur].notna(), None)
    sd = pick("self_deposited")
    if sd:
        out = out[~df[sd].astype(str).str.lower().eq("true").values]
    return out[out.loss_usd.notna() | out.loss_per_dollar.notna()].reset_index(drop=True)


def e2_loss_proxy(px):
    """Per (event, vault) loss proxy from E2 for depeg-loss events."""
    val = px.set_index(["chain_id", "vault", "day"]).aum_usd
    rows = []
    for e in jsonl(EVENTS_DIR / "events.jsonl"):
        if e["type"] != "depeg_loss" or e.get("excluded"):
            continue
        depth = 1.0 - float(e["min_market_price"])
        d0 = e["onset_ts"] // DAY
        for x in e["exposed_vaults"]:
            if x["group"] != "present_at_t0" or x.get("self_deposited"):
                continue
            c, v = x["chain_id"], x["vault"].lower()
            base = val.get((c, v, d0 - 1))
            loss = (x.get("exposure_t0_usd") or 0.0) * depth
            rows.append({"event_id": e["event_id"], "chain_id": c, "vault": v, "loss_usd": loss,
                         "loss_usd_counted": (x.get("exposure_t0_usd_counted") or 0.0) * depth,
                         "loss_per_dollar": loss / base if base and base > 0 else np.nan,
                         "t0_day": d0, "curator_entity": x.get("curator")})
    return pd.DataFrame(rows)


def depositor_losses(px):
    """(per event-vault loss table, source label)."""
    e5 = e5_losses()
    if e5 is not None:
        return e5, "e5"
    return e2_loss_proxy(px), "e2_proxy"


def vault_loss_per_dollar(sample, losses):
    """Sum over depeg-loss events of a vault's loss per dollar; 0 for a vault active at some depeg-loss t0 without a loss, NaN."""
    t0s = sorted({e["onset_ts"] // DAY for e in jsonl(EVENTS_DIR / "events.jsonl")
                  if e["type"] == "depeg_loss" and not e.get("excluded")})
    tot = losses.groupby(["chain_id", "vault"]).loss_per_dollar.sum(min_count=1)
    out = []
    for r in sample.itertuples(index=False):
        a, b = day_of(r.first_active), day_of(r.last_active)
        k = (r.chain_id, r.vault.lower())
        if k in tot.index and not pd.isna(tot[k]):
            out.append(float(tot[k]) if np.isfinite(tot[k]) else np.nan)
        else:
            out.append(0.0 if any(a <= t <= b for t in t0s) else np.nan)
    return np.array(out)


def rank(x):
    from scipy.stats import rankdata
    return rankdata(x)


def cluster_shuffle(y, codes, rng):
    """E4's clustered permutation."""
    k = codes.max() + 1
    cnt = np.bincount(codes, minlength=k)
    means = np.bincount(codes, weights=y, minlength=k) / np.maximum(cnt, 1)
    dev = y - means[codes]
    order = np.lexsort((rng.random(len(codes)), codes))
    base = np.argsort(codes, kind="stable")
    dshuf = np.empty_like(dev)
    dshuf[base] = dev[order]
    present = np.flatnonzero(cnt > 0)
    perm_means = means.copy()
    perm_means[present] = means[rng.permutation(present)]
    return perm_means[codes] + dshuf


def cluster_codes(entities, vaults):
    """Curator entity, or the vault itself when unattributed."""
    lab = [e if isinstance(e, str) and e else "vault:" + v for e, v in zip(entities, vaults)]
    return pd.factorize(pd.Series(lab))[0]


def spearman_r(rx, ry):
    if len(rx) < 3 or np.std(rx) == 0 or np.std(ry) == 0:
        return np.nan
    return float(np.corrcoef(rx, ry)[0, 1])


def within_class_test(df, xcol, ycol, stat, rng, nperm=N_PERM, min_n=MIN_CLASS_N):
    """Pooled within-class statistic with a clustered permutation p."""
    parts = []
    for cls, g in df.groupby("class"):
        if cls not in WITHIN_CLASSES and cls != "all":
            continue
        g = g[np.isfinite(g[xcol]) & np.isfinite(g[ycol])]
        if len(g) < min_n:
            continue
        x = g[xcol].values.astype(float)
        y = g[ycol].values.astype(float)
        codes = cluster_codes(g.curator_entity.values, g.vault.values)
        val = stat(x, y)
        if not np.isfinite(val):
            continue
        parts.append((cls, x, y, codes, val))
    if not parts:
        return {"pooled": None, "by_class": {}}
    w = np.array([len(p[1]) for p in parts], dtype=float)
    obs = float(np.average([p[4] for p in parts], weights=w))
    hits = 0
    for _ in range(nperm):
        vals = [stat(x, cluster_shuffle(y, codes, rng)) for _c, x, y, codes, _v in parts]
        vals = np.nan_to_num(np.array(vals, dtype=float))
        if abs(np.average(vals, weights=w)) >= abs(obs) - 1e-12:
            hits += 1
    return {"pooled": obs, "p_perm_clustered": (hits + 1) / (nperm + 1), "n": int(w.sum()),
            "classes_used": [p[0] for p in parts],
            "by_class": {p[0]: {"value": p[4], "n": len(p[1]), "clusters": int(p[3].max() + 1)} for p in parts}}


def stat_spearman(x, y):
    return spearman_r(rank(x), rank(y))


def stat_group(x, y):
    """Rank-biserial correlation of y between fee-charging (x > 0) and zero-fee (x == 0) vaults."""
    a, b = y[x > 0], y[x == 0]
    if len(a) < 3 or len(b) < 3:
        return np.nan
    r = rank(np.concatenate([a, b]))
    u = r[:len(a)].sum() - len(a) * (len(a) + 1) / 2
    return float(2 * u / (len(a) * len(b)) - 1)


def group_medians(df, ycol):
    out = {}
    for name, g in (("zero_fee", df[df.fee_rate == 0]), ("fee_charging", df[df.fee_rate > 0])):
        y = g[ycol].dropna()
        out[name] = {"n": int(len(y)), "median": float(y.median()) if len(y) else None}
    return out


def contrast_within_curator(df, ycol, rng, nperm=N_PERM):
    """Contrast 2: in each curator x class cell with at least two fee levels, the median metric of the vaults at the cell's hig."""
    cells = []
    for (ent, cls), g in df[df.curator_entity.notna() & df["class"].isin(WITHIN_CLASSES)].groupby(
            ["curator_entity", "class"]):
        g = g[np.isfinite(g[ycol]) & np.isfinite(g.fee_rate)]
        if g.fee_rate.nunique() < 2:
            continue
        cells.append((ent, cls, g.fee_rate.values.astype(float), g[ycol].values.astype(float)))

    def diff(fee, y):
        lo, hi = fee.min(), fee.max()
        return float(np.median(y[fee == hi]) - np.median(y[fee == lo]))
    if not cells:
        return {"cells": 0}
    obs_cells = [diff(f, y) for _e, _c, f, y in cells]
    obs = float(np.mean(obs_cells))
    hits = 0
    for _ in range(nperm):
        v = np.mean([diff(rng.permutation(f), y) for _e, _c, f, y in cells])
        if abs(v) >= abs(obs) - 1e-12:
            hits += 1
    pos = int(sum(d > 0 for d in obs_cells))
    neg = int(sum(d < 0 for d in obs_cells))
    return {"mean_high_minus_low": obs, "p_perm_within_cell": (hits + 1) / (nperm + 1), "cells": len(cells),
            "curators": len({c[0] for c in cells}), "vaults": int(sum(len(c[2]) for c in cells)),
            "cells_high_greater": pos, "cells_high_smaller": neg,
            "median_cell_difference": float(np.median(obs_cells)),
            "by_cell": [{"curator": e, "class": c, "n": len(f), "fee_low": float(f.min()), "fee_high": float(f.max()),
                         "high_minus_low": d} for (e, c, f, _y), d in zip(cells, obs_cells)]}


def fee_change_events(hist, sample_keys):
    """Contrast 3 candidates."""
    out = []
    for key, rows in hist.items():
        if key not in sample_keys:
            continue
        prev = None
        changes = []
        for r in rows:
            if prev is not None and r.perf_fee != prev and r.event in ("SetFee", "SetPerformanceFee",
                                                                      "GovSetInterestFee"):
                changes.append((int(r.from_ts) // DAY, prev, r.perf_fee))
            prev = r.perf_fee
        for i, (d, old, new) in enumerate(changes):
            near = [c for j, c in enumerate(changes) if j != i and abs(c[0] - d) <= WINDOW_FEE_CHANGE]
            out.append({"chain_id": key[0], "vault": key[1], "day": d, "fee_old": old, "fee_new": new,
                        "clean": not near})
    return out


def contrast_fee_changes(events, daily, ent_of, rng, tl_cols=None, nperm=N_PERM):
    """Contrast 3: tail loss (`tl_cols` of E4's daily table) and liquidity in the 60 days after a fee change minus the 60 days before, against the fee change."""
    tl_cols = tl_cols or TL_MAIN
    rows = []
    for ev in events:
        if not ev["clean"]:
            continue
        g = daily.get((ev["chain_id"], ev["vault"]))
        if g is None:
            continue
        d = ev["day"]
        b = g[(g.day >= d - WINDOW_FEE_CHANGE) & (g.day < d) & (g.active == 1)]
        a = g[(g.day > d) & (g.day <= d + WINDOW_FEE_CHANGE) & (g.active == 1)]
        if len(b) < MIN_WINDOW_ACTIVE or len(a) < MIN_WINDOW_ACTIVE:
            continue
        r = {**ev, "curator_entity": ent_of.get((ev["chain_id"], ev["vault"])), "d_fee": ev["fee_new"] - ev["fee_old"]}
        for c in tl_cols:
            r["d_" + c] = float(a[c].mean() - b[c].mean())
        r["d_L_mean"] = float(a.L.mean() - b.L.mean())
        r["d_L_p10"] = float(np.nanpercentile(a.L, 10) - np.nanpercentile(b.L, 10))
        rows.append(r)
    df = pd.DataFrame(rows)
    res = {"events_clean_with_windows": int(len(df)), "events_total": len(events),
           "events_not_clean": int(sum(not e["clean"] for e in events))}
    if len(df) < 5:
        return res, df
    codes = cluster_codes(df.curator_entity.values, df.vault.values)
    for c in ["d_" + t for t in tl_cols] + ["d_L_mean", "d_L_p10"]:
        y = df[c].values
        x = df.d_fee.values
        obs = stat_spearman(x, y)
        inc, dec = y[x > 0], y[x < 0]
        diff = float(np.mean(inc) - np.mean(dec)) if len(inc) and len(dec) else np.nan
        hits = hits_d = 0
        for _ in range(nperm):
            xs = cluster_shuffle(x, codes, rng)
            if abs(stat_spearman(xs, y)) >= abs(obs) - 1e-12:
                hits += 1
            if np.isfinite(diff):
                lab = cluster_shuffle((x > 0).astype(float), codes, rng)
                thr = np.sort(lab)[::-1][len(inc) - 1] if len(inc) else np.inf
                pi = lab >= thr
                if pi.sum() and (~pi).sum():
                    if abs(np.mean(y[pi]) - np.mean(y[~pi])) >= abs(diff) - 1e-12:
                        hits_d += 1
        res[c] = {"spearman_dfee": obs, "p_perm_clustered": (hits + 1) / (nperm + 1),
                  "mean_after_increase": float(np.mean(inc)) if len(inc) else None,
                  "mean_after_decrease": float(np.mean(dec)) if len(dec) else None,
                  "increase_minus_decrease": diff,
                  "p_perm_clustered_diff": (hits_d + 1) / (nperm + 1) if np.isfinite(diff) else None,
                  "n_increase": int(len(inc)), "n_decrease": int(len(dec))}
    return res, df


def contrast_across_protocols(df, ycol, rng, nperm=N_PERM):
    """Contrast 4: for a curator active on several protocols within one class, the metric on the protocol where its median fee."""
    cells = []
    for (ent, cls), g in df[df.curator_entity.notna() & df["class"].isin(WITHIN_CLASSES)].groupby(
            ["curator_entity", "class"]):
        g = g[np.isfinite(g[ycol])]
        per = g.groupby("protocol").agg(fee=("fee_rate", "median"), y=(ycol, "median"), n=("vault", "size"))
        if len(per) < 2:
            continue
        cells.append((ent, cls, per.index.tolist(), per.fee.values, per.y.values, per.n.values))

    def stat(fee, y):
        vals = []
        for i in range(len(fee)):
            for j in range(i + 1, len(fee)):
                if fee[i] == fee[j]:
                    continue
                hi, lo = (i, j) if fee[i] > fee[j] else (j, i)
                vals.append(y[hi] - y[lo])
        return vals
    pairs = [v for c in cells for v in stat(c[3], c[4])]
    if not pairs:
        return {"cells": len(cells), "pairs": 0,
                "by_cell": [{"curator": e, "class": c, "protocols": ps, "fee_median": list(map(float, f)),
                             "metric_median": list(map(float, y)), "n": list(map(int, n))}
                            for e, c, ps, f, y, n in cells]}
    obs = float(np.mean(pairs))
    hits = 0
    for _ in range(nperm):
        v = [x for c in cells for x in stat(rng.permutation(c[3]), c[4])]
        if v and abs(np.mean(v)) >= abs(obs) - 1e-12:
            hits += 1
    return {"mean_high_fee_minus_low_fee": obs, "p_perm_within_cell": (hits + 1) / (nperm + 1),
            "cells": len(cells), "pairs": len(pairs), "pairs_high_greater": int(sum(p > 0 for p in pairs)),
            "pairs_high_smaller": int(sum(p < 0 for p in pairs)),
            "by_cell": [{"curator": e, "class": c, "protocols": ps, "fee_median": list(map(float, f)),
                         "metric_median": list(map(float, y)), "n": list(map(int, n))}
                        for e, c, ps, f, y, n in cells]}


def vault_table():
    """E4 sample vaults with fee rate (mean performance fee over active days from fee_history), collateral class and event loss per dollar."""
    sample = e4_sample()
    sample["vault"] = sample.vault.str.lower()
    cls = pd.read_csv(OUT / "classes.csv", dtype={"vault": str})
    sample = sample.merge(cls[["chain_id", "vault", "class", "class_loan", "share_unpegged_of_value",
                               "share_USD", "share_ETH", "share_BTC"]], on=["chain_id", "vault"], how="left")
    hist = load_history()
    act = e4_daily(["active"])
    act = act[act.active == 1]
    days = {(c, v.lower()): g.day.values for (c, v), g in act.groupby(["chain_id", "vault"])}
    sample["fee_rate"] = [float(np.mean(fee_on_days(hist.get((c, v), []), days.get((c, v), np.zeros(0)))))
                          if len(days.get((c, v), ())) else np.nan
                          for c, v in zip(sample.chain_id, sample.vault)]
    px = e3_prices()
    losses, src = depositor_losses(px)
    sample["loss_per_dollar"] = vault_loss_per_dollar(sample, losses)
    return sample, hist, losses, src, px


def metric_contrasts(df, metrics, rng):
    """Contrasts 1, 1 (group), 2 and 4 for each (column, label) in metrics."""
    out = {"contrast1_cross_section": {}, "contrast1_group": {}, "contrast2_within_curator": {},
           "contrast4_across_protocols": {}}
    nocls = df.assign(**{"class": "all"})
    for col, label in metrics:
        t0 = time.time()
        out["contrast1_cross_section"][col] = {
            "label": label,
            "within_class": within_class_test(df, "fee_rate", col, stat_spearman, rng),
            "no_control": within_class_test(nocls, "fee_rate", col, stat_spearman, rng)}
        out["contrast1_group"][col] = {
            "label": label, "effect": "rank-biserial: P(fee-charging > zero-fee) - P(<)",
            "within_class": within_class_test(df, "fee_rate", col, stat_group, rng),
            "no_control": within_class_test(nocls, "fee_rate", col, stat_group, rng),
            "medians_no_control": group_medians(df, col),
            "medians_by_class": {c: group_medians(g, col) for c, g in df.groupby("class")}}
        out["contrast2_within_curator"][col] = contrast_within_curator(df, col, rng)
        out["contrast4_across_protocols"][col] = contrast_across_protocols(df, col, rng)
        print(f"  {col}: {time.time() - t0:.0f}s", flush=True)
    return out


def stage_contrasts():
    """contrasts.json: the four contrasts on the oracle-aware tail loss (E4's tl_*), with the first-pass tail loss (tl_v1_*) in."""
    e4_in = e4_stamps()
    rng = np.random.default_rng(SEED)
    df, hist, losses, src, _px = vault_table()
    wpickle("vault_table.pkl", df)
    main = [m for m in METRICS if m[0] in df.columns]
    v1 = [(c, "first-pass " + c[6:].replace("_", " ")) for c in df.columns if c.startswith("tl_v1_")]
    sens = [(c, c.replace("_", " ")) for c in df.columns
            if c.startswith("tl_") and not c.startswith("tl_v1_") and c not in dict(main)]
    res = {"sample": {"vaults": int(len(df)), "class_counts": df["class"].value_counts().to_dict(),
                      "zero_fee_vaults": int((df.fee_rate == 0).sum()),
                      "fee_rate_vs_e4_perf_fee_mean_max_abs_diff": float((df.fee_rate - df.perf_fee_mean).abs().max()),
                      "loss_source": src},
           **metric_contrasts(df, main, rng)}
    res["artefact_check"] = {
        "question": "first pass: zero-fee vaults showed lower tail loss because they hold volatile collateral",
        "share_unpegged_of_value": {"medians_no_control": group_medians(df, "share_unpegged_of_value"),
                                    "medians_by_class": {c: group_medians(g, "share_unpegged_of_value")
                                                         for c, g in df.groupby("class")}},
        "zero_fee_by_class": pd.crosstab(df["class"], df.fee_rate == 0).rename(
            columns={True: "zero_fee", False: "fee_charging"}).to_dict(orient="index"),
        "tl_headline_rank_biserial": {
            "no_control": res["contrast1_group"]["tl_headline"]["no_control"],
            "within_class": res["contrast1_group"]["tl_headline"]["within_class"]},
        "tl_d1000_rank_biserial": {
            "no_control": res["contrast1_group"]["tl_d1000"]["no_control"],
            "within_class": res["contrast1_group"]["tl_d1000"]["within_class"]}}
    ent_of = {(c, v): e for c, v, e in zip(df.chain_id, df.vault, df.curator_entity)}
    daily_cols = list(pd.read_csv(E4_DIR / "vault_daily.csv", nrows=0).columns)
    v1_daily = [c for c in TL_V1 if c in daily_cols]
    daily = e4_daily(["active", "L", *TL_MAIN, *v1_daily])
    daily["vault"] = daily.vault.str.lower()
    dmap = {k: g for k, g in daily.groupby(["chain_id", "vault"])}
    evs = fee_change_events(hist, set(ent_of))
    c3, c3df = contrast_fee_changes(evs, dmap, ent_of, rng)
    res["contrast3_within_vault"] = c3
    if sens:
        res["tail_loss_sensitivity"] = {"what": "E4's other oracle-aware tail-loss columns", **metric_contrasts(df, sens, rng)}
    if v1:
        res["appendix_v1"] = {"what": "the first-pass tail loss (E4 tl_v1_*: closed form, oracle type acting "
                                      "only through liquidation friction)", **metric_contrasts(df, v1, rng)}
        if len(v1_daily) == len(TL_V1):
            res["appendix_v1"]["contrast3_within_vault"] = contrast_fee_changes(evs, dmap, ent_of, rng, TL_V1)[0]
    check_e4_unchanged(e4_in)
    if len(c3df):
        write_csv(c3df, "fee_changes.csv", {"what": "contrast 3: clean performance-fee changes of E4 sample "
                                                    "vaults with 30 active days on each side; metric after "
                                                    "minus before over 60-day windows"})
    params = {"class_threshold": CLASS_THRESHOLD, "min_class_n": MIN_CLASS_N, "within_classes": list(WITHIN_CLASSES),
              "n_perm": N_PERM, "seed": SEED,
              "window_days": WINDOW_FEE_CHANGE, "min_window_active": MIN_WINDOW_ACTIVE}
    write_json(res, "contrasts.json", {
        "what": "the four fee contrasts of docs/experiment.md E6, within collateral class, with permutation "
                "p-values clustered by curator",
        "method": {
            "fee_rate": "mean over the vault's E4 active days of the performance fee in force at day end "
                        "(fee_history.csv; Euler lending: the interest fee)",
            "contrast1": "per class with at least min_class_n vaults, Spearman of fee rate with each metric; "
                         "pooled = class-size-weighted mean; null: metric shuffled within class by curator "
                         "(curator means across curators, deviations within curators, as in E4)",
            "contrast1_group": "rank-biserial of each metric between fee-charging and zero-fee vaults, same "
                               "pooling and null",
            "contrast2": "curator x class cells with two or more fee levels: median metric at the highest fee "
                         "minus at the lowest; mean over cells; null: fees shuffled within cells",
            "contrast3": "performance-fee changes with no other change within the window; after minus "
                         "before of daily tail loss and L over active days; Spearman with the fee change and "
                         "increase minus decrease means; null: clustered shuffle of the fee change",
            "contrast4": "curator x class cells on two or more protocols: metric median of the protocol with "
                         "the higher median fee minus the lower, over protocol pairs; null: protocol fee "
                         "medians permuted within cells",
            "tail_loss": "E4's oracle-aware tail loss (tl_d0150 to tl_d1000 and tl_headline) in the main blocks; "
                         "the first-pass closed form (tl_v1_*) in appendix_v1",
            "loss_per_dollar": "sum over depeg-loss events of the vault's loss per dollar (" + src + "); 0 "
                               "when a depeg-loss t0 falls in its active period without loss, NaN otherwise"},
        "parameters": params,
        "inputs": {**e4_in,
                   "classes": stamp(OUT / "classes.csv"), "fee_history": stamp(OUT / "fee_history.csv"),
                   "losses": stamp(E5_EVENT_VAULT) if src == "e5" else stamp(EVENTS_DIR / "events.jsonl")}})


def ols_cluster(y, X, groups):
    """OLS with CR1 cluster-robust standard errors."""
    y = np.asarray(y, float)
    X = np.asarray(X, float)
    n, k = X.shape
    xtx_inv = np.linalg.pinv(X.T @ X)
    beta = xtx_inv @ X.T @ y
    e = y - X @ beta
    codes, uniq = pd.factorize(pd.Series(groups))
    G = len(uniq)
    score = np.zeros((G, k))
    np.add.at(score, codes, X * e[:, None])
    meat = score.T @ score
    c = G / (G - 1) * (n - 1) / (n - k)
    V = c * xtx_inv @ meat @ xtx_inv
    return beta, np.sqrt(np.diag(V)), e, G


def demean(a, codes):
    """Subtract group means (the within transformation of a fixed effect)."""
    a = np.asarray(a, float)
    cnt = np.bincount(codes)
    if a.ndim == 1:
        return a - (np.bincount(codes, weights=a) / cnt)[codes]
    return np.column_stack([demean(a[:, j], codes) for j in range(a.shape[1])])


def dummies(labels):
    codes, uniq = pd.factorize(pd.Series(labels))
    D = np.zeros((len(codes), len(uniq)))
    D[np.arange(len(codes)), codes] = 1.0
    return D


MONDAY0 = dt.date(2024, 1, 1).toordinal() - dt.date(1970, 1, 1).toordinal()


def weekly_panel(tl_col="tl_headline"):
    """Vault-weeks of E4 sample allocator vaults."""
    sample = e4_sample()
    sample = sample[sample.kind != "euler_lending"]
    sample["vault"] = sample.vault.str.lower()
    daily = e4_daily(["active", "value_usd", "vault_apy", tl_col])
    daily["vault"] = daily.vault.str.lower()
    daily = daily.merge(sample[["chain_id", "vault"]], on=["chain_id", "vault"])
    daily["week"] = (daily.day - MONDAY0) // 7
    daily["apy"] = daily.vault_apy.clip(upper=APY_CAP)
    act = daily[daily.active == 1]
    wk = act.groupby(["chain_id", "vault", "week"]).agg(apy=("apy", "mean"), tl=(tl_col, "mean"),
                                                        active_days=("day", "size"))
    last = daily.sort_values("day").groupby(["chain_id", "vault", "week"]).agg(
        value_end=("value_usd", "last"), active_end=("active", "last"))
    wk = wk.join(last, how="inner").reset_index()
    px = e3_prices().set_index(["chain_id", "vault", "day"]).usd_per_raw
    proto = {(c, v): p for c, v, p in zip(sample.chain_id, sample.vault, sample.protocol)}
    flows = []
    for (c, v), pr in proto.items():
        f = DATA / f"vaults/{pr}/{c}/flows/{v}.csv"
        if not f.exists():
            continue
        g = pd.read_csv(f, usecols=["timestamp", "kind", "assets"], dtype={"kind": str, "assets": float})
        if g.empty:
            continue
        g["day"] = g.timestamp // DAY
        g["sign"] = np.where(g.kind.str.lower() == "deposit", 1.0, -1.0)
        g["usd"] = g.sign * g.assets * px.reindex(list(zip([c] * len(g), [v] * len(g), g.day))).values
        g["week"] = (g.day - MONDAY0) // 7
        w = g.groupby("week").agg(net_usd=("usd", "sum"), unpriced=("usd", lambda x: int(x.isna().sum())))
        w["chain_id"], w["vault"] = c, v
        flows.append(w.reset_index())
    fl = pd.concat(flows, ignore_index=True)
    prev = wk.rename(columns={"week": "week_prev"})
    prev["week"] = prev.week_prev + 1
    panel = prev.merge(fl, on=["chain_id", "vault", "week"], how="left")
    panel["net_usd"] = panel.net_usd.fillna(0.0)
    panel = panel[(panel.active_days >= 4) & (panel.active_end == 1) & (panel.value_end >= 10_000)
                  & np.isfinite(panel.apy) & np.isfinite(panel.tl) & (panel.week <= panel.week_prev.max())]
    panel = panel.merge(sample[["chain_id", "vault", "curator_entity", "protocol", "kind"]],
                        on=["chain_id", "vault"])
    panel["cluster"] = [e if isinstance(e, str) else "vault:" + v for e, v in zip(panel.curator_entity, panel.vault)]
    panel["flow_rate"] = panel.net_usd / panel.value_end
    lo, hi = panel.flow_rate.quantile([0.01, 0.99])
    panel["flow_rate_w"] = panel.flow_rate.clip(lo, hi)
    return panel.reset_index(drop=True), (float(lo), float(hi))


def flow_models(panel, rng, nperm=2000):
    """Spec A: flow ~ APY + tail loss + curator FE (the spec)."""
    import statsmodels.api as sm
    y = panel.flow_rate_w.values
    X = panel[["apy", "tl"]].values
    cl = panel.cluster.values
    fe_c = pd.factorize(panel.cluster)[0]
    out = {}
    yA, XA = demean(y, fe_c), demean(X, fe_c)
    b, se, _e, G = ols_cluster(yA, XA, cl)
    n, k = XA.shape
    adj = np.sqrt((n - k) / (n - k - (fe_c.max() + 1)))
    se = se * adj
    out["A_curator_fe"] = {"coef": {"apy": b[0], "tail_loss": b[1]}, "se_cluster": {"apy": se[0], "tail_loss": se[1]},
                           "t": {"apy": b[0] / se[0], "tail_loss": b[1] / se[1]}, "n": int(n), "clusters": int(G),
                           "fixed_effects": int(fe_c.max() + 1)}
    D = dummies(panel.cluster)
    m = sm.OLS(y, np.column_stack([X, D])).fit(cov_type="cluster", cov_kwds={"groups": fe_c})
    out["A_statsmodels_dummies"] = {"coef": {"apy": float(m.params[0]), "tail_loss": float(m.params[1])},
                                    "se_cluster": {"apy": float(m.bse[0]), "tail_loss": float(m.bse[1])},
                                    "p": {"apy": float(m.pvalues[0]), "tail_loss": float(m.pvalues[1])}}
    perm_b = np.zeros((nperm, 2))
    idx_by = [np.flatnonzero(fe_c == g) for g in range(fe_c.max() + 1)]
    for i in range(nperm):
        Xp = X.copy()
        for ix in idx_by:
            Xp[ix, 0] = X[rng.permutation(ix), 0]
        bp = np.linalg.lstsq(demean(Xp, fe_c), yA, rcond=None)[0]
        perm_b[i] = bp
    if nperm:
        out["A_permutation_apy_within_curator"] = {
            "n_perm": nperm, "p_apy": float((np.sum(np.abs(perm_b[:, 0]) >= abs(b[0]) - 1e-15) + 1) / (nperm + 1)),
            "perm_apy_coef_mean": float(perm_b[:, 0].mean()), "perm_apy_coef_sd": float(perm_b[:, 0].std()),
            "perm_apy_coef_q025_q975": [float(x) for x in np.percentile(perm_b[:, 0], [2.5, 97.5])]}
    XB = np.column_stack([X, np.log(panel.value_end.values), dummies(panel.cluster), dummies(panel.week)])
    bB, seB, _e, _G = ols_cluster(y, XB, cl)
    out["B_curator_week_fe_log_size"] = {"coef": {"apy": bB[0], "tail_loss": bB[1], "log_value": bB[2]},
                                         "se_cluster": {"apy": seB[0], "tail_loss": seB[1], "log_value": seB[2]},
                                         "n": int(len(y))}
    fe_v = pd.factorize(panel.chain_id.astype(str) + ":" + panel.vault)[0]
    bC, seC, _e, _G = ols_cluster(demean(y, fe_v), demean(X, fe_v), cl)
    adjC = np.sqrt((n - k) / (n - k - (fe_v.max() + 1)))
    seC = seC * adjC
    out["C_vault_fe"] = {"coef": {"apy": bC[0], "tail_loss": bC[1]}, "se_cluster": {"apy": seC[0], "tail_loss": seC[1]},
                         "n": int(len(y)), "vaults": int(fe_v.max() + 1)}
    return out


def stage_flows():
    """flows.json: the weekly flow regressions on the oracle-aware headline tail loss, with the first-pass headline in appendix_v1."""
    e4_in = e4_stamps()
    rng = np.random.default_rng(SEED)
    panel, wins = weekly_panel()
    res = {"sample": {"vault_weeks": int(len(panel)), "vaults": int(panel.groupby(["chain_id", "vault"]).ngroups),
                      "clusters": int(panel.cluster.nunique()),
                      "weeks": [day_str(MONDAY0 + 7 * int(panel.week.min())), day_str(MONDAY0 + 7 * int(panel.week.max()))],
                      "winsor_flow_rate_p1_p99": list(wins),
                      "flow_rate_mean": float(panel.flow_rate_w.mean()), "apy_mean": float(panel.apy.mean()),
                      "tl_mean": float(panel.tl.mean()),
                      "unpriced_flow_rows": int(panel.unpriced.fillna(0).sum())},
           "headline_tl": flow_models(panel, rng)}
    if "tl_v1_headline" in pd.read_csv(E4_DIR / "vault_daily.csv", nrows=0).columns:
        p1, _w = weekly_panel("tl_v1_headline")
        res["appendix_v1"] = {"what": "the same models with E4's first-pass headline tail loss (tl_v1_headline)",
                              **flow_models(p1, rng, nperm=0)}
    a = res["headline_tl"]
    res["two_methods_agree"] = {
        "own_ols_apy": a["A_curator_fe"]["coef"]["apy"], "statsmodels_apy": a["A_statsmodels_dummies"]["coef"]["apy"],
        "own_se_apy": a["A_curator_fe"]["se_cluster"]["apy"], "statsmodels_se_apy": a["A_statsmodels_dummies"]["se_cluster"]["apy"],
        "permutation_p_apy": a["A_permutation_apy_within_curator"]["p_apy"],
        "same_sign": bool(np.sign(a["A_curator_fe"]["coef"]["apy"]) == np.sign(a["A_statsmodels_dummies"]["coef"]["apy"]))}
    check_e4_unchanged(e4_in)
    write_json(res, "flows.json", {
        "what": "weekly net inflow per vault (flows/, USD at the E3 price of the day) over the vault's E4 USD "
                "value at the end of the previous week, regressed on the previous week's mean vault APY "
                "(capped at 1) and oracle-aware headline tail loss (E4 vault_daily tl_headline) with curator "
                "fixed effects; CR1 "
                "standard errors clustered by curator (an unattributed vault is its own curator)",
        "sample_rule": "E4 sample allocator vaults (Euler lending vaults have no depositor flow events); previous "
                       "week with at least 4 active days, active at its end, value at least 10k USD; flow rate "
                       "winsorized at the 1st and 99th percentiles",
        "units": "flow rate per week as a fraction of value; APY and tail loss as fractions",
        "inputs": {**e4_in,
                   "e3_vault_daily": stamp(DERIVED / "aum/vault_daily.csv")}})
    wpickle("flow_panel.pkl", panel)
    print(json.dumps(res["headline_tl"]["A_curator_fe"], default=float, indent=1))


def entity_daily(px):
    """Entity of each vault-day (attribution interval covering the day end); E3 exclusions and ungoverned Euler lending days dropped."""
    iv = curator_intervals()
    px = px[px.excluded_reason.isna() & ((px.kind != "euler_lending") | (px.governed == 1))].copy()
    ent = np.full(len(px), None, dtype=object)
    for (c, v), idx in px.groupby(["chain_id", "vault"]).indices.items():
        ent[idx] = entity_on_days(iv.get((c, v), []), px.day.values[idx])
    px["entity"] = ent
    return px


def pooled_exposure_effect(df, col, rng):
    """Exposed minus unexposed change with event fixed effects (events with both groups), CR1 errors by curator and a permutati."""
    d = df[np.isfinite(df[col])]
    d = d[d.event_id.isin(d[d.exposed].event_id) & d.event_id.isin(d[~d.exposed].event_id)]
    if d.empty or d.entity.nunique() < 3:
        return None
    yv = (d[col] if col.endswith("_log") else d[col].clip(upper=d[col].quantile(0.99))).values
    X = np.column_stack([d.exposed.values.astype(float), dummies(d.event_id)])
    b, se, _e, G = ols_cluster(yv, X, d.entity.values)
    ev_codes = pd.factorize(d.event_id)[0]
    groups = [np.flatnonzero(ev_codes == g) for g in range(ev_codes.max() + 1)]
    exp = d.exposed.values
    nperm = N_PERM // 5
    hits = 0
    for _ in range(nperm):
        perm = exp.copy()
        for ix in groups:
            perm[ix] = exp[rng.permutation(ix)]
        bp = np.linalg.lstsq(np.column_stack([perm.astype(float), X[:, 1:]]), yv, rcond=None)[0][0]
        if abs(bp) >= abs(b[0]) - 1e-12:
            hits += 1
    per_event = d.groupby(["event_id", "exposed"])[col].median().unstack()
    return {"pooled_exposed_minus_unexposed": float(b[0]), "se_cluster_curator": float(se[0]),
            "p_perm_within_event": (hits + 1) / (nperm + 1), "n": int(len(d)),
            "n_exposed": int(d.exposed.sum()), "events": int(d.event_id.nunique()), "curators": int(G),
            "median_exposed": float(np.median(d[d.exposed][col])),
            "median_unexposed": float(np.median(d[~d.exposed][col])),
            "per_event_median_difference": {k: float(r.get(True) - r.get(False))
                                            for k, r in per_event.iterrows()
                                            if np.isfinite(r.get(True, np.nan)) and np.isfinite(r.get(False, np.nan))}}


def stage_reputation():
    """For each E2 event with third-party exposure."""
    rng = np.random.default_rng(SEED)
    px = entity_daily(e3_prices())
    px = px[px.entity.notna()]
    end_day = int(px.day.max())
    bycur = pd.read_csv(DERIVED / "aum/by_curator_daily.csv", usecols=["entity", "date", "aum_usd"])
    bycur["day"] = (pd.to_datetime(bycur.date) - pd.Timestamp("1970-01-01")).dt.days
    tot = bycur.set_index(["entity", "day"]).aum_usd
    rows, skipped = [], Counter()
    for e in jsonl(EVENTS_DIR / "events.jsonl"):
        if e.get("excluded"):
            continue
        if not ((e.get("exposure_third_party_usd_at_t0") or 0) > 0):
            skipped["no_third_party_exposure_at_t0"] += 1
            continue
        d0 = e["onset_ts"] // DAY - 1
        d1 = d0 + REPUTATION_DAYS
        if d1 > end_day:
            skipped["window_past_data_end"] += 1
            continue
        protos = set(e["protocols"])
        loans = {m["loan_symbol"] for m in e["affected_markets"] if m.get("loan_symbol")}
        exp_v = {(x["chain_id"], x["vault"].lower()) for x in e["exposed_vaults"]}
        present = {x.get("curator") for x in e["exposed_vaults"] if x["group"] == "present_at_t0"
                   and x.get("third_party") and x.get("curator")}
        touched = {x.get("curator") for x in e["exposed_vaults"] if x.get("curator")}
        seg = px[px.protocol.isin(protos) & px.asset_symbol.isin(loans) & px.day.isin([d0, d1])]
        a = seg.groupby(["entity", "day"]).net_usd.sum().unstack(fill_value=0.0)
        keyv = list(zip(seg.chain_id, seg.vault))
        oth = seg[[k not in exp_v for k in keyv]].groupby(["entity", "day"]).net_usd.sum().unstack(fill_value=0.0)
        for ent in a.index:
            before = float(a.loc[ent].get(d0, 0.0))
            after = float(a.loc[ent].get(d1, 0.0))
            if ent in touched and ent not in present:
                continue
            if before < MIN_SEGMENT_USD:
                continue
            ob = float(oth.loc[ent].get(d0, 0.0)) if ent in oth.index else 0.0
            oa = float(oth.loc[ent].get(d1, 0.0)) if ent in oth.index else 0.0
            tb, ta = tot.get((ent, d0)), tot.get((ent, d1))
            rows.append({"event_id": e["event_id"], "t0": e["onset_utc"][:10], "type": e["type"],
                         "entity": ent, "exposed": ent in present, "segment_usd_t0": before,
                         "segment_usd_t90": after, "segment_change": after / before - 1.0,
                         "other_vaults_usd_t0": ob, "other_vaults_usd_t90": oa,
                         "other_vaults_change": oa / ob - 1.0 if ob >= MIN_SEGMENT_USD else np.nan,
                         "total_usd_t0": tb, "total_usd_t90": ta,
                         "total_change": ta / tb - 1.0 if tb and ta is not None and tb > 0 else np.nan})
    df = pd.DataFrame(rows)
    for a, b, c in (("segment_usd_t0", "segment_usd_t90", "segment_change"),
                    ("other_vaults_usd_t0", "other_vaults_usd_t90", "other_vaults_change"),
                    ("total_usd_t0", "total_usd_t90", "total_change")):
        x0, x1 = df[a].astype(float), df[b].astype(float)
        df[c + "_log"] = np.where(np.isfinite(df[c]), np.log(np.maximum(x1, 0.01 * x0) / x0), np.nan)
    res = {"events_used": int(df[df.exposed].event_id.nunique()), "skipped": dict(skipped),
           "rows": int(len(df)), "exposed_rows": int(df.exposed.sum())}
    for col in ("segment_change", "other_vaults_change", "total_change",
                "segment_change_log", "other_vaults_change_log", "total_change_log"):
        res[col] = pooled_exposure_effect(df, col, rng)
        res[col + "_by_type"] = {t: pooled_exposure_effect(df[df.type == t], col, rng)
                                 for t in ("depeg_loss", "lockup")}
    write_csv(df, "reputation_rows.csv", {"what": "per E2 event with third-party exposure at t0 and curator: "
                                                  "segment AUM the day before t0 and 90 days later"})
    write_json(res, "reputation.json", {
        "what": "AUM change over the 90 days after t0 for curators exposed at t0 (third-party vaults present at "
                "t0) against curators not exposed, on the same protocols and loan asset symbols as the "
                "event's affected markets; pooled with event fixed effects",
        "method": {"segment_change": "net USD (E3 aum minus holdings of other counted vaults) of the curator's "
                                     "vaults in the segment, day before t0 to 90 days later, minus 1",
                   "other_vaults_change": "same, without the vaults listed as exposed in the event (spillover "
                                          "to the curator's other vaults)",
                   "total_change": "the curator's whole AUM (by_curator_daily.csv)",
                   "log_variants": "log(end / start) with the end floored at 1 percent of the start",
                   "pooled": "OLS of the change (relative change: top 1 percent clipped) on an exposed dummy "
                             "with event fixed effects, CR1 errors clustered by curator; permutation p "
                             "shuffling the exposed label within events; only events with both groups",
                   "groups": "curators of vaults that exited before t0 or entered after are in neither group; "
                             f"both groups need at least {MIN_SEGMENT_USD:.0f} USD of segment AUM at t0"},
        "inputs": {"events": stamp(EVENTS_DIR / "events.jsonl"), "e3_vault_daily": stamp(DERIVED / "aum/vault_daily.csv"),
                   "e3_by_curator_daily": stamp(DERIVED / "aum/by_curator_daily.csv")}})
    print(json.dumps({k: v for k, v in res.items() if k in ("events_used", "skipped", "rows", "exposed_rows")}))
    for col in ("segment_change", "other_vaults_change", "total_change",
                "segment_change_log", "other_vaults_change_log", "total_change_log"):
        if res.get(col):
            print(col, {k: res[col][k] for k in ("pooled_exposed_minus_unexposed", "se_cluster_curator",
                                                 "p_perm_within_event", "n", "events")})


def stage_table5():
    """table5.csv: per curator, average fee rate, cumulative fee income, realized depositor loss, their ratio, fee income of th."""
    px = entity_daily(e3_prices())
    px = px[px.entity.notna()]
    hist = load_history()
    fee = np.zeros(len(px))
    for (c, v), idx in px.groupby(["chain_id", "vault"]).indices.items():
        fee[idx] = fee_on_days(hist.get((c, v), []), px.day.values[idx])
    px["fee"] = fee
    end = day_of("2026-10-01")
    win = px[(px.day <= end) & (px.net_usd > 0)]
    w = win.groupby("entity").apply(lambda g: np.average(g.fee, weights=g.net_usd), include_groups=False)
    ndays = end - D0 + 1
    aum_avg = win.groupby("entity").net_usd.sum() / ndays
    inc = pd.read_csv(OUT / "fee_income_by_curator.csv").set_index("entity")
    losses, src = depositor_losses(e3_prices())
    if "curator_entity" not in losses:
        ent = {(x["chain_id"], x["vault"].lower(), e["event_id"]): x.get("curator")
               for e in jsonl(EVENTS_DIR / "events.jsonl") for x in e["exposed_vaults"]}
        losses["curator_entity"] = [ent.get((c, v, ev)) for c, v, ev in zip(losses.chain_id, losses.vault, losses.event_id)]
    loss = losses.groupby("curator_entity").loss_usd_counted.sum()
    loss_events = losses[losses.loss_usd_counted > 0].groupby("curator_entity").event_id.nunique()
    fe = pd.read_csv(OUT / "fee_income_events.csv", dtype={"vault": str})
    fe = fe[(fe.group == "present_at_t0") & fe.third_party.astype(str).eq("True")]
    pre_sum = fe.groupby("curator_entity").fee_usd_curator_365d.sum()
    n_ev = fe.groupby("curator_entity").event_id.nunique()
    daily = pd.read_csv(OUT / "fee_income.csv", usecols=["chain_id", "vault", "date", "fee_usd_curator"],
                        dtype={"vault": str})
    daily["day"] = daily.date.map(day_of)
    fee_by = {k: g for k, g in daily.groupby(["chain_id", "vault"])}
    pre = {}
    for ent, g in fe.groupby("curator_entity"):
        days = defaultdict(set)
        for r in g.itertuples(index=False):
            d0 = day_of(r.t0)
            days[(r.chain_id, r.vault)].update(range(d0 - 365, d0))
        pre[ent] = float(sum(fee_by[k][fee_by[k].day.isin(dd)].fee_usd_curator.sum()
                             for k, dd in days.items() if k in fee_by))
    rep = pd.read_csv(OUT / "reputation_rows.csv")
    rep = rep[rep.exposed]
    rep_seg = rep.groupby("entity").segment_change.mean()
    rep_tot = rep.groupby("entity").total_change.mean()
    names = curator_names()
    ents = sorted(set(aum_avg.index) | set(inc.index))
    rows = []
    for e in ents:
        f_inc = float(inc.fee_usd_curator.get(e, 0.0))
        l = float(loss.get(e, 0.0))
        rows.append({"entity": e, "name": names.get(e, e), "aum_avg_usd": float(aum_avg.get(e, 0.0)),
                     "avg_fee_rate": float(w.get(e, np.nan)), "fee_income_usd": f_inc,
                     "fees_paid_by_depositors_usd": float(inc.fee_usd_total.get(e, 0.0)),
                     "depositor_loss_usd": l, "fee_income_over_loss": f_inc / l if l > 0 else np.nan,
                     "loss_events": int(loss_events.get(e, 0)),
                     "events_present_at_t0": int(n_ev.get(e, 0)),
                     "fee_income_affected_vaults_365d_before_usd": float(pre.get(e, 0.0)),
                     "fee_income_affected_vaults_365d_before_sum_over_events_usd": float(pre_sum.get(e, 0.0)),
                     "aum_change_90d_after_segment_mean": float(rep_seg.get(e, np.nan)),
                     "aum_change_90d_after_total_mean": float(rep_tot.get(e, np.nan)),
                     "events_in_reputation_test": int((rep.entity == e).sum())})
    df = pd.DataFrame(rows).sort_values("aum_avg_usd", ascending=False)
    df.insert(0, "rank_by_aum", np.arange(1, len(df) + 1))
    write_csv(df, "table5.csv", {
        "what": "Table 5: fee income against depositor losses per curator, ranked by average daily net AUM "
                "2024-01-01 to 2026-10-01 (E3, attributed vault-days)",
        "columns_note": {
            "avg_fee_rate": "net-USD-weighted mean over the curator's vault-days of the performance fee in force "
                            "(Euler lending: interest fee)",
            "fee_income_usd": "fee_income_by_curator.csv fee_usd_curator (phantom accruals left out)",
            "fees_paid_by_depositors_usd": "all fee shares minted in the curator's vaults, including the "
                                           "protocol's part (an Euler lending vault without a fee receiver "
                                           "sends the whole interest fee to the protocol)",
            "depositor_loss_usd": f"sum of loss_usd_counted over the curator's vaults that are not "
                                  f"self-deposited ({src}); Euler lending vaults net of the Earn vaults "
                                  "that supply them",
            "fee_income_affected_vaults_365d_before_usd": "fee income of the curator's third-party vaults "
                                                          "present at t0 over the union of the 365-day windows "
                                                          "before its events (a vault-day counted once)",
            "fee_income_affected_vaults_365d_before_sum_over_events_usd": "the same summed event by event",
            "aum_change_90d_after_segment_mean": "mean over the curator's exposed events of the relative "
                                                 "change of its segment AUM (reputation_rows.csv)"},
        "loss_source": src,
        "inputs": {"fee_income_by_curator": stamp(OUT / "fee_income_by_curator.csv"),
                   "fee_income_events": stamp(OUT / "fee_income_events.csv"),
                   "reputation_rows": stamp(OUT / "reputation_rows.csv"),
                   "losses": stamp(E5_EVENT_VAULT) if src == "e5" else stamp(EVENTS_DIR / "events.jsonl")}})
    with pd.option_context("display.width", 250, "display.max_columns", 20):
        print(df.head(12).to_string(index=False))


FIG_CLASS_GROUPS = (("stable", "stable collateral", "#2a78d6"),
                    ("eth|btc", "ETH or BTC collateral", "#eb6834"),
                    ("mixed|other|none", "mixed or other", "#1baf7a"))


def stage_figures():
    """fig6_data.csv and figures/fig6_fees.pdf."""
    e4_in = {k: v for k, v in json.loads((OUT / "contrasts.json.meta.json").read_text())["inputs"].items()
             if k.startswith("e4_")}
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    df = rpickle("vault_table.pkl")
    con = json.loads((OUT / "contrasts.json").read_text())
    cols = [("yield_after_fee", "after-fee yield"), ("tl_headline", "tail loss (headline)"),
            ("L_p10", "liquidity (p10 of L)"), ("loss_per_dollar", "event loss per dollar")]
    keep = ["chain_id", "vault", "protocol", "kind", "name", "curator_entity", "class", "fee_rate"] + [c for c, _ in cols]
    keep += [c for c in ("tl_v1_headline",) if c in df.columns]
    fig_df = df[keep].copy()
    write_csv(fig_df, "fig6_data.csv", {"what": "Fig 6 data: one row per E4 sample vault; tl_headline is E4's "
                                                "oracle-aware headline tail loss, tl_v1_headline the first pass "
                                                "(appendix)",
                                        "inputs": {"contrasts": stamp(OUT / "contrasts.json"),
                                                   **e4_in}})
    plt.rcParams.update({"font.size": 7, "axes.linewidth": 0.5, "xtick.major.width": 0.5,
                         "ytick.major.width": 0.5, "pdf.fonttype": 42})
    fig, axes = plt.subplots(1, 4, figsize=(7.2, 2.3))
    for ax, (c, label) in zip(axes, cols):
        d = fig_df[np.isfinite(fig_df[c]) & np.isfinite(fig_df.fee_rate)]
        lo, hi = np.percentile(d[c], [2, 98])
        for ent, g in d[d.curator_entity.notna()].groupby("curator_entity"):
            if len(g) > 1:
                g = g.sort_values("fee_rate")
                ax.plot(g.fee_rate, g[c].clip(lo, hi), color="#c8c7c2", lw=0.3, zorder=1)
        for pat, name, color in FIG_CLASS_GROUPS:
            g = d[d["class"].str.fullmatch(pat)]
            ax.scatter(g.fee_rate, g[c].clip(lo, hi), s=4, color=color, lw=0.25, edgecolor="white",
                       label=name, zorder=2)
        r = con["contrast1_cross_section"][c]
        wc, nc = r["within_class"], r["no_control"]
        ax.set_title(f"ρ within class {wc['pooled']:+.2f} (p {wc['p_perm_clustered']:.3f})\n"
                     f"ρ pooled {nc['pooled']:+.2f} (p {nc['p_perm_clustered']:.3f})", fontsize=6, color="#0b0b0b")
        ax.set_xlabel("performance fee rate")
        ax.set_ylabel(label)
        pad = 0.04 * (hi - lo)
        ax.set_ylim(lo - pad, hi + pad)
        ax.grid(True, lw=0.3, color="#e6e5e0")
        ax.spines[["top", "right"]].set_visible(False)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=6, frameon=False, markerscale=2,
               handletextpad=0.2, columnspacing=1.2)
    fig.tight_layout(pad=0.4, w_pad=0.8, rect=(0, 0.07, 1, 1))
    out = OUT / "figures/fig6_fees.pdf"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out)
    fig.savefig(WORK / "fig6_fees.png", dpi=200)
    plt.close(fig)
    io_utils._meta_sidecar(out, GENERATOR, {"what": "Fig 6, fees and performance; y values clipped to the "
                                                    "2nd and 98th percentiles of each panel; colours by "
                                                    "collateral class; grey lines join a curator's vaults",
                                            "data": "fig6_data.csv"})


class IndepFee:
    """Independent recomputation of one vault-day's fee income from the raw files with the csv module and Python integers only."""

    def __init__(self):
        self.dec = {}
        for r in jsonl(DATA / "registry/vault_aum.jsonl"):
            self.dec[(r["chain_id"], r["vault"].lower())] = r.get("price_decimals") or r.get("asset_decimals")

    @staticmethod
    def grep_rows(path, needle):
        import csv as _csv
        head = open(path).readline()
        res = subprocess.run(["grep", "-F", needle, str(path)], capture_output=True, text=True,
                             env={**os.environ, "LC_ALL": "C"})
        return list(_csv.DictReader([head] + res.stdout.splitlines(keepends=True)))

    def day(self, proto, cid, vault, day):
        import csv as _csv
        fees = self.grep_rows(DATA / f"vaults/{proto}/{cid}/fees.csv", vault)
        ev = []
        for r in fees:
            if r["vault"].lower() != vault:
                continue
            sh = [int(r.get(c) or 0) for c in ("feeShares", "performanceFeeShares", "managementFeeShares",
                                                "protocolFeeShares")]
            if sum(sh) == 0:
                continue
            ev.append((int(r["block"]), int(r["log_index"]), "fee", sh, int(r["newTotalAssets"]), int(r["timestamp"])))
        p = DATA / f"vaults/{proto}/{cid}/flows/{vault}.csv"
        with open(p) as fh:
            for r in _csv.DictReader(fh):
                sgn = 1 if r["kind"].lower() == "deposit" else -1
                ev.append((int(r["block"]), int(r["log_index"]), "flow", sgn * int(r["shares"]), None, int(r["timestamp"])))
        ev.sort(key=lambda x: (x[0], x[1]))
        supply = 0
        shares_day, assets_cur, assets_tot = 0, 0.0, 0.0
        for b, li, kind, x, nta, ts in ev:
            if kind == "flow":
                supply += x
                continue
            minted = sum(x)
            if ts // DAY == day:
                cur = x[0] + x[1] + x[2]
                shares_day += cur
                v = minted * nta / (supply + minted)
                assets_tot += v
                assets_cur += v * cur / minted
            supply += minted
        e3 = [r for r in self.grep_rows(DERIVED / "aum/vault_daily.csv", vault)
              if r["vault"].lower() == vault and int(r["chain_id"]) == cid and r["date"] == day_str(day)]
        price = float(e3[0]["price_usd"]) if e3 and e3[0]["price_usd"] else None
        dec = self.dec.get((cid, vault))
        usd = assets_cur * price / 10 ** dec if price is not None and dec is not None else None
        return {"fee_shares_curator": str(shares_day), "fee_assets_curator": assets_cur, "fee_usd_curator": usd,
                "decimals_registry": dec}


def recipient_mint_check(row, accr, heads):
    """Fee shares minted to each recipient of a vault, against the vault's Transfer logs (HyperSync, through the scan head)."""
    from common import hypersync_client as hs
    cid, proto, v = int(row.chain_id), row.protocol, row.vault
    slug = drpc_slug(cid)
    T = evmabi.topic0("Transfer(address,address,uint256)")
    head = heads[f"{proto}:{cid}"]["block"]
    a = accr[(accr.chain_id == cid) & (accr.vault == v)]
    first = int(a.block.min())
    reg = registry_vaults().get((cid, v), {})
    start = int(reg.get("created_block") or first)
    exp = defaultdict(int)
    for r in a.itertuples(index=False):
        exp[r.fee_recipient] += int(r.perf_shares)
        exp[r.mgmt_fee_recipient if isinstance(r.mgmt_fee_recipient, str) else r.fee_recipient] += int(r.mgmt_shares)
    flows = pd.read_csv(DATA / f"vaults/{proto}/{cid}/flows/{v}.csv", dtype=str)
    out = {"chain_id": cid, "protocol": proto, "kind": row.kind, "vault": v, "recipients": []}
    for rec, fee_sh in exp.items():
        if fee_sh == 0:
            continue
        rt = "0x" + "0" * 24 + rec[2:]
        to, _ = hs.scan_logs(hypersync_slug(cid), addresses=[v], topics=[[T], [], [rt]],
                             from_block=start, to_block=head + 1)
        fr, _ = hs.scan_logs(hypersync_slug(cid), addresses=[v], topics=[[T], [rt]],
                             from_block=start, to_block=head + 1)
        mint = sum(int(x["data"], 16) for x in to if x["topics"][1][-40:] == "0" * 40)
        tin = sum(int(x["data"], 16) for x in to)
        tout = sum(int(x["data"], 16) for x in fr)
        dep = flows[(flows.kind.str.lower() == "deposit") & (flows.owner.str.lower() == rec)]
        dep_sh = sum(int(x) for x in dep.shares)
        bal = multicall.aggregate3(slug, [(v, evmabi.encode_call("balanceOf(address)", ["address"], [rec]))],
                                   block=head)[0]
        bal = int(bal[1], 16) if bal[0] and bal[1] not in ("0x", "") else None
        out["recipients"].append({
            "recipient": rec, "fee_shares_from_fees_csv": str(fee_sh),
            "mints_to_recipient": str(mint), "deposit_shares_to_recipient": str(dep_sh),
            "mints_minus_deposits": str(mint - dep_sh), "gap": str(mint - dep_sh - fee_sh),
            "match": mint - dep_sh == fee_sh,
            "transfers_in": str(tin), "transfers_out": str(tout), "balance_from_logs": str(tin - tout),
            "balanceOf_at_head": str(bal) if bal is not None else None,
            "balance_match": bal == tin - tout if bal is not None else None})
    out["match"] = all(r["match"] for r in out["recipients"])
    return out


def stage_validate():
    """validation.json: 20 random vault-days of fee income recomputed by IndepFee; 10 random vaults' fee shares against their r."""
    from concurrent.futures import ThreadPoolExecutor
    rng = np.random.default_rng(SEED + 6)
    res = {}
    calls0 = rpc.calls_made()
    daily = pd.read_csv(OUT / "fee_income.csv", dtype={"vault": str, "perf_shares": str, "mgmt_shares": str,
                                                       "protocol_shares": str})
    pool = daily[(daily.kind != "euler_lending") & daily.priced & ~daily.phantom & daily.excluded.isna()]
    pick = pool.iloc[rng.choice(len(pool), 20, replace=False)]
    ind = IndepFee()
    rows = []
    for r in pick.itertuples(index=False):
        got = ind.day(r.protocol, int(r.chain_id), r.vault, day_of(r.date))
        mine_sh = int(r.perf_shares) + int(r.mgmt_shares)
        rel_a = abs(got["fee_assets_curator"] - r.fee_assets_curator) / max(abs(r.fee_assets_curator), 1e-300)
        rel_u = (abs(got["fee_usd_curator"] - r.fee_usd_curator) / max(abs(r.fee_usd_curator), 1e-300)
                 if got["fee_usd_curator"] is not None else None)
        rows.append({"chain_id": int(r.chain_id), "vault": r.vault, "kind": r.kind, "date": r.date,
                     "fee_shares": str(mine_sh), "fee_shares_indep": got["fee_shares_curator"],
                     "shares_identical": str(mine_sh) == got["fee_shares_curator"],
                     "fee_assets": r.fee_assets_curator, "fee_assets_indep": got["fee_assets_curator"],
                     "rel_diff_assets": rel_a, "fee_usd": r.fee_usd_curator, "fee_usd_indep": got["fee_usd_curator"],
                     "rel_diff_usd": rel_u})
    res["fee_income_vault_days"] = {
        "n": len(rows), "shares_identical": int(sum(x["shares_identical"] for x in rows)),
        "assets_within_1e-12": int(sum(x["rel_diff_assets"] <= 1e-12 for x in rows)),
        "usd_within_1e-9": int(sum(x["rel_diff_usd"] is not None and x["rel_diff_usd"] <= 1e-9 for x in rows)),
        "max_rel_diff_assets": max(x["rel_diff_assets"] for x in rows),
        "max_rel_diff_usd": max((x["rel_diff_usd"] for x in rows if x["rel_diff_usd"] is not None), default=None),
        "rows": rows}
    print("fee income:", {k: v for k, v in res["fee_income_vault_days"].items() if k != "rows"}, flush=True)
    accr = rpickle("accruals_valued.pkl")
    accr = accr[accr.kind != "euler_lending"]
    byv = pd.read_csv(OUT / "fee_income_by_vault.csv", dtype={"vault": str})
    byv = byv[byv.kind != "euler_lending"].reset_index(drop=True)
    order = rng.permutation(len(byv))[:10]
    heads = e3_heads()
    picked = [(int(byv.chain_id.iat[i]), byv.vault.iat[i]) for i in order]
    cache = WORK / "recipient_checks.json"
    prev = json.loads(cache.read_text()) if cache.exists() else None
    if prev and [tuple(x) for x in prev["picked"]] == picked and prev["accruals_rows"] == len(accr):
        checks, rec_calls = prev["checks"], prev["rpc_calls"]
    else:
        c0 = rpc.calls_made()
        with ThreadPoolExecutor(max_workers=5) as ex:
            checks = list(ex.map(lambda i: recipient_mint_check(byv.iloc[i], accr, heads), order))
        rec_calls = rpc.calls_made() - c0
        cache.write_text(json.dumps({"picked": picked, "accruals_rows": len(accr), "checks": checks,
                                     "rpc_calls": rec_calls}))
    res["recipient_transfers"] = {"n": len(checks), "vaults_matching": int(sum(c["match"] for c in checks)),
                                  "recipients": int(sum(len(c["recipients"]) for c in checks)),
                                  "recipients_matching": int(sum(r["match"] for c in checks for r in c["recipients"])),
                                  "rpc_calls": rec_calls,
                                  "balance_checks_matching": int(sum(all(r["balance_match"] for r in c["recipients"])
                                                                     for c in checks)),
                                  "vaults": checks}
    print("recipients:", res["recipient_transfers"]["vaults_matching"], "of", len(checks), flush=True)
    con = json.loads((OUT / "contrasts.json").read_text())
    art = {}
    for c in ("tl_headline", "tl_d1000", "tl_d0150", "yield_after_fee", "L_p10"):
        g = con["contrast1_group"][c]
        art[c] = {"rank_biserial_no_control": g["no_control"]["pooled"],
                  "p_no_control": g["no_control"].get("p_perm_clustered"),
                  "rank_biserial_within_class": g["within_class"]["pooled"],
                  "p_within_class": g["within_class"].get("p_perm_clustered"),
                  "medians_no_control": g["medians_no_control"]}
    if "appendix_v1" in con:
        g = con["appendix_v1"]["contrast1_group"].get("tl_v1_headline")
        if g:
            art["appendix_v1_tl_v1_headline"] = {
                "rank_biserial_no_control": g["no_control"]["pooled"],
                "p_no_control": g["no_control"].get("p_perm_clustered"),
                "rank_biserial_within_class": g["within_class"]["pooled"],
                "p_within_class": g["within_class"].get("p_perm_clustered")}
    art["share_unpegged_of_value"] = con["artefact_check"]["share_unpegged_of_value"]["medians_no_control"]
    art["zero_fee_by_class"] = con["artefact_check"]["zero_fee_by_class"]
    res["artefact_check"] = art
    fl = json.loads((OUT / "flows.json").read_text())
    res["flow_regression"] = fl["two_methods_agree"]
    vt = rpickle("vault_table.pkl")
    res["fee_rate_vs_e4_perf_fee_mean"] = {
        "max_abs_diff": float((vt.fee_rate - vt.perf_fee_mean).abs().max()),
        "vaults_within_1e-9": int(((vt.fee_rate - vt.perf_fee_mean).abs() <= 1e-9).sum()), "n": int(len(vt))}
    meta = json.loads((OUT / "classes.csv.meta.json").read_text())
    res["class_unpegged_vs_e4_spearman_usd_vaults"] = meta["check_vs_e4_share_unpegged_spearman_usd_vaults"]
    res["e4_implied_fee_comparison"] = e4_fee_comparison()
    res["rpc_calls"] = rpc.calls_made() - calls0 + rec_calls
    write_json(res, "validation.json", {"what": "E6 validation: independent fee recomputation, recipient "
                                                "Transfer logs, the first-pass artefact, the flow regression "
                                                "by two methods, consistency with E4",
                                        "dRPC_calls": res["rpc_calls"]})
    print("rpc calls", res["rpc_calls"])


def e4_fee_comparison():
    """Fee income against the fee E4 deducts from returns."""
    byv = pd.read_csv(OUT / "fee_income_by_vault.csv", dtype={"vault": str})
    top = byv[byv.kind != "euler_lending"].head(20)
    d = e4_daily(["value_usd", "ret_before_fee", "ret_after_fee"])
    d["vault"] = d.vault.str.lower()
    inc = pd.read_csv(OUT / "fee_income.csv", usecols=["chain_id", "vault", "date", "fee_usd_curator", "phantom"],
                      dtype={"vault": str})
    out = []
    for r in top.itertuples(index=False):
        g = d[(d.chain_id == r.chain_id) & (d.vault == r.vault)].sort_values("day")
        if g.empty:
            out.append({"vault": r.vault, "in_e4_sample": False})
            continue
        e4fee = (g.ret_before_fee - g.ret_after_fee) * g.value_usd.shift(1)
        days = set(g.date[e4fee.notna()])
        mine = inc[(inc.chain_id == r.chain_id) & (inc.vault == r.vault) & inc.date.isin(days)].fee_usd_curator.sum()
        out.append({"vault": r.vault, "chain_id": int(r.chain_id), "in_e4_sample": True,
                    "fee_usd_e6": float(mine), "fee_usd_e4_implied": float(e4fee.sum()),
                    "ratio": float(mine / e4fee.sum()) if e4fee.sum() else None})
    return out


STAGES = ("history", "income", "classes", "contrasts", "flows", "reputation", "table5", "figures",
          "validate")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--stage", default="all", choices=("all",) + STAGES)
    ap.add_argument("--reuse-accruals", action="store_true",
                    help="income: revalue the accruals of the last run instead of re-reading fees.csv")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    WORK.mkdir(parents=True, exist_ok=True)
    fns = {"history": stage_history, "income": lambda: stage_income(args.reuse_accruals),
           "classes": stage_classes, "contrasts": stage_contrasts, "flows": stage_flows,
           "reputation": stage_reputation, "table5": stage_table5, "figures": stage_figures,
           "validate": stage_validate}
    for st in (STAGES if args.stage == "all" else (args.stage,)):
        t0 = time.time()
        print(f"== {st}", flush=True)
        fns[st]()
        print(f"== {st} done in {time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
