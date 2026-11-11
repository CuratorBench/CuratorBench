# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0"]
# ///
"""Gas per rebalance and gas prices for Table 7, measured on chain."""
import asyncio
import csv
import datetime as dt
import json
import pathlib
import statistics
import sys
from collections import defaultdict

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import context as C

GAS_DIR = C.OUT / "gas"
WORK_DIR = C.WORK / "gas"
END_UTC = "2026-10-01 00:00"
SEL_MM = "0x7299aa31"
SEL_4626 = "0xbe9c5800"
TOPICS_MM = {"0x89bf199df65bf65155e3e0a8abc4ad4a1be606220c8295840dba2ab5656c1f6d",
             "0xdd8bf5226dff861316e0fa7863fdb7dc7b87c614eb29a135f524eb79d5a1189a"}
TOPICS_4626 = {"0x73e8a9d66522fa5c6fcec2e89aad8e52f6f66eadeb7fc7c172cfcbacb5283851",
               "0x416d9fcbb15941508bf9f557a950fb085de673ae90db45a32efdc9b35f29b0f1"}
PAIRS = {("morpho", 1), ("morpho", 8453), ("morpho", 42161), ("morpho", 999), ("morpho", 747474),
         ("moolah", 56), ("euler_v2", 1), ("euler_v2", 9745),
         ("silo_v2", 1), ("silo_v2", 146), ("silo_v2", 42161), ("silo_v2", 43114)}
KINDS = {"morpho": {"morpho_v1"}, "moolah": {"moolah_vault"}, "euler_v2": {"euler_earn"},
         "silo_v2": {"silo_vault"}}
MIN_FIT = 30


def selector(proto):
    return SEL_MM if proto in ("morpho", "moolah") else SEL_4626


def topics(proto):
    return TOPICS_MM if proto in ("morpho", "moolah") else TOPICS_4626


def slug(cid):
    return json.loads((C.ROOT / "code/config/chains.json").read_text())[str(cid)]["hypersync"]


def _int(x):
    if x is None:
        return None
    return int(x, 16) if isinstance(x, str) else int(x)


def first_block(proto, cid, n=200):
    """Earliest block among the first lines of the pair's vault action log (written in block order)."""
    out = None
    with open(C.DATA / "vaults" / proto / str(cid) / "actions.jsonl") as fh:
        for i, line in enumerate(fh):
            if i >= n:
                break
            b = json.loads(line)["block"]
            out = b if out is None else min(out, b)
    return out or 0


async def _scan(proto, cid, vaults, end_ts):
    """The vaults' ReallocateSupply and ReallocateWithdraw logs (indexed by address and topic), joined with their transactions and blocks; kept."""
    sys.path.insert(0, str(C.ROOT / "code"))
    from common import hypersync_client as H
    from hypersync import (BlockField, FieldSelection, LogField, LogSelection, Query,
                           TransactionField)
    client = H._client(slug(cid))
    height = await client.get_height()
    fields = FieldSelection(
        block=[BlockField.NUMBER, BlockField.TIMESTAMP],
        transaction=[TransactionField.HASH, TransactionField.BLOCK_NUMBER, TransactionField.TO,
                     TransactionField.SIGHASH, TransactionField.STATUS, TransactionField.GAS_USED,
                     TransactionField.EFFECTIVE_GAS_PRICE, TransactionField.L1_FEE],
        log=[LogField.ADDRESS, LogField.TOPIC0, LogField.TOPIC1, LogField.TOPIC2, LogField.TRANSACTION_HASH,
             LogField.BLOCK_NUMBER])
    sel = [LogSelection(address=vaults[i:i + 500], topics=[sorted(topics(proto))])
           for i in range(0, len(vaults), 500)]
    txs, logs, bts = {}, defaultdict(set), {}
    vset = set(vaults)
    frm, pages = first_block(proto, cid), 0
    while frm < height:
        r = await client.get(Query(from_block=frm, to_block=height, logs=sel, field_selection=fields))
        for b in r.data.blocks or []:
            bts[b.number] = _int(b.timestamp)
        for t in r.data.transactions or []:
            txs[t.hash] = {"block": t.block_number, "to": (t.to or "").lower(), "sighash": (t.sighash or "").lower(),
                           "status": _int(t.status), "gas_used": _int(t.gas_used),
                           "gas_price": _int(t.effective_gas_price), "l1_fee": _int(t.l1_fee) or 0}
        for lg in r.data.logs or []:
            tp = list(lg.topics or [])
            if len(tp) > 2 and tp[2] is not None:
                logs[lg.transaction_hash].add(((lg.address or "").lower(), tp[2].lower()))
        if r.next_block <= frm:
            break
        frm = r.next_block
        pages += 1
        if pages % 50 == 0:
            print(f"  {proto} {cid}: block {frm:,} of {height:,}, {len(txs):,} transactions", flush=True)
        if bts and max(bts.values()) > end_ts:
            break
    out = []
    for h, t in txs.items():
        ts = bts.get(t["block"])
        if (ts is None or ts > end_ts or not t["gas_used"] or t["status"] != 1
                or t["sighash"] != selector(proto) or t["to"] not in vset):
            continue
        k = len({m for v, m in logs.get(h, ()) if v == t["to"]})
        out.append({"protocol": proto, "chain_id": cid, "tx_hash": h, "block": t["block"], "timestamp": ts,
                    "vault": t["to"], "markets_touched": k, "gas_used": t["gas_used"],
                    "effective_gas_price_wei": t["gas_price"], "l1_fee_wei": t["l1_fee"]})
    return sorted(out, key=lambda x: (x["block"], x["tx_hash"])), len(txs)


def sample(pairs=PAIRS, end_utc=END_UTC, log=lambda m: print(m, flush=True)):
    """Every direct reallocate transaction of the pairs' registry vaults."""
    end_ts = C.parse_utc(end_utc)
    rows = []
    for proto, cid in sorted(pairs):
        reg = C.e2.vault_registry(proto, cid)
        vaults = sorted(v for v, r in reg.items() if r.get("kind") in KINDS[proto])
        got, n_all = asyncio.run(_scan(proto, cid, vaults, end_ts))
        log(f"gas sample {proto} {cid}: {len(vaults)} vaults, {n_all} transactions with a reallocate log, "
            f"{len(got)} direct reallocate calls")
        rows += got
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    path = WORK_DIR / "reallocate_txs.csv"
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return rows


def fit(rows, min_n=5):
    """gas = a + b k by least squares over the median gas used at each number of markets touched k >= 1 with at least `min_n` transactions (medians."""
    by = defaultdict(list)
    for r in rows:
        if r["markets_touched"] >= 1:
            by[r["markets_touched"]].append(r["gas_used"])
    pts = [(k, statistics.median(g)) for k, g in sorted(by.items()) if len(g) >= min_n]
    if not pts:
        return None, 0.0
    if len(pts) == 1:
        k, g = pts[0]
        return 0.0, g / k
    mx, my = statistics.fmean(k for k, _ in pts), statistics.fmean(g for _, g in pts)
    b = sum((k - mx) * (g - my) for k, g in pts) / sum((k - mx) ** 2 for k, _ in pts)
    a = my - b * mx
    if a < 0:
        a, b = 0.0, sum(k * g for k, g in pts) / sum(k * k for k, _ in pts)
    return a, b


def month(ts):
    return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime("%Y-%m")


def build(rows, log=print):
    GAS_DIR.mkdir(parents=True, exist_ok=True)
    by_pair = defaultdict(list)
    by_proto = defaultdict(list)
    for r in rows:
        by_pair[(r["protocol"], r["chain_id"])].append(r)
        if r["chain_id"] != 42161:
            by_proto[r["protocol"]].append(r)
    gas_rows = []
    for (proto, cid) in sorted(PAIRS):
        rs = by_pair.get((proto, cid), [])
        own = len([r for r in rs if r["markets_touched"] >= 1]) >= MIN_FIT
        a, b = fit(rs if own else by_proto[proto])
        ks = [r["markets_touched"] for r in rs]
        gs = [r["gas_used"] for r in rs]
        gas_rows.append({"protocol": proto, "chain_id": cid, "transactions": len(rs),
                         "fit_source": "own" if own else f"pooled {proto} (chains other than Arbitrum)",
                         "gas_base": round(a) if a is not None else None, "gas_per_market": round(b),
                         "median_gas_used": statistics.median(gs) if gs else None,
                         "median_markets_touched": statistics.median(ks) if ks else None,
                         "first_utc": C.ts_iso(rs[0]["timestamp"]) if rs else None,
                         "last_utc": C.ts_iso(rs[-1]["timestamp"]) if rs else None})
    price_rows = []
    by_cm = defaultdict(list)
    for r in rows:
        by_cm[(r["chain_id"], month(r["timestamp"]))].append(r)
    for (cid, mo), rs in sorted(by_cm.items()):
        price_rows.append({"chain_id": cid, "month": mo, "transactions": len(rs),
                           "median_gas_price_gwei": statistics.median(r["effective_gas_price_wei"] for r in rs) / 1e9,
                           "median_l1_fee_native": statistics.median(r["l1_fee_wei"] for r in rs) / 1e18,
                           "median_fee_native": statistics.median(
                               (r["gas_used"] * r["effective_gas_price_wei"] + r["l1_fee_wei"]) for r in rs) / 1e18})
    meta = {"generator": "code/autocurator/gas.py", "source": "HyperSync transactions (to = registry vault, "
            "reallocate selector, status 1) joined with their logs and blocks", "end_utc": END_UTC,
            "selectors": {"morpho/moolah": SEL_MM, "euler_v2/silo_v2": SEL_4626}}
    for path, rws in ((GAS_DIR / "rebalance_gas.csv", gas_rows), (GAS_DIR / "gas_price_monthly.csv", price_rows)):
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rws[0]))
            w.writeheader()
            w.writerows(rws)
        C.write_sidecar(path, {**meta, "rows": len(rws)})
    log(f"gas tables written: {len(gas_rows)} pairs, {len(price_rows)} chain-months")
    return gas_rows, price_rows


class GasModel:
    """Cost of one rebalance from the tables build() writes."""

    def __init__(self, gas_dir=GAS_DIR):
        self.gas = {(r["protocol"], int(r["chain_id"])): (float(r["gas_base"]), float(r["gas_per_market"]))
                    for r in csv.DictReader(open(gas_dir / "rebalance_gas.csv")) if r["gas_base"]}
        self.price = defaultdict(dict)
        for r in csv.DictReader(open(gas_dir / "gas_price_monthly.csv")):
            self.price[int(r["chain_id"])][r["month"]] = (float(r["median_gas_price_gwei"]),
                                                          float(r["median_l1_fee_native"]))

    def gas_units(self, proto, cid, k):
        a, b = self.gas[(proto, int(cid))]
        return a + b * k

    def price_at(self, cid, ts):
        months = self.price.get(int(cid))
        if not months:
            return None
        mo = month(ts)
        if mo in months:
            return months[mo]
        key = min(months, key=lambda m: abs((dt.date.fromisoformat(m + "-01")
                                             - dt.date.fromisoformat(mo + "-01")).days))
        return months[key]

    def fee_native(self, proto, cid, k, ts):
        p = self.price_at(cid, ts)
        if p is None or (proto, int(cid)) not in self.gas:
            return None
        gwei, l1 = p
        return self.gas_units(proto, cid, k) * gwei * 1e-9 + l1


def load_sample(path=WORK_DIR / "reallocate_txs.csv"):
    ints = ("chain_id", "block", "timestamp", "markets_touched", "gas_used", "effective_gas_price_wei", "l1_fee_wei")
    with open(path) as fh:
        return [{k: (int(v) if k in ints else v) for k, v in r.items()} for r in csv.DictReader(fh)]


if __name__ == "__main__":
    build(load_sample() if "--from-sample" in sys.argv else sample())
