# /// script
# requires-python = ">=3.11"
# dependencies = ["pycryptodome"]
# ///
"""Wei-exact validation of the AdaptiveCurveIRM port (irm.py) against chain."""

import csv
import json
import pathlib
import sys

from Crypto.Hash import keccak

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "data"))
import irm
from legacy_rpc import (MORPHO_BLUE, TOPIC_ACCRUE_INTEREST,
                    batch_timestamps as _batch_ts, get_logs, words)

DATA = HERE.parents[1] / "data"
FULL_LIMIT = 50_000
TAIL_N = 25_000

TOPIC_BORROW_RATE_UPDATE = "0x" + keccak.new(
    data=b"BorrowRateUpdate(bytes32,uint256,uint256)",
    digest_bits=256).hexdigest()

PARAMS = json.load(open(DATA / "markets" / "params.json"))
CRISIS_MARKETS = [
    (1, "0xbd1ad3b968f5f0552dbd8cf1989a62881407c5cccf9e49fb3657c8731caf0c1f"),
]


def batch_timestamps(cid, blocks, chunk=500):
    return _batch_ts(cid, blocks, chunk=chunk)


def accrual_walk(csv_path):
    """Stream the panel CSV; yield (block, log_index, pre_state) per accrue row plus the creation block."""
    creation = None
    prev = (0, 0, 0, 0)
    accruals = []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            if row["kind"] == "create":
                creation = int(row["block"])
            elif row["kind"] == "accrue":
                accruals.append((int(row["block"]), int(row["log_index"]),
                                 prev))
            prev = (int(row["total_supply_assets"]),
                    int(row["total_supply_shares"]),
                    int(row["total_borrow_assets"]),
                    int(row["total_borrow_shares"]))
    return creation, accruals


def fetch_events(cid, mid, irm_addr, from_block, to_block):
    """AccrueInterest (Blue) + BorrowRateUpdate (IRM) for one market, keyed (block, tx)."""
    acc = {}
    for lg in get_logs(MORPHO_BLUE, [TOPIC_ACCRUE_INTEREST, mid],
                       from_block, to_block, chain_id=cid):
        acc[(int(lg["blockNumber"], 16), int(lg["logIndex"], 16))] = \
            words(lg["data"])
    bru = {}
    for lg in get_logs(irm_addr, [TOPIC_BORROW_RATE_UPDATE, mid],
                       from_block, to_block, chain_id=cid):
        b = int(lg["blockNumber"], 16)
        bru[b] = words(lg["data"])
    return acc, bru


def validate_market(cid, mid):
    csv_path = DATA / "markets" / f"{cid}-{mid}.csv"
    p = PARAMS[f"{cid}-{mid}"]
    irm_addr = p["irm"]
    if int(irm_addr, 16) == 0:
        return {"market": f"{cid}-{mid}", "skipped": "zero IRM"}

    creation, accruals = accrual_walk(csv_path)
    n = len(accruals)
    if n == 0:
        return {"market": f"{cid}-{mid}", "skipped": "no accruals"}

    if n <= FULL_LIMIT:
        k0, seed_rat = 0, irm.INITIAL_RATE_AT_TARGET
        from_block = creation
        last_update_block = creation
    else:
        k0 = n - TAIL_N
        from_block = accruals[k0 - 1][0]
        last_update_block = from_block
        seed_rat = None

    span = accruals[k0:]
    print(f"  {cid}-{mid[:10]}…: {n} accruals, validating "
          f"{'all' if k0 == 0 else f'tail {len(span)}'}")

    acc_ev, bru_ev = fetch_events(cid, mid, irm_addr, from_block,
                                  span[-1][0])
    if seed_rat is None:
        seed_rat = bru_ev[from_block][1]

    ts = batch_timestamps(
        cid, [last_update_block] + [a[0] for a in span])

    rat = seed_rat
    last_ts = ts[last_update_block]
    checked, mismatches = 0, []
    for block, log_index, (tsa, tss, tba, tbs) in span:
        ev = acc_ev.get((block, log_index))
        if ev is None:
            mismatches.append({"block": block, "field": "missing-event"})
            continue
        elapsed = ts[block] - last_ts
        got = irm.accrue_interest(tsa, tss, tba, tbs, rat, elapsed, fee=0)
        _, _, _, _, end_rat, avg_rate, interest, fee_shares = got
        exp_rate, exp_interest, exp_fee = ev
        exp_rat = bru_ev.get(block, (None, None))[1]
        for field, mine, chain_v in (
                ("avg_rate", avg_rate, exp_rate),
                ("interest", interest, exp_interest),
                ("fee_shares", fee_shares, exp_fee),
                ("rate_at_target", end_rat, exp_rat)):
            if chain_v is not None and mine != chain_v:
                mismatches.append({"block": block, "field": field,
                                   "mine": str(mine), "chain": str(chain_v)})
        rat = exp_rat if exp_rat is not None else end_rat
        last_ts = ts[block]
        checked += 1
        if mismatches and len(mismatches) >= 5:
            break

    return {"market": f"{cid}-{mid}", "n_accruals": n, "checked": checked,
            "mismatches": mismatches}


def pick_samples():
    """ETH size spread + Base spread + crisis market, by file size proxy."""
    def by_chain(prefix):
        files = sorted((DATA / "markets").glob(f"{prefix}-0x*.csv"),
                       key=lambda p: p.stat().st_size)
        return files

    eth = by_chain("1")
    base = by_chain("8453")
    picks = []
    for f in (eth[len(eth) // 4], eth[len(eth) // 2],
              eth[3 * len(eth) // 4], eth[-1],
              base[len(base) // 2], base[-1]):
        cid, mid = f.stem.split("-", 1)
        picks.append((int(cid), mid))
    picks.extend(CRISIS_MARKETS)
    seen, out = set(), []
    for cm in picks:
        if cm not in seen:
            seen.add(cm)
            out.append(cm)
    return out


def main():
    samples = pick_samples()
    print(f"validating {len(samples)} markets: " +
          ", ".join(f"{c}-{m[:10]}…" for c, m in samples))
    results = []
    for cid, mid in samples:
        results.append(validate_market(cid, mid))
        r = results[-1]
        if "skipped" in r:
            print(f"    -> skipped: {r['skipped']}")
        else:
            print(f"    -> {r['checked']} transitions, "
                  f"{len(r['mismatches'])} mismatches")

    outdir = DATA / "replay"
    outdir.mkdir(exist_ok=True)
    with open(outdir / "irm-validation.json", "w") as f:
        json.dump(results, f, indent=1)

    total = sum(r.get("checked", 0) for r in results)
    bad = sum(len(r.get("mismatches", [])) for r in results)
    print(f"\nIRM VALIDATION: {total} accrual transitions checked, "
          f"{bad} mismatches "
          f"({'PASS — wei-exact' if bad == 0 else 'FAIL'})")


if __name__ == "__main__":
    main()
