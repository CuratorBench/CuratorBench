# /// script
# requires-python = ">=3.11"
# ///
"""Block <-> timestamp anchors for interpolation."""

import csv
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from legacy_rpc import block_number, block_timestamp

DATA = pathlib.Path(__file__).parents[2] / "data"
OUT = DATA / "blocktime"

RANGES = {1: (18_800_000, 50_000), 8453: (13_000_000, 200_000)}


def build(cid):
    start, step = RANGES[cid]
    head = block_number(chain_id=cid)
    path = OUT / f"{cid}.csv"
    known = {}
    if path.exists():
        with open(path) as f:
            for r in csv.DictReader(f):
                known[int(r["block"])] = int(r["timestamp"])
    blocks = list(range(start, head, step)) + [head]
    fetched = 0
    for b in blocks:
        if b not in known:
            known[b] = block_timestamp(b, chain_id=cid)
            fetched += 1
            time.sleep(0.05)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["block", "timestamp"])
        for b in sorted(known):
            w.writerow([b, known[b]])
    print(f"chain {cid}: {len(known)} anchors ({fetched} fetched), "
          f"head {head:,}")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for cid in RANGES:
        build(cid)
