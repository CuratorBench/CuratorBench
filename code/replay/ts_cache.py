"""Persistent exact-block-timestamp cache (data/blocktime/exact-{chain}.csv)."""

import csv
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE.parent / "data"))
from legacy_rpc import batch_timestamps

DATA = HERE.parents[1] / "data"


class TsCache:
    def __init__(self, cid):
        self.cid = cid
        self.path = DATA / "blocktime" / f"exact-{cid}.csv"
        self.ts = {}
        if self.path.exists():
            with open(self.path) as f:
                for row in csv.reader(f):
                    if row and row[0] != "block":
                        self.ts[int(row[0])] = int(row[1])

    def ensure(self, blocks, quiet=False):
        """Fetch any missing blocks (batched), append them to disk, and return the full block->ts dict (shared; do not mutate)."""
        missing = sorted(set(blocks) - self.ts.keys())
        if missing:
            got = batch_timestamps(self.cid, missing, quiet=quiet)
            self.path.parent.mkdir(exist_ok=True)
            new_file = not self.path.exists()
            with open(self.path, "a") as f:
                w = csv.writer(f)
                if new_file:
                    w.writerow(["block", "timestamp"])
                for b in missing:
                    w.writerow([b, got[b]])
            self.ts.update(got)
        return self.ts
