# /// script
# requires-python = ">=3.11"
# ///
"""Metrics layer: daily per-vault series from trajectories x panel."""

import csv
import json
import pathlib
import sys
from bisect import bisect_right

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE.parent / "data"))

DATA = HERE.parents[1] / "data"
OUT = DATA / "metrics" / "vault-daily"
DAY = 86400


class BlockClock:
    """Linear interpolation over the sparse anchor table (block_time.py)."""

    def __init__(self, cid):
        rows = list(csv.reader(open(DATA / "blocktime" / f"{cid}.csv")))
        pts = [(int(r[0]), int(r[1])) for r in rows[1:]]
        self.blocks = [p[0] for p in pts]
        self.ts = [p[1] for p in pts]

    def to_ts(self, block):
        i = max(1, bisect_right(self.blocks, block) - 1)
        i = min(i, len(self.blocks) - 1)
        b0, b1 = self.blocks[i - 1], self.blocks[i]
        t0, t1 = self.ts[i - 1], self.ts[i]
        return t0 + (block - b0) * (t1 - t0) / max(b1 - b0, 1)

    def to_block(self, ts):
        i = max(1, bisect_right(self.ts, ts) - 1)
        i = min(i, len(self.ts) - 1)
        t0, t1 = self.ts[i - 1], self.ts[i]
        b0, b1 = self.blocks[i - 1], self.blocks[i]
        return int(b0 + (ts - t0) * (b1 - b0) / max(t1 - t0, 1))


_market_cache = {}


def market_daily(cid, mid, grid_blocks):
    """(price, idle, util) sampled at each grid block; ffill from events."""
    key = (cid, mid, grid_blocks)
    if key in _market_cache:
        return _market_cache[key]
    path = DATA / "markets" / f"{cid}-{mid}.csv"
    out = []
    if not path.exists():
        out = [(None, 0.0, 0.0)] * len(grid_blocks)
    else:
        gi = 0
        state = None
        with open(path) as f:
            for row in csv.reader(f):
                if row[0] == "block":
                    continue
                b = int(row[0])
                while gi < len(grid_blocks) and grid_blocks[gi] < b:
                    out.append(_snap(state))
                    gi += 1
                if gi >= len(grid_blocks):
                    break
                state = (int(row[3]), int(row[4]), int(row[5]))
        while gi < len(grid_blocks):
            out.append(_snap(state))
            gi += 1
    if len(_market_cache) > 400:
        _market_cache.clear()
    _market_cache[key] = out
    return out


def _snap(state):
    if not state or state[1] == 0:
        return (None, 0.0, 0.0)
    tsa, tss, tba = state
    price = tsa / tss
    idle = float(max(tsa - tba, 0))
    util = tba / tsa if tsa > 0 else 0.0
    return (price, idle, util)


def build_vault(key, clock_by_chain):
    cid_s, addr = key.split("-", 1)
    cid = int(cid_s)
    meta = json.load(open(DATA / "trajectories" / f"{key}.meta.json"))
    if meta["n_events"] == 0:
        return None
    events = []
    with open(DATA / "trajectories" / f"{key}.events.csv") as f:
        for r in csv.DictReader(f):
            events.append((int(r["block"]), r["market_id"],
                           int(r["shares_delta"])))
    mids = sorted({e[1] for e in events})

    clock = clock_by_chain[cid]
    t0 = (int(clock.to_ts(meta["creation_block"])) // DAY + 1) * DAY
    t1 = int(clock.to_ts(meta["head_block"])) // DAY * DAY
    if t1 <= t0:
        return None
    days = list(range(t0, t1 + 1, DAY))
    grid_blocks = [clock.to_block(t) for t in days]

    shares_on_grid = {}
    for mid in mids:
        evs = sorted((e[0], e[2]) for e in events if e[1] == mid)
        cum, gi, acc = [], 0, 0
        for b, d in evs:
            while gi < len(grid_blocks) and grid_blocks[gi] < b:
                cum.append(acc)
                gi += 1
            acc += d
        while gi < len(grid_blocks):
            cum.append(acc)
            gi += 1
        shares_on_grid[mid] = cum

    market_series = {mid: market_daily(cid, mid, tuple(grid_blocks))
                     for mid in mids}

    rows = []
    prev_shares, prev_prices = None, None
    for di, (ts, blk) in enumerate(zip(days, grid_blocks)):
        value = withdrawable = 0.0
        util_max = 0.0
        n_mkts = 0
        prices = {}
        for mid in mids:
            sh = shares_on_grid[mid][di]
            price, idle, util = market_series[mid][di]
            prices[mid] = price
            if sh <= 0 or price is None:
                continue
            v = sh * price
            value += v
            withdrawable += min(v, idle)
            util_max = max(util_max, util)
            n_mkts += 1
        ret = ""
        if prev_shares is not None:
            num = den = 0.0
            for mid in mids:
                sh = prev_shares[mid]
                p0, p1 = prev_prices.get(mid), prices.get(mid)
                if sh > 0 and p0 and p1:
                    den += sh * p0
                    num += sh * (p1 - p0)
            if den > 0:
                ret = num / den
        rows.append((ts, blk, value, ret,
                     (withdrawable / value) if value > 0 else "",
                     round(util_max, 6), n_mkts))
        prev_shares = {mid: shares_on_grid[mid][di] for mid in mids}
        prev_prices = prices

    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / f"{key}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "block", "value", "ret", "liq", "util_max",
                    "n_markets"])
        w.writerows(rows)
    active = sum(1 for r in rows if r[2] > 0)
    return {"vault": key, "name": meta["name"], "days": len(rows),
            "active_days": active}


def main():
    clock_by_chain = {1: BlockClock(1), 8453: BlockClock(8453)}
    metas = sorted(DATA.glob("trajectories/*.meta.json"))
    summary = []
    for i, mp in enumerate(metas):
        key = mp.stem.removesuffix(".meta")
        try:
            r = build_vault(key, clock_by_chain)
        except Exception as e:
            r = {"vault": key, "error": str(e)[:150]}
        if r:
            summary.append(r)
            tag = r.get("error", f"{r.get('active_days')}d active")
            print(f"[{i + 1}/{len(metas)}] {key} {tag}")
    with open(DATA / "metrics" / "series-summary.json", "w") as f:
        json.dump(summary, f, indent=1)
    ok = sum(1 for r in summary if "error" not in r)
    print(f"\nDONE: {ok} vault series built, "
          f"{sum(1 for r in summary if 'error' in r)} errors")


if __name__ == "__main__":
    main()
