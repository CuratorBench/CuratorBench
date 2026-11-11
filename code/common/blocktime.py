"""Timestamp -> block number resolution for archive sampling."""
import bisect

from . import rpc


class BlockClock:
    def __init__(self, target):
        self.target = target
        self.anchors = []
        latest = rpc.get_block(target, "latest")
        self.head = (int(latest["timestamp"], 16), int(latest["number"], 16))
        self._add(*self.head)
        head_n = self.head[1]
        ladder = [1, 0, 1000, 100_000, 1_000_000, 10_000_000, 50_000_000]
        ladder += [head_n - d for d in (50_000_000, 10_000_000, 1_000_000,
                                        100_000, 1000)]
        first = None
        for b in ladder:
            if b <= 0 or b >= head_n:
                continue
            try:
                first = rpc.get_block(target, b)
            except Exception:
                first = None
            if first:
                break
        if first is None:
            raise RuntimeError("no early block readable; cannot anchor clock")
        self._add(int(first["timestamp"], 16), int(first["number"], 16))

    def _add(self, ts, block):
        row = (ts, block)
        i = bisect.bisect_left(self.anchors, row)
        if i < len(self.anchors) and self.anchors[i] == row:
            return
        self.anchors.insert(i, row)

    def _fetch(self, block):
        block = max(0, min(block, self.head[1]))
        b = rpc.get_block(self.target, block)
        ts = int(b["timestamp"], 16)
        self._add(ts, block)
        return ts, block

    def prefetch(self, ts_lo, ts_hi, n=None, threads=24,
                 target_spacing=4 * 3600):
        """Anchor densely across [ts_lo, ts_hi] with threaded block fetches so block_at_fast can interpolate with no further fetches."""
        from concurrent.futures import ThreadPoolExecutor
        b_lo = self.block_at(ts_lo, tol_secs=6 * 3600)
        b_hi = self.block_at(ts_hi, tol_secs=6 * 3600)
        if b_hi <= b_lo:
            return
        if n is None:
            n = min(8000, max(300, (ts_hi - ts_lo) // target_spacing))
        step = max(1, (b_hi - b_lo) // n)
        want = list(range(b_lo, b_hi, step))

        def fetch(b):
            try:
                blk = rpc.get_block(self.target, b)
                return int(blk["timestamp"], 16), b
            except Exception:
                return None
        with ThreadPoolExecutor(max_workers=threads) as pool:
            for r in pool.map(fetch, want):
                if r:
                    self._add(*r)

    def block_at_fast(self, ts):
        """Pure interpolation between the nearest anchors."""
        if ts >= self.head[0]:
            return self.head[1]
        i = bisect.bisect_right(self.anchors, (ts, 1 << 62))
        lo = self.anchors[i - 1] if i > 0 else self.anchors[0]
        hi = self.anchors[i] if i < len(self.anchors) else self.head
        if lo[0] >= ts or hi[0] <= lo[0]:
            return lo[1]
        frac = (ts - lo[0]) / (hi[0] - lo[0])
        return lo[1] + int(frac * (hi[1] - lo[1]))

    def block_at(self, ts, tol_secs=1800):
        """Last block at or before ts (clamped to chain range)."""
        if ts >= self.head[0]:
            return self.head[1]
        i = bisect.bisect_right(self.anchors, (ts, 1 << 62))
        lo = self.anchors[i - 1] if i > 0 else self.anchors[0]
        hi = self.anchors[i] if i < len(self.anchors) else self.head
        if lo[0] >= ts:
            return lo[1]
        for _ in range(30):
            if hi[0] == lo[0] or hi[1] - lo[1] <= 1:
                return lo[1]
            frac = (ts - lo[0]) / (hi[0] - lo[0])
            guess = lo[1] + int(frac * (hi[1] - lo[1]))
            guess = min(max(guess, lo[1] + 1), hi[1] - 1)
            gts, gb = self._fetch(guess)
            if gts <= ts:
                lo = (gts, gb)
                if ts - gts <= tol_secs:
                    return gb
            else:
                hi = (gts, gb)
        return lo[1]

    def block_at_verified(self, ts, tol_secs=600):
        """block_at(), checked against the answer block's own timestamp and, if off, corrected by true bisection between the surrounding anchors."""
        b = self.block_at(ts, tol_secs=tol_secs)
        try:
            bt, _ = self._fetch(b)
        except Exception:
            bt = None
        if bt is not None and bt <= ts and ts - bt <= max(tol_secs, 3600):
            return b
        lo, hi = 1, self.head[1]
        for a_ts, a_b in self.anchors:
            if a_ts <= ts:
                lo = max(lo, a_b)
            else:
                hi = min(hi, a_b)
        while hi - lo > 1:
            mid = (lo + hi) // 2
            mt, _ = self._fetch(mid)
            if mt <= ts:
                lo = mid
            else:
                hi = mid
        return lo
