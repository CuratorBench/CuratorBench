"""Replay operator R: rate-impact-aware counterfactual replay."""

import csv
import heapq
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "data"))
import irm

DATA = HERE.parents[1] / "data"
PARAMS = json.load(open(DATA / "markets" / "params.json"))


class MarketSim:
    """Counterfactual state machine for one Blue market."""

    def __init__(self, cid, mid, state, rate_at_target, last_update_ts):
        self.cid, self.mid = cid, mid
        self.tsa, self.tss, self.tba, self.tbs = state
        self.rate_at_target = rate_at_target
        self.last_update_ts = last_update_ts
        self.clips = 0
        self.last_avg_rate = 0


    def liquidity(self):
        return max(self.tsa - self.tba, 0)

    def utilization_wad(self):
        if self.tsa <= 0:
            return 0
        return irm.w_div_down(self.tba, self.tsa)

    def shares_to_assets(self, shares):
        return irm.to_assets_down(shares, self.tsa, self.tss)


    def accrue(self, ts, u_hist_wad=None, elasticity_w=0):
        """Accrue to timestamp ts through the ported IRM."""
        elapsed = ts - self.last_update_ts
        if elapsed <= 0:
            return 0
        tba_eff = self.tba
        if elasticity_w and u_hist_wad is not None:
            u_cf = self.utilization_wad()
            u_eff = ((irm.WAD - elasticity_w) * u_cf
                     + elasticity_w * u_hist_wad) // irm.WAD
            tba_eff = irm.w_mul_down(u_eff, self.tsa)
        avg_rate, end_rat = irm.borrow_rate(self.rate_at_target, self.tsa,
                                            tba_eff, elapsed)
        interest = irm.w_mul_down(
            tba_eff, irm.w_taylor_compounded(avg_rate, elapsed))
        self.tba += interest
        self.tsa += interest
        self.rate_at_target = end_rat
        self.last_update_ts = ts
        self.last_avg_rate = avg_rate
        return interest


    def apply_foreign(self, kind, d_tsa, d_tss, d_tba, d_tbs, convention):
        if convention == "recorded" or kind == "liquidate":
            self.tsa += d_tsa
            self.tss += d_tss
            self.tba += d_tba
            self.tbs += d_tbs
            self._floor()
            return
        if kind in ("supply", "withdraw"):
            by_assets = convention in ("assets", "mixed")
            if kind == "supply":
                if by_assets:
                    assets = d_tsa
                    shares = irm.to_shares_down(assets, self.tsa, self.tss)
                else:
                    shares = d_tss
                    assets = irm.to_assets_up(shares, self.tsa, self.tss)
                self.tsa += assets
                self.tss += shares
            else:
                if by_assets:
                    assets = -d_tsa
                    shares = irm.to_shares_up(assets, self.tsa, self.tss)
                else:
                    shares = -d_tss
                    assets = irm.to_assets_down(shares, self.tsa, self.tss)
                if assets > self.liquidity():
                    self.clips += 1
                    assets = self.liquidity()
                    shares = irm.to_shares_up(assets, self.tsa, self.tss)
                self.tsa -= assets
                self.tss -= shares
        elif kind in ("borrow", "repay"):
            by_shares = convention in ("shares", "mixed")
            if kind == "borrow":
                if by_shares:
                    shares = d_tbs
                    assets = irm.to_assets_down(shares, self.tba, self.tbs)
                else:
                    assets = d_tba
                    shares = irm.to_shares_up(assets, self.tba, self.tbs)
                if assets > self.liquidity():
                    self.clips += 1
                    assets = self.liquidity()
                    shares = irm.to_shares_up(assets, self.tba, self.tbs)
                self.tba += assets
                self.tbs += shares
            else:
                if by_shares:
                    shares = -d_tbs
                    assets = irm.to_assets_up(shares, self.tba, self.tbs)
                else:
                    assets = -d_tba
                    shares = irm.to_shares_down(assets, self.tba, self.tbs)
                self.tba -= assets
                self.tbs -= shares
        self._floor()

    def _floor(self):
        if self.tba < 0:
            self.tba = 0
        if self.tbs < 0:
            self.tbs = 0
        if self.tsa < 0:
            self.tsa = 0
        if self.tss < 0:
            self.tss = 0


    def policy_supply(self, assets):
        shares = irm.to_shares_down(assets, self.tsa, self.tss)
        self.tsa += assets
        self.tss += shares
        return shares

    def policy_withdraw_assets(self, assets):
        """Withdraw up to `assets`, clipped at liquidity."""
        avail = self.liquidity()
        if assets > avail:
            self.clips += 1
            assets = avail
        shares = irm.to_shares_up(assets, self.tsa, self.tss)
        self.tsa -= assets
        self.tss -= shares
        return assets, shares

    def policy_withdraw_shares(self, shares):
        assets = irm.to_assets_down(shares, self.tsa, self.tss)
        avail = self.liquidity()
        if assets > avail:
            self.clips += 1
            assets = avail
            shares = irm.to_shares_up(assets, self.tsa, self.tss)
        self.tsa -= assets
        self.tss -= shares
        return assets, shares

    def apply_recorded_own(self, d_tsa, d_tss):
        """Identity mode: re-issue the vault's own recorded supply/withdraw by raw deltas."""
        self.tsa += d_tsa
        self.tss += d_tss
        return d_tss


def load_market_rows(cid, mid, upto_block=None):
    """Panel rows as tuples (block, log_index, kind, tsa, tss, tba, tbs)."""
    rows = []
    with open(DATA / "markets" / f"{cid}-{mid}.csv") as f:
        for r in csv.reader(f):
            if r[0] == "block":
                continue
            b = int(r[0])
            if upto_block is not None and b > upto_block:
                break
            rows.append((b, int(r[1]), r[2], int(r[3]), int(r[4]),
                         int(r[5]), int(r[6])))
    return rows


def load_vault_events(cid, addr, mids=None):
    """Trajectory events: (block, log_index, market_id, kind, assets, shares_delta)."""
    out = []
    with open(DATA / "trajectories" / f"{cid}-{addr}.events.csv") as f:
        for r in csv.DictReader(f):
            if mids is not None and r["market_id"] not in mids:
                continue
            out.append((int(r["block"]), int(r["log_index"]),
                        r["market_id"], r["kind"], int(r["assets"]),
                        int(r["shares_delta"])))
    return out


class Replay:
    """One replay run of a policy over a vault's market universe."""

    def __init__(self, cid, vault_addr, start_block, end_block,
                 timestamps, convention="mixed", elasticity_w=0):
        """timestamps: dict block -> exact unix ts covering every event block in [start_block, end_block] plus each market's last a."""
        self.cid = cid
        self.vault = vault_addr.lower()
        self.b0, self.b1 = start_block, end_block
        self.ts = timestamps
        self.convention = convention
        self.elasticity_w = elasticity_w

        self.vault_events = load_vault_events(cid, self.vault)
        self.mids = sorted({e[2] for e in self.vault_events})
        self.own_keys = {(e[0], e[1]) for e in self.vault_events}

        self.markets = {}
        self.hist = {}
        self.shares = {}
        self.hist_shares = {}
        self.idle = 0
        self.nav_path = []
        self._setup()


    def _setup(self):
        for mid in self.mids:
            rows = load_market_rows(self.cid, mid, upto_block=self.b1)
            pre = [r for r in rows if r[0] <= self.b0]
            window = [r for r in rows if r[0] > self.b0]
            if pre:
                state = pre[-1][3:7]
                last_acc = next((r for r in reversed(pre)
                                 if r[2] in ("accrue", "create")), None)
                last_ts = self.ts[last_acc[0]] if last_acc else self.ts[self.b0]
            else:
                state = (0, 0, 0, 0)
                last_ts = self.ts.get(self.b0, 0)
            rat = seed_rate_at_target(self.cid, mid, self.b0)
            self.markets[mid] = MarketSim(self.cid, mid, state, rat, last_ts)
            ev = []
            prev = state
            for r in window:
                b, li, kind = r[0], r[1], r[2]
                post = r[3:7]
                deltas = tuple(post[i] - prev[i] for i in range(4))
                ev.append((b, li, kind, deltas, prev, post))
                prev = post
            self.hist[mid] = ev

            h = 0
            for e in self.vault_events:
                if e[2] == mid and e[0] <= self.b0:
                    h += e[5]
            self.hist_shares[mid] = h
            self.shares[mid] = h


    def run(self, policy, decision_ts_grid=(), audit_hook=None):
        """Merge all market event streams + decision grid, process in order."""
        heap = []
        for mid, evs in self.hist.items():
            for e in evs:
                heapq.heappush(heap, (e[0], e[1], "hist", mid, e))
        own_by_key = {}
        for e in self.vault_events:
            if self.b0 < e[0] <= self.b1:
                own_by_key[(e[0], e[1])] = e
        grid = sorted(t for t in decision_ts_grid)
        gi = 0

        while heap:
            block, li, _tag, mid, e = heapq.heappop(heap)
            ets = self.ts.get(block)
            while (ets is not None and gi < len(grid)
                   and grid[gi] < ets):
                self._decide(policy, grid[gi], block)
                gi += 1
            self._process(block, li, mid, e, own_by_key, policy)
            if audit_hook is not None:
                audit_hook(self, block, li, mid, e[5])
        while gi < len(grid):
            self._decide(policy, grid[gi], self.b1)
            gi += 1
        return self

    def _decide(self, policy, ts, block):
        for m in self.markets.values():
            m.accrue(ts, elasticity_w=0)
        if hasattr(policy, "decide"):
            policy.decide(self, ts, block)
        self._record_nav(block, ts)

    def _process(self, block, li, mid, e, own_by_key, policy):
        _b, _li, kind, deltas, hist_pre, _post = e
        m = self.markets[mid]
        ets = self.ts.get(block)
        if kind in ("accrue", "create") and ets is None:
            raise KeyError(f"exact timestamp required for {kind} "
                           f"at block {block}")
        if kind == "accrue":
            u_hist = (irm.w_div_down(hist_pre[2], hist_pre[0])
                      if hist_pre[0] > 0 else 0)
            m.accrue(ets, u_hist_wad=u_hist, elasticity_w=self.elasticity_w)
        elif (block, li) in self.own_keys:
            oe = own_by_key.get((block, li))
            self.hist_shares[mid] += oe[5]
            if hasattr(policy, "on_own_event"):
                policy.on_own_event(self, mid, oe, deltas)
        elif kind == "create":
            m.last_update_ts = ets
            m.rate_at_target = irm.INITIAL_RATE_AT_TARGET
        else:
            m.apply_foreign(kind, *deltas, self.convention)
        if ets is not None:
            self._record_nav(block, ets)


    def nav(self):
        return self.idle + sum(
            self.markets[mid].shares_to_assets(s)
            for mid, s in self.shares.items() if s > 0)

    def _record_nav(self, block, ts):
        self.nav_path.append((block, ts, self.nav()))

    def total_clips(self):
        return sum(m.clips for m in self.markets.values())


def seed_rate_at_target(cid, mid, block, _cache={}):
    """rateAtTarget entering `block`."""
    key = (cid, mid, block)
    if key in _cache:
        return _cache[key]
    from Crypto.Hash import keccak
    from legacy_rpc import get_logs
    topic = "0x" + keccak.new(
        data=b"BorrowRateUpdate(bytes32,uint256,uint256)",
        digest_bits=256).hexdigest()
    irm_addr = PARAMS[f"{cid}-{mid}"]["irm"]
    if int(irm_addr, 16) == 0:
        _cache[key] = 0
        return 0
    from legacy_rpc import words
    span = 50_000
    hi = block
    while hi > 0 and span < 40_000_000:
        lo = max(hi - span, 0)
        logs = get_logs(irm_addr, [topic, mid], lo, hi, chain_id=cid)
        if logs:
            lg = max(logs, key=lambda g: (int(g["blockNumber"], 16),
                                          int(g["logIndex"], 16)))
            _cache[key] = words(lg["data"])[1]
            return _cache[key]
        hi = lo
        span *= 4
    _cache[key] = 0
    return 0


def prepare_timestamp_blocks(cid, vault_addr, b0, b1, accruals_only=False):
    """Blocks whose exact timestamps a Replay needs."""
    vault_addr = vault_addr.lower()
    events = load_vault_events(cid, vault_addr)
    mids = sorted({e[2] for e in events})
    blocks = {b0, b1}
    for mid in mids:
        rows = load_market_rows(cid, mid, upto_block=b1)
        pre = [r for r in rows if r[0] <= b0]
        if pre:
            last_acc = next((r for r in reversed(pre)
                             if r[2] in ("accrue", "create")), None)
            if last_acc:
                blocks.add(last_acc[0])
        blocks.update(r[0] for r in rows if r[0] > b0
                      and (not accruals_only or r[2] in ("accrue", "create")))
    return sorted(blocks)


class IdentityPolicy:
    """Re-issues the vault's own recorded actions (raw deltas)."""

    def on_own_event(self, engine, mid, oe, deltas):
        m = engine.markets[mid]
        d_tsa, d_tss = deltas[0], deltas[1]
        m.apply_recorded_own(d_tsa, d_tss)
        engine.shares[mid] += oe[5]

