# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "pycryptodome"]
# ///
"""Counterfactual replay for policies that may allocate to any market."""
import argparse
import csv
import heapq
import json
import math
import random
import sys
import time

import context as C

sys.path.insert(0, str(C.ROOT / "code" / "replay"))
import irm
from engine import MarketSim

WAD = irm.WAD
YEAR = C.YEAR
DAY = C.DAY

KINDS = {"create": 0, "accrue": 1, "supply": 2, "withdraw": 3, "borrow": 4, "repay": 5,
         "supply_collateral": 6, "withdraw_collateral": 7, "liquidate": 8,
         "broker_liquidate": 9, "set_fee": 10}
KIND_NAME = {v: k for k, v in KINDS.items()}
FOREIGN = {2, 3, 4, 5}
PRICINGS = ("impact", "displace")
RAW = {8, 9}

MOOLAH_ADAPTIVE = "0xfe7dae87ebb11a7beb9f534bb23267992d9cde7c"
MOOLAH_FIXED = "0x5f9f9173b405c6ceafa7f98d09e4b8447e9797e6"
MOOLAH_GLOBAL_CAP = int(0.3e18) // YEAR
MOOLAH_GLOBAL_CAP_FROM = (62355178, 554)
MOOLAH_END_CAP_FROM_TS = 1763631588
MOOLAH_END_FLOOR_FROM_TS = 1774938600
SEED = 20261009


class MorphoRates:
    """AdaptiveCurveIRM, or no interest for an IRM-less market."""

    def __init__(self, irm_addr):
        self.none = not irm_addr or int(irm_addr, 16) == 0

    def __call__(self, sim, rat, tsa, tba, elapsed, key, ts):
        if self.none:
            return 0, rat
        return irm.borrow_rate(rat, tsa, tba, elapsed)

    def cap(self, key, ts):
        return None


class MoolahParams:
    """rate_cap and rate_floor per market, min_cap, in (block, log_index) order."""

    def __init__(self, cid=56):
        self.ev = {}
        p = C.DATA / f"panel/moolah/{cid}/irm_params.csv"
        rows = []
        with open(p) as fh:
            for r in csv.DictReader(fh):
                rows.append((int(r["block"]), int(r["log_index"]), int(r["timestamp"]),
                             r["market_id"], r["param"], int(r["new"])))
        rows.sort()
        self.rows = rows
        self.by_market = {}
        for b, li, ts, m, name, new in rows:
            self.by_market.setdefault(m, []).append((b, li, ts, name, new))

    def at(self, m, key, ts):
        """(cap, floor) in force before the row `key` = (block, log_index), or at timestamp ts when key is None (a decision-time accrual)."""
        cap = floor = 0
        for b, li, t, name, new in self.by_market.get(m, []):
            if key is not None:
                if (b, li) >= key:
                    break
            elif t > ts:
                break
            if name == "rate_cap":
                cap = new
            elif name == "rate_floor":
                floor = new
        if cap == 0:
            global_on = (key >= MOOLAH_GLOBAL_CAP_FROM) if key is not None else ts >= 1758765298
            cap = MOOLAH_GLOBAL_CAP if global_on else None
        return cap, floor


class MoolahRates:
    """Lista's AdaptiveCurveIRM fork with per-market cap and floor (see the module docstring), or the fixed-rate IRM."""

    def __init__(self, market, irm_addr, params):
        self.m = market
        self.kind = ("fixed" if irm_addr == MOOLAH_FIXED else
                     "none" if not irm_addr or int(irm_addr, 16) == 0 else "adaptive")
        self.params = params
        self.fixed_rate = 0

    def __call__(self, sim, rat, tsa, tba, elapsed, key, ts):
        if self.kind == "none":
            return 0, rat
        if self.kind == "fixed":
            return self.fixed_rate, rat
        avg, end = irm.borrow_rate(rat, tsa, tba, elapsed)
        cap, floor = self.params.at(self.m, key, ts)
        if cap is not None:
            avg = min(avg, cap)
        avg = max(avg, floor)
        if ts >= MOOLAH_END_FLOOR_FROM_TS:
            end = max(end, 4 * floor)
        if ts >= MOOLAH_END_CAP_FROM_TS and cap is not None:
            end = min(end, cap // 4)
        return avg, end

    def cap(self, key, ts):
        if self.kind != "adaptive":
            return None
        return self.params.at(self.m, key, ts)


class Sim(MarketSim):
    """MarketSim with a pluggable rate model and Morpho Blue's fee shares."""

    def __init__(self, cid, mid, state, rat, last_ts, fee, rates):
        super().__init__(cid, mid, state, rat, last_ts)
        self.fee = fee
        self.rates = rates
        self.taker = False
        self.u_hist_wad = None

    def _taker_tba(self):
        u = self.u_hist_wad if self.u_hist_wad is not None else self.utilization_wad()
        return irm.w_mul_down(u, self.tsa)

    def accrue(self, ts, u_hist_wad=None, elasticity_w=0, key=None):
        if self.last_update_ts is None:
            self.last_update_ts = ts
            return 0
        elapsed = ts - self.last_update_ts
        if elapsed <= 0:
            return 0
        tba_eff = self.tba
        if self.taker:
            tba_eff = self._taker_tba()
            avg, end = self.rates(self, self.rate_at_target, self.tsa, tba_eff, elapsed, key, ts)
            interest = irm.w_mul_down(tba_eff, irm.w_taylor_compounded(avg, elapsed))
            self.tba += irm.w_mul_down(self.tba, irm.w_taylor_compounded(avg, elapsed))
            self.tsa += interest
            if self.fee and interest:
                fee_amount = irm.w_mul_down(interest, self.fee)
                self.tss += irm.to_shares_down(fee_amount, self.tsa - fee_amount, self.tss)
            self.rate_at_target = end
            self.last_update_ts = ts
            self.last_avg_rate = avg
            return interest
        if elasticity_w and u_hist_wad is not None:
            u_cf = self.utilization_wad()
            u_eff = ((WAD - elasticity_w) * u_cf + elasticity_w * u_hist_wad) // WAD
            tba_eff = irm.w_mul_down(u_eff, self.tsa)
        avg, end = self.rates(self, self.rate_at_target, self.tsa, tba_eff, elapsed, key, ts)
        interest = irm.w_mul_down(tba_eff, irm.w_taylor_compounded(avg, elapsed))
        self.tba += interest
        self.tsa += interest
        if self.fee and interest:
            fee_amount = irm.w_mul_down(interest, self.fee)
            self.tss += irm.to_shares_down(fee_amount, self.tsa - fee_amount, self.tss)
        self.rate_at_target = end
        self.last_update_ts = ts
        self.last_avg_rate = avg
        return interest

    def view(self, ts):
        """(tsa, tss, tba, avg rate) as if accrued to ts, without mutating."""
        if self.last_update_ts is None or ts <= self.last_update_ts:
            return self.tsa, self.tss, self.tba, self.last_avg_rate
        elapsed = ts - self.last_update_ts
        tba_eff = self._taker_tba() if self.taker else self.tba
        avg, _end = self.rates(self, self.rate_at_target, self.tsa, tba_eff, elapsed, None, ts)
        interest = irm.w_mul_down(tba_eff, irm.w_taylor_compounded(avg, elapsed))
        tsa, tss = self.tsa + interest, self.tss
        if self.fee and interest:
            fee_amount = irm.w_mul_down(interest, self.fee)
            tss += irm.to_shares_down(fee_amount, tsa - fee_amount, tss)
        tba_v = self.tba + (irm.w_mul_down(self.tba, irm.w_taylor_compounded(avg, elapsed)) if self.taker else interest)
        return tsa, tss, tba_v, avg

    def rate_at(self, tsa, tba, ts):
        """Instantaneous borrow rate per second at a hypothetical state (no adaptation of the rate at target)."""
        if isinstance(self.rates, MoolahRates) and self.rates.kind == "fixed":
            return self.rates.fixed_rate
        if getattr(self.rates, "none", False) or getattr(self.rates, "kind", "") == "none":
            return 0
        if tsa <= 0:
            return 0
        u = irm.w_div_down(min(tba, tsa), tsa)
        err_norm = (WAD - irm.TARGET_UTILIZATION if u > irm.TARGET_UTILIZATION
                    else irm.TARGET_UTILIZATION)
        err = irm.w_div_to_zero(u - irm.TARGET_UTILIZATION, err_norm)
        rat = self.rate_at_target or irm.INITIAL_RATE_AT_TARGET
        r = irm._curve(rat, err)
        cf = self.rates.cap(None, ts) if hasattr(self.rates, "cap") else None
        if cf is not None:
            cap, floor = cf
            if cap is not None:
                r = min(r, cap)
            r = max(r, floor)
        return r


def supply_apy(rate_per_s, util, fee):
    """Lenders' APY from a per-second borrow rate (WAD), utilization and fee."""
    r = rate_per_s / 1e18 * YEAR
    return (math.exp(min(r, 50.0)) - 1.0) * util * (1.0 - fee)


def read_market_rows(proto, cid, mid, start_ts, stop_ts):
    """(pre, window): the state entering start_ts and the rows in (start_ts, stop_ts] as tuples (block, li, mi placeholder, ts,."""
    path = C.e2.panel_dir(proto, cid) / f"{mid}.csv"
    pre = {"state": (0, 0, 0, 0), "rat": 0, "last_ts": None, "fee": 0, "rate": 0,
           "created": False}
    window = []
    with open(path, newline="") as fh:
        rd = csv.reader(fh)
        head = next(rd)
        ix = {k: i for i, k in enumerate(head)}
        b_, l_, t_, k_ = ix["block"], ix["log_index"], ix["timestamp"], ix["kind"]
        s_, ss_, bo_, bs_ = (ix["total_supply_assets"], ix["total_supply_shares"],
                             ix["total_borrow_assets"], ix["total_borrow_shares"])
        r_, rat_, f_ = ix["borrow_rate"], ix["rate_at_target"], ix["fee"]
        for row in rd:
            ts = int(row[t_])
            if ts > stop_ts:
                break
            kind = KINDS.get(row[k_], -1)
            br = int(row[r_]) if row[r_] else None
            rat = int(row[rat_]) if row[rat_] else None
            fee = int(row[f_]) if row[f_] else None
            state = (int(row[s_]), int(row[ss_]), int(row[bo_]), int(row[bs_]))
            if ts <= start_ts:
                pre["state"] = state
                if kind in (0, 1):
                    pre["last_ts"] = ts
                    pre["created"] = True
                if rat is not None:
                    pre["rat"] = rat
                if br is not None:
                    pre["rate"] = br
                if fee is not None:
                    pre["fee"] = fee
                continue
            window.append((int(row[b_]), int(row[l_]), 0, ts, kind) + state + (br, rat, fee))
    return pre, window


_ROWS = {}


def clear_row_cache():
    _ROWS.clear()


class ExactReplay:
    """Integer-exact multi-market replay for Morpho and Moolah (module docstring)."""

    approximate = False
    taker = False

    def __init__(self, proto, cid, markets, start_ts, stop_ts, convention="mixed",
                 elasticity_w=0, own_rows=None, moolah_params=None, hist_rate_at_target=True, pricing="impact"):
        self.proto, self.cid = proto, int(cid)
        self.markets = list(markets)
        self.start_ts, self.stop_ts = start_ts, stop_ts
        self.convention = convention
        self.elasticity_w = elasticity_w
        self.hist_rat = hist_rate_at_target
        if pricing not in PRICINGS:
            raise ValueError(pricing)
        self.taker = pricing == "displace"
        self.own = own_rows or {}
        mk = C.Markets(proto, cid)
        params = moolah_params or (MoolahParams(cid) if proto == "moolah" else None)
        self.sims, self.rows, self.created = [], [], []
        for i, m in enumerate(self.markets):
            key = (proto, self.cid, m, i, start_ts, stop_ts)
            if key not in _ROWS:
                pre, window = read_market_rows(proto, cid, m, start_ts, stop_ts)
                _ROWS[key] = (pre, [r[:2] + (i,) + r[3:] for r in window])
            pre, rows_i = _ROWS[key]
            irm_addr = mk.irm.get(m)
            rates = MoolahRates(m, irm_addr, params) if proto == "moolah" else MorphoRates(irm_addr)
            if isinstance(rates, MoolahRates) and rates.kind == "fixed":
                rates.fixed_rate = pre["rate"]
            sim = Sim(cid, m, pre["state"], pre["rat"], pre["last_ts"], pre["fee"] or 0, rates)
            sim.taker = self.taker
            self.sims.append(sim)
            self.created.append(pre["created"])
            self.rows.append(rows_i)
        self.n = len(self.markets)
        self._ix = {m: i for i, m in enumerate(self.markets)}
        self.prev = [s.tsa for s in self.sims]
        self.prev_state = [(s.tsa, s.tss, s.tba, s.tbs) for s in self.sims]
        self.shares = [0] * self.n
        self.ledger = [0] * self.n
        self.writeoff = [0] * self.n
        self.idle = 0
        self.clips = 0
        self.foreign_clips = 0
        self.actions = []
        self.mismatches = []
        self.n_rows = sum(len(r) for r in self.rows)
        for i in range(self.n):
            self._set_eff(i)

    def index(self, m):
        return self._ix[m]

    def day_state(self, ts, markets=None):
        """(idle, {market: (position, cfi, utilization, exit liquidity)}) at ts for the markets the vault holds (and `markets`, held or not)."""
        out = {}
        want = {i for i in range(self.n) if self.shares[i] > 0}
        if markets:
            want |= {self._ix[m] for m in markets if m in self._ix}
        for i in sorted(want):
            tsa, tss, tba, _ = self.sims[i].view(ts)
            pos = irm.to_assets_down(self.shares[i], tsa, tss) if self.shares[i] > 0 else 0
            u = self._hist_util(i) if self.taker else (tba / tsa if tsa > 0 else 0.0)
            out[self.markets[i]] = (float(pos), (tsa + 1) / (tss + 10 ** 6), u, float(self.exit_liquidity(i)))
        return float(self.idle), out


    def run(self, grid, hook, audit=False):
        """Process every row; call hook(self, ts) at each grid timestamp (sorted), before the first row with a timestamp at or after it."""
        grid = sorted(set(int(g) for g in grid))
        gi, ng = 0, len(grid)
        for row in heapq.merge(*self.rows):
            b, li, mi, ts, kind, tsa, tss, tba, tbs, br, rat, fee = row
            while gi < ng and grid[gi] <= ts:
                hook(self, grid[gi])
                gi += 1
            self._process(b, li, mi, ts, kind, (tsa, tss, tba, tbs), br, rat, fee)
            if audit:
                s = self.sims[mi]
                if (s.tsa, s.tss, s.tba, s.tbs) != (tsa, tss, tba, tbs) and len(self.mismatches) < 20:
                    self.mismatches.append({"block": b, "log_index": li, "market": self.markets[mi],
                                            "kind": KIND_NAME.get(kind)})
        while gi < ng:
            hook(self, grid[gi])
            gi += 1
        return self

    def _process(self, b, li, mi, ts, kind, post, br, rat, fee):
        self._process_row(b, li, mi, ts, kind, post, br, rat, fee)
        self._set_eff(mi)

    def _process_row(self, b, li, mi, ts, kind, post, br, rat, fee):
        s = self.sims[mi]
        pre = self.prev_state[mi]
        self.prev_state[mi] = post
        if kind == 1:
            if isinstance(s.rates, MoolahRates) and s.rates.kind == "fixed" and br is not None:
                s.rates.fixed_rate = br
            u_hist = irm.w_div_down(pre[2], pre[0]) if pre[0] > 0 else 0
            s.accrue(ts, u_hist_wad=u_hist, elasticity_w=self.elasticity_w, key=(b, li))
            if self.hist_rat and rat is not None:
                s.rate_at_target = rat
            return
        if (b, li) in self.own:
            d_tsa, d_tss = post[0] - pre[0], post[1] - pre[1]
            s.apply_recorded_own(d_tsa, d_tss)
            self.shares[mi] += self.own[(b, li)]
            return
        if kind == 0:
            s.last_update_ts = ts
            s.rate_at_target = rat if rat is not None else irm.INITIAL_RATE_AT_TARGET
            if fee is not None:
                s.fee = fee
            self.created[mi] = True
            return
        if kind == 10:
            if fee is not None:
                s.fee = fee
            return
        deltas = tuple(post[i] - pre[i] for i in range(4))
        if kind == 3 and self.shares[mi] > 0 and self.convention in ("assets", "mixed"):
            assets = -deltas[0]
            foreign = s.tss - self.shares[mi]
            if irm.to_shares_up(assets, s.tsa, s.tss) > foreign:
                assets = irm.to_assets_down(max(foreign, 0), s.tsa, s.tss)
                self.foreign_clips += 1
                deltas = (-assets,) + deltas[1:]
        if kind in FOREIGN:
            s.apply_foreign(KIND_NAME[kind], *deltas, self.convention)
            if s.tss < self.shares[mi]:
                s.tss = self.shares[mi]
        elif kind in RAW:
            if self.convention != "recorded" and deltas[3] < 0 and pre[2] > 0 and pre[3] > 0 and s.tbs > 0:
                num, den = s.tba * pre[3], s.tbs * pre[2]
                deltas = (deltas[0] * num // den, deltas[1], deltas[2] * num // den, deltas[3])
            if self.taker and deltas[0] < 0 and pre[0] > 0:
                pos = irm.to_assets_down(self.shares[mi], s.tsa, s.tss) if self.shares[mi] > 0 else 0
                deltas = (deltas[0] * s.tsa // max(pre[0], pos),) + deltas[1:]
            if deltas[0] < 0 and self.shares[mi] > 0 and s.tss > 0:
                self.writeoff[mi] += -deltas[0] * self.shares[mi] // s.tss
            s.apply_foreign("liquidate", *deltas, self.convention)

    def _eff_supply(self, i):
        """Displacement pricing."""
        s = self.sims[i]
        pos = irm.to_assets_down(self.shares[i], s.tsa, s.tss) if self.shares[i] > 0 else 0
        return max(self.prev_state[i][0], pos)

    def _set_eff(self, i):
        if self.taker:
            sup = self._eff_supply(i)
            self.sims[i].u_hist_wad = irm.w_div_down(min(self.prev_state[i][2], sup), sup) if sup > 0 else 0

    def _hist_util(self, i):
        sup = self._eff_supply(i)
        return min(self.prev_state[i][2] / sup, 1.0) if sup > 0 else 0.0


    def exit_liquidity(self, i):
        """What the vault can withdraw from market i now."""
        hist = self.prev_state[i]
        if self.taker:
            return max(self._eff_supply(i) - hist[2], 0)
        cf = self.sims[i].liquidity()
        if hist[0] > 0 and hist[2] >= C.PINNED_U * hist[0]:
            return max(min(cf, hist[0] - hist[2]), 0)
        return max(cf, 0)

    def observe(self, ts, markets=None):
        """Per market (all loaded markets, or `markets`)."""
        out = {}
        for m in (self.markets if markets is None else markets):
            i = self._ix[m]
            s = self.sims[i]
            tsa, tss, tba, avg = s.view(ts)
            pos = irm.to_assets_down(self.shares[i], tsa, tss) if self.shares[i] > 0 else 0
            if self.taker:
                tsa = self._eff_supply(i)
                tba = min(self.prev_state[i][2], tsa)
            util = tba / tsa if tsa > 0 else 0.0
            rate = s.rate_at(tsa, tba, ts)
            fee = (s.fee or 0) / 1e18
            out[m] = {"supply": float(tsa), "borrow": float(tba), "liquidity": float(self.exit_liquidity(i)),
                      "util": util, "borrow_apr": rate / 1e18 * YEAR,
                      "supply_apy": supply_apy(rate, min(util, 1.0), fee),
                      "position": float(pos), "fee": fee, "created": self.created[i]}
        return out

    def apy_after(self, m, ts, delta):
        """Supply APY of market m if the policy's position changed by delta (raw units) at ts, at the current rate at target."""
        i = self._ix[m]
        s = self.sims[i]
        tsa, tss, tba, _avg = s.view(ts)
        if self.taker:
            pos = irm.to_assets_down(self.shares[i], tsa, tss) if self.shares[i] > 0 else 0
            sup = max(self.prev_state[i][0], max(pos + int(delta), 0))
            if sup <= 0:
                return 0.0
            b = min(self.prev_state[i][2], sup)
            return supply_apy(s.rate_at(sup, b, ts), b / sup, (s.fee or 0) / 1e18)
        tsa2 = max(tsa + int(delta), 0)
        if tsa2 <= 0:
            return 0.0
        util = min(tba / tsa2, 1.0)
        return supply_apy(s.rate_at(tsa2, tba, ts), util, (s.fee or 0) / 1e18)

    def nav(self, ts):
        total = self.idle
        for i, s in enumerate(self.sims):
            if self.shares[i] > 0:
                tsa, tss, _tba, _ = s.view(ts)
                total += irm.to_assets_down(self.shares[i], tsa, tss)
        return total

    def fund(self, amount):
        self.idle += int(amount)

    def payout(self, amount):
        """Pay a depositor's withdrawal from idle cash; returns what was paid."""
        paid = min(max(int(amount), 0), self.idle)
        self.idle -= paid
        return paid

    def execute(self, ts, targets):
        """Move the markets in `targets` to their target positions {market."""
        want = {self._ix[m]: max(int(v), 0) for m, v in targets.items() if m in self._ix}
        cur = {}
        for i in want:
            if self.shares[i] > 0:
                tsa, tss, _tba, _ = self.sims[i].view(ts)
                cur[i] = irm.to_assets_down(self.shares[i], tsa, tss)
        for i in sorted(cur):
            tgt = want[i]
            if cur[i] > tgt:
                s = self.sims[i]
                s.accrue(ts)
                asked = cur[i] - tgt
                cap = self.exit_liquidity(i)
                if tgt == 0 and cap >= cur[i]:
                    assets, shares = s.policy_withdraw_shares(self.shares[i])
                else:
                    assets, shares = s.policy_withdraw_assets(min(asked, cap))
                shares = min(shares, self.shares[i])
                self.shares[i] -= shares
                self.ledger[i] -= assets
                self.idle += assets
                clipped = assets < asked
                self.clips += int(clipped)
                self.actions.append({"ts": ts, "market": self.markets[i], "side": "withdraw",
                                     "asked": str(asked), "done": str(assets), "clipped": clipped})
        for i, tgt in sorted(want.items()):
            have = cur.get(i, 0)
            if tgt > have and self.idle > 0:
                amt = min(tgt - have, self.idle)
                s = self.sims[i]
                s.accrue(ts)
                self.shares[i] += s.policy_supply(amt)
                self.ledger[i] += amt
                self.idle -= amt
                self.actions.append({"ts": ts, "market": self.markets[i], "side": "supply",
                                     "asked": str(tgt - have), "done": str(amt), "clipped": False})
        for i in want:
            self._set_eff(i)

    def snapshot(self, ts):
        """Positions, principal ledger and write-offs per market at ts."""
        mk = {}
        for i, s in enumerate(self.sims):
            if self.shares[i] > 0 or self.ledger[i] or self.writeoff[i]:
                tsa, tss, tba, _ = s.view(ts)
                pos = irm.to_assets_down(self.shares[i], tsa, tss) if self.shares[i] > 0 else 0
                mk[self.markets[i]] = {"position": pos, "ledger": self.ledger[i],
                                       "writeoff": self.writeoff[i], "liquidity": self.exit_liquidity(i),
                                       "aps": (tsa + 1) / (tss + 10 ** 6), "cfi": (tsa + 1) / (tss + 10 ** 6),
                                       "u": tba / tsa if tsa > 0 else 0.0}
        return {"ts": ts, "idle": self.idle, "markets": mk}


class ApproxReplay:
    """Day-step replay for Euler v2 and Silo v2 (module docstring)."""

    approximate = True
    taker = False

    def __init__(self, proto, cid, markets, start_ts, stop_ts, panel=None, pricing="impact"):
        if pricing not in PRICINGS:
            raise ValueError(pricing)
        self.taker = pricing == "displace"
        self.proto, self.cid = proto, int(cid)
        self.markets = list(markets)
        self._ix = {m: i for i, m in enumerate(self.markets)}
        self.start_ts, self.stop_ts = start_ts, stop_ts
        self.panel = panel or C.PanelDaily()
        self.mk = C.Markets(proto, cid)
        self.series = [self.panel.get(proto, cid, m) for m in self.markets]
        n = len(self.markets)
        self.pos = [0.0] * n
        self.cfi = [1.0] * n
        self.ledger = [0.0] * n
        self.writeoff = [0.0] * n
        self.idle = 0.0
        self.clips = 0
        self.capped_days = 0
        self.actions = []
        self.day = C.day_of_ts(start_ts) - 1
        self.n_rows = 0

    def index(self, m):
        return self._ix[m]

    def _hist(self, i, d):
        ser = self.series[i]
        r = ser.at(d) if ser else None
        if r is None:
            return 0.0, 0.0, None, 0.0
        return float(r[1]), float(r[2]), ser.aps(d), (r[4] or 0.0)

    def exit_liquidity(self, i):
        """Counterfactual idle liquidity (historical supply plus the vault's position, minus historical borrow), or the historical."""
        s, b, _a, _r = self._hist(i, self.day)
        if self.taker:
            return max(max(s, self.pos[i]) - b, 0.0)
        if s > 0 and b >= C.PINNED_U * s:
            return max(s - b, 0.0)
        return max(s + self.pos[i] - b, 0.0)

    def _step(self, d):
        """Advance positions and per-unit values from the end of day d - 1 to the end of day d."""
        for i in range(len(self.markets)):
            if self.series[i] is None:
                continue
            s0, b0, a0, apr0 = self._hist(i, d - 1)
            s1, b1, a1, apr1 = self._hist(i, d)
            if not a0 or not a1:
                continue
            g = a1 / a0
            p = self.pos[i]
            u = b0 / s0 if s0 > 0 else 0.0
            sp = max(s0, p) if self.taker else s0 + p
            u2 = b0 / sp if sp > 0 else 0.0
            if g >= 1.0:
                bound = 5.0 * apr0 * min(u, 1.0) / 365.0 + 1e-6
                if g - 1.0 > bound:
                    g = 1.0 + bound
                    if p > 0:
                        self.capped_days += 1
                scale = (u2 / u) ** 2 if u > 0 else 0.0
                f = 1.0 + (g - 1.0) * scale
            else:
                loss = (1.0 - g) * s0 / sp if sp > 0 else 0.0
                self.writeoff[i] += p * loss
                f = 1.0 - loss
            self.pos[i] = p * f
            self.cfi[i] *= f

    def run(self, grid, hook, audit=False):
        grid = sorted(set(int(g) for g in grid))
        for g in grid:
            d_end = C.day_of_ts(g) - 1
            while self.day < d_end:
                self.day += 1
                self._step(self.day)
            hook(self, g)
        return self

    def observe(self, ts, markets=None):
        d = self.day
        out = {}
        for m in (self.markets if markets is None else markets):
            i = self._ix[m]
            s, b, _a, apr = self._hist(i, d)
            s2 = max(s, self.pos[i]) if self.taker else s + self.pos[i]
            u_hist = b / s if s > 0 else 0.0
            u = b / s2 if s2 > 0 else 0.0
            r = apr * (u / u_hist) if u_hist > 0 else apr
            fee = self.mk.interest_fee(m, ts) or 0.0
            out[m] = {"supply": s2, "borrow": b, "liquidity": self.exit_liquidity(i), "util": u,
                      "borrow_apr": r, "supply_apy": (math.exp(min(r, 50.0)) - 1.0) * min(u, 1.0) * (1 - fee),
                      "position": self.pos[i], "fee": fee, "created": self.series[i] is not None
                      and self.series[i].at(d) is not None}
        return out

    def day_state(self, ts, markets=None):
        """(idle, {market: (position, cfi, utilization, exit liquidity)}) for held markets (and `markets`)."""
        want = {i for i, p in enumerate(self.pos) if p > 0}
        if markets:
            want |= {self._ix[m] for m in markets if m in self._ix}
        out = {}
        for i in sorted(want):
            s, b, _a, _r = self._hist(i, self.day)
            s2 = max(s, self.pos[i]) if self.taker else s + self.pos[i]
            out[self.markets[i]] = (self.pos[i], self.cfi[i], b / s2 if s2 > 0 else 0.0, self.exit_liquidity(i))
        return self.idle, out

    def apy_after(self, m, ts, delta):
        i = self._ix[m]
        s, b, _a, apr = self._hist(i, self.day)
        if self.taker:
            s2 = max(s, self.pos[i] + delta, 0.0)
        else:
            s2 = max(s + self.pos[i] + delta, 0.0)
        if s <= 0 or s2 <= 0:
            return 0.0
        u_hist = b / s
        u = min(b / s2, 1.0)
        r = apr * (u / u_hist) if u_hist > 0 else apr
        fee = self.mk.interest_fee(m, ts) or 0.0
        return (math.exp(min(r, 50.0)) - 1.0) * u * (1 - fee)

    def nav(self, ts):
        return self.idle + sum(self.pos)

    def fund(self, amount):
        self.idle += float(amount)

    def payout(self, amount):
        paid = min(max(float(amount), 0.0), self.idle)
        self.idle -= paid
        return paid

    def execute(self, ts, targets):
        want = {self._ix[m]: max(float(v), 0.0) for m, v in targets.items() if m in self._ix}
        for i in sorted(want):
            tgt = want[i]
            if self.pos[i] > tgt + 1e-9:
                avail = self.exit_liquidity(i)
                asked = self.pos[i] - tgt
                done = min(asked, avail)
                clipped = done < asked - 1e-9
                self.clips += int(clipped)
                self.pos[i] -= done
                self.ledger[i] -= done
                self.idle += done
                self.actions.append({"ts": ts, "market": self.markets[i], "side": "withdraw",
                                     "asked": asked, "done": done, "clipped": clipped})
        for i, tgt in sorted(want.items()):
            if tgt > self.pos[i] + 1e-9 and self.idle > 0:
                asked = tgt - self.pos[i]
                amt = min(asked, self.idle)
                self.pos[i] += amt
                self.ledger[i] += amt
                self.idle -= amt
                self.actions.append({"ts": ts, "market": self.markets[i], "side": "supply",
                                     "asked": asked, "done": amt, "clipped": False})

    def snapshot(self, ts):
        mk = {}
        for i, m in enumerate(self.markets):
            if self.pos[i] > 0 or self.ledger[i] or self.writeoff[i]:
                s, b, a, _r = self._hist(i, self.day)
                s2 = s + self.pos[i]
                mk[m] = {"position": self.pos[i], "ledger": self.ledger[i], "writeoff": self.writeoff[i],
                         "liquidity": self.exit_liquidity(i), "aps": a, "cfi": self.cfi[i],
                         "u": b / s2 if s2 > 0 else 0.0}
        return {"ts": ts, "idle": self.idle, "markets": mk}


def make_replay(group_or_proto, cid, markets, start_ts, stop_ts, panel=None, pricing="impact", **kw):
    proto = group_or_proto
    if proto in ("morpho", "moolah"):
        return ExactReplay(proto, cid, markets, start_ts, stop_ts, pricing=pricing, **kw)
    return ApproxReplay(proto, cid, markets, start_ts, stop_ts, panel=panel, pricing=pricing)


def panel_head_block(proto, cid):
    """Exclusive scan head of a Morpho or Moolah panel (_checkpoint.json)."""
    ck = json.loads((C.e2.panel_dir(proto, cid) / "_checkpoint.json").read_text())
    return int(ck["to_block"])


def pick_vaults(proto, cid, n, rng, min_rows=100, max_rows=20000):
    """Vaults with positions whose files have a moderate number of rows."""
    files = C.e2.position_files(proto, cid)
    reg = C.e2.vault_registry(proto, cid)
    pool = []
    for v, p in sorted(files.items()):
        if v not in reg:
            continue
        with open(p) as fh:
            k = sum(1 for _ in fh) - 1
        if min_rows <= k <= max_rows:
            pool.append(v)
    rng.shuffle(pool)
    return pool[:n]


def identity_one(proto, cid, vault, days=180):
    """Replay one vault's own actions over the `days` before its last own event, in the recorded convention, auditing every row."""
    path = C.e2.position_files(proto, cid)[vault]
    head = panel_head_block(proto, cid)
    rows, beyond = [], 0
    with open(path) as fh:
        for r in csv.DictReader(fh):
            if int(r["block"]) >= head:
                beyond += 1
                continue
            rows.append((int(r["block"]), int(r["log_index"]), int(r["timestamp"]), r["market_id"],
                         int(r["shares_delta"]), int(r["vault_cum_shares"])))
    stop = rows[-1][2]
    start = stop - days * DAY
    markets = sorted({r[3] for r in rows if (C.e2.panel_dir(proto, cid) / f"{r[3]}.csv").exists()})
    own = {(r[0], r[1]): r[4] for r in rows if start < r[2] <= stop and r[3] in markets}
    rp = ExactReplay(proto, cid, markets, start, stop, convention="recorded", own_rows=own)
    for i, m in enumerate(markets):
        rp.shares[i] = sum(r[4] for r in rows if r[3] == m and r[2] <= start)
    expected = {}
    for r in rows:
        if r[3] in markets and start < r[2] <= stop:
            expected[(r[0], r[1])] = (r[3], r[5])
    share_bad = []
    checked = [0]

    orig = rp._process

    def process(b, li, mi, ts, kind, post, br, rat, fee):
        orig(b, li, mi, ts, kind, post, br, rat, fee)
        if (b, li) in expected:
            m, cum = expected[(b, li)]
            checked[0] += 1
            if rp.shares[mi] != cum and len(share_bad) < 5:
                share_bad.append({"block": b, "log_index": li, "market": m,
                                  "replay": str(rp.shares[mi]), "file": str(cum)})

    rp._process = process
    t = time.time()
    rp.run([], lambda e, ts: None, audit=True)
    final = {m: 0 for m in markets}
    for r in rows:
        if r[3] in markets and r[2] <= stop:
            final[r[3]] = r[5]
    final_bad = sum(1 for i, m in enumerate(markets) if rp.shares[i] != final[m])
    accruals = sum(1 for rr in rp.rows for x in rr if x[4] == 1)
    return {"protocol": proto, "chain_id": cid, "vault": vault,
            "window_utc": [C.ts_iso(start), C.ts_iso(stop)], "markets": len(markets),
            "rows_replayed": rp.n_rows, "accruals_recomputed": accruals, "own_events": len(own),
            "own_events_checked": checked[0], "own_events_after_panel_head": beyond,
            "state_mismatches": len(rp.mismatches),
            "state_mismatch_examples": rp.mismatches[:3], "share_mismatches": len(share_bad),
            "share_mismatch_examples": share_bad, "final_share_mismatches": final_bad,
            "seconds": round(time.time() - t, 1),
            "pass": not rp.mismatches and not share_bad and final_bad == 0}


def identity_main(args):
    rng = random.Random(SEED)
    picks = []
    for cid, n in ((1, 7), (8453, 3)):
        picks += [("morpho", cid, v) for v in pick_vaults("morpho", cid, n, rng)]
    picks += [("moolah", 56, v) for v in pick_vaults("moolah", 56, 3, rng)]
    out = []
    for proto, cid, v in picks:
        r = identity_one(proto, cid, v, days=args.days)
        print(f"{'PASS' if r['pass'] else 'FAIL'} {proto}:{cid}:{v} rows {r['rows_replayed']} "
              f"accruals {r['accruals_recomputed']} own {r['own_events']} {r['seconds']}s", flush=True)
        out.append(r)
    res = {"seed": SEED, "window_days": args.days, "vaults": out,
           "passed": sum(r["pass"] for r in out), "total": len(out),
           "rows_replayed": sum(r["rows_replayed"] for r in out),
           "accruals_recomputed": sum(r["accruals_recomputed"] for r in out),
           "own_events_checked": sum(r["own_events_checked"] for r in out),
           "rpc_calls": 0}
    path = C.OUT / "replay_identity.json"
    C.write_json(path, res)
    C.write_sidecar(path, {"generator": "code/autocurator/replay.py identity",
                           "what": "identity validation of the multi-market replay on the E1 panel"})
    print(f"{res['passed']}/{res['total']} pass; {res['rows_replayed']} rows, "
          f"{res['accruals_recomputed']} accruals, {res['own_events_checked']} own events checked")
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("identity", help="identity validation on 10 Morpho and 3 Moolah vaults")
    p.add_argument("--days", type=int, default=180)
    args = ap.parse_args()
    if args.cmd == "identity":
        identity_main(args)


if __name__ == "__main__":
    main()
