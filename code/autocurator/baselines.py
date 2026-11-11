# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "pycryptodome"]
# ///
"""E8 baselines: policies that need no model, run through the same replay."""
import csv
import json
import time

import context as C
import decide as D

BLIND_SUBTYPE_OK = {"underlying_at_own_feed"}
NOT_BLIND = {"tracks_market", "min_of_two"}
UTIL_CAP = 0.90
LLAMA_CHAIN = {1: "Ethereum", 8453: "Base", 42161: "Arbitrum", 56: "BSC", 43114: "Avalanche",
               146: "Sonic", 9745: "Plasma", 59144: "Linea", 999: "Hyperliquid L1",
               747474: "Katana", 4663: "Robinhood"}


def oracle_blind(orc):
    t = (orc or {}).get("type")
    if t in NOT_BLIND:
        return False
    if t == "exchange_rate":
        return (orc or {}).get("subtype") not in BLIND_SUBTYPE_OK
    return True


def open_markets(dc):
    return [m for m in dc["markets"] if m in dc["open"]]


def rule_filter(dc):
    """Open markets that pass rule_based's two rules (module docstring)."""
    gc, ts, obs = dc["gc"], dc["ts"], dc["obs"]
    keep, dropped = [], {}
    for m in open_markets(dc):
        fams = gc.families(m, ts)
        if not fams or any(oracle_blind(gc.mk.oracle(m, cv)) for _f, w, _t, cv, _lt in fams if w >= 0.02):
            dropped[m] = "fixed-price or blind oracle"
        elif obs[m]["util"] > UTIL_CAP:
            dropped[m] = "utilization above 90%"
        else:
            keep.append(m)
    dc["record"]["dropped"] = len(dropped)
    return keep


def equal_weights(ms):
    return {m: 1.0 / len(ms) for m in ms} if ms else {}


class MaxAPY(D.Policy):
    name = "max_apy"

    def decide(self, dc):
        obs, nav = dc["obs"], dc["nav"]
        best, rate = None, 0.0
        for m in open_markets(dc):
            r = dc["apy_after"](m, nav - obs[m]["position"])
            if r > rate + 1e-12:
                best, rate = m, r
        dc["record"]["rate_after"] = rate
        return {best: 1.0} if best else {}


class EqualWeight(D.Policy):
    name = "equal"

    def decide(self, dc):
        return equal_weights(open_markets(dc))


class RuleBased(D.Policy):
    name = "rule_based"

    def decide(self, dc):
        return equal_weights(rule_filter(dc))


POLICIES = {"max_apy": MaxAPY, "equal": EqualWeight, "rule_based": RuleBased}


class Aave:
    """DefiLlama yields API, cached in the work directory."""

    def __init__(self):
        self.cache = C.WORK / "llama_yields"
        self.cache.mkdir(parents=True, exist_ok=True)
        self.requests = 0

    def _get(self, url, name):
        p = self.cache / name
        if p.exists():
            return json.loads(p.read_text())
        import requests
        for attempt in range(5):
            try:
                r = requests.get(url, timeout=120)
                self.requests += 1
                if r.status_code == 200:
                    p.write_text(r.text)
                    return r.json()
            except requests.RequestException:
                pass
            time.sleep(2 ** attempt)
        raise RuntimeError(f"DefiLlama request failed: {url}")

    def pool(self, cid, token):
        chain = LLAMA_CHAIN.get(int(cid))
        pools = self._get("https://yields.llama.fi/pools", "pools.json")["data"]
        cands = [p for p in pools if p.get("project") == "aave-v3" and p.get("chain") == chain
                 and token.lower() in [u.lower() for u in (p.get("underlyingTokens") or [])]]
        if not cands:
            return None
        plain = [p for p in cands if not p.get("poolMeta")] or cands
        return max(plain, key=lambda p: p.get("tvlUsd") or 0)

    def series(self, pool_id):
        d = self._get(f"https://yields.llama.fi/chart/{pool_id}", f"chart_{pool_id}.json")["data"]
        out = {}
        for r in d:
            day = C.e2.day_of(r["timestamp"][:10])
            if r.get("apyBase") is not None:
                out[day] = float(r["apyBase"]) / 100.0
        return out


def passive_aave(group, aave):
    """{"pool", "symbol", "series"."""
    p = aave.pool(group.chain_id, group.loan_token)
    if p is None:
        return None
    ser = aave.series(p["pool"])
    path = C.OUT / "aave" / f"{group.chain_id}_{group.loan_token}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "apy_base"])
        for d in sorted(ser):
            w.writerow([C.day_iso(d), ser[d]])
    C.write_sidecar(path, {"source": f"https://yields.llama.fi/chart/{p['pool']}", "pool": p["pool"],
                           "symbol": p.get("symbol"), "chain": p.get("chain"), "project": "aave-v3",
                           "generator": "code/autocurator/baselines.py"})
    return {"pool": p["pool"], "symbol": p.get("symbol"), "series": ser}


def aave_span(series, d0, d1, min_share=0.5):
    """Passive Aave over days d0 to d1."""
    have = [series[d] for d in range(d0, d1 + 1) if d in series]
    n = d1 - d0 + 1
    if not have or len(have) < min_share * n:
        return None, len(have), n
    import e4span
    rets = [(1.0 + a) ** (1 / 365) - 1.0 for a in have]
    return e4span.E4.annualize(rets), len(have), n


if __name__ == "__main__":
    print(__doc__)
