# /// script
# requires-python = ">=3.11"
# ///
"""Stress tail loss (Def."""

import csv
import json
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
from vault_series import BlockClock, market_daily

DATA = HERE.parents[1] / "data"
OUT = DATA / "metrics" / "vault-daily-tl"
DAY = 86400

RHO = 0.9
ETA = 0.05
DEPEGS = (0.15, 1.0)

STABLE_SYM = re.compile(
    r"usd|dai|frax|gho|eur|dola|mim|susd|usr\b|wusr|deusd|musd|rusd|"
    r"csusdl|verusd|wm\b|stusd", re.I)
STABLE_EXTRA = ("USCC", "USTBL", "USYC", "EUTBL", "XSGD", "BILL", "IB01",
                "BASIS", "EDGE", "MEV", "HYPER", "GLO", "RLP", "fxSAVE",
                "F-ONE", "Re7YIELD", "JTRSY", "ACRDX")

CLUSTERS = {
    "usd0pp": {
        "0x35d8949372d46b7a3d5a56006ae77b215fc69bc0",
        "0x270d664d2fc7d962012a787aec8661ca83df24eb",
        "0x5bae9a5d67d1ca5b09b14c91935f635cfbf3b685",
        "0xf696fe29ef85e892b5926313897d178288faa07e",
        "0x1d08e7adc263cfc70b1babe6dc5bb339c16eec52",
    },
    "deusd": {
        "0x15700b564ca08d9439c58ca5053166e8317aa138",
        "0x5c5b196abe0d54485975d1ec29617d42d9198326",
        "0xb4b8925c4cbce692f37c9d946883f2e330a042a9",
    },
}


def is_stable_class(cid, addr, tokinfo):
    sym = tokinfo.get(f"{cid}-{addr}", {}).get("symbol") or ""
    return bool(STABLE_SYM.search(sym)) or \
        any(s in sym for s in STABLE_EXTRA)


def loss_rate(u_stressed, lltv, depeg, mode):
    if mode == "hard":
        r = 1 - (1 - depeg) / lltv
    else:
        r = 1 - (1 - depeg) * (1 - ETA) / lltv
    return u_stressed * max(0.0, r)


def build(key, params, modes, tokinfo, clocks):
    cid = int(key.split("-")[0])
    meta = json.load(open(DATA / "trajectories" / f"{key}.meta.json"))
    if meta["n_events"] == 0:
        return None
    events = []
    with open(DATA / "trajectories" / f"{key}.events.csv") as f:
        for r in csv.DictReader(f):
            events.append((int(r["block"]), r["market_id"],
                           int(r["shares_delta"])))
    mids = sorted({e[1] for e in events})

    clock = clocks[cid]
    t0 = (int(clock.to_ts(meta["creation_block"])) // DAY + 1) * DAY
    t1 = int(clock.to_ts(meta["head_block"])) // DAY * DAY
    if t1 <= t0:
        return None
    days = list(range(t0, t1 + 1, DAY))
    grid = tuple(clock.to_block(t) for t in days)

    shares = {}
    for mid in mids:
        evs = sorted((e[0], e[2]) for e in events if e[1] == mid)
        cum, gi, acc = [], 0, 0
        for b, d in evs:
            while gi < len(grid) and grid[gi] < b:
                cum.append(acc)
                gi += 1
            acc += d
        while gi < len(grid):
            cum.append(acc)
            gi += 1
        shares[mid] = cum
    series = {mid: market_daily(cid, mid, grid) for mid in mids}

    attr = {}
    for mid in mids:
        p = params.get(f"{cid}-{mid}")
        if p is None:
            continue
        coll = p["collateral_token"].lower()
        fams = []
        if is_stable_class(cid, coll, tokinfo):
            fams.append(coll)
        for cname, members in CLUSTERS.items():
            if coll in members:
                fams.append(f"cluster:{cname}")
        attr[mid] = {"lltv": int(p["lltv"]) / 1e18,
                     "mode": modes.get(f"{cid}-{mid}", "track"),
                     "coll": coll, "fams": fams,
                     "stable": bool(fams)}

    rows = []
    for di, ts in enumerate(days):
        vals = {}
        for mid in mids:
            sh = shares[mid][di]
            price, _idle, _u = series[mid][di]
            if sh > 0 and price:
                vals[mid] = sh * price
        total = sum(vals.values())
        if total <= 0:
            rows.append((ts, 0.0, "", "", "", ""))
            continue
        fam_loss = {}
        vol_share = 0.0
        for mid, v in vals.items():
            a = attr.get(mid)
            if a is None:
                continue
            alpha = v / total
            _p, _i, u = series[mid][di]
            u_s = u / (1 - RHO * (1 - u)) if u > 0 else 0.0
            u_s = min(u_s, 1.0)
            if not a["stable"]:
                vol_share += alpha
                continue
            for fam in a["fams"]:
                for dp in DEPEGS:
                    li = loss_rate(u_s, a["lltv"], dp, a["mode"])
                    fam_loss[(fam, dp)] = fam_loss.get((fam, dp), 0.0) \
                        + alpha * li
        if fam_loss:
            (wf, _), tl_wc = max(fam_loss.items(), key=lambda kv: kv[1])
            tl_mod = max((v for (f, dp), v in fam_loss.items()
                          if dp == 0.15), default=0.0)
        else:
            wf, tl_wc, tl_mod = "", 0.0, 0.0
        rows.append((ts, total, round(tl_wc, 6), round(tl_mod, 6),
                     wf, round(vol_share, 4)))

    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / f"{key}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "value", "tl_wc", "tl_mod", "family_wc",
                    "vol_share"])
        w.writerows(rows)
    funded = [r for r in rows if r[1] and r[1] > 0]
    return {"vault": key, "name": meta["name"], "days": len(rows),
            "funded_days": len(funded)}


def main():
    params = json.load(open(DATA / "markets" / "params.json"))
    modes = json.load(open(DATA / "markets" / "oracle-modes.json"))["by_market"]
    tokinfo = {k.lower(): v for k, v in json.load(
        open(DATA / "markets" / "token-info.json")).items()}
    clocks = {1: BlockClock(1), 8453: BlockClock(8453)}

    summary = []
    metas = sorted(DATA.glob("trajectories/*.meta.json"))
    for i, mp in enumerate(metas):
        key = mp.stem.removesuffix(".meta")
        try:
            r = build(key, params, modes, tokinfo, clocks)
        except Exception as e:
            r = {"vault": key, "error": str(e)[:150]}
        if r:
            summary.append(r)
            print(f"[{i + 1}/{len(metas)}] {key} "
                  f"{r.get('error', str(r.get('funded_days')) + 'd funded')}")
    with open(DATA / "metrics" / "tl-summary.json", "w") as f:
        json.dump(summary, f, indent=1)
    errs = sum(1 for r in summary if "error" in r)
    print(f"\nDONE: {len(summary) - errs} vault TL series, {errs} errors")


if __name__ == "__main__":
    main()
