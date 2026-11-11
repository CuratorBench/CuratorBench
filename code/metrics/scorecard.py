# /// script
# requires-python = ">=3.11"
# ///
"""Scorecard: per-vault and per-curator aggregation of the metric family (F1 quality dispersion)."""

import csv
import json
import math
import pathlib

DATA = pathlib.Path(__file__).parents[2] / "data"
DAY = 86400

MIN_VALUE_USD = 10_000
PEAK_FRACTION = 0.01
MAX_DAILY_RET = 0.03
STABLE = {"USDC", "USDT", "DAI", "USD0", "USDA", "PYUSD", "USDS", "USR",
          "FRAX", "GHO", "crvUSD", "USDe", "eUSD", "USDbC", "EURC", "EURe",
          "verUSD", "wM", "USDL", "RUSD", "MUSD"}
CURATOR_PATTERNS = [
    ("Gauntlet", "gauntlet"), ("Steakhouse Financial", "steakhouse"),
    ("Steakhouse Financial", "steak"),
    ("Steakhouse Financial", "smokehouse"), ("MEV Capital", "mev capital"),
    ("MEV Capital", "mc."), ("RE7 Labs", "re7"),
    ("Block Analitica", "block analitica"), ("B.Protocol", "b.protocol"),
    ("Hyperithm", "hyperithm"),
]


IMPOSTOR_NAMES = {"not gauntlet"}


def infer_curator(name, symbol=""):
    if (name or "").strip().lower() in IMPOSTOR_NAMES:
        return None
    s = f"{name or ''} {symbol or ''}".lower()
    for curator, pat in CURATOR_PATTERNS:
        if pat in s:
            return curator
    return None


def curator_map():
    sel = json.load(open(DATA / "trajectories" / "selection.json"))
    out = {}
    for v in sel:
        key = f"{v['chain_id']}-{v['address'].lower()}"
        names = [c["name"] for c in v.get("curators", [])] or \
            v.get("match_curators")
        if not names:
            inferred = infer_curator(v.get("name"), v.get("symbol"))
            names = [inferred] if inferred else ["(unattributed)"]
        out[key] = {"curators": names, "asset": v.get("asset"),
                    "name": v.get("name"), "covers": v.get("covers", {}),
                    "tvl_now": v.get("tvl_usd_now", 0)}
    return out


def annualize(rets):
    """Geometric annualization of daily returns."""
    if not rets:
        return None
    log_sum = sum(math.log1p(r) for r in rets)
    return math.expm1(log_sum * 365 / len(rets))


def tl_series(key):
    """ts -> (tl_wc, tl_mod, vol_share) from tail_loss.py output."""
    path = DATA / "metrics" / "vault-daily-tl" / f"{key}.csv"
    if not path.exists():
        return {}
    out = {}
    for r in csv.DictReader(open(path)):
        if r["tl_wc"] != "":
            out[int(float(r["ts"]))] = (float(r["tl_wc"]), float(r["tl_mod"]),
                                        float(r["vol_share"]))
    return out


def vault_stats(key, dec):
    path = DATA / "metrics" / "vault-daily" / f"{key}.csv"
    if not path.exists():
        return None
    unit = 10 ** dec.get(key, {}).get("decimals", 6)
    days = []
    for r in csv.DictReader(open(path)):
        days.append((int(float(r["ts"])), float(r["value"]) / unit,
                     float(r["ret"]) if r["ret"] else None,
                     float(r["liq"]) if r["liq"] else None))
    peak = max(d[1] for d in days)
    floor = max(MIN_VALUE_USD, PEAK_FRACTION * peak)
    active = [d for d in days if d[1] >= floor]
    if len(active) < 30:
        return None
    rets_all = [d[2] for d in active if d[2] is not None]
    rets = [r for r in rets_all if r <= MAX_DAILY_RET]
    liqs = [d[3] for d in active if d[3] is not None]
    liqs_sorted = sorted(liqs)
    out = {
        "active_days": len(active),
        "yield_ann": annualize(rets),
        "ret_days_excluded": len(rets_all) - len(rets),
        "liq_min": min(liqs) if liqs else None,
        "liq_p10": liqs_sorted[len(liqs) // 10] if liqs else None,
        "liq_med": liqs_sorted[len(liqs) // 2] if liqs else None,
        "value_peak": peak,
        "value_mean": sum(d[1] for d in active) / len(active),
    }
    tl = tl_series(key)
    tls = [tl[d[0]] for d in active if d[0] in tl]
    if tls:
        wc = sorted(t[0] for t in tls)
        md = sorted(t[1] for t in tls)
        out["tl_wc_med"] = wc[len(wc) // 2]
        out["tl_wc_p90"] = wc[int(len(wc) * 0.9)]
        out["tl_mod_med"] = md[len(md) // 2]
        out["vol_share_med"] = sorted(t[2] for t in tls)[len(tls) // 2]
    return out


def main():
    cmap = curator_map()
    dec = json.load(open(DATA / "universe" / "asset-decimals.json"))
    vaults = {}
    for p in sorted((DATA / "metrics" / "vault-daily").glob("*.csv")):
        key = p.stem
        info = cmap.get(key, {})
        st = vault_stats(key, dec)
        if st is None:
            continue
        st["name"] = info.get("name")
        st["curators"] = info.get("curators", ["(unattributed)"])
        st["asset"] = info.get("asset")
        st["stable"] = info.get("asset") in STABLE
        vaults[key] = st

    curators = {}
    for key, st in vaults.items():
        if not st["stable"] or st["yield_ann"] is None:
            continue
        for c in st["curators"]:
            curators.setdefault(c, []).append((key, st))

    table = {}
    for c, items in sorted(curators.items()):
        ys = [st["yield_ann"] for _, st in items]
        lm = [st["liq_min"] for _, st in items if st["liq_min"] is not None]
        w = [st["value_mean"] for _, st in items]
        wy = (sum(y * x for y, x in zip(ys, w)) / sum(w)) if sum(w) else None
        tw = [(st["tl_wc_med"], st["value_mean"]) for _, st in items
              if st.get("tl_wc_med") is not None]
        tl_vw = (sum(t * x for t, x in tw) / sum(x for _, x in tw)) \
            if tw else None
        table[c] = {
            "n_vaults": len(items),
            "aum_mean_musd": round(sum(w) / 1e6, 1),
            "yield_ann_vw": round(wy, 5),
            "yield_ann_range": [round(min(ys), 5), round(max(ys), 5)],
            "liq_min_median": round(sorted(lm)[len(lm) // 2], 4) if lm else None,
            "tl_wc_med_vw": round(tl_vw, 5) if tl_vw is not None else None,
        }

    out = {"vaults": vaults, "curators": table,
           "scope": "stablecoin loan-asset vaults, >=30 active days, "
                    ">= $10k position"}
    (DATA / "metrics").mkdir(exist_ok=True)
    with open(DATA / "metrics" / "scorecard.json", "w") as f:
        json.dump(out, f, indent=1, default=str)

    print(f"{len(vaults)} vaults scored ({sum(1 for v in vaults.values() if v['stable'])} stable)")
    print(f"\n{'curator':28s} {'n':>3s} {'AUM(M$)':>8s} {'yield(vw)':>9s} "
          f"{'yield range':>18s} {'L_min med':>9s} {'TL_wc(vw)':>9s}")
    for c, t in sorted(table.items(), key=lambda kv: -kv[1]['aum_mean_musd']):
        rng = f"{t['yield_ann_range'][0]:.2%}–{t['yield_ann_range'][1]:.2%}"
        tl = f"{t['tl_wc_med_vw']:.2%}" if t['tl_wc_med_vw'] is not None else ""
        print(f"{c:28s} {t['n_vaults']:3d} {t['aum_mean_musd']:8.1f} "
              f"{t['yield_ann_vw']:9.2%} {rng:>18s} "
              f"{t['liq_min_median'] if t['liq_min_median'] is not None else '':>9} "
              f"{tl:>9s}")


if __name__ == "__main__":
    main()
