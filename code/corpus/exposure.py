"""Peak curated exposure in USD per collateral token, for ranking the corpus."""
import csv
import datetime as dt
import json
import pathlib
from collections import defaultdict

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
DAY = 86400
D0 = (dt.date(2024, 1, 1) - dt.date(1970, 1, 1)).days
D1 = (dt.date(2026, 10, 4) - dt.date(1970, 1, 1)).days
N = D1 - D0 + 1
UNIT_TOKEN = {"ETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
              "BTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
              "EUR": "0x1abaea1f7c830bd89acc67ec4af516284b1bc33c"}
ZERO = "0x" + "0" * 40


def jsonl(path):
    with open(path) as fh:
        return [json.loads(x) for x in fh if x.strip()]


def ffill_days(days, values):
    """Dense day-end array over [D0, D1] from sparse (day, value) points."""
    out = np.zeros(N)
    if len(days) == 0:
        return out
    idx = np.clip(np.asarray(days, dtype=np.int64) - D0, 0, N - 1)
    order = np.argsort(idx, kind="stable")
    idx, vals = idx[order], np.asarray(values, dtype=float)[order]
    last = np.full(N, -1)
    last[idx] = np.arange(len(idx))
    last = np.maximum.accumulate(last)
    ok = last >= 0
    out[ok] = vals[last[ok]]
    return out


def unit_prices():
    """{unit: daily USD price array}; USD is 1."""
    df = pd.read_csv(DATA / "prices/1.csv", usecols=["date", "token", "price_usd"],
                     dtype={"date": str, "token": str, "price_usd": float})
    out = {"USD": np.ones(N)}
    for unit, tok in UNIT_TOKEN.items():
        sub = df[(df.token.str.lower() == tok) & df.price_usd.notna()]
        days = [(dt.date.fromisoformat(x) - dt.date(1970, 1, 1)).days for x in sub.date]
        out[unit] = ffill_days(days, sub.price_usd.values)
    return out


def token_book():
    """{(chain, token): {"decimals", "symbol", "name"}} from E1 and the E2 supplement."""
    book = {}
    for path in (DATA / "prices/tokens.jsonl", DATA / "events/tokens_supplement.jsonl"):
        for r in jsonl(path):
            book.setdefault((r["chain_id"], r["token"].lower()), r)
    return book


def e3_daily():
    """{(chain, vault): (usd_per_raw, gross_usd)} daily arrays from E3."""
    out = {}
    for name in ("vault_daily.csv", "lending_daily.csv"):
        df = pd.read_csv(DATA / "aum" / name,
                         usecols=["chain_id", "vault", "date", "counted_assets", "aum_usd",
                                  "held_by_vaults_usd"],
                         dtype={"chain_id": np.int64, "vault": str, "date": str,
                                "counted_assets": float, "aum_usd": float,
                                "held_by_vaults_usd": float})
        df["day"] = (pd.to_datetime(df.date) - pd.Timestamp("1970-01-01")).dt.days
        for (cid, v), sub in df.groupby(["chain_id", "vault"], sort=False):
            ok = sub[(sub.counted_assets > 0) & sub.aum_usd.notna()]
            conv = ffill_days(ok.day.values, (ok.aum_usd / ok.counted_assets).values)
            gross = ffill_days(sub.day.values,
                               (sub.aum_usd.fillna(0) + sub.held_by_vaults_usd.fillna(0)).values)
            gross[:max(0, int(sub.day.min()) - D0)] = np.nan
            out[(int(cid), v.lower())] = (conv, gross)
    return out


def sample_vaults():
    rows = list(csv.DictReader(open(DATA / "metrics/e4/vault_scorecard.csv")))
    return [r for r in rows if r["status"] == "in"]


def morpho_aps(proto, cid, mid, cache):
    key = (proto, cid, mid)
    if key not in cache:
        p = DATA / f"panel/{proto}/{cid}/{mid}.csv"
        if not p.exists():
            cache[key] = None
        else:
            df = pd.read_csv(p, usecols=["timestamp", "total_supply_assets", "total_supply_shares"],
                             dtype={"timestamp": np.int64, "total_supply_assets": float,
                                    "total_supply_shares": float})
            df["day"] = df.timestamp // DAY
            last = df.drop_duplicates("day", keep="last")
            aps = (last.total_supply_assets.values + 1.0) / (last.total_supply_shares.values + 1e6)
            cache[key] = ffill_days(last.day.values, aps)
    return cache[key]


def euler_supply(cid, vault, cache):
    key = (cid, vault)
    if key not in cache:
        p = DATA / f"panel/euler_v2/{cid}/{vault}.csv"
        if not p.exists():
            cache[key] = None
        else:
            df = pd.read_csv(p, usecols=["timestamp", "cash", "totalBorrows"],
                             dtype={"timestamp": np.int64, "cash": float, "totalBorrows": float})
            df["day"] = df.timestamp // DAY
            last = df.drop_duplicates("day", keep="last")
            cache[key] = ffill_days(last.day.values, (last.cash + last.totalBorrows).values)
    return cache[key]


def euler_split(cid, coll_asset):
    """{vault: {collateral token."""
    p = DATA / f"vaults/euler_v2/{cid}/borrow_distribution.csv"
    if not p.exists():
        return {}
    df = pd.read_csv(p, usecols=["vault", "timestamp", "totalBorrows", "collateral_vault", "attributed"],
                     dtype={"vault": str, "timestamp": np.int64, "totalBorrows": float,
                            "collateral_vault": str, "attributed": float})
    df = df[df.collateral_vault.notna() & (df.totalBorrows > 0)]
    df["token"] = df.collateral_vault.str.lower().map(coll_asset)
    df = df[df.token.notna()]
    df["day"] = df.timestamp // DAY
    out = defaultdict(dict)
    for (v, tok), sub in df.groupby(["vault", "token"], sort=False):
        g = sub.groupby("day").attributed.sum()
        tot = sub.groupby("day").totalBorrows.first()
        share = (g / tot).clip(0, 1)
        arr = np.zeros(N)
        idx = np.clip(share.index.values.astype(np.int64) - D0, 0, N - 1)
        arr[idx] = share.values
        out[v.lower()][tok] = arr
    return out


def anchors_from(paths, col):
    """{market: (days, assets per share)} from exact Deposit/Withdraw conversions."""
    pts = defaultdict(list)
    for p in paths:
        df = pd.read_csv(p, usecols=["timestamp", col, "kind", "assets", "event_shares"],
                         dtype={"timestamp": np.int64, col: str, "kind": str, "assets": float,
                                "event_shares": float})
        df = df[df.kind.isin(["Deposit", "Withdraw"]) & (df.assets > 0) & (df.event_shares > 0)]
        for m, sub in df.groupby(col):
            pts[m.lower()].extend(zip((sub.timestamp // DAY).tolist(),
                                      (sub.assets / sub.event_shares).tolist()))
    out = {}
    for m, lst in pts.items():
        lst.sort()
        out[m] = ffill_days([x[0] for x in lst], [x[1] for x in lst])
    return out


def shares_by_market(path, col, shc):
    df = pd.read_csv(path, usecols=["timestamp", col, shc],
                     dtype={"timestamp": np.int64, col: str, shc: float})
    out = {}
    if df.empty:
        return out
    df["day"] = df.timestamp // DAY
    for m, sub in df.groupby(col, sort=False):
        last = sub.drop_duplicates("day", keep="last")
        out[m.lower()] = ffill_days(last.day.values, last[shc].values)
    return out


def compute(log=print):
    """Daily USD exposure per (chain, collateral token) and per-vault peaks."""
    units = unit_prices()
    book = token_book()
    sample = sample_vaults()
    reg_asset = {}
    for proto in ("morpho", "moolah", "silo_v2", "euler_v2"):
        for p in (DATA / f"registry/vaults/{proto}").glob("*.jsonl"):
            for r in jsonl(p):
                reg_asset[(r["chain_id"], r["vault"].lower())] = r["asset"].lower()
    euler_asset = {}
    for p in (DATA / "registry/markets/euler_v2").glob("*.jsonl"):
        for r in jsonl(p):
            if r.get("type") == "vault":
                euler_asset[(r["chain_id"], r["vault"].lower())] = r["asset"].lower()
    mkt_coll = {}
    for proto in ("morpho", "moolah"):
        for p in (DATA / f"registry/markets/{proto}").glob("*.jsonl"):
            for r in jsonl(p):
                mkt_coll[(proto, r["chain_id"], r["market_id"].lower())] = r["collateral_token"].lower()
    silo_coll = {}
    by_cfg = defaultdict(list)
    for p in (DATA / "registry/markets/silo_v2").glob("*.jsonl"):
        for r in jsonl(p):
            by_cfg[(r["chain_id"], r["silo_config"].lower())].append((r["silo"].lower(), r["token"].lower()))
    for (cid, _cfg), pair in by_cfg.items():
        if len(pair) == 2:
            (s0, t0), (s1, t1) = pair
            silo_coll[(cid, s0)] = t1
            silo_coll[(cid, s1)] = t0

    expo = defaultdict(lambda: {"daily": np.zeros(N), "vaults": {}, "protocols": set()})
    aps_cache, sup_cache = {}, {}
    lending_in = {(int(r["chain_id"]), r["vault"].lower()) for r in sample
                  if r["kind"] == "euler_lending"}
    splits = {}

    def usd(asset_cid, asset, unit):
        dec = (book.get((asset_cid, asset)) or {}).get("decimals")
        if dec is None or unit not in units:
            return None
        return units[unit] / (10.0 ** int(dec))

    e3 = e3_daily()
    pending = []

    def add(cid, tok, vault, proto, arr):
        if tok not in (None, ZERO) and np.any(arr > 0):
            pending.append((cid, tok, vault, proto, arr))

    def flush(cid, vault, conv):
        """Convert the vault's raw positions to USD, cap at E3 gross value."""
        if not pending:
            return
        e3c, gross = e3.get((cid, vault), (None, None))
        rate = conv if e3c is None else np.where(e3c > 0, e3c, conv)
        usd_arrs = [(c, t, v, p, a * rate) for c, t, v, p, a in pending]
        total = sum(a for *_x, a in usd_arrs)
        scale = np.ones(N)
        if gross is not None:
            over = np.isfinite(gross) & (total > gross * 1.01) & (total > 0)
            scale[over] = gross[over] / total[over]
        for c, t, v, p, a in usd_arrs:
            arr = a * scale
            e = expo[(c, t)]
            e["daily"] += arr
            e["vaults"][v] = max(e["vaults"].get(v, 0.0), float(arr.max()))
            e["protocols"].add(p)
        pending.clear()

    skipped = defaultdict(int)
    silo_anchor_cache = {}
    for r in sample:
        cid, v, kind, unit = int(r["chain_id"]), r["vault"].lower(), r["kind"], r["loan_unit"]
        proto = r["protocol"]
        asset = (euler_asset if kind == "euler_lending" else reg_asset).get((cid, v))
        conv = usd(cid, asset, unit) if asset else None
        if conv is None:
            skipped["no_asset_or_decimals"] += 1
            continue
        if kind in ("morpho_v1", "morpho_v2", "moolah_vault"):
            pdir = "moolah" if proto == "moolah" else "morpho"
            path = DATA / f"vaults/{pdir}/{cid}/positions/{v}.csv"
            if not path.exists():
                skipped["no_positions"] += 1
                continue
            for m, sh in shares_by_market(path, "market_id", "vault_cum_shares").items():
                tok = mkt_coll.get((pdir, cid, m))
                aps = morpho_aps(pdir, cid, m, aps_cache)
                if tok is None or aps is None:
                    continue
                add(cid, tok, v, pdir, sh * aps)
        elif kind == "silo_vault":
            pdir = DATA / f"vaults/silo_v2/{cid}/positions"
            path = pdir / f"{v}.csv"
            if not path.exists():
                skipped["no_positions"] += 1
                continue
            if cid not in silo_anchor_cache:
                silo_anchor_cache[cid] = anchors_from(sorted(pdir.glob("0x*.csv")), "market")
            anc = silo_anchor_cache[cid]
            for m, sh in shares_by_market(path, "market", "cum_shares").items():
                tok = silo_coll.get((cid, m))
                if tok is None or m not in anc:
                    continue
                add(cid, tok, v, "silo_v2", sh * anc[m])
        elif kind in ("euler_lending", "euler_earn"):
            if cid not in splits:
                coll_asset = {vv: a for (c, vv), a in euler_asset.items() if c == cid}
                splits[cid] = euler_split(cid, coll_asset)
            sp = splits[cid]
            if kind == "euler_lending":
                sup = euler_supply(cid, v, sup_cache)
                if sup is None:
                    skipped["no_panel"] += 1
                    continue
                for tok, share in sp.get(v, {}).items():
                    add(cid, tok, v, "euler_v2", sup * share)
            else:
                path = DATA / f"vaults/euler_v2/{cid}/positions/{v}.csv"
                if not path.exists():
                    skipped["no_positions"] += 1
                    continue
                anc = anchors_from([path], "strategy")
                for m, sh in shares_by_market(path, "strategy", "cum_shares").items():
                    if (cid, m) in lending_in or m not in anc:
                        continue
                    for tok, share in sp.get(m, {}).items():
                        add(cid, tok, v, "euler_v2", sh * anc[m] * share)
        flush(cid, v, conv)
    log(f"exposure: {len(sample)} sample vaults, {len(expo)} collateral tokens, skipped {dict(skipped)}")
    return expo, book, dict(skipped)


def day_iso(i):
    return (dt.date(1970, 1, 1) + dt.timedelta(days=D0 + int(i))).isoformat()
