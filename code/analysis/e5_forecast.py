# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy>=1.26", "pandas>=2.1", "scipy>=1.11", "matplotlib>=3.8", "requests>=2.31", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0"]
# ///
"""Does the tail loss measured before a depeg-loss event rank the loss the event then caused?."""
import importlib.util
import pathlib
import sys

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "code"))
from common import io_utils, outroot

GENERATOR = "code/analysis/e5_forecast.py"
DERIVED = outroot.OUT_DATA
OUT = "metrics/e5/forecast_test.json"
WINDOW_D = 30
SEED = 20261018


def load_e4():
    spec = importlib.util.spec_from_file_location("e4_metrics", ROOT / "code/analysis/e4_metrics.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def day_of(s):
    return (pd.to_datetime(s) - pd.Timestamp("1970-01-01")).dt.days.astype(np.int64)


def vault_level(e4, by, rng):
    """The exposed vaults of fig_correlations.event_correlations, one row each."""
    v = pd.read_csv(DERIVED / "metrics/e4/vault_scorecard.csv", low_memory=False)
    v = v[(v.status == "in") & v.yield_after_fee.notna()].copy()
    v["vault"] = v.vault.str.lower()
    e = pd.read_csv(DERIVED / "metrics/e5/event_vault.csv", low_memory=False)
    e["vault"] = e.vault.str.lower()
    d = e[(e.event_type == "depeg_loss") & (e.value_t0_usd > 0)
          & (e.group.eq("present_at_t0") | (e.loss_usd.fillna(0) > 0))].copy()
    d = d[[k in set(zip(v.chain_id, v.vault)) for k in zip(d.chain_id, d.vault)]]
    t0d = day_of(d.t0_utc.str[:10])
    d["tl_pre_1d"] = [float(by[(c, x)].get(t - 1, np.nan)) if (c, x) in by else np.nan
                      for c, x, t in zip(d.chain_id, d.vault, t0d)]
    recs = []
    for (c, x), g in d.groupby(["chain_id", "vault"]):
        ok = np.isfinite(g.tl_pre_1d.values)
        pre = float(np.average(g.tl_pre_1d.values[ok], weights=g.value_t0_usd.values[ok])) if ok.any() else np.nan
        recs.append((c, x, pre, float(g.loss_usd.fillna(0).sum() / g.value_t0_usd.sum())))
    g = pd.DataFrame(recs, columns=["chain_id", "vault", "tl_pre_1d", "event_loss"])
    g = g.merge(v[["chain_id", "vault", "tl_headline", "curator_entity"]], on=["chain_id", "vault"])
    g["cluster"] = [c if isinstance(c, str) and c else f"vault:{ch}:{x}"
                    for c, ch, x in zip(g.curator_entity, g.chain_id, g.vault)]
    out = {"vaults": int(len(g))}
    for col, src in (("tl_pre_1d", "tl_pre_1d"), ("tl_whole_period", "tl_headline")):
        h = g[np.isfinite(g[src])]
        out[col] = e4.perm_tests(h[src].values, h.event_loss.values, h.cluster.values, rng)
    return out


def main():
    e4 = load_e4()
    rng = np.random.default_rng(SEED)
    ev = pd.read_csv(DERIVED / "metrics/e5/event_vault.csv", low_memory=False)
    ev["vault"] = ev.vault.str.lower()
    rows = ev[(ev.event_type == "depeg_loss") & (ev.position_t0_usd.fillna(0) > 0)
              & ev.loss_per_dollar.notna()].copy()
    rows["t0d"] = day_of(rows.t0_utc.str[:10])
    sc = pd.read_csv(DERIVED / "metrics/e4/vault_scorecard.csv", low_memory=False)
    sc = sc[sc.status == "in"].copy()
    sc["vault"] = sc.vault.str.lower()
    keys = set(zip(sc.chain_id, sc.vault))
    rows = rows[[k in keys for k in zip(rows.chain_id, rows.vault)]]
    daily = pd.read_csv(DERIVED / "metrics/e4/vault_daily.csv", usecols=["chain_id", "vault", "date",
                                                                          "tl_headline"])
    daily["vault"] = daily.vault.str.lower()
    daily["d"] = day_of(daily.date)
    by_all = {k: g.set_index("d").tl_headline for k, g in daily.groupby(["chain_id", "vault"])}
    want = set(zip(rows.chain_id, rows.vault))
    daily = daily[[k in want for k in zip(daily.chain_id, daily.vault)]]
    by = {k: g.set_index("d").tl_headline for k, g in daily.groupby(["chain_id", "vault"])}
    pre1, pre30, n30 = [], [], []
    for c, v, d in zip(rows.chain_id, rows.vault, rows.t0d):
        s = by.get((c, v))
        x = s.get(d - 1, np.nan) if s is not None else np.nan
        w = s[(s.index >= d - WINDOW_D) & (s.index <= d - 1)].dropna() if s is not None else pd.Series([])
        pre1.append(float(x) if x is not None else np.nan)
        pre30.append(float(w.mean()) if len(w) else np.nan)
        n30.append(int(len(w)))
    rows["tl_pre_1d"] = pre1
    rows["tl_pre_30d_mean"] = pre30
    rows["tl_pre_30d_days"] = n30
    whole = dict(zip(zip(sc.chain_id, sc.vault), sc.tl_headline))
    rows["tl_whole_period"] = [whole.get(k, np.nan) for k in zip(rows.chain_id, rows.vault)]
    rows = rows[np.isfinite(rows.tl_pre_1d)].copy()
    rows["cluster"] = [c if isinstance(c, str) and c else f"vault:{ch}:{v}"
                       for c, ch, v in zip(rows.curator, rows.chain_id, rows.vault)]
    out = {"n_rows": int(len(rows)), "events": sorted(rows.event_id.unique().tolist()),
           "vaults": int(rows[["chain_id", "vault"]].drop_duplicates().shape[0]),
           "curators": int(rows.curator.dropna().nunique()),
           "rows_with_positive_loss": int((rows.loss_per_dollar > 0).sum()),
           "vault_event_level": {}, "curator_level": {}, "vault_level": vault_level(e4, by_all, rng)}
    for col in ("tl_pre_1d", "tl_pre_30d_mean", "tl_whole_period"):
        d = rows[np.isfinite(rows[col])]
        out["vault_event_level"][col] = e4.perm_tests(d[col].values, d.loss_per_dollar.values,
                                                      d.cluster.values, rng)
    cur = rows[rows.curator.notna() & (rows.curator != "")]
    for col in ("tl_pre_1d", "tl_pre_30d_mean", "tl_whole_period"):
        recs = []
        for ent, g in cur[np.isfinite(cur[col])].groupby("curator"):
            w = g.value_t0_usd.fillna(0).values
            if w.sum() <= 0:
                continue
            recs.append((ent, float(np.average(g[col].values, weights=w)),
                         float(g.loss_usd.fillna(0).sum() / g.value_t0_usd.sum())))
        c = pd.DataFrame(recs, columns=["curator", "x", "y"])
        out["curator_level"][col] = e4.perm_tests(c.x.values, c.y.values, c.curator.values, rng) \
            if len(c) >= 3 else {"n": int(len(c))}
    def plain(x):
        x = x.item() if hasattr(x, "item") else x
        return None if isinstance(x, float) and not np.isfinite(x) else x
    out["rows"] = [{k: plain(x) for k, x in r.items()} for r in rows[[
                        "event_id", "family", "chain_id", "vault", "name", "curator", "t0_utc",
                        "value_t0_usd", "position_t0_usd", "loss_usd", "loss_per_dollar", "tl_pre_1d",
                        "tl_pre_30d_mean", "tl_pre_30d_days", "tl_whole_period"]].to_dict("records")]
    meta = {"what": "Spearman rank tests of pre-onset tail loss (E4 tl_headline on the day before t0, "
                    "and its 30-day mean) against realized loss per dollar in E5's depeg-loss events, "
                    "vault-event and curator level (see the script docstring)",
            "parameters": {"window_days": WINDOW_D, "seed": SEED, "n_perm": e4.N_PERM,
                           "n_boot": e4.N_BOOT},
            "inputs": {p: e4.file_stamp(DERIVED / p) for p in (
                "metrics/e5/event_vault.csv", "metrics/e4/vault_daily.csv",
                "metrics/e4/vault_scorecard.csv")}}
    io_utils.write_json(OUT, out, GENERATOR, meta)
    for lvl in ("vault_event_level", "curator_level", "vault_level"):
        for col, r in out[lvl].items():
            if not isinstance(r, dict):
                continue
            print(lvl, col, {k: r.get(k) for k in ("rho", "p_perm_clustered", "ci95_cluster_bootstrap", "n",
                                                   "clusters")})


if __name__ == "__main__":
    main()
