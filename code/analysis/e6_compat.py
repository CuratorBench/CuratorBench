# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy>=1.26", "pandas>=2.1", "scipy>=1.11", "matplotlib>=3.8", "statsmodels>=0.14", "requests>=2.31", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "hypersync==1.2.0"]
# ///
"""E6 extension: is the curator's pay tied to what depositors get?."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "code" / "analysis"))
sys.path.insert(0, str(ROOT / "code"))
import e6_incentives as e6

DATA = ROOT / "data"
E4 = e6.E4_DIR
E5 = e6.DERIVED / "metrics/e5"
E6 = e6.OUT
OUT = E6 / "compat.json"
ACT_OUT = E6 / "activity_by_vault.csv"
SEED = 20261018
N_PERM = 5000
PUBLIC_ALLOCATOR = {"0xfd32fa2ca22c76dd6e550706ad913fc6ce91c75d", "0xa090dd1a701408df1d4d0b85b716c87565f90467",
                    "0x517505be22d9068687334e69ae7a02fc77edf4fc", "0x769583af5e9d03589f159ebec31cc2c23e8c355e",
                    "0x39eb6da5e88194c82b13491df2e8b3e213ed2412"}
REALLOC = {"ReallocateSupply", "ReallocateWithdraw"}
CONFIG = {"SetCap", "SubmitCap", "SetSupplyQueue", "SetWithdrawQueue", "IncreaseAbsoluteCap",
          "DecreaseAbsoluteCap", "IncreaseRelativeCap", "DecreaseRelativeCap"}
V2_ALLOC = {"Allocate", "Deallocate"}
OUTCOMES = ["vault_apy", "yield_before_fee", "yield_after_fee", "tl_headline", "L_p10", "event_loss", "activity"]


def activity(sample):
    """Distinct curator-action transactions per vault (sample vaults only)."""
    want = defaultdict(set)
    for r in sample.itertuples():
        want[(r.protocol, str(r.chain_id))].add(r.vault.lower())
    txs = defaultdict(set)
    allocators = defaultdict(set)
    for f in sorted((DATA / "vaults").glob("*/*/actions.jsonl")):
        proto, cid = f.parts[-3], f.parts[-2]
        keep = want.get((proto if proto != "euler_v2" else "euler", cid)) or want.get((proto, cid))
        if not keep:
            continue
        pending = []
        with open(f) as fh:
            for line in fh:
                if '"event":"' not in line:
                    continue
                d = json.loads(line)
                v = d.get("vault", "").lower()
                if v not in keep:
                    continue
                ev = d.get("event")
                fl = d.get("fields") or {}
                if ev == "SetIsAllocator":
                    a = str(fl.get("allocator", "")).lower()
                    if fl.get("isAllocator") and a not in PUBLIC_ALLOCATOR:
                        allocators[(cid, v)].add(a)
                elif ev in REALLOC:
                    if str(fl.get("caller", "")).lower() not in PUBLIC_ALLOCATOR:
                        txs[(cid, v)].add(d["tx_hash"])
                elif ev in CONFIG:
                    txs[(cid, v)].add(d["tx_hash"])
                elif ev in V2_ALLOC:
                    pending.append((cid, v, str(fl.get("sender", "")).lower(), d["tx_hash"]))
        for cid_, v, s, tx in pending:
            if s in allocators[(cid_, v)]:
                txs[(cid_, v)].add(tx)
        print(f"activity: {f.parts[-3]}/{cid} done", flush=True)
    return {k: len(s) for k, s in txs.items()}


def main():
    rng = np.random.default_rng(SEED)
    v = pd.read_csv(E4 / "vault_scorecard.csv")
    v = v[v.status == "in"].copy()
    v["vault"] = v.vault.str.lower()
    f6 = pd.read_csv(E6 / "fig6_data.csv")[["chain_id", "vault", "class", "fee_rate"]]
    f6["vault"] = f6.vault.str.lower()
    v = v.merge(f6, on=["chain_id", "vault"], how="left")
    fi = pd.read_csv(E6 / "fee_income_by_vault.csv")[["chain_id", "vault", "fee_usd_curator"]]
    fi["vault"] = fi.vault.str.lower()
    v = v.merge(fi, on=["chain_id", "vault"], how="left")
    v["fee_usd_curator"] = v.fee_usd_curator.fillna(0.0)
    v["usd_days"] = v.net_usd_days
    ev = pd.read_csv(E5 / "event_vault.csv", low_memory=False)
    ev["vault"] = ev.vault.str.lower()
    dl = ev[(ev.event_type == "depeg_loss") & (ev.value_t0_usd > 0)
            & (ev.group.eq("present_at_t0") | (ev.loss_usd.fillna(0) > 0))]
    g = dl.groupby(["chain_id", "vault"]).agg(loss=("loss_usd", "sum"), val=("value_t0_usd", "sum"))
    g = (g.loss.fillna(0) / g.val).rename("event_loss").reset_index()
    v = v.merge(g, on=["chain_id", "vault"], how="left")
    v["event_loss"] = v.event_loss.fillna(0.0)
    proto_map = v.protocol.where(v.protocol != "euler_v2", "euler_v2")
    v["protocol"] = proto_map
    if "--reuse-activity" in sys.argv and ACT_OUT.exists():
        a0 = pd.read_csv(ACT_OUT)
        counts = {(str(c), x.lower()): int(n) for c, x, n in zip(a0.chain_id, a0.vault, a0.act_tx)}
    else:
        counts = activity(v[v.kind != "euler_lending"])
    v["act_tx"] = [counts.get((str(c), a), 0) for c, a in zip(v.chain_id, v.vault)]
    v["activity"] = np.where(v.kind != "euler_lending", v.act_tx / np.maximum(v.active_days / 30.4, 1.0), np.nan)
    v[["chain_id", "vault", "kind", "curator_entity", "act_tx", "active_days", "activity"]].to_csv(ACT_OUT, index=False)
    v["cl"] = v.curator_entity.fillna("")
    out = {"sample": {"vaults": int(len(v)), "allocator_vaults": int((v.kind != "euler_lending").sum()),
                      "fee_charging": int((v.fee_rate > 0).sum())}}

    def wc(x, y, df=None, stat=e6.stat_spearman):
        df = v if df is None else df
        d = df[np.isfinite(df[x]) & np.isfinite(df[y])]
        r = e6.within_class_test(d, x, y, stat, rng, nperm=N_PERM)
        return {"value": r["pooled"], "p": r.get("p_perm_clustered"), "n": r.get("n")}

    out["q1_fee_rate_vault"] = {y: wc("fee_rate", y) for y in OUTCOMES}
    cs = pd.read_csv(E4 / "curator_scorecard.csv")
    cs = cs[cs.curator_name.fillna("") != ""]
    act_c = v[v.kind != "euler_lending"].groupby("curator_entity").apply(
        lambda d: d.act_tx.sum() / max(d.active_days.sum() / 30.4, 1.0)).rename("activity")
    evl = v.groupby("curator_entity").apply(
        lambda d: np.average(d.event_loss, weights=d.usd_days) if d.usd_days.sum() > 0 else np.nan).rename("event_loss")
    feec = v.groupby("curator_entity").fee_usd_curator.sum().rename("fee_income")
    rate_c = v[np.isfinite(v.fee_rate)].groupby("curator_entity").apply(
        lambda d: np.average(d.fee_rate, weights=d.usd_days) if d.usd_days.sum() > 0 else np.nan).rename("fee_rate")
    c = cs.merge(rate_c, on="curator_entity", how="left")
    c = c.merge(act_c, on="curator_entity", how="left").merge(evl, on="curator_entity", how="left") \
         .merge(feec, on="curator_entity", how="left")
    c["usd_days"] = c.aum_avg_usd

    def sp(x, y, d):
        d = d[np.isfinite(d[x]) & np.isfinite(d[y])]
        rx, ry = e6.rank(d[x].values), e6.rank(d[y].values)
        obs = e6.spearman_r(rx, ry)
        hits = sum(abs(e6.spearman_r(rx, rng.permutation(ry))) >= abs(obs) - 1e-12 for _ in range(N_PERM))
        return {"value": obs, "p": (hits + 1) / (N_PERM + 1), "n": int(len(d))}

    out["q1_fee_rate_curator"] = {y: sp("fee_rate", y, c) for y in OUTCOMES if y in c}
    v["log_fee"] = np.log(v.fee_usd_curator.where(v.fee_usd_curator > 0))
    out["q2_fee_income_vault"] = {y: wc("fee_usd_curator", y) for y in OUTCOMES + ["usd_days"]}
    out["q2_fee_income_curator"] = {y: sp("fee_income", y, c) for y in OUTCOMES + ["usd_days"] if y in c}
    pos = v[(v.fee_usd_curator > 0) & (v.usd_days > 0)]
    a = np.log(pos.usd_days.values)
    b = np.log(pos.fee_usd_curator.values) - a
    tot = np.var(a + b)
    out["q2_variance_share"] = {"n": int(len(pos)), "var_log_fee": float(tot),
                                "share_size": float((np.var(a) + np.cov(a, b)[0, 1]) / tot),
                                "share_fee_per_usd_day": float((np.var(b) + np.cov(a, b)[0, 1]) / tot)}
    panel, _w = e6.weekly_panel()
    panel = panel.merge(v[["chain_id", "vault", "fee_rate"]], on=["chain_id", "vault"], how="left")
    panel = panel[np.isfinite(panel.fee_rate)]
    y = panel.flow_rate_w.values
    X = panel[["apy", "tl", "fee_rate"]].values
    fe = pd.factorize(panel.cluster)[0]
    bA, se, _e, G = e6.ols_cluster(e6.demean(y, fe), e6.demean(X, fe), panel.cluster.values)
    out["q3_flows_with_fee"] = {"coef": dict(zip(["apy", "tail_loss", "fee_rate"], map(float, bA))),
                                "se": dict(zip(["apy", "tail_loss", "fee_rate"], map(float, se))),
                                "n": int(len(panel)), "clusters": int(G)}
    av = v[v.kind != "euler_lending"]
    out["q4_activity"] = {"median_per_month": float(np.nanmedian(av.activity)),
                          "fee_rate": wc("fee_rate", "activity", av),
                          "outcomes": {y: wc("activity", y, av) for y in OUTCOMES if y != "activity"}}
    rows = ev[(ev.third_party == True) & ev.group.isin(["exited_before_t0", "present_at_t0"])].copy()
    rows = rows.merge(v[["chain_id", "vault", "activity", "fee_rate", "class"]], on=["chain_id", "vault"], how="inner")
    rows["exited"] = (rows.group == "exited_before_t0").astype(float)

    def rb(df, col):
        d = df[np.isfinite(df[col])]
        a_, b_ = d[d.exited == 1][col].values, d[d.exited == 0][col].values
        if len(a_) < 3 or len(b_) < 3:
            return None
        r = e6.rank(np.concatenate([a_, b_]))
        u = r[:len(a_)].sum() - len(a_) * (len(a_) + 1) / 2
        obs = 2 * u / (len(a_) * len(b_)) - 1
        codes = e6.cluster_codes(d.curator.values, d.vault.values)
        x = d.exited.values
        yv = d[col].values
        hits = 0
        for _ in range(N_PERM):
            ys = e6.cluster_shuffle(yv, codes, rng)
            a2, b2 = ys[x == 1], ys[x == 0]
            r2 = e6.rank(np.concatenate([a2, b2]))
            u2 = r2[:len(a2)].sum() - len(a2) * (len(a2) + 1) / 2
            if abs(2 * u2 / (len(a2) * len(b2)) - 1) >= abs(obs) - 1e-12:
                hits += 1
        return {"rank_biserial_exited_vs_present": float(obs), "p": (hits + 1) / (N_PERM + 1),
                "n_exited": int(len(a_)), "n_present": int(len(b_)),
                "median_exited": float(np.median(a_)), "median_present": float(np.median(b_))}

    out["q4_q5_reactions"] = {
        "all_events": {"activity": rb(rows, "activity"), "fee_rate": rb(rows, "fee_rate")},
        "depeg_loss": {"activity": rb(rows[rows.event_type == "depeg_loss"], "activity"),
                       "fee_rate": rb(rows[rows.event_type == "depeg_loss"], "fee_rate")}}
    ex = rows[(rows.exited == 1) & np.isfinite(rows.exit_lead_days.astype(float))].copy()
    ex["lead"] = ex.exit_lead_days.astype(float)
    out["q5_lead_vs_fee"] = sp("fee_rate", "lead", ex.assign(fee_rate=ex.fee_rate))
    out["q5_lead_vs_activity"] = sp("activity", "lead", ex)
    OUT.write_text(json.dumps(out, indent=1, default=float) + "\n")
    meta = {"generator": "code/analysis/e6_compat.py", "written_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seed": SEED, "n_perm": N_PERM, "rpc_calls": 0}
    (E6 / "compat.json.meta.json").write_text(json.dumps(meta, indent=1) + "\n")
    print(json.dumps(out, indent=1, default=float)[:6000])


if __name__ == "__main__":
    main()
