# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.8", "numpy>=1.26", "pandas>=2.1", "scipy>=1.11"]
# ///
"""Fig: rank correlations among displayed APY and the four metrics, across vaults (left) and across named curators (right)."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
from scipy.stats import rankdata

import style

RANK = style.DATA_OUT / "metrics/e4/rank_agreement.json"
VAULTS = style.DATA_OUT / "metrics/e4/vault_scorecard.csv"
CURATORS = style.DATA_OUT / "metrics/e4/curator_scorecard.csv"
E5_VAULT = style.DATA_OUT / "metrics/e5/event_vault.csv"
E5_CURATOR = style.DATA_OUT / "metrics/e5/curator_summary.csv"

SEED = 20261018
N_PERM, N_BOOT = 10_000, 2_000
M = ["vault_apy", "yield_after_fee", "tl_headline", "L_p10", "event_loss"]
NAME = {"vault_apy": "Displayed\nAPY", "yield_after_fee": "Realized\nyield",
        "tl_headline": "Tail loss", "L_p10": "Liquidity", "event_loss": "Event loss"}
DIVERGING = ["#0d366b", "#256abf", "#86b6ef", "#f0efec", "#f2a09f", "#e34948", "#9e2726"]


def perm_tests(x, y, clusters, rng):
    """e4_metrics.perm_tests."""
    x = np.asarray(x, float)
    y = np.asarray(y, float)
    rx, ry = rankdata(x), rankdata(y)
    obs = float(np.corrcoef(rx, ry)[0, 1])
    codes, uniq = pd.factorize(pd.Series(clusters))
    k = len(uniq)
    means = np.bincount(codes, weights=ry, minlength=k) / np.bincount(codes, minlength=k)
    dev = ry - means[codes]
    base = np.argsort(codes, kind="stable")
    clus = 0
    for _ in range(N_PERM):
        order = np.lexsort((rng.random(len(codes)), codes))
        dshuf = np.empty_like(dev)
        dshuf[base] = dev[order]
        ystar = means[rng.permutation(k)][codes] + dshuf
        if abs(np.corrcoef(rx, rankdata(ystar))[0, 1]) >= abs(obs) - 1e-12:
            clus += 1
    members = [np.flatnonzero(codes == c) for c in range(k)]
    boots = []
    for _ in range(N_BOOT):
        idx = np.concatenate([members[c] for c in rng.integers(0, k, k)])
        if len(np.unique(x[idx])) > 1 and len(np.unique(y[idx])) > 1:
            boots.append(float(np.corrcoef(rankdata(x[idx]), rankdata(y[idx]))[0, 1]))
    lo, hi = np.percentile(boots, [2.5, 97.5])
    return {"rho": obs, "p_perm_clustered": (clus + 1) / (N_PERM + 1),
            "ci95_cluster_bootstrap": [float(lo), float(hi)], "n": int(len(x)), "clusters": int(k)}


def event_correlations():
    rng = np.random.default_rng(SEED)
    v = pd.read_csv(VAULTS)
    v = v[(v.status == "in") & v.yield_after_fee.notna()].copy()
    v["vault"] = v.vault.str.lower()
    e = pd.read_csv(E5_VAULT, low_memory=False)
    e["vault"] = e.vault.str.lower()
    d = e[(e.event_type == "depeg_loss") & (e.value_t0_usd > 0)
          & (e.group.eq("present_at_t0") | (e.loss_usd.fillna(0) > 0))]
    g = d.groupby(["chain_id", "vault"]).agg(loss=("loss_usd", "sum"), val=("value_t0_usd", "sum"))
    g = (g.loss.fillna(0) / g.val).rename("event_loss").reset_index()
    v = v.merge(g, on=["chain_id", "vault"], how="left")
    v["cluster"] = v.curator_entity.fillna("").where(v.curator_entity.fillna("") != "", v.vault)
    c = pd.read_csv(CURATORS)
    c = c[c.curator_name.fillna("") != ""]
    cs = pd.read_csv(E5_CURATOR)[["curator", "loss_per_dollar_value", "depeg_events_entered"]]
    c = c.merge(cs, left_on="curator_entity", right_on="curator", how="left")
    c["event_loss"] = c.loss_per_dollar_value.where(c.depeg_events_entered.fillna(0) > 0)
    out = {"vault_level": {}, "curator_level": {}, "vault_exposed": {}, "curator_exposed": {}}
    vx, cx = v[v.event_loss.notna()], c[c.event_loss.notna()]
    for m in M[:-1]:
        out["vault_level"][m] = perm_tests(v[m], v.event_loss.fillna(0), v.cluster, rng)
        out["curator_level"][m] = perm_tests(c[m], c.event_loss.fillna(0), c.curator_entity, rng)
        out["vault_exposed"][m] = perm_tests(vx[m], vx.event_loss, vx.cluster, rng)
        out["curator_exposed"][m] = perm_tests(cx[m], cx.event_loss, cx.curator_entity, rng)
    return out


def main():
    plt = style.setup()
    from matplotlib.colors import LinearSegmentedColormap, to_hex
    ra = json.loads(RANK.read_text())
    ev = event_correlations()

    def cell(level, a, b):
        if "event_loss" in (a, b):
            return ev[level][b if a == "event_loss" else a]
        p = ra[level]["pairs"]
        return p.get(f"{a}|{b}") or p[f"{b}|{a}"]

    cmap = LinearSegmentedColormap.from_list("diverging", DIVERGING)
    n = len(M)
    W = style.COL_IN
    cs = (W - 0.06) / n
    ch = 0.64 * cs
    x0, y0 = 0.03, 0.17
    H = n * ch + y0 + 0.03
    fig = plt.figure(figsize=(W, H))
    ax = fig.add_axes([x0 / W, y0 / H, n * cs / W, n * ch / H])
    for i in range(n):
        for j in range(n):
            y = n - 1 - i
            if i == j:
                ax.add_patch(plt.Rectangle((j + 0.03, y + 0.03), 0.94, 0.94, color=style.SURFACE,
                                           ec=style.GRID, lw=0.6))
                ax.text(j + 0.5, y + 0.5, NAME[M[i]], ha="center", va="center", fontsize=6.5,
                        color=style.INK, linespacing=1.05)
                continue
            level = "vault_level" if i > j else "curator_level"
            d = cell(level, M[i], M[j])
            r, (lo, hi) = d["rho"], d["ci95_cluster_bootstrap"]
            fill = to_hex(cmap((r + 1) / 2))
            ink = style.ink_on(fill)
            ax.add_patch(plt.Rectangle((j + 0.03, y + 0.03), 0.94, 0.94, color=fill, lw=0))
            ax.text(j + 0.5, y + 0.62, f"{r:.2f}".replace("-", "\u2212"), ha="center", va="center",
                    fontsize=7.5, color=ink, fontweight="bold" if (lo > 0 or hi < 0) else "normal")
            ax.text(j + 0.5, y + 0.27,
                    f"[{lo:.2f}, {hi:.2f}]".replace("-0.00", "0.00").replace("-", "\u2212"),
                    ha="center", va="center", fontsize=5.2, color=ink)
    ax.set_xlim(0, n)
    ax.set_ylim(0, n)
    ax.set_axis_off()
    nv, nc = ra["vault_level"]["n_vaults"], ra["curator_level"]["n_curators"]
    ax.text(0.03, -0.14, f"Below the diagonal: {nv} vaults", ha="left", va="top",
            fontsize=6.5, color=style.INK2)
    ax.text(n - 0.03, -0.14, f"Above the diagonal: {nc} curators", ha="right", va="top",
            fontsize=6.5, color=style.INK2)
    style.save(fig, "fig_correlations", [RANK, VAULTS, CURATORS, E5_VAULT, E5_CURATOR],
               "Rank correlations among displayed APY, realized yield, tail loss, liquidity "
               "and event loss, across vaults and across named curators",
               extra={"event_loss_correlations": ev, "seed": SEED, "n_perm": N_PERM, "n_boot": N_BOOT})


if __name__ == "__main__":
    main()
