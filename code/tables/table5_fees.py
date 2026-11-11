# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Table 5, fee income against depositor losses (E6 with E5's losses)."""
from __future__ import annotations

import math
from collections import defaultdict

import tabfmt as T

GENERATOR = "code/tables/table5_fees.py"
FEES = T.DATA / "metrics/e6/fee_income_by_curator.csv"
FEES_VAULT = T.DATA / "metrics/e6/fee_income_by_vault.csv"
EVENT_VAULT = T.DATA / "metrics/e5/event_vault.csv"
E6_TABLE = T.DATA / "metrics/e6/table5.csv"
SCORECARD = T.DATA / "metrics/e4/curator_scorecard.csv"
N_TOP = 12
LOSS_FLOOR_USD = 1e6
MIN_FEE_USD = 10_000


def close(a, b) -> bool:
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-6)


def main():
    fees = {r["entity"]: r for r in T.read_csv(FEES)}
    loss = defaultdict(float)
    loss_all = 0.0
    for r in T.read_csv(EVENT_VAULT):
        if T.number(r["loss_usd_counted"]):
            loss_all += float(r["loss_usd_counted"])
        if r["curator"] and r["self_deposited"] != "True" and T.number(r["loss_usd_counted"]):
            loss[r["curator"]] += float(r["loss_usd_counted"])
    e6 = {r["entity"]: r for r in T.read_csv(E6_TABLE)}
    names = {r["curator_entity"]: T.curator(r["curator_name"]) for r in T.read_csv(SCORECARD)}
    names.update({k: T.curator(r["name"]) for k, r in e6.items()})
    for k, r in e6.items():
        assert close(float(r["fee_income_usd"]), float(fees[k]["fee_usd_curator"]) if k in fees else 0.0), k
        assert close(float(r["depositor_loss_usd"]), loss.get(k, 0.0)), k
    top = [r["curator_entity"] for r in sorted(T.read_csv(SCORECARD), key=lambda r: int(r["rank_by_aum"]))][:N_TOP]
    more = sorted((k for k, v in loss.items() if v >= LOSS_FLOOR_USD and k not in top), key=lambda k: -loss[k])

    def cells(name, fee, lost, rate=None, pre=None, aum=None):
        return [name, T.pct(rate), T.usd_m(fee), T.usd_m(lost),
                T.ratio(lost / fee) if fee >= MIN_FEE_USD and lost else T.MISSING,
                T.usd_k(pre), T.pct(aum, signed=True)]

    def curator(k):
        r = e6.get(k, {})
        n_rep = int(r.get("events_in_reputation_test") or 0)
        return T.row(cells(T.tex(names.get(k, k)), float(fees[k]["fee_usd_curator"]) if k in fees else 0.0,
                           loss.get(k, 0.0), r.get("avg_fee_rate"),
                           r.get("fee_income_affected_vaults_365d_before_usd")
                           if int(r.get("events_present_at_t0") or 0) else None,
                           r.get("aum_change_90d_after_segment_mean") if n_rep else None))

    body = [r"\begin{tabular}{lrrrrrr}", r"\toprule",
            T.row(["", "Mean", "Fee", "Depositor", "Loss per", "Fees", "AUM"]),
            T.row(["Curator", "fee rate", "income", "loss", "fee", "before", "after"]),
            T.row(["", r"(\%)", r"(\$M)", r"(\$M)", "dollar", r"(\$k)", r"(\%)"]),
            r"\midrule"]
    body += [curator(k) for k in top]
    if more:
        body.append(r"\midrule")
        body += [curator(k) for k in more]
    fee_all = sum(T.number(r["fee_usd_curator"]) or 0.0 for r in T.read_csv(FEES_VAULT))
    body += [r"\midrule",
             T.row(cells("All curated vaults", fee_all, loss_all)),
             r"\bottomrule"]
    body.append(r"\end{tabular}")
    print(T.rel(T.write("table5_fees", GENERATOR, [FEES, FEES_VAULT, EVENT_VAULT, E6_TABLE, SCORECARD], body)))


if __name__ == "__main__":
    main()
