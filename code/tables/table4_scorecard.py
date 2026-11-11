# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Table 4, the curator scorecard (E4 with the E5 outcome columns), one column."""
from __future__ import annotations

import tabfmt as T

GENERATOR = "code/tables/table4_scorecard.py"
SCORECARD = T.DATA / "metrics/e4/curator_scorecard.csv"
E5 = T.DATA / "metrics/e5/curator_summary.csv"
N_ROWS = 12


def main():
    cards = sorted(T.read_csv(SCORECARD), key=lambda r: int(r["rank_by_aum"]))[:N_ROWS]
    e5 = {r["curator"]: r for r in T.read_csv(E5)}
    body = [r"\begin{tabular}{llrrrrrrrrr}", r"\toprule",
            T.row(["", "", "", "AUM", "", "", "", "", r"\multicolumn{3}{c}{Events}"]),
            r"\cmidrule(lr){9-11}",
            T.row(["Curator", "Prot.", "Vaults", r"(\$M)", "APY", r"$\yieldm$", r"$\TailLoss$", r"$\Lp$",
                   "in", "out", "loss"]),
            r"\midrule"]
    for r in cards:
        s = e5.get(r["curator_entity"])
        prot = "".join(T.PROTOCOL_SHORT[p] for p in T.PROTOCOL_ORDER if p in r["protocols"].split(";"))
        body.append(T.row(
            [T.tex(T.curator(r["curator_name"])), prot, T.count(r["vaults"]),
             f"{float(r['aum_avg_usd']) / 1e6:,.0f}".replace(",", "{,}"),
             T.pct(r["vault_apy"]), T.pct(r["yield_after_fee"]), T.pct(r["tl_headline"]),
             T.pct(r["L_p10"]),
             T.count(s["events_entered"] if s else 0), T.count(s["events_exited_before_onset"] if s else 0),
             T.pct(s["loss_per_dollar_value"]) if s else T.MISSING]))
    body += [r"\bottomrule", r"\end{tabular}"]
    print(T.rel(T.write("table4_scorecard", GENERATOR, [SCORECARD, E5], body)))

if __name__ == "__main__":
    main()
