# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Incentive table: what the fee rate and the fee income go with (E6)."""
from __future__ import annotations

import json

import tabfmt as T

GENERATOR = "code/tables/table_incentives.py"
COMPAT = T.DATA / "metrics/e6/compat.json"
P_BOLD = 0.05
ROWS = [("vault_apy", r"Displayed \acs{APY}"), ("yield_before_fee", "Yield before fee"),
        ("yield_after_fee", "Yield after fee"), ("tl_headline", "Tail loss"), ("L_p10", "Liquidity"),
        ("event_loss", "Event loss"), ("activity", "Activity"), ("usd_days", "Value managed")]
COLS = ["q1_fee_rate_vault", "q1_fee_rate_curator", "q2_fee_income_vault", "q2_fee_income_curator"]


def cell(block, key) -> str:
    e = block.get(key)
    if not e:
        return T.MISSING
    v = f"{e['value']:.2f}".replace("-", "$-$")
    return rf"\textbf{{{v}}}" if e["p"] < P_BOLD else v


def main():
    with open(COMPAT) as fh:
        c = json.load(fh)
    body = [r"\begin{tabular}{@{}lrrrr@{}}", r"\toprule",
            r" & \multicolumn{2}{c}{Fee rate} & \multicolumn{2}{c}{Fee income} \\",
            r"\cmidrule(lr){2-3} \cmidrule(lr){4-5}",
            T.row(["", "Vaults", "Curators", "Vaults", "Curators"]), r"\midrule"]
    body += [T.row([name] + [cell(c[col], key) for col in COLS]) for key, name in ROWS]
    body += [r"\bottomrule", r"\end{tabular}"]
    print(T.rel(T.write("table_incentives", GENERATOR, [COMPAT], body)))
    n = {col: sorted({e["n"] for e in c[col].values()}) for col in COLS}
    print("n per column:", n)


if __name__ == "__main__":
    main()
