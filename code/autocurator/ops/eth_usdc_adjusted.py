"""Ethereum USDC group (morpho_1_USDC_a0b869), common window, with two pinned spells treated as frozen by hand."""
import csv, glob, gzip, math, statistics as st
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RUNS = ROOT / "data/autocurator/runs"
OUT = ROOT / "data/autocurator/results/eth_usdc_adjusted.csv"
GROUP = "morpho_1_USDC_a0b869"
D0, D1 = "2025-10-02", "2026-10-01"
SPELLS = [("2026-09-10", D1), ("2026-06-21", D1)]
HOT = 0.001

rows = []
for p in sorted(RUNS.glob(f"*/{GROUP}__*__r1.e4.csv.gz")):
    rs = [(r["date"], float(r["ret_before_fee"])) for r in csv.DictReader(gzip.open(p, "rt"))
          if D0 <= r["date"] <= D1 and r["ret_before_fee"]]
    med = st.median(x for _, x in rs)
    adj = [med if x > HOT and any(a <= d <= b for a, b in SPELLS) else x for d, x in rs]
    rows.append({"label": p.parent.name, "yield_raw": math.prod(1 + x for _, x in rs) - 1,
                 "yield_adjusted": math.prod(1 + x for x in adj) - 1,
                 "days_replaced": sum(a != x for a, (_, x) in zip(adj, rs))})
with open(OUT, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]))
    w.writeheader(); w.writerows(rows)
print(OUT, len(rows))
