# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Rebuild the tables of the paper in tables/ from the files under data/."""
from __future__ import annotations

import importlib
import sys

TABLES = ["table2_components", "table3_events", "table4_scorecard", "table_incentives", "table5_fees",
          "table6_autocurator"]


def main():
    names = sys.argv[1:] or TABLES
    unknown = [n for n in names if n not in TABLES]
    if unknown:
        sys.exit(f"unknown table(s): {', '.join(unknown)}; choose from {', '.join(TABLES)}")
    for name in names:
        importlib.import_module(name).main()


if __name__ == "__main__":
    main()
