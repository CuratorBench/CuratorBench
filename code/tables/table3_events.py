# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Table 3, the stress-event set (E2)."""
from __future__ import annotations

import sys

import tabfmt as T

sys.path.insert(0, str(T.FIG_CODE))
import style

GENERATOR = "code/tables/table3_events.py"
NOTE_WIDTH = 120
MAIN = T.DATA / "events/table3_main.csv"
FULL = T.DATA / "events/table3.csv"
EVENTS = T.DATA / "events/events.jsonl"
FAMILIES = T.DATA / "events/collateral_families.jsonl"


def display_names():
    """{event_id: collateral display name}."""
    members = {f["family_id"]: [m["symbol"] for m in f.get("members", [])] for f in T.read_jsonl(FAMILIES)}
    out = {}
    for e in T.read_jsonl(EVENTS):
        fam = e["collateral_family"]
        out[e["event_id"]] = style.event_display(e, members.get(fam["family_id"], []))
    return out


def protocols(cell: str) -> str:
    have = set(cell.split(";"))
    return ", ".join(T.PROTOCOL_LABEL[p] for p in T.PROTOCOL_ORDER if p in have)


def exposure(r) -> float:
    return max(float(r["exposure_third_party_usd"]), float(r["exposure_third_party_usd_peak_after_t0"]))


TRIGGER_MARK = r"$^\ddagger$"


def date_cell(r) -> str:
    return r["date"] + (TRIGGER_MARK if r["first_trigger"] != r["date"] else "")


def trigger_note(rows, names) -> str | None:
    """Footnote for events dated at their loss onset whose first trigger (a lock-up episode) came earlier."""
    diff = [r for r in rows if r["first_trigger"] != r["date"]]
    if not diff:
        return None
    items = "; ".join(f"{T.tex(names[r['event_id']])} {r['first_trigger']}" for r in diff)
    return (f"{TRIGGER_MARK}Loss onset of a depeg-loss event whose first episode is a lock-up; "
            f"first trigger: {items}.")


def main_table(names):
    rows = T.read_csv(MAIN)
    body = [r"\begin{tabular}{rllllrrrrrr}", r"\toprule",
            T.row(["", "", "", "", "", "", r"Affected", r"Curated", r"Min.", r"Exposed", r"Exposed"]),
            T.row(["\\#", "Date", "Collateral", "Type", "Protocols", "Markets", r"supply (\$M)",
                   r"exposure (\$M)", "price", "vaults", "curators"]),
            r"\midrule"]
    for r in rows:
        body.append(T.row([r["main_text_rank"], date_cell(r), T.tex(names[r["event_id"]]),
                           style.EVENT_TYPE_LABEL[r["type"]], protocols(r["protocols"]), T.count(r["markets"]),
                           T.usd_m(r["affected_supply_usd"]), T.usd_m(exposure(r)),
                           T.price(r["min_market_price"]), T.count(r["exposed_vaults"]),
                           T.count(r["exposed_curators"])]))
    body.append(r"\bottomrule")
    note = trigger_note(rows, names)
    if note:
        body += T.notes(note, 11, NOTE_WIDTH)
    body.append(r"\end{tabular}")
    return T.write("table3_events_main", GENERATOR, [MAIN, EVENTS, FAMILIES], body)


PROT_LETTER = {"morpho": "M", "euler_v2": "E", "moolah": "L", "silo_v2": "S"}
TYPE_LETTER = {"lockup": "lock-up", "depeg_loss": "failure"}


def full_table(names):
    """One column wide: protocols and types as letters, no notes (the caption carries them)."""
    rows = T.read_csv(FULL)
    body = [r"\begin{tabular}{@{}llcl rrrrr@{}}", r"\toprule",
            T.row(["", "", "", "", "", r"Exp.", "Min.", "", ""]),
            T.row(["Date", "Collateral", "Type", "Prot.", "Mkts", r"(\$M)", "price", "Vaults", "Cur."]),
            r"\midrule"]
    for r in rows:
        name = T.tex(names[r["event_id"]]) + (r"$^\dagger$" if r["self_curated"] == "True" else "")
        have = set(r["protocols"].split(";"))
        prot = "".join(PROT_LETTER[p] for p in T.PROTOCOL_ORDER if p in have)
        body.append(T.row([date_cell(r), name, TYPE_LETTER.get(r["type"], r["type"][:1].upper()), prot,
                           T.count(r["markets"]), T.usd_m(r["exposure_third_party_usd"]),
                           T.price(r["min_market_price"]), T.count(r["exposed_vaults"]),
                           T.count(r["exposed_curators"])]))
    body.append(r"\bottomrule")
    body.append(r"\end{tabular}")
    return T.write("table3_events_full", GENERATOR, [FULL, EVENTS, FAMILIES], body)


def top_table(names):
    """The main-text events, one column wide."""
    rows = T.read_csv(MAIN)
    body = [r"\begin{tabular}{@{}llcl rrrrr@{}}", r"\toprule",
            T.row(["", "", "", "", "", r"Exp.", "Min.", "", ""]),
            T.row(["Date", "Collateral", "Type", "Prot.", "Mkts", r"(\$M)", "price", "Vaults", "Cur."]),
            r"\midrule"]
    for r in rows:
        have = set(r["protocols"].split(";"))
        prot = "".join(PROT_LETTER[p] for p in T.PROTOCOL_ORDER if p in have)
        body.append(T.row([date_cell(r), T.tex(names[r["event_id"]]), TYPE_LETTER.get(r["type"], r["type"][:1].upper()),
                           prot, T.count(r["markets"]), T.usd_m(exposure(r)), T.price(r["min_market_price"]),
                           T.count(r["exposed_vaults"]), T.count(r["exposed_curators"])]))
    body.append(r"\bottomrule")
    body.append(r"\end{tabular}")
    return T.write("table3_events_top", GENERATOR, [MAIN, EVENTS, FAMILIES], body)


def main():
    names = display_names()
    for p in (main_table(names), full_table(names), top_table(names)):
        print(T.rel(p))


if __name__ == "__main__":
    main()
