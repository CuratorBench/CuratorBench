"""Corpus assets: collateral families with curated exposure, ranked."""
import json
import pathlib
import re
import sys
from collections import defaultdict

import numpy as np

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "code" / "analysis"))
sys.path.insert(0, str(ROOT / "code" / "data"))
sys.path.insert(0, str(HERE))
import exposure
from e4_metrics import PT_RE, underlying_symbol

DATA = ROOT / "data"
MIN_USD = 1_000_000.0
SYMBOL_ALIASES = {"USD0++": "BUSD0"}
TOKEN_GROUPS = {
    (9745, "0x29ad7fe4516909b9e498b5a65339e54791293234"): "metronome-msusd",
    (56, "0x5a110fc00474038f6c02e89c707d638602ea44b5"): "astherus-usdf",
    (1, "0xdcd0f5ab30856f28385f641580bbd85f88349124"): "autonomous-alusd",
    (1, "0x6645aa5a6f4ff29b30d53a483a02acc2cb85b241"): "autonomous-alusd",
    (1, "0xd2e230f71ca8db211067bc4070a94d268b313fa3"): "autonomous-alusd",
}


def group_symbol(sym):
    """Upper-case underlying symbol a token groups by."""
    m = PT_RE.fullmatch(sym or "")
    inner = (m.group(1) if m else sym or "").upper()
    if inner in SYMBOL_ALIASES:
        return SYMBOL_ALIASES[inner]
    return underlying_symbol(sym or "").upper()


def slug(text):
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return s or "unnamed"


class Union:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[ra] = rb


def build(log=print):
    """Return (assets, tokens, events_by_asset)."""
    expo, book, skipped = exposure.compute(log)
    fams = exposure.jsonl(DATA / "events/collateral_families.jsonl")
    events = exposure.jsonl(DATA / "events/events.jsonl")
    fam_of = {}
    for f in fams:
        for m in f["members"]:
            fam_of[(m["chain_id"], m["token"].lower())] = f
    tokens = set(expo) | set(fam_of)
    uf = Union()
    sym_of = {}
    for cid, tok in tokens:
        node = ("tok", cid, tok)
        uf.find(node)
        f = fam_of.get((cid, tok))
        sym = (book.get((cid, tok)) or {}).get("symbol")
        if not sym and f:
            sym = next(m["symbol"] for m in f["members"]
                       if m["chain_id"] == cid and m["token"].lower() == tok)
        sym_of[(cid, tok)] = sym or ""
        if (cid, tok) in TOKEN_GROUPS:
            uf.union(node, ("group", TOKEN_GROUPS[(cid, tok)]))
        else:
            und = group_symbol(sym)
            if und:
                uf.union(node, ("sym", und))
        if f:
            uf.union(node, ("fam", f["family_id"]))
    groups = defaultdict(list)
    for cid, tok in tokens:
        groups[uf.find(("tok", cid, tok))].append((cid, tok))

    ev_by_fam = defaultdict(list)
    for e in events:
        for fid in (e.get("families") or [e["collateral_family"]["family_id"]]):
            ev_by_fam[fid].append(e)

    assets = []
    for members in groups.values():
        daily = np.zeros(exposure.N)
        rows = []
        fam_ids = sorted({fam_of[m]["family_id"] for m in members if m in fam_of})
        for cid, tok in members:
            e = expo.get((cid, tok))
            d = e["daily"] if e else np.zeros(exposure.N)
            daily += d
            meta = book.get((cid, tok)) or {}
            rows.append({
                "chain_id": cid, "token": tok, "symbol": sym_of[(cid, tok)],
                "name": meta.get("name"),
                "e2_family": (fam_of.get((cid, tok)) or {}).get("family_id"),
                "peak_exposure_usd": round(float(d.max()), 2),
                "peak_day": exposure.day_iso(d.argmax()) if d.max() > 0 else None,
                "protocols": sorted(e["protocols"]) if e else [],
                "n_vaults": len(e["vaults"]) if e else 0,
            })
        rows.sort(key=lambda r: -r["peak_exposure_usd"])
        evs = {}
        for fid in fam_ids:
            for e in ev_by_fam[fid]:
                evs[e["event_id"]] = e
        lead = rows[0]
        label = (next((TOKEN_GROUPS[(m["chain_id"], m["token"])] for m in rows
                       if (m["chain_id"], m["token"]) in TOKEN_GROUPS), None)
                 or underlying_symbol(lead["symbol"]) or lead["symbol"] or lead["token"][:10])
        e2_labels = sorted({f["label"] for f in fams if f["family_id"] in fam_ids})
        assets.append({
            "label": label,
            "e2_families": fam_ids,
            "e2_family_labels": e2_labels,
            "e2_events": [{"event_id": k, "type": v["type"], "onset_utc": v["onset_utc"],
                           "onset_ts": v["onset_ts"], "excluded": v.get("excluded")}
                          for k, v in sorted(evs.items())],
            "peak_exposure_usd": round(float(daily.max()), 2),
            "peak_day": exposure.day_iso(daily.argmax()) if daily.max() > 0 else None,
            "members": rows,
        })
    assets.sort(key=lambda a: (-a["peak_exposure_usd"], a["label"]))
    used = set()
    for i, a in enumerate(assets, 1):
        s = slug(a["label"])
        if s in used:
            s = f"{s}-{slug(a['members'][0]['token'][:8])}"
        used.add(s)
        a["asset"] = s
        a["rank"] = i
        a["tier"] = ("A" if a["e2_events"] else
                     "B" if a["peak_exposure_usd"] >= MIN_USD else "C")
    return assets, skipped
