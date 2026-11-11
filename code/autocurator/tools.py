"""The two corpus tools of an AutoCurator call (E7, E8)."""
import bisect
import json
import sys
from collections import defaultdict

import context as C

sys.path.insert(0, str(C.ROOT / "code" / "corpus"))
import corpus as K
import leakage as L

LIST_MAX = 30
DOC_TOKENS = 3000
DOC_CHARS = 4 * DOC_TOKENS
TITLE_CHARS = 160


class Corpus:
    """The documents about each corpus asset, loaded once, each with the moment from which the cutoff filter exposes it."""

    def __init__(self):
        self.by_id = {}
        self.vis = {}
        self.skipped = 0
        rows = defaultdict(list)
        drop = K.excluded()
        for p in K.doc_files():
            try:
                d = json.loads(p.read_text())
                K.validate_doc(d)
            except (ValueError, OSError, json.JSONDecodeError, KeyError):
                self.skipped += 1
                continue
            if d["id"] in self.by_id or d["id"] in drop:
                continue
            m = L.margin(d)
            if m is None:
                continue
            self.by_id[d["id"]] = d
            self.vis[d["id"]] = K.visible_from(d) + m
            for a in d["assets"]:
                rows[a].append((self.vis[d["id"]], d["id"]))
        self.by_asset = {a: sorted(x) for a, x in rows.items()}
        self._t = {a: [x[0] for x in xs] for a, xs in self.by_asset.items()}
        self.n_docs = len(self.by_id)

    def visible(self, key, t):
        """Documents about `key` visible at t, oldest first."""
        xs = self.by_asset.get(key, [])
        i = bisect.bisect_left(self._t.get(key, []), t)
        return [self.by_id[x[1]] for x in xs[:i]]

    def is_visible(self, doc_id, t):
        v = self.vis.get(doc_id)
        return v is not None and v < t


def doc_date(d):
    return C.ts_iso(K.visible_from(d))[:10]


def list_documents(corpus, key, t, keys):
    """Text of a list_documents result (module docstring)."""
    if key not in keys:
        return f"list_documents({key}): not a collateral key of the table; keys: {', '.join(sorted(keys))}"
    docs = corpus.visible(key, t)
    if not docs:
        return f"list_documents({key}): no documents"
    lines = [f"list_documents({key}): {len(docs)} documents, the newest {min(len(docs), LIST_MAX)} "
             f"(id | date | type | title):"]
    for d in reversed(docs[-LIST_MAX:]):
        lines.append(f"{d['id']} | {doc_date(d)} | {d['source_type']} | {(d.get('title') or '').strip()[:TITLE_CHARS]}")
    return "\n".join(lines)


def read_document(corpus, doc_id, t, keys):
    """Text of a read_document result (module docstring)."""
    d = corpus.by_id.get(doc_id)
    if d is None or not corpus.is_visible(doc_id, t) or not set(d["assets"]) & set(keys):
        return f"read_document({doc_id}): no such document about a collateral of the table"
    txt = d["text"]
    cut = len(txt) > DOC_CHARS
    return (f"read_document({doc_id}): {(d.get('title') or '').strip()} | {doc_date(d)} | {d['source_type']}\n"
            + txt[:DOC_CHARS] + ("\n[cut at about 3,000 tokens]" if cut else ""))
