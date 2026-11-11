"""Document store and asset matching for the corpus crawl."""
import hashlib
import json
import os
import re
import threading
from collections import Counter

import corpus

COMMON_WORDS = {
    "stream", "level", "cap", "prime", "origin", "spark", "sky", "avant", "strata",
    "falcon", "usual", "resolv", "elixir", "lido", "frax", "ethena", "midas", "kelp",
    "renzo", "lombard", "solv", "ondo", "maple", "coinbase", "binance", "lista",
    "pendle", "treehouse", "mantle", "base", "plasma", "sonic", "katana", "hyperliquid",
    "yield", "reserve", "reservoir", "noble", "openeden", "infinifi", "gauntlet",
    "steakhouse", "morpho", "euler", "silo", "aave", "compound", "curve", "convex",
    "yearn", "origin", "anzen", "superform", "upshift", "hyperithm", "felix", "kinetiq",
    "thunderhead", "liquid", "fluid", "royco", "terminal", "apyx", "metronome", "main",
    "street", "stables", "labs", "finance", "protocol", "money", "dao", "network",
    "capital", "money", "bond", "vault", "index", "token", "dollar", "bitcoin", "ether",
}
SAFE_SINGLE = {"ethena", "resolv", "elixir", "lido", "frax", "midas", "renzo", "lombard",
               "ondo", "pendle", "treehouse", "openeden", "infinifi", "kinetiq", "anzen",
               "metronome", "usual", "solv", "kelp", "royco", "apyx", "superform", "upshift"}


def sha256(b):
    return hashlib.sha256(b).hexdigest()


class Store:
    def __init__(self, keep_existing=False):
        self.lock = threading.Lock()
        self.by_sha = {}
        self.by_id = {}
        for p in corpus.doc_files():
            d = json.loads(p.read_text())
            self.by_sha[d["sha256"]] = p
            self.by_id[d["id"]] = p
        self.excluded = corpus.excluded()
        self.keep_existing = keep_existing
        self.preexisting = set(self.by_id) if keep_existing else set()
        self.held = Counter()

    def has_id(self, doc_id):
        return doc_id in self.by_id

    def doc_for_sha(self, sha):
        p = self.by_sha.get(sha)
        return json.loads(p.read_text()) if p else None

    @staticmethod
    def _write(path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        if isinstance(data, bytes):
            tmp.write_bytes(data)
        else:
            tmp.write_text(data)
        os.replace(tmp, path)

    def _remove(self, path):
        d = json.loads(path.read_text())
        raw = corpus.DATA / d["raw_path"]
        for p in (path, raw):
            if p.exists():
                p.unlink()
        self.by_id.pop(d["id"], None)
        return d

    def put(self, fields, raw, ext):
        """Write a document (fields as for corpus.make_doc, without the raw hash and path) and its raw payload."""
        rsha = sha256(raw)
        with self.lock:
            old_path = self.by_sha.get(rsha)
            asset0 = sorted(set(fields["assets"]))[0]
            probe_id = corpus.doc_id(fields["source"], fields["url"], fields["published_ts"])
            raw_rel = f"documents/{asset0}/{probe_id}.raw.{ext}"
            doc = corpus.make_doc(**fields, raw_sha256=rsha, raw_path=raw_rel)
            if doc["id"] in self.excluded:
                return None, "held: excluded from the corpus"
            if self.keep_existing:
                old_id = old_path.stem if old_path is not None else None
                if old_id in self.preexisting or doc["id"] in self.preexisting:
                    for a in fields["assets"]:
                        self.held[a] += 1
                    return None, f"held: already in the corpus as {old_id or doc['id']}"
            if old_path is not None:
                old = json.loads(old_path.read_text())
                if corpus.visible_from(old) <= corpus.visible_from(doc):
                    merged = sorted(set(old["assets"]) | set(doc["assets"]))
                    also = sorted(set(old.get("also_at") or []) | {doc["url"]} - {old["url"]})
                    if merged == old["assets"] and also == (old.get("also_at") or []):
                        return old, "exists"
                    old["assets"] = merged[:]
                    if also:
                        old["also_at"] = also
                    self._write(old_path, json.dumps(old, ensure_ascii=False, indent=1))
                    return old, "merged"
                prev = self._remove(old_path)
                doc["assets"] = sorted(set(prev["assets"]) | set(doc["assets"]))
                doc["also_at"] = sorted(set(prev.get("also_at") or []) | {prev["url"]} - {doc["url"]})
                status = "replaced"
            else:
                status = "new"
            path = corpus.DATA / "documents" / asset0 / f"{doc['id']}.json"
            self._write(corpus.DATA / raw_rel, raw)
            self._write(path, json.dumps(doc, ensure_ascii=False, indent=1))
            self.by_sha[rsha] = path
            self.by_id[doc["id"]] = path
            return doc, status


NOT_ASSET_NAMES = {"mev capital", "re7", "re7 labs", "gauntlet", "steakhouse",
                   "steakhouse financial", "block analitica", "b.protocol", "hyperithm",
                   "telosc", "sentora", "clearstar", "k3 capital", "9summits", "yearn", "felix",
                   "kpk", "karpatkey", "llamarisk", "chaos labs", "morpho", "euler", "silo",
                   "aave", "pendle", "lista", "lista dao", "moolah", "curve", "uniswap",
                   "coinbase", "binance", "spark", "sky", "maker", "makerdao"}


SYMBOL_STOP = {"PST", "IMF", "SPX", "REKT", "CULT", "BITCOIN", "ANON", "MOG", "JOE"}


class Matcher:
    """Which corpus assets a text mentions."""

    def __init__(self, assets):
        self.rx = {}
        self.addr = {}
        self.generic = set()
        per_issuer = Counter(a.get("issuer") for a in assets if a.get("issuer"))
        for a in assets:
            terms = set()
            for m in a["members"]:
                s = (m.get("symbol") or "").strip()
                if len(s) >= 3 and not s.startswith("0x") and s.upper() not in SYMBOL_STOP:
                    terms.add(re.escape(s))
            self.addr[a["asset"]] = {m["token"].lower() for m in a["members"]}
            if per_issuer.get(a.get("issuer"), 0) == 1:
                for n in a.get("news_names") or []:
                    n = n.strip()
                    if not n or n.lower() in NOT_ASSET_NAMES:
                        continue
                    if " " in n or n.lower() in SAFE_SINGLE or (
                            n.lower() not in COMMON_WORDS and len(n) >= 4 and not n.isalpha()):
                        terms.add(re.escape(n))
                    elif n.lower() not in COMMON_WORDS and len(n) >= 5 and any(c.isupper() for c in n[1:]):
                        terms.add(re.escape(n))
            if terms:
                self.rx[a["asset"]] = re.compile(
                    r"(?<![A-Za-z0-9])(?:" + "|".join(sorted(terms, key=len, reverse=True))
                    + r")(?![A-Za-z0-9])")
            if a.get("generic"):
                self.generic.add(a["asset"])
        self.addr_rx = re.compile(r"0x[0-9a-fA-F]{40}")

    def find(self, title, body, allow=None):
        """Assets named in the title, or in the body for non-generic assets."""
        found = set()
        addrs_t = {x.lower() for x in self.addr_rx.findall(title or "")}
        addrs_b = {x.lower() for x in self.addr_rx.findall(body or "")}
        for k in set(self.rx) | set(self.addr):
            if allow is not None and k not in allow:
                continue
            rx = self.rx.get(k)
            if (rx and rx.search(title or "")) or (self.addr[k] & addrs_t):
                found.add(k)
            elif k not in self.generic and ((rx and rx.search(body or "")) or (self.addr[k] & addrs_b)):
                found.add(k)
        return found
