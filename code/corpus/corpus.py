"""CuratorBench text corpus."""
import hashlib
import json
import pathlib
from datetime import datetime, timezone

DATA = pathlib.Path(__file__).parents[2] / "data" / "corpus"
INDEX = DATA / "documents.jsonl"
EXCLUDED = DATA / "excluded.jsonl"

EVIDENCE_GRADES = ("archive_capture", "platform_api", "page_metadata",
                   "http_last_modified", "http_date", "body_date", "inferred")
SOURCE_TYPES = ("governance_forum", "docs", "audit", "news",
                "curator_announcement", "issuer_announcement", "web_archive",
                "other")
PRECISIONS = ("second", "day")
REQUIRED = ("id", "source", "source_type", "url", "final_url", "assets",
            "title", "text", "language", "published_ts", "published_at",
            "published_precision", "timestamp_evidence", "timestamp_source",
            "retrieved_ts", "retrieved_at", "retrieval_method", "sha256",
            "text_sha256", "raw_path")


def iso(ts):
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def doc_id(source, url, published_ts):
    h = hashlib.sha256(f"{source}|{url}|{int(published_ts)}".encode())
    return h.hexdigest()[:16]


def visible_from(d):
    """Moment from which the held text is evidenced to be public."""
    return max(d["published_ts"], d.get("modified_ts") or d["published_ts"])


def make_doc(*, source, source_type, url, final_url, assets, title, text,
             published_ts, precision, evidence, evidence_source, retrieved_ts,
             method, raw_sha256, raw_path, language="und", modified_ts=None,
             found_by=None, extra=None):
    d = {
        "id": doc_id(source, url, published_ts),
        "source": source, "source_type": source_type,
        "url": url, "final_url": final_url, "assets": sorted(set(assets)),
        "title": title or "", "text": text, "language": language,
        "published_ts": int(published_ts), "published_at": iso(published_ts),
        "published_precision": precision,
        "timestamp_evidence": evidence, "timestamp_source": evidence_source,
        "retrieved_ts": int(retrieved_ts), "retrieved_at": iso(retrieved_ts),
        "retrieval_method": method,
        "sha256": raw_sha256,
        "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "raw_path": raw_path, "found_by": found_by,
    }
    if modified_ts is not None and int(modified_ts) > int(published_ts):
        d["modified_ts"] = int(modified_ts)
        d["modified_at"] = iso(modified_ts)
    if extra:
        d.update(extra)
    return validate_doc(d)


def validate_doc(d):
    """Raise ValueError on any schema violation."""
    for k in REQUIRED:
        if k not in d:
            raise ValueError(f"doc {d.get('id', '?')}: missing field {k}")
    if d["timestamp_evidence"] not in EVIDENCE_GRADES:
        raise ValueError(f"doc {d['id']}: bad timestamp_evidence")
    if d["source_type"] not in SOURCE_TYPES:
        raise ValueError(f"doc {d['id']}: bad source_type")
    if d["published_precision"] not in PRECISIONS:
        raise ValueError(f"doc {d['id']}: bad published_precision")
    if not d["text"].strip():
        raise ValueError(f"doc {d['id']}: empty text")
    if visible_from(d) > d["retrieved_ts"]:
        raise ValueError(f"doc {d['id']}: published after retrieval "
                         f"({visible_from(d)} > {d['retrieved_ts']})")
    if d["text_sha256"] != hashlib.sha256(d["text"].encode()).hexdigest():
        raise ValueError(f"doc {d['id']}: text hash mismatch (tampered?)")
    return d


def doc_files():
    """Paths of all document JSON files (raw payloads excluded)."""
    root = DATA / "documents"
    if not root.exists():
        return []
    return sorted(p for p in root.glob("*/*.json") if ".raw." not in p.name)


def excluded():
    """Ids of documents withheld from the benchmark (data/corpus/excluded.jsonl)."""
    if not EXCLUDED.exists():
        return set()
    with open(EXCLUDED) as fh:
        return {json.loads(line)["id"] for line in fh if line.strip()}


def load_all(index=INDEX):
    """Every document in the index, with text, read from its JSON file."""
    docs = []
    if not index.exists():
        return docs
    with open(index) as fh:
        for line in fh:
            if line.strip():
                row = json.loads(line)
                docs.append(json.loads((DATA / row["path"]).read_text()))
    return docs
