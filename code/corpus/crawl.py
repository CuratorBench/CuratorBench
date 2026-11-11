# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy>=1.26", "pandas>=2.1", "scipy>=1.11", "matplotlib>=3.8", "requests>=2.31", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "beautifulsoup4>=4.12", "lxml>=5.0", "trafilatura>=1.12", "pypdf>=4.0", "python-dateutil>=2.9", "py3langid>=0.3"]
# ///
"""E7 corpus: a dated, provenance-complete document corpus about every collateral asset with curated exposure."""
import argparse
import ast
import gzip
import hashlib
import json
import os
import pathlib
import random
import re
import subprocess
import sys
import time
import traceback
import urllib.parse
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "code"))
sys.path.insert(0, str(HERE))
import corpus
import extract
import fetch
import leakage
from common import io_utils
from store import Matcher, Store, sha256

GENERATOR = "code/corpus/crawl.py"
CORPUS = corpus.DATA
SEEDS = HERE / "seeds"
WORK = pathlib.Path(os.environ.get("E7_WORK", "/tmp/e7_corpus_work"))
DAY = 86400
NOW = int(time.time())

PROTOCOL_FORUMS = [
    ("https://forum.morpho.org", "full", "morpho"),
    ("https://forum.euler.finance", "full", "euler"),
    ("https://gov.silo.finance", "full", "silo"),
    ("https://governance.aave.com", "title", "aave"),
]
PROTOCOL_SNAPSHOTS = ["listavote.eth", "morpho.eth", "eulerdao.eth", "silofinance.eth"]
FULL_FORUM_MAX_TOPICS = 400
RISK_TITLE = re.compile(r"risk|oracle|withdraw|depeg|peg\b|slash|insuran|incident|secur|audit|"
                        r"reserve|emergency|pause|exploit|redempt|collateral|liquidat|backing|"
                        r"post-?mortem|vulnerab|attack|loss", re.I)
DOC_PAGES = {"A": 60, "B": 20}
CENSUS_UNIVERSE = (pathlib.Path(os.environ["CB_INPUTS"]).resolve() if os.environ.get("CB_INPUTS")
                   else ROOT / "data" / "inputs") / "token_universe.jsonl"
KEY_WORDS = re.compile(r"mechan|mint|redeem|redempt|reserve|backing|collateral|transparen|proof|"
                       r"risk|audit|secur|oracle|peg|stabil|faq|overview|how-it-works|introduc|"
                       r"tokenomic|strateg|custod|counterpart|insuran|withdraw|stak|yield|"
                       r"architect|design|liquidat|govern|fee|depeg|emergency|pause", re.I)
NEWS_SITEMAPS = [
    ("coindesk.com", "https://www.coindesk.com/sitemap-index.xml"),
    ("theblock.co", "https://www.theblock.co/sitemap_tbco_index.xml"),
    ("decrypt.co", "https://decrypt.co/sitemap_index.xml"),
    ("cointelegraph.com", "https://cointelegraph.com/sitemap.xml"),
    ("thedefiant.io", "https://thedefiant.io/sitemap/index.xml"),
    ("protos.com", "https://protos.com/sitemap_index.xml"),
    ("unchainedcrypto.com", "https://unchainedcrypto.com/sitemap_index.xml"),
    ("blockworks.co", "https://blockworks.co/sitemap.xml"),
    ("rekt.news", "https://rekt.news/leaderboard/"),
]
CURATOR_SITES = [
    ("https://www.steakhouse.financial", "curator_announcement"),
    ("https://www.gauntlet.xyz", "curator_announcement"),
    ("https://mevcapital.com", "curator_announcement"),
    ("https://re7.capital", "curator_announcement"),
    ("https://www.hyperithm.com", "curator_announcement"),
    ("https://sentora.com", "curator_announcement"),
    ("https://kpk.io", "curator_announcement"),
    ("https://blockanalitica.com", "curator_announcement"),
    ("https://clearstar.xyz", "curator_announcement"),
    ("https://llamarisk.com", "other"),
    ("https://chaoslabs.xyz", "other"),
]
CURATOR_PAGES_MAX = 1500
ARCHIVED_PAGES_PER_ROOT = 25
NEWS_FROM = datetime(2023, 10, 1, tzinfo=timezone.utc)
NOT_ARTICLE = re.compile(r"/(convert|price|prices|quote|podcasts?|videos?|tag|tags|author|authors|"
                         r"events?|price-index|data|learn/glossary)/")
RISK_SLUG = re.compile(r"depeg|hack|exploit|collaps|risk|pause|redempt|reserve|concern|warn|freez|"
                       r"loss|bad-debt|liquidat|insolv|lawsuit|audit|curator|morpho|euler|silo|"
                       r"aave|vault|withdraw|halt|probe|investigat|drain|attack|unpeg|backing|"
                       r"transparen|bankrupt|default|stablecoin", re.I)
MEDIA_HOSTS = ("coindesk.com", "theblock.co", "dlnews.com", "blockworks.co", "decrypt.co",
               "cointelegraph.com", "thedefiant.io", "protos.com", "unchainedcrypto.com",
               "rekt.news", "bloomberg.com", "reuters.com", "ft.com", "forbes.com",
               "cryptoslate.com", "beincrypto.com", "crypto.news", "ambcrypto.com",
               "coingape.com", "u.today", "bitcoinist.com", "newsbtc.com", "cryptobriefing.com",
               "coinmarketcap.com", "binance.com", "tradingview.com", "theblockbeats.info",
               "wublock123.com", "panewslab.com", "odaily.news", "techflowpost.com",
               "fortune.com", "wsj.com", "cnbc.com", "axios.com", "banklesstimes.com",
               "bankless.com", "messari.io", "kaiko.com", "chainalysis.com", "elliptic.co")

FETCHER = None
STORE = None
KEEP_EXISTING = False


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def jsonl(path):
    with open(path) as fh:
        return [json.loads(x) for x in fh if x.strip()]


def write_json(rel, obj, meta):
    return io_utils.write_json(f"corpus/{rel}", obj, GENERATOR, meta)


def fetcher():
    global FETCHER
    if FETCHER is None:
        WORK.mkdir(parents=True, exist_ok=True)
        FETCHER = fetch.Fetcher(CORPUS / "cache", WORK / "requests.jsonl")
    return FETCHER


def store():
    global STORE
    if STORE is None:
        STORE = Store(keep_existing=KEEP_EXISTING)
    return STORE


def save_stats(stage, seconds, extra=None):
    """Accumulate the fetcher's counters per stage in crawl_stats.json."""
    p = CORPUS / "crawl_stats.json"
    stats = json.loads(p.read_text()) if p.exists() else {"stages": {}}
    summ = fetcher().summary()
    run = {"finished_utc": fetch.iso_now(), "seconds": round(seconds, 1), **summ,
           "failures": fetcher().failures[:500], "n_failures": len(fetcher().failures)}
    if extra:
        run.update(extra)
    if STORE is not None and STORE.keep_existing:
        run["keep_existing"] = True
        run["held"] = dict(STORE.held)
        STORE.held.clear()
    stats["stages"].setdefault(stage, []).append(run)
    write_json("crawl_stats.json", stats, {"what": "request statistics per stage run "
                                           "(requests, cache hits, failures, robots refusals, per host)"})
    fetcher().stats.clear()
    fetcher().by_host.clear()
    fetcher().failures.clear()


def load_assets():
    rows = jsonl(CORPUS / "assets.jsonl")
    return [r for r in rows if r["row"] == "asset"]


def select(assets, args):
    out = assets
    if args.asset:
        out = [a for a in out if a["asset"] in set(args.asset)]
    if args.tier:
        out = [a for a in out if a["tier"] in set(args.tier)]
    return [a for a in out if a["tier"] in ("A", "B")]


BASE_ASSETS = {"USDC", "WETH", "USDT", "EURC", "USD₮0", "USDT0", "USDG", "AUSD", "cbBTC",
               "PYUSD", "WHYPE", "wS", "WBTC", "vbUSDC", "vbUSDT", "WBNB", "ETH", "DAI"}


def llama_registry():
    """DefiLlama protocol rows indexed by lower-case token address."""
    rec = fetcher().get("https://api.llama.fi/protocols")
    if rec["status"] != 200:
        return {}, rec["final_url"]
    by_addr = defaultdict(list)
    for p in json.loads(rec["body"]):
        addr = (p.get("address") or "").lower()
        if ":" in addr:
            addr = addr.split(":", 1)[1]
        if re.fullmatch(r"0x[0-9a-f]{40}", addr):
            by_addr[addr].append(p)
    return by_addr, rec["final_url"]


def census_llama_ids():
    """{(chain, address): DefiLlama stablecoin id} from the token universe, whose rows carry each token's addresses and source ids."""
    out = {}
    p = CENSUS_UNIVERSE
    if not p.exists():
        return out
    for r in jsonl(p):
        sid = (r.get("source_ids") or {}).get("defillama")
        if not sid:
            continue
        for cid, addr in (r.get("addresses") or {}).items():
            if str(cid).isdigit() and addr:
                out[(int(cid), addr.lower())] = str(sid)
    return out


def llama_stablecoins(built):
    """{asset: [DefiLlama stablecoin rows]} matched by the E2 census family id (census:defillama:<id>), by a member token's add."""
    rec = fetcher().get("https://stablecoins.llama.fi/stablecoins?includePrices=false")
    if rec["status"] != 200:
        return {}
    rows = json.loads(rec["body"])["peggedAssets"]
    by_sym = defaultdict(list)
    for r in rows:
        by_sym[(r.get("symbol") or "").upper()].append(r["id"])
    by_addr = census_llama_ids()
    out = defaultdict(list)
    for a in built:
        addrs = {m["token"] for m in a["members"]}
        cands = {}
        for fid in a["e2_families"]:
            if fid.startswith("census:defillama:"):
                cands[fid.split(":")[-1]] = f"E2 census family {fid}"
        for m in a["members"]:
            sid = by_addr.get((m["chain_id"], m["token"]))
            if sid:
                cands.setdefault(sid, f"census price universe row with address {m['chain_id']}:{m['token']}")
        for m in a["members"]:
            for sid in by_sym.get((m["symbol"] or "").upper(), [])[:3]:
                cands.setdefault(str(sid), None)
        for sid, why in cands.items():
            det = fetcher().get(f"https://stablecoins.llama.fi/stablecoin/{sid}")
            if det["status"] != 200:
                continue
            d = json.loads(det["body"])
            addr = (d.get("address") or "").lower()
            if why is None and addr not in addrs:
                continue
            fb = (f"defillama stablecoins api (https://stablecoins.llama.fi/stablecoin/{sid}): "
                  + (why or f"address field = {addr}"))
            al = d.get("auditLinks") or []
            al = al if isinstance(al, list) else re.split(r"[\s,]+", al)
            out[a["asset"]].append({"name": d.get("name"), "url": d.get("url"), "docs": d.get("wiki"),
                                    "audit_links": [x for x in al if isinstance(x, str) and x.startswith("http")],
                                    "twitter": d.get("twitter"), "github": [],
                                    "found_by": fb})
    return out


def stage_assets(args):
    import assets as asset_mod
    t0 = time.time()
    built, skipped = asset_mod.build(log)
    issuers = {}
    seeds_path = SEEDS / "issuers.jsonl"
    if seeds_path.exists():
        for r in jsonl(seeds_path):
            for k in r["assets"]:
                issuers.setdefault(k, r)
    llama, llama_url = llama_registry()
    stables = llama_stablecoins(built)
    rows = []
    for a in built:
        iss = issuers.get(a["asset"]) or {}
        llama_rows = []
        for m in a["members"]:
            for p in llama.get(m["token"], []):
                if p["name"] not in {x["name"] for x in llama_rows}:
                    llama_rows.append({"name": p["name"], "url": p.get("url"),
                                       "github": p.get("github") or [],
                                       "audit_links": p.get("audit_links") or [],
                                       "twitter": p.get("twitter"),
                                       "found_by": f"defillama protocols api ({llama_url}): "
                                                   f"address field = {m['token']}"})
        llama_rows += stables.get(a["asset"], [])
        syms = {m["symbol"] for m in a["members"] if m["symbol"]}
        row = {
            "row": "asset", "asset": a["asset"], "label": a["label"], "rank": a["rank"],
            "tier": a["tier"], "peak_exposure_usd": a["peak_exposure_usd"],
            "peak_day": a["peak_day"], "e2_families": a["e2_families"],
            "e2_family_labels": a["e2_family_labels"], "e2_events": a["e2_events"],
            "generic": bool(syms) and syms <= BASE_ASSETS,
            "members": [{k: m[k] for k in ("chain_id", "token", "symbol")} for m in a["members"]],
            "issuer": iss.get("issuer"), "issuer_name": iss.get("issuer_name"),
            "official_domains": iss.get("official_domains", []),
            "docs": iss.get("docs", []), "key_pages": iss.get("key_pages", []),
            "forums": iss.get("forums", []), "github": iss.get("github", []),
            "audits": iss.get("audits", []), "blog": iss.get("blog", []),
            "news_names": sorted(set(iss.get("news_names", [])) | {a["label"]}),
            "defillama": llama_rows,
            "issuer_notes": iss.get("notes"), "symbol_collisions": iss.get("symbol_collisions"),
        }
        rows.append(row)
    tok_rows = []
    for a in built:
        for m in a["members"]:
            tok_rows.append({"row": "token", "asset": a["asset"], **m})
    io_utils.write_jsonl("corpus/assets.jsonl", rows + tok_rows, GENERATOR, {
        "what": "corpus assets (collateral families with curated exposure) ranked by peak "
                "curated exposure, one row per asset (row=asset) and per member token (row=token)",
        "exposure": "code/corpus/exposure.py: E4 sample positions valued daily, capped at E3 value",
        "families": "code/corpus/assets.py: E2 families joined with tokens of the same underlying symbol",
        "issuer_sources": "code/corpus/seeds/issuers.jsonl (web search, each URL with found_by); "
                          "defillama protocols api matched by token address",
        "tiers": "A: in an E2 event; B: peak exposure >= $1M; C: below (not crawled)",
        "skipped_vaults": skipped,
        "counts": dict(Counter(r["tier"] for r in rows)),
    })
    save_stats("assets", time.time() - t0, {"assets": len(rows), "tokens": len(tok_rows)})
    log(f"assets: {len(rows)} assets, {len(tok_rows)} tokens, tiers {Counter(r['tier'] for r in rows)}")


def ext_for(rec, url=""):
    ct = (rec.get("headers") or {}).get("content-type", "").lower()
    body = rec.get("body") or b""
    if "pdf" in ct or body[:5] == b"%PDF-":
        return "pdf"
    if "json" in ct:
        return "json"
    if "xml" in ct and b"<html" not in body[:2000].lower():
        return "xml"
    if "html" in ct or b"<html" in body[:2000].lower() or b"<!doctype" in body[:200].lower():
        return "html"
    if url.endswith(".md") or "markdown" in ct:
        return "md"
    return "txt"


def http_extra(rec):
    return {"http_status": rec.get("status"), "http_headers": rec.get("headers") or {},
            "redirects": rec.get("redirects") or []}


def date_http_page(rec, html=None, text=None, url=None):
    """Best evidence for a live HTML/PDF fetch, in the order the corpus rules give."""
    fetched = rec["fetched_ts"]
    pub = mod = None
    if html is not None:
        d = extract.html_dates(html, fetched)
        pub, mod = d["published"], d["modified"]
        if pub:
            return pub[0], pub[1], "page_metadata", pub[2], (mod[0] if mod and mod[0] > pub[0] else None)
        if mod:
            return mod[0], mod[1], "page_metadata", mod[2] + " (last modification: the held " \
                "text is evidenced from then)", None
    lm = fetch.header_time(rec, "last-modified")
    if lm and extract.MIN_TS <= lm <= fetched + 60:
        return lm, "second", "http_last_modified", f"Last-Modified: {rec['headers']['last-modified']}", None
    if text:
        b = extract.body_date(text, fetched)
        if b:
            return b[0], b[1], "body_date", b[2], None
    if url:
        u = extract.url_date(url, fetched)
        if u:
            return u[0], u[1], "inferred", u[2], None
    dh = fetch.header_time(rec, "date")
    if dh and extract.MIN_TS <= dh <= fetched + 60:
        return dh, "second", "http_date", f"Date: {rec['headers']['date']} (no earlier evidence)", None
    return fetched, "second", "http_date", "time of our fetch (no Date header)", None


def make_from_http(rec, *, url, assets, source, source_type, found_by, method="http_get",
                   title_hint=None, extra=None):
    """A document from a live HTTP response (HTML or PDF)."""
    body = rec["body"]
    ext = ext_for(rec, url)
    if ext == "pdf":
        try:
            title, text, pub, mod = extract.pdf_text_and_dates(body, rec["fetched_ts"])
        except Exception as e:
            return None, f"pdf_error: {type(e).__name__}"
        lm = fetch.header_time(rec, "last-modified")
        if lm and extract.MIN_TS <= lm <= rec["fetched_ts"] + 60:
            when = (lm, "second", "http_last_modified",
                    f"Last-Modified: {rec['headers']['last-modified']}", None)
        elif pub:
            when = (pub[0], pub[1], "page_metadata", pub[2], mod[0] if mod and mod[0] > pub[0] else None)
        else:
            when = date_http_page(rec, None, text, url)
        pdf_dates = {"pdf_created": corpus.iso(pub[0]) if pub else None,
                     "pdf_modified": corpus.iso(mod[0]) if mod else None}
        extra = {**(extra or {}), **pdf_dates}
    elif ext in ("html", "xml", "txt", "md"):
        html = body.decode("utf-8", "replace")
        if ext == "html":
            text = extract.html_text(html, url)
            title = extract.html_title(html)
            when = date_http_page(rec, html, text, url)
        else:
            text, title = html.strip(), ""
            when = date_http_page(rec, None, text, url)
    else:
        return None, f"unsupported content {ext}"
    if not text or len(text.strip()) < 80:
        return None, "no_text"
    ts, prec, ev, src, modified = when
    fields = dict(source=source, source_type=source_type, url=url, final_url=rec["final_url"],
                  assets=assets, title=(title or title_hint or "")[:500], text=text,
                  published_ts=ts, precision=prec, evidence=ev, evidence_source=src,
                  retrieved_ts=rec["fetched_ts"], method=method, language=extract.language(text),
                  modified_ts=modified, found_by=found_by,
                  extra={**http_extra(rec), **(extra or {})})
    return (fields, body, ext), None


def put(item):
    fields, raw, ext = item
    try:
        return store().put(fields, raw, ext)
    except ValueError as e:
        return None, f"invalid: {e}"


def discourse_sitemap_topics(base):
    """{topic id: slug} from the forum's sitemap (lists topics /latest omits)."""
    out = {}
    urls, _files = sitemap_urls(base)
    for u in urls:
        m = re.match(re.escape(base) + r"/t/([^/]+)/(\d+)", u)
        if m:
            out[int(m.group(2))] = m.group(1)
    return out


def discourse_topics(base):
    """All listed topics of a Discourse forum."""
    topics, page = {}, 0
    while page < 2000:
        rec = fetcher().get(f"{base}/latest.json?no_definitions=true&page={page}")
        if rec["status"] != 200:
            break
        tl = json.loads(rec["body"]).get("topic_list", {})
        rows = tl.get("topics") or []
        new = 0
        for t in rows:
            if t["id"] not in topics:
                topics[t["id"]] = t
                new += 1
        if not rows or not new or not tl.get("more_topics_url"):
            break
        page += 1
    for tid, slug in discourse_sitemap_topics(base).items():
        topics.setdefault(tid, {"id": tid, "slug": slug, "title": None})
    return list(topics.values())


def discourse_thread(base, topic_id):
    """(topic json, [post objects], [(url, record)]) of one topic."""
    rec = fetcher().get(f"{base}/t/{topic_id}.json")
    if rec["status"] != 200:
        return None, [], []
    tj = json.loads(rec["body"])
    posts = {p["id"]: (p, rec) for p in tj.get("post_stream", {}).get("posts", [])}
    stream = tj.get("post_stream", {}).get("stream", [])
    missing = [i for i in stream if i not in posts]
    for i in range(0, len(missing), 20):
        chunk = missing[i:i + 20]
        q = "&".join(f"post_ids[]={x}" for x in chunk)
        r2 = fetcher().get(f"{base}/t/{topic_id}/posts.json?{q}")
        if r2["status"] == 200:
            for p in json.loads(r2["body"]).get("post_stream", {}).get("posts", []):
                posts[p["id"]] = (p, r2)
    ordered = [posts[i] for i in stream if i in posts]
    return tj, ordered, None


def thread_assets(tj, posts, matcher, forced=()):
    """Assets a whole thread is about."""
    texts = [extract.fragment_text(p.get("cooked") or "") for p, _r in posts]
    found = set(forced) | matcher.find(tj.get("title", ""), "")
    if texts:
        found |= matcher.find("", texts[0])
    counts = Counter(a for t in texts for a in matcher.find("", t))
    return found | {a for a, n in counts.items() if n >= 2}


def ingest_thread(base, host, tj, posts, assets, found_by, matcher):
    """One document per post, tagged with the thread's assets plus the assets the post itself names; posts with no asset are skipped."""
    title = tj.get("title") or ""
    slug = tj.get("slug") or "topic"
    n_new, ids = 0, []
    for p, rec in posts:
        if p.get("hidden") or p.get("deleted_at") or not p.get("cooked"):
            continue
        body = extract.fragment_text(p["cooked"])
        if not body:
            continue
        tags = set(assets) | matcher.find("", body)
        if not tags:
            continue
        text = f"{title}\n\nPost #{p['post_number']} by {p.get('username', '?')}\n\n{body}"
        created = extract.parse_when(p["created_at"])
        if not created:
            continue
        modified = None
        if (p.get("version") or 1) > 1 and p.get("updated_at"):
            u = extract.parse_when(p["updated_at"])
            modified = u[0] if u else None
        raw = json.dumps(p, sort_keys=True, ensure_ascii=False).encode()
        fields = dict(
            source=f"discourse:{host}", source_type="governance_forum",
            url=f"{base}/t/{slug}/{tj['id']}/{p['post_number']}", final_url=rec["final_url"],
            assets=sorted(tags), title=f"{title} (post {p['post_number']})", text=text,
            published_ts=created[0], precision="second", evidence="platform_api",
            evidence_source=f"discourse post.created_at={p['created_at']}",
            retrieved_ts=rec["fetched_ts"], method="discourse_json",
            language=extract.language(body), modified_ts=modified, found_by=found_by,
            extra={"forum_topic_id": tj["id"], "post_id": p["id"], "post_number": p["post_number"],
                   "post_version": p.get("version"), "post_updated_at": p.get("updated_at"),
                   "author": p.get("username")})
        doc, status = put((fields, raw, "json"))
        if doc is not None:
            ids.append(doc["id"])
            n_new += status in ("new", "replaced")
    return n_new, ids


def crawl_discourse(base, mode, matcher, issuer_assets=None, label=""):
    host = fetch.host_of(base)
    about = fetcher().get(f"{base}/about.json")
    if about["status"] != 200 or not about["body"].lstrip().startswith(b"{"):
        log(f"forum {base}: not a Discourse forum or unreachable ({about['status']})")
        return {"forum": base, "status": about["status"], "topics": 0, "kept": 0, "posts": 0}
    topics = discourse_topics(base)
    if mode == "auto":
        mode = "full" if len(topics) <= FULL_FORUM_MAX_TOPICS else "title"
    kept = posts_new = 0
    specific = {k for k in matcher.rx if k not in matcher.generic}
    for t in topics:
        title = t.get("title") or ""
        if mode == "title" and not matcher.find(title, "", allow=specific) and not (
                issuer_assets and RISK_TITLE.search(title)):
            continue
        tj, posts, _ = discourse_thread(base, t["id"])
        if tj is None:
            continue
        found = thread_assets(tj, posts, matcher, issuer_assets or ())
        n, ids = ingest_thread(base, host, tj, posts, found,
                               f"discourse topic list {base}/latest.json ({label}, mode {mode})", matcher)
        kept += bool(ids)
        posts_new += n
    log(f"forum {base}: {len(topics)} topics listed, {kept} kept, {posts_new} new post documents")
    return {"forum": base, "mode": mode, "topics": len(topics), "kept": kept, "posts_new": posts_new}


SNAPSHOT_Q = ("query($space: String!, $skip: Int!) { proposals(first: 100, skip: $skip, "
              "where: {space: $space}, orderBy: \"created\", orderDirection: asc) "
              "{ id title body created start end author state link space { id name } } }")


def crawl_snapshot(space, matcher, issuer_assets=None):
    n_new = kept = 0
    skip = 0
    while True:
        payload = json.dumps({"query": SNAPSHOT_Q, "variables": {"space": space, "skip": skip}})
        rec = fetcher().get("https://hub.snapshot.org/graphql", method="POST", data=payload,
                            headers={"content-type": "application/json"})
        if rec["status"] != 200:
            break
        props = (json.loads(rec["body"]).get("data") or {}).get("proposals") or []
        for p in props:
            body = (p.get("body") or "").strip()
            found = matcher.find(p.get("title", ""), body)
            if issuer_assets:
                found |= set(issuer_assets)
            if not found or not body:
                continue
            kept += 1
            raw = json.dumps(p, sort_keys=True, ensure_ascii=False).encode()
            text = f"{p.get('title', '')}\n\n{body}"
            fields = dict(
                source=f"snapshot:{space}", source_type="governance_forum",
                url=p.get("link") or f"https://snapshot.box/#/s:{space}/proposal/{p['id']}",
                final_url="https://hub.snapshot.org/graphql", assets=sorted(found),
                title=p.get("title", ""), text=text, published_ts=int(p["created"]),
                precision="second", evidence="platform_api",
                evidence_source=f"snapshot proposal.created={p['created']}",
                retrieved_ts=rec["fetched_ts"], method="snapshot_graphql",
                language=extract.language(body),
                found_by=f"snapshot graphql proposals of space {space}",
                extra={"snapshot_space": space, "proposal_id": p["id"], "author": p.get("author"),
                       "vote_start": p.get("start"), "vote_end": p.get("end"), "state": p.get("state")})
            doc, status = put((fields, raw, "json"))
            if doc is not None and status in ("new", "replaced"):
                n_new += 1
        if len(props) < 100:
            break
        skip += 100
    log(f"snapshot {space}: {kept} proposals kept, {n_new} new documents")
    return {"space": space, "kept": kept, "new": n_new}


def registrable(host):
    """Last two labels of a host name (gov.usual.money -> usual.money)."""
    h = re.sub(r"^https?://", "", (host or "").lower()).split("/")[0].split(":")[0]
    return ".".join(h.split(".")[-2:])


def snapshot_space(url):
    m = re.search(r"#/(?:s:)?([A-Za-z0-9_.\-]+\.(?:eth|xyz|com|org|io))", url)
    return m.group(1) if m else None


def stage_forums(args):
    t0 = time.time()
    assets = select(load_assets(), args)
    matcher = Matcher(assets)
    report = {"discourse": [], "snapshot": []}
    jobs = []
    if not args.asset:
        jobs += [(base, mode, None, proto) for base, mode, proto in PROTOCOL_FORUMS]
    by_forum = defaultdict(set)
    spaces = defaultdict(set)
    for a in assets:
        own = {registrable(d["domain"]) for d in a.get("official_domains") or []}
        for f in a.get("forums") or []:
            u = f["url"].rstrip("/")
            if f.get("platform") == "snapshot" or "snapshot" in u:
                s = snapshot_space(u)
                if s:
                    spaces[s].add(a["asset"])
                continue
            parts = urllib.parse.urlsplit(u)
            if "/t/" in parts.path or parts.scheme not in ("http", "https") or not parts.netloc:
                continue
            base = f"{parts.scheme}://{parts.netloc}"
            by_forum[base]
            if registrable(parts.netloc) in own:
                by_forum[base].add(a["asset"])
    protocol_bases = {b for b, _m, _p in PROTOCOL_FORUMS}
    for base, owned in sorted(by_forum.items()):
        if base not in protocol_bases:
            jobs.append((base, "auto", sorted(owned) or None, "issuer" if owned else "other"))
    with ThreadPoolExecutor(6) as ex:
        futs = {ex.submit(crawl_discourse, b, m, matcher, own, lab): b for b, m, own, lab in jobs}
        for f in as_completed(futs):
            try:
                report["discourse"].append(f.result())
            except Exception as e:
                log(f"forum {futs[f]} failed: {e}")
                report["discourse"].append({"forum": futs[f], "error": repr(e)[:300]})
    snap = [(s, None) for s in PROTOCOL_SNAPSHOTS] if not args.asset else []
    snap += [(s, sorted(o)) for s, o in sorted(spaces.items()) if s not in PROTOCOL_SNAPSHOTS]
    for s, own in snap:
        try:
            report["snapshot"].append(crawl_snapshot(s, matcher, own))
        except Exception as e:
            report["snapshot"].append({"space": s, "error": repr(e)[:300]})
    save_stats("forums", time.time() - t0, {"report": report})


def sitemap_urls(root, limit_files=40):
    """Page URLs (with lastmod) under a site root, from its sitemap(s)."""
    parts = urllib.parse.urlsplit(root)
    origin = f"{parts.scheme}://{parts.netloc}"
    prefix = root.rstrip("/")
    cands = [prefix + "/sitemap.xml", origin + "/sitemap.xml", prefix + "/sitemap-pages.xml"]
    rb = fetcher().get(origin + "/robots.txt")
    if rb["status"] == 200:
        for line in rb["body"].decode("utf-8", "replace").splitlines():
            if line.lower().startswith("sitemap:"):
                cands.insert(0, line.split(":", 1)[1].strip())
    seen, out, files = set(), {}, 0
    queue = list(dict.fromkeys(cands))
    while queue and files < limit_files:
        sm = queue.pop(0)
        if sm in seen:
            continue
        seen.add(sm)
        rec = fetcher().get(sm)
        files += 1
        if rec["status"] != 200:
            continue
        body = rec["body"]
        if body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)
        try:
            tree = ET.fromstring(body)
        except ET.ParseError:
            continue
        ns = tree.tag.split("}")[0] + "}" if tree.tag.startswith("{") else ""
        if tree.tag.endswith("sitemapindex"):
            for s in tree.findall(f"{ns}sitemap"):
                loc = s.findtext(f"{ns}loc")
                if loc:
                    queue.append(loc.strip())
            continue
        for u in tree.findall(f"{ns}url"):
            loc = (u.findtext(f"{ns}loc") or "").strip()
            if loc.startswith(prefix):
                out[loc] = (u.findtext(f"{ns}lastmod") or "").strip()
    return out, sorted(seen)


def same_site_links(root, rec):
    if rec["status"] != 200 or ext_for(rec) != "html":
        return []
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(rec["body"], "lxml")
    prefix = root.rstrip("/")
    out = []
    for a in soup.find_all("a", href=True):
        u = urllib.parse.urljoin(rec["final_url"], a["href"]).split("#")[0]
        if u.startswith(prefix) and u not in out:
            out.append(u)
    return out


def issuer_groups(assets):
    """{issuer: (assets of that issuer, best tier, first rank)} for crawl units."""
    groups = defaultdict(list)
    for a in assets:
        groups[a.get("issuer") or f"asset:{a['asset']}"].append(a)
    out = []
    for iss, members in groups.items():
        tier = "A" if any(m["tier"] == "A" for m in members) else "B"
        out.append((iss, members, tier, min(m["rank"] for m in members)))
    out.sort(key=lambda x: (x[2], x[3]))
    return out


def pick_assets(members, title, text, matcher):
    """Assets of the issuer the page names; all of them when it names none."""
    keys = {m["asset"] for m in members}
    found = matcher.find(title, text, allow=keys)
    return sorted(found or keys)


def site_pages(roots, cap, prefer):
    """{page URL: found_by} under the given roots."""
    found = {}
    for root in roots:
        sm, files = sitemap_urls(root)
        if sm:
            for u in sm:
                found.setdefault(u, f"sitemap of {root} ({', '.join(files[:3])})")
        else:
            found.setdefault(root, f"root {root}")
            for u in same_site_links(root, fetcher().get(root)):
                found.setdefault(u, f"linked_from: {root}")
    ranked = sorted(found, key=lambda u: (not prefer.search(u), len(u)))
    return {u: found[u] for u in ranked[:cap]}


def crawl_docs_issuer(iss, members, tier, matcher):
    """Docs pages (key pages first, then the docs roots' pages) and blog posts of one issuer."""
    roots, keys, blogs = [], [], []
    for a in members:
        roots += [d["url"] for d in a.get("docs") or []]
        keys += [(k["url"], k.get("found_by")) for k in a.get("key_pages") or []]
        blogs += [b["url"] for b in a.get("blog") or []]
        for row in a.get("defillama") or []:
            roots += [u for u in (row.get("docs"), row.get("url")) if u]
    roots = list(dict.fromkeys(r for r in roots if r.startswith("http")))
    blogs = list(dict.fromkeys(b for b in blogs if b.startswith("http")))
    jobs = {(u[:-3] if u.endswith(".md") else u): (fb, "docs") for u, fb in keys}
    for u, fb in site_pages(roots, DOC_PAGES[tier], KEY_WORDS).items():
        jobs.setdefault(u, (fb, "docs"))
    for u, fb in site_pages(blogs, DOC_PAGES[tier] // 2, RISK_SLUG).items():
        jobs.setdefault(u, (fb, "issuer_announcement"))
    n_new = n_fail = 0
    for u, (fb, stype) in jobs.items():
        rec = fetcher().get(u)
        if rec["status"] != 200:
            n_fail += 1
            continue
        item, err = make_from_http(rec, url=u, assets=[members[0]["asset"]],
                                   source=f"docs:{fetch.host_of(u)}", source_type=stype,
                                   found_by=fb or "issuer docs")
        if item is None:
            n_fail += 1
            continue
        item[0]["assets"] = pick_assets(members, item[0]["title"], item[0]["text"], matcher)
        doc, status = put(item)
        if doc is not None and status in ("new", "replaced"):
            n_new += 1
    return {"issuer": iss, "roots": roots, "blogs": blogs, "pages": len(jobs), "new": n_new,
            "failed": n_fail}


def stage_docs(args):
    t0 = time.time()
    assets = select(load_assets(), args)
    matcher = Matcher(assets)
    groups = issuer_groups(assets)
    report = []
    with ThreadPoolExecutor(8) as ex:
        futs = {ex.submit(crawl_docs_issuer, iss, mem, tier, matcher): iss
                for iss, mem, tier, _r in groups}
        for f in as_completed(futs):
            try:
                r = f.result()
            except Exception as e:
                r = {"issuer": futs[f], "error": repr(e)[:300]}
                traceback.print_exc()
            report.append(r)
            log(f"docs {r}")
    save_stats("docs", time.time() - t0, {"report": report})


def git(*a, cwd=None, binary=False):
    r = subprocess.run(["git", *a], cwd=cwd, capture_output=True, timeout=900,
                       env={**os.environ, "GIT_TERMINAL_PROMPT": "0"})
    if r.returncode != 0:
        raise RuntimeError(r.stderr.decode("utf-8", "replace")[:400])
    return r.stdout if binary else r.stdout.decode("utf-8", "replace")


def github_repos(entry):
    """[(org, repo)] for a GitHub URL."""
    parts = [p for p in urllib.parse.urlsplit(entry).path.split("/") if p]
    if len(parts) >= 2:
        return [(parts[0], parts[1].removesuffix(".git"))]
    if not parts:
        return []
    org = parts[0]
    for kind in ("orgs", "users"):
        rec = fetcher().get(f"https://api.github.com/{kind}/{org}/repos?per_page=100&type=public")
        if rec["status"] == 200:
            rows = json.loads(rec["body"])
            pick = [r for r in rows if re.search(r"audit|secur|docs|documentation|whitepaper",
                                                 (r["name"] + " " + (r.get("description") or "")), re.I)]
            return [(org, r["name"]) for r in pick[:6]]
    return []


def clone(org, repo):
    d = WORK / "git" / f"{org}__{repo}"
    url = f"https://github.com/{org}/{repo}"
    if (d / ".git").exists() or (d / "HEAD").exists():
        return d
    d.parent.mkdir(parents=True, exist_ok=True)
    git("clone", "--quiet", "--filter=blob:none", "--no-checkout", "--bare", url, str(d))
    return d


def crawl_repo(org, repo, members, matcher, found_by):
    d = clone(org, repo)
    head = git("rev-parse", "HEAD", cwd=d).strip()
    out = {"repo": f"{org}/{repo}", "pdf_new": 0, "readme_new": 0}
    log_txt = git("log", "--all", "--diff-filter=A", "--format=@@%H %ct", "--name-only", "--",
                  "*.pdf", "*.PDF", cwd=d)
    added, cur = {}, None
    for line in log_txt.splitlines():
        if line.startswith("@@"):
            sha, ct = line[2:].split()
            cur = (sha, int(ct))
        elif line.strip() and cur:
            p = line.strip()
            if p not in added or cur[1] < added[p][1]:
                added[p] = cur
    for path, (sha, ct) in sorted(added.items(), key=lambda x: x[1][1])[:120]:
        try:
            size = int(git("cat-file", "-s", f"{sha}:{path}", cwd=d).strip())
            if size > 30 * 1024 * 1024:
                continue
            data = git("show", f"{sha}:{path}", cwd=d, binary=True)
            title, text, pub, _mod = extract.pdf_text_and_dates(data, NOW)
        except Exception:
            continue
        if len(text) < 200:
            continue
        url = f"https://github.com/{org}/{repo}/blob/{sha}/{urllib.parse.quote(path)}"
        fields = dict(source=f"git:{org}/{repo}", source_type="audit", url=url, final_url=url,
                      assets=pick_assets(members, title or path, text, matcher),
                      title=title or pathlib.PurePath(path).name, text=text, published_ts=ct,
                      precision="second", evidence="platform_api",
                      evidence_source=f"git commit {sha} committer date (commit that added {path})",
                      retrieved_ts=NOW, method="git", language=extract.language(text),
                      found_by=found_by,
                      extra={"git_commit": sha, "git_path": path, "git_head": head,
                             "pdf_created": corpus.iso(pub[0]) if pub else None})
        doc, status = put((fields, data, "pdf"))
        if doc is not None and status in ("new", "replaced"):
            out["pdf_new"] += 1
    readme_log = git("log", "--all", "--format=%H %ct", "--", "README.md", cwd=d)
    seen_blobs = set()
    for line in readme_log.splitlines()[:200]:
        sha, ct = line.split()
        try:
            blob = git("rev-parse", f"{sha}:README.md", cwd=d).strip()
        except RuntimeError:
            continue
        if blob in seen_blobs:
            continue
        seen_blobs.add(blob)
        if len(seen_blobs) > 30:
            break
        data = git("cat-file", "blob", blob, cwd=d, binary=True)
        text = data.decode("utf-8", "replace").strip()
        if len(text) < 200:
            continue
        url = f"https://github.com/{org}/{repo}/blob/{sha}/README.md"
        fields = dict(source=f"git:{org}/{repo}", source_type="docs", url=url, final_url=url,
                      assets=pick_assets(members, repo, text, matcher),
                      title=f"{org}/{repo} README.md at {corpus.iso(int(ct))[:10]}", text=text,
                      published_ts=int(ct), precision="second", evidence="platform_api",
                      evidence_source=f"git commit {sha} committer date (README.md blob {blob})",
                      retrieved_ts=NOW, method="git", language=extract.language(text),
                      found_by=found_by, extra={"git_commit": sha, "git_path": "README.md"})
        doc, status = put((fields, data, "md"))
        if doc is not None and status in ("new", "replaced"):
            out["readme_new"] += 1
    return out


def stage_github(args):
    t0 = time.time()
    assets = select(load_assets(), args)
    matcher = Matcher(assets)
    report = []
    for iss, members, _tier, _rank in issuer_groups(assets):
        entries = []
        for a in members:
            entries += [(g["url"], g.get("found_by")) for g in a.get("github") or []]
            for row in a.get("defillama") or []:
                entries += [(f"https://github.com/{g}", row["found_by"]) for g in row.get("github") or []]
        repos = {}
        for url, fb in dict.fromkeys(entries):
            if "github.com" not in url:
                continue
            try:
                for org, repo in github_repos(url):
                    repos.setdefault((org, repo), f"{fb}; repo listed from {url}")
            except Exception as e:
                report.append({"issuer": iss, "entry": url, "error": repr(e)[:200]})
        for (org, repo), fb in list(repos.items())[:8]:
            try:
                r = crawl_repo(org, repo, members, matcher, fb)
            except Exception as e:
                r = {"repo": f"{org}/{repo}", "error": repr(e)[:300]}
            r["issuer"] = iss
            report.append(r)
            log(f"github {r}")
    save_stats("github", time.time() - t0, {"report": report})


def slug_terms(a):
    """Slug phrases that name an asset in a news URL."""
    from store import SAFE_SINGLE
    terms = set()
    for n in [m["symbol"] for m in a["members"]]:
        s = re.sub(r"[^a-z0-9]+", "-", (n or "").lower()).strip("-")
        if s and not s.startswith(("pt-", "0x")) and len(s) >= 4 and s not in SLUG_STOP:
            terms.add(s)
    for n in a.get("news_names") or []:
        s = re.sub(r"[^a-z0-9]+", "-", (n or "").lower()).strip("-")
        if not s or s in SLUG_STOP or s.startswith(("pt-", "0x")):
            continue
        if ("-" in s and not set(s.split("-")) <= SLUG_STOP) or s in SAFE_SINGLE - SLUG_STOP:
            terms.add(s)
    return terms


SLUG_STOP = {
    "usdc", "usdt", "weth", "wbtc", "btcb", "dai", "eth", "usds", "susd", "usd1", "aave", "morpho",
    "euler", "silo", "pendle", "curve", "uniswap", "lido", "frax", "vault", "yield", "dollar",
    "well", "said", "cult", "rekt", "stone", "river", "joe", "meth", "gold", "edge", "farm", "prime",
    "level", "apex", "global", "basis", "hyper", "bond", "win", "pump", "sonic", "base", "plasma",
    "spark", "stream", "origin", "anchor", "noble", "falcon", "avant", "strata", "usual", "money",
    "cap", "finance", "labs", "protocol", "network", "capital", "dao", "token", "coin", "stable",
    "circle", "sky", "maker", "makerdao", "kraken", "coinbase", "binance", "tether", "paypal",
    "ripple", "bitgo", "wintermute", "eigen", "eigenlayer", "bitcoin", "ether", "solana", "hype",
    "toshi", "mamo", "pepe", "uni", "comp", "aero", "sol", "link", "katana", "mantle",
    "hyperliquid", "midas", "anon"}


def news_sitemap_urls(site, index_url):
    """{article URL: lastmod} from a news site's sitemaps since NEWS_FROM."""
    out = {}
    seen = set()
    queue = [index_url]
    files = 0
    while queue and files < 800:
        sm = queue.pop(0)
        if sm in seen:
            continue
        seen.add(sm)
        rec = fetcher().get(sm)
        files += 1
        if rec["status"] != 200:
            continue
        body = rec["body"]
        if body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)
        try:
            tree = ET.fromstring(body)
        except ET.ParseError:
            if ext_for(rec) == "html":
                for u in same_site_links(f"https://{site}/", rec):
                    out.setdefault(u, "listing")
            continue
        ns = tree.tag.split("}")[0] + "}" if tree.tag.startswith("{") else ""
        if tree.tag.endswith("sitemapindex"):
            for s in tree.findall(f"{ns}sitemap"):
                loc = (s.findtext(f"{ns}loc") or "").strip()
                lm = (s.findtext(f"{ns}lastmod") or "").strip()
                w = extract.parse_when(lm) if lm else None
                old_year = re.search(r"(?<!\d)(20(?:0\d|1\d|2[0-2]))(?!\d)", loc)
                if loc and not old_year and (w is None or w[0] >= NEWS_FROM.timestamp() - 40 * DAY):
                    if not re.search(r"(tag|author|category|video|page)s?[-_]?sitemap|/(tag|author|category)",
                                     loc, re.I):
                        queue.append(loc)
            continue
        for u in tree.findall(f"{ns}url"):
            loc = (u.findtext(f"{ns}loc") or "").strip()
            lm = (u.findtext(f"{ns}lastmod") or "").strip()
            if loc:
                out[loc] = lm
    return out, files


def stage_newsmap(args):
    """Candidate news URLs whose slug names an asset; written to data/corpus/newsmap.jsonl with the sitemap and the matched phrase."""
    t0 = time.time()
    assets = select(load_assets(), args)
    terms = {a["asset"]: slug_terms(a) for a in assets}
    tier = {a["asset"]: a["tier"] for a in assets}
    rows = []
    report = []
    for site, idx in NEWS_SITEMAPS:
        try:
            urls, files = news_sitemap_urls(site, idx)
        except Exception as e:
            report.append({"site": site, "error": repr(e)[:300]})
            continue
        hits = 0
        for u, lm in urls.items():
            when = extract.url_date(u, NOW) or (extract.parse_when(lm) if lm and lm != "listing" else None)
            if lm != "listing" and (when is None or when[0] < NEWS_FROM.timestamp()):
                continue
            slug = urllib.parse.urlsplit(u).path.lower()
            if NOT_ARTICLE.search(slug):
                continue
            toks = "-" + re.sub(r"[^a-z0-9]+", "-", slug) + "-"
            for k, ts in terms.items():
                hit = next((t for t in sorted(ts, key=len, reverse=True) if f"-{t}-" in toks), None)
                if hit:
                    rows.append({"url": u, "asset": k, "site": site, "phrase": hit,
                                 "lastmod": lm, "risk_slug": bool(RISK_SLUG.search(slug)),
                                 "found_by": f"sitemap {idx} (slug contains '{hit}')"})
                    hits += 1
        report.append({"site": site, "sitemap_files": files, "urls": len(urls), "hits": hits})
        log(f"newsmap {site}: {files} sitemap files, {len(urls)} urls, {hits} slug hits")
    keep = []
    by_asset = defaultdict(list)
    for r in rows:
        by_asset[r["asset"]].append(r)
    for k, lst in by_asset.items():
        cap = 60 if tier[k] == "A" else 15
        lst.sort(key=lambda r: (not r["risk_slug"], r["lastmod"]), reverse=False)
        risky = [r for r in lst if r["risk_slug"]]
        rest = sorted([r for r in lst if not r["risk_slug"]], key=lambda r: r["lastmod"], reverse=True)
        keep += (risky + rest)[:cap]
    nm = CORPUS / "newsmap.jsonl"
    if args.asset and nm.exists():
        keep = [r for r in jsonl(nm) if r["asset"] not in terms] + keep
    io_utils.write_jsonl("corpus/newsmap.jsonl", keep, GENERATOR, {
        "what": "news article URLs discovered from news-site sitemaps whose URL slug names a corpus asset",
        "sites": report, "cap": "tier A 60, tier B 15 per asset; risk-worded slugs first",
        "candidates_before_cap": len(rows)})
    save_stats("newsmap", time.time() - t0, {"report": report, "kept": len(keep)})


def crawl_site_pages(root, stype, matcher, specific):
    urls, files = sitemap_urls(root, limit_files=60)
    urls = sorted(urls, key=lambda u: (not KEY_WORDS.search(u) and not RISK_SLUG.search(u), u))
    kept = new = 0
    for u in urls[:CURATOR_PAGES_MAX]:
        if re.search(r"/(tag|tags|author|category|careers?|jobs|legal|privacy|terms)(/|$)", u):
            continue
        rec = fetcher().get(u)
        if rec["status"] != 200:
            continue
        item, err = make_from_http(rec, url=u, assets=["_"], source=f"site:{fetch.host_of(root)}",
                                   source_type=stype, found_by=f"sitemap of {root} ({', '.join(files[:2])})")
        if item is None:
            continue
        found = matcher.find(item[0]["title"], item[0]["text"], allow=specific)
        if not found:
            continue
        item[0]["assets"] = sorted(found)
        kept += 1
        doc, status = put(item)
        new += bool(doc) and status in ("new", "replaced")
    return {"site": root, "sitemap_files": len(files), "urls": len(urls), "kept": kept, "new": new}


def stage_curators(args):
    """Curator and risk-research sites."""
    t0 = time.time()
    assets = select(load_assets(), args)
    matcher = Matcher(assets)
    specific = {k for k in matcher.rx if k not in matcher.generic}
    report = []
    with ThreadPoolExecutor(6) as ex:
        futs = {ex.submit(crawl_site_pages, r, st, matcher, specific): r for r, st in CURATOR_SITES}
        for f in as_completed(futs):
            try:
                r = f.result()
            except Exception as e:
                r = {"site": futs[f], "error": repr(e)[:300]}
            report.append(r)
            log(f"curators {r}")
    save_stats("curators", time.time() - t0, {"report": report})


def wayback_cdx(url, limit=None, extra=""):
    """Capture list of a URL (status 200, collapsed by digest), oldest first."""
    q = urllib.parse.urlencode({"url": url,
                                "fl": "timestamp,original,statuscode,mimetype,digest,length",
                                "filter": "statuscode:200", "collapse": "digest"})
    if limit:
        q += f"&limit={limit}"
    rec = fetcher().get(f"https://web.archive.org/web/timemap/json?{q}{extra}")
    if rec["status"] != 200 or not rec["body"].strip():
        return [], rec
    try:
        rows = json.loads(rec["body"])
    except ValueError:
        return [], rec
    if not rows:
        return [], rec
    head = rows[0]
    return [dict(zip(head, r)) for r in rows[1:]], rec


def capture_ts(stamp):
    return int(datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp())


def doc_from_capture(cap, *, url, assets, source_type, found_by, prefer_page_date):
    """A document from one Wayback capture (raw bytes via the id_ mode)."""
    wb = f"https://web.archive.org/web/{cap['timestamp']}id_/{cap['original']}"
    rec = fetcher().get(wb)
    if rec["status"] != 200:
        return None, f"wayback {rec['status']}"
    cts = capture_ts(cap["timestamp"])
    body = rec["body"]
    ext = ext_for(rec, cap["original"])
    page = {}
    if ext == "pdf":
        try:
            title, text, pub, _m = extract.pdf_text_and_dates(body, cts)
        except Exception as e:
            return None, f"pdf_error {type(e).__name__}"
    elif ext in ("html", "xml", "txt", "md", "json"):
        html = body.decode("utf-8", "replace")
        if ext == "html":
            text, title = extract.html_text(html, cap["original"]), extract.html_title(html)
            d = extract.html_dates(html, cts)
            pub = d["published"]
            page = {"page_published": corpus.iso(pub[0]) if pub else None,
                    "page_modified": corpus.iso(d["modified"][0]) if d["modified"] else None}
        else:
            text, title, pub = html.strip(), "", None
    else:
        return None, f"unsupported {ext}"
    if not text or len(text.strip()) < 80:
        return None, "no_text"
    ts, prec, ev, src = cts, "second", "archive_capture", f"wayback capture {cap['timestamp']}"
    if prefer_page_date and pub and pub[0] <= cts:
        ts, prec, ev, src = pub[0], pub[1], "page_metadata", f"{pub[2]} (in wayback capture {cap['timestamp']})"
    fields = dict(source="wayback", source_type=source_type, url=url, final_url=wb, assets=assets,
                  title=title[:500], text=text, published_ts=ts, precision=prec, evidence=ev,
                  evidence_source=src, retrieved_ts=rec["fetched_ts"], method="wayback_id",
                  language=extract.language(text), found_by=found_by,
                  extra={"capture_timestamp": cap["timestamp"], "capture_ts": cts,
                         "capture_digest": cap.get("digest"), **page, **http_extra(rec)})
    return (fields, body, ext), None


def x_status_time(url):
    m = re.search(r"(?:x|twitter)\.com/[^/]+/status(?:es)?/(\d+)", url)
    if not m:
        return None
    return int(((int(m.group(1)) >> 22) + 1288834974657) / 1000)


def seed_type(kind, url, members):
    host = fetch.host_of(url)
    domains = {d["domain"].lower().removeprefix("www.") for a in members
               for d in a.get("official_domains") or []}
    if kind == "audit":
        return "audit"
    if kind == "curator_announcement":
        return "curator_announcement"
    if any(host == d or host.endswith("." + d) for d in domains) or kind == "issuer_announcement":
        return "issuer_announcement"
    if kind in ("news", "post_mortem", "analysis", "red_flag") and any(
            host == h or host.endswith("." + h) for h in MEDIA_HOSTS):
        return "news"
    if kind == "news":
        return "news"
    return "other"


def seed_rows(assets):
    """Every seed document: research documents, red flags, audit links, the newsmap."""
    by_key = {a["asset"]: a for a in assets}
    rows = []
    path = SEEDS / "issuers.jsonl"
    if path.exists():
        for r in jsonl(path):
            keys = [k for k in r["assets"] if k in by_key]
            if not keys:
                continue
            for d in r.get("documents") or []:
                rows.append({"url": d["url"], "assets": keys, "kind": d.get("kind", "other"),
                             "found_by": d.get("found_by"), "title": d.get("title")})
            for d in r.get("red_flags") or []:
                rows.append({"url": d["url"], "assets": keys, "kind": "red_flag",
                             "found_by": d.get("found_by"), "event_id": d.get("event_id"),
                             "why": d.get("why"), "title": None})
            for d in r.get("audits") or []:
                rows.append({"url": d["url"], "assets": keys, "kind": "audit",
                             "found_by": d.get("found_by"), "title": d.get("auditor")})
            for d in r.get("forums") or []:
                if "/t/" in d["url"]:
                    rows.append({"url": d["url"], "assets": keys, "kind": "governance_forum",
                                 "found_by": d.get("found_by"), "title": None})
    for a in assets:
        for row in a.get("defillama") or []:
            for u in row.get("audit_links") or []:
                rows.append({"url": u, "assets": [a["asset"]], "kind": "audit",
                             "found_by": row["found_by"] + " (audit_links)", "title": None})
    nm = CORPUS / "newsmap.jsonl"
    if nm.exists():
        for r in jsonl(nm):
            if r["asset"] in by_key:
                rows.append({"url": r["url"], "assets": [r["asset"]], "kind": "news",
                             "found_by": r["found_by"], "title": None})
    merged = {}
    for r in rows:
        u = r["url"].strip()
        if not u.startswith("http"):
            continue
        m = merged.setdefault(u, {**r, "assets": []})
        m["assets"] = sorted(set(m["assets"]) | set(r["assets"]))
        if r["kind"] == "red_flag":
            m["kind"] = "red_flag"
            m["event_id"] = r.get("event_id") or m.get("event_id")
            m["why"] = r.get("why") or m.get("why")
    return list(merged.values())


def discourse_seed(url):
    m = re.match(r"(https?://[^/]+)/t/(?:[^/]+/)?(\d+)", url)
    return (m.group(1), int(m.group(2))) if m else (None, None)


def fetch_seed(row, by_key, matcher):
    """Live fetch of one seed."""
    url = row["url"]
    members = [by_key[k] for k in row["assets"]]
    stype = seed_type(row["kind"], url, members)
    extra = {"seed_kind": row["kind"]}
    base, tid = discourse_seed(url)
    if base:
        about = fetcher().get(f"{base}/about.json")
        if about["status"] == 200 and about["body"].lstrip().startswith(b"{"):
            tj, posts, _ = discourse_thread(base, tid)
            if tj is not None:
                found = thread_assets(tj, posts, matcher, row["assets"])
                _n, ids = ingest_thread(base, fetch.host_of(base), tj, posts, found,
                                        row["found_by"], matcher)
                m = re.search(r"/t/[^/]+/\d+/(\d+)", url)
                if m:
                    want = int(m.group(1))
                    ids = [i for i, (p, _r) in zip(ids, posts) if p.get("post_number") == want] or ids
                return "discourse", ids, None
    if x_status_time(url):
        return "x", [], "x"
    rec = fetcher().get(url)
    blocked = rec["status"] in (401, 402, 403, 429, 451, "robots_refused", "error") or \
        (isinstance(rec["status"], int) and rec["status"] >= 500)
    item = err = None
    if rec["status"] == 200:
        item, err = make_from_http(rec, url=url, assets=row["assets"],
                                   source=f"web:{fetch.host_of(url)}", source_type=stype,
                                   found_by=row["found_by"], title_hint=row.get("title"), extra=extra)
    if item is None:
        need = "fallback" if (blocked or err == "no_text") else None
        return "failed", [f"{rec['status']} {err}"], need
    doc, status = put(item)
    if doc is None:
        return "invalid", [status], None
    need = "earlier" if doc["timestamp_evidence"] in ("http_date", "inferred") else None
    return status, [doc["id"]], need


def archive_seed(row, by_key, mode):
    """Archive copy of a seed."""
    url = row["url"]
    members = [by_key[k] for k in row["assets"]]
    extra = {"seed_kind": row["kind"]}
    if mode == "x":
        return fetch_x(url, row, members, extra)
    stype = seed_type(row["kind"], url, members)
    caps, _ = wayback_cdx(url, limit=4)
    if not caps:
        return "failed", ["no archive copy"]
    item, err = None, None
    for cap in caps:
        item, err = doc_from_capture(cap, url=url, assets=row["assets"], source_type=stype,
                                     found_by=row["found_by"] + "; earliest playable wayback capture",
                                     prefer_page_date=True)
        if item is not None:
            break
    if item is None:
        return "failed", [str(err)]
    item[0]["extra"] = {**item[0]["extra"], **extra, "archive_reason": mode}
    doc, status = put(item)
    return (status, [doc["id"]]) if doc else ("invalid", [status])


def fetch_x(url, row, members, extra):
    """An X post only from a dated archive copy; dated by its status id."""
    variants = [url, url.replace("x.com", "twitter.com")]
    for v in variants:
        caps, _ = wayback_cdx(v, limit=3)
        item = None
        for cap in caps:
            item, err = doc_from_capture(cap, url=url, assets=row["assets"], source_type="other",
                                         found_by=row["found_by"] + "; wayback capture",
                                         prefer_page_date=False)
            if item is not None:
                break
        if item is None:
            continue
        created = x_status_time(url)
        f = item[0]
        f["published_ts"], f["precision"], f["evidence"] = created, "second", "platform_api"
        f["evidence_source"] = (f"creation time encoded in X status id; text from wayback capture "
                                f"{cap['timestamp']}")
        f["source"] = "x"
        f["extra"] = {**f.get("extra", {}), **extra}
        doc, status = put(item)
        return (status, [doc["id"]]) if doc else ("invalid", [status])
    return "failed", ["no archive copy"]


def stage_news(args):
    t0 = time.time()
    assets = select(load_assets(), args)
    by_key = {a["asset"]: a for a in assets}
    matcher = Matcher(assets)
    rows = seed_rows(assets)
    log(f"news: {len(rows)} seed URLs")
    outcome = Counter()
    fails, flags = [], []
    result = {}
    later = []
    with ThreadPoolExecutor(10) as ex:
        futs = {ex.submit(fetch_seed, r, by_key, matcher): r for r in rows}
        for f in as_completed(futs):
            r = futs[f]
            try:
                status, info, need = f.result()
            except Exception as e:
                status, info, need = "error", [repr(e)[:200]], None
            result[r["url"]] = (status, info)
            if need:
                later.append((r, need))
    log(f"news: live pass done, {len(later)} seeds need an archive copy")
    with ThreadPoolExecutor(2) as ex:
        futs = {ex.submit(archive_seed, r, by_key, need): (r, need) for r, need in later}
        for f in as_completed(futs):
            r, need = futs[f]
            try:
                st2, info2 = f.result()
            except Exception as e:
                st2, info2 = "error", [repr(e)[:200]]
            st, info = result[r["url"]]
            ok2 = st2 in ("new", "replaced", "merged", "exists")
            if need == "earlier":
                result[r["url"]] = (st, info + (info2 if ok2 else []))
            else:
                result[r["url"]] = (f"archive:{st2}" if ok2 else st, info2 if ok2 else info)
    for r in rows:
        status, info = result[r["url"]]
        outcome[status] += 1
        ids = [i for i in info if re.fullmatch(r"[0-9a-f]{16}", str(i))]
        if not ids:
            fails.append({"url": r["url"], "kind": r["kind"], "why": info})
        if r["kind"] == "red_flag":
            flags.append({"url": r["url"], "assets": r["assets"], "event_id": r.get("event_id"),
                          "why": r.get("why"), "found_by": r["found_by"], "doc_ids": ids,
                          "fetch_status": status})
    log(f"news: {dict(outcome)}")
    rfp = CORPUS / "red_flags.jsonl"
    if args.asset and rfp.exists():
        mine = {x["url"] for x in flags}
        flags += [x for x in jsonl(rfp) if x["url"] not in mine and not set(x["assets"]) & set(by_key)]
    io_utils.write_jsonl("corpus/red_flags.jsonl", sorted(flags, key=lambda x: x["url"]), GENERATOR, {
        "what": "public red flags named by the source research (seeds/issuers.jsonl red_flags), "
                "with the event they precede and the corpus documents they resolved to",
        "resolved": sum(1 for x in flags if x["doc_ids"])})
    (WORK / "news_failures.jsonl").write_text("".join(json.dumps(x) + "\n" for x in fails))
    save_stats("news", time.time() - t0, {"outcome": dict(outcome), "seed_failures": fails[:400]})


def url_slug(url):
    s = re.sub(r"[^a-z0-9]+", "-", url.lower().split("://", 1)[-1]).strip("-")[:80]
    return f"{s}-{hashlib.sha1(url.encode()).hexdigest()[:8]}"


def key_pages(a, threads):
    """[(url, kind)] of an asset."""
    out = []
    for d in a.get("docs") or []:
        out.append((d["url"], "docs_root"))
    for k in a.get("key_pages") or []:
        u = k["url"][:-3] if k["url"].endswith(".md") else k["url"]
        out.append((u, k.get("kind") or "key_page"))
    for d in a.get("official_domains") or []:
        dom = d["domain"].strip().lower()
        dom = re.sub(r"^https?://", "", dom).strip("/")
        if dom:
            out.append((f"https://{dom}/", "homepage"))
    for row in a.get("defillama") or []:
        if row.get("docs"):
            out.append((row["docs"], "docs_root"))
        if row.get("url"):
            out.append((row["url"], "homepage"))
    for t in threads.get(a["asset"], [])[:5]:
        out.append((t, "governance_thread"))
    seen, res = set(), []
    for u, k in out:
        if u.startswith("http") and u not in seen:
            seen.add(u)
            res.append((u, k))
    return res


def forum_threads():
    """{asset: [thread URLs by number of posts]} from the forum documents."""
    count = defaultdict(Counter)
    for p in corpus.doc_files():
        d = json.loads(p.read_text())
        if d["source"].startswith("discourse:"):
            thread = d["url"].rsplit("/", 1)[0]
            for a in d["assets"]:
                count[a][thread] += 1
    return {a: [u for u, _n in c.most_common()] for a, c in count.items()}


def norm_url(u):
    """Comparable form of a URL."""
    p = urllib.parse.urlsplit(u.strip())
    host = (p.hostname or "").lower().removeprefix("www.")
    path = (p.path or "/").rstrip("/") or "/"
    return host + path + ("?" + p.query if p.query else "")


def cdx_prefix(root):
    """Every capture under a docs root in one CDX query (matchType=prefix)."""
    p = urllib.parse.urlsplit(root)
    prefix = (p.hostname or "") + (p.path.rstrip("/") or "")
    return wayback_cdx(prefix, limit=50000, extra="&matchType=prefix&filter=mimetype:text/html")


def stage_wayback(args):
    """Capture lists of every asset's key pages, and for each E2 event the capture of each key page of its assets closest before onset."""
    t0 = time.time()
    assets = select(load_assets(), args)
    threads = forum_threads()
    jobs = []
    for a in assets:
        pages = key_pages(a, threads)
        if a["tier"] != "A":
            pages = [(u, k) for u, k in pages if k != "governance_thread"]
        else:
            n = 0
            keep = []
            for u, k in pages:
                if k == "governance_thread":
                    n += 1
                    if n > 3:
                        continue
                keep.append((u, k))
            pages = keep
        for u, kind in pages:
            jobs.append((a["asset"], u, kind))
    roots = sorted({u for _k, u, kind in jobs if kind == "docs_root"})
    log(f"wayback: {len(jobs)} key pages over {len(assets)} assets, {len(roots)} docs roots")
    prefix_rows = {}
    with ThreadPoolExecutor(2) as ex:
        for root, (caps, rec) in zip(roots, ex.map(cdx_prefix, roots)):
            prefix_rows[root] = caps
            io_utils.write_json(f"corpus/wayback/_prefix/{url_slug(root)}.json", {
                "root": root, "cdx_status": rec["status"], "cdx_url": rec["final_url"],
                "queried_at": corpus.iso(rec["fetched_ts"]), "captures": caps},
                GENERATOR, {"what": "Wayback CDX prefix listing of a documentation root "
                                    "(status 200, collapsed by digest)", "captures": len(caps)})
    by_norm = defaultdict(list)
    for root, caps in prefix_rows.items():
        for c in caps:
            by_norm[norm_url(c["original"])].append(c)
    root_norms = [norm_url(r) for r in roots]

    def under_root(u):
        nu = norm_url(u)
        return any(nu == r or nu.startswith(r.rstrip("/") + "/") for r in root_norms)

    lists = {}
    exact = []
    for k, u, kind in jobs:
        if under_root(u):
            caps = sorted({c["timestamp"] + c["digest"]: c for c in by_norm.get(norm_url(u), [])}.values(),
                          key=lambda c: c["timestamp"])
            lists[(k, u)] = (caps, "prefix listing")
        else:
            exact.append((k, u))
    log(f"wayback: {len(lists)} pages from prefix listings, {len(exact)} exact queries")
    with ThreadPoolExecutor(2) as ex:
        futs = {ex.submit(wayback_cdx, u): (k, u) for k, u in dict.fromkeys(exact)}
        for f in as_completed(futs):
            k, u = futs[f]
            try:
                caps, _rec = f.result()
            except Exception:
                caps = []
            lists[(k, u)] = (caps, "exact query")
    kinds = {(k, u): kind for k, u, kind in jobs}
    for (k, u), (caps, how) in lists.items():
        io_utils.write_json(f"corpus/wayback/{k}/{url_slug(u)}.json", {
            "asset": k, "url": u, "kind": kinds[(k, u)], "listed_by": how, "captures": caps},
            GENERATOR, {"what": "Wayback capture list of a key page (status 200, collapsed by digest)",
                        "captures": len(caps)})
    todo = {}
    for a in assets:
        for e in a["e2_events"]:
            if e.get("excluded"):
                continue
            for (k, u), (caps, _how) in lists.items():
                if k != a["asset"] or kinds[(k, u)] == "governance_thread":
                    continue
                before = sorted((c for c in caps if capture_ts(c["timestamp"]) < e["onset_ts"]),
                                key=lambda c: c["timestamp"], reverse=True)
                if before:
                    c = before[0]
                    key = (c["timestamp"], c["original"])
                    todo.setdefault(key, {"cap": c, "alts": before[1:3], "url": u, "assets": set(),
                                          "events": set()})
                    todo[key]["assets"].add(k)
                    todo[key]["events"].add(e["event_id"])
    asset_roots = defaultdict(set)
    for k, u, kind in jobs:
        if kind == "docs_root":
            asset_roots[k].add(u)
    for a in assets:
        terms = {re.sub(r"[^a-z0-9]+", "", (m["symbol"] or "").lower()) for m in a["members"]}
        terms = {t for t in terms if len(t) >= 3}
        for root in asset_roots.get(a["asset"], ()):
            pages = defaultdict(list)
            for c in prefix_rows.get(root, []):
                if "html" not in (c.get("mimetype") or "") or "?" in c["original"] \
                        or "/.well-known/" in c["original"]:
                    continue
                pages[norm_url(c["original"])].append(c)
            rel = []
            for nu, caps in pages.items():
                path = nu.split("/", 1)[-1].lower()
                flat = re.sub(r"[^a-z0-9]+", "", path)
                if KEY_WORDS.search(path) or any(t in flat for t in terms):
                    rel.append((not any(t in flat for t in terms), len(path), nu, caps))
            rel.sort()
            for e in a["e2_events"]:
                if e.get("excluded"):
                    continue
                n = 0
                for _p, _l, nu, caps in rel:
                    before = sorted((c for c in caps if capture_ts(c["timestamp"]) < e["onset_ts"]),
                                    key=lambda c: c["timestamp"], reverse=True)
                    if not before:
                        continue
                    c = before[0]
                    key = (c["timestamp"], c["original"])
                    item = todo.setdefault(key, {"cap": c, "alts": before[1:3], "url": c["original"],
                                                 "assets": set(), "events": set()})
                    item["assets"].add(a["asset"])
                    item["events"].add(e["event_id"])
                    n += 1
                    if n >= ARCHIVED_PAGES_PER_ROOT:
                        break
    log(f"wayback: {len(todo)} captures before event onsets to fetch")
    outcome = Counter()

    def one(item):
        res = err = None
        for cap in [item["cap"]] + item["alts"]:
            res, err = doc_from_capture(cap, url=item["url"], assets=sorted(item["assets"]),
                                        source_type="web_archive",
                                        found_by=f"wayback CDX of key page {item['url']}: latest playable "
                                                 f"capture before onset of {', '.join(sorted(item['events']))}",
                                        prefer_page_date=False)
            if res is not None:
                break
        if res is None:
            return "failed:" + str(err)
        res[0]["extra"]["before_events"] = sorted(item["events"])
        doc, status = put(res)
        return status if doc else "invalid"

    with ThreadPoolExecutor(2) as ex:
        for st in ex.map(one, todo.values()):
            outcome[st.split(":")[0]] += 1
    n_caps = sum(len(c) for c, _h in lists.values())
    save_stats("wayback", time.time() - t0, {
        "key_pages": len(jobs), "docs_roots": len(roots), "captures_listed": n_caps,
        "pages_with_captures": sum(1 for c, _h in lists.values() if c),
        "fetch_outcome": dict(outcome), "to_fetch": len(todo)})
    log(f"wayback: {dict(outcome)}")


def all_docs():
    out = []
    for p in corpus.doc_files():
        d = json.loads(p.read_text())
        d["_path"] = str(p.relative_to(CORPUS))
        out.append(d)
    return out


def stage_index(args):
    t0 = time.time()
    docs = all_docs()
    rows = []
    for d in sorted(docs, key=lambda d: (d["published_ts"], d["id"])):
        row = {k: v for k, v in d.items() if k not in ("text", "_path")}
        row["path"] = d["_path"]
        row["text_chars"] = len(d["text"])
        rows.append(row)
    io_utils.write_jsonl("corpus/documents.jsonl", rows, GENERATOR, {
        "what": "index of the corpus documents (all fields except text; path points to the "
                "document JSON with text, raw_path to the raw payload)",
        "schema": "code/corpus/corpus.py", "filter": "code/corpus/leakage.py CutoffView",
        "by_source_type": dict(Counter(d["source_type"] for d in docs)),
        "by_evidence": dict(Counter(d["timestamp_evidence"] for d in docs))})
    log(f"index: {len(rows)} documents in {time.time() - t0:.0f} s")


def event_assets(assets):
    out = defaultdict(set)
    for a in assets:
        for e in a["e2_events"]:
            out[e["event_id"]].add(a["asset"])
    return out


def stage_coverage(args):
    assets = load_assets()
    docs = corpus.load_all()
    by_asset = defaultdict(list)
    for d in docs:
        for a in d["assets"]:
            by_asset[a].append(d)
    events = {e["event_id"]: e for e in jsonl(ROOT / "data/events/events.jsonl")}
    ea = event_assets(assets)
    per_event = []
    for eid, e in sorted(events.items()):
        t0 = e["onset_ts"]
        ds = {d["id"]: d for k in ea.get(eid, ()) for d in by_asset[k]}.values()
        visible = leakage.CutoffView(t0, docs=list(ds))
        vis = len(visible)
        win = sum(1 for d in visible if corpus.visible_from(d) >= t0 - 30 * DAY)
        after = sum(1 for d in ds if corpus.visible_from(d) >= t0)
        quarantined = sum(1 for d in ds if d["timestamp_evidence"] == "inferred")
        per_event.append({
            "event_id": eid, "type": e["type"], "onset_utc": e["onset_utc"],
            "family": e["collateral_family"]["label"], "excluded": e.get("excluded"),
            "assets": sorted(ea.get(eid, ())), "docs_total": len(list(ds)),
            "visible_before_onset": vis, "visible_in_30d_before_onset": win,
            "after_onset": after, "inferred_quarantined": quarantined,
            "visible_by_source_type": dict(Counter(d["source_type"] for d in visible))})
    months = Counter(corpus.iso(corpus.visible_from(d))[:7] for d in docs)
    crawled = [a for a in assets if a["tier"] in ("A", "B")]
    no_docs = [{"asset": a["asset"], "tier": a["tier"], "rank": a["rank"],
                "peak_exposure_usd": a["peak_exposure_usd"], "issuer": a.get("issuer")}
               for a in crawled if not by_asset.get(a["asset"])]
    cov = {
        "documents": len(docs),
        "by_source_type": dict(Counter(d["source_type"] for d in docs)),
        "by_evidence": dict(Counter(d["timestamp_evidence"] for d in docs)),
        "by_source_type_and_evidence": {st: dict(Counter(d["timestamp_evidence"] for d in docs
                                                         if d["source_type"] == st))
                                        for st in corpus.SOURCE_TYPES},
        "by_asset": {a["asset"]: {"tier": a["tier"], "rank": a["rank"], "documents": len(by_asset.get(a["asset"], [])),
                                  "by_source_type": dict(Counter(d["source_type"] for d in by_asset.get(a["asset"], [])))}
                     for a in crawled},
        "by_month": dict(sorted(months.items())),
        "events": per_event,
        "assets_without_documents": no_docs,
        "tiers": dict(Counter(a["tier"] for a in assets)),
    }
    write_json("coverage.json", cov, {"what": "corpus coverage by source type, evidence, asset and "
                                              "month; per E2 event the documents about its collateral "
                                              "visible to the cutoff filter at onset, in the 30 days "
                                              "before onset, and after onset",
                                      "month_basis": "visible_from (max of published and modified)"})
    log(f"coverage: {len(docs)} docs, {len(no_docs)} crawled assets without documents")


META_RX = r'<meta[^>]*?(?:property|name|itemprop)\s*=\s*["\']{key}["\'][^>]*?content\s*=\s*["\']([^"\']+)["\']'
META_RX2 = r'<meta[^>]*?content\s*=\s*["\']([^"\']+)["\'][^>]*?(?:property|name|itemprop)\s*=\s*["\']{key}["\']'
PUB_KEYS_RX = re.compile(r'(?:property|name|itemprop)\s*=\s*["\'](article:published_time|datePublished|'
                         r'publish-date|pubdate|parsely-pub-date|sailthru\.date|og:published_time)["\']'
                         r'|"datePublished"\s*:', re.I)


def iso_parse(v):
    """Independent date parse (stdlib only, ISO 8601 or a few fixed formats)."""
    v = v.strip().replace("Z", "+00:00")
    if re.fullmatch(r"\d{4}-\d\d-\d\d", v):
        return int(datetime.fromisoformat(v).replace(tzinfo=timezone.utc).timestamp()), False
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        for fmt in ("%b %d, %YT%H:%M:%S%z", "%B %d, %YT%H:%M:%S%z", "%a, %d %b %Y %H:%M:%S %z",
                    "%Y-%m-%d %H:%M:%S%z", "%Y/%m/%d %H:%M:%S%z", "%b %d, %Y", "%B %d, %Y"):
            try:
                dt = datetime.strptime(v.replace("+00:00", "+0000"), fmt)
                break
            except ValueError:
                continue
        else:
            raise
    if dt.tzinfo is None:
        return int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp()), False
    return int(dt.timestamp()), True


def verify_doc(d):
    """Re-derive the timestamp from the raw payload (or the cached response for HTTP headers) by a parse independent of the crawler's extractor."""
    raw_p = CORPUS / d["raw_path"]
    if not raw_p.exists():
        return False, "raw payload missing"
    raw = raw_p.read_bytes()
    if sha256(raw) != d["sha256"]:
        return False, "raw sha256 mismatch"
    ev, ts = d["timestamp_evidence"], d["published_ts"]
    src = d.get("timestamp_source") or ""
    if ev == "archive_capture":
        m = re.search(r"web\.archive\.org/web/(\d{14})id_/", d["final_url"])
        got = capture_ts(m.group(1)) if m else None
        return got == ts, f"capture id {m.group(1) if m else None}"
    if ev == "platform_api":
        if d["source"].startswith("discourse:"):
            p = json.loads(raw)
            got, _ = iso_parse(p["created_at"])
            return got == ts, f"raw created_at {p['created_at']}"
        if d["source"].startswith("snapshot:"):
            p = json.loads(raw)
            return int(p["created"]) == ts, f"raw created {p['created']}"
        if d["source"] == "x":
            m = re.search(r"/status(?:es)?/(\d+)", d["url"])
            got = ((int(m.group(1)) >> 22) + 1288834974657) // 1000
            return abs(got - ts) <= 1, f"status id {m.group(1)}"
        if d["source"].startswith("git:"):
            org_repo = d["source"][4:]
            g = WORK / "git" / org_repo.replace("/", "__")
            if not g.exists():
                return False, "clone missing"
            got = int(git("show", "-s", "--format=%ct", d["git_commit"], cwd=g).strip())
            blob_ok = git("show", f"{d['git_commit']}:{d['git_path']}", cwd=g, binary=True) == raw
            return got == ts and blob_ok, f"git commit time {got}, blob identical {blob_ok}"
    if ev == "page_metadata":
        if d["raw_path"].endswith(".pdf"):
            m = re.search(rb"/CreationDate\s*\(D:(\d{8})", raw)
            if not m:
                return None, "pdf creation date not found in raw (object stream)"
            day = datetime.strptime(m.group(1).decode(), "%Y%m%d").replace(tzinfo=timezone.utc)
            return abs(int(day.timestamp()) - ts) < 2 * DAY, f"pdf CreationDate {m.group(1).decode()}"
        html = raw.decode("utf-8", "replace")
        m = re.match(r"(meta\[([^\]]+)\]|json-ld (\w+)|time\[datetime\])=(\S+)", src)
        if not m:
            return False, f"unparsed source {src[:80]}"
        if m.group(2):
            key = re.escape(m.group(2))
            vals = re.findall(META_RX.format(key=key), html, re.I) + re.findall(META_RX2.format(key=key), html, re.I)
        elif m.group(3):
            vals = re.findall(r'"%s"\s*:\s*"([^"]+)"' % re.escape(m.group(3)), html)
        else:
            vals = re.findall(r'<time[^>]*datetime\s*=\s*["\']([^"\']+)', html, re.I)
        for v in vals:
            try:
                got, has_time = iso_parse(v)
            except ValueError:
                continue
            if got == ts or (not has_time and abs(got - ts) < DAY):
                return True, f"raw {m.group(1)} value {v}"
        return False, f"raw values {vals[:3]} for {m.group(1)}"
    if ev in ("http_last_modified", "http_date"):
        hdrs = d.get("http_headers") or {}
        name = "last-modified" if ev == "http_last_modified" else "date"
        from email.utils import parsedate_to_datetime
        if name in hdrs:
            got = int(parsedate_to_datetime(hdrs[name]).timestamp())
            ok = got == ts
        else:
            ok = ts == d["retrieved_ts"]
            got = d["retrieved_ts"]
        html = raw.decode("utf-8", "replace") if d["raw_path"].endswith(".html") else ""
        stronger = bool(PUB_KEYS_RX.search(html))
        return ok and not stronger, f"{name} {got}" + (" but raw has publication metadata" if stronger else "")
    if ev == "body_date":
        m = re.search(r"body: (.+)$", src, re.S)
        snippet = ast.literal_eval(m.group(1)) if m else ""
        dm = re.search(r"(20\d\d)-(\d\d)-(\d\d)", snippet)
        if dm:
            day = datetime(int(dm.group(1)), int(dm.group(2)), int(dm.group(3)), tzinfo=timezone.utc)
        else:
            dm = re.search(r"([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(20\d\d)|(\d{1,2})\s+([A-Za-z]{3,9})\.?,?\s+(20\d\d)",
                           snippet)
            if not dm:
                return False, f"no date in dateline {snippet[:60]!r}"
            mon, dd, yy = (dm.group(1), dm.group(2), dm.group(3)) if dm.group(1) else \
                (dm.group(5), dm.group(4), dm.group(6))
            day = datetime.strptime(f"{mon[:3].title()} {int(dd)} {yy}", "%b %d %Y").replace(tzinfo=timezone.utc)
        in_text = re.sub(r"\s+", " ", snippet)[:25] in re.sub(r"\s+", " ", d["text"])
        return in_text and int(day.timestamp()) == ts, f"dateline {snippet[:60]!r} in text {in_text}"
    if ev == "inferred":
        m = re.search(r"/(20\d\d)[/-](\d\d?)[/-](\d\d?)", d["url"])
        if not m:
            return False, "no url date"
        got = int(datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc).timestamp())
        return got == ts, f"url date {m.group(0)}"
    return False, f"no verifier for {ev}/{d['source']}"


def selfcheck_case(case, eid, label, onset, ds, ea, red_flags):
    """Cutoff filter at one onset."""
    view = list(leakage.CutoffView(onset, docs=ds))
    leaks = [d["id"] for d in view if corpus.visible_from(d) >= onset]
    reverified = Counter()
    bad = []
    for d in view:
        try:
            ok, det = verify_doc(d)
        except Exception as e:
            ok, det = False, repr(e)[:100]
        reverified[str(ok)] += 1
        if ok is False:
            bad.append({"id": d["id"], "url": d["url"], "detail": det})
    vis_ids = {d["id"]: d for d in view}
    flags = []
    for rf in red_flags:
        hit = [vis_ids[i] for i in rf["doc_ids"] if i in vis_ids]
        if hit and set(rf["assets"]) & set(ea.get(eid, ())):
            first = min(hit, key=corpus.visible_from)
            flags.append({"published_at": first["published_at"], "url": rf["url"],
                          "doc_id": first["id"], "source_type": first["source_type"],
                          "evidence": first["timestamp_evidence"],
                          "research_event": rf.get("event_id"), "why": rf.get("why"),
                          "title": first["title"][:160]})
    flags.sort(key=lambda x: x["published_at"])
    return {"case": case, "event_id": eid, "cutoff": label, "onset_utc": corpus.iso(onset),
            "assets": sorted(ea.get(eid, ())), "documents_about_assets": len(ds),
            "visible_at_onset": len(view), "leaks": len(leaks), "leak_ids": leaks,
            "visible_reverified_from_raw": dict(reverified),
            "visible_reverify_failures": bad[:50],
            "red_flags_visible_at_onset": flags,
            "red_flags_note": None if flags else
            "no document the source research marked as a red flag is visible before onset"}


def stage_validate(args):
    docs = corpus.load_all()
    rng = random.Random(20261008)
    sample = rng.sample(docs, min(40, len(docs)))
    rows, disagree = [], []
    for d in sample:
        try:
            ok, detail = verify_doc(d)
        except Exception as e:
            ok, detail = False, f"verifier error {e!r}"[:200]
        r = {"id": d["id"], "url": d["url"], "source_type": d["source_type"],
             "evidence": d["timestamp_evidence"], "published_at": d["published_at"],
             "agrees": ok, "detail": detail}
        rows.append(r)
        if ok is not True:
            disagree.append(r)
    assets = load_assets()
    ea = event_assets(assets)
    sc = json.loads((ROOT / "data/events/selfcheck.json").read_text())
    by_asset = defaultdict(list)
    for d in docs:
        for a in d["assets"]:
            by_asset[a].append(d)
    rfp = CORPUS / "red_flags.jsonl"
    red_flags = jsonl(rfp) if rfp.exists() else []
    events = {e["event_id"]: e for e in jsonl(ROOT / "data/events/events.jsonl")}
    cases = []
    for c in sc["cases"]:
        for ev in c["events"]:
            eid = ev["event_id"]
            ds = list({d["id"]: d for k in ea.get(eid, ()) for d in by_asset[k]}.values())
            cutoffs = {"event onset": events[eid]["onset_ts"],
                       "case episode onset": min(ep["onset_ts"] for ep in ev["matching_episodes"])}
            for label, onset in sorted(cutoffs.items(), key=lambda x: x[1]):
                if label == "case episode onset" and onset == cutoffs["event onset"]:
                    continue
                cases.append(selfcheck_case(c["case"], eid, label, onset, ds, ea, red_flags))
    shas = Counter(d["sha256"] for d in docs)
    empty = [d["id"] for d in docs if not d["text"].strip()]
    res = {
        "sample": {"n": len(sample), "agree": sum(1 for r in rows if r["agrees"] is True),
                   "disagree": len(disagree), "rows": rows, "disagreements": disagree},
        "selfcheck_events": cases,
        "empty_text": {"n": len(empty), "ids": empty[:50]},
        "duplicate_sha256": {"n": sum(1 for v in shas.values() if v > 1),
                             "shas": [s for s, v in shas.items() if v > 1][:50]},
        "future_reference_flags": leakage.future_reference_flags(docs),
        "documents": len(docs),
    }
    write_json("validation.json", res, {"what": "E7 corpus validation: independent re-parse of 40 "
                                                "random documents' timestamps from their raw payloads; "
                                                "cutoff filter at the onset of the 7 self-check events "
                                                "(leaks, re-verification, red flags); empty texts; "
                                                "duplicate payloads", "seed": 20261008})
    log(f"validate: sample {res['sample']['agree']}/{len(sample)} agree; "
        f"leaks {[c['leaks'] for c in cases]}; empty {len(empty)}; dup {res['duplicate_sha256']['n']}")


STAGES = {"assets": stage_assets, "forums": stage_forums, "docs": stage_docs,
          "github": stage_github, "curators": stage_curators, "newsmap": stage_newsmap,
          "news": stage_news,
          "wayback": stage_wayback, "index": stage_index, "coverage": stage_coverage,
          "validate": stage_validate}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--stage", nargs="+", default=list(STAGES), choices=list(STAGES))
    ap.add_argument("--asset", nargs="*", help="only these corpus asset keys")
    ap.add_argument("--tier", nargs="*", choices=["A", "B"], help="only these tiers")
    ap.add_argument("--keep-existing", action="store_true",
                    help="never modify a document already in the store")
    args = ap.parse_args()
    global KEEP_EXISTING
    KEEP_EXISTING = args.keep_existing
    WORK.mkdir(parents=True, exist_ok=True)
    for st in args.stage:
        log(f"=== stage {st}")
        t0 = time.time()
        STAGES[st](args)
        log(f"=== stage {st} done in {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()
