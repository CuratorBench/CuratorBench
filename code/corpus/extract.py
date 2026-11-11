"""Text and timestamp evidence from raw payloads."""
import io
import json
import re
import warnings
from datetime import datetime, timezone

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

MIN_TS = 1230768000

PUB_META = [
    "article:published_time", "og:article:published_time", "article:published",
    "datepublished", "published_time", "og:published_time", "publish-date",
    "publish_date", "publishdate", "pubdate", "parsely-pub-date", "sailthru.date",
    "dc.date.issued", "dcterms.issued", "dc.date", "dcterms.created", "date",
    "citation_publication_date", "citation_date", "article.published",
    "original-publish-date", "datecreated",
]
MOD_META = ["article:modified_time", "og:updated_time", "datemodified",
            "article:modified", "dcterms.modified", "last-modified", "modified_time",
            "dc.date.modified", "lastmod"]
MONTHS = ("january|february|march|april|may|june|july|august|september|october|"
          "november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec")
BODY_RX = [
    re.compile(r"(?:published|posted|released|dated|date)\s*(?:on|:)?\s*"
               r"((?:" + MONTHS + r")\.?\s+\d{1,2},?\s+20\d\d)", re.I),
    re.compile(r"(?:published|posted|released|dated|date)\s*(?:on|:)?\s*"
               r"(\d{1,2}\s+(?:" + MONTHS + r")\.?,?\s+20\d\d)", re.I),
    re.compile(r"(?:published|posted|released|dated|date)\s*(?:on|:)?\s*(20\d\d-\d\d-\d\d)", re.I),
]
URL_RX = [re.compile(r"/(20\d\d)/(\d{1,2})/(\d{1,2})(?:/|$)"),
          re.compile(r"[/_-](20\d\d)-(\d\d)-(\d\d)(?:[/_.-]|$)")]


def parse_when(value):
    """(unix seconds, precision) from a date string, or None."""
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    if re.fullmatch(r"\d{10}(\.\d+)?|\d{13}", v):
        n = float(v)
        n = n / 1000 if n > 1e12 else n
        return int(n), "second"
    from dateutil import parser as dparser
    try:
        dt = dparser.parse(v, fuzzy=False)
    except (ValueError, OverflowError, TypeError):
        return None
    has_time = bool(re.search(r"\d{1,2}:\d{2}", v))
    if dt.tzinfo is None or not has_time:
        day = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
        return int(day.timestamp()), "day"
    return int(dt.astimezone(timezone.utc).timestamp()), "second"


def _ok(ts, fetched_ts):
    return ts is not None and MIN_TS <= ts[0] <= fetched_ts + 60


def _jsonld_dates(soup):
    pub, mod = [], []

    def walk(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in ("datePublished", "dateCreated", "uploadDate") and isinstance(v, str):
                    pub.append((k, v))
                elif k == "dateModified" and isinstance(v, str):
                    mod.append((k, v))
                else:
                    walk(v)
        elif isinstance(o, list):
            for x in o:
                walk(x)

    for tag in soup.find_all("script", type=re.compile("ld\\+json", re.I)):
        try:
            walk(json.loads(tag.string or tag.get_text() or "null"))
        except (ValueError, TypeError):
            continue
    return pub, mod


def html_dates(html, fetched_ts):
    """Publication and modification evidence from page metadata."""
    soup = BeautifulSoup(html, "lxml")
    metas = {}
    for m in soup.find_all("meta"):
        key = (m.get("property") or m.get("name") or m.get("itemprop") or "").strip().lower()
        val = m.get("content")
        if key and val and key not in metas:
            metas[key] = val
    pub = mod = None
    for k in PUB_META:
        if k in metas:
            w = parse_when(metas[k])
            if _ok(w, fetched_ts):
                pub = (w[0], w[1], f"meta[{k}]={metas[k]}")
                break
    ld_pub, ld_mod = _jsonld_dates(soup)
    if pub is None:
        for k, v in ld_pub:
            w = parse_when(v)
            if _ok(w, fetched_ts):
                pub = (w[0], w[1], f"json-ld {k}={v}")
                break
    if pub is None:
        for t in soup.find_all("time"):
            if t.get("datetime") and (t.get("itemprop", "").lower() == "datepublished"
                                      or t.has_attr("pubdate")):
                w = parse_when(t["datetime"])
                if _ok(w, fetched_ts):
                    pub = (w[0], w[1], f"time[datetime]={t['datetime']}")
                    break
    for k in MOD_META:
        if k in metas:
            w = parse_when(metas[k])
            if _ok(w, fetched_ts):
                mod = (w[0], w[1], f"meta[{k}]={metas[k]}")
                break
    if mod is None:
        for k, v in ld_mod:
            w = parse_when(v)
            if _ok(w, fetched_ts):
                mod = (w[0], w[1], f"json-ld {k}={v}")
                break
    return {"published": pub, "modified": mod}


def body_date(text, fetched_ts):
    head = text[:1500]
    for rx in BODY_RX:
        m = rx.search(head)
        if m:
            w = parse_when(m.group(1))
            if _ok(w, fetched_ts):
                return (w[0], "day", f"body: {m.group(0)[:80]!r}")
    return None


def url_date(url, fetched_ts):
    for rx in URL_RX:
        m = rx.search(url)
        if m:
            try:
                d = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
            except ValueError:
                continue
            ts = int(d.timestamp())
            if MIN_TS <= ts <= fetched_ts:
                return (ts, "day", f"url path {m.group(0)}")
    return None


def html_title(html):
    soup = BeautifulSoup(html, "lxml")
    for key in ("og:title", "twitter:title"):
        m = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
        if m and m.get("content"):
            return m["content"].strip()
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    h1 = soup.find("h1")
    return h1.get_text(" ", strip=True) if h1 else ""


def fragment_text(html):
    """Plain text of an HTML fragment (Discourse cooked posts)."""
    soup = BeautifulSoup(html or "", "lxml")
    for t in soup(["script", "style"]):
        t.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")
    text = soup.get_text("\n")
    return re.sub(r"\n{3,}", "\n\n", re.sub(r"[ \t\xa0]+", " ", text)).strip()


def html_text(html, url=None):
    """Main text of a full HTML page."""
    import trafilatura
    text = None
    try:
        text = trafilatura.extract(html, url=url, favor_recall=True, include_comments=False,
                                   include_tables=True, deduplicate=True)
    except Exception:
        text = None
    if not text or len(text) < 200:
        soup = BeautifulSoup(html, "lxml")
        for t in soup(["script", "style", "noscript", "svg", "nav", "footer", "header"]):
            t.decompose()
        alt = re.sub(r"\n{3,}", "\n\n", re.sub(r"[ \t\xa0]+", " ", soup.get_text("\n"))).strip()
        if len(alt) > len(text or ""):
            text = alt
    return (text or "").strip()


def pdf_text_and_dates(data, fetched_ts):
    """(title, text, published, modified) of a PDF payload."""
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(data))
    parts = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            continue
    text = re.sub(r"[ \t\xa0]+", " ", "\n".join(parts)).strip()
    info = reader.metadata or {}
    title = (info.get("/Title") or "").strip() if info else ""
    pub = mod = None
    for key, slot in (("/CreationDate", "pub"), ("/ModDate", "mod")):
        raw = info.get(key) if info else None
        if not raw:
            continue
        m = re.match(r"D:(\d{4})(\d{2})(\d{2})(\d{2})?(\d{2})?(\d{2})?([Zz+\-])?(\d{2})?'?(\d{2})?", str(raw))
        if not m:
            continue
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hh, mi, ss = int(m.group(4) or 0), int(m.group(5) or 0), int(m.group(6) or 0)
        try:
            dt = datetime(y, mo, d, hh, mi, ss, tzinfo=timezone.utc)
        except ValueError:
            continue
        prec = "day"
        if m.group(7):
            prec = "second"
            if m.group(7) in "+-" and m.group(8):
                off = (int(m.group(8)) * 60 + int(m.group(9) or 0)) * 60
                dt = dt.replace(tzinfo=timezone.utc)
                ts = int(dt.timestamp()) - off if m.group(7) == "+" else int(dt.timestamp()) + off
            else:
                ts = int(dt.timestamp())
        else:
            ts = int(dt.timestamp())
        if MIN_TS <= ts <= fetched_ts + 60:
            val = (ts, prec, f"pdf {key}={raw}")
            if slot == "pub":
                pub = val
            else:
                mod = val
    return title, text, pub, mod


def language(text):
    import py3langid
    if not text or len(text) < 40:
        return "und"
    lang, _score = py3langid.classify(text[:5000])
    return lang
