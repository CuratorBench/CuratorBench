"""Polite HTTP client for the corpus crawl."""
import gzip
import hashlib
import json
import os
import re
import threading
import time
import urllib.parse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

USER_AGENT = ("CuratorBenchCorpus/1.0 (academic research crawler for a study of "
              "DeFi lending risk; honours robots.txt; one request per second per host)")
ROBOTS_TOKEN = "curatorbenchcorpus"
DEFAULT_INTERVAL = 1.0
SLOW_HOSTS = {"web.archive.org": 2.5, "archive.org": 2.5, "api.github.com": 2.0,
              "api.coingecko.com": 6.0}
MAX_RETRIES = 4
TIMEOUT = 40
MAX_BYTES = 40 * 1024 * 1024
KEEP_HEADERS = ("content-type", "last-modified", "date", "etag", "location",
                "content-length", "retry-after", "x-robots-tag", "x-ratelimit-remaining",
                "x-ratelimit-reset")
MAX_RATE_WAIT = 3700


def utc_now():
    return int(time.time())


def host_of(url):
    return (urllib.parse.urlsplit(url).hostname or "").lower()


class Robots:
    """One host's robots.txt rules for our user agent (RFC 9309)."""

    def __init__(self, text):
        self.rules, self.delay = self._parse(text or "")

    @staticmethod
    def _parse(text):
        groups, cur, last_was_agent = [], None, False
        for raw in text.splitlines():
            line = raw.split("#", 1)[0].strip()
            if ":" not in line:
                continue
            key, val = (x.strip() for x in line.split(":", 1))
            key = key.lower()
            if key == "user-agent":
                if not last_was_agent:
                    cur = {"agents": [], "rules": [], "delay": None}
                    groups.append(cur)
                cur["agents"].append(val.lower())
                last_was_agent = True
                continue
            last_was_agent = False
            if cur is None:
                continue
            if key in ("allow", "disallow"):
                cur["rules"].append((key == "allow", val))
            elif key == "crawl-delay":
                try:
                    cur["delay"] = float(val)
                except ValueError:
                    pass
        mine = [g for g in groups if any(a != "*" and a in ROBOTS_TOKEN for a in g["agents"])]
        if not mine:
            mine = [g for g in groups if "*" in g["agents"]]
        rules, delay = [], None
        for g in mine:
            rules.extend(g["rules"])
            delay = g["delay"] if g["delay"] is not None else delay
        return rules, delay

    @staticmethod
    def _match(pattern, path):
        if not pattern:
            return -1
        anchored = pattern.endswith("$")
        pat = pattern[:-1] if anchored else pattern
        rx = "".join(".*" if c == "*" else re.escape(c) for c in pat)
        if re.match(rx + ("$" if anchored else ""), path):
            return len(pattern)
        return -1

    def allowed(self, url):
        parts = urllib.parse.urlsplit(url)
        path = urllib.parse.unquote(parts.path or "/") + ("?" + parts.query if parts.query else "")
        best_len, best_allow = -1, True
        for allow, pat in self.rules:
            n = self._match(pat, path)
            if n > best_len or (n == best_len and allow and n >= 0):
                best_len, best_allow = n, allow
        return best_allow if best_len >= 0 else True


class Fetcher:
    def __init__(self, cache_dir, log_path=None):
        self.cache_dir = cache_dir
        self.log_path = log_path
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.robots = {}
        self.host_locks = defaultdict(threading.Lock)
        self.next_ok = defaultdict(float)
        self.penalty = defaultdict(int)
        self.meta_lock = threading.Lock()
        self.stats = Counter()
        self.by_host = defaultdict(Counter)
        self.failures = []

    def _cache_paths(self, url, method="GET", body=None):
        key = method + " " + url + ("\n" + body if body else "")
        h = hashlib.sha256(key.encode()).hexdigest()[:32]
        d = self.cache_dir / (host_of(url) or "_")
        return d / f"{h}.json", d / f"{h}.body.gz"

    def _read_cache(self, url, method="GET", body=None):
        mp, bp = self._cache_paths(url, method, body)
        if not mp.exists():
            return None
        rec = json.loads(mp.read_text())
        rec["body"] = gzip.decompress(bp.read_bytes()) if bp.exists() else b""
        rec["from_cache"] = True
        rec["cache_path"] = str(bp)
        return rec

    def _write_cache(self, rec, method="GET", body=None):
        mp, bp = self._cache_paths(rec["url"], method, body)
        mp.parent.mkdir(parents=True, exist_ok=True)
        tmp = bp.with_name(bp.name + ".tmp")
        tmp.write_bytes(gzip.compress(rec["body"], mtime=0))
        os.replace(tmp, bp)
        meta = {k: v for k, v in rec.items() if k not in ("body", "from_cache", "cache_path")}
        tmp = mp.with_name(mp.name + ".tmp")
        tmp.write_text(json.dumps(meta, sort_keys=True))
        os.replace(tmp, mp)
        rec["cache_path"] = str(bp)

    def _wait(self, host):
        interval = SLOW_HOSTS.get(host, DEFAULT_INTERVAL)
        rb = self.robots.get(host)
        if rb is not None and rb.delay:
            interval = max(interval, min(rb.delay, 30.0))
        with self.host_locks[host]:
            now = time.monotonic()
            wait = self.next_ok[host] - now
            if wait > 0:
                time.sleep(wait)
            self.next_ok[host] = time.monotonic() + interval

    def _throttle(self, host, overloaded):
        """Host-wide backoff: after a 429 or 503 every thread waits for that host, doubling per consecutive overload (at most 5 minutes)."""
        with self.host_locks[host]:
            if overloaded:
                self.penalty[host] += 1
                pause = min(2 ** self.penalty[host], 300)
                self.next_ok[host] = max(self.next_ok[host], time.monotonic() + pause)
                with self.meta_lock:
                    self.stats["host_backoffs"] += 1
            else:
                self.penalty[host] = 0

    def robots_for(self, url):
        parts = urllib.parse.urlsplit(url)
        host = (parts.hostname or "").lower()
        if host in self.robots:
            return self.robots[host]
        robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
        rec = self._read_cache(robots_url)
        if rec is None:
            rec = self._request(robots_url, check_robots=False)
        text = ""
        if rec.get("status") == 200 and b"<html" not in rec["body"][:500].lower():
            text = rec["body"].decode("utf-8", "replace")
        elif rec.get("status") in (401, 403):
            text = "User-agent: *\nDisallow: /\n"
        self.robots[host] = Robots(text)
        return self.robots[host]

    def _log(self, row):
        if self.log_path:
            with self.meta_lock, open(self.log_path, "a") as fh:
                fh.write(json.dumps(row) + "\n")

    def _request(self, url, check_robots=True, method="GET", data=None, headers=None):
        """One URL, following redirects by hand; returns a cache record."""
        hops = []
        cur = url
        for _hop in range(6):
            host = host_of(cur)
            if check_robots and not self.robots_for(cur).allowed(cur):
                with self.meta_lock:
                    self.stats["robots_refused"] += 1
                    self.by_host[host]["robots_refused"] += 1
                return {"url": url, "final_url": cur, "status": "robots_refused",
                        "headers": {}, "fetched_ts": utc_now(), "redirects": hops,
                        "body": b""}
            resp, err = None, None
            for attempt in range(MAX_RETRIES + 1):
                self._wait(host)
                with self.meta_lock:
                    self.stats["requests"] += 1
                    self.by_host[host]["requests"] += 1
                try:
                    resp = self.session.request(method, cur, data=data, headers=headers,
                                                timeout=TIMEOUT, allow_redirects=False,
                                                stream=True)
                    content = resp.raw.read(MAX_BYTES + 1, decode_content=True)
                    resp._content = content[:MAX_BYTES]
                    err = None
                except requests.RequestException as e:
                    resp, err = None, f"{type(e).__name__}: {e}"[:300]
                self._log({"ts": utc_now(), "url": cur, "attempt": attempt,
                           "status": getattr(resp, "status_code", None), "error": err})
                retry = err is not None or resp.status_code == 429 or resp.status_code >= 500
                self._throttle(host, resp is not None and resp.status_code in (429, 503))
                if resp is not None and resp.status_code in (403, 429) and \
                        resp.headers.get("x-ratelimit-remaining") == "0":
                    reset = int(resp.headers.get("x-ratelimit-reset", "0") or 0)
                    wait = min(max(reset - time.time(), 0) + 5, MAX_RATE_WAIT)
                    with self.meta_lock:
                        self.stats["rate_limit_waits"] += 1
                    time.sleep(wait)
                    continue
                if not retry or attempt == MAX_RETRIES:
                    break
                with self.meta_lock:
                    self.stats["retries"] += 1
                delay = 2 ** (attempt + 1)
                if resp is not None and resp.headers.get("retry-after", "").isdigit():
                    delay = max(delay, min(int(resp.headers["retry-after"]), 300))
                time.sleep(delay)
            if resp is None:
                with self.meta_lock:
                    self.stats["failures"] += 1
                    self.by_host[host]["failures"] += 1
                return {"url": url, "final_url": cur, "status": "error", "error": err,
                        "headers": {}, "fetched_ts": utc_now(), "redirects": hops,
                        "body": b""}
            hdrs = {k.lower(): v for k, v in resp.headers.items() if k.lower() in KEEP_HEADERS}
            if resp.status_code in (301, 302, 303, 307, 308) and "location" in hdrs:
                hops.append({"url": cur, "status": resp.status_code})
                cur = urllib.parse.urljoin(cur, hdrs["location"])
                if resp.status_code == 303:
                    method, data = "GET", None
                continue
            if resp.status_code >= 400:
                with self.meta_lock:
                    self.stats["http_errors"] += 1
                    self.by_host[host][f"http_{resp.status_code}"] += 1
            return {"url": url, "final_url": cur, "status": resp.status_code,
                    "headers": hdrs, "fetched_ts": utc_now(), "redirects": hops,
                    "body": resp.content or b"",
                    "sha256": hashlib.sha256(resp.content or b"").hexdigest()}
        return {"url": url, "final_url": cur, "status": "too_many_redirects", "headers": {},
                "fetched_ts": utc_now(), "redirects": hops, "body": b""}

    def get(self, url, refresh=False, method="GET", data=None, headers=None):
        """Cached fetch. Transport failures and robots refusals are not cached (they are retried on the next run); HTTP statuses are."""
        body_key = data if isinstance(data, str) else (json.dumps(data, sort_keys=True) if data else None)
        if not refresh:
            rec = self._read_cache(url, method, body_key)
            if rec is not None:
                with self.meta_lock:
                    self.stats["cache_hits"] += 1
                return rec
        rec = self._request(url, method=method, data=data, headers=headers)
        if isinstance(rec["status"], int):
            self._write_cache(rec, method, body_key)
        else:
            with self.meta_lock:
                self.failures.append({"url": url, "status": rec["status"],
                                      "error": rec.get("error")})
        rec["from_cache"] = False
        return rec

    def summary(self):
        return {"totals": dict(self.stats),
                "hosts": {h: dict(c) for h, c in sorted(self.by_host.items())},
                "n_hosts": len(self.by_host)}


def header_time(rec, name):
    """Unix seconds of an HTTP date header, or None."""
    v = (rec.get("headers") or {}).get(name)
    if not v:
        return None
    try:
        dt = parsedate_to_datetime(v)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
