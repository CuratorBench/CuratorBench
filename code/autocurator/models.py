# /// script
# requires-python = ">=3.11"
# dependencies = ["requests>=2.31"]
# ///
"""Model adapters, the request archive, prices and the leakage rule."""
import calendar
import datetime as dt
import gzip
import hashlib
import json
import os
import pathlib
import re
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
ARCHIVE = ROOT / "data" / "autocurator" / "archive"
D6 = ROOT / "docs" / "d6-llm-selection.md"
sys.path.insert(0, str(ROOT / "code"))

DAY = 86400
LEAKAGE_MARGIN_D = 30
KEY_NAMES = {"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY",
             "google": "GOOGLE_API_KEY", "openrouter": "OPENROUTER_API_KEY", "gateway": "MODEL_GATEWAY_KEY"}
CHARS_PER_TOKEN = 4.0
MAX_ATTEMPTS = 30
MAX_WAIT_S = 120.0
THINKING_TOKENS = 1000

MODELS = {
    "claude-opus-4-6": dict(vendor="anthropic", provider="anthropic", api_id="claude-opus-4-6",
                            cutoff="2025-08", price_in=5.0, price_out=25.0, context=1_000_000,
                            temperature=True, seed=False, open_weight=False, reasoning=False),
    "gpt-5.4-2026-03-05": dict(vendor="openai", provider="openrouter", api_id="openai/gpt-5.4",
                               host="openai/flex", cutoff="2025-08-31", price_in=1.25, price_out=7.5,
                               context=1_050_000, temperature=True, seed=True, open_weight=False,
                               reasoning=False, reasoning_effort="none"),
    "gpt-5.5-2026-04-23": dict(vendor="openai", provider="openai", api_id="gpt-5.5-2026-04-23",
                               cutoff="2025-12-01", price_in=5.0, price_out=30.0, context=1_050_000,
                               temperature=True, seed=True, open_weight=False, reasoning=False,
                               reasoning_effort="none"),
    "gpt-4.1-2025-04-14": dict(vendor="openai", provider="openai", api_id="gpt-4.1-2025-04-14",
                               cutoff="2024-06-01", price_in=2.0, price_out=8.0, context=1_050_000,
                               temperature=True, seed=True, open_weight=False, reasoning=False),
    "gpt-4o-2024-08-06": dict(vendor="openai", provider="openai", api_id="gpt-4o-2024-08-06",
                              cutoff="2023-10-01", price_in=2.5, price_out=10.0, context=128_000,
                              temperature=True, seed=True, open_weight=False, reasoning=False),
    "gemini-2.5-pro": dict(vendor="google", provider="google", api_id="gemini-2.5-pro",
                           cutoff="2025-01", price_in=1.25, price_out=10.0, context=1_000_000,
                           temperature=True, seed=True, open_weight=False, reasoning=False,
                           thinking=True),
    "gemini-2.5-flash-medium": dict(vendor="google", provider="openrouter", api_id="google/gemini-2.5-flash",
                                    host="google-ai-studio/flex", cutoff="2025-01", price_in=0.15,
                                    price_out=1.25, context=1_048_576, temperature=False, seed=True,
                                    open_weight=False, reasoning=True, reasoning_effort="medium",
                                    extra_tokens=8000),
    "Llama-3.3-70B-Instruct": dict(vendor="meta", provider="openrouter",
                                   api_id="meta-llama/llama-3.3-70b-instruct", host="akashml/fp8",
                                   cutoff="2023-12", weights_release="2024-12-06",
                                   price_in=0.20, price_out=0.52, context=131_072,
                                   temperature=True, seed=True, open_weight=True, reasoning=False),
    "gpt-oss-120b": dict(vendor="openai", provider="openrouter", api_id="openai/gpt-oss-120b",
                         host="coreweave/fp4", cutoff="2024-06-01", weights_release="2025-08-05",
                         price_in=0.03, price_out=0.17, context=131_072,
                         temperature=True, seed=True, open_weight=True, reasoning=False,
                         thinking=True),
    "gemma-4-31b-it": dict(vendor="google", provider="openrouter", api_id="google/gemma-4-31b-it",
                           host="together", cutoff="2025-01", weights_release="2026-03-31",
                           price_in=0.39, price_out=0.97, context=256_000,
                           temperature=True, seed=True, open_weight=True, reasoning=False),
    "deepseek-v3.2": dict(vendor="deepseek", provider="openrouter", api_id="deepseek/deepseek-v3.2",
                          host="baidu/fp8", cutoff=None, released="2025-12-01", weights_release="2025-12-01",
                          price_in=0.28, price_out=0.42, context=131_072,
                          temperature=True, seed=True, open_weight=True, reasoning=False, thinking_off=True),
    "deepseek-v3.1": dict(vendor="deepseek", provider="openrouter", api_id="deepseek/deepseek-chat-v3.1",
                          host="coreweave/fp8", cutoff=None, released="2025-08-21", weights_release="2025-08-21",
                          price_in=0.55, price_out=1.65, context=161_000,
                          temperature=True, seed=True, open_weight=True, reasoning=False, thinking_off=True),
    "qwen3-235b-a22b-2507": dict(vendor="alibaba", provider="openrouter", api_id="qwen/qwen3-235b-a22b-2507",
                                 host="parasail/fp8", cutoff=None, released="2025-07-21",
                                 weights_release="2025-07-21", price_in=0.14, price_out=0.80, context=131_072,
                                 temperature=True, seed=True, open_weight=True, reasoning=False),
    "kimi-k2-0711": dict(vendor="moonshot", provider="openrouter", api_id="moonshotai/kimi-k2",
                         host="novita/fp8", cutoff=None, released="2025-07-11", weights_release="2025-07-11",
                         price_in=0.57, price_out=2.30, context=131_072, json_mode=False,
                         resample_empty=True,
                         temperature=True, seed=True, open_weight=True, reasoning=False),
    "glm-4.5": dict(vendor="zhipu", provider="openrouter", api_id="z-ai/glm-4.5",
                    host="z-ai/fp8", cutoff=None, released="2025-07-28", weights_release="2025-07-28",
                    price_in=0.60, price_out=2.20, context=131_072,
                    temperature=True, seed=False, open_weight=True, reasoning=False, thinking_off=True),
    "Llama-4-Maverick-17B-128E-Instruct": dict(
        vendor="meta", provider="openrouter", api_id="meta-llama/llama-4-maverick",
        cutoff="2024-08", weights_release="2025-04-05", price_in=0.188, price_out=0.652,
        context=1_000_000, temperature=True, seed=True, open_weight=True, reasoning=False),
    "gpt-5.2-2025-12-11": dict(vendor="openai", provider="openai", api_id="gpt-5.2-2025-12-11",
                               cutoff="2025-08-31", price_in=1.75, price_out=14.0, context=400_000,
                               temperature=True, seed=True, open_weight=False, reasoning=False,
                               reasoning_effort="none"),
    "claude-opus-4-8": dict(vendor="anthropic", provider="anthropic", api_id="claude-opus-4-8",
                            cutoff="2026-01", price_in=5.0, price_out=25.0, context=1_000_000,
                            temperature=False, seed=False, open_weight=False, reasoning=False),
    "gpt-4o-mini-2024-07-18": dict(vendor="openai", provider="openai", api_id="gpt-4o-mini-2024-07-18",
                                   cutoff="2023-10-01", price_in=0.15, price_out=0.60, context=128_000,
                                   temperature=True, seed=True, open_weight=False, reasoning=False),
    "gpt-5.4-2026-03-05-medium": dict(vendor="openai", provider="openrouter", api_id="openai/gpt-5.4",
                                      host="openai/flex", cutoff="2025-08-31", price_in=1.25, price_out=7.5,
                                      context=1_050_000, temperature=False, seed=True, open_weight=False,
                                      reasoning=True, reasoning_effort="medium", extra_tokens=8000),
    "gpt-5.4-mini-medium": dict(vendor="openai", provider="openrouter", api_id="openai/gpt-5.4-mini",
                                host="openai/flex", cutoff="2025-08-31", price_in=0.375, price_out=2.25,
                                context=400_000, temperature=False, seed=True, open_weight=False,
                                reasoning=True, reasoning_effort="medium", extra_tokens=8000),
    "gpt-oss-120b-medium": dict(vendor="openai", provider="openrouter", api_id="openai/gpt-oss-120b",
                                host="coreweave/fp4", cutoff="2024-06-01", weights_release="2025-08-05",
                                price_in=0.03, price_out=0.17, context=131_072, temperature=False, seed=True,
                                open_weight=True, reasoning=True, reasoning_effort="medium", extra_tokens=8000,
                                json_mode=False),
    "deepseek-v3.1-thinking": dict(vendor="deepseek", provider="openrouter", api_id="deepseek/deepseek-chat-v3.1",
                                   host="coreweave/fp8", cutoff=None, released="2025-08-21",
                                   weights_release="2025-08-21", price_in=0.55, price_out=1.65, context=161_000,
                                   temperature=False, seed=True, open_weight=True, reasoning=True,
                                   thinking_on=True, extra_tokens=8000),
    "glm-4.5-air-thinking": dict(vendor="zhipu", provider="openrouter", api_id="z-ai/glm-4.5-air",
                                 host="z-ai/fp8", cutoff=None, released="2025-07-28", weights_release="2025-07-28",
                                 price_in=0.20, price_out=1.10, context=131_072, json_mode=False,
                                 temperature=False, seed=False, open_weight=True, reasoning=True,
                                 thinking_on=True, extra_tokens=8000,
                                 resample_empty=True),
    "claude-opus-4-6-medium": dict(vendor="anthropic", provider="gateway", api_id="claude-opus-4-6",
                                   cutoff="2025-08", price_in=5.0, price_out=25.0, context=1_000_000,
                                   temperature=False, seed=False, open_weight=False, reasoning=True,
                                   reasoning_effort="medium", extra_tokens=8000),
    "claude-haiku-4-5": dict(vendor="anthropic", provider="gateway", api_id="claude-haiku-4-5",
                             cutoff="2025-07", price_in=1.0, price_out=5.0, context=200_000,
                             temperature=True, seed=False, open_weight=False, reasoning=False,
                             extra_tokens=4096),
    "claude-haiku-4-5-medium": dict(vendor="anthropic", provider="gateway", api_id="claude-haiku-4-5",
                                    cutoff="2025-07", price_in=1.0, price_out=5.0, context=200_000,
                                    temperature=False, seed=False, open_weight=False, reasoning=True,
                                    reasoning_effort="medium", extra_tokens=8000),
    "claude-opus-5-5-medium": dict(vendor="anthropic", provider="gateway", api_id="claude-opus-5-5",
                                   cutoff="2026-06", price_in=4.0, price_out=20.0, context=1_000_000,
                                   temperature=False, seed=False, open_weight=False, reasoning=True,
                                   thinking_budget=4096, extra_tokens=4096, leakage_control=True),
    "gpt-6.1-sol-medium": dict(vendor="openai", provider="gateway", api_id="gpt-6.1-sol",
                               cutoff="2026-04-30", price_in=2.0, price_out=10.0, context=1_050_000,
                               temperature=False, seed=True, open_weight=False, reasoning=True,
                               reasoning_effort="medium", extra_tokens=8000, leakage_control=True),
    "stub": dict(vendor="none", provider="stub", api_id="stub", cutoff=None, price_in=0.0,
                 price_out=0.0, context=10_000_000, temperature=True, seed=True,
                 open_weight=False, reasoning=False),
}
PROPOSED = ["gpt-5.4-2026-03-05", "claude-opus-4-6", "gemini-2.5-pro", "Llama-3.3-70B-Instruct",
            "deepseek-v3.1", "qwen3-235b-a22b-2507", "kimi-k2-0711", "glm-4.5"]
BATCH_DISCOUNT = {"anthropic": 0.5}

CACHE = {
    "anthropic": dict(kind="explicit", read=0.10, write=1.25, min_tokens=1024, step=1),
    "openai": dict(kind="prefix", read=0.10, write=1.0, min_tokens=1024, step=128),
    "google": dict(kind="prefix", read=0.25, write=1.0, min_tokens=2048, step=1),
    "none": dict(kind=None, read=1.0, write=1.0, min_tokens=0, step=1),
}
ANTHROPIC_MIN = {"claude-opus-4-6": 4096, "claude-opus-4-5": 4096, "claude-haiku-4-5": 4096, "claude-opus-4-7": 2048,
                 "claude-opus-5": 512, "claude-fable-5": 512, "claude-fable-5-1": 512, "claude-opus-5-5": 512,
                 "claude-sonnet-5-5": 512, "claude-haiku-5-5": 512}
ANTHROPIC_READ = {"claude-opus-5-5": 0.05, "claude-fable-5-1": 0.025}


def cache_rule(model_id):
    """The CACHE entry of a model id (a MODELS key or a d6 id), with its model-specific minimum prefix and read multiple."""
    mid = model_id.split("/")[-1]
    spec = MODELS.get(model_id) or MODELS.get(mid) or {}
    prov = spec.get("provider")
    if (prov == "openrouter" and spec.get("open_weight", True)) or (not prov and "/" in model_id):
        return dict(CACHE["none"], vendor="none")
    if prov == "anthropic" or mid.startswith("claude-"):
        base = next((k for k in sorted(ANTHROPIC_MIN, key=len, reverse=True) if mid.startswith(k)), None)
        r = dict(CACHE["anthropic"], vendor="anthropic")
        if base:
            r["min_tokens"] = ANTHROPIC_MIN[base]
            r["read"] = ANTHROPIC_READ.get(base, r["read"])
        return r
    if prov == "openai" or mid.startswith(("gpt-", "o3", "o4")):
        r = dict(CACHE["openai"], vendor="openai")
        if mid.startswith("gpt-4o"):
            r["read"] = 0.5
        elif mid.startswith(("gpt-4.1", "o3")):
            r["read"] = 0.25
        return r
    if prov == "google" or mid.startswith("gemini-"):
        r = dict(CACHE["google"], vendor="google")
        if "flash" in mid:
            r["min_tokens"] = 1024
        if not mid.startswith("gemini-2"):
            r["read"] = 0.10
        return r
    return dict(CACHE["none"], vendor="none")
MIN_INTERVAL_S = {"anthropic": 0.2, "openai": 0.1, "google": 0.2, "openrouter": 0.2, "gateway": 0.2, "stub": 0.0}


def cutoff_date(text):
    """Last day covered by a stated cutoff ('YYYY-MM' is the month's end)."""
    if text is None:
        return None
    if len(text) == 7:
        y, m = int(text[:4]), int(text[5:])
        return dt.date(y, m, calendar.monthrange(y, m)[1])
    return dt.date.fromisoformat(text)


def bound_text(model_id):
    """The stated cutoff, else the release date (None for the stub)."""
    spec = MODELS[model_id]
    return spec["cutoff"] or spec.get("released")


def bound_date(model_id, rule="stated"):
    spec = MODELS[model_id]
    if rule == "weight_release":
        if not spec.get("open_weight"):
            raise ValueError(f"{model_id} has no weight release bound (closed weights)")
        return dt.date.fromisoformat(spec["weights_release"])
    return cutoff_date(bound_text(model_id))


def eligible(model_id, t0, rule="stated"):
    """(ok, testable_from_ts, reason)."""
    b = bound_date(model_id, rule)
    if b is None:
        return True, None, "stub (dry run only)"
    end = int(dt.datetime(b.year, b.month, b.day, tzinfo=dt.timezone.utc).timestamp()) + DAY
    first = end + LEAKAGE_MARGIN_D * DAY
    ok = int(t0) > first
    why = (f"{rule} bound {b.isoformat()} + {LEAKAGE_MARGIN_D} d = "
           f"{dt.datetime.fromtimestamp(first, dt.timezone.utc).date().isoformat()} "
           f"{'<' if ok else '>='} t0 {dt.datetime.fromtimestamp(int(t0), dt.timezone.utc).date().isoformat()}")
    return ok, first, why


def first_day(cutoff_text):
    """First UTC day whose 00:00 decision a model with this stated cutoff may make (the cutoff day's end plus 30 days is before that moment)."""
    b = cutoff_date(cutoff_text)
    end = int(dt.datetime(b.year, b.month, b.day, tzinfo=dt.timezone.utc).timestamp()) + DAY
    return (end + LEAKAGE_MARGIN_D * DAY) // DAY + 1


class LeakageError(RuntimeError):
    pass


def require_eligible(model_id, t0, rule="stated", allow_leakage=False):
    ok, _first, why = eligible(model_id, t0, rule)
    if not ok and not allow_leakage:
        raise LeakageError(f"{model_id} refused: {why}")
    return ok, why


def estimate_tokens(text):
    return int(len(text) / CHARS_PER_TOKEN + 0.5)


def cost_usd(model_id, tokens_in, tokens_out, batch=False, cache_read=0, cache_write=0):
    """tokens_in counts every input token, cached ones included; cache_read and cache_write are the parts read from and written to the cache."""
    spec = MODELS[model_id]
    rule = cache_rule(model_id)
    plain = tokens_in - cache_read - cache_write
    c = (plain * spec["price_in"] + cache_read * spec["price_in"] * rule["read"]
         + cache_write * spec["price_in"] * rule["write"] + tokens_out * spec["price_out"]) / 1e6
    if batch:
        c *= BATCH_DISCOUNT.get(spec["provider"], 1.0)
    return c


def sha256_text(s):
    return hashlib.sha256(s.encode()).hexdigest()


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


BLOB_MIN = 2000


def _swap_all(obj, table):
    """obj with every string that is a key of `table` replaced by its value."""
    if isinstance(obj, dict):
        return {k: _swap_all(v, table) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_swap_all(v, table) for v in obj]
    return table[obj] if isinstance(obj, str) and obj in table else obj


class Archive:
    """Content-addressed, gzip-compressed record of every request and response."""

    def __init__(self, label, root=ARCHIVE):
        self.root = pathlib.Path(root) / label
        self.count = 0
        self.bytes = 0

    def blob(self, text):
        sha = sha256_text(text)
        path = self.root / "blobs" / sha[:2] / f"{sha}.txt.gz"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
            with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as fh:
                fh.write(text)
            os.replace(tmp, path)
            self.bytes += path.stat().st_size
        return sha

    def put(self, purpose, record, messages=None):
        """Write a record; the parts of each user message in `messages` that are BLOB_MIN characters or longer go to the blob store."""
        swap = {}
        for msg in messages or []:
            parts = msg.get("parts") or []
            refs = [{"blob": self.blob(p)} if len(p) >= BLOB_MIN else {"text": p} for p in parts]
            joined = "".join(parts)
            if len(parts) > 1 or len(joined) >= BLOB_MIN:
                swap[joined] = {"$parts": refs}
            for p_, r in zip(parts, refs):
                if "blob" in r:
                    swap.setdefault(p_, {"$parts": [r]})
        if swap:
            record = dict(record)
            record["request"] = _swap_all(record["request"], swap)
        req_sha = record["request_sha256"]
        resp_sha = record["response_sha256"]
        path = self.root / purpose / f"{req_sha[:16]}__{resp_sha[:16]}.json.gz"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
            with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as fh:
                fh.write(canonical(record))
            os.replace(tmp, path)
            self.bytes += path.stat().st_size
        self.count += 1
        return path


def load_record(path):
    """An archived record with its request reassembled from the blobs; raises if the request does not hash to request_sha256."""
    path = pathlib.Path(path)
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        rec = json.load(fh)
    blobs = path.parent.parent / "blobs"

    def restore(o):
        if isinstance(o, dict):
            if set(o) == {"$parts"}:
                out = []
                for r in o["$parts"]:
                    if "text" in r:
                        out.append(r["text"])
                    else:
                        with gzip.open(blobs / r["blob"][:2] / f"{r['blob']}.txt.gz", "rt", encoding="utf-8") as fh:
                            out.append(fh.read())
                return "".join(out)
            return {k: restore(v) for k, v in o.items()}
        if isinstance(o, list):
            return [restore(v) for v in o]
        return o
    rec["request"] = restore(rec["request"])
    if sha256_text(canonical(rec["request"])) != rec["request_sha256"]:
        raise ValueError(f"{path}: reassembled request does not match request_sha256")
    return rec


def _empty_at_limit(d):
    """A response with no answer text."""
    ch = (d.get("choices") or [{}])[0]
    return not (ch.get("message", {}).get("content") or "").strip()


class Completion:
    def __init__(self, text, tokens_in, tokens_out, cost, path, request_sha, usage_source,
                 cache_read=0, cache_write=0):
        self.text = text
        self.tokens_in = tokens_in
        self.tokens_out = tokens_out
        self.cache_read = cache_read
        self.cache_write = cache_write
        self.cost = cost
        self.path = path
        self.request_sha = request_sha
        self.usage_source = usage_source
        self.reused = False


def user_text(msg):
    return "".join(msg["parts"])


class Client:
    """Base: builds the provider request, archives, retries."""

    provider = None

    def __init__(self, model_id, archive, batch=False):
        self.model_id = model_id
        self.spec = MODELS[model_id]
        self.archive = archive
        self.batch = batch
        self.calls = 0
        self.tokens_in = 0
        self.tokens_out = 0
        self.cache_read = 0
        self.cost = 0.0
        self._last = 0.0
        self.resume = os.environ.get("AUTOCURATOR_RESUME") == "1" and self.spec["provider"] != "stub"
        self.reused = 0

    def body(self, system, messages, max_tokens, json_schema, breaks=(), cache_key=None):
        raise NotImplementedError

    def send(self, body):
        """Return (raw response text, parsed text, usage dict or None); usage {"in"."""
        raise NotImplementedError

    def complete(self, system, user, max_tokens=2000, json_schema=None, purpose="misc",
                 stub=None, meta=None, parts=None):
        """One user message (`parts`, when given, must concatenate to it)."""
        if parts is not None and "".join(parts) != user:
            raise ValueError("parts do not concatenate to the user message")
        return self.chat(system, [{"role": "user", "parts": list(parts) if parts else [user]}], max_tokens,
                         json_schema, purpose, stub, meta)

    def chat(self, system, messages, max_tokens=2000, json_schema=None, purpose="misc", stub=None, meta=None,
             breaks=(), cache_key=None):
        """A conversation ending with a user message."""
        if not messages or messages[-1]["role"] != "user":
            raise ValueError("a conversation ends with a user message")
        if len(breaks) > 4:
            raise ValueError("at most 4 cache breakpoints")
        body = self.body(system, messages, max_tokens, json_schema, tuple(breaks), cache_key)
        req = {"provider": self.spec["provider"], "model": self.model_id, "body": body}
        if self.spec.get("endpoint"):
            req["endpoint"] = self.spec["endpoint"]
        req_sha = sha256_text(canonical(req))
        if self.resume:
            hit = self._archived(purpose, req_sha, (meta or {}).get("repeat", 1))
            if hit is not None:
                return hit
        started = time.time()
        raw, text, usage, attempts = self._with_retries(body, stub, meta)
        finished = time.time()
        prompt = system + "".join(user_text(m) if m["role"] == "user" else m["text"] for m in messages)
        if usage:
            t_in, t_out, src = usage["in"], usage["out"], "api"
            c_read, c_write = usage.get("cache_read", 0), usage.get("cache_write", 0)
        else:
            t_in = estimate_tokens(prompt)
            t_out = estimate_tokens(text)
            c_read = c_write = 0
            src = "estimated (characters / 4)"
        cost = cost_usd(self.model_id, t_in, t_out, self.batch, c_read, c_write)
        rec = {"request": req, "request_sha256": req_sha, "response_raw": raw,
               "response_text": text if text != raw else None, "response_sha256": sha256_text(raw),
               "started_utc": started, "finished_utc": finished, "attempts": attempts,
               "tokens_in": t_in, "tokens_out": t_out, "cache_read_tokens": c_read, "cache_write_tokens": c_write,
               "usage_source": src, "usage": usage, "cost_usd": cost, "batch": self.batch, "purpose": purpose,
               "meta": meta or {}, "dry_run": self.spec["provider"] == "stub"}
        path = self.archive.put(purpose, rec, [m for m in messages if m["role"] == "user"])
        self.calls += 1
        self.tokens_in += t_in
        self.tokens_out += t_out
        self.cache_read += c_read
        self.cost += cost
        return Completion(text, t_in, t_out, cost, path, req_sha, src, c_read, c_write)

    def _archived(self, purpose, req_sha, repeat):
        """The archived completion of this request and repeat, or None."""
        for path in sorted((self.archive.root / purpose).glob(f"{req_sha[:16]}__*.json.gz")):
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                rec = json.load(fh)
            if (rec["request_sha256"] != req_sha or rec.get("dry_run")
                    or (rec.get("meta") or {}).get("repeat", 1) != repeat):
                continue
            if self.spec.get("resample_empty") and _empty_at_limit(json.loads(rec["response_raw"])):
                continue
            text = rec["response_text"] if rec.get("response_text") is not None else rec["response_raw"]
            self.reused += 1
            self.calls += 1
            self.tokens_in += rec["tokens_in"]
            self.tokens_out += rec["tokens_out"]
            self.cache_read += rec["cache_read_tokens"]
            self.cost += rec["cost_usd"]
            c = Completion(text, rec["tokens_in"], rec["tokens_out"], rec["cost_usd"], path, req_sha,
                           rec["usage_source"], rec["cache_read_tokens"], rec["cache_write_tokens"])
            c.reused = True
            return c
        return None

    def _with_retries(self, body, stub, meta):
        wait = MIN_INTERVAL_S.get(self.spec["provider"], 0.2) - (time.time() - self._last)
        if wait > 0:
            time.sleep(wait)
        delay = 2.0
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._last = time.time()
            try:
                raw, text, usage = self.send(body) if stub is None else self.send_stub(stub, meta)
                return raw, text, usage, attempt
            except RetryableError as e:
                if attempt == MAX_ATTEMPTS:
                    raise
                time.sleep(min(e.retry_after or delay, MAX_WAIT_S))
                delay = min(delay * 2, MAX_WAIT_S)
        raise RuntimeError("unreachable")

    def send_stub(self, stub, meta):
        raise RuntimeError("a stub answer was supplied to a live client")


class RetryableError(RuntimeError):
    def __init__(self, msg, retry_after=None):
        super().__init__(msg)
        self.retry_after = retry_after


def _api_key(provider):
    from common import env
    key = env.get(KEY_NAMES[provider])
    if not key:
        raise RuntimeError(f"{KEY_NAMES[provider]} is not set in code/.env")
    return key


def _post(url, headers, body, timeout=600):
    import requests
    try:
        r = requests.post(url, headers=headers, data=json.dumps(body), timeout=timeout)
    except requests.RequestException as e:
        raise RetryableError(f"network: {type(e).__name__}") from None
    if r.status_code == 429 or r.status_code >= 500:
        ra = r.headers.get("retry-after")
        raise RetryableError(f"HTTP {r.status_code}", float(ra) if ra and ra.isdigit() else None)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")
    return r.text


def _flat(messages):
    """[(role, text)] with each user message's parts joined."""
    return [(m["role"], user_text(m) if m["role"] == "user" else m["text"]) for m in messages]


class StubClient(Client):
    """Dry run: the caller's stub answers; the record carries the cost the target model would have charged for the same prompt (estimated, without caching."""

    def __init__(self, archive, target=None):
        super().__init__("stub", archive)
        self.target = target

    def body(self, system, messages, max_tokens, json_schema, breaks=(), cache_key=None):
        return {"system": system, "messages": [{"role": r, "content": t} for r, t in _flat(messages)],
                "max_tokens": max_tokens, "json_schema": json_schema, "temperature": 0,
                "cache_breaks": [list(b) for b in breaks], "cache_key": cache_key}

    def send(self, body):
        raise RuntimeError("the stub client needs a stub answer")

    def send_stub(self, stub, meta):
        text = stub(meta)
        return text, text, None

    def chat(self, system, messages, max_tokens=2000, json_schema=None, purpose="misc", stub=None, meta=None,
             breaks=(), cache_key=None):
        if stub is None:
            raise RuntimeError("dry run: every call needs a deterministic stub")
        c = super().chat(system, messages, max_tokens, json_schema, purpose, stub, meta, breaks, cache_key)
        if self.target:
            c.target_cost = cost_usd(self.target, c.tokens_in, c.tokens_out)
        return c


class AnthropicClient(Client):
    URL = "https://api.anthropic.com/v1/messages"

    def body(self, system, messages, max_tokens, json_schema, breaks=(), cache_key=None):
        msgs = []
        for i, m in enumerate(messages):
            if m["role"] == "assistant":
                msgs.append({"role": "assistant", "content": m["text"]})
                continue
            blocks = []
            for j, p in enumerate(m["parts"]):
                if not p:
                    continue
                b = {"type": "text", "text": p}
                if (i, j) in breaks:
                    b["cache_control"] = {"type": "ephemeral"}
                blocks.append(b)
            msgs.append({"role": "user", "content": blocks})
        b = {"model": self.spec["api_id"], "max_tokens": max_tokens, "system": system, "messages": msgs}
        if self.spec.get("temperature"):
            b["temperature"] = 0
        return b

    def send(self, body):
        headers = {"x-api-key": _api_key("anthropic"), "anthropic-version": "2023-06-01",
                   "content-type": "application/json"}
        raw = _post(self.URL, headers, body)
        d = json.loads(raw)
        text = "".join(c.get("text", "") for c in d.get("content", []) if c.get("type") == "text")
        u = d.get("usage") or {}
        cr, cw = u.get("cache_read_input_tokens") or 0, u.get("cache_creation_input_tokens") or 0
        return raw, text, {"in": (u.get("input_tokens") or 0) + cr + cw, "out": u.get("output_tokens", 0),
                           "cache_read": cr, "cache_write": cw}


class OpenAIClient(Client):
    URL = "https://api.openai.com/v1/chat/completions"
    KEY = "openai"

    def body(self, system, messages, max_tokens, json_schema, breaks=(), cache_key=None):
        b = {"model": self.spec["api_id"],
             "messages": [{"role": "system", "content": system}]
             + [{"role": r, "content": t} for r, t in _flat(messages)],
             "max_completion_tokens": max_tokens}
        if cache_key:
            b["prompt_cache_key"] = cache_key
        if self.spec.get("reasoning_effort"):
            b["reasoning_effort"] = self.spec["reasoning_effort"]
        if self.spec.get("temperature") and not self.spec.get("reasoning"):
            b["temperature"] = 0
        if self.spec.get("seed"):
            b["seed"] = 20261009
        if json_schema is not None:
            b["response_format"] = {"type": "json_schema",
                                    "json_schema": {"name": "answer", "strict": True,
                                                    "schema": json_schema}}
        return b

    def headers(self):
        return {"Authorization": f"Bearer {_api_key(self.KEY)}", "content-type": "application/json"}

    def send(self, body):
        raw = _post(self.URL, self.headers(), body)
        d = json.loads(raw)
        if not d.get("choices"):
            raise RetryableError(f"no choices: {raw[:300]}")
        text = d["choices"][0]["message"].get("content") or ""
        if self.spec.get("resample_empty") and _empty_at_limit(d):
            raise RetryableError("empty answer", 0.1)
        u = d.get("usage") or {}
        det = u.get("prompt_tokens_details") or {}
        return raw, text, {"in": u.get("prompt_tokens", 0), "out": u.get("completion_tokens", 0),
                           "cache_read": det.get("cached_tokens") or 0,
                           "cache_write": det.get("cache_write_tokens") or 0}


class OpenRouterClient(OpenAIClient):
    """OpenAI-compatible; the host is pinned so the checkpoint stays fixed."""
    URL = "https://openrouter.ai/api/v1/chat/completions"
    KEY = "openrouter"

    def body(self, system, messages, max_tokens, json_schema, breaks=(), cache_key=None):
        b = super().body(system, messages, max_tokens, None)
        b.pop("max_completion_tokens", None)
        b["max_tokens"] = max_tokens
        if json_schema is not None and self.spec.get("json_mode", True):
            b["response_format"] = {"type": "json_object"}
        if self.spec.get("thinking_off"):
            b["reasoning"] = {"enabled": False}
        if self.spec.get("thinking_on"):
            b["reasoning"] = {"enabled": True}
        if self.spec.get("reasoning_effort") and self.spec["reasoning_effort"] != "none":
            b.pop("reasoning_effort", None)
            b["reasoning"] = {"effort": self.spec["reasoning_effort"]}
        b["max_tokens"] = max_tokens + self.spec.get("extra_tokens", 0)
        if self.spec.get("host"):
            b["provider"] = {"order": [self.spec["host"]], "allow_fallbacks": False}
        b["usage"] = {"include": True}
        return b


class GatewayClient(OpenAIClient):
    """An OpenAI-compatible Chat Completions endpoint for GPT and Claude models, named by MODEL_GATEWAY_URL; cost stays at the vendor's list price."""
    URL = os.environ.get("MODEL_GATEWAY_URL", "")
    KEY = "gateway"

    def headers(self):
        return {"Authorization": f"Bearer {_api_key(self.KEY)}", "content-type": "application/json"}

    def body(self, system, messages, max_tokens, json_schema, breaks=(), cache_key=None):
        if self.spec["vendor"] != "anthropic":
            b = super().body(system, messages, max_tokens, json_schema, breaks, cache_key)
            b.pop("prompt_cache_key", None)
            b["max_completion_tokens"] = max_tokens + self.spec.get("extra_tokens", 0)
            return b
        b = {"model": self.spec["api_id"],
             "messages": [{"role": "system", "content": system}]
             + [{"role": r, "content": t} for r, t in _flat(messages)],
             "max_tokens": max_tokens + self.spec.get("extra_tokens", 0)}
        if self.spec.get("reasoning_effort"):
            b["reasoning_effort"] = self.spec["reasoning_effort"]
        elif self.spec.get("thinking_budget"):
            b["thinking"] = {"type": "enabled", "budget_tokens": self.spec["thinking_budget"]}
        elif self.spec.get("temperature"):
            b["temperature"] = 0
        return b

    def send(self, body):
        raw, text, usage = super().send(body)
        if self.spec["vendor"] == "anthropic":
            usage["cache_write"] = max(usage["in"] - usage["cache_read"], 0)
        return raw, text, usage


class GoogleClient(Client):
    URL = "https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent"

    def body(self, system, messages, max_tokens, json_schema, breaks=(), cache_key=None):
        cfg = {"maxOutputTokens": max_tokens, "temperature": 0, "seed": 20261009}
        if json_schema is not None:
            cfg["responseMimeType"] = "application/json"
        return {"systemInstruction": {"parts": [{"text": system}]},
                "contents": [{"role": "model" if r == "assistant" else "user", "parts": [{"text": t}]}
                             for r, t in _flat(messages)],
                "generationConfig": cfg}

    def send(self, body):
        headers = {"x-goog-api-key": _api_key("google"), "content-type": "application/json"}
        raw = _post(self.URL.format(m=self.spec["api_id"]), headers, body)
        d = json.loads(raw)
        parts = ((d.get("candidates") or [{}])[0].get("content") or {}).get("parts") or []
        text = "".join(p.get("text", "") for p in parts)
        u = d.get("usageMetadata") or {}
        return raw, text, {"in": u.get("promptTokenCount", 0),
                           "out": u.get("candidatesTokenCount", 0) + u.get("thoughtsTokenCount", 0),
                           "cache_read": u.get("cachedContentTokenCount") or 0, "cache_write": 0}


CLIENTS = {"anthropic": AnthropicClient, "openai": OpenAIClient, "google": GoogleClient,
           "openrouter": OpenRouterClient, "gateway": GatewayClient}


def make_client(model_id, label, live=False, batch=False, target=None):
    """A stub client unless live is True."""
    archive = Archive(label)
    if not live:
        return StubClient(archive, target=target)
    if model_id == "stub":
        raise ValueError("the stub model has no live mode")
    spec = MODELS[model_id]
    return CLIENTS[spec.get("endpoint") or spec["provider"]](model_id, archive, batch=batch)


class BatchRunner:
    """Submit independent requests as one provider batch (Anthropic Message Batches, OpenAI Batch API), poll until done, return texts by custom id."""

    def __init__(self, client, poll_s=60):
        self.client = client
        self.poll_s = poll_s

    def run(self, items):
        """items: [(custom_id, system, user, max_tokens, json_schema)] -> {custom_id."""
        prov = self.client.spec["provider"]
        if prov == "anthropic":
            return self._anthropic(items)
        if prov == "openai":
            return self._openai(items)
        raise ValueError(f"no batch mode for provider {prov}")

    def _anthropic(self, items):
        import requests
        headers = {"x-api-key": _api_key("anthropic"), "anthropic-version": "2023-06-01",
                   "content-type": "application/json"}
        reqs = [{"custom_id": cid, "params": self.client.body(s, [{"role": "user", "parts": [u]}], mt, js)}
                for cid, s, u, mt, js in items]
        r = requests.post("https://api.anthropic.com/v1/messages/batches", headers=headers,
                          data=json.dumps({"requests": reqs}), timeout=600)
        r.raise_for_status()
        bid = r.json()["id"]
        while True:
            st = requests.get(f"https://api.anthropic.com/v1/messages/batches/{bid}",
                              headers=headers, timeout=120).json()
            if st.get("processing_status") == "ended":
                break
            time.sleep(self.poll_s)
        out = {}
        res = requests.get(st["results_url"], headers=headers, timeout=600)
        for line in res.text.splitlines():
            d = json.loads(line)
            m = (d.get("result") or {}).get("message") or {}
            text = "".join(c.get("text", "") for c in m.get("content", []) if c.get("type") == "text")
            u = m.get("usage") or {}
            out[d["custom_id"]] = (text, u.get("input_tokens", 0), u.get("output_tokens", 0))
        return out

    def _openai(self, items):
        import requests
        key = _api_key("openai")
        h = {"Authorization": f"Bearer {key}"}
        lines = [json.dumps({"custom_id": cid, "method": "POST", "url": "/v1/chat/completions",
                             "body": self.client.body(s, [{"role": "user", "parts": [u]}], mt, js)})
                 for cid, s, u, mt, js in items]
        up = requests.post("https://api.openai.com/v1/files", headers=h,
                           files={"file": ("batch.jsonl", "\n".join(lines))},
                           data={"purpose": "batch"}, timeout=600)
        up.raise_for_status()
        b = requests.post("https://api.openai.com/v1/batches", headers={**h, "content-type": "application/json"},
                          data=json.dumps({"input_file_id": up.json()["id"],
                                           "endpoint": "/v1/chat/completions",
                                           "completion_window": "24h"}), timeout=600)
        b.raise_for_status()
        bid = b.json()["id"]
        while True:
            st = requests.get(f"https://api.openai.com/v1/batches/{bid}", headers=h, timeout=120).json()
            if st["status"] in ("completed", "failed", "expired", "cancelled"):
                break
            time.sleep(self.poll_s)
        if st["status"] != "completed":
            raise RuntimeError(f"batch {bid} {st['status']}")
        res = requests.get(f"https://api.openai.com/v1/files/{st['output_file_id']}/content",
                           headers=h, timeout=600)
        out = {}
        for line in res.text.splitlines():
            d = json.loads(line)
            body = d["response"]["body"]
            text = body["choices"][0]["message"].get("content") or ""
            u = body.get("usage") or {}
            out[d["custom_id"]] = (text, u.get("prompt_tokens", 0), u.get("completion_tokens", 0))
        return out


_MONTHS = {m: i for i, m in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct",
                                         "Nov", "Dec"], 1)}


def _cutoff_text(cell):
    """'YYYY-MM-DD' or 'YYYY-MM' from a d6 cutoff cell, None when not stated."""
    c = cell.strip().strip("`")
    if not c or c.lower().startswith(("not", "n/a")):
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", c)
    if m:
        return m.group(1)
    m = re.match(r"([A-Z][a-z]{2}) (\d{4})", c)
    if m and m.group(1) in _MONTHS:
        return f"{m.group(2)}-{_MONTHS[m.group(1)]:02d}"
    return None


def d6_models(path=D6):
    """Every candidate model of d6 section 1."""
    text = pathlib.Path(path).read_text()
    sec = text[text.index("## 1. Candidate models"):text.index("## 2.")]
    out, table, head = [], None, None
    for line in sec.splitlines():
        if line.startswith("### "):
            table, head = line[4:].strip(), None
            continue
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if head is None:
            head = cells
            continue
        if set(cells[0]) <= set("-: "):
            continue
        r = dict(zip(head, cells))
        mid = cells[0].strip("`").strip()
        cut = next((r[k] for k in r if "cutoff" in k.lower()), "")
        price = next((r[k] for k in r if "price" in k.lower()), "")
        ctx = r.get("Context", "")
        pm = re.search(r"\$([\d.]+)\s*/\s*\$([\d.]+)", price)
        cm = re.search(r"([\d.]+)\s*([KM])", ctx)
        short = mid.split("/")[-1]
        key = mid if mid in MODELS else (short if short in MODELS else None)
        out.append({"id": mid, "table": table, "cutoff": _cutoff_text(cut), "cutoff_cell": cut,
                    "price_in": float(pm.group(1)) if pm else None, "price_out": float(pm.group(2)) if pm else None,
                    "context": int(float(cm.group(1)) * (1e3 if cm.group(2) == "K" else 1e6)) if cm else None,
                    "status": r.get("Status", ""), "key": key})
    return out


def earliest_cutoff_first_day(path=D6):
    """First day a model of d6 may act, for the model with the earliest stated cutoff."""
    days = [first_day(m["cutoff"]) for m in d6_models(path) if m["cutoff"]]
    return min(days)


def eligibility_table(events, rule="stated", models=None):
    """[(model, event_id, ok, reason)] for a list of (event_id, t0)."""
    out = []
    for m in models or PROPOSED:
        for eid, t0 in events:
            ok, _f, why = eligible(m, t0, rule)
            out.append((m, eid, ok, why))
    return out


if __name__ == "__main__":
    print(__doc__)
