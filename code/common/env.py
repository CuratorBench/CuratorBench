"""Load code/.env into a dict, independent of the caller's cwd."""
from pathlib import Path

_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
_cache = None


def _load():
    global _cache
    if _cache is None:
        got = {}
        for line in _ENV_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            got[k.strip()] = v.split("#")[0].strip() if "#" in v else v.strip()
        _cache = got
    return _cache


def get(key, default=None):
    val = _load().get(key, default)
    if val in ("", None):
        return default
    return val


def require(key):
    val = get(key)
    if val is None:
        raise KeyError(f"{key} missing from {_ENV_PATH}")
    return val
