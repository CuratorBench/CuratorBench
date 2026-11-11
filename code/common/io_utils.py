"""Writers for data/ with provenance sidecars."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from . import outroot

DATA = Path(__file__).resolve().parents[2] / "data"
OUT = outroot.OUT_DATA


def _atomic_text(path: Path, text: str):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def _meta_sidecar(path: Path, generator: str, meta: dict):
    side = path.with_suffix(path.suffix + ".meta.json")
    _atomic_text(side, json.dumps({
        "generator": generator,
        "written_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **meta,
    }, indent=2, sort_keys=True) + "\n")


def write_jsonl(relpath: str, rows, generator: str, meta: dict) -> Path:
    path = OUT / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_text(path, "".join(json.dumps(row, sort_keys=True) + "\n"
                               for row in rows))
    _meta_sidecar(path, generator, {**meta, "rows": len(rows)})
    return path


def write_json(relpath: str, obj, generator: str, meta: dict) -> Path:
    path = OUT / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_text(path, json.dumps(obj, indent=2, sort_keys=True) + "\n")
    _meta_sidecar(path, generator, meta)
    return path


def read_jsonl(relpath: str):
    path = DATA / relpath
    with path.open() as fh:
        return [json.loads(line) for line in fh if line.strip()]
