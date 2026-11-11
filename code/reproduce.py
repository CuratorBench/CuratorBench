# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Reproduction check of the analysis layer (E2 to E6 and the figures)."""
import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
COMPARED = ["events", "aum", "metrics/e4_depeg_only", "metrics/e4", "metrics/e5", "metrics/e6"]
FIGURE_SCRIPTS = ["fig2_aum_events", "fig_correlations", "fig4_frontier", "figA_event_matrix"]
TABLE_SCRIPTS = []
STEPS = {
    "e3": ["code/analysis/e3_aum.py"],
    "e2": ["code/analysis/e2_events.py"],
    "e4": ["code/analysis/e4_metrics.py --freeze-stuck 0", "code/analysis/e4_metrics.py"],
    "e5": ["code/analysis/e5_events.py", "code/analysis/e5_inflows.py", "code/analysis/e5_forecast.py"],
    "e6": ["code/analysis/e6_incentives.py", "code/analysis/e6_compat.py"],
    "fig": [f"code/figures/{n}.py" for n in FIGURE_SCRIPTS],
    "tab": [f"code/tables/{n}.py" for n in TABLE_SCRIPTS],
}
ORDER = ["e3", "e2", "e4", "e5", "e6", "fig", "tab"]
TIMESTAMP_KEYS = {"written_utc", "mtime_utc", "fetched_utc", "finished_utc"}
RUN_COUNTERS = {"defillama_requests", "rpc_requests_this_run", "moolah_rpc_requests_last_run",
                "dRPC_calls", "rpc_calls"}
csv.field_size_limit(1 << 30)


E3_ARCHIVE_CACHES = ("monthly_reread.json", "chain_sample.json", "owner_funded_reads.json")


def seed(out: Path, work: Path, e4_api: Path, e6_rec: Path, e3_cache: Path | None = None):
    """Copy the caches of network reads into the scratch run."""
    done = []
    if e3_cache is not None and e3_cache.is_dir():
        (work / "e3").mkdir(parents=True, exist_ok=True)
        for name in E3_ARCHIVE_CACHES:
            if (e3_cache / name).is_file():
                shutil.copy2(e3_cache / name, work / "e3" / name)
                done.append(str(e3_cache / name))
    ev = out / "events"
    ev.mkdir(parents=True, exist_ok=True)
    for name in ("prices_supplement.csv", "moolah_oracle_samples.csv"):
        for f in (name, name + ".meta.json"):
            shutil.copy2(DATA / "events" / f, ev / f)
        done.append(f"data/events/{name}")
    if e4_api.is_dir():
        dst = work / "e4" / "api"
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(e4_api, dst)
        done.append(f"{e4_api} ({sum(1 for _ in dst.iterdir())} files)")
    if e6_rec.is_file():
        (work / "e6").mkdir(parents=True, exist_ok=True)
        shutil.copy2(e6_rec, work / "e6" / "recipient_checks.json")
        done.append(str(e6_rec))
    return done


def run_steps(steps, out: Path, work: Path, logs: Path):
    env = {k: v for k, v in os.environ.items() if k not in ("E6_OUT", "E6_E4_DIR")}
    env.update({"CB_OUT": str(out), "E2_WORK": str(work / "e2"), "E3_AUM_CACHE": str(work / "e3"),
                "E4_WORK": str(work / "e4"), "E5_WORK": str(work / "e5"), "E6_WORK": str(work / "e6"),
                "PYTHONDONTWRITEBYTECODE": "1"})
    logs.mkdir(parents=True, exist_ok=True)
    timing = {}
    for step in steps:
        for script in STEPS[step]:
            argv = script.split()
            name = Path(argv[0]).stem + ("_depeg_only" if argv[1:] == ["--freeze-stuck", "0"] else "")
            t0 = time.time()
            print(f"[run] {script}", flush=True)
            with open(logs / f"{name}.log", "w") as fh:
                rc = subprocess.run(["uv", "run", "--quiet", *argv], cwd=ROOT, env=env,
                                    stdout=fh, stderr=subprocess.STDOUT).returncode
            timing[name] = round(time.time() - t0, 1)
            print(f"[run] {script}: exit {rc}, {timing[name]} s", flush=True)
            if rc != 0:
                sys.exit(f"{script} failed; see {logs / (name + '.log')}")
    return timing


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def num(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v else None


class NumDiff:
    """Largest absolute and relative difference over numeric field pairs, and a count of non-numeric differing fields by name."""

    def __init__(self):
        self.max_abs, self.max_rel, self.at = 0.0, 0.0, None
        self.text = {}

    def add(self, name, a, b):
        if a == b:
            return
        x, y = num(a), num(b)
        if x is not None and y is not None and not isinstance(a, bool):
            d = abs(x - y)
            r = d / max(abs(x), abs(y), 1e-300)
            if d > self.max_abs:
                self.max_abs, self.at = d, name
            self.max_rel = max(self.max_rel, r)
        else:
            self.text[name] = self.text.get(name, 0) + 1

    def summary(self):
        s = []
        if self.at is not None:
            s.append(f"max abs diff {self.max_abs:.3g} (at {self.at}), max rel {self.max_rel:.3g}")
        if self.text:
            s.append("non-numeric fields differ: " + ", ".join(
                f"{k} x{v}" for k, v in sorted(self.text.items(), key=lambda kv: -kv[1])[:6]))
        return "; ".join(s)


def walk_json(a, b, path, nd: NumDiff, diffs: list):
    """Collect (path, committed, reproduced) for every differing leaf; structural differences carry None values."""
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                diffs.append((f"{path}/{k} only in {'committed' if k in a else 'reproduced'}", None, None))
                continue
            walk_json(a[k], b[k], f"{path}/{k}", nd, diffs)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            diffs.append((f"{path} length {len(a)} vs {len(b)}", None, None))
        for i, (x, y) in enumerate(zip(a, b)):
            walk_json(x, y, f"{path}[{i}]", nd, diffs)
    elif a != b:
        diffs.append((path, a, b))
        leaf = path.rsplit("/", 1)[-1].split("[")[0] or path
        nd.add(leaf, a, b)


def is_sha(x) -> bool:
    return isinstance(x, str) and len(x) == 64 and all(c in "0123456789abcdef" for c in x)


def strip_keys(o, keys):
    if isinstance(o, dict):
        return {k: strip_keys(v, keys) for k, v in o.items() if k not in keys}
    if isinstance(o, list):
        return [strip_keys(v, keys) for v in o]
    return o


def normalize_paths(text: str, out: Path) -> str:
    """Write the scratch root and the repository's data/ both as data/."""
    for root in (str(out.resolve()), str(out), str(DATA)):
        text = text.replace(root + "/figures", "figures").replace(root, "data")
    return text.replace(str(ROOT) + "/", "")


def short(s, n=160):
    s = s.rstrip("\n")
    return s if len(s) <= n else s[:n] + "..."


def line_hash(line: str) -> int:
    return int.from_bytes(hashlib.blake2b(line.encode(), digest_size=16).digest(), "big")


def compare_lines(a: Path, b: Path, kind: str):
    """Line-wise comparison of two text files (CSV or JSONL)."""
    header = None
    na = nb = ndiff = 0
    first, dates = [], set()
    nd = NumDiff()
    sa, sb = 0, 0
    with open(a, newline="") as fa, open(b, newline="") as fb:
        while True:
            la, lb = fa.readline(), fb.readline()
            if not la and not lb:
                break
            if la:
                na += 1
                sa = (sa + line_hash(la)) % (1 << 128)
            if lb:
                nb += 1
                sb = (sb + line_hash(lb)) % (1 << 128)
            if kind == "csv" and header is None:
                header = next(csv.reader([la])) if la else []
            if la == lb:
                continue
            ndiff += 1
            if not la or not lb:
                if len(first) < 2:
                    first.append(f"line {na}: committed {short(la)} | reproduced {short(lb)}")
                continue
            if kind == "csv":
                ra, rb = next(csv.reader([la])), next(csv.reader([lb]))
                cols = [header[i] if header and i < len(header) else f"col{i}" for i in range(len(ra))]
                changed = []
                for i, (x, y) in enumerate(zip(ra, rb)):
                    nd.add(cols[i], x, y)
                    if x != y:
                        changed.append(f"{cols[i]} {short(x, 40)} -> {short(y, 40)}")
                for c in ("date", "day"):
                    if c in cols:
                        dates.add(ra[cols.index(c)])
                if len(first) < 2:
                    first.append(f"line {na} ({short(','.join(ra[:4]), 70)}): " + "; ".join(changed[:4]))
            else:
                try:
                    diffs = []
                    walk_json(json.loads(la), json.loads(lb), "", nd, diffs)
                    if len(first) < 2:
                        first.append(f"line {na}: " + "; ".join(
                            f"{d[0]} {short(str(d[1]), 40)} -> {short(str(d[2]), 40)}" for d in diffs[:4]))
                except ValueError:
                    nd.add("line", la, lb)
    det = [f"{na} vs {nb} lines, {ndiff} differ"]
    if na == nb and sa == sb and ndiff:
        det.append("same lines in a different order")
    if dates:
        det.append("dates of differing rows: " + ", ".join(sorted(dates)[:6]) + (" ..." if len(dates) > 6 else ""))
    s = nd.summary()
    if s:
        det.append(s)
    return det + first


def compare_json(a: Path, b: Path, out: Path, meta: bool):
    ja = json.loads(normalize_paths(a.read_text(), out) if meta else a.read_text())
    jb = json.loads(normalize_paths(b.read_text(), out) if meta else b.read_text())
    if meta:
        ja, jb = strip_keys(ja, TIMESTAMP_KEYS), strip_keys(jb, TIMESTAMP_KEYS)
    if ja == jb:
        return True, []
    nd, diffs = NumDiff(), []
    walk_json(ja, jb, "", nd, diffs)
    paths = [d[0] for d in diffs]
    det = [f"{len(paths)} fields differ: " + ", ".join(paths[:6]) + (" ..." if len(paths) > 6 else "")]
    if meta:
        hashes = [d for d in diffs if is_sha(d[1]) and is_sha(d[2])]
        counters = [d for d in diffs if d[0].rsplit("/", 1)[-1] in RUN_COUNTERS]
        if len(hashes) + len(counters) == len(diffs):
            det.append(f"only recorded input sha256 ({len(hashes)}) and network run counters "
                       f"({len(counters)}) differ")
    s = nd.summary()
    if s:
        det.append(s)
    return False, det


def figure_data(rel: str, out: Path, results: dict):
    """(data identical?, detail) for a PDF or PNG."""
    p = Path(rel)
    side_rel = str(p.with_suffix(".pdf")) + ".meta.json"
    sa, sb = ROOT / side_rel, reproduced_path(side_rel, out)
    if rel.startswith("figures/drafts/"):
        src = {"fig3_radar_draft": "data/metrics/e4/fig3_data.csv",
               "fig4_frontier_draft": "data/metrics/e4/fig4_data.csv"}[p.stem]
        r = results.get(src)
        return (r is not None and r["identical"]), f"data {src}: {'identical' if r and r['identical'] else 'differs'}"
    if not sa.exists() or not sb.exists():
        return False, "no sidecar"
    ma, mb = json.loads(sa.read_text()), json.loads(sb.read_text())
    if "inputs" in ma:
        ia = {normalize_paths(k, out): v for k, v in ma["inputs"].items()}
        ib = {normalize_paths(k, out): v for k, v in mb.get("inputs", {}).items()}
        diff = sorted(k for k in set(ia) | set(ib) if ia.get(k) != ib.get(k))
        return not diff, ("input sha256 identical (" + ", ".join(Path(k).name for k in sorted(ia)) + ")"
                          if not diff else "inputs differ: " + ", ".join(diff))
    if "data" in ma:
        d = ma["data"]
        src = d if d.startswith("data/") else str(Path(rel).parent.parent / d)
        r = results.get(src)
        ok = r is not None and r["identical"]
        return ok, f"data {src}: {'identical' if ok else 'differs'}"
    return False, "sidecar names no data"


def git_lines(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.split("\n")


def strip_creation_date(path: Path) -> bytes:
    return re.sub(rb"/CreationDate \(D:[^)]*\)", b"", path.read_bytes())


def committed_files():
    """The analysis outputs in the working copy."""
    dirs = [f"data/{d}" for d in COMPARED] + ["figures"]
    files = [f for f in git_lines("ls-files", "--", *dirs) if f]
    files += [f"tables/{n}.tex" for n in TABLE_SCRIPTS if (ROOT / f"tables/{n}.tex").exists()]
    files += [f for f in git_lines("ls-files", "--others", "--ignored", "--exclude-standard", "--", *dirs) if f]
    files += [f for f in git_lines("ls-files", "--others", "--exclude-standard", "--", *dirs) if f]
    return sorted(f for f in set(files) if not f.endswith(("README.md", ".tmp")) and "__pycache__" not in f)


def reproduced_path(rel: str, out: Path) -> Path:
    if rel.startswith("tables/"):
        return out / "tables" / rel.removeprefix("tables/")
    return out / rel.removeprefix("data/") if rel.startswith("data/") else out / rel


def tracked(rels):
    return {f for f in git_lines("ls-files", "--", *rels) if f}


def modified(rels):
    """Tracked files whose working copy differs from HEAD."""
    return {f for f in git_lines("diff", "--name-only", "HEAD", "--", *rels) if f}


def compare(out: Path):
    files = committed_files()
    in_git, changed = tracked(files), modified(files)
    ignored = {f for f in git_lines("ls-files", "--others", "--ignored", "--exclude-standard", "--",
                                    *[f"data/{d}" for d in COMPARED], "figures") if f}
    results = {}
    order = sorted(files, key=lambda f: (f.endswith((".pdf", ".png")), f.endswith(".meta.json"), f))
    for rel in order:
        a, b = ROOT / rel, reproduced_path(rel, out)
        r = {"file": rel, "tracked": rel in in_git, "ignored": rel in ignored, "identical": False,
             "result": "", "detail": [], "modified_since_head": rel in changed}
        results[rel] = r
        if not b.exists():
            r["result"] = "not produced"
            continue
        if rel.endswith(".meta.json"):
            same, det = compare_json(a, b, out, meta=True)
            r["identical"] = same
            r["result"] = "identical ignoring timestamps" if same else "differs"
            r["detail"] = det
            continue
        byte_same = sha256(a) == sha256(b)
        if rel.endswith((".pdf", ".png")):
            ok, det = figure_data(rel, out, results)
            r["identical"] = ok
            if byte_same:
                how = "bytes identical"
            elif rel.endswith(".pdf") and strip_creation_date(a) == strip_creation_date(b):
                how = "bytes differ only in /CreationDate"
            else:
                how = "bytes differ"
            r["result"] = ("data identical" if ok else "data differs") + "; " + how
            r["detail"] = [det]
            continue
        if byte_same:
            r["identical"], r["result"] = True, "identical"
            continue
        r["result"] = "differs"
        if rel.endswith(".csv"):
            r["detail"] = compare_lines(a, b, "csv")
        elif rel.endswith(".jsonl"):
            r["detail"] = compare_lines(a, b, "jsonl")
        elif rel.endswith(".json"):
            r["detail"] = compare_json(a, b, out, meta=False)[1]
    produced = set()
    for d in COMPARED:
        produced |= {"data/" + str(p.relative_to(out)) for p in (out / d).rglob("*") if p.is_file()}
    if (out / "figures").exists():
        produced |= {str(p.relative_to(out)) for p in (out / "figures").rglob("*") if p.is_file()}
    extra = sorted(p for p in produced - set(files) if not p.endswith(".tmp"))
    return [results[f] for f in files], extra


def report(rows, extra, seeded, timing, out: Path):
    lines = ["| file | in git | result | detail |", "|---|---|---|---|"]
    for r in rows:
        det = "<br>".join(x.replace("|", "\\|") for x in r["detail"])
        git = ("yes, modified since HEAD" if r["modified_since_head"] else "yes") if r["tracked"] \
            else "ignored" if r.get("ignored") else "untracked (new)"
        lines.append(f"| `{r['file']}` | {git} | {r['result']} | {det} |")
    n_same = sum(r["identical"] for r in rows)
    head = [f"Reproduction of the analysis layer into {out}", "",
            f"{n_same} of {len(rows)} committed files reproduce "
            "(byte for byte; sidecars ignoring timestamps; figures by their data).", ""]
    if seeded:
        head += ["Seeded caches of network reads: " + "; ".join(seeded), ""]
    if extra:
        head += ["Produced but not committed: " + ", ".join(extra), ""]
    if timing:
        head += ["Run time (s): " + ", ".join(f"{k} {v}" for k, v in timing.items()), ""]
    text = "\n".join(head + lines) + "\n"
    (out / "reproduce_report.md").write_text(text)
    (out / "reproduce_report.json").write_text(json.dumps(
        {"rows": rows, "extra": extra, "seeded": seeded, "timing": timing}, indent=2) + "\n")
    print(text)
    w = max(len(r["file"]) for r in rows)
    print(f"{'file'.ljust(w)}  result")
    for r in rows:
        print(f"{r['file'].ljust(w)}  {r['result']}")
        for d in r["detail"]:
            print(f"{' ' * w}    {d}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", required=True, help="scratch directory (outside the repository)")
    ap.add_argument("--steps", nargs="*", choices=ORDER, default=ORDER)
    ap.add_argument("--compare-only", action="store_true")
    ap.add_argument("--no-seed", action="store_true", help="do not copy caches of network reads")
    ap.add_argument("--e4-api-cache", default="/tmp/e4_metrics_work/api",
                    help="Morpho API answers E4 used (copied into the scratch E4_WORK)")
    ap.add_argument("--e6-recipient-cache", default="/tmp/e6_incentives_work/recipient_checks.json",
                    help="E6 validation's recipient Transfer-log checks (copied into the scratch E6_WORK)")
    ap.add_argument("--e3-cache", default=None,
                    help="an E3 cache directory ($E3_AUM_CACHE) whose archive reads at fixed blocks "
                         f"({', '.join(E3_ARCHIVE_CACHES)}) are copied into the scratch E3 cache; "
                         "DefiLlama and HyperSync stay live")
    a = ap.parse_args()
    base = Path(a.out).resolve()
    if base == ROOT or ROOT in base.parents:
        sys.exit("--out must lie outside the repository")
    out, work, logs = base / "data", base / "work", base / "logs"
    seeded, timing = [], {}
    if not a.compare_only:
        out.mkdir(parents=True, exist_ok=True)
        if not a.no_seed:
            seeded = seed(out, work, Path(a.e4_api_cache), Path(a.e6_recipient_cache),
                          Path(a.e3_cache) if a.e3_cache else None)
            (base / "seeded.json").write_text(json.dumps(seeded, indent=2) + "\n")
        timing = run_steps([s for s in ORDER if s in a.steps], out, work, logs)
        (base / "timing.json").write_text(json.dumps(timing, indent=2) + "\n")
    else:
        if (base / "seeded.json").exists():
            seeded = json.loads((base / "seeded.json").read_text())
        if (base / "timing.json").exists():
            timing = json.loads((base / "timing.json").read_text())
    rows, extra = compare(out)
    report(rows, extra, seeded, timing, out)


if __name__ == "__main__":
    main()
