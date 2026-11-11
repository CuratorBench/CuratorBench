"""Shared style for the paper figures."""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "code"))
from common import outroot

DATA_OUT = outroot.OUT_DATA
FIG_DIR = outroot.FIGURES

TEXT_IN = 395.82 / 72.27
COL_IN = 241.14 / 72.27
FONT_PT = 8
PREVIEW_DPI = 300

SURFACE = "#ffffff"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"

PROTOCOLS = ["morpho", "euler_v2", "moolah", "silo_v2"]
PROTOCOL_LABEL = {"morpho": "Morpho", "euler_v2": "Euler v2", "moolah": "Moolah",
                  "silo_v2": "Silo v2"}
PROTOCOL_COLOR = {"morpho": "#2a78d6",
                  "euler_v2": "#1baf7a",
                  "moolah": "#eda100",
                  "silo_v2": "#4a3aa7"}

EXPOSED = "#e34948"
NOT_EXPOSED = INK2
EVENT_TYPE_LABEL = {"depeg_loss": "Collateral failure", "lockup": "Lock-up"}
GROUP_LABEL = {"present_at_t0": "Present at t0", "exited_before_t0": "Exited before t0"}
GROUP_COLOR = {"present_at_t0": EXPOSED, "exited_before_t0": NOT_EXPOSED}

PROFILE = "#eb6834"

LIQUIDITY_CMAP_RANGE = (0.12, 0.88)


LOSS_CLASS_BOUNDS = [0.0, 0.01, 0.10, 0.50]
LOSS_CLASS_LABEL = ["0", "under 1%", "1–10%", "10–50%", "50% or more"]
LOSS_CLASS_COLOR = ["#86b6ef", "#3987e5", "#256abf", "#184f95", "#0d366b"]
NO_LOSS_MEASURED = GRID


def loss_class(x: float) -> int:
    """Index into LOSS_CLASS_* for a loss per dollar x >= 0."""
    if x <= 0:
        return 0
    if x < LOSS_CLASS_BOUNDS[1]:
        return 1
    if x < LOSS_CLASS_BOUNDS[2]:
        return 2
    if x < LOSS_CLASS_BOUNDS[3]:
        return 3
    return 4


def _lum(hex_color: str) -> float:
    h = hex_color.lstrip("#")
    rgb = [int(h[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    lin = [c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]


def ink_on(hex_fill: str) -> str:
    """Text colour for a label set inside a fill."""
    f = _lum(hex_fill)
    white = 1.05 / (f + 0.05)
    ink = (f + 0.05) / (_lum(INK) + 0.05)
    return "#ffffff" if white > ink else INK


COLLATERAL_DISPLAY = {"XUSD": "xUSD", "DEUSD": "deUSD", "MSUSD": "msUSD", "STETH": "stETH",
                      "RSETH": "rsETH", "PT-tUSDe-18DEC2025": "PT-tUSDe"}


CURATOR_SHORT = {"B.Protocol and Block Analitica": "B.Protocol", "Steakhouse Financial": "Steakhouse",
                 "RE7 Labs": "RE7"}


def curator_display(name: str) -> str:
    return CURATOR_SHORT.get(name, name)


EVENT_DISPLAY = {("census:defillama:197", "census:s2:RLP"): ("Resolv (RLP, USR)", "Resolv")}


def event_display(event, member_symbols=(), short=False) -> str:
    """Display name of an E2 event."""
    key = tuple(sorted(event.get("families") or []))
    if key in EVENT_DISPLAY:
        return EVENT_DISPLAY[key][1 if short else 0]
    return collateral_display(event["collateral_family"]["label"], member_symbols)


def collateral_display(label: str, member_symbols=()) -> str:
    if label in COLLATERAL_DISPLAY:
        return COLLATERAL_DISPLAY[label]
    if label.isupper():
        hits = {s for s in member_symbols if s.upper() == label}
        if len(hits) == 1:
            return hits.pop()
    return label


def setup():
    """matplotlib with the shared rc settings; returns pyplot."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
        "font.size": FONT_PT,
        "mathtext.fontset": "custom", "mathtext.rm": "Arial", "mathtext.it": "Arial:italic",
        "mathtext.bf": "Arial:bold", "mathtext.sf": "Arial", "mathtext.cal": "Arial",
        "axes.titlesize": FONT_PT,
        "axes.labelsize": FONT_PT,
        "xtick.labelsize": FONT_PT,
        "ytick.labelsize": FONT_PT,
        "legend.fontsize": FONT_PT,
        "axes.edgecolor": AXIS,
        "axes.linewidth": 0.6,
        "axes.labelcolor": INK2,
        "axes.titlecolor": INK,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "xtick.color": AXIS,
        "ytick.color": AXIS,
        "xtick.labelcolor": INK2,
        "ytick.labelcolor": INK2,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "xtick.major.size": 3,
        "ytick.major.size": 3,
        "xtick.minor.size": 1.5,
        "xtick.minor.width": 0.5,
        "grid.color": GRID,
        "grid.linewidth": 0.5,
        "grid.linestyle": "-",
        "lines.linewidth": 1.0,
        "lines.solid_capstyle": "round",
        "lines.solid_joinstyle": "round",
        "legend.frameon": False,
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "svg.fonttype": "none",
    })
    return plt


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _rel(p: Path) -> str:
    """Path as recorded in a sidecar."""
    p = Path(p)
    return str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p)


def save(fig, name: str, inputs: list[Path], what: str, extra: dict | None = None):
    """Write figures/<name>.pdf (vector), figures/<name>.png (300 dpi preview) and figures/<name>.pdf.meta.json with the sha256 of every input."""
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    pdf, png = FIG_DIR / f"{name}.pdf", FIG_DIR / f"{name}.png"
    fig.savefig(pdf, metadata={"CreationDate": None, "Creator": None, "Producer": None})
    fig.savefig(png, dpi=PREVIEW_DPI)
    side = {"generator": f"code/figures/{name}.py", "what": what,
            "written_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "inputs": {_rel(p): sha256(p) for p in inputs},
            "size_in": [round(v, 3) for v in fig.get_size_inches()]}
    if extra:
        side.update(extra)
    (FIG_DIR / f"{name}.pdf.meta.json").write_text(json.dumps(side, indent=2, sort_keys=True) + "\n")
    return pdf, png
