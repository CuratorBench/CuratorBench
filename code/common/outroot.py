"""Output root of the analysis layer."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

_ENV = os.environ.get("CB_OUT")
OUT_DATA = Path(_ENV).resolve() if _ENV else DATA
FIGURES = OUT_DATA / "figures" if _ENV else ROOT / "figures"
