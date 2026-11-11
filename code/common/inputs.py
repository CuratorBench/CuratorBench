"""Input tables that the event construction and three cross-checks read."""
import os
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
_ENV = os.environ.get("CB_INPUTS")
INPUTS = pathlib.Path(_ENV).resolve() if _ENV else ROOT / "data" / "inputs"


def rel(path) -> str:
    """The path as written into provenance fields."""
    return "data/inputs/" + pathlib.Path(path).resolve().relative_to(INPUTS.resolve()).as_posix()
