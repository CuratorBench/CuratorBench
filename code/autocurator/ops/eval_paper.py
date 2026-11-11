# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "scipy>=1.11", "pycryptodome", "matplotlib>=3.8"]
# ///
"""evaluate.evaluate over the paper's labels."""
import sys, time, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import context as C, evaluate as E, models as M
import run as RUN


def main():
    O = Path(__file__).resolve().parent
    LLM = (O / "reaccount_labels.txt").read_text().split() + (O / "reaccount_labels_2.txt").read_text().split() \
        + ["deepseek-v3.1-ablate-no_docs", "deepseek-v3.1-aggressive-ablate-no_docs"]
    RULES = [f"{b}@common" for b in ("baseline-max_apy", "baseline-equal", "baseline-rule_based",
                                   "ablate-no_llm", "ablate-cheap_nlp")]
    labels = LLM + [l + "-impact" for l in LLM] + RULES + [b.replace("@", "-impact@") for b in RULES]
    models = sorted({E.label_model(l) for l in LLM})
    t = time.time()
    E.evaluate(RUN.stub_start_day(None), labels=set(labels), models=models, workers=int(sys.argv[1]) if len(sys.argv) > 1 else 8)
    print(f"{len(labels)} labels, {len(models)} models, {time.time() - t:,.0f} s")


if __name__ == "__main__":
    main()
