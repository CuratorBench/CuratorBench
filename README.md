# CuratorBench

Code and data of "Who Curates the Curators? Measuring and Automating the Curation of DeFi Lending Vaults".

Requirements: Python 3.11+. Figures need matplotlib and numpy. The pipeline and AutoCurator need [uv](https://docs.astral.sh/uv/) and API keys in `code/.env`. Run everything from this folder.

## Tables I to VI

    gunzip -k data/aum/vault_daily.csv.gz
    CB_TABLES=rebuilt python3 code/tables/build_all.py
    diff -r rebuilt tables

## Figures 1 to 8

    python3 code/figures/fig0_teaser.py          # Figure 1
    python3 code/figures/fig_overview.py         # Figure 2
    python3 code/figures/fig2_aum_events.py      # Figure 3
    python3 code/figures/fig4_frontier.py        # Figure 4
    python3 code/figures/fig_correlations.py     # Figure 5 (also pandas, scipy)
    python3 code/figures/figA_event_matrix.py    # Figure 6
    python3 code/figures/fig5_autocurator.py     # Figure 7
    python3 code/figures/figA_prompt_dialog.py   # Figure 8

## Events and metrics

Download the state history (31 archives and `SHA256SUMS`) from the release of this repository, then unpack it into `data/`:

    shasum -a 256 -c SHA256SUMS
    cat state-vaults-morpho-8453.tar.zst.part-* > state-vaults-morpho-8453.tar.zst
    for f in state-*.tar.zst; do zstd -dc "$f" | tar -x --strip-components=1 -C data; done

Rebuild the stress events, the metrics, the event outcomes and the incentive results, and compare them with the stored ones:

    uv run code/reproduce.py

## AutoCurator

    uv run code/autocurator/ops/eval_paper.py                                             # evaluate the recorded runs
    uv run code/corpus/crawl.py                                                           # fetch the documents
    uv run code/autocurator/run.py decide --live --model gpt-5.4-mini-medium --mandate default   # a new run

## License

Code: MIT. Data: CC BY 4.0.
