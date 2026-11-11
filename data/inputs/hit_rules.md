# Hit rules of the collateral-failure hit table

This file states the rules behind `hit_events.jsonl` (rule version R19), as `code/analysis/e2_events.py` uses them. The script applies the same rules to Moolah and records this file's hash in the provenance of `data/events/depeg_events.jsonl`.

## Indicators, per lending market

- I1: the accounting price of the collateral, which is the price the market's oracle reports, is at least 1.08 times its market price.
- I2: utilization is pinned.
- I3: the borrow rate is at its cap.
- I4: liquidations are absent.

A collateral failure needs I1 together with I2, I3 or I4, and the collateral's own market price below 0.97 of its peg.

## Hit (R19)

A hit is a market and a pinned window with a depeg window of the collateral's family and with I1 at some time between 30 days before the window starts and the window's end. A hit whose collateral never read below 0.97 of its peg is dropped.

## Events

Hits of one collateral family whose windows overlap or are less than 7 days apart merge into one event, across chains.

## Market ids in `hit_events.jsonl`

`morpho:<chain>:<market id>`, `euler:<chain>:<lending vault>:<collateral vault>`, `silo:<chain>:<silo config>`.
