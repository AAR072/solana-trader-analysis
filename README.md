# Solana Trader Analysis

A reusable, mint-agnostic toolkit for reconstructing Solana token trading
activity and ranking wallets by realized PNL. It can also profile the recent
cross-token history of profitable wallets to separate selection edge from
execution skill.

## What it does

- Discovers token metadata, supply, decimals, liquidity pools, and SOL/USD.
- Fetches transaction signatures and parses enhanced balance changes in batches.
- Stores compact event data rather than raw transactions.
- Builds per-wallet FIFO ledgers with realized PNL, entry/exit prices, market
  caps, multiples, holding time, and timing flags.
- Produces optional smart-money, good-trader, serial-sniper, burner, and
  overlap views from cross-token history.

## Quick start

Create a Helius API key, then keep it outside the repository:

```bash
export HELIUS_KEY='your-helius-key'
python3.12 run.py <TOKEN_MINT> --top 100
```

The default output directory is `tok_<mint-prefix>/`. To run the optional
cross-token wallet profiling stage:

```bash
python3.12 run.py <TOKEN_MINT> --scorecards
```

Use bounded runs to validate a mint before fetching its full history:

```bash
python3.12 run.py <TOKEN_MINT> --metadata-only
python3.12 run.py <TOKEN_MINT> --max-signatures 300 --max-batches 3 --jobs 2
```

Completed batches are cached and reused. Pass `--refresh` to deliberately
refetch a token.

## Outputs

Each token directory contains the ranked data and run metadata:

- `traders_all.csv` — every observed wallet ranked by realized FIFO PNL.
- `traders_clean.csv` — a filtered view excluding launch/bundle/sniper,
  quick-flip, transfer-fed, and other configured flags.
- `events.jsonl` and `event_batches/` — compact, resumable event cache.
- `metadata.json`, `signatures.json`, `fetch_manifest.json`, and `summary.json`.
- With `--scorecards`: `wallet_scorecards.csv`, `smart_money.csv`,
  `good_traders.csv`, `sniper_insiders.csv`, and `elite_both.csv`.

Generated token data is ignored by Git. Only the reusable source and
documentation should be committed.

## Method and limitations

PNL is realized FIFO PNL on the observed signatures. Transfer-in inventory and
sells whose earlier inventory is outside the fetched history are treated as
zero-basis and can overstate PNL when the scan is incomplete. Stablecoin quotes
are converted using the recorded SOL/USD input.

`bundle`, `sniper_60`, `cheap_entry`, `transfer_fed`, and related labels are
screening heuristics. They indicate timing or flow patterns; they do not prove
common ownership, a specific block-building service, or intent.

The cross-token scorecard is a candidate-ranking aid, not a global wallet
leaderboard. Its sample is seeded from profitable wallets in the selected
token and is limited to the configured recent swap history.

## Security

Never commit API keys or generated transaction data. Set `HELIUS_KEY` in the
shell or a secrets manager. The repository's `.gitignore` excludes local
secrets, caches, bytecode, and generated token outputs.
