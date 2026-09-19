#!/usr/bin/env python3.12
"""Profile qualifying winners across recent cross-token swap history.

This is intentionally a separate, opt-in stage: the token PNL pass is fully
reproducible from its compact cache, while wallet history requires additional
Helius calls. It writes scorecards into the supplied token output directory.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from run import JsonClient, PipelineError, STABLES, USDC, USDT, WSOL, require_key

QUOTE = {WSOL, USDC, USDT}
HERE = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile token winners across recent cross-token swap history.")
    parser.add_argument("--input-dir", required=True, help="token output directory containing traders_all.csv")
    parser.add_argument("--threshold", type=float, default=0.3, help="minimum realized token PNL in SOL")
    parser.add_argument("--max-pages", type=int, default=6, help="up to 100 swaps per wallet per page")
    parser.add_argument("--jobs", type=int, default=6, help="concurrent wallet-history workers")
    parser.add_argument("--rps", type=float, default=4.0, help="shared history request rate limit")
    parser.add_argument("--usd-sol", type=float, help="SOL/USD override; otherwise read metadata/summary")
    return parser.parse_args()


def parse_wallet_event(tx: dict, wallet: str, usd_sol: float) -> dict | None:
    if tx.get("transactionError"):
        return None
    token_delta = defaultdict(int)
    decimals: dict[str, int] = {}
    wsol = 0; stable_usd = 0.0; native = 0
    for account in tx.get("accountData") or []:
        if account.get("account") == wallet:
            native += account.get("nativeBalanceChange", 0) or 0
        for change in account.get("tokenBalanceChanges") or []:
            if change.get("userAccount") != wallet:
                continue
            mint = change.get("mint")
            try:
                amount = int(change["rawTokenAmount"]["tokenAmount"])
                dec = int(change["rawTokenAmount"]["decimals"])
            except (KeyError, TypeError, ValueError):
                continue
            if mint == WSOL:
                wsol += amount
            elif mint in STABLES:
                stable_usd += amount / (10 ** dec)
            elif mint not in QUOTE:
                token_delta[mint] += amount; decimals[mint] = dec
    if not token_delta:
        return None
    mint = max(token_delta, key=lambda item: abs(token_delta[item]))
    amount = token_delta[mint]
    if amount == 0:
        return None
    return {
        "mint": mint, "ven": amount, "dec": decimals[mint],
        "sol": native + wsol, "stable_usd": stable_usd,
        "ts": tx.get("timestamp"), "slot": tx.get("slot"),
    }


def wallet_swaps(client: JsonClient, wallet: str, max_pages: int) -> list[dict]:
    swaps: list[dict] = []
    before = None
    for _ in range(max(1, max_pages)):
        url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={client.key}&type=SWAP&limit=100"
        if before:
            url += f"&before={before}"
        page = client.get(url, timeout=60)
        if not page or not isinstance(page, list):
            break
        swaps.extend(page)
        before = page[-1].get("signature")
        if len(page) < 100 or not before:
            break
    return swaps


def score_wallet(wallet: str, transactions: list[dict], usd_sol: float) -> dict | None:
    events = [event for event in (parse_wallet_event(tx, wallet, usd_sol) for tx in transactions) if event and event.get("ts") is not None]
    if not events:
        return None
    by_mint = defaultdict(list)
    for event in events:
        by_mint[event["mint"]].append(event)
    tokens = []
    for mint, token_events in by_mint.items():
        token_events.sort(key=lambda event: (event["ts"], event.get("slot") or 0))
        inventory = []; realized = 0.0; buy_value = 0.0; sell_value = 0.0; bought = 0.0; sold = 0.0
        fills = []; first_buy = None; hold_weight = 0.0; hold_units = 0.0
        for event in token_events:
            amount = event["ven"] / (10 ** event["dec"])
            quote = event["sol"] / 1e9 + event["stable_usd"] / usd_sol
            if amount > 0 and quote < 0:
                cost = -quote; inventory.append([amount, cost, event["ts"]]); buy_value += cost; bought += amount
                fills.append(cost / amount); first_buy = first_buy if first_buy is not None else event["ts"]
            elif amount < 0 and quote > 0:
                quantity = -amount; proceeds = quote; sell_value += proceeds; sold += quantity; remain = quantity
                fills.append(proceeds / quantity)
                while remain > 1e-12 and inventory:
                    item = inventory[0]; take = min(item[0], remain); fraction = take / item[0] if item[0] else 1.0
                    cost_part = item[1] * fraction; realized += proceeds * (take / quantity) - cost_part
                    hold_weight += take * (event["ts"] - item[2]); hold_units += take
                    item[0] -= take; item[1] -= cost_part; remain -= take
                    if item[0] <= 1e-12:
                        inventory.pop(0)
                if remain > 1e-12:
                    realized += proceeds * (remain / quantity)
        if not bought and not sold:
            continue
        avg_buy = buy_value / bought if bought else 0.0; avg_sell = sell_value / sold if sold else 0.0
        multiples = (avg_sell / avg_buy) if avg_buy and avg_sell else None
        early = None
        if fills and avg_buy:
            low, high = min(fills), max(fills)
            early = int(high > low and avg_buy <= low + 0.25 * (high - low))
        tokens.append({"mint": mint, "pnl": realized, "buy_v": buy_value, "sell_v": sell_value, "mult": multiples, "hold": hold_weight / hold_units if hold_units else None, "closed": sold > 0, "early": early, "size": buy_value})
    closed = [token for token in tokens if token["closed"]]
    if not tokens:
        return None
    wins = [token for token in closed if token["pnl"] > 0]; losses = [token for token in closed if token["pnl"] < 0]
    gross_wins = sum(token["pnl"] for token in wins); gross_losses = -sum(token["pnl"] for token in losses)
    profit_factor = gross_wins / gross_losses if gross_losses else (99.0 if gross_wins else 0.0)
    multiples = [token["mult"] for token in closed if token["mult"]]
    sizes = [token["size"] for token in tokens if token["size"] > 0]
    holds = [token["hold"] for token in closed if token["hold"] is not None]
    early_scores = [token["early"] for token in closed if token["early"] is not None]
    return {
        "wallet": wallet, "swaps": len(events), "n_tokens": len(tokens), "n_closed": len(closed),
        "span_days": round((max(event["ts"] for event in events) - min(event["ts"] for event in events)) / 86400, 1),
        "last_active": max(event["ts"] for event in events), "pnl_sol": round(sum(token["pnl"] for token in tokens), 6),
        "pnl_usd": round(sum(token["pnl"] for token in tokens) * usd_sol, 2), "win_rate": round(len(wins) / len(closed), 3) if closed else 0,
        "profit_factor": round(min(profit_factor, 99), 2), "avg_size_sol": round(statistics.mean(sizes), 6) if sizes else 0,
        "median_mult": round(statistics.median(multiples), 4) if multiples else 0, "p90_mult": round(sorted(multiples)[int(0.9 * (len(multiples) - 1))], 4) if multiples else 0,
        "flip_rate": round(sum(hold < 300 for hold in holds) / len(holds), 3) if holds else 0,
        "runner_rate": round(sum(mult >= 5 for mult in multiples) / len(multiples), 3) if multiples else 0,
        "early_score": round(statistics.mean(early_scores), 3) if early_scores else 0,
    }


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    traders_path = input_dir / "traders_all.csv"
    if not traders_path.exists():
        print(f"error: missing {traders_path}", file=sys.stderr); return 2
    if args.max_pages <= 0 or args.jobs <= 0:
        print("error: --max-pages and --jobs must be positive", file=sys.stderr); return 2
    try:
        key = require_key(); client = JsonClient(key, args.rps)
    except PipelineError as exc:
        print(f"error: {exc}", file=sys.stderr); return 2
    usd_sol = args.usd_sol
    if usd_sol is None:
        for metadata_name in ("metadata.json", "summary.json"):
            path = input_dir / metadata_name
            if path.exists():
                try:
                    usd_sol = float(json.loads(path.read_text()).get("usd_sol"))
                    if usd_sol > 0: break
                except (TypeError, ValueError, KeyError):
                    usd_sol = None
    if not usd_sol or not math.isfinite(usd_sol):
        print("error: provide --usd-sol or include usd_sol in metadata.json/summary.json", file=sys.stderr); return 2
    with traders_path.open() as handle:
        candidates = [row["wallet"] for row in csv.DictReader(handle) if row.get("is_dev") != "1" and float(row.get("pnl_sol") or 0) >= args.threshold]
    print(f"profiling {len(candidates)} wallets from {input_dir} (token PNL >= {args.threshold} SOL)...", flush=True)
    results: dict[str, dict] = {}; done = 0; lock = threading.Lock()

    def work(wallet: str):
        nonlocal done
        try:
            result = score_wallet(wallet, wallet_swaps(client, wallet, args.max_pages), usd_sol)
        except Exception as exc:
            result = None
            print(f"  warning: {wallet[:10]}… {exc}", file=sys.stderr)
        with lock:
            done += 1
            if result: results[wallet] = result
            if done % 20 == 0 or done == len(candidates): print(f"  {done}/{len(candidates)}", flush=True)

    with ThreadPoolExecutor(max_workers=args.jobs) as executor:
        list(executor.map(work, candidates))
    all_rows = list(results.values())
    for row in all_rows:
        row["burner"] = int(row["n_tokens"] <= 2 and row["span_days"] < 1)
    followable = [row for row in all_rows if not row["burner"] and row["n_closed"] >= 3]

    def zscore(values):
        mean = statistics.mean(values); stdev = statistics.pstdev(values) or 1.0
        return lambda value: (value - mean) / stdev

    if followable:
        z_pnl = zscore([row["pnl_sol"] for row in followable]); z_p90 = zscore([row["p90_mult"] for row in followable]); z_early = zscore([row["early_score"] for row in followable]); z_ntok = zscore([row["n_tokens"] for row in followable]); z_size = zscore([row["avg_size_sol"] for row in followable]); z_wr = zscore([row["win_rate"] for row in followable]); z_pf = zscore([min(row["profit_factor"], 10) for row in followable]); z_ncl = zscore([row["n_closed"] for row in followable]); z_med = zscore([min(row["median_mult"], 5) for row in followable]); z_flip = zscore([row["flip_rate"] for row in followable])
        for row in followable:
            row["smart_money_score"] = round(z_pnl(row["pnl_sol"]) + z_p90(row["p90_mult"]) + z_early(row["early_score"]) + 0.7 * z_ntok(row["n_tokens"]) + 0.5 * z_size(row["avg_size_sol"]) + 0.3 * z_wr(row["win_rate"]), 2)
            row["good_trader_score"] = round(1.2 * z_wr(row["win_rate"]) + z_pf(min(row["profit_factor"], 10)) + 0.8 * z_ncl(row["n_closed"]) + z_med(min(row["median_mult"], 5)) - 0.5 * z_flip(row["flip_rate"]) + 0.4 * z_pnl(row["pnl_sol"]), 2)
    for row in all_rows:
        row.setdefault("smart_money_score", ""); row.setdefault("good_trader_score", "")
    columns = ["wallet", "pnl_sol", "pnl_usd", "n_tokens", "n_closed", "swaps", "span_days", "win_rate", "profit_factor", "avg_size_sol", "median_mult", "p90_mult", "runner_rate", "flip_rate", "early_score", "burner", "smart_money_score", "good_trader_score"]

    def dump(name: str, values: list[dict]):
        with (input_dir / name).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns); writer.writeheader()
            for row in values: writer.writerow({column: row.get(column, "") for column in columns})

    dump("wallet_scorecards.csv", sorted(all_rows, key=lambda row: -row["pnl_sol"]))
    dump("smart_money.csv", sorted(followable, key=lambda row: -row.get("smart_money_score", -999)))
    dump("good_traders.csv", sorted(followable, key=lambda row: -row.get("good_trader_score", -999)))
    print(f"profiled {len(all_rows)} | followable {len(followable)} | burners {sum(row['burner'] for row in all_rows)}", flush=True)
    print(f"wrote {input_dir / 'wallet_scorecards.csv'}, smart_money.csv, good_traders.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
