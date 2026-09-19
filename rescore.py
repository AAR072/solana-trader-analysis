#!/usr/bin/env python3.12
"""Split wallet scorecards into smart money, good traders, and launch bots."""
from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import Counter
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classify wallet_scorecards.csv without refetching history.")
    parser.add_argument("--input-dir", required=True, help="token output directory")
    return parser.parse_args()


def number(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def zscore(values):
    mean = statistics.mean(values); stdev = statistics.pstdev(values) or 1.0
    return lambda value: (value - mean) / stdev


def main() -> int:
    parsed = parse_args(); input_dir = Path(parsed.input_dir).expanduser().resolve()
    score_path = input_dir / "wallet_scorecards.csv"; traders_path = input_dir / "traders_all.csv"
    if not score_path.exists() or not traders_path.exists():
        print(f"error: expected {score_path} and {traders_path}", file=sys.stderr); return 2
    with score_path.open() as handle:
        rows = list(csv.DictReader(handle))
    with traders_path.open() as handle:
        token_rows = {row["wallet"]: row for row in csv.DictReader(handle)}
    for row in rows:
        for key in ("pnl_sol", "pnl_usd", "span_days", "win_rate", "profit_factor", "avg_size_sol", "median_mult", "p90_mult", "runner_rate", "flip_rate", "early_score"):
            row[key] = number(row.get(key))
        for key in ("n_tokens", "n_closed", "swaps"):
            row[key] = int(number(row.get(key)))
        token_row = token_rows.get(row["wallet"], {})
        row["token_flagged"] = int(any(token_row.get(key) == "1" for key in ("bundle", "sniped_60", "cheap_entry", "transfer_fed", "dev_recv_flag")))
        if row["n_tokens"] <= 2 and row["span_days"] < 1:
            row["cls"] = "burner"
        elif row["median_mult"] > 10 or row["p90_mult"] > 20 or (row["avg_size_sol"] < 0.1 and row["p90_mult"] > 5):
            row["cls"] = "serial_sniper"
        elif row["n_closed"] >= 3:
            row["cls"] = "trader"
        else:
            row["cls"] = "other"

    traders = [row for row in rows if row["cls"] == "trader"]
    if traders:
        z_pnl = zscore([row["pnl_sol"] for row in traders]); z_size = zscore([row["avg_size_sol"] for row in traders]); z_tokens = zscore([row["n_tokens"] for row in traders]); z_p90 = zscore([min(row["p90_mult"], 20) for row in traders]); z_early = zscore([row["early_score"] for row in traders]); z_wr = zscore([row["win_rate"] for row in traders]); z_pf = zscore([min(row["profit_factor"], 10) for row in traders]); z_closed = zscore([row["n_closed"] for row in traders]); z_flip = zscore([row["flip_rate"] for row in traders])
        for row in traders:
            row["SM"] = round(z_pnl(row["pnl_sol"]) + 0.9 * z_size(row["avg_size_sol"]) + 0.7 * z_tokens(row["n_tokens"]) + 0.6 * z_p90(min(row["p90_mult"], 20)) + 0.6 * z_early(row["early_score"]) + 0.3 * z_wr(row["win_rate"]), 2)
            row["GT"] = round(1.4 * z_wr(row["win_rate"]) + z_pf(min(row["profit_factor"], 10)) + 0.8 * z_closed(row["n_closed"]) - 0.5 * z_flip(row["flip_rate"]) + 0.5 * z_pnl(row["pnl_sol"]), 2)
    for row in rows:
        row.setdefault("SM", ""); row.setdefault("GT", "")

    smart = sorted([row for row in traders if not row["token_flagged"] and row["avg_size_sol"] >= 0.5 and row["n_tokens"] >= 5 and row["pnl_sol"] > 0], key=lambda row: -row["SM"])
    good = sorted([row for row in traders if not row["token_flagged"] and row["n_closed"] >= 5 and row["win_rate"] >= 0.5 and row["pnl_sol"] > 0], key=lambda row: -row["GT"])
    sniper = sorted([row for row in rows if row["cls"] == "serial_sniper"], key=lambda row: -row["pnl_sol"])
    elite = [row for row in smart if row["wallet"] in {candidate["wallet"] for candidate in good}]

    columns = ["wallet", "pnl_sol", "pnl_usd", "n_tokens", "n_closed", "swaps", "span_days", "win_rate", "profit_factor", "avg_size_sol", "median_mult", "p90_mult", "runner_rate", "flip_rate", "early_score", "cls", "burner", "token_flagged", "SM", "GT"]

    def dump(name: str, values: list[dict]) -> None:
        with (input_dir / name).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns); writer.writeheader()
            for row in values:
                writer.writerow({key: row.get(key, "") for key in columns})

    dump("smart_money.csv", smart); dump("good_traders.csv", good); dump("sniper_insiders.csv", sniper); dump("elite_both.csv", elite)
    print(f"classes={dict(Counter(row['cls'] for row in rows))}")
    print(f"smart_money={len(smart)} good_traders={len(good)} elite_both={len(elite)} serial_snipers={len(sniper)}")
    print(f"wrote scorecard views in {input_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
