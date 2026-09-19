#!/usr/bin/env python3.12
"""Reusable Solana memecoin trader-analysis pipeline.

Examples:
    export HELIUS_KEY='...'
    python3.12 run.py <MINT>
    python3.12 run.py <MINT> --scorecards --top 100
    python3.12 run.py <MINT> --metadata-only

The pipeline stores compact balance-change events, not raw transactions. Each
mint gets its own output directory, so a run can be resumed without mixing
tokens. PNL is realized FIFO PNL on the observed ledger; transfers and sells
whose earlier inventory is outside the fetch are treated as zero-basis and are
called out in summary.json.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

VERSION = "0.2.0"
WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
STABLES = {USDC, USDT}
BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
UA = {"User-Agent": "solana-trader-analysis/0.2", "Content-Type": "application/json"}
HERE = Path(__file__).resolve().parent


class PipelineError(RuntimeError):
    pass


def require_key() -> str:
    key = os.environ.get("HELIUS_KEY") or os.environ.get("HELIUS_API_KEY")
    if not key:
        raise PipelineError("Set HELIUS_KEY (or HELIUS_API_KEY) before running; no API key is stored in the code.")
    return key


class JsonClient:
    """Small stdlib-only HTTP client with shared pacing and retries."""

    def __init__(self, key: str, rps: float = 6.0):
        self.key = key
        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={key}"
        self.enhanced_url = f"https://api.helius.xyz/v0/transactions?api-key={key}"
        self.rps = max(0.2, float(rps))
        self._pace_lock = threading.Lock()
        self._next_request = 0.0

    def _pace(self) -> None:
        with self._pace_lock:
            now = time.monotonic()
            wait = max(0.0, self._next_request - now)
            self._next_request = max(now, self._next_request) + 1.0 / self.rps
        if wait:
            time.sleep(wait)

    @staticmethod
    def _retry_delay(attempt: int, retry_after: str | None = None) -> float:
        try:
            if retry_after:
                return min(60.0, max(1.0, float(retry_after)))
        except ValueError:
            pass
        return min(60.0, (2.0 ** attempt) + random.random())

    def post(self, url: str, payload: dict, timeout: int = 60, tries: int = 7):
        body = json.dumps(payload, separators=(",", ":")).encode()
        last: Exception | None = None
        for attempt in range(tries):
            self._pace()
            try:
                req = urllib.request.Request(url, data=body, headers=UA, method="POST")
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    value = json.loads(response.read())
                if isinstance(value, dict) and value.get("error"):
                    err = value["error"]
                    code = err.get("code") if isinstance(err, dict) else None
                    message = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                    raise PipelineError(f"RPC error {code}: {message}")
                return value
            except urllib.error.HTTPError as exc:
                last = exc
                if exc.code not in (408, 425, 429, 500, 502, 503, 504):
                    try:
                        detail = exc.read(200).decode("utf-8", "replace")
                    except Exception:
                        detail = ""
                    raise PipelineError(f"HTTP {exc.code} from {url.split('?')[0]} {detail}") from exc
                time.sleep(self._retry_delay(attempt, exc.headers.get("Retry-After")))
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last = exc
                time.sleep(self._retry_delay(attempt))
            except PipelineError as exc:
                # JSON-RPC throttling/temporary errors may arrive as HTTP 200.
                last = exc
                if "RPC error" not in str(exc):
                    raise
                time.sleep(self._retry_delay(attempt))
        raise PipelineError(f"request failed after {tries} attempts: {last}") from last

    def get(self, url: str, timeout: int = 45, tries: int = 6):
        last: Exception | None = None
        for attempt in range(tries):
            self._pace()
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA["User-Agent"]})
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    return json.loads(response.read())
            except urllib.error.HTTPError as exc:
                last = exc
                if exc.code not in (408, 425, 429, 500, 502, 503, 504):
                    return None
                time.sleep(self._retry_delay(attempt, exc.headers.get("Retry-After")))
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last = exc
                time.sleep(self._retry_delay(attempt))
        return None

    def rpc(self, method: str, params: list):
        response = self.post(self.rpc_url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        if not isinstance(response, dict) or "result" not in response:
            raise PipelineError(f"unexpected RPC response for {method}")
        return response["result"]


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def write_json(path: Path, value) -> None:
    atomic_write(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode())


def validate_mint(mint: str) -> str:
    mint = mint.strip()
    if not BASE58_RE.fullmatch(mint):
        raise PipelineError("mint does not look like a Solana base58 address (32–44 characters)")
    return mint


def choose_pair(pairs: list[dict], mint: str) -> dict | None:
    candidates = [
        pair for pair in pairs
        if pair.get("chainId") == "solana" and pair.get("baseToken", {}).get("address") == mint
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda pair: float((pair.get("liquidity") or {}).get("usd") or 0.0))


def dex_metadata(client: JsonClient, mint: str) -> dict:
    response = client.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}") or {}
    pairs = response.get("pairs") or []
    sol_pairs = [pair for pair in pairs if pair.get("chainId") == "solana"]
    pair = choose_pair(sol_pairs, mint)
    pools = sorted({pair.get("pairAddress") for pair in sol_pairs if pair.get("pairAddress")})
    if not pair:
        raise PipelineError("DexScreener returned no Solana pair for this mint")
    base = pair.get("baseToken", {})
    return {
        "symbol": base.get("symbol") or mint[:8], "name": base.get("name") or "",
        "pools": pools, "selected_pair": pair.get("pairAddress"), "dex": pair.get("dexId"),
        "price_native": float(pair["priceNative"]) if pair.get("priceNative") else None,
        "price_usd": float(pair["priceUsd"]) if pair.get("priceUsd") else None,
        "market_cap_usd": pair.get("marketCap") or pair.get("fdv"),
        "pair_created_at": pair.get("pairCreatedAt"),
        "pair_quote": (pair.get("quoteToken") or {}).get("address"),
    }


def sol_price_usd(client: JsonClient) -> float | None:
    response = client.get(f"https://api.dexscreener.com/latest/dex/tokens/{WSOL}") or {}
    candidates = []
    for pair in response.get("pairs") or []:
        if pair.get("chainId") != "solana" or pair.get("baseToken", {}).get("address") != WSOL:
            continue
        if pair.get("quoteToken", {}).get("address") not in STABLES or not pair.get("priceUsd"):
            continue
        candidates.append(pair)
    if not candidates:
        return None
    pair = max(candidates, key=lambda item: float((item.get("liquidity") or {}).get("usd") or 0.0))
    return float(pair["priceUsd"])


def token_info(client: JsonClient, mint: str, usd_sol_override: float | None) -> dict:
    dex = dex_metadata(client, mint)
    supply_response = client.rpc("getTokenSupply", [mint])
    try:
        value = supply_response["value"]
        supply = float(value["uiAmountString"])
        decimals = int(value["decimals"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PipelineError("getTokenSupply returned no usable supply") from exc
    if usd_sol_override is not None:
        usd_sol, price_source = float(usd_sol_override), "cli"
    elif dex.get("price_usd") and dex.get("price_native"):
        usd_sol, price_source = dex["price_usd"] / dex["price_native"], "selected_pair"
    else:
        usd_sol = sol_price_usd(client)
        price_source = "wsol_stable_pair" if usd_sol else None
    return {
        "mint": mint, "symbol": dex["symbol"], "name": dex["name"], "supply": supply,
        "decimals": decimals, "usd_sol": usd_sol, "usd_sol_source": price_source,
        "pools": dex["pools"], "selected_pair": dex["selected_pair"], "dex": dex["dex"],
        "price_native": dex["price_native"], "price_usd": dex["price_usd"],
        "market_cap_usd": dex["market_cap_usd"], "pair_created_at": dex["pair_created_at"],
    }


def get_signatures(client: JsonClient, mint: str, max_signatures: int | None) -> tuple[list[dict], bool]:
    found: dict[str, dict] = {}
    before = None
    complete = True
    while True:
        remaining = 1000 if max_signatures is None else max_signatures - len(found)
        if remaining <= 0:
            complete = False
            break
        params: list = [mint, {"limit": min(1000, remaining)}]
        if before:
            params[1]["before"] = before
        batch = client.rpc("getSignaturesForAddress", params) or []
        if not batch:
            break
        for item in batch:
            found[item["signature"]] = {
                "signature": item["signature"], "blockTime": item.get("blockTime"),
                "slot": item.get("slot"), "err": item.get("err"),
            }
        print(f"  signatures: {len(found)}", flush=True)
        before = batch[-1]["signature"]
        if len(batch) < min(1000, remaining):
            break
    ordered = sorted(found.values(), key=lambda item: (item.get("blockTime") or 0, item["signature"]))
    return ordered, complete


def event_from_tx(tx: dict, mint: str, usd_sol: float | None) -> list[dict]:
    if not tx or tx.get("transactionError"):
        return []
    sig, ts, slot = tx.get("signature"), tx.get("timestamp"), tx.get("slot")
    if not sig or ts is None:
        return []
    signer, source = tx.get("feePayer"), tx.get("source")
    token_delta = defaultdict(int); decimals: dict[str, int] = {}
    wsol_delta = defaultdict(int); stable_usd = defaultdict(float)
    native_delta = defaultdict(int); other_accounts: set[str] = set()
    for account in tx.get("accountData") or []:
        address = account.get("account")
        native = account.get("nativeBalanceChange") or 0
        if native:
            native_delta[address] += native
        for change in account.get("tokenBalanceChanges") or []:
            owner, change_mint = change.get("userAccount"), change.get("mint")
            try:
                amount = int(change["rawTokenAmount"]["tokenAmount"])
                dec = int(change["rawTokenAmount"]["decimals"])
            except (KeyError, TypeError, ValueError):
                continue
            if change_mint == mint:
                token_delta[owner] += amount; decimals[owner] = dec
            elif change_mint == WSOL:
                wsol_delta[owner] += amount
            elif change_mint in STABLES:
                stable_usd[owner] += amount / (10 ** dec)
            else:
                other_accounts.add(owner)
    events = []
    for wallet, amount in token_delta.items():
        if not amount:
            continue
        sol_lamports = native_delta[wallet] + wsol_delta[wallet]
        stable = stable_usd[wallet]
        quote_sol = sol_lamports / 1e9 + (stable / usd_sol if usd_sol else 0.0)
        if amount > 0 and quote_sol < 0:
            kind = "b"
        elif amount < 0 and quote_sol > 0:
            kind = "s"
        elif amount > 0:
            kind = "ti"
        else:
            kind = "to"
        events.append({
            "sig": sig, "ts": ts, "slot": slot, "sgn": signer, "w": wallet,
            "ven": amount, "dec": decimals.get(wallet, 0), "sol": sol_lamports,
            "stable_usd": stable, "k": kind, "o": source,
            "me": 1 if wallet == signer else 0, "other": 1 if wallet in other_accounts else 0,
        })
    return events


def clear_event_cache(outdir: Path) -> None:
    for filename in ("events.jsonl", "signatures.json", "fetch_manifest.json"):
        path = outdir / filename
        if path.exists():
            path.unlink()
    batch_dir = outdir / "event_batches"
    if batch_dir.exists():
        for path in batch_dir.glob("*.jsonl"):
            path.unlink()


def fetch_event_batches(client: JsonClient, mint: str, signatures: list[dict], usd_sol: float | None,
                       outdir: Path, jobs: int, max_batches: int | None) -> dict:
    batch_size = 100
    all_batches = [signatures[i:i + batch_size] for i in range(0, len(signatures), batch_size)]
    selected = all_batches if max_batches is None else all_batches[:max_batches]
    batch_dir = outdir / "event_batches"; batch_dir.mkdir(parents=True, exist_ok=True)
    todo = [(index, batch) for index, batch in enumerate(selected)
            if not (batch_dir / f"{index:06d}.jsonl").exists()]
    stats = {"batches_total": len(selected), "batches_done": 0, "null_transactions": 0,
             "missing_transactions": 0, "complete": max_batches is None}
    lock = threading.Lock()

    def work(item: tuple[int, list[dict]]) -> None:
        index, batch = item
        sigs = [entry["signature"] for entry in batch]
        response = client.post(client.enhanced_url, {"transactions": sigs})
        if not isinstance(response, list):
            raise PipelineError(f"enhanced endpoint returned non-list for batch {index}")
        by_sig = {tx.get("signature"): tx for tx in response if isinstance(tx, dict)}
        lines: list[str] = []; missing = 0; nulls = 0
        for sig in sigs:
            tx = by_sig.get(sig)
            if tx is None:
                missing += 1; continue
            if tx.get("transactionError"):
                nulls += 1; continue
            lines.extend(json.dumps(event) for event in event_from_tx(tx, mint, usd_sol))
        atomic_write(batch_dir / f"{index:06d}.jsonl", ("\n".join(lines) + ("\n" if lines else "")).encode())
        with lock:
            stats["batches_done"] += 1; stats["missing_transactions"] += missing; stats["null_transactions"] += nulls
            if stats["batches_done"] % 10 == 0 or stats["batches_done"] == len(todo):
                print(f"  event batches: {stats['batches_done']}/{len(todo)}", flush=True)

    if todo:
        with ThreadPoolExecutor(max_workers=max(1, jobs)) as executor:
            list(executor.map(work, todo))
    else:
        stats["batches_done"] = len(selected)
    stats["complete"] = max_batches is None and stats["batches_done"] == len(todo)
    event_lines: list[bytes] = []
    for index in range(len(selected)):
        path = batch_dir / f"{index:06d}.jsonl"
        if not path.exists():
            stats["complete"] = False; continue
        data = path.read_bytes()
        if data:
            event_lines.append(data)
    atomic_write(outdir / "events.jsonl", b"".join(event_lines))
    write_json(outdir / "fetch_manifest.json", stats)
    return stats


def event_quote(event: dict, usd_sol: float | None) -> float:
    stable = event.get("stable_usd")
    if stable is None:
        stable = (event.get("usdc") or 0) / 1e6
    return (event.get("sol") or 0) / 1e9 + (stable / usd_sol if usd_sol else 0.0)


def analyse(events: list[dict], token: dict, outdir: Path, pre_slots: int, insider_mc: float) -> tuple[list[dict], dict]:
    if not events:
        raise PipelineError("no compact trade events were produced")
    pools = set(token.get("pools") or [])
    sig_count = len({event.get("sig") for event in events})
    wallet_sigs: dict[str, set[str]] = defaultdict(set)
    for event in events:
        wallet_sigs[event["w"]].add(event["sig"])
    auto_pool_cutoff = max(30, int(sig_count * 0.30))
    auto_pools = {wallet for wallet, sigs in wallet_sigs.items() if len(sigs) > auto_pool_cutoff}
    excluded = pools | auto_pools
    usable = [event for event in events if event["w"] not in excluded]
    if not usable:
        raise PipelineError("all events were classified as pool/vault accounts")
    usable.sort(key=lambda event: (event["ts"], event.get("slot") or 0, event["sig"]))
    creation = usable[0]
    create_ts, create_slot, dev = creation["ts"], creation.get("slot") or 0, creation.get("sgn")
    last_ts = usable[-1]["ts"]; usd_sol = token.get("usd_sol"); supply = token.get("supply")
    wallets = defaultdict(lambda: {
        "inv": [], "realized": 0.0, "buy_v": 0.0, "sell_v": 0.0, "bought": 0.0, "sold": 0.0,
        "nb": 0, "ns": 0, "t_first": None, "last_sell": None, "transfers_in": 0.0,
        "dev_recv": 0.0, "hold_weight": 0.0, "hold_units": 0.0, "first_buy": None,
        "first_slot": None, "txs": set(), "unmatched_sell": 0.0,
    })
    for event in usable:
        state = wallets[event["w"]]; state["txs"].add(event["sig"])
        if state["t_first"] is None:
            state["t_first"] = event["ts"]
        amount = event["ven"] / (10 ** event.get("dec", token.get("decimals", 0))); quote = event_quote(event, usd_sol)
        if event["k"] == "b":
            cost = max(-quote, 0.0); state["inv"].append([amount, cost, event["ts"]])
            state["buy_v"] += cost; state["bought"] += amount; state["nb"] += 1
            if state["first_buy"] is None:
                state["first_buy"], state["first_slot"] = event["ts"], event.get("slot") or 0
        elif event["k"] == "ti":
            state["inv"].append([amount, 0.0, event["ts"]]); state["transfers_in"] += amount
            if event.get("sgn") == dev:
                state["dev_recv"] += amount
        elif event["k"] == "s":
            quantity, proceeds = -amount, max(quote, 0.0)
            if quantity <= 0:
                continue
            state["sell_v"] += proceeds; state["sold"] += quantity; state["ns"] += 1; state["last_sell"] = event["ts"]
            remain, realized = quantity, 0.0
            while remain > 1e-12 and state["inv"]:
                item = state["inv"][0]; take = min(item[0], remain); fraction = take / item[0] if item[0] else 1.0
                cost_part = item[1] * fraction; realized += proceeds * (take / quantity) - cost_part
                state["hold_weight"] += take * (event["ts"] - item[2]); state["hold_units"] += take
                item[0] -= take; item[1] -= cost_part; remain -= take
                if item[0] <= 1e-12:
                    state["inv"].pop(0)
            if remain > 1e-12:
                state["unmatched_sell"] += remain; realized += proceeds * (remain / quantity)
            state["realized"] += realized

    rows: list[dict] = []
    for wallet, state in wallets.items():
        bought, sold = state["bought"], state["sold"]; avg_buy = state["buy_v"] / bought if bought else 0.0; avg_sell = state["sell_v"] / sold if sold else 0.0
        first_buy_after = state["first_buy"] - create_ts if state["first_buy"] is not None else None
        entry_mc = avg_buy * supply * usd_sol if avg_buy and usd_sol and supply else None; exit_mc = avg_sell * supply * usd_sol if avg_sell and usd_sol and supply else None
        rows.append({
            "wallet": wallet, "pnl_sol": round(state["realized"], 6), "pnl_usd": round(state["realized"] * usd_sol, 2) if usd_sol else "",
            "roi": round(state["realized"] / state["buy_v"], 6) if state["buy_v"] else "", "buy_sol": round(state["buy_v"], 6), "sell_sol": round(state["sell_v"], 6),
            "notional_sol": round(state["buy_v"] + state["sell_v"], 6), "buys": state["nb"], "sells": state["ns"], "ntx": len(state["txs"]),
            "avg_buy_price_sol": avg_buy, "avg_sell_price_sol": avg_sell, "entry_mc_usd": round(entry_mc, 2) if entry_mc is not None else "", "exit_mc_usd": round(exit_mc, 2) if exit_mc is not None else "",
            "multiple": round(avg_sell / avg_buy, 6) if avg_buy and avg_sell else "", "avg_hold_sec": round(state["hold_weight"] / state["hold_units"], 2) if state["hold_units"] else "",
            "held_tokens": round(sum(item[0] for item in state["inv"]), 6), "unmatched_sell_tokens": round(state["unmatched_sell"], 6), "first_buy_after_create_s": first_buy_after,
            "is_dev": int(wallet == dev), "bundle": int(state["first_slot"] is not None and state["first_slot"] - create_slot <= pre_slots),
            "sniped_15": int(first_buy_after is not None and first_buy_after <= 15), "sniped_60": int(first_buy_after is not None and first_buy_after <= 60), "sniped_300": int(first_buy_after is not None and first_buy_after <= 300),
            "quickflip": int(state["t_first"] is not None and state["last_sell"] is not None and bought and sold / bought >= 0.5 and state["last_sell"] - state["t_first"] <= 300),
            "cheap_entry": int(entry_mc is not None and entry_mc < insider_mc), "transfer_fed": int(state["transfers_in"] > 0.5 * max(bought + state["transfers_in"], 1.0)), "dev_recv_flag": int(state["dev_recv"] > 0),
        })
    rows.sort(key=lambda row: row["pnl_sol"] or 0.0)
    rows.reverse()
    columns = ["rank", "wallet", "pnl_sol", "pnl_usd", "roi", "buy_sol", "sell_sol", "notional_sol", "buys", "sells", "ntx", "avg_buy_price_sol", "avg_sell_price_sol", "entry_mc_usd", "exit_mc_usd", "multiple", "avg_hold_sec", "held_tokens", "unmatched_sell_tokens", "first_buy_after_create_s", "is_dev", "bundle", "sniped_15", "sniped_60", "sniped_300", "quickflip", "cheap_entry", "transfer_fed", "dev_recv_flag"]

    def dump_csv(path: Path, values: list[dict]) -> None:
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns); writer.writeheader()
            for index, row in enumerate(values, 1):
                output = {column: row.get(column, "") for column in columns}; output["rank"] = index; writer.writerow(output)

    dump_csv(outdir / "traders_all.csv", rows)
    clean = [row for row in rows if not any(row[field] for field in ("is_dev", "bundle", "sniped_60", "quickflip", "cheap_entry", "transfer_fed", "dev_recv_flag"))]
    dump_csv(outdir / "traders_clean.csv", clean)
    cohorts = Counter()
    for row in rows:
        if row["is_dev"]: cohort = "dev"
        elif row["bundle"]: cohort = "bundle"
        elif row["sniped_60"] or row["cheap_entry"]: cohort = "sniper/insider"
        elif row["transfer_fed"] or row["dev_recv_flag"]: cohort = "transfer-fed"
        elif row["quickflip"]: cohort = "quickflip"
        else: cohort = "real trader"
        cohorts[cohort] += 1
    manifest = json.loads((outdir / "fetch_manifest.json").read_text()) if (outdir / "fetch_manifest.json").exists() else {}
    summary = {
        "pipeline_version": VERSION, "mint": token["mint"], "symbol": token["symbol"], "supply": supply, "decimals": token["decimals"], "usd_sol": usd_sol, "usd_sol_source": token.get("usd_sol_source"),
        "create_ts": create_ts, "last_ts": last_ts, "dev": dev, "wallets": len(rows), "pools_excluded": sorted(excluded), "cohorts": dict(cohorts),
        "profit_wallets": sum(row["pnl_sol"] > 0 for row in rows), "net_realized_sol": round(sum(row["pnl_sol"] for row in rows), 6), "event_count": len(events), "signature_count": sig_count,
        "fetch_complete": bool(manifest.get("complete", False)), "missing_transactions": manifest.get("missing_transactions", 0), "null_transactions": manifest.get("null_transactions", 0),
        "basis_policy": "FIFO; transfer-in inventory and sells beyond observed inventory are zero-basis.",
        "classification_notes": {"bundle": "first buy within pre_slots of the earliest observed event; heuristic, not proof of a Jito bundle", "sniper_60": "first buy within 60 seconds of the earliest observed event", "cheap_entry": "entry market cap below insider_mc USD"},
    }
    write_json(outdir / "summary.json", summary)
    return rows, summary


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main() -> int:
    parser = argparse.ArgumentParser(description="Rank Solana token traders by realized FIFO PNL and optionally profile their cross-token history.")
    parser.add_argument("mint", help="Solana token mint address"); parser.add_argument("--output-dir", help="output directory (default: ./tok_<mint-prefix>)")
    parser.add_argument("--top", type=positive_int, default=30, help="number of rows printed (CSV always contains all rows)"); parser.add_argument("--min-pnl", type=float, default=0.0, help="minimum realized PNL for printed rows"); parser.add_argument("--min-notional", type=float, default=0.0, help="minimum buy+sell notional in SOL for printed rows")
    parser.add_argument("--insider-mc", type=float, default=3000.0, help="entry market cap below this USD is flagged cheap_entry"); parser.add_argument("--pre-slots", type=positive_int, default=4, help="creation-slot window used for the bundle heuristic"); parser.add_argument("--usd-sol", type=float, help="override SOL/USD used for USDC conversion and market caps")
    parser.add_argument("--rps", type=float, default=6.0, help="shared request rate limit"); parser.add_argument("--jobs", type=positive_int, default=5, help="concurrent enhanced-API batch workers"); parser.add_argument("--max-signatures", type=positive_int, help="cap signatures for a bounded test/partial run"); parser.add_argument("--max-batches", type=positive_int, help="cap enhanced batches for a bounded test/partial run")
    parser.add_argument("--refresh", action="store_true", help="discard this mint's cached signatures/events and refetch"); parser.add_argument("--metadata-only", action="store_true", help="fetch and save metadata without signatures or transactions")
    parser.add_argument("--scorecards", action="store_true", help="profile qualifying winners across recent cross-token swap history"); parser.add_argument("--scorecard-threshold", type=float, default=0.3, help="minimum token PNL in SOL for scorecard candidates"); parser.add_argument("--scorecard-pages", type=positive_int, default=6, help="up to 100 swaps per wallet per page")
    args = parser.parse_args()
    try:
        mint = validate_mint(args.mint); key = require_key(); client = JsonClient(key, args.rps)
        outdir = Path(args.output_dir or f"tok_{mint[:8]}").expanduser().resolve(); outdir.mkdir(parents=True, exist_ok=True)
        if args.refresh: clear_event_cache(outdir)
        token = token_info(client, mint, args.usd_sol); write_json(outdir / "metadata.json", token)
        if token.get("usd_sol") is None: raise PipelineError("could not determine SOL/USD; rerun with --usd-sol <value>")
        print(f"== {token['symbol']} ({mint})\noutput={outdir}\nsupply={token['supply']:.0f} decimals={token['decimals']} SOL=${token['usd_sol']:.2f} pools={len(token['pools'])}", flush=True)
        if args.metadata_only:
            print(f"wrote {outdir / 'metadata.json'}"); return 0
        signatures_path = outdir / "signatures.json"; sig_complete = False
        if signatures_path.exists() and not args.refresh:
            cached = json.loads(signatures_path.read_text()); signatures, sig_complete = cached["signatures"], bool(cached.get("complete")); print(f"using cached signatures: {len(signatures)}", flush=True)
        else:
            print("fetching signatures...", flush=True); signatures, sig_complete = get_signatures(client, mint, args.max_signatures); atomic_write(signatures_path, json.dumps({"signatures": signatures, "complete": sig_complete}, indent=2).encode())
        if args.max_signatures and len(signatures) >= args.max_signatures: sig_complete = False
        if not (outdir / "events.jsonl").exists() or args.refresh:
            print(f"fetching enhanced events from {len(signatures)} signatures...", flush=True); fetch_event_batches(client, mint, signatures, token["usd_sol"], outdir, args.jobs, args.max_batches)
        else:
            print(f"using cached events: {outdir / 'events.jsonl'}", flush=True)
        events = [json.loads(line) for line in (outdir / "events.jsonl").read_text().splitlines() if line.strip()]
        rows, summary = analyse(events, token, outdir, args.pre_slots, args.insider_mc); summary["signature_count"] = len(signatures); summary["event_signature_count"] = len({event.get("sig") for event in events}); summary["signature_scan_complete"] = sig_complete; summary["signature_scan_limit"] = args.max_signatures; write_json(outdir / "summary.json", summary)
        eligible = [row for row in rows if row["pnl_sol"] >= args.min_pnl and row["notional_sol"] >= args.min_notional]
        print(f"\n=== {token['symbol']} realized PNL ===\nwallets={len(rows)} events={len(events)} net_realized={summary['net_realized_sol']:.4f} SOL profitable={summary['profit_wallets']}\ncohorts={summary['cohorts']}")
        for rank, row in enumerate(eligible[:args.top], 1):
            tags = "".join(tag for tag, flag in ((" dev", "is_dev"), (" bundle", "bundle"), (" sniper", "sniped_60"), (" cheap", "cheap_entry"), (" flip", "quickflip"), (" xfer-fed", "transfer_fed")) if row[flag]); multiple = f"x{row['multiple']:.2f}" if row["multiple"] != "" else f"held {row['held_tokens']:.0f}"; usd = f"${row['pnl_usd']:,.0f}" if row["pnl_usd"] != "" else "USD n/a"
            print(f"{rank:>3}. {row['wallet'][:10]}… {row['pnl_sol']:>9.3f} SOL ({usd:>10}) buy {row['buy_sol']:>7.3f} sell {row['sell_sol']:>7.3f} {multiple}{tags}")
        print(f"\nwrote {outdir / 'traders_all.csv'} and {outdir / 'traders_clean.csv'}")
        if args.scorecards:
            threshold = max(0.0, args.scorecard_threshold); scorecard_cmd = [sys.executable, str(HERE / "wallet_scorecard.py"), "--input-dir", str(outdir), "--threshold", str(threshold), "--max-pages", str(args.scorecard_pages), "--usd-sol", str(token["usd_sol"])]
            rescore_cmd = [sys.executable, str(HERE / "rescore.py"), "--input-dir", str(outdir)]; subprocess.run(scorecard_cmd, check=True); subprocess.run(rescore_cmd, check=True)
        return 0
    except PipelineError as exc:
        print(f"error: {exc}", file=sys.stderr); return 2
    except KeyboardInterrupt:
        print("interrupted; completed event batches remain cached", file=sys.stderr); return 130


if __name__ == "__main__":
    raise SystemExit(main())
