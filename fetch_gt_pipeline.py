#!/usr/bin/env python3
"""Discover recent new tokens from GeckoTerminal + enrich with real OHLCV → Desk CSV.

Pipeline:
  1. GT /networks/solana/new_pools  (multi-page, each page = 20 pools)
  2. Filter: skip SOL/USDC quote, skip liq < min_liq_usd, skip dups
  3. GT per-pool /ohlcv/minute  (200 candles, rate-limited)
  4. Derive true_multiple_path + liq_growth from real candles
  5. Emit CSV with same columns as fetch_dexscreener.py

Pure stdlib. GT free tier = 30 rpm → default 28 rpm to be safe.

Usage:
  python fetch_gt_pipeline.py --out gt_new_sol.csv --pages 5 --min-liq 1000
  python fetch_gt_pipeline.py --out gt_new_sol.csv --pages 10 --min-liq 500
  python fetch_gt_pipeline.py --out gt_new_sol.csv --pages 30 --min-liq 300  # catch more
"""

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# ── Shared rate limiter ─────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, rpm: int):
        self.min_interval = 60.0 / rpm
        self._next_ok = 0.0

    def wait(self):
        now = time.monotonic()
        if now < self._next_ok:
            time.sleep(self._next_ok - now)
        self._next_ok = time.monotonic() + self.min_interval


GT_BASE = "https://api.geckoterminal.com/api/v2"
GT_HEADERS = {"User-Agent": "rh_trencher-gt/0.1", "accept": "application/json"}


def gt_get(path: str, max_retries: int = 3) -> Any:
    """GET GT endpoint, return parsed JSON or None on failure."""
    url = f"{GT_BASE}{path}"
    backoff = 1.5
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=GT_HEADERS)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(backoff)
                backoff *= 2
                continue
            if e.code == 404:
                return None
            time.sleep(backoff)
            backoff *= 2
        except Exception:
            time.sleep(backoff)
            backoff *= 2
    return None


# ── Step 1: discover new pools ─────────────────────────────────────────────
def discover_new_pools(chain: str, pages: int, limiter: RateLimiter) -> list[dict]:
    """Fetch new_pools from GT, flatten into clean pool dicts."""
    pools = []
    seen = set()

    for page in range(1, pages + 1):
        limiter.wait()
        data = gt_get(f"/networks/{chain}/new_pools?page={page}")
        if not data or "data" not in data:
            print(f"  page {page}: empty or failed, stopping")
            break
        batch = data["data"]
        print(f"  page {page}: got {len(batch)} pools")

        for item in batch:
            a = item.get("attributes", {})
            rels = item.get("relationships", {})
            addr = a.get("address", "")
            if addr in seen or not addr:
                continue
            seen.add(addr)

            # ── Token addresses from relationships (format: "solana_<mint_address>") ──
            base_rel = rels.get("base_token", {}).get("data", {}) or {}
            quote_rel = rels.get("quote_token", {}).get("data", {}) or {}
            base_full_id = base_rel.get("id", "")
            quote_full_id = quote_rel.get("id", "")
            base_address = base_full_id.split("_", 1)[1] if "_" in base_full_id else base_full_id
            quote_address = quote_full_id.split("_", 1)[1] if "_" in quote_full_id else quote_full_id

            # ── Dex from relationships ──
            dex_id = (rels.get("dex", {}).get("data", {}) or {}).get("id", "")

            # ── Symbol from name: "TOAD / SOL" or "OTC->UP / SOL" ──
            name = a.get("name", "")
            if " / " in name:
                parts = name.split(" / ", 1)
                base_symbol = parts[0].strip().replace("-", "_").replace(">", "_")
                quote_symbol = parts[1].strip()
            else:
                base_symbol = name.strip().replace("-", "_")
                quote_symbol = ""

            pool = {
                "pair_address": addr,
                "dex_id": dex_id,
                "name": name,
                "reserve_in_usd": float(a.get("reserve_in_usd") or 0),
                "volume_24h": float((a.get("volume_usd") or {}).get("h24", 0) if isinstance(a.get("volume_usd"), dict) else 0),
                "price_change": a.get("price_change_percentage", {}),
                "base_symbol": base_symbol,
                "base_address": base_address,
                "quote_symbol": quote_symbol,
                "quote_address": quote_address,
                "created_at": a.get("pool_created_at", ""),
            }
            pools.append(pool)

    return pools


def filter_pools(pools: list[dict], min_liq_usd: float = 500,
                 skip_quotes: set[str] = frozenset({"SOL", "USDC", "WSOL", "BONK", "JUP", "JTO"})) -> list[dict]:
    """Apply quality filters."""
    out = []
    for p in pools:
        # Skip if base token IS a known major (not a meme coin)
        # Actually skip only if quote is SOL/USDC — base is the token we want
        quote = p["quote_symbol"].upper()
        if quote in skip_quotes or quote == "":
            # Actually SOL/USDC as quote is FINE — we're buying the base token with SOL
            # Skip only if base is SOL/USDC
            pass
        base = p["base_symbol"].upper()
        if base in ("SOL", "USDC", "WSOL"):
            continue
        if p["reserve_in_usd"] < min_liq_usd:
            continue
        out.append(p)
    return out


# ── Step 2: fetch OHLCV + derive path/growth ────────────────────────────────
def fetch_ohlcv(pool_address: str, chain: str, limiter: RateLimiter,
                limit: int = 200) -> list[list[float]]:
    """Fetch 1-minute candles, ascending."""
    limiter.wait()
    data = gt_get(f"/networks/{chain}/pools/{pool_address}/ohlcv/minute?limit={limit}&currency=usd&token=base")
    if not data:
        return []
    cs = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
    cs.reverse()
    return cs


def derive_path_growth(candles: list[list[float]], marks: int = 5,
                       warmup_min: int = 3, mark_step_min: int = 6
                       ) -> tuple[list[float], float]:
    """Same logic as enrich_ohlcv.candles_to_path_and_growth."""
    if not candles:
        return [], 0.0
    closes = [c[4] for c in candles]
    n = len(closes)
    # growth
    first = closes[0] if closes[0] > 0 else 1e-9
    last = closes[-1] if closes[-1] > 0 else first
    growth = round(last / first, 4) if first > 0 else 1.0
    # path
    step_idxs = [warmup_min + j * mark_step_min for j in range(marks)]
    baseline_idx = min(warmup_min, n - 1)
    baseline = closes[baseline_idx] if closes[baseline_idx] > 0 else (closes[0] if closes[0] > 0 else 1e-9)
    path = []
    for idx in step_idxs:
        real_idx = min(idx, n - 1)
        price = closes[real_idx]
        if price <= 0:
            price = closes[max(0, real_idx - 1)]
        path.append(round(price / baseline, 4))
    return path, growth


# ── Step 3: assemble CSV row ───────────────────────────────────────────────
ETH_USD = 2300.0  # rough, for liq_eth conversion


def infer_theme(ticker: str, name: str) -> str:
    """Best-effort theme inference from ticker/name for Desk rebuild.

    Returns a short label that NarrativeEngine can match (or '').
    """
    text = f"{ticker} {name}".lower()
    # Order matters: more specific first
    pet_kw = ["dog", "inu", "kitty", "cat", "kitten", "shiba", "pepe", "frog",
              "horse", "bull", "bear", "whale", "wolf", "fox", "lion", "tiger",
              "panda", "koala", "pig", "cow", "chicken", "monkey", "ape", "pepe",
              "meme", "floork", "flork", "bonk", "wif", "bark"]
    ai_kw = ["ai", "gpt", "llm", "claude", "bot", "agent", "neural", "model",
             "deep", "machine", "intelligence", "sora", "gemini", "mistral",
             "perplexity", "hunter", "runway"]
    politics_kw = ["trump", "maga", "biden", "obama", "politics", "war", "usa",
                  "america", "president", "congress", "election", "vote"]
    porn_kw = ["porn", "sex", "hentai", "nsfw", "xxx", "nude", "adult", "paedo"]
    crime_kw = ["cartel", "mafia", "gang", "drug", "cocaine", "weed", "cannabis",
                "heroin", "meth", "crime", "heist"]
    # hood/underground = crime + porn + subversive
    if any(k in text for k in crime_kw) or any(k in text for k in porn_kw):
        return "hood"
    if any(k in text for k in politics_kw):
        return "political"
    if any(k in text for k in ai_kw):
        return "ai-craze"
    if any(k in text for k in pet_kw):
        return "pet"
    return ""


def pool_to_row(pool: dict, t_min: int, path: list[float], growth: float,
                candles_count: int) -> dict[str, Any]:
    """Map discovered pool → Desk-compatible CSV row."""
    liq_usd = pool["reserve_in_usd"]
    liquidity_eth = liq_usd / ETH_USD

    if not path:
        # Fallback: flat 1.0 path (no price info)
        path = [1.0] * 5

    theme_hint = infer_theme(pool.get("base_symbol", ""), pool.get("name", ""))

    return {
        "t_min": int(t_min),
        "ticker": pool["base_symbol"] or "?",
        "name": pool["name"] or pool["base_symbol"] or "?",
        "description": "",  # GT doesn't give desc
        "launchpad": pool["dex_id"],
        "liquidity_eth": round(liquidity_eth, 4),
        "liq_growth": str(growth),
        "deployer": "",
        "holders": "[]",
        "linked_groups": "[]",
        "selling_linked": 0,
        "true_multiple_path": json.dumps(path),
        "theme_hint": theme_hint,
        # provenance
        "_pair_address": pool["pair_address"],
        "_token_address": pool["base_address"],
        "_price_usd": "",
        "_fdv": "",
        "_mcap": "",
        "_url": f"https://www.geckoterminal.com/solana/pools/{pool['pair_address']}",
        "_dex": pool["dex_id"],
        "_gt_candles": str(candles_count),
        "_source": "geckoterminal_new_pools",
    }


# ── Main pipeline ──────────────────────────────────────────────────────────
def run_pipeline(out_path: str, chain: str = "solana", pages: int = 10,
                 min_liq_usd: float = 500, rpm: int = 28, marks: int = 5) -> dict[str, Any]:
    limiter = RateLimiter(rpm)
    t0 = time.time()

    print(f"[fetch_gt_pipeline] chain={chain}  pages={pages}  min_liq=${min_liq_usd:.0f}  rpm={rpm}")

    # 1. Discover
    print(f"\n--- Step 1: discover new pools ({pages} pages) ---")
    all_pools = discover_new_pools(chain, pages, limiter)
    print(f"  discovered: {len(all_pools)}")

    # 2. Filter
    filtered = filter_pools(all_pools, min_liq_usd=min_liq_usd)
    print(f"  after filter (liq>={min_liq_usd}, skip SOL/USDC as base): {len(filtered)}")

    # Sort: newest first (we'll assign t_min as discovery batch offset)
    # GT doesn't expose created_at ordering clearly — just keep as-is

    # 3. Enrich + build rows
    print(f"\n--- Step 2: enrich OHLCV + build rows ---")
    rows = []
    # t_min spacing: each pool gets a slot 3 minutes apart (arbitrary, Desk simulates chronologically)
    for i, pool in enumerate(filtered):
        candles = fetch_ohlcv(pool["pair_address"], chain, limiter, limit=200)
        path, growth = derive_path_growth(candles, marks=marks)
        t_min = i * 3  # 3 min apart, just a placeholder for chronological flow
        row = pool_to_row(pool, t_min, path, growth, len(candles))
        rows.append(row)

        if (i + 1) % 10 == 0 or i == len(filtered) - 1:
            elapsed = time.time() - t0
            print(f"  [{i+1}/{len(filtered)}] {row['ticker']:10s} liq=${pool['reserve_in_usd']:>10.0f}  "
                  f"growth={growth:.4f}  candles={len(candles):>4d}  path_l={len(path)}  "
                  f"elapsed={elapsed:.0f}s")

    # 4. Write CSV
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "t_min", "ticker", "name", "description", "launchpad",
        "liquidity_eth", "liq_growth", "deployer", "holders", "linked_groups",
        "selling_linked", "true_multiple_path", "theme_hint",
        "_pair_address", "_token_address", "_price_usd", "_fdv", "_mcap",
        "_url", "_dex", "_gt_candles", "_source",
    ]
    with out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    stats = {
        "total_discovered": len(all_pools),
        "after_filter": len(filtered),
        "rows_written": len(rows),
        "elapsed_s": round(time.time() - t0, 1),
        "output": str(out),
    }
    return stats


# ── CLI ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="GT new_pools → Desk CSV pipeline")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--chain", default="solana")
    ap.add_argument("--pages", type=int, default=10, help="Number of GT new_pools pages (20 each)")
    ap.add_argument("--min-liq", type=float, default=500, help="Minimum USD liquidity filter")
    ap.add_argument("--rpm", type=int, default=28)
    ap.add_argument("--marks", type=int, default=5)
    args = ap.parse_args()

    stats = run_pipeline(args.out, args.chain, args.pages, args.min_liq,
                         rpm=args.rpm, marks=args.marks)

    print(f"\nDone in {stats['elapsed_s']}s")
    print(f"  discovered={stats['total_discovered']}  after_filter={stats['after_filter']}  written={stats['rows_written']}")
    print(f"  → {stats['output']}")


if __name__ == "__main__":
    main()
