#!/usr/bin/env python3
"""Enrich TokenLaunch CSV with REAL OHLCV price paths from GeckoTerminal.

Reads CSV produced by fetch_dexscreener.py, fetches per-pool 1-minute candles
from GeckoTerminal, replaces the synthetic `true_multiple_path` proxy with a
real chain-on-chain price-derived multiple path, writes a new CSV.

Pure stdlib. Rate-limited to ~28 req/min (GeckoTerminal free tier = 30 rpm).

Usage:
  python enrich_ohlcv.py --in recent_sol.csv --out recent_sol_ohlcv.csv
  python enrich_ohlcv.py --in recent_sol.csv --out recent_sol_ohlcv.csv --minutes 240
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# ── Rate limiter (token bucket) ──────────────────────────────────────────────
class RateLimiter:
    """Minimal token bucket — ensures we never exceed RPM."""
    def __init__(self, rpm: int):
        self.min_interval = 60.0 / rpm
        self._next_ok = 0.0

    def wait(self):
        now = time.monotonic()
        if now < self._next_ok:
            time.sleep(self._next_ok - now)
        self._next_ok = time.monotonic() + self.min_interval


# ── GeckoTerminal OHLCV fetcher ──────────────────────────────────────────────
GT_BASE = "https://api.geckoterminal.com/api/v2"
GT_HEADERS = {"User-Agent": "rh_trencher-enrich/0.1", "accept": "application/json"}


def fetch_ohlcv_minute(pool_address: str, chain: str = "solana",
                       limit: int = 200, max_retries: int = 4) -> list[list[float]]:
    """Fetch 1-minute candles from GeckoTerminal.

    Returns list of [ts, open, high, low, close, volume] sorted ASCENDING (oldest first).
    Empty list if pool not found or no candles.
    """
    url = f"{GT_BASE}/networks/{chain}/pools/{pool_address}/ohlcv/minute?limit={limit}&currency=usd&token=base"
    backoff = 1.5
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=GT_HEADERS)
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.load(r)
            cs = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
            # GT returns DESCENDING (newest first) — reverse to ASCENDING
            cs.reverse()
            return cs
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(backoff)
                backoff *= 2
                continue
            if e.code == 404:
                return []  # pool not indexed by GT yet
            time.sleep(backoff)
            backoff *= 2
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            # SSL EOF, connection reset, etc.
            time.sleep(backoff)
            backoff *= 2
    return []


def candles_to_path_and_growth(candles: list[list[float]], marks: int = 5,
                               mark_step_min: int = 6, warmup_min: int = 3
                               ) -> tuple[list[float], float]:
    """Convert ascending GT minute-candles → (true_multiple_path, growth_multiple).

    growth_multiple = last_close / first_close  (e.g. 1.057 = up 5.7%, 0.137 = rug to 13.7%)
    This matches Desk's liq_growth expectation: Desk score uses g-1, so g=1.057 → +5.7%.
    """
    if not candles:
        return [], 0.0

    closes = [c[4] for c in candles]   # oldest first
    n = len(closes)

    # ── growth ──────────────────────────────────────────────────────────
    first_close = closes[0] if closes[0] > 0 else (closes[1] if n > 1 else 1e-9)
    last_close = closes[-1] if closes[-1] > 0 else closes[-2] if n > 1 else first_close
    growth = round(last_close / first_close, 4) if first_close > 0 else 1.0

    # ── path (true_multiple_path, normalised to first mark = 1.0) ─────
    step_idxs = [warmup_min + j * mark_step_min for j in range(marks)]
    path = []
    baseline_idx = min(warmup_min, n - 1)
    baseline = closes[baseline_idx]
    if baseline <= 0:
        baseline = closes[0] if closes[0] > 0 else 1e-9

    for idx in step_idxs:
        real_idx = min(idx, n - 1)
        price = closes[real_idx]
        if price <= 0:
            price = closes[real_idx - 1] if real_idx > 0 else baseline
        path.append(round(price / baseline, 4))

    return path, growth


# ── Hour-candle fallback ─────────────────────────────────────────────────────
def fetch_ohlcv_hour(pool_address: str, chain: str = "solana",
                     limit: int = 48) -> list[list[float]]:
    url = f"{GT_BASE}/networks/{chain}/pools/{pool_address}/ohlcv/hour?limit={limit}&currency=usd&token=base"
    try:
        req = urllib.request.Request(url, headers=GT_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.load(r)
        cs = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        cs.reverse()
        return cs
    except Exception:
        return []


def hour_candles_to_path(hour_cs: list[list[float]], marks: int = 5) -> list[float]:
    """Coarse fallback when minute candles are too sparse.

    Interpolates hour closes to minute grid takes points at 3,9,15,21,27 min
    by sampling the hour candle closes that bracket each mark.
    """
    if not hour_cs:
        return []
    closes_hour = [c[4] for c in hour_cs]
    baseline = closes_hour[0] if closes_hour[0] > 0 else 1e-9
    # Each hour candle covers 60 minutes. We take 5 marks at 3,9,15,21,27 min
    # which are all within the first hour candle — so marks all = hour_close[0]/baseline ≈ 1.0
    # Better: take marks at 0.05h, 0.15h, 0.25h, 0.35h, 0.45h → all in first hour
    # → return flat 1.0s (no info). Skip this fallback — just return empty path
    # and let caller keep the synthetic proxy path.
    return []


# ── Main enrich pipeline ─────────────────────────────────────────────────────
def enrich_csv(in_path: str, out_path: str, chain: str = "solana",
               marks: int = 5, rpm: int = 28, fix_growth: bool = True) -> dict[str, Any]:
    """Read CSV, add GT OHLCV-derived path, write new CSV. Returns stats."""
    limiter = RateLimiter(rpm)

    with open(in_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    fieldnames = list(rows[0].keys()) if rows else []
    for col in ("_gt_candles_count", "_growth_from_ohlcv",
                "true_multiple_path", "_enriched"):
        if col not in fieldnames:
            fieldnames.append(col)

    stats = {"total": len(rows), "enriched": 0, "fallback_proxy": 0,
             "no_pair": 0, "growth_fixed": 0}
    t_start = time.time()

    for i, row in enumerate(rows):
        pair = row.get("_pair_address", "").strip()
        ticker = row.get("ticker", "?")

        if not pair:
            stats["no_pair"] += 1
            row["_enriched"] = "no_pair_address"
            continue

        limiter.wait()
        candles = fetch_ohlcv_minute(pair, chain=chain, limit=200)

        if len(candles) < 2:
            row["_gt_candles_count"] = str(len(candles))
            row["_enriched"] = "proxy_too_few_candles"
            stats["fallback_proxy"] += 1
            continue

        path, growth = candles_to_path_and_growth(candles, marks=marks)

        if len(path) == 0:
            row["_gt_candles_count"] = str(len(candles))
            row["_enriched"] = "proxy_empty_path"
            stats["fallback_proxy"] += 1
            continue

        row["true_multiple_path"] = json.dumps(path)
        row["_gt_candles_count"] = str(len(candles))
        row["_enriched"] = "real_ohlcv"

        if fix_growth:
            old_growth = row.get("liq_growth", "0")
            try:
                old_growth_f = float(old_growth)
            except ValueError:
                old_growth_f = 0.0
            row["liq_growth"] = str(growth)
            row["_growth_from_ohlcv"] = f"{old_growth_f}→{growth}"
            stats["growth_fixed"] += 1

        stats["enriched"] += 1

        if (i + 1) % 10 == 0:
            elapsed = time.time() - t_start
            eta = (elapsed / (i + 1)) * (len(rows) - i - 1)
            print(f"  [{i+1}/{len(rows)}] last={ticker:8s} ok={stats['enriched']} "
                  f"proxy={stats['fallback_proxy']} growth_fix={stats['growth_fixed']} ETA={eta:.0f}s")

    # Write
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    stats["elapsed_s"] = round(time.time() - t_start, 1)
    stats["output"] = str(out)
    return stats


# ── CLI ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="Enrich TokenLaunch CSV with GeckoTerminal OHLCV-derived price paths"
    )
    ap.add_argument("--in", dest="in_path", required=True,
                    help="Input CSV (from fetch_dexscreener.py)")
    ap.add_argument("--out", required=True, help="Output enriched CSV path")
    ap.add_argument("--chain", default="solana")
    ap.add_argument("--marks", type=int, default=5,
                    help="Number of path marks (default 5, matches Desk's 5-mark path)")
    ap.add_argument("--rpm", type=int, default=28,
                    help="Requests per minute (default 28, safe below GT free tier 30)")
    ap.add_argument("--no-fix-growth", action="store_true",
                    help="Don't overwrite liq_growth with OHLCV-derived growth multiplier")
    args = ap.parse_args()

    print(f"[enrich_ohlcv] {args.in_path} → {args.out}")
    print(f"  chain={args.chain}  marks={args.marks}  rpm={args.rpm}  fix_growth={not args.no_fix_growth}")

    if not Path(args.in_path).exists():
        print(f"ERROR: input not found: {args.in_path}")
        sys.exit(1)

    stats = enrich_csv(args.in_path, args.out, chain=args.chain,
                       marks=args.marks, rpm=args.rpm,
                       fix_growth=not args.no_fix_growth)

    print(f"\nDone in {stats['elapsed_s']}s")
    print(f"  total={stats['total']}  enriched(real OHLCV)={stats['enriched']}")
    print(f"  fallback(kept proxy)={stats['fallback_proxy']}  no_pair={stats['no_pair']}")
    print(f"  growth_fixed={stats.get('growth_fixed',0)}")
    print(f"  → {stats['output']}")


if __name__ == "__main__":
    main()
