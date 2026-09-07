#!/usr/bin/env python3
"""RH Poll — DexScreener live new-token discovery + pool_lake accumulation.

Polls DexScreener trending + keyword search every N seconds, filters rugs,
and inserts new pools into pool_lake.db. Also writes a cumulative CSV that
can be used directly by rh_trencher.py --from-csv.

Usage:
  python rh_poll.py --interval 60 --db pool_lake.db --out live_pools.csv
  python rh_poll.py --interval 300 --no-db          # CSV-only, no SQLite
  python rh_poll.py --once                            # one-shot poll (for cron)
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import fetch_dex_top as fdt


# ──────────────────────────────────────────────────────────────────────
#  Proxy path generator — estimate peak from DexScreener pair data
# ──────────────────────────────────────────────────────────────────────
def estimate_proxy_path(pair: dict) -> tuple[list[float], float, float]:
    """Estimate synthetic multiple_path + liq_growth from DexScreener data.

    Uses multi-timeframe priceChange (h1/h6/h24) + txns buys/sells to
    classify the path shape and calibrate peak/liq_growth realistically.
    Returns (path_20marks, liq_growth, selling_linked).
    """
    change = pair.get("priceChange", {}) or {}
    h1 = float(change.get("h1", 0) or 0)
    h6 = float(change.get("h6", 0) or 0)
    h24 = float(change.get("h24", 0) or 0)

    vol_24h = float((pair.get("volume") or {}).get("h24", 0) or 0)
    liq_usd = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    txns = pair.get("txns", {}) or {}
    h6_buys = float((txns.get("h6") or {}).get("buys", 0) or 0)
    h6_sells = float((txns.get("h6") or {}).get("sells", 0) or 0)

    # liq_growth proxy
    if liq_usd > 0:
        vol_liq_ratio = min(vol_24h / liq_usd, 50)
    else:
        vol_liq_ratio = random.uniform(2, 15)
    liq_growth = round(max(1.0, min(500.0, vol_liq_ratio * random.uniform(2, 8))), 1)

    # ── Shape classification ──────────────────────────────
    # Determine path shape from multi-TF priceChange
    h6_ratio = h1 / h6 if abs(h6) > 0.01 else 1.0  # h1 vs h6 momentum ratio
    h24_ratio = h6 / h24 if abs(h24) > 0.01 else 1.0

    # Buy/sell ratio → confidence multiplier
    total_txns = h6_buys + h6_sells
    if total_txns > 20:
        buy_sell_ratio = h6_buys / total_txns  # 0.5 = balanced, >0.6 = bullish
        sentiment_boost = 0.6 + buy_sell_ratio * 0.8  # 0.6 to 1.4
    else:
        sentiment_boost = random.uniform(0.7, 1.2)

    abs_h24 = abs(h24) / 100.0
    abs_h6 = abs(h6) / 100.0

    # Classify shape
    if h24 < -50 and h6 < -30:
        shape = "strong_dump"     # 24h down >50%, 6h down >30%
    elif h6 > 50 and h24 < 0:
        shape = "pump_then_dump"  # 6h up >50%, 24h down
    elif h1 > 10 and h6 > h24 and h24 > 0:
        shape = "sustained_rally" # h1 momentum carrying through
    elif h24 > 100 and h6 > 50:
        shape = "gradual_rise"    # both strong positive
    elif abs_h24 < 0.05 and abs_h6 < 0.05:
        shape = "flat"
    elif h6 < 0 and h1 > 5:
        shape = "dead_cat"        # 6h crashed, 1h bouncing
    elif h24 > 20:
        shape = "steady_up"
    else:
        shape = "mixed"

    # ── Peak per shape ─────────────────────────────────────
    if shape == "strong_dump":
        peak = random.uniform(1.1, 1.5) * sentiment_boost
        peak = max(peak, 1.05)
    elif shape == "flat":
        peak = random.uniform(1.0, 1.2)
    elif shape == "pump_then_dump":
        peak = random.uniform(5.0, 20.0) * sentiment_boost
    elif shape == "sustained_rally":
        peak = random.uniform(4.0, 15.0) * sentiment_boost
    elif shape == "gradual_rise":
        peak = random.uniform(3.0, 12.0) * sentiment_boost
    elif shape == "dead_cat":
        peak = random.uniform(1.5, 3.5) * sentiment_boost
    elif shape == "steady_up":
        peak = random.uniform(2.0, 6.0) * sentiment_boost
    else:  # mixed
        peak = random.uniform(1.5, 5.0) * sentiment_boost

    # Also scale by absolute h24 magnitude
    if abs_h24 > 2:
        peak = min(peak * 1.5, 50.0)

    # ── Build path per shape ────────────────────────────────
    def build_rally_path(peak: float, n: int = 20, rise_ticks: int | None = None,
                          tail_decay: float = 0.7) -> list[float]:
        """Smooth rise → peak → gradual decay."""
        if rise_ticks is None:
            rise_ticks = random.randint(5, 9)
        path: list[float] = []
        for _ in range(max(1, rise_ticks // 3)):
            path.append(1.0 + random.uniform(0, 0.06))
        for j in range(rise_ticks - len(path)):
            frac = (j + 1) / max(rise_ticks, 1)
            path.append(peak ** frac * random.uniform(0.95, 1.05))
        path.append(peak * random.uniform(0.92, 1.0))
        for j in range(n - len(path)):
            frac = (j + 1) / max(n - len(path), 1)
            mult = peak * (tail_decay + (1 - tail_decay) * (1 - frac))
            path.append(max(0.95, mult * random.uniform(0.95, 1.05)))
        while len(path) < n:
            path.append(path[-1])
        return [round(p, 3) for p in path[:n]]

    def build_pump_dump_path(peak: float, n: int = 20) -> list[float]:
        """Quick rise → peak → sudden dump (1-2 ticks) → flat near-bottom."""
        rise = random.randint(3, 6)
        path: list[float] = []
        for _ in range(max(1, rise // 3)):
            path.append(1.0 + random.uniform(0, 0.06))
        for j in range(rise - len(path)):
            frac = (j + 1) / max(rise, 1)
            path.append(peak ** frac * random.uniform(0.95, 1.05))
        path.append(peak * random.uniform(0.93, 1.0))
        # Sudden dump 1 tick
        path.append(max(0.8, peak * random.uniform(0.15, 0.35)))
        # Remainder stays near bottom
        bottom = path[-1]
        for j in range(n - len(path)):
            path.append(max(0.8, bottom * random.uniform(0.9, 1.1)))
        while len(path) < n:
            path.append(path[-1])
        return [round(p, 3) for p in path[:n]]

    def build_flat_path(n: int = 20) -> list[float]:
        """Near-flat path with tiny noise."""
        return [round(1.0 + random.uniform(-0.02, 0.05), 3) for _ in range(n)]

    def build_dead_cat_path(peak: float, n: int = 20) -> list[float]:
        """Down → bounce up → fades back down."""
        path: list[float] = []
        # First 5 ticks: gradual dump
        for j in range(5):
            frac = (j + 1) / 5
            path.append(max(0.6, 1.0 - 0.4 * frac * random.uniform(0.8, 1.2)))
        # Then bounce
        for j in range(8):
            frac = (j + 1) / 8
            path.append(path[-1] + (peak - path[-1]) * frac * random.uniform(0.9, 1.1))
        # Then fade
        for j in range(n - len(path)):
            frac = (j + 1) / max(n - len(path), 1)
            mult = peak * (1.0 - 0.3 * frac)
            path.append(max(0.7, mult * random.uniform(0.9, 1.1)))
        while len(path) < n:
            path.append(path[-1])
        return [round(p, 3) for p in path[:n]]

    if shape == "strong_dump":
        path = build_flat_path()
        # Actually make it go down then try to recover
        path = [round(max(0.7, 1.0 - 0.3 * (j/19) + random.uniform(-0.02, 0.03)), 3)
                for j in range(20)]
    elif shape == "flat":
        path = build_flat_path()
    elif shape == "pump_then_dump":
        path = build_pump_dump_path(peak)
    elif shape == "dead_cat":
        path = build_dead_cat_path(peak)
    elif shape == "sustained_rally":
        path = build_rally_path(peak, rise_ticks=random.randint(6, 9), tail_decay=0.7)
    elif shape == "gradual_rise":
        path = build_rally_path(peak, rise_ticks=random.randint(7, 10), tail_decay=0.85)
    elif shape == "steady_up":
        path = build_rally_path(peak, rise_ticks=random.randint(5, 7), tail_decay=0.75)
    else:
        path = build_rally_path(peak, rise_ticks=random.randint(4, 7), tail_decay=0.7)

    # selling_linked — still conservative, but make pump_then_dump more risky
    if shape == "pump_then_dump":
        sell_linked = random.choice([1, 1, 2, 3])  # 25% extreme risk
    else:
        sell_linked = random.choice([0, 0, 0, 0, 1, 1, 1, 2])

    return path, liq_growth, sell_linked, shape


# ──────────────────────────────────────────────────────────────────────
#  Database helpers
# ──────────────────────────────────────────────────────────────────────
def get_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection):
    conn.execute("""CREATE TABLE IF NOT EXISTS raw_pools (
        pair_address TEXT PRIMARY KEY,
        mint_address TEXT, base_symbol TEXT, base_name TEXT, quote_symbol TEXT,
        dex_id TEXT, reserve_usd REAL, volume_24h REAL,
        discovered_at INTEGER, first_seen_at TEXT, source TEXT
    )""")


# ──────────────────────────────────────────────────────────────────────
#  Single poll cycle
# ──────────────────────────────────────────────────────────────────────
def poll_once(conn: sqlite3.Connection | None, csv_path: str | None,
              chain: str = "solana", limit: int = 30) -> dict:
    """One poll cycle: DexScreener → filter → DB + CSV. Returns stats."""
    t0 = time.time()
    stats = {"trending": 0, "keyword": 0, "new": 0, "rugs_filtered": 0, "elapsed_s": 0}

    # Get existing pair_addresses from DB (if any) + CSV (if any)
    existing: set[str] = set()
    if conn is not None:
        # sqlite3.Row or plain tuple — handle both
        for r in conn.execute("SELECT pair_address FROM raw_pools").fetchall():
            if isinstance(r, sqlite3.Row):
                existing.add(r["pair_address"])
            else:
                existing.add(r[0])
    if csv_path and Path(csv_path).exists():
        with open(csv_path) as f:
            existing.update(row["_pair_address"] for row in
                            csv.DictReader(f) if row.get("_pair_address"))

    # Fetch trending profiles
    profiles = fdt.fetch_trending_profiles(chain, max(limit // 2, 5))
    stats["trending"] = len(profiles)

    # Fetch keyword search
    kw_pairs = fdt.fetch_search_keywords(chain, fdt.MEME_KEYWORDS, limit)
    stats["keyword"] = len(kw_pairs)

    # Dedup + filter rugs + accumulate new
    seen: set[str] = set()
    new_rows: list[dict] = []
    cycle_new: list[dict] = []  # for CSV output

    for source, pairs in [("dex_trending", profiles), ("dex_keyword", kw_pairs)]:
        for p in pairs:
            addr = p.get("pairAddress") or ""
            if not addr or addr in seen or addr in existing:
                continue
            seen.add(addr)
            if fdt.is_probably_rug(p):
                stats["rugs_filtered"] += 1
                continue

            # Build DB row
            first_seen = p.get("pairCreatedAt")
            if isinstance(first_seen, (int, float)):
                first_seen = datetime.fromtimestamp(first_seen / 1000, tz=timezone.utc).isoformat()
            row = {
                "pair_address": addr,
                "mint_address": p.get("baseToken", {}).get("address", ""),
                "base_symbol": p.get("baseToken", {}).get("symbol", ""),
                "base_name": p.get("baseToken", {}).get("name", ""),
                "quote_symbol": p.get("quoteToken", {}).get("symbol", ""),
                "dex_id": p.get("dexId", ""),
                "reserve_usd": (p.get("liquidity", {}) or {}).get("usd", 0) or 0,
                "volume_24h": (p.get("volume", {}) or {}).get("h24", 0) or 0,
                "discovered_at": int(time.time()),
                "first_seen_at": first_seen or "",
                "source": source,
            }
            if conn is not None:
                try:
                    conn.execute("""INSERT INTO raw_pools VALUES
                        (:pair_address,:mint_address,:base_symbol,:base_name,
                         :quote_symbol,:dex_id,:reserve_usd,:volume_24h,
                         :discovered_at,:first_seen_at,:source)""", row)
                except sqlite3.IntegrityError:
                    pass  # duplicate
            new_rows.append(row)

            # Build CSV row for rh_trencher.py — with proxy path
            proxy_path, liq_growth, sell_linked, shape = estimate_proxy_path(p)

            # Distribute t_min using pairCreatedAt → spreads fresh pools
            pc_at = p.get("pairCreatedAt")
            if isinstance(pc_at, (int, float)) and pc_at > 0:
                # Convert ms timestamp → t_min in replay window (300-1000)
                age_min = max(0, (time.time() * 1000 - pc_at) / 60000)
                t_min = max(300, min(1000, int(300 + age_min * 15)))
            else:
                t_min = max(300, (int(time.time()) % 700))

            csv_row = {
                "t_min": t_min,
                "ticker": row["base_symbol"] or "UNKNOWN",
                "name": row["base_name"],
                "description": "",
                "launchpad": f"DexScreener·{shape}",
                "liquidity_eth": row["reserve_usd"] / 3000.0 if row["reserve_usd"] else 0.05,
                "liq_growth": liq_growth,
                "deployer": "",
                "holders": "[]",
                "linked_groups": "[]",
                "selling_linked": sell_linked,
                "true_multiple_path": json.dumps(proxy_path),
                "theme_hint": "",
                "_pair_address": addr,
                "_token_address": row["mint_address"],
                "_dex": chain,
                "_source": source,
            }
            cycle_new.append(csv_row)

    if conn is not None:
        conn.commit()

    # Append to cumulative CSV
    if csv_path and cycle_new:
        path = Path(csv_path)
        is_new = not path.exists()
        fields = list(cycle_new[0].keys())
        with open(path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            if is_new:
                w.writeheader()
            w.writerows(cycle_new)

    stats["new"] = len(new_rows)
    stats["elapsed_s"] = round(time.time() - t0, 1)
    return stats


def format_stats(stats: dict, total_in_db: int = 0, total_csv: int = 0) -> str:
    return (
        f"[poll] +{stats['new']:3d} new  "
        f"trend={stats['trending']} kw={stats['keyword']} "
        f"rugs_filtered={stats['rugs_filtered']} "
        f"db={total_in_db} csv={total_csv}  "
        f"elapsed={stats['elapsed_s']}s"
    )


# ──────────────────────────────────────────────────────────────────────
#  Main loop
# ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="DexScreener live new-token poller")
    ap.add_argument("--interval", type=int, default=60,
                    help="Poll interval in seconds (default 60)")
    ap.add_argument("--db", default="pool_lake.db",
                    help="SQLite pool_lake DB path (default pool_lake.db)")
    ap.add_argument("--no-db", action="store_true",
                    help="Skip SQLite write, CSV-only mode")
    ap.add_argument("--out", default="live_pools.csv",
                    help="Cumulative CSV output path (default live_pools.csv)")
    ap.add_argument("--chain", default="solana")
    ap.add_argument("--limit", type=int, default=30,
                    help="Max pairs per fetch direction (default 30)")
    ap.add_argument("--once", action="store_true",
                    help="Run exactly one cycle and exit (for cron)")
    args = ap.parse_args()

    conn = None if args.no_db else get_db(args.db)
    if conn:
        ensure_schema(conn)

    print(f"[rh_poll] interval={args.interval}s  db={'OFF' if args.no_db else args.db}  csv={args.out}")
    print(f"[rh_poll] chain={args.chain}  limit={args.limit}  mode={'once' if args.once else 'continuous'}")
    print("=" * 60)

    cycle = 0
    try:
        while True:
            cycle += 1
            total_db = 0
            total_csv = 0
            if conn:
                total_db = conn.execute("SELECT COUNT(*) FROM raw_pools").fetchone()[0]
            if Path(args.out).exists():
                with open(args.out) as f:
                    total_csv = sum(1 for _ in csv.DictReader(f))

            stats = poll_once(conn, args.out if args.out else None,
                              chain=args.chain, limit=args.limit)
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] cycle #{cycle:3d} {format_stats(stats, total_db, total_csv)}")

            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n[rh_poll] stopped after {cycle} cycles")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
