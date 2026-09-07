#!/usr/bin/env python3
"""RH Monitor — Continuous live monitoring.

Polls DexScreener for new tokens every N minutes → merges into pool_lake_full.csv
→ runs multi-seed backtest → prints stats. Optional: writes JSON to dashboard.

Usage:
  python rh_monitor.py                         # 5min interval, pool_lake_full.csv
  python rh_monitor.py --interval 3            # 3min
  python rh_monitor.py --once                  # single poll + backtest cycle
  python rh_monitor.py --base pool_lake_full_synth.csv  # use a different base CSV
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import rh_poll
import rh_trencher


# ──────────────────────────────────────────────────────────────────────
#  Merge fresh DexScreener rows into base CSV (dedup by _pair_address)
# ──────────────────────────────────────────────────────────────────────
def merge_csvs(base: str, fresh: str, out: str) -> tuple[int, int]:
    """Merge fresh CSV into base CSV. Returns (base_count, added_count)."""
    base_path = Path(base)
    fresh_path = Path(fresh)
    if not base_path.exists():
        return (0, 0)
    if not fresh_path.exists():
        fresh_path.write_text("")  # empty

    # Load base
    with open(base_path) as f:
        base_rows = list(csv.DictReader(f))
    base_pairs = {r.get("_pair_address", "") for r in base_rows}

    # Load fresh, dedup
    fresh_rows = []
    with open(fresh_path) as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []
        for r in reader:
            if r.get("_pair_address") and r["_pair_address"] not in base_pairs:
                fresh_rows.append(r)
                base_pairs.add(r["_pair_address"])

    # Write merged
    all_rows = base_rows + fresh_rows
    if all_rows:
        fields = list(all_rows[0].keys())
        with open(out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(all_rows)

    return (len(base_rows), len(fresh_rows))


# ──────────────────────────────────────────────────────────────────────
#  Multi-seed backtest with summarised output
# ──────────────────────────────────────────────────────────────────────
def run_backtest(csv_path: str, seeds: int = 5, max_positions: int = 10,
                 lg_proxy: bool = True) -> dict:
    """Run N-seed backtest, return summary dict."""
    mults: list[float] = []
    bankrolls: list[float] = []
    entries_list: list[int] = []
    wins_list: list[int] = []
    losses_list: list[int] = []
    halt_count = 0
    themes: list[str] = []

    for seed in range(seeds):
        d = rh_trencher.run_replay(
            verbose=False, seed=seed, from_csv=csv_path,
            lg_proxy=lg_proxy, max_positions=max_positions,
            realistic=True,  # always realistic when using real data
        )
        start_bankroll = getattr(d, "start", 1000.0)
        mult = d.bankroll / start_bankroll
        mults.append(mult)
        bankrolls.append(d.bankroll)
        entries_list.append(d.entered)
        exits = [f for f in d.feed if f.side == "EXIT"]
        wins_list.append(sum(1 for f in exits if f.multiple > 1.0))
        losses_list.append(sum(1 for f in exits if f.multiple <= 1.0))
        halt_count += int(any("HALT" in f.note or "halt" in f.note.lower()
                              for f in d.feed))
        # theme detection from narrative
        themes.append(getattr(d.narrative, "focus_theme", None) or "(none)")

    if not mults:
        return {}

    def safe_stat(vals: list[float | int], fn):
        return fn(vals) if vals else 0

    return {
        "seeds": seeds,
        "mult_mean": safe_stat(mults, statistics.mean),
        "mult_median": safe_stat(mults, statistics.median),
        "mult_min": min(mults),
        "mult_max": max(mults),
        "mult_std": safe_stat(mults, statistics.stdev) if len(mults) > 1 else 0.0,
        "bankroll_mean": safe_stat(bankrolls, statistics.mean),
        "entries_mean": safe_stat(entries_list, statistics.mean),
        "wins_mean": safe_stat(wins_list, statistics.mean),
        "losses_mean": safe_stat(losses_list, statistics.mean),
        "halt_rate": halt_count / seeds,
        "dominant_theme": max(set(themes), key=themes.count) if themes else "(none)",
        "profit_rate": sum(1 for m in mults if m > 1.0) / seeds,
    }


def print_summary(stats: dict, total_pools: int, added: int):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n{'='*60}")
    print(f"[{ts}] MONITOR  pools={total_pools}  +{added} new this cycle")
    print(f"{'='*60}")
    print(f"  mult  mean={stats['mult_mean']:,.1f}  "
          f"median={stats['mult_median']:,.1f}  "
          f"min={stats['mult_min']:,.1f}  "
          f"max={stats['mult_max']:,.1f}  "
          f"std={stats['mult_std']:.1f}")
    print(f"  bankroll  mean=${stats['bankroll_mean']:,.0f}")
    print(f"  entries/wins/losses  "
          f"{stats['entries_mean']:.0f} / {stats['wins_mean']:.0f} / {stats['losses_mean']:.0f}")
    print(f"  halt_rate={stats['halt_rate']:.0%}  "
          f"dominant_theme={stats['dominant_theme']}  "
          f"profit_rate={stats['profit_rate']:.0%}")
    print(f"{'='*60}\n")


# ──────────────────────────────────────────────────────────────────────
#  Main loop
# ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="RH continuous live monitor")
    ap.add_argument("--interval", type=int, default=5,
                    help="Poll interval in minutes (default 5)")
    ap.add_argument("--base", default="pool_lake_full.csv",
                    help="Base CSV to merge fresh DexScreener rows into")
    ap.add_argument("--out", default="_monitor_merged.csv",
                    help="Output merged CSV path")
    ap.add_argument("--fresh", default="_monitor_fresh.csv",
                    help="Temporary fresh-poll CSV path")
    ap.add_argument("--db", default="pool_lake.db",
                    help="pool_lake.db path")
    ap.add_argument("--no-db", action="store_true")
    ap.add_argument("--chain", default="solana")
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--max-positions", type=int, default=10)
    ap.add_argument("--once", action="store_true",
                    help="Run exactly one cycle and exit")
    ap.add_argument("--json-out", default=None,
                    help="If set, write latest stats as JSON to this path")
    args = ap.parse_args()

    conn = None if args.no_db else sqlite3.connect(args.db)
    if conn:
        conn.row_factory = sqlite3.Row
        rh_poll.ensure_schema(conn)

    print(f"[rh_monitor] interval={args.interval}min  base={args.base}  seeds={args.seeds}")
    print(f"[rh_monitor] db={'OFF' if args.no_db else args.db}  out={args.out}")
    print(f"[rh_monitor] chain={args.chain}  limit={args.limit}  max_pos={args.max_positions}")

    cycle = 0
    try:
        while True:
            cycle += 1
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"\n[{ts}] ── CYCLE #{cycle} ──")

            # 1. Poll DexScreener
            rh_poll.poll_once(conn, args.fresh, chain=args.chain, limit=args.limit)

            # 2. Merge fresh into base CSV
            base_count, added = merge_csvs(args.base, args.fresh, args.out)

            # 3. Run backtest on merged CSV
            if Path(args.out).exists():
                stats = run_backtest(args.out, seeds=args.seeds,
                                     max_positions=args.max_positions)
                if stats:
                    print_summary(stats, base_count + added, added)

                    if args.json_out:
                        stats_json = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "cycle": cycle, "total_pools": base_count + added,
                            "added": added, **stats,
                        }
                        Path(args.json_out).write_text(json.dumps(stats_json, indent=2))
            else:
                print(f"[monitor] {args.out} not found, skipping backtest")

            if args.once:
                break
            wait_s = args.interval * 60
            print(f"[monitor] sleeping {args.interval}min (Ctrl+C to stop)...")
            time.sleep(wait_s)

    except KeyboardInterrupt:
        print(f"\n[rh_monitor] stopped after {cycle} cycles")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
