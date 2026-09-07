#!/usr/bin/env python3
"""RH Live — 一键启动 monitor (后台) + dashboard (前台) + live tick engine.

用法:
  python run_live.py                                  # 默认配置启动 (replay mode)
  python run_live.py --live                            # 模拟盘 live tick 模式
  python run_live.py --live --interval 15 --bankroll 1000
  python run_live.py --live --chain solana --max-age 30 --min-liq 500
  python run_live.py --interval 5 --seeds 5          # 自定义参数
  python run_live.py --once                            # 单次 poll+backtest, 不启 server
  python run_live.py --no-monitor                      # 只启 server (base CSV replay)
  python run_live.py --csv pool_lake_full_synth.csv   # 指定 base CSV
"""
from __future__ import annotations

import argparse
import csv
import json
import queue
import sqlite3
import statistics
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import rh_monitor
import rh_poll
import rh_server
import rh_trencher


LIVE_BANNER = """
╔═══════════════════════════════════════════════════════════════╗
║  🟢 LIVE SIMULATION MODE                                      ║
║  策略信号输出 · 不会真实下单                                   ║
║  数据源: DexScreener API                                     ║
║  时钟: Unix epoch real-time                                  ║
║  Dashboard: http://{host}:{port}                              ║
║  Ctrl+C to stop                                              ║
╚═══════════════════════════════════════════════════════════════╝
"""


def monitor_loop(base_csv: str, merged_csv: str, stats_json: str,
                 interval_min: int, seeds: int, max_positions: int,
                 chain: str, limit: int, use_db: bool, poll_interval_min: int):
    """后台循环: poll DexScreener → merge CSV → backtest → write JSON."""
    from rh_monitor import poll_once, merge_csvs, run_backtest

    conn = None
    if use_db:
        conn = sqlite3.connect("pool_lake.db")
        conn.row_factory = sqlite3.Row
        rh_poll.ensure_schema(conn)

    cycle = 0
    while True:
        cycle += 1
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] ⚡ MONITOR cycle #{cycle} (poll + backtest)", flush=True)

        poll_once(conn, "_live_fresh.csv", chain=chain, limit=limit)

        base_count, added = merge_csvs(base_csv, "_live_fresh.csv", merged_csv)
        if added == 0 and cycle > 1:
            print(f"  → no new pools, skip backtest", flush=True)
            try:
                prev = json.loads(Path(stats_json).read_text())
                prev["cycle"] = cycle
                prev["total_pools"] = base_count
                prev["added"] = 0
                prev["timestamp"] = datetime.now(timezone.utc).isoformat()
                Path(stats_json).write_text(json.dumps(prev, indent=2))
            except Exception:
                pass
            time.sleep(interval_min * 60)
            continue

        stats = run_backtest(merged_csv, seeds=seeds, max_positions=max_positions)
        if stats:
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "cycle": cycle,
                "total_pools": base_count + added,
                "added": added,
                **stats,
            }
            Path(stats_json).write_text(json.dumps(payload, indent=2))
            print(f"  ✅ mult={stats['mult_mean']:.1f}x  "
                  f"pools={base_count+added}  "
                  f"theme={stats['dominant_theme']}  "
                  f"profit={stats['profit_rate']:.0%}", flush=True)

        time.sleep(interval_min * 60)


def _start_live_engine(args) -> tuple:
    """Start FX feed, DexClient, TickEngine; return (fx, dex, engine)."""
    from fx_feed import FXFeed
    from dex_client import DexClient
    from live_tick import TickEngine

    desk = rh_trencher.Desk(
        start_usd=args.bankroll,
        live_mode=True,
        realistic=True,
        max_positions=args.max_positions,
    )

    fx = FXFeed(interval_seconds=args.fx_interval)
    fx.start()
    desk.fx_feed = fx  # Task 7: attach for SSE extras

    dex = DexClient(chain=args.chain)

    engine = TickEngine(
        desk=desk,
        dex=dex,
        fx=fx,
        interval=args.interval,
        max_age_min=args.max_age,
        min_liquidity_usd=args.min_liq,
    )

    t_discover = threading.Thread(target=engine.run_discover, daemon=True,
                                  name="discover")
    t_tick = threading.Thread(target=engine.run_tick, daemon=True,
                              name="tick")
    t_discover.start()
    t_tick.start()

    print(f"[run_live] discover_loop started (interval={args.interval}s, "
          f"chain={args.chain})")
    print(f"[run_live] tick_loop started (interval={args.interval}s)")
    print(f"[run_live] FX feed: ETH=${fx.get_eth_usd():.2f} SOL=${fx.get_sol_usd():.2f}")

    return fx, dex, engine


def _shutdown_live(fx, engine):
    """Graceful shutdown: stop engine, then stop fx feed."""
    print("\n[run_live] shutting down...")
    engine.stop()
    fx.stop()
    print("[run_live] engine stopped")
    time.sleep(0.5)
    print("[run_live] fx feed stopped")


def main():
    ap = argparse.ArgumentParser(
        description="RH Live — monitor + dashboard + live tick engine")
    ap.add_argument("--live", action="store_true",
                    help="启动 live tick 模拟盘模式 (discover + tick loops)")
    ap.add_argument("--interval", type=int, default=15,
                    help="Tick/discover interval in seconds (default 15)")
    ap.add_argument("--fx-interval", type=int, default=300,
                    help="FX rate refresh interval in seconds (default 300)")
    ap.add_argument("--max-age", type=int, default=30,
                    help="Max token age in minutes for discovery (default 30)")
    ap.add_argument("--min-liq", type=float, default=500.0,
                    help="Min liquidity in USD for discovery (default 500)")
    ap.add_argument("--bankroll", type=float, default=500.0,
                    help="Starting bankroll in USD (default 500)")
    ap.add_argument("--chain", default="solana",
                    help="Blockchain chain (default: solana)")
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--max-positions", type=int, default=3)
    ap.add_argument("--poll-interval", type=int, default=2,
                    help="DexScreener poll interval in minutes (default 2)")
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--csv", default="pool_lake_full_synth.csv",
                    help="Base CSV for backtest + dashboard default")
    ap.add_argument("--no-monitor", action="store_true",
                    help="Skip monitor thread, only run dashboard")
    ap.add_argument("--no-db", action="store_true")
    ap.add_argument("--once", action="store_true",
                    help="Run one monitor cycle + backtest and exit")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()

    if args.live:
        # ── Live simulation mode ──────────────────────────────────────
        print(LIVE_BANNER.format(host=args.host, port=args.port))
        print(f"[run_live] bankroll=${args.bankroll}  max_positions={args.max_positions}",
              flush=True)
        print(f"[run_live] max_age={args.max_age}min  min_liq=${args.min_liq}  "
              f"chain={args.chain}", flush=True)

        fx, dex, engine = _start_live_engine(args)

        def _sigint_handler(sig, frame):
            _shutdown_live(fx, engine)
            sys.exit(0)

        signal.signal(signal.SIGINT, _sigint_handler)

        try:
            rh_server.main(args.host, args.port, live_mode=True)
        except KeyboardInterrupt:
            _shutdown_live(fx, engine)
        return

    # ── Replay / Monitor mode (existing behavior) ─────────────────
    base_csv = args.csv
    merged_csv = "_live_merged.csv"
    fresh_csv = "_live_fresh.csv"
    stats_json = "_live_stats.json"

    if not Path(base_csv).exists():
        print(f"ERROR: base CSV not found: {base_csv}")
        sys.exit(1)

    if args.once:
        print(f"[run_live] one-shot: poll + backtest {base_csv}")
        rh_poll.poll_once(
            None if args.no_db else sqlite3.connect("pool_lake.db"),
            fresh_csv, chain=args.chain, limit=args.limit
        )
        base_count, added = rh_monitor.merge_csvs(base_csv, fresh_csv, merged_csv)
        print(f"  pools: {base_count} base + {added} fresh = {base_count+added} merged")
        stats = rh_monitor.run_backtest(merged_csv, seeds=args.seeds,
                                        max_positions=args.max_positions)
        if stats:
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "cycle": 1, "total_pools": base_count + added, "added": added,
                **stats,
            }
            Path(stats_json).write_text(json.dumps(payload, indent=2))
            print(f"\n{'='*60}")
            print(f"  MULT  mean={stats['mult_mean']:,.1f}x  "
                  f"std={stats['mult_std']:.1f}  "
                  f"bankroll=${stats['bankroll_mean']:,.0f}")
            print(f"  ENTRIES {stats['entries_mean']:.0f} avg  "
                  f"W/L {stats['wins_mean']:.0f}/{stats['losses_mean']:.0f}")
            print(f"  THEME {stats['dominant_theme']}  "
                  f"PROFIT {stats['profit_rate']:.0%}")
            print(f"  JSON written to {stats_json}")
            print(f"{'='*60}")
        return

    print(r"""
╔═══════════════════════════════════════════════════════════════╗
║  RH LIVE · Monitor + Dashboard                                ║
╠═══════════════════════════════════════════════════════════════╣
║  Monitor:  every {}min → DexScreener → merge → backtest      ║
║  Dashboard: http://{}:{}                                      ║
║  CSV:      {}                                            ║
║  JSON:     {}                                            ║
╚═══════════════════════════════════════════════════════════════╝
    """.format(args.interval, args.host, args.port, base_csv, stats_json))

    mon_thread = None
    if not args.no_monitor:
        print("[run_live] starting monitor thread...")
        mon_thread = threading.Thread(
            target=monitor_loop,
            args=(base_csv, merged_csv, stats_json,
                  args.interval, args.seeds, args.max_positions,
                  args.chain, args.limit,
                  not args.no_db, args.poll_interval),
            daemon=True,
        )
        mon_thread.start()

    time.sleep(2)

    print(f"[run_live] starting dashboard on http://{args.host}:{args.port}")
    print("[run_live] open URL in browser → click 'Monitor' button")
    print("[run_live] Ctrl+C to stop\n")
    try:
        rh_server.main(args.host, args.port, base_csv, stats_json)
    except KeyboardInterrupt:
        print("\n[run_live] stopped")


if __name__ == "__main__":
    main()
