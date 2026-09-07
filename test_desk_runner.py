#!/usr/bin/env python3
"""In-process test: DeskRunner auto-run → ENTRY/EXIT signals → Swap Bundles."""
import json, os, sys, threading, time
from pathlib import Path

for line in Path(".env").read_text(encoding="utf-8-sig").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, ".")
from rh_server_live import ServerState, JupiterExecutor, start_live_queue
from rh_desk_runner import DeskRunner

# Load CSV
from fetch_dexscreener import load_csv_as_tokenlaunches
raw = load_csv_as_tokenlaunches("reports/dex_trending_real.csv")
print(f"[init] CSV tokens: {[(t.t_min, t.ticker, t.theme_hint, max(t.true_multiple_path or [1])) for t in raw]}")

# Build
ex = JupiterExecutor.from_env()
mm = json.loads(Path("ticker_mint_map.json").read_text())
srv = ServerState(ex, "", mm, 200.0, 100, "reports/dex_trending_real.csv", 50.0)
queue = start_live_queue(maxsize=200)
runner = DeskRunner(srv, tick_ms=300, seed=7, max_positions=3)

# Watch thread
def watch():
    time.sleep(0.5)
    while True:
        try:
            st = queue.get(timeout=5)
            t = st["t"]
            eq_pts = len(st["equity"])
            open_pos = len(st["positions"])
            closed = len(st["closed_trades"])
            mon = st["monitor"]
            pending_swap = mon.get("pending_swaps", 0)
            submitted = mon.get("submitted_swaps", 0)
            state = st["state"]
            print(f"[t={t:3d}] bankroll=${mon['bankroll']:8.2f} mult={mon['multiple']:.2f}x entries={mon['entries']} open={open_pos} closed={closed} pending_swap={pending_swap} state={state}")
            for f in st["feed"][-2:]:
                print(f"    FEED [{f['side']:5s}] {f['ticker']:12s} mult={f['mult']:.2f}x {f['note'][:40]}")
            for sw in st.get("swaps", {}).get("pending", []):
                print(f"    SWAP [{sw['side']:4s}] {sw['ticker']:12s} ${sw['amount_usd']:.2f} impact={sw['quote']['price_impact_pct']:.4f}%")
            if st.get("done"):
                print("[DONE] DeskRunner finished!")
                break
        except Exception as e:
            print(f"[watch] timeout: {e}")
            break

wt = threading.Thread(target=watch, daemon=True)
wt.start()
runner.start()
runner._thread.join(timeout=60)

print("\n" + "="*60)
print(f"Total ticks: {runner._tick_count}")
print(f"Closed trades: {len(runner.closed_trades)}")
for ct in runner.closed_trades[-5:]:
    print(f"  {ct['ticker']} PnL={ct['pnl_mult']:.2f}x (${ct['pnl_usd']:.2f}) {ct['win']}")
print(f"Pending swaps: {list(srv.trader.pending_swaps.keys())}")
print(f"Submitted swaps: {list(srv.trader.submitted_swaps.keys())}")
