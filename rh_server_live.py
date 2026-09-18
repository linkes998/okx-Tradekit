#!/usr/bin/env python3
"""RH Trencher — Live Trading Dashboard with Wallet Connect & One-Click Swaps.

Wires LiveTrader into the desk signal pipeline so users can:
  1. Connect their Solana wallet (paste pubkey)
  2. View incoming BUY/SELL signals from the strategy engine
  3. Review quote details (price impact, fee, expected output)
  4. One-click submit signed swap bundles via RPC

Architecture:
  rh_trencher.Desk(signal_callback=trader.on_desk_fill)
       ↓ fills → trader.pending_swaps
  rh_server_live.HTTPServer exposes /api/ swaps + /events SSE feed
"""
from __future__ import annotations

import json
import os
import queue
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from typing import Any

from rh_trencher import Desk, TokenLaunch, scenario
from rh_live_trader import LiveTrader, PendingSwap
from rh_jupiter_executor import JupiterExecutor
from db_trades import TradeDB
try:
    from rh_okx_executor import OKXExecutor
except ImportError:
    OKXExecutor = None

# ── Globals ──────────────────────────────────────────────────────
_server: "ServerState | None" = None

# ── Live-mode push pipeline ──────────────────────────────────────
_live_queue: queue.Queue[dict] | None = None
_runner: "DeskRunner | None" = None


def push_state(state: dict) -> None:
    if _live_queue is not None:
        try:
            _live_queue.put_nowait(state)
        except queue.Full:
            # Fallback: drop oldest and push new
            try:
                _live_queue.get_nowait()
                _live_queue.put_nowait(state)
            except queue.Empty:
                pass
    # Always update latest state for polling fallback
    global _latest_state
    _latest_state = state


# Global cache for polling fallback when SSE queue is full or client disconnected
_latest_state: dict | None = None


def start_live_queue(maxsize: int = 30) -> queue.Queue:
    global _live_queue
    _live_queue = queue.Queue(maxsize=maxsize)
    return _live_queue


# ── Server state object ─────────────────────────────────────────
class ServerState:
    """Shared mutable state for the live trading server."""

    def __init__(self, executor: JupiterExecutor, user_wallet: str,
                 ticker_mint_map: dict[str, str], sol_usd: float,
                 slippage_bps: int = 100, csv_path: str | None = None,
                 stake_usd: float = 50.0):
        self.executor = executor
        self.user_wallet = user_wallet
        self.ticker_mint_map = ticker_mint_map
        self.slippage_bps = slippage_bps
        self.stake_usd = stake_usd
        self.csv_path = csv_path
        self._sol_usd = sol_usd
        self._sol_price_fetched = False

        self.executor_type = type(executor).__name__
        self._feed_events: list[dict] = []
        self._swaps_event = threading.Event()
        # Polling fallback cache — updated on every push_state call
        self._state_lock = threading.Lock()
        self._last_state: dict | None = None
        self._last_state_ts: float = 0.0
        self.trader = LiveTrader(
            executor=executor,
            user_wallet=user_wallet,
            ticker_mint_map=ticker_mint_map,
            sol_usd=sol_usd,
            slippage_bps=slippage_bps,
            enable_buys=True,
            enable_sells=True,
            rpc_url=os.environ.get("RH_SOLANA_RPC_URL"),
            log_fn=self._log,
        )
        self.desk = Desk(
            start_usd=500,
            max_positions=3,
            live_mode=True,
            realistic=True,
            signal_callback=self.trader.on_desk_fill,
        )

    def _log(self, msg: str) -> None:
        print(f"[LIVE_SERVER] {msg}")
        self._feed_events.append({"t": int(time.time()), "msg": msg})
        if len(self._feed_events) > 200:
            self._feed_events = self._feed_events[-200:]
        self._swaps_event.set()

    def push_state(self, state: dict) -> None:
        """Update the internal _last_state cache so polling endpoints can serve it."""
        with self._state_lock:
            self._last_state = state
            self._last_state_ts = time.time()

    def get_latest_state(self) -> dict | None:
        with self._state_lock:
            return self._last_state

    def enqueue_swap(self, swap_key: str) -> dict:
        """Called by POST /api/swap/submit — fires up RPC submission."""
        return self.trader.submit_swap(swap_key)

    def confirm_swap(self, signature: str) -> dict:
        return self.trader.confirm_tx(signature)

    def refresh_sol_price(self) -> float:
        if self._sol_price_fetched:
            return self._sol_usd
        self._sol_price_fetched = True
        self._sol_usd = self.trader.refresh_sol_price()
        return self._sol_usd

    @property
    def sol_usd(self) -> float:
        return self._sol_usd

    def update_trade_mode(self, mode: str) -> None:
        """Called by /api/settings to update trade mode immediately."""
        if hasattr(self, 'runner') and self.runner:
            self.runner.update_trade_mode(mode)

    def manual_signal(self, ticker: str, side: str, usd: float, note: str = "") -> dict:
        """Manually inject a signal for testing."""
        from rh_trencher import Fill
        fill = Fill(
            t_min=int(time.time()), ticker=ticker, side=side,
            usd=usd, multiple=1.0, note=note,
        )
        ps = self.trader.on_desk_fill(fill)
        return {"ok": ps is not None, "ticker": ticker, "side": side,
                "key": next(iter(self.trader.pending_swaps.keys())) if ps else None}

    def status(self) -> dict:
        s = self.trader.status()
        s["user_wallet_display"] = (
            self.user_wallet[:8] + "..." + self.user_wallet[-6:]
            if len(self.user_wallet) > 14 else self.user_wallet
        )
        s["connected"] = bool(self.user_wallet)
        s["sol_usd"] = self._sol_usd
        s["recent_signals"] = self._feed_events[-20:]
        # OKX hot-swap status: reflect whether current executor is OKX + auth-ready
        exe = self.executor
        s["okx_connected"] = bool(exe and exe.__class__.__name__.startswith("OKX") and getattr(exe, "_auth_ready", False))
        s["executor_type"] = self.executor_type
        return s

    def swaps_snapshot(self) -> dict:
        """JSON-serializable view of pending + submitted + failed swaps."""
        out: dict[str, list] = {"pending": [], "submitted": [], "failed": []}
        for key, ps in self.trader.pending_swaps.items():
            b = getattr(ps, "bundle", None)
            q = getattr(ps, "quote", None)
            out["pending"].append({
                "key": key,
                "ticker": getattr(ps, "ticker", ""),
                "side": getattr(ps, "side", ""),
                "amount_usd": getattr(ps, "amount_usd", 0),
                "fee_usd": getattr(ps, "fee_amount_usd", 0),
                "note": getattr(ps, "note", ""),
                "tx_b64": b.transaction_base64 if b else None,
                "bundle": {"transaction_base64": b.transaction_base64} if b else None,
                "created_at": getattr(ps, "created_at", ""),
                "quote": {
                    "in_amount": getattr(q, "in_amount", None),
                    "out_amount": getattr(q, "out_amount", None),
                    "price_impact_pct": getattr(q, "price_impact_pct", None),
                    "fee_bps": getattr(q, "fee_bps", None),
                    "fee_amount": getattr(q, "fee_amount", None),
                } if q else None,
            })
        for key, v in self.trader.submitted_swaps.items():
            out["submitted"].append({**v, "key": key})
        for key, v in self.trader.failed_swaps.items():
            out["failed"].append({**v, "key": key})
        return out

    def run_replay_once(self) -> list[dict]:
        """Run one replay and return all state dicts (for testing)."""
        states = []
        for s in step_live_replay(server=self):
            states.append(s)
        return states


# ── Replay helper ───────────────────────────────────────────────
def step_live_replay(seed: int = 7, loss_pause_n: int = 3,
                     max_positions: int = 3, thin_cut: float | None = None,
                     server: ServerState | None = None,
                     max_slippage_pct: float = 0.02, peak_proxy_coef: float = 0.6):
    """Run the replay with the server's LiveTrader attached."""
    from rh_trencher import scenario as _scenario

    rng = __import__("numpy").random.default_rng(seed)
    desk = Desk(narrative_seed=seed, loss_pause_n=loss_pause_n,
                max_positions=max_positions, thin_cut_override=thin_cut,
                live_mode=True, realistic=True,
                max_slippage_pct=max_slippage_pct, peak_proxy_coef=peak_proxy_coef)

    if server and server.csv_path:
        try:
            from fetch_dexscreener import load_csv_as_tokenlaunches
            raw_tokens = load_csv_as_tokenlaunches(server.csv_path)
        except Exception:
            raw_tokens = _scenario(rng)
    else:
        raw_tokens = _scenario(rng)

    events = sorted(raw_tokens, key=lambda t: (t.t_min, t.ticker))
    i, pending_marks = 0, []

    def schedule_marks(tok, entry_t):
        for j, m in enumerate(tok.true_multiple_path or []):
            pending_marks.append((entry_t + 1 + j * 2, tok, m))

    seen, scatter_tokens = set(), []
    for t in raw_tokens:
        if t.ticker not in seen:
            scatter_tokens.append(t)
            seen.add(t.ticker)
    scatter_meta = [{"ticker": t.ticker, "theme": t.theme_hint,
                     "desc": (t.description or "")[:40]} for t in scatter_tokens]

    open_trades: dict[str, dict] = {}
    closed_trades: list[dict] = []
    trader = server.trader if server else None

    def make_state(t):
        nonlocal open_trades, closed_trades
        for f in desk.feed[-50:]:
            if trader:
                trader.on_desk_fill(f)
            if f.side == "ENTRY" and f.ticker not in open_trades:
                pos = next((p for p in desk.positions if p.ticker == f.ticker), None)
                entry_usd = pos.entry_usd if pos else f.usd
                open_trades[f.ticker] = {"entry_min": f.t_min, "entry_usd": round(entry_usd, 2)}
            elif f.side in ("EXIT", "STOP") and f.ticker in open_trades:
                ot = open_trades.pop(f.ticker)
                exit_usd = round(ot["entry_usd"] * f.multiple, 2)
                pnl_mult = round(f.multiple, 3)
                pnl_usd = round(exit_usd - ot["entry_usd"], 2)
                closed_trades.append({
                    "ticker": f.ticker, "entry_min": ot["entry_min"],
                    "entry_usd": ot["entry_usd"],
                    "exit_min": f.t_min, "exit_usd": exit_usd,
                    "pnl_mult": pnl_mult, "pnl_usd": pnl_usd,
                    "why": f.note or "", "win": f.multiple >= 1.0,
                })

        df = desk.narrative.project_2d(scatter_tokens)
        pts = df[["x", "y"]].values.tolist() if hasattr(df, "values") else []
        centroid_xy = None
        c = desk.narrative.cluster_centroid
        if c is not None:
            import numpy as np
            X = np.vstack([desk.narrative.embed_token(t) for t in scatter_tokens] + [c])
            X = X - X.mean(axis=0)
            U, S, _ = np.linalg.svd(X, full_matrices=False)
            xy_all = (U[:, :2] * S[:2]).tolist()
            pts = xy_all[:-1]
            centroid_xy = xy_all[-1]

        snap = desk.snapshot(t)
        equity = [(int(ts), float(eq)) for ts, eq, _ in desk.equity_curve]
        tail = desk.feed[-20:]
        feed = [{"t": f.t_min, "side": f.side, "ticker": f.ticker or "-",
                 "mult": round(f.multiple, 2), "note": f.note} for f in tail]
        positions = []
        for p in desk.positions:
            positions.append({
                "ticker": p.ticker, "entry_min": p.entry_min,
                "entry_usd": round(p.entry_usd, 2),
                "peak_mult": round(p.peak_mult, 3),
                "current_mult": round(p.current_mult, 3),
                "size_frac": round(p.size_frac * 100, 1),
            })
        wins = sum(1 for ct in closed_trades if ct["win"])
        losses = len(closed_trades) - wins
        monitor = {
            "multiple": round(desk.bankroll / desk.start, 2),
            "bankroll": round(desk.bankroll, 2),
            "total_pools": len(raw_tokens),
            "entries": desk.entered,
            "wins": wins, "losses": losses,
            "dominant_theme": desk.narrative.cluster_label,
            "profit_rate": round(wins / len(closed_trades), 3) if closed_trades else 0.0,
            "open_count": len(desk.positions),
            "state": desk.state.name,
        }
        if trader:
            swaps = trader.status()
            monitor["pending_swaps"] = swaps["pending_count"]
            monitor["submitted_swaps"] = swaps["submitted_count"]
        return {
            "t": int(t), "snap": snap, "equity": equity,
            "scatter": {"points": pts, "centroid": centroid_xy, "tokens": scatter_meta},
            "feed": feed, "positions": positions,
            "closed_trades": list(reversed(closed_trades[-50:])),
            "monitor": monitor, "bankroll": round(desk.bankroll, 2),
            "state": desk.state.name,
            "narrative": {"label": desk.narrative.cluster_label,
                          "min_match": desk.narrative.min_match},
            "done": False,
        }

    yield make_state(0)
    for t in raw_tokens:
        if t.t_min not in {e.t_min for e in events[:i]}:
            pass  # simplified loop — see rh_server.py for full logic
        i += 1
        desk.on_launch(t)
        desk.equity_curve.append((t.t_min, desk.bankroll, "tick"))
        yield make_state(t.t_min)
    s = make_state(500)
    s["done"] = True
    yield s


# ──────────────────────────────────────────────────────────────────────
#  HTML
# ──────────────────────────────────────────────────────────────────────
INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AI Trade Desk · OKX Spot & Futures</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Manrope:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
/* ── V1 Terminal Design Tokens ── */
:root{
  --bg:#0a0b0d;--surface:#111317;--surface2:#181c23;
  --border:#232830;--border-bright:#2e3540;
  --text:#e4e8f0;--text-dim:#6b7280;--text-mid:#9ca3af;
  --green:#22c55e;--green-dim:#166534;--green-glow:rgba(34,197,94,0.15);
  --red:#ef4444;--red-dim:#7f1d1d;
  --amber:#f59e0b;--amber-dim:#92400e;
  --blue:#3b82f6;--blue-dim:#1e3a5f;
  --cyan:#06b6d4;--purple:#a855f7;
  --mono:'JetBrains Mono',monospace;--sans:'Manrope',sans-serif;
  --hi:#22c55e;--warn:#ef4444;--gold:#f59e0b;--dim:#6b7280;--fg:#e4e8f0;--line:#232830;--card:#111317;
}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:12px;line-height:1.5;overflow-x:hidden}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border-bright);border-radius:2px}

/* ── LAYOUT: sidebar + topbar + main ── */
.app{display:grid;grid-template-columns:220px 1fr;grid-template-rows:48px 1fr;min-height:100vh}
.topbar{grid-column:1/-1;background:var(--surface);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 20px;gap:20px;z-index:100;height:48px}
.logo{font-family:var(--mono);font-weight:700;font-size:14px;letter-spacing:.08em;color:var(--cyan);text-transform:uppercase;cursor:pointer}
.logo span{color:var(--text-dim);font-weight:400}
.topbar-sep{width:1px;height:20px;background:var(--border)}
.topbar-stat{display:flex;flex-direction:column;gap:1px}
.topbar-stat-label{font-size:9px;text-transform:uppercase;letter-spacing:.1em;color:var(--text-dim);font-family:var(--mono)}
.topbar-stat-val{font-family:var(--mono);font-size:13px;font-weight:500;color:var(--text)}
.topbar-stat-val.up{color:var(--green)}
.topbar-stat-val.down{color:var(--red)}
.equity-bkdn{font-size:8px;color:var(--text-dim);letter-spacing:.05em;margin-left:4px;white-space:nowrap}
.topbar-spacer{flex:1}
.topbar-badge{font-family:var(--mono);font-size:10px;padding:2px 8px;border-radius:2px;border:1px solid var(--border-bright);color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em;cursor:default}
.topbar-badge.live{border-color:var(--green);color:var(--green);background:rgba(34,197,94,0.15)}
.topbar-badge.demo{border-color:var(--amber);color:var(--amber);background:rgba(245,158,11,0.15)}
.topbar-btn{font-family:var(--mono);font-size:10px;padding:4px 10px;border-radius:2px;border:1px solid var(--border);background:transparent;color:var(--text-mid);cursor:pointer;text-transform:uppercase;letter-spacing:.05em;transition:all .15s}
.topbar-btn:hover{border-color:var(--cyan);color:var(--cyan)}
.topbar-btn.active{border-color:var(--cyan);color:var(--cyan);background:rgba(6,182,212,0.1)}
.topbar-btn.primary{background:var(--cyan);color:var(--bg);border-color:var(--cyan);font-weight:600}
.topbar-btn.primary:hover{background:#0891b2}
.topbar-btn.lang{padding:4px 8px}

/* Sidebar */
.sidebar{background:var(--surface);border-right:1px solid var(--border);padding:16px 0;display:flex;flex-direction:column;gap:2px}
.nav-section{padding:8px 16px 4px;font-size:9px;text-transform:uppercase;letter-spacing:.15em;color:var(--text-dim);font-family:var(--mono)}
.nav-item{display:flex;align-items:center;gap:10px;padding:7px 16px;cursor:pointer;color:var(--text-mid);transition:all .15s;border-left:2px solid transparent;font-size:12px}
.nav-item:hover{background:var(--surface2);color:var(--text)}
.nav-item.active{background:var(--surface2);color:var(--cyan);border-left-color:var(--cyan)}
.nav-item svg{width:14px;height:14px;opacity:.7}
.nav-item .badge{margin-left:auto;font-family:var(--mono);font-size:9px;background:var(--blue-dim);color:var(--blue);padding:1px 5px;border-radius:2px}
.tier-badge{font-family:var(--mono);font-size:9px;padding:2px 6px;border-radius:2px;text-transform:uppercase;letter-spacing:.05em}
.tier-badge.free{background:var(--surface2);color:var(--text-dim);border:1px solid var(--border)}
.tier-badge.premium{background:rgba(255,193,7,.15);color:#ffc107;border:1px solid rgba(255,193,7,.3)}
.tier-badge.admin{background:rgba(220,53,69,.15);color:#dc3545;border:1px solid rgba(220,53,69,.3)}

.main{overflow-y:auto;padding:20px 24px}

/* Page sections */
.page-section{display:none;flex-direction:column;gap:16px}
.page-section.active{display:flex}
.page-title{font-family:var(--mono);font-size:11px;text-transform:uppercase;letter-spacing:.15em;color:var(--text-dim);margin-bottom:4px}
.page-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.page-title-main{font-size:14px;font-weight:600;color:var(--text)}
.page-subtitle{font-size:11px;color:var(--text-dim);font-family:var(--mono)}

/* Metrics Row */
.metrics-row{display:grid;grid-template-columns:repeat(5,1fr);gap:1px;background:var(--border);border:1px solid var(--border);border-radius:4px;overflow:hidden;margin-bottom:16px}
.metric-card{background:var(--surface);padding:12px 16px;display:flex;flex-direction:column;gap:4px;min-height:72px;justify-content:center}
.metric-label{font-size:9px;text-transform:uppercase;letter-spacing:.12em;color:var(--text-dim);font-family:var(--mono);margin-bottom:4px}
.metric-value{font-family:var(--mono);font-size:22px;font-weight:700;color:var(--text);line-height:1.1;letter-spacing:-.02em}
.metric-value.up{color:var(--green)}
.metric-value.down{color:var(--red)}
.metric-value.win{color:var(--amber)}
.metric-value.blue{color:var(--blue)}
.metric-sub{font-size:10px;color:var(--text-dim);font-family:var(--mono);margin-top:2px}
.metric-sub span{color:var(--green)}
.metric-sub span.red{color:var(--red)}

/* Grid layouts */
.grid-main-side{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(380px,1fr);gap:1px;background:var(--border);border:1px solid var(--border);border-radius:4px;overflow:hidden;margin-bottom:16px}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--border);border:1px solid var(--border);border-radius:4px;overflow:hidden;margin-bottom:16px}
.grid-full{border:1px solid var(--border);border-radius:4px;overflow:hidden;margin-bottom:16px}

/* Panels */
.panel{background:var(--surface);padding:0}
.panel.with-pad{padding:16px}
.panel-header{display:flex;align-items:center;justify-content:space-between;padding:14px 16px 0;margin-bottom:12px}
.panel-title{font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:var(--text-dim);display:flex;align-items:center;gap:8px}
.panel-title .dot{width:6px;height:6px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green)}
.panel-title .dot.amber{background:var(--amber);box-shadow:0 0 6px var(--amber)}
.panel-title .dot.red{background:var(--red);box-shadow:0 0 6px var(--red)}
.panel-title .dot.blue{background:var(--blue);box-shadow:0 0 6px var(--blue)}
.panel-action{font-family:var(--mono);font-size:9px;color:var(--text-dim);cursor:pointer;text-transform:uppercase;letter-spacing:.08em;transition:color .15s,background .15s}
.panel-action:hover{color:var(--cyan);background:rgba(0,229,255,0.06)}
.panel-action.active{color:var(--cyan);background:rgba(0,229,255,0.1)}

/* Canvas Chart */
.chart-wrap{position:relative;width:100%;height:260px;overflow:hidden;padding:0 16px 12px}
.chart-wrap canvas{display:block;width:100%;height:100%}
.equity-card .chart-wrap{height:280px}

/* Filter bar */
.filter-bar{display:flex;align-items:center;gap:4px;padding:8px 16px;background:var(--surface2);border-top:1px solid var(--border);flex-wrap:wrap}
.filter-group{display:flex;align-items:center;gap:2px}
.filter-label{font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.08em;color:var(--text-dim);margin-right:6px}
.filter-tab{font-family:var(--mono);font-size:9px;padding:3px 8px;border-radius:2px;border:1px solid transparent;background:transparent;color:var(--text-mid);cursor:pointer;text-transform:uppercase;letter-spacing:.05em;transition:all .12s}
.filter-tab:hover{color:var(--text);background:var(--border)}
.filter-tab.active{background:var(--cyan);color:var(--bg);border-color:var(--cyan);font-weight:600}
.filter-search{background:var(--surface);border:1px solid var(--border);color:var(--text);padding:4px 10px;font-family:var(--mono);font-size:10px;border-radius:2px;width:130px}
.filter-search:focus{outline:none;border-color:var(--cyan)}

/* Signal Feed list */
.feed-list{display:flex;flex-direction:column;min-height:280px;max-height:320px;overflow-y:auto}
.feed-item{display:flex;gap:10px;padding:8px 16px;border-bottom:1px solid var(--border);transition:background .12s}
.feed-item:last-child{border-bottom:none}
.feed-item:hover{background:var(--surface2)}
.feed-item.hidden{display:none}
.feed-time{color:var(--text-dim);font-size:9px;white-space:nowrap;flex-shrink:0;width:48px;text-align:right;font-variant-numeric:tabular-nums;padding-top:2px;font-family:var(--mono)}
.feed-body{display:flex;flex-direction:column;gap:2px;flex:1;min-width:0}
.feed-ticker{font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:11px}
.feed-note{color:var(--text-dim);font-size:9px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-family:var(--mono)}
.feed-pnl{text-align:right;white-space:nowrap;flex-shrink:0;font-size:10px;min-width:56px;padding-top:2px;font-family:var(--mono);font-variant-numeric:tabular-nums}
.feed-pnl.side-buy,.feed-pnl.pnl-pos{color:var(--green)}
.feed-pnl.side-sell,.feed-pnl.pnl-neg{color:var(--red)}

/* Legacy pre-based feed (hidden) */
#feed-log{display:none}

/* Tables */
.data-table{width:100%;border-collapse:collapse;table-layout:fixed}
.data-table th{text-align:left;font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.1em;color:var(--text-dim);padding:8px 12px;border-bottom:1px solid var(--border);font-weight:400;white-space:nowrap;background:var(--surface2)}
.data-table td{padding:9px 12px;font-family:var(--mono);font-size:11px;border-bottom:1px solid var(--border);vertical-align:middle;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.data-table tbody tr{transition:background .1s}
.data-table tbody tr:last-child td{border-bottom:none}
.data-table tbody tr:hover td{background:var(--surface2)}
.side-buy{color:var(--green)}
.side-sell{color:var(--red)}
.pnl-pos{color:var(--green)}
.pnl-neg{color:var(--red)}
.tag{display:inline-block;font-family:var(--mono);font-size:9px;padding:1px 6px;border-radius:2px;text-transform:uppercase;letter-spacing:.05em}
.tag-okx{background:var(--blue-dim);color:var(--blue)}
.tag-jup{background:rgba(168,85,247,0.15);color:var(--purple)}
.tag-long{background:var(--green-dim);color:var(--green)}
.tag-short{background:var(--red-dim);color:var(--red)}

/* Decision list */
#decision-list{display:flex;flex-direction:column}
.decision-item{padding:10px 16px;border-bottom:1px solid var(--border);transition:background .12s}
.decision-item:last-child{border-bottom:none}
.decision-item:hover{background:var(--surface2)}
.decision-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;gap:8px}
.decision-pair{font-weight:600;color:var(--text);font-size:12px;font-family:var(--mono)}
.decision-side{padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;font-family:var(--mono);text-transform:uppercase;letter-spacing:.05em}
.decision-side.buy{background:var(--green-dim);color:var(--green)}
.decision-side.sell{background:var(--red-dim);color:var(--red)}
.decision-time{color:var(--text-dim);font-size:10px;font-family:var(--mono)}
.decision-reasons{margin:6px 0 4px 16px;color:var(--text-dim);font-size:10px;line-height:1.6}
.decision-risk{color:var(--amber);font-size:10px;margin-top:4px;font-family:var(--mono)}
.decision-actions{display:flex;gap:6px;margin-top:8px}
.decision-actions button{font-family:var(--mono);font-size:9px;padding:4px 12px;border-radius:2px;cursor:pointer;font-weight:600;text-transform:uppercase;letter-spacing:.05em;transition:all .15s}
.btn-confirm{background:var(--green);color:var(--bg);border:0}
.btn-confirm:hover{background:#16a34a}
.btn-ignore{background:transparent;color:var(--text-mid);border:1px solid var(--border)}
.btn-ignore:hover{border-color:var(--text-dim);color:var(--text)}
.btn-auto-executed{color:var(--text-dim);font-size:10px;padding:4px 0;font-family:var(--mono)}

/* Landing */
.landing-wrap{display:flex;justify-content:center;padding:40px 0}
.landing-inner{background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:36px;text-align:center;max-width:640px}
.landing-inner h2{font-family:var(--mono);font-size:14px;color:var(--cyan);margin-bottom:8px;letter-spacing:.04em}
.landing-inner .sub{color:var(--text-dim);font-size:11px;line-height:1.7;margin-bottom:24px}
.landing-inner .step-list{text-align:left;margin:16px 0 28px;display:flex;flex-direction:column;gap:12px}
.landing-inner .step-item{display:flex;align-items:flex-start;gap:12px;font-size:12px;color:var(--text)}
.landing-inner .step-num{flex-shrink:0;width:24px;height:24px;border-radius:50%;background:var(--cyan);color:var(--bg);font-size:11px;font-weight:700;display:flex;align-items:center;justify-content:center;font-family:var(--mono)}
.landing-inner .btn-row{display:flex;gap:12px;justify-content:center;margin-top:12px}
.landing-inner .risk-note{color:var(--text-dim);font-size:10px;margin-top:20px;line-height:1.5}

/* Risk bar */
#risk-bar{background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:10px 16px;margin-bottom:14px;display:flex;align-items:center;gap:14px;font-size:11px}
.risk-label{color:var(--text-dim);font-family:var(--mono);text-transform:uppercase;letter-spacing:.1em;font-size:9px;white-space:nowrap}
.risk-track{flex:1;height:6px;background:var(--border);border-radius:3px;overflow:hidden}
.risk-fill{height:100%;border-radius:3px;transition:width .4s ease}
.risk-text{color:var(--text);font-family:var(--mono);white-space:nowrap;font-size:11px}

/* Swap bundle actions */
.action-btn{background:var(--green);color:var(--bg);border:0;padding:4px 10px;font-size:10px;cursor:pointer;font-family:var(--mono);font-weight:700;border-radius:2px;text-transform:uppercase;letter-spacing:.04em}
.action-btn:disabled{opacity:.3;cursor:not-allowed}
.action-btn.failed{background:var(--red);color:#fff}
.tx-link{color:var(--blue);text-decoration:none;font-size:10px}
.tx-link:hover{text-decoration:underline}

/* Manual signal */
.sig-row{display:flex;gap:8px;align-items:center;margin:12px 0;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:4px;flex-wrap:wrap}
.sig-row label{color:var(--text-dim);font-size:11px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.08em}
.sig-row input,.sig-row select{background:var(--surface);border:1px solid var(--border);color:var(--text);padding:5px 8px;font-size:11px;font-family:var(--mono);border-radius:2px}
.sig-row button{background:var(--cyan);color:var(--bg);border:0;padding:5px 14px;font-size:10px;font-family:var(--mono);font-weight:700;border-radius:2px;cursor:pointer;text-transform:uppercase;letter-spacing:.05em}
.sig-row button:hover{background:#0891b2}

/* Badge dots */
.badge-dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--red);animation:blink 2s infinite;vertical-align:middle}
.badge-dot.live{background:var(--green);box-shadow:0 0 6px var(--green);animation:blink 2s infinite}
.badge-dot.done{background:var(--amber);animation:none;box-shadow:0 0 6px var(--amber)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.35}}

/* Tooltip */
.has-tip{position:relative;cursor:help}
.has-tip::after{content:attr(data-tip);position:absolute;bottom:calc(100% + 6px);left:50%;transform:translateX(-50%);background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:6px 10px;font-size:11px;font-family:var(--mono);white-space:pre-line;max-width:320px;width:max-content;border-radius:4px;box-shadow:0 4px 12px rgba(0,0,0,.5);pointer-events:none;opacity:0;transition:opacity .15s ease;z-index:999;line-height:1.5}
.has-tip:hover::after{opacity:1}

/* Modal */
.modal-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.7);z-index:1000;display:flex;align-items:center;justify-content:center}
.modal-overlay.hidden{display:none}
.modal-box{background:var(--surface);border:1px solid var(--cyan);border-radius:6px;padding:24px;max-width:560px;width:90%;max-height:85vh;overflow-y:auto}
.modal-box h2{margin:0 0 16px;font-size:14px;color:var(--cyan);font-family:var(--mono);letter-spacing:.04em}
.modal-box .close-btn{float:right;background:none;border:none;color:var(--text-dim);font-size:18px;cursor:pointer;padding:0 4px}
.modal-box .close-btn:hover{color:var(--text)}
.risk-options,.mode-options{display:flex;gap:10px;margin:12px 0}
.risk-option,.mode-option{flex:1;padding:12px;border:1px solid var(--border);border-radius:4px;cursor:pointer;text-align:center;transition:border-color .2s;background:var(--surface2)}
.risk-option:hover,.mode-option:hover{border-color:var(--border-bright)}
.risk-option.active,.mode-option.active{border-color:var(--cyan);background:rgba(6,182,212,0.08)}
.risk-name,.mode-name{font-weight:600;font-size:12px;color:var(--text);font-family:var(--mono);text-transform:uppercase;letter-spacing:.05em}
.risk-desc,.mode-desc{font-size:10px;margin-top:6px;color:var(--text-dim);line-height:1.5}

/* ── Member Center: Trade Settings ─────────────────────────────────────── */
.btn-save{background:var(--green);color:var(--bg);border:none;padding:5px 14px;border-radius:3px;font-family:var(--mono);font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;cursor:pointer;transition:all .15s}
.btn-save:hover{background:#059669;box-shadow:0 0 8px rgba(16,185,129,.3)}
.risk-options-member,.mode-options-member{gap:10px}
.risk-option-member,.mode-option-member{padding:14px 12px;border:1px solid var(--border);border-radius:4px;cursor:pointer;text-align:center;transition:all .2s;background:var(--surface2)}
.risk-option-member:hover,.mode-option-member:hover{border-color:var(--border-bright);transform:translateY(-1px)}
.risk-option-member.active,.mode-option-member.active{border-color:var(--cyan);background:rgba(6,182,212,0.1);box-shadow:0 0 12px rgba(6,182,212,.15)}
.risk-member-icon,.mode-member-icon{font-size:20px;margin-bottom:6px}
.risk-member-name,.mode-member-name{font-weight:700;font-size:12px;color:var(--text);font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px}
.risk-member-desc,.mode-member-desc{font-size:10px;color:var(--text-dim);line-height:1.5}

/* Ticker multi-select picker */
.ticker-picker-trigger{position:relative}
.ticker-picker-trigger:focus-within{border-color:var(--cyan)}
.ticker-tag{display:inline-flex;align-items:center;gap:4px;background:rgba(6,182,212,0.15);border:1px solid rgba(6,182,212,0.3);color:var(--cyan);font-size:10px;font-family:var(--mono);padding:2px 6px;border-radius:2px;text-transform:uppercase}
.ticker-tag button{background:none;border:none;color:var(--cyan);cursor:pointer;font-size:10px;line-height:1;padding:0 2px;opacity:.7}
.ticker-tag button:hover{opacity:1}
.ticker-item{display:flex;align-items:center;gap:6px;padding:5px 8px;border-radius:3px;cursor:pointer;font-size:11px;font-family:var(--mono);color:var(--text-mid);transition:all .1s;text-transform:uppercase}
.ticker-item:hover{background:rgba(6,182,212,0.1);color:var(--text)}
.ticker-item.selected{color:var(--cyan);background:rgba(6,182,212,0.12)}
.ticker-item input[type=checkbox]{accent-color:var(--cyan);cursor:pointer}
.modal-section{margin-bottom:18px}
.modal-section label{color:var(--text-dim);font-size:11px;text-transform:uppercase;letter-spacing:.1em;display:block;margin-bottom:8px;font-family:var(--mono)}
.modal-footer{display:flex;justify-content:flex-end;gap:8px;margin-top:20px}
.modal-footer button{padding:6px 18px;border-radius:4px;font-family:var(--mono);font-size:11px;cursor:pointer;font-weight:600;text-transform:uppercase;letter-spacing:.04em}
.modal-footer .btn-primary{background:var(--cyan);color:var(--bg);border:0}
.modal-footer .btn-secondary{background:transparent;color:var(--text-mid);border:1px solid var(--border)}
.strat-content{font-size:11px;line-height:1.7;color:var(--text);margin-top:8px}
.strat-content h3{color:var(--cyan);font-size:12px;margin:14px 0 6px;font-family:var(--mono)}
.strat-content ul{padding-left:18px;margin:4px 0}
.strat-content li{margin-bottom:4px;color:var(--text-dim)}

.empty-row{text-align:center;color:var(--text-dim);padding:20px;font-family:var(--mono);font-size:11px;border:none}

/* Pager bar */
.pager-bar{display:flex;align-items:center;justify-content:center;gap:5px;padding:6px 12px;border-top:1px solid var(--border);background:var(--surface)}
.page-btn{background:transparent;border:1px solid var(--border);color:var(--text-dim);padding:2px 8px;border-radius:3px;font-size:11px;cursor:pointer;font-family:var(--mono);transition:all .12s}
.page-btn:hover{border-color:var(--cyan);color:var(--cyan)}
.page-btn.active{background:rgba(6,182,212,0.15);border-color:var(--cyan);color:var(--cyan)}
.page-info{font-size:10px;color:var(--text-dim);font-family:var(--mono);margin-right:8px}
</style>
</head>
<body>
<div class="app">
  <!-- Top Bar -->
  <div class="topbar">
    <div class="logo" onclick="document.location.reload()">AI<span>Trade</span>Desk</div>
    <div class="topbar-sep"></div>
    <div class="topbar-stat"><div class="topbar-stat-label">Equity<span id="equity-breakdown" class="equity-bkdn" style="display:none"></span></div><div class="topbar-stat-val up" id="topbar-equity">$—</div></div>
    <div class="topbar-stat"><div class="topbar-stat-label">24h PnL</div><div class="topbar-stat-val" id="topbar-pnl">—</div></div>
    <div class="topbar-stat"><div class="topbar-stat-label">Win Rate</div><div class="topbar-stat-val" id="topbar-winrate">—</div></div>
    <div class="topbar-stat"><div class="topbar-stat-label">Trades</div><div class="topbar-stat-val" id="topbar-trades">—</div></div>
    <div class="topbar-stat"><div class="topbar-stat-label">Kelly Used</div><div class="topbar-stat-val" id="topbar-kelly">—</div></div>
    <div class="topbar-spacer"></div>
    <span id="mon-data-source" class="topbar-badge demo" style="display:none" title="Data source">● DEMO</span>
    <span id="mon-demo-badge" class="topbar-badge demo" style="display:none">DEMO</span>
    <span id="pill" class="topbar-badge live">● IDLE</span>
    <button id="btnMemberCenter" class="topbar-btn" onclick="navigateToPage('members')" data-i18n="nav_members">Members</button>
    <button id="btnLangEN" class="topbar-btn lang active" onclick="setLang('en')">EN</button>
    <button id="btnLangZH" class="topbar-btn lang" onclick="setLang('zh')">中文</button>
    <button class="topbar-btn lang" onclick="manualTest()">DEBUG</button>
    <div id="user-status" style="margin-left:8px;font-size:10px;color:var(--text-dim)"></div>
  </div>

  <!-- Hidden backup IDs for legacy JS -->
  <div style="display:none">
    <span id="mon-mult"></span><span id="mon-bank"></span><span id="mon-entry"></span>
    <span id="mon-wl"></span><span id="mon-pending"></span><span id="mon-submitted"></span>
    <span id="mon-state"></span><span id="mon-pnl"></span><span id="mon-mode"></span><span id="mon-sol"></span>
  </div>
  <!-- Sidebar -->
  <div class="sidebar">
    <div class="nav-section" data-i18n="nav_overview">Overview</div>
    <div class="nav-item active" data-page="dashboard"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg><span data-i18n="nav_dashboard">Dashboard</span></div>
    <div class="nav-item" data-page="positions"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg><span data-i18n="nav_positions">Positions</span><span class="badge" id="nav-positions-count">0</span></div>
    <div class="nav-item" data-page="trades"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg><span data-i18n="nav_trades">Trades</span></div>
    <div class="nav-section" data-i18n="nav_engine">Engine</div>
    <div class="nav-item" data-page="spot"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.07 4.93A10 10 0 1 0 4.93 19.07"/></svg><span data-i18n="nav_spot">OKX Spot</span></div>
    <div class="nav-item" data-page="perp"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg><span data-i18n="nav_perp">Perp Engine</span></div>
    <div class="nav-section" data-i18n="nav_analytics">Analytics</div>
    <div class="nav-item" data-page="kelly"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.21 15.89A10 10 0 1 1 8 2.83"/><path d="M22 12A10 10 0 0 0 12 2v10z"/></svg><span data-i18n="nav_kelly">Kelly Criterion</span></div>
    <div class="nav-item" data-page="reports"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg><span data-i18n="nav_reports">Reports</span></div>
    <div class="nav-section" data-i18n="nav_community">Community</div>
    <div class="nav-item" data-page="leaderboard"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 21V7m0 0L4 11m4-4l4 4M16 21V11m0 0l-4-4-4 4"/><path d="M3 3v18h18"/></svg><span data-i18n="nav_leaderboard">Leaderboard</span></div>
    <div class="nav-item" data-page="members"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg><span data-i18n="nav_members">Members</span></div>
  </div>

  <!-- Main -->
  <div class="main">

    <!-- ═══ DASHBOARD PAGE ═══ -->
    <div class="page-section active" id="page-dashboard">
    <div class="sig-row" id="manualRow" style="display:none">
      <label>Ticker:</label><input id="manualTicker" type="text" value="BONK" style="width:90px">
      <label>Side:</label><select id="manualSide"><option value="ENTRY">ENTRY (BUY)</option><option value="EXIT">EXIT (SELL)</option></select>
      <label>USD:</label><input id="manualUsd" type="number" value="50" min="1" step="1" style="width:72px">
      <button onclick="sendManualSignal()">SEND</button>
    </div>

    <div class="landing-wrap" id="landing-card" style="display:none">
      <div class="landing-inner">
        <h2 data-i18n="welcomeTitle">WELCOME TO AI TRADE DESK</h2>
        <p class="sub" data-i18n="welcomeDesc">An AI trading assistant designed for OKX Spot &amp; Futures.<br>AI scans potential pairs → decides timing &amp; position size → you confirm or fully automate.</p>
        <div class="step-list">
          <div class="step-item"><span class="step-num">1</span><span data-i18n="step1">Connect your OKX API (trading permissions only, never withdrawal)</span></div>
          <div class="step-item"><span class="step-num">2</span><span data-i18n="step2">Choose risk preference (Conservative / Balanced / Aggressive)</span></div>
          <div class="step-item"><span class="step-num">3</span><span data-i18n="step3">Start AI scanning — view decisions and positions in real time</span></div>
        </div>
        <div class="btn-row"><button class="topbar-btn primary" style="padding:8px 24px;font-size:12px" onclick="openApiModal()" data-i18n="connectOkxBtn">CONNECT OKX API</button></div>
        <div class="risk-note" data-i18n="riskDisclaimer">Trading involves risk. Past performance is not indicative of future results. Funds remain in your own OKX account.</div>
      </div>
    </div>

    <div id="risk-bar" style="display:none">
      <span class="risk-label" data-i18n="riskExposure">Risk Exposure</span>
      <div class="risk-track"><div class="risk-fill" id="risk-fill" style="width:0%;background:var(--green)"></div></div>
      <span class="risk-text" id="risk-text">0% / 8%</span>
    </div>

    <!-- Metrics Row -->
    <div class="metrics-row" id="monitor-card">
      <div class="metric-card">
        <div class="metric-label" data-i18n="bankroll">Bankroll</div>
        <div class="metric-value" id="mon-bank">—</div>
        <div class="metric-sub"><span id="mon-mult-sub">1.00x</span> <span class="red" id="topbar-pnl-mini"></span></div>
      </div>
      <div class="metric-card">
        <div class="metric-label" data-i18n="todayPnl">24h PnL</div>
        <div class="metric-value" id="mon-pnl">—</div>
        <div class="metric-sub" id="mon-pnl-sub">0.00%</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Open Positions</div>
        <div class="metric-value" id="mon-entry">—</div>
        <div class="metric-sub"><span id="mon-pos-sub">0 spot</span></div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Kelly Fraction</div>
        <div class="metric-value" id="mon-kelly-panel">—</div>
        <div class="metric-sub" id="mon-kelly-sub">ATR: —</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Builder Code</div>
        <div class="metric-value" style="font-size:14px">RB2-OKX</div>
        <div class="metric-sub">Comm returned: $—</div>
      </div>
    </div>

    <!-- Equity + Feed -->
    <div class="grid-main-side">
      <div class="panel with-pad" id="equity-card" style="padding:0">
        <div class="panel-header">
          <div class="panel-title"><span class="dot"></span> Equity Curve</div>
          <div style="display:flex;gap:12px">
            <span class="panel-action" style="color:var(--cyan)">3H</span><span class="panel-action">4H</span>
            <span class="panel-action">1D</span><span class="panel-action">ALL</span>
          </div>
        </div>
        <div class="chart-wrap"><canvas id="equity"></canvas></div>
      </div>

      <div class="panel with-pad" id="feed-card" style="padding:0">
        <div class="panel-header" style="padding:14px 16px 8px;margin-bottom:0">
          <div class="panel-title"><span class="dot amber"></span> Strategy Signals</div>
        </div>
        <div class="filter-bar">
          <div style="flex:1"></div>
          <input class="filter-search" type="text" placeholder="Search ticker..." id="feed-search">
        </div>
        <div id="feed-list" class="feed-list" style="min-height:280px;max-height:320px;overflow-y:auto">
          <div class="empty-row">— waiting for signals —</div>
        </div>
        <pre id="feed-log" style="display:none"></pre>
      </div>
    </div>

    <!-- Positions + Decisions -->
    <div class="grid-2">
      <div class="panel with-pad" id="positions-card" style="padding:0">
        <div class="panel-header"><div class="panel-title"><span class="dot blue"></span> Open Positions</div><span class="panel-action">View All →</span></div>
        <table class="data-table">
          <thead><tr><th>Ticker</th><th>Side</th><th>Entry</th><th>Buy $</th><th>Peak</th><th>Current</th><th>Size%</th></tr></thead>
          <tbody id="pos-body"><tr><td colspan="7" class="empty-row">— no open positions —</td></tr></tbody>
        </table>
        <div style="border-top:1px solid var(--border)">
          <div class="panel-header" style="padding:10px 16px 6px"><div class="panel-title"><span class="dot"></span> History Trades</div></div>
          <table class="data-table">
            <thead><tr><th>Ticker</th><th>Side</th><th>Buy</th><th>Sell</th><th>P/L</th><th>Realized</th></tr></thead>
            <tbody id="hist-body"><tr><td colspan="6" class="empty-row">— no history yet —</td></tr></tbody>
          </table>
        </div>
      </div>

      <div class="panel with-pad" id="decision-log-card" style="padding:0">
        <div class="panel-header"><div class="panel-title"><span class="dot amber"></span> AI Decisions <span id="decision-badge" class="badge" style="background:var(--amber-dim);color:var(--amber);padding:1px 5px;border-radius:2px;font-size:9px">(0)</span></div></div>
        <div id="decision-list" style="max-height:460px;overflow-y:auto">
          <div class="empty-row">— no AI decisions yet —</div>
        </div>
      </div>
    </div>

    <!-- Swaps -->
    <div class="grid-full" id="swaps-card">
      <div class="panel-header"><div class="panel-title"><span class="dot"></span> Swap Bundles <span id="swap-badge" class="badge" style="background:var(--blue-dim);color:var(--blue);padding:1px 5px;border-radius:2px;font-size:9px">(0)</span></div></div>
      <table class="data-table">
        <thead><tr><th>Side</th><th>Ticker</th><th>Amount $</th><th>Fee $</th><th>Impact</th><th>Out (est)</th><th>Action</th></tr></thead>
        <tbody id="swap-body"><tr><td colspan="7" class="empty-row">— no pending swaps —</td></tr></tbody>
      </table>
      <div id="swap-pager" class="pager-bar" style="display:none"></div>
    </div>

    <!-- Perp -->
    <div class="grid-full" id="perp-positions-card" style="display:none">
      <div class="panel-header"><div class="panel-title"><span class="dot"></span> Perpetual Positions <span id="perp-pos-badge" class="badge" style="background:var(--purple);color:#fff;padding:1px 5px;border-radius:2px;font-size:9px">(0)</span></div></div>
      <table class="data-table">
        <thead><tr><th>Ticker</th><th>Side</th><th>Entry $</th><th>Current $</th><th>Notional</th><th>Leverage</th><th>PnL $</th><th>PnL %</th><th>Elapsed</th></tr></thead>
        <tbody id="perp-pos-body"><tr><td colspan="9" class="empty-row">— no perpetual positions —</td></tr></tbody>
      </table>
      <div id="perp-pager" class="pager-bar" style="display:none"></div>
    </div>
    </div><!-- /page-dashboard -->

    <!-- ═══ POSITIONS PAGE ═══ -->
    <div class="page-section" id="page-positions">
      <div class="page-header">
        <div><div class="page-title-main">Positions</div><div class="page-subtitle">Live spot &amp; perpetual positions across all platforms</div></div>
        <div style="display:flex;gap:8px">
          <button class="topbar-btn" onclick="refreshPositionsPage()">Refresh</button>
        </div>
      </div>
      <div class="grid-2">
        <div class="panel with-pad">
          <div class="panel-header"><div class="panel-title"><span class="dot blue"></span> OKX Spot Positions</div><span class="panel-action" id="spot-pos-badge" style="color:var(--blue)">0</span></div>
          <table class="data-table">
            <thead><tr><th>Ticker</th><th>Qty</th><th>Avg $</th><th>Current $</th><th>PnL $</th><th>PnL %</th></tr></thead>
            <tbody id="spot-pos-body"><tr><td colspan="6" class="empty-row">— no spot positions —</td></tbody>
          </table>
        </div>
        <div class="panel with-pad">
          <div class="panel-header"><div class="panel-title"><span class="dot" style="background:var(--purple);box-shadow:0 0 6px var(--purple)"></span> OKX Perp Positions</div><span class="panel-action" id="perp-pos-badge-page" style="color:var(--purple)">0</span></div>
          <table class="data-table">
            <thead><tr><th>Ticker</th><th>Side</th><th>Entry $</th><th>Notional</th><th>Leverage</th><th>PnL $</th><th>PnL %</th></tr></thead>
            <tbody id="perp-pos-body-page"><tr><td colspan="7" class="empty-row">— no perp positions —</td></tbody>
          </table>
        </div>
      </div>
    </div><!-- /page-positions -->

    <!-- ═══ TRADES PAGE ═══ -->
    <div class="page-section" id="page-trades">
      <div class="page-header">
        <div><div class="page-title-main">Trade History</div><div class="page-subtitle">Persistent trade records from database</div></div>
        <div style="display:flex;gap:8px;align-items:center">
          <span id="trades-stat" class="page-subtitle" style="margin-right:8px">0 trades</span>
          <button class="topbar-btn" onclick="loadTradesPage()">Refresh</button>
          <button class="topbar-btn" style="color:var(--red);border-color:var(--red)" onclick="confirmClearTrades()">Clear</button>
        </div>
      </div>
      <div class="metrics-row" style="grid-template-columns:repeat(4,1fr);margin-bottom:12px">
        <div class="metric-card"><div class="metric-label">Total Trades</div><div class="metric-value" id="tr-stat-total">—</div></div>
        <div class="metric-card"><div class="metric-label">Win Rate</div><div class="metric-value" id="tr-stat-winrate">—</div></div>
        <div class="metric-card"><div class="metric-label">Total PnL</div><div class="metric-value" id="tr-stat-pnl">—</div></div>
        <div class="metric-card"><div class="metric-label">Avg PnL/Trade</div><div class="metric-value" id="tr-stat-avg">—</div></div>
      </div>
      <div class="panel with-pad" style="padding:0">
        <table class="data-table">
          <thead><tr><th>ID</th><th>Ticker</th><th>Side</th><th>Entry $</th><th>Exit $</th><th>PnL $</th><th>Mult</th><th>Win</th><th>Platform</th><th>Why</th><th>Time</th></tr></thead>
          <tbody id="trades-body"><tr><td colspan="11" class="empty-row">Loading...</td></tr></tbody>
        </table>
        <div id="trades-pager" class="pager-bar"></div>
      </div>
    </div><!-- /page-trades -->

    <!-- ═══ OKX SPOT PAGE ═══ -->
    <div class="page-section" id="page-spot">
      <div class="page-header">
        <div><div class="page-title-main">OKX Spot</div><div class="page-subtitle">Spot trading &amp; balance management</div></div>
      </div>
      <div class="metrics-row" style="grid-template-columns:repeat(3,1fr)">
        <div class="metric-card"><div class="metric-label">Total Equity (USD)</div><div class="metric-value" id="okx-spot-equity">—</div></div>
        <div class="metric-card"><div class="metric-label">Available USDT</div><div class="metric-value" id="okx-spot-avail">—</div></div>
        <div class="metric-card"><div class="metric-label">Holdings Value</div><div class="metric-value" id="okx-spot-holdings">—</div></div>
      </div>
      <div class="panel with-pad">
        <div class="panel-header"><div class="panel-title"><span class="dot blue"></span> Spot Holdings</div></div>
        <table class="data-table">
          <thead><tr><th>Asset</th><th>Total</th><th>Available</th><th>Locked</th><th>USDT Value</th></tr></thead>
          <tbody id="okx-spot-holdings-body"><tr><td colspan="5" class="empty-row">Loading...</td></tbody>
        </table>
      </div>
      <div class="panel with-pad">
        <div class="panel-header"><div class="panel-title"><span class="dot"></span> Recent Spot Trades</div></div>
        <table class="data-table">
          <thead><tr><th>Time</th><th>Pair</th><th>Side</th><th>Exec Price</th><th>Amount</th><th>Filled</th><th>Fee</th></tr></thead>
          <tbody id="okx-spot-trades-body"><tr><td colspan="7" class="empty-row">No recent trades</td></tbody>
        </table>
      </div>
    </div><!-- /page-spot -->

    <!-- ═══ PERP ENGINE PAGE ═══ -->
    <div class="page-section" id="page-perp">
      <div class="page-header">
        <div><div class="page-title-main">Perp Engine</div><div class="page-subtitle">Perpetual futures trading with auto risk controls</div></div>
      </div>
      <div class="metrics-row" style="grid-template-columns:repeat(4,1fr)">
        <div class="metric-card"><div class="metric-label">Perp Equity</div><div class="metric-value" id="okx-perp-equity">—</div></div>
        <div class="metric-card"><div class="metric-label">Open UPL</div><div class="metric-value" id="okx-perp-upl">—</div></div>
        <div class="metric-card"><div class="metric-label">Open Positions</div><div class="metric-value" id="okx-perp-positions-count">—</div></div>
        <div class="metric-card"><div class="metric-label">Auto-Close Rules</div><div class="metric-value" style="font-size:12px">TP 3% / SL 1.5% / 30min</div></div>
      </div>
      <div class="panel with-pad">
        <div class="panel-header"><div class="panel-title"><span class="dot" style="background:var(--purple);box-shadow:0 0 6px var(--purple)"></span> Live Perp Positions</div></div>
        <table class="data-table">
          <thead><tr><th>Ticker</th><th>Side</th><th>Size</th><th>Entry Price</th><th>Mark Price</th><th>Leverage</th><th>UPL $</th><th>UPL %</th><th>Margin</th></tr></thead>
          <tbody id="okx-perp-body"><tr><td colspan="9" class="empty-row">Loading...</td></tbody>
        </table>
      </div>
      <div class="panel with-pad">
        <div class="panel-header"><div class="panel-title"><span class="dot amber"></span> Auto-Close Rules Configuration</div><span class="panel-action" onclick="openPerpSettings()">Configure →</span></div>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;padding:8px 0">
          <div style="background:var(--surface2);padding:12px;border-radius:4px"><div style="font-size:9px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em;margin-bottom:4px">Take Profit</div><div style="font-family:var(--mono);font-size:18px;color:var(--green)" id="perp-rule-tp">3.0%</div><div style="font-size:9px;color:var(--text-dim);margin-top:2px">Auto-close on profit</div></div>
          <div style="background:var(--surface2);padding:12px;border-radius:4px"><div style="font-size:9px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em;margin-bottom:4px">Stop Loss</div><div style="font-family:var(--mono);font-size:18px;color:var(--red)" id="perp-rule-sl">1.5%</div><div style="font-size:9px;color:var(--text-dim);margin-top:2px">Auto-close on loss</div></div>
          <div style="background:var(--surface2);padding:12px;border-radius:4px"><div style="font-size:9px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em;margin-bottom:4px">Max Hold Time</div><div style="font-family:var(--mono);font-size:18px;color:var(--amber)" id="perp-rule-hold">30 min</div><div style="font-size:9px;color:var(--text-dim);margin-top:2px">Force close after</div></div>
          <div style="background:var(--surface2);padding:12px;border-radius:4px"><div style="font-size:9px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em;margin-bottom:4px">Status</div><div style="font-family:var(--mono);font-size:18px;color:var(--green)">ACTIVE</div><div style="font-size:9px;color:var(--text-dim);margin-top:2px">Auto-close enabled</div></div>
        </div>
      </div>
    </div><!-- /page-perp -->

    <!-- ═══ KELLY PAGE ═══ -->
    <div class="page-section" id="page-kelly">
      <div class="page-header">
        <div><div class="page-title-main">Kelly Criterion</div><div class="page-subtitle">Optimal position sizing based on win rate and payout ratio</div></div>
      </div>
      <div class="grid-2">
        <div class="panel with-pad">
          <div class="panel-header"><div class="panel-title"><span class="dot"></span> Kelly Calculator</div></div>
          <div style="display:flex;flex-direction:column;gap:12px;padding:8px 0">
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:11px;color:var(--text-dim)">Win Rate</span><span id="kelly-winrate-display" style="font-family:var(--mono);font-size:16px;font-weight:600">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:11px;color:var(--text-dim)">Avg Win / Avg Loss</span><span id="kelly-payout-display" style="font-family:var(--mono);font-size:16px;font-weight:600">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:11px;color:var(--text-dim)">Full Kelly %</span><span id="kelly-full-display" style="font-family:var(--mono);font-size:16px;font-weight:600;color:var(--cyan)">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:11px;color:var(--text-dim)">Half Kelly (recommended)</span><span id="kelly-half-display" style="font-family:var(--mono);font-size:16px;font-weight:600;color:var(--green)">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:11px;color:var(--text-dim)">Quarter Kelly (conservative)</span><span id="kelly-quarter-display" style="font-family:var(--mono);font-size:16px;font-weight:600;color:var(--amber)">—</span></div>
            <hr style="border:none;border-top:1px solid var(--border);margin:4px 0">
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:11px;color:var(--text-dim)">Current Trade Size</span><span id="kelly-current-display" style="font-family:var(--mono);font-size:16px;font-weight:600">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:11px;color:var(--text-dim)">Kelly Utilization</span><span id="kelly-util-display" style="font-family:var(--mono);font-size:16px;font-weight:600">—</span></div>
          </div>
        </div>
        <div class="panel with-pad">
          <div class="panel-header"><div class="panel-title"><span class="dot amber"></span> Trade Statistics</div></div>
          <div style="display:flex;flex-direction:column;gap:10px;padding:8px 0">
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Total Trades</span><span id="kelly-stat-total" style="font-family:var(--mono);font-size:14px">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Wins</span><span id="kelly-stat-wins" style="font-family:var(--mono);font-size:14px;color:var(--green)">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Losses</span><span id="kelly-stat-losses" style="font-family:var(--mono);font-size:14px;color:var(--red)">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Profit Factor</span><span id="kelly-stat-pf" style="font-family:var(--mono);font-size:14px">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Max Consecutive Wins</span><span id="kelly-stat-maxwin" style="font-family:var(--mono);font-size:14px">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Max Consecutive Losses</span><span id="kelly-stat-maxloss" style="font-family:var(--mono);font-size:14px">—</span></div>
          </div>
        </div>
      </div>
    </div><!-- /page-kelly -->

    <!-- ═══ REPORTS PAGE ═══ -->
    <div class="page-section" id="page-reports">
      <div class="page-header">
        <div><div class="page-title-main">Reports</div><div class="page-subtitle">Performance analytics and trading statistics</div></div>
      </div>
      <div class="grid-main-side">
        <div class="panel with-pad" style="padding:0">
          <div class="panel-header"><div class="panel-title"><span class="dot"></span> Equity Curve</div>
            <div style="display:flex;gap:12px">
              <span class="panel-action" style="color:var(--cyan)">3H</span><span class="panel-action">4H</span>
              <span class="panel-action">1D</span><span class="panel-action">ALL</span>
            </div>
          </div>
          <div class="chart-wrap"><canvas id="equity-report"></canvas></div>
        </div>
        <div class="panel with-pad">
          <div class="panel-header"><div class="panel-title"><span class="dot amber"></span> Performance Summary</div></div>
          <div style="display:flex;flex-direction:column;gap:10px;padding:8px 0">
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Total Trades</span><span id="rpt-stat-total" style="font-family:var(--mono);font-size:14px">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Win Rate</span><span id="rpt-stat-winrate" style="font-family:var(--mono);font-size:14px;color:var(--green)">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Total PnL</span><span id="rpt-stat-pnl" style="font-family:var(--mono);font-size:14px">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Avg Trade PnL</span><span id="rpt-stat-avg" style="font-family:var(--mono);font-size:14px">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Best Trade</span><span id="rpt-stat-best" style="font-family:var(--mono);font-size:14px;color:var(--green)">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Worst Trade</span><span id="rpt-stat-worst" style="font-family:var(--mono);font-size:14px;color:var(--red)">—</span></div>
            <div style="display:flex;justify-content:space-between;align-items:center"><span style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.08em">Profit Factor</span><span id="rpt-stat-pf" style="font-family:var(--mono);font-size:14px">—</span></div>
          </div>
        </div>
      </div>
      <div class="grid-full">
        <div class="panel with-pad" style="padding:0">
          <div class="panel-header"><div class="panel-title"><span class="dot"></span> Trade Log</div>
            <div style="display:flex;gap:8px;align-items:center">
              <select id="rpt-filter-platform" class="filter-search" style="width:80px" onchange="loadReportsPage()">
                <option value="all">All Platforms</option>
                <option value="JUP">Jupiter</option>
                <option value="OKX">OKX</option>
              </select>
              <button class="topbar-btn" onclick="loadReportsPage()">Refresh</button>
            </div>
          </div>
          <table class="data-table">
            <thead><tr><th>ID</th><th>Time</th><th>Ticker</th><th>Side</th><th>Entry $</th><th>Exit $</th><th>PnL $</th><th>Mult</th><th>Platform</th><th>Why</th></tr></thead>
            <tbody id="rpt-trades-body"><tr><td colspan="10" class="empty-row">Loading...</td></tr></tbody>
          </table>
          <div id="rpt-pager" class="pager-bar"></div>
        </div>
      </div>
    </div><!-- /page-reports -->

    <!-- ═══ LEADERBOARD PAGE ═══ -->
    <div class="page-section" id="page-leaderboard">
      <div class="page-header"><div><div class="page-title-main">Profit Leaderboard</div><div class="page-subtitle">Top traders by total PnL</div></div><button class="btn-primary" onclick="syncLeaderboard()" id="sync-lb-btn">Sync My Stats</button></div>
      <div id="leaderboard-login-msg" class="panel" style="text-align:center;padding:40px;display:none"><div style="font-size:13px;color:var(--text-dim);margin-bottom:12px">Please log in to sync your stats and appear on the leaderboard</div><button class="btn-primary" onclick="showAuthModal()">Log In / Register</button></div>
      <div id="leaderboard-content" style="display:none">
        <div class="grid-3" id="lb-stats-cards"></div>
        <div class="panel" style="margin-top:16px"><div class="panel-header"><div class="panel-title"><span class="dot green"></span> Top Traders</div></div>
          <div class="table-wrap"><table class="data-table"><thead><tr><th>#</th><th>Trader</th><th>Tier</th><th>Total PnL</th><th>Trades</th><th>Win Rate</th></tr></thead><tbody id="lb-tbody"></tbody></table></div>
        </div>
      </div>
      <div id="leaderboard-loading" class="panel" style="text-align:center;padding:40px"><div class="spinner"></div><div style="margin-top:8px;font-size:11px;color:var(--text-dim)">Loading leaderboard...</div></div>
    </div><!-- /page-leaderboard -->

    <!-- ═══ MEMBERS / USER CENTER PAGE ═══ -->
    <div class="page-section" id="page-members">
      <div class="page-header"><div><div class="page-title-main" data-i18n="nav_members">Members</div><div class="page-subtitle" id="members-subtitle">Log in to continue</div></div></div>

      <!-- Login prompt -->
      <div id="members-login-msg" class="panel" style="text-align:center;padding:40px"><div style="font-size:13px;color:var(--text-dim);margin-bottom:12px" data-i18n="login_required">Please log in to access your member center</div><button class="btn-primary" onclick="showAuthModal()">Log In</button></div>

      <!-- Admin panel (admin users only) -->
      <div id="members-admin-panel" style="display:none">
        <div class="panel"><div class="panel-header"><div class="panel-title"><span class="dot blue"></span> All Members</div><button class="btn-secondary" onclick="loadMembersPage()">Refresh</button></div>
          <div class="table-wrap"><table class="data-table"><thead><tr><th>ID</th><th>Username</th><th>Email</th><th>Role</th><th>Tier</th><th>Registered</th><th>Last Login</th><th>Actions</th></tr></thead><tbody id="members-tbody"></tbody></table></div>
        </div>
        <div class="panel" style="margin-top:16px"><div class="panel-header"><div class="panel-title"><span class="dot amber"></span> Quick Actions</div></div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <button class="btn-secondary" onclick="resetAllLeaderboard()">Reset Leaderboard</button>
            <button class="btn-secondary" onclick="grantAllFreeToPremium()">Bulk Upgrade to Premium</button>
          </div>
        </div>
      </div>

      <!-- User center (all logged-in users) -->
      <div id="members-user-panel" style="display:none">
        <!-- Tabs -->
        <div style="display:flex;gap:0;margin-bottom:16px;border-bottom:1px solid var(--border)">
          <div id="tab-trade-settings" style="flex:1;text-align:center;padding:10px;cursor:pointer;color:var(--green);border-bottom:2px solid var(--green);font-family:var(--mono);font-size:11px;letter-spacing:.05em;text-transform:uppercase" onclick="switchMemberTab('trade-settings')"><span data-i18n="trade_settings">Trade Settings</span></div>
          <div id="tab-api-keys" style="flex:1;text-align:center;padding:10px;cursor:pointer;color:var(--text-dim);font-family:var(--mono);font-size:11px;letter-spacing:.05em;text-transform:uppercase" onclick="switchMemberTab('api-keys')"><span data-i18n="api_key_management">API Key Management</span></div>
        </div>

        <!-- Trade Settings Tab -->
        <div id="panel-trade-settings" style="display:block">
          <div class="panel">
            <div class="panel-header"><div class="panel-title"><span class="dot green"></span> <span data-i18n="trade_settings">Trade Settings</span></div><button class="btn-save" onclick="saveTradeSettings()"><span data-i18n="save">Save</span></button></div>
            <div style="padding:16px;display:flex;flex-direction:column;gap:20px">
              <!-- Risk Preference -->
              <div>
                <label style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono);display:block;margin-bottom:10px"><span data-i18n="risk_preference">Risk Preference</span></label>
                <div class="risk-options-member" style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px">
                  <div class="risk-option-member" data-value="conservative" onclick="setRiskFromCenter('conservative')">
                    <div class="risk-member-icon">🛡</div>
                    <div class="risk-member-name"><span data-i18n="conservative">Conservative</span></div>
                    <div class="risk-member-desc"><span data-i18n="conservativeDesc">Max 1-2% per trade, total risk ≤5%</span></div>
                  </div>
                  <div class="risk-option-member" data-value="balanced" onclick="setRiskFromCenter('balanced')">
                    <div class="risk-member-icon">⚖</div>
                    <div class="risk-member-name"><span data-i18n="balanced">Balanced</span></div>
                    <div class="risk-member-desc"><span data-i18n="balancedDesc">Max 2-4% per trade, total risk ≤8%</span></div>
                  </div>
                  <div class="risk-option-member" data-value="aggressive" onclick="setRiskFromCenter('aggressive')">
                    <div class="risk-member-icon">🔥</div>
                    <div class="risk-member-name"><span data-i18n="aggressive">Aggressive</span></div>
                    <div class="risk-member-desc"><span data-i18n="aggressiveDesc">Max 4-6% per trade, total risk ≤12%</span></div>
                  </div>
                </div>
              </div>
              <!-- Trade Mode -->
              <div>
                <label style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono);display:block;margin-bottom:10px"><span data-i18n="trade_mode">Trade Mode</span></label>
                <div class="mode-options-member" style="display:grid;grid-template-columns:repeat(2,1fr);gap:10px">
                  <div class="mode-option-member" data-value="signal_only" onclick="setModeFromCenter('signal_only')">
                    <div class="mode-member-icon">👁</div>
                    <div class="mode-member-name"><span data-i18n="signal_only">Signal Only</span></div>
                    <div class="mode-member-desc"><span data-i18n="signal_only_desc">AI gives suggestions, you confirm manually</span></div>
                  </div>
                  <div class="mode-option-member" data-value="auto" onclick="setModeFromCenter('auto')">
                    <div class="mode-member-icon">⚡</div>
                    <div class="mode-member-name"><span data-i18n="auto_exec">Auto Execute</span></div>
                    <div class="mode-member-desc"><span data-i18n="auto_exec_desc">AI places orders directly</span></div>
                  </div>
                </div>
              </div>
              <!-- Running Time -->
              <div>
                <label style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono);display:block;margin-bottom:10px"><span data-i18n="runtime">Running Time</span></label>
                <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
                  <div style="display:flex;align-items:center;gap:8px">
                    <span style="font-size:10px;color:var(--text-dim);font-family:var(--mono)">START</span>
                    <input id="us-start-time" type="time" value="00:00" style="background:var(--surface);border:1px solid var(--border);color:var(--text);padding:6px 8px;border-radius:3px;font-size:12px;font-family:var(--mono);width:100px">
                  </div>
                  <span style="color:var(--text-dim);font-size:12px">—</span>
                  <div style="display:flex;align-items:center;gap:8px">
                    <span style="font-size:10px;color:var(--text-dim);font-family:var(--mono)">END</span>
                    <input id="us-end-time" type="time" value="23:59" style="background:var(--surface);border:1px solid var(--border);color:var(--text);padding:6px 8px;border-radius:3px;font-size:12px;font-family:var(--mono);width:100px">
                  </div>
                  <div style="display:flex;align-items:center;gap:6px;margin-left:8px">
                    <input type="checkbox" id="us-time-unlimited" onchange="toggleTimeUnlimited()" style="accent-color:var(--green)">
                    <label for="us-time-unlimited" style="font-size:10px;color:var(--text-dim)"><span data-i18n="unlimited">Unlimited (24h)</span></label>
                  </div>
                </div>
              </div>
              <!-- Max Position -->
              <div style="display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap">
                <div>
                  <label style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono);display:block;margin-bottom:6px"><span data-i18n="max_position_usd">Max Position per Trade (USD)</span></label>
                  <input id="us-max-position" type="number" value="100" min="10" max="10000" step="10" style="width:120px;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:6px 8px;border-radius:3px;font-size:12px;font-family:var(--mono)">
                </div>
                <div>
                  <label style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono);display:block;margin-bottom:6px"><span data-i18n="take_profit">Take Profit (%)</span></label>
                  <input id="us-take-profit" type="number" value="3.0" min="0.1" max="50" step="0.1" style="width:100px;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:6px 8px;border-radius:3px;font-size:12px;font-family:var(--mono)">
                </div>
                <div>
                  <label style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono);display:block;margin-bottom:6px"><span data-i18n="stop_loss">Stop Loss (%)</span></label>
                  <input id="us-stop-loss" type="number" value="1.5" min="0.1" max="50" step="0.1" style="width:100px;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:6px 8px;border-radius:3px;font-size:12px;font-family:var(--mono)">
                </div>
              </div>
              <!-- Allowed Tickers Multi-Select -->
              <div>
                <label style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono);display:block;margin-bottom:6px"><span data-i18n="allowed_tickers">Filter Tickers</span> <span style="color:var(--text-dim);opacity:.6;text-transform:none;letter-spacing:0;font-size:9px">(leave empty = no filter)</span></label>
                <div id="ticker-picker-container" style="position:relative">
                  <div id="us-ticker-input" class="ticker-picker-trigger" onclick="toggleTickerPicker()" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:7px 10px;border-radius:3px;margin-top:4px;font-size:12px;cursor:text;min-height:32px;display:flex;align-items:center;flex-wrap:wrap;gap:4px">
                    <span style="color:var(--text-dim);font-size:11px" id="ticker-placeholder">Click to select coins...</span>
                  </div>
                  <div id="us-ticker-dropdown" style="display:none;position:absolute;top:100%;left:0;right:0;background:var(--surface);border:1px solid var(--border);border-radius:3px;margin-top:2px;z-index:100;max-height:240px;overflow-y:auto">
                    <div style="padding:8px;border-bottom:1px solid var(--border);display:flex;gap:6px;align-items:center">
                      <button onclick="selectAllTickers()" style="font-size:9px;padding:2px 8px;background:var(--cyan);color:var(--bg);border:none;border-radius:2px;cursor:pointer;font-family:var(--mono);text-transform:uppercase">All</button>
                      <button onclick="clearAllTickers()" style="font-size:9px;padding:2px 8px;background:transparent;color:var(--text-dim);border:1px solid var(--border);border-radius:2px;cursor:pointer;font-family:var(--mono);text-transform:uppercase">Clear</button>
                      <input id="ticker-search" type="text" placeholder="Search..." oninput="filterTickers(this.value)" style="margin-left:auto;background:transparent;border:1px solid var(--border);color:var(--text);padding:3px 8px;border-radius:2px;font-size:11px;width:120px">
                    </div>
                    <div id="ticker-list" style="padding:6px;display:grid;grid-template-columns:repeat(4,1fr);gap:4px"></div>
                  </div>
                </div>
                <input type="hidden" id="us-allowed-tickers">
              </div>
            </div>
          </div>
        </div>

        <!-- API Keys Tab -->
        <div id="panel-api-keys" style="display:none">
          <div class="panel">
            <div class="panel-header"><div class="panel-title"><span class="dot amber"></span> <span data-i18n="api_key_management">API Key Management</span></div><button class="btn-save" onclick="saveApiKeys()"><span data-i18n="save">Save</span></button></div>
            <div style="padding:16px;display:flex;flex-direction:column;gap:12px">
              <div>
                <label style="font-size:10px;color:var(--text-dim)"><span data-i18n="api_key">API Key</span></label>
                <input id="us-api-key" type="text" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:6px;border-radius:2px;margin-top:4px;font-size:11px;font-family:var(--mono)">
              </div>
              <div>
                <label style="font-size:10px;color:var(--text-dim)"><span data-i18n="api_secret">API Secret</span></label>
                <input id="us-api-secret" type="password" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:6px;border-radius:2px;margin-top:4px;font-size:11px;font-family:var(--mono)">
              </div>
              <div>
                <label style="font-size:10px;color:var(--text-dim)"><span data-i18n="api_passphrase">API Passphrase</span></label>
                <input id="us-api-passphrase" type="password" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:6px;border-radius:2px;margin-top:4px;font-size:11px;font-family:var(--mono)">
              </div>
              <div id="us-api-status" style="font-size:10px;color:var(--text-dim)"></div>
            </div>
          </div>
        </div>
      </div>
    </div><!-- /page-members -->

    <!-- ═══ AUTH MODAL ═══ -->
    <div id="auth-modal" class="modal-overlay" style="display:none;position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,.7);align-items:center;justify-content:center">
      <div class="modal-box" style="max-width:400px">
        <button class="close-btn" onclick="closeAuthModal()">✕</button>
        <h2 id="auth-modal-title">Login</h2>
        <div id="auth-tabs" style="display:flex;gap:0;margin-bottom:16px;border-bottom:1px solid var(--border)">
          <div id="tab-login" style="flex:1;text-align:center;padding:8px;cursor:pointer;color:var(--green);border-bottom:2px solid var(--green)" onclick="switchAuthTab('login')">Login</div>
          <div id="tab-register" style="flex:1;text-align:center;padding:8px;cursor:pointer;color:var(--text-dim)" onclick="switchAuthTab('register')">Register</div>
        </div>
        <div id="auth-login-form">
          <div style="margin-bottom:12px"><label style="font-size:10px;color:var(--text-dim)">Username</label><input id="auth-login-user" type="text" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:2px;margin-top:4px"></div>
          <div style="margin-bottom:12px"><label style="font-size:10px;color:var(--text-dim)">Password</label><input id="auth-login-pass" type="password" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:2px;margin-top:4px"></div>
          <button class="btn-primary" onclick="doLogin()" style="width:100%">Login</button>
        </div>
        <div id="auth-register-form" style="display:none">
          <div style="margin-bottom:12px"><label style="font-size:10px;color:var(--text-dim)">Username</label><input id="auth-reg-user" type="text" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:2px;margin-top:4px"></div>
          <div style="margin-bottom:12px"><label style="font-size:10px;color:var(--text-dim)">Password</label><input id="auth-reg-pass" type="password" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:2px;margin-top:4px"></div>
          <div style="margin-bottom:12px"><label style="font-size:10px;color:var(--text-dim)">Email (optional)</label><input id="auth-reg-email" type="email" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:2px;margin-top:4px"></div>
          <button class="btn-primary" onclick="doRegister()" style="width:100%">Register</button>
        </div>
        <div id="auth-msg" style="margin-top:12px;font-size:11px;text-align:center"></div>
      </div>
    </div>

    <!-- ═══ MEMBER EDIT MODAL ═══ -->
    <div id="member-edit-modal" class="modal-overlay" style="display:none;position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,.7);align-items:center;justify-content:center">
      <div class="modal-box" style="max-width:360px">
        <button class="close-btn" onclick="closeMemberEdit()">✕</button>
        <h2>Edit Member</h2>
        <input type="hidden" id="edit-member-id">
        <div style="margin-bottom:12px"><label style="font-size:10px;color:var(--text-dim)">Role</label>
          <select id="edit-member-role" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:2px;margin-top:4px">
            <option value="user">User</option><option value="admin">Admin</option>
          </select>
        </div>
        <div style="margin-bottom:12px"><label style="font-size:10px;color:var(--text-dim)">Tier</label>
          <select id="edit-member-tier" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:2px;margin-top:4px">
            <option value="free">Free</option><option value="premium">Premium</option>
          </select>
        </div>
        <button class="btn-primary" onclick="saveMemberEdit()" style="width:100%">Save</button>
        <button class="btn-secondary" onclick="deleteMember()" style="width:100%;margin-top:8px;color:var(--red);border-color:var(--red)">Delete Member</button>
      </div>
    </div>

    <!-- ═══ PERP AUTO-CLOSE SETTINGS MODAL ═══ -->
    <div id="perp-settings-modal" class="modal-overlay" style="display:none;position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,.7);align-items:center;justify-content:center">
      <div class="modal-box" style="max-width:480px">
        <button class="close-btn" onclick="closePerpSettings()">✕</button>
        <h2>Perp Auto-Close Settings</h2>
        <div style="font-size:11px;color:var(--text-dim);margin-bottom:16px">Configure automatic position closing rules for perpetual futures</div>
        <div style="display:flex;flex-direction:column;gap:12px">
          <div>
            <label style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em">Take Profit Threshold (%)</label>
            <input id="perp-tp" type="number" step="0.1" value="3" data-default="3" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px 10px;font-family:var(--mono);border-radius:2px;margin-top:4px;font-size:13px">
          </div>
          <div>
            <label style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em">Stop Loss Threshold (%)</label>
            <input id="perp-sl" type="number" step="0.1" value="1.5" data-default="1.5" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px 10px;font-family:var(--mono);border-radius:2px;margin-top:4px;font-size:13px">
          </div>
          <div>
            <label style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em">Max Hold Time (seconds)</label>
            <input id="perp-max-hold" type="number" value="1800" data-default="1800" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px 10px;font-family:var(--mono);border-radius:2px;margin-top:4px;font-size:13px">
          </div>
          <div>
            <label style="font-size:10px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em">Max Position Size (USD)</label>
            <input id="perp-max-size" type="number" value="1000" data-default="1000" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px 10px;font-family:var(--mono);border-radius:2px;margin-top:4px;font-size:13px">
          </div>
          <div style="display:flex;align-items:center;gap:10px;padding:8px 0">
            <input id="perp-reopen" type="checkbox" checked data-default="true" style="width:16px;height:16px;cursor:pointer">
            <label style="font-size:12px;color:var(--text)">Allow same-coin re-entry after close</label>
          </div>
        </div>
        <div class="modal-footer" style="margin-top:16px">
          <button class="btn-secondary" onclick="closePerpSettings()">Cancel</button>
          <button class="btn-primary" onclick="savePerpSettings()">Save Settings</button>
        </div>
      </div>
    </div>

  </div><!-- /main -->

<script>

// === i18n ===
const I18N = {
  en: {
    hint:"OKX Spot & Futures · AI Auto Trading",
    liveMonitor:"📡 Live Monitor", multiple:"Multiple", bankroll:"Bankroll",
    entries:"Entries", winsLosses:"Wins / Losses", pendingSwaps:"Pending Swaps",
    submittedSwaps:"Submitted", solPrice:"SOL Price", deskState:"Desk State",
    todayPnl:"Today P&L", aiMode:"AI Mode",
    equityTitle:"Equity (USD) · log scale · scroll window",
    swapsTitle:"🔗 Swap Bundles", side:"Side", ticker:"Ticker",
    amountUsd:"Amount $", feeUsd:"Fee $", impact:"Impact", output:"Out (est)", action:"Action",
    positionsTitle:"Open Positions", entryTime:"Entry Time", entryPrice:"Buy Price", entryUsd:"Entry $",
    peakMult:"Peak", currentMult:"Current", sizeFrac:"Size%",
    historyTitle:"History Trades", exitTime:"Exit Time", exitUsd:"Sell $",
    buyPrice:"Buy Price", sellPrice:"Sell Price",
    pnlMult:"P/L", pnlUsd:"Realized", why:"Why",
    deskStatus:"Desk status", state:"state", theme:"theme",
    enteredRejected:"entered / rejected", expectancy:"expectancy",
    kelly:"full / used Kelly", currentOpen:"current open",
    feed:"Signal Feed", idle:"idle", running:"running", done:"done",
    noData:"No data", noOpen:"— no open positions —", noHistory:"— no closed trades yet —",
    submit:"Submit", submitted:"Submitted", failed:"Failed", pending:"Pending",
    connectWallet:"Connect Wallet", disconnect:"Disconnect", connected:"Connected",
    manualSignal:"Manual Signal Test",
    welcomeTitle:"Welcome to AI Trade Desk",
    welcomeDesc:"An AI trading assistant designed for OKX Spot &amp; Futures.<br>AI scans potential pairs → decides timing &amp; position size → you confirm or fully automate.",
    step1:"Connect your OKX API (trading permissions only, never withdrawal)",
    step2:"Choose risk preference (Conservative / Balanced / Aggressive)",
    step3:"Start AI scanning — view decisions and positions in real time",
    connectOkxBtn:"Connect OKX API",
    simulationBtn:"View Simulation",
    riskDisclaimer:"Trading involves risk. Past performance is not indicative of future results. Funds remain in your own OKX account.",
    decisionLog:"🧠 AI Decision Log",
    decisionPct:"Account <span id='dec-pct-val'>%s</span>",
    confirmBtn:"Confirm", ignoreBtn:"Ignore", detailBtn:"Details",
    autoExecuted:"Auto-executed", executing:"Executing...",
    riskExposure:"Risk Exposure",
    settings:"Settings",
    riskPref:"Risk Preference", conservative:"Conservative", balanced:"Balanced", aggressive:"Aggressive",
    conservativeDesc:"Max 1-2% per trade, total ≤5%", balancedDesc:"Max 2-4% per trade, total ≤8%", aggressiveDesc:"Max 4-6% per trade, total ≤12%",
    tradeMode:"Trade Mode", signalOnly:"Signal Only", autoExec:"Auto Execute",
    signalOnlyDesc:"AI suggests, you confirm", autoExecDesc:"AI places orders automatically",
    strategyGuide:"Strategy Guide",
    strat_h1:"How Our AI Works",
    strat_s1:"Scan the Market", strat_s1d:"Monitor OKX spot & futures popular pairs for price, volume, funding rate, volatility in real time.",
    strat_s2:"Identify Opportunities", strat_s2d:"AI combines multi-dimensional signals to find pairs with high-probability directional opportunities (not long-term predictions).",
    strat_s3:"Decide Timing & Size", strat_s3d:"Timing: trend, volatility, funding analysis. Size: auto-calculated from your risk preference and account size.",
    strat_s4:"Built-in Risk Control", strat_s4d:"Every suggestion includes stop-loss reference, max position limits, and total account risk circuit breaker.",
    strat_s5:"You Always Have Final Control", strat_s5d:"Switch between Signal Only (AI suggests, you confirm) and Auto Execute modes. Every decision leaves a reviewable log.",
    strat_trust:"Trust Statement",
    strat_t1:"All trades execute via your own OKX API — funds stay in your account.",
    strat_t2:"Historical trades and equity curve are publicly visible in real time.",
    strat_t3:"No profit promises. Past performance does not indicate future results. Trading involves risk.",
    // Navigation
    nav_overview:"Overview", nav_dashboard:"Dashboard", nav_positions:"Positions", nav_trades:"Trades",
    nav_engine:"Engine", nav_spot:"OKX Spot", nav_perp:"Perp Engine",
    nav_analytics:"Analytics", nav_kelly:"Kelly Criterion", nav_reports:"Reports",
    nav_community:"Community", nav_leaderboard:"Leaderboard", nav_members:"Members",
  },
  zh: {
    hint:"OKX 现货/合约 · AI 自动交易",
    liveMonitor:"📡 实时监视器", multiple:"倍数", bankroll:"可用资金",
    entries:"入场次数", winsLosses:"胜 / 负", pendingSwaps:"待签名",
    submittedSwaps:"已提交", solPrice:"SOL 价格", deskState:"运行状态",
    todayPnl:"今日盈亏", aiMode:"AI 模式",
    equityTitle:"净值曲线 (USD) · 对数刻度 · 滚动窗口",
    swapsTitle:"🔗 Swap 交易包", side:"方向", ticker:"币种",
    amountUsd:"金额 $", feeUsd:"手续费 $", impact:"滑点影响", output:"预期输出", action:"操作",
    positionsTitle:"当前持仓", entryTime:"入场时间", entryPrice:"买入价格", entryUsd:"入场 $",
    peakMult:"峰值", currentMult:"当前", sizeFrac:"仓位%",
    historyTitle:"历史交易", exitTime:"出场时间", exitUsd:"卖出 $",
    buyPrice:"买入价格", sellPrice:"卖出价格",
    pnlMult:"盈亏倍率", pnlUsd:"已实现", why:"原因",
    deskStatus:"交易台状态", state:"状态", theme:"主题",
    enteredRejected:"入场 / 拒绝", expectancy:"期望值",
    kelly:"Kelly 满 / 实", currentOpen:"当前持仓详情",
    feed:"信号流", idle:"待机", running:"运行中", done:"完成",
    noData:"无数据", noOpen:"— 暂无持仓 —", noHistory:"— 暂无历史交易 —",
    submit:"提交", submitted:"已提交", failed:"失败", pending:"待签名",
    connectWallet:"连接钱包", disconnect:"断开", connected:"已连接",
    manualSignal:"手动信号测试",
    welcomeTitle:"欢迎使用 AI Trade Desk",
    welcomeDesc:"专为 OKX 现货与合约设计的 AI 交易助手。<br>AI 自动扫描潜力品种 → 判断买卖时机与建议手数 → 你确认或全自动执行。",
    step1:"连接你的 OKX API（只开交易权限，不开提币）",
    step2:"选择风险偏好（保守 / 平衡 / 激进）",
    step3:"开启 AI 扫描，实时查看决策与持仓",
    connectOkxBtn:"立即连接 OKX API",
    simulationBtn:"先看模拟表现",
    riskDisclaimer:"交易有风险，过往表现不代表未来结果。资金始终在你自己的 OKX 账户中。",
    decisionLog:"🧠 AI 决策日志",
    decisionPct:'账户 <span id="dec-pct-val">%s</span>',
    confirmBtn:"确认执行", ignoreBtn:"忽略", detailBtn:"查看详情",
    autoExecuted:"已自动执行", executing:"执行中...",
    riskExposure:"风险敞口",
    settings:"设置",
    riskPref:"风险偏好", conservative:"保守", balanced:"平衡", aggressive:"激进",
    conservativeDesc:"单笔最大 1-2%，总风险 ≤5%", balancedDesc:"单笔最大 2-4%，总风险 ≤8%",
    aggressiveDesc:"单笔最大 4-6%，总风险 ≤12%",
    tradeMode:"交易模式", signalOnly:"仅信号", autoExec:"全自动",
    signalOnlyDesc:"AI 只给建议，需手动确认", autoExecDesc:"AI 直接下单执行",
    strategyGuide:"策略说明",
    strat_h1:"我们的 AI 如何工作？",
    strat_s1:"扫描市场", strat_s1d:"实时监控 OKX 现货与合约热门品种的价格、成交量、资金费率、波动率等数据。",
    strat_s2:"识别潜力机会", strat_s2d:"AI 综合多维度信号，找出短期有较高概率出现方向性机会的品种(不做长期预测)。",
    strat_s3:"判断买卖时机与手数", strat_s3d:"时机：结合趋势、波动、资金面给出入场建议。手数：根据你设定的风险偏好和账户规模，自动计算建议仓位(默认保守)。",
    strat_s4:"风险控制内置", strat_s4d:"每笔建议都带止损参考、单笔最大仓位限制、账户总风险熔断。你可随时调整或关闭自动执行。",
    strat_s5:"你始终拥有最终控制权", strat_s5d:"可选择「仅信号」模式(AI 只给建议，你手动确认)或「全自动」模式。所有决策都会留下可查看的理由日志。",
    strat_trust:"信任声明",
    strat_t1:"所有交易通过你自己的 OKX API 执行，资金始终在你账户。",
    strat_t2:"历史交易与 Equity 曲线实时公开可查。",
    strat_t3:"不承诺收益，过往表现不代表未来结果。交易有风险。",
    // Navigation
    nav_overview:"概览", nav_dashboard:"仪表盘", nav_positions:"持仓", nav_trades:"交易",
    nav_engine:"引擎", nav_spot:"OKX 现货", nav_perp:"永续引擎",
    nav_analytics:"分析", nav_kelly:"Kelly 准则", nav_reports:"报告",
    nav_community:"社区", nav_leaderboard:"排行榜", nav_members:"会员",
  }
};
let lang = "en";
function setLang(l){lang=l;applyLang();}
function applyLang(){
  document.querySelectorAll("[data-i18n]").forEach(el=>{
    const k=el.getAttribute("data-i18n");
    if(!I18N[lang][k])return;
    const childEls=el.children.length;
    if(childEls===0){el.textContent=I18N[lang][k];}
    else{const tn=Array.from(el.childNodes).find(n=>n.nodeType===Node.TEXT_NODE&&n.textContent.trim());if(tn)tn.textContent=I18N[lang][k]+" ";}
  });
  document.getElementById("btnLangEN").classList.toggle("active",lang==="en");
  document.getElementById("btnLangZH").classList.toggle("active",lang==="zh");
}

const $ = id => document.getElementById(id);
const eqCanvas=$("equity"), eqCtx=eqCanvas.getContext("2d");
let eqW=[], eqV=[];
// OKX real-balance accumulator: builds equity curve from live balance snapshots
let _eqOkxAccum=[]; // [{t, v}] sorted by t
let _eqOkxLastSec=0;
let animFrame=null;
let lastSSE=null;
let _sseThrottle=100;
let _lastSSETime=0;

// === Equity draw ===
let eqAutoScroll=true;
let _pulsePhase=0;
function drawEquity(){
  const dpr=window.devicePixelRatio||1;
  const W=eqCanvas.width/dpr, H=eqCanvas.height/dpr;
  eqCtx.save();
  eqCtx.setTransform(1,0,0,1,0,0);
  eqCtx.fillStyle="#0a0b0d"; eqCtx.fillRect(0,0,eqCanvas.width,eqCanvas.height);
  eqCtx.scale(dpr,dpr);
  if(eqW.length<2){eqCtx.restore();return;}

  // Apply time-window filter
  const windowMin=window.__equity_window||Infinity;
  const nowSec=Math.floor(Date.now()/1000);
  let visT=eqW, visV=eqV;
  if(windowMin!==Infinity){
    const cutoff=nowSec-windowMin*60;
    const idx=eqW.findIndex(t=>t>=cutoff);
    if(idx>=0){visT=eqW.slice(idx);visV=eqV.slice(idx);}
  }
  const maxVisible=400;
  if(visT.length>maxVisible){
    visT=visT.slice(visT.length-maxVisible);
    visV=visV.slice(visV.length-maxVisible);
  }
  if(visT.length<2){eqCtx.restore();return;}

  const vMin=Math.min(...visV), vMax=Math.max(...visV);
  const vRange=vMax-vMin;
  const vMid=(vMax+vMin)/2;

  let vmin, vmax, useLog=true;
  if(vRange < vMid*0.01){
    useLog=false;
    const pad=vRange*0.5||vMid*0.02;
    vmin=Math.max(0, vMin-pad);
    vmax=vMax+pad;
  }else{
    const start=vMid;
    vmin=Math.max(10,Math.min(start*0.5,vMin*0.8));
    vmax=Math.max(start*2,Math.max(...visV)*1.2);
  }

  const padL=60,padR=24,padT=14,padB=30;
  const pw=W-padL-padR, ph=H-padT-padB;
  const logVmin=Math.log10(Math.max(0.01,vmin)), logVmax=Math.log10(vmax);

  const tMin=visT[0], tMax=visT[visT.length-1];
  const sx=t=>padL+pw*((t-tMin)/(tMax-tMin||1));
  const sy=v=>padT+ph*(1-(useLog?(Math.log10(Math.max(vmin,v))-logVmin)/(logVmax-logVmin):((v-vmin)/(vmax-vmin))));

  eqCtx.strokeStyle="#232830"; eqCtx.lineWidth=0.5;
  eqCtx.font="10px JetBrains Mono,monospace"; eqCtx.fillStyle="#6b7280";
  const yStep=useLog?Math.pow(10,Math.ceil(logVmin)):(vmax-vMin)/5;
  for(let v=useLog?Math.pow(10,Math.ceil(logVmin)):vmin;v<=vmax;v+=yStep){
    const y=sy(v);
    if(y>=padT && y<=padT+ph){
      eqCtx.beginPath();
      eqCtx.moveTo(padL,y);
      eqCtx.lineTo(W-padR,y);
      eqCtx.stroke();
      eqCtx.fillText("$"+v.toLocaleString(undefined,{maximumFractionDigits:0}),6,y+3);
    }
  }

  const lastVis=visT.length-1;

  // Gradient fill under the line
  const lastX=sx(visT[lastVis]), firstX=sx(visT[0]);
  const grad=eqCtx.createLinearGradient(0,padT,0,padT+ph);
  grad.addColorStop(0,"rgba(34,197,94,0.18)");
  grad.addColorStop(0.5,"rgba(34,197,94,0.06)");
  grad.addColorStop(1,"rgba(34,197,94,0.0)");
  eqCtx.beginPath();
  eqCtx.moveTo(sx(visT[0]),sy(visV[0]));
  for(let i=1;i<visT.length;i++){
    eqCtx.lineTo(sx(visT[i]),sy(visV[i]));
  }
  eqCtx.lineTo(lastX,padT+ph);
  eqCtx.lineTo(firstX,padT+ph);
  eqCtx.closePath();
  eqCtx.fillStyle=grad;
  eqCtx.fill();

  // Glow layer
  eqCtx.save();
  eqCtx.shadowColor="rgba(34,197,94,0.5)";
  eqCtx.shadowBlur=8;
  eqCtx.strokeStyle="#22c55e"; eqCtx.lineWidth=2.5;
  eqCtx.beginPath();
  for(let i=0;i<visT.length;i++){
    const x=sx(visT[i]), y=sy(visV[i]);
    i===0?eqCtx.moveTo(x,y):eqCtx.lineTo(x,y);
  }
  eqCtx.stroke();
  eqCtx.restore();

  eqCtx.restore();

}

// === Monitor ===
function updateMonitor(m, okxBalance){
  if(!m)return;
  $("mon-mult").textContent=(m.multiple||1).toFixed(2)+"x";
  // OKX mode: use real balance instead of simulator bankroll
  if(okxBalance){
    const mb=$("mon-bank");
    if(mb) mb.textContent="$"+okxBalance.total_eq_usd.toFixed(2);
  } else {
    const mb=$("mon-bank");
    if(mb) mb.textContent="$"+(m.bankroll||0).toFixed(0);
  }
  if((m.multiple||1)>=1){$("mon-bank").classList.add('up');$("mon-bank").classList.remove('down');}
  else{$("mon-bank").classList.add('down');$("mon-bank").classList.remove('up');}
  $("mon-entry").textContent=m.entries||0;
  $("mon-wl").textContent=(m.wins||0)+" / "+(m.losses||0);
  $("mon-pending").textContent=m.pending_swaps||0;
  $("mon-submitted").textContent=m.submitted_swaps||0;
  $("mon-state").textContent=m.state||"—";
  document.getElementById("pill").className="badge-dot live";
}
function fmt(v,d){if(v>=1e9)return(v/1e9).toFixed(d)+"B";if(v>=1e6)return(v/1e6).toFixed(d)+"M";if(v>=1e3)return(v/1e3).toFixed(d)+"K";return v.toFixed(d);}

// === Feed ===
function renderFeed(feed){
  const lines=(feed||[]).map(f=>{
    const cls=({ENTRY:"entry",EXIT:"entry",STOP:"stop",HALT:"halt",LEARN:"learn",REJECT:"reject",NOT_BUY:"notbuy"})[f.side]||"";
    return `<span class="${cls}">[${String(f.t).padStart(3,"0")}] ${f.side.padEnd(7)} ${String(f.ticker).padEnd(10)} ${String(f.mult).padStart(6)}x  ${f.note||""}</span>`;
  });
  $("feed-log").innerHTML=lines.join("\n")||(lang==="zh"?"— 等待信号 —":"— waiting for signals —");
}

// === Positions ===
function renderPositions(positions){
  const tbody=$("pos-body");
  if(!positions||!positions.length){
    tbody.innerHTML=`<tr><td colspan="8" class="empty-row">— ${lang==="zh"?"暂无持仓":"no open positions"} —</td></tr>`;return;
  }
  tbody.innerHTML=positions.map(p=>{
    const nowColor=(p.current_mult||1)>=1?'var(--green)':'var(--red)';
    const entryDate=p.entry_ts?new Date(p.entry_ts*1000).toLocaleString("en-CA",{year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",hour12:false}).replace("T"," "):`${p.entry_min}m`;
    const sideCls=(p.side==="BUY"||p.side==="LONG")?'side-buy':'side-sell';
    const sideLabel=p.side==="LONG"?"LONG":(p.side==="SHORT"?"SHORT":p.side||"BUY");
    return `<tr><td style="font-weight:600">${p.ticker}</td><td class="${sideCls}">${sideLabel}</td><td style="color:var(--text-dim)">${entryDate}</td><td>$${(p.entry_price||0).toFixed(6)}</td><td>$${p.entry_usd.toFixed(2)}</td>
      <td>${(p.peak_mult||0).toFixed(2)}x</td><td style="color:${nowColor};font-weight:600">${(p.current_mult||1).toFixed(2)}x</td><td>${p.size_frac}%</td></tr>`;
  }).join("");
}

// === Perpetual Positions ===
let _perpPage=0;
const PERP_PAGE_SIZE=10;
function renderPerpPositions(perpPositions, perpStats, perpConfig){
  const card=document.getElementById("perp-positions-card");
  const tbody=document.getElementById("perp-pos-body");
  const badge=document.getElementById("perp-pos-badge");
  const pager=document.getElementById("perp-pager");
  if(!card||!tbody)return;
  if(!perpPositions||!perpPositions.length){
    card.style.display="none";return;
  }
  card.style.display="";
  if(badge)badge.textContent=`(${perpPositions.length})`;
  const total=perpPositions.length;
  const totalPages=Math.max(1,Math.ceil(total/PERP_PAGE_SIZE));
  if(_perpPage>=totalPages)_perpPage=0;
  const start=_perpPage*PERP_PAGE_SIZE;
  const page=perpPositions.slice(start,start+PERP_PAGE_SIZE);
  tbody.innerHTML=page.map(p=>{
    const sideColor=p.side==="LONG"?"var(--green)":"var(--red)";
    const pnlCls=p.upl>=0?"pnl-pos":"pnl-neg";
    const sign=p.upl>=0?"+":"";
    const pnlPct = p.avg_px ? ((p.upl / (Math.abs(p.size)*p.avg_px))*100).toFixed(2) : "0.00";
    return `<tr>
      <td style="font-weight:600">${p.ticker}</td>
      <td style="color:${sideColor};font-weight:600">${p.side}</td>
      <td>$${p.avg_px?.toFixed(4)||"—"}</td>
      <td>$${p.last_px?.toFixed(4)||"—"}</td>
      <td>$${(p.size_usd||0).toFixed(2)}</td>
      <td>${p.lever||5}x</td>
      <td class="${pnlCls}">${sign}$${(p.upl||0).toFixed(2)}</td>
      <td class="${pnlCls}">${sign}${pnlPct}%</td>
      <td style="color:var(--text-dim)">${Math.abs(p.size).toFixed(2)}</td>
    </tr>`;
  }).join("");
  if(pager){
    let btns='';
    for(let i=0;i<totalPages;i++){
      btns+=`<button class="page-btn${i===_perpPage?' active':''}" onclick="setPerpPage(${i})">${i+1}</button>`;
    }
    pager.innerHTML=`<span class="page-info">${total} ${lang==="zh"?"条记录":"records"}</span>${btns}`;
  }
}
function setPerpPage(p){_perpPage=p;const s=window.__lastDeskState||{};if(s.perp_positions)renderPerpPositions(s.perp_positions,s.perp_stats,s.perp_config);}

// === History ===
function renderHistory(closed){
  const tbody=$("hist-body");
  if(!closed||!closed.length){
    tbody.innerHTML=`<tr><td colspan="10" class="empty-row">— ${lang==="zh"?"暂无历史":"no history yet"} —</td></tr>`;return;
  }
  tbody.innerHTML=closed.map(c=>{
    const sign=c.pnl_usd>0?"+":"";
    const entryDate=c.entry_ts?new Date(c.entry_ts*1000).toLocaleString("en-CA",{year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",hour12:false}).replace("T"," "):`${c.entry_min}m`;
    const exitDate=c.exit_ts?new Date(c.exit_ts*1000).toLocaleString("en-CA",{year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",hour12:false}).replace("T"," "):`${c.exit_min}m`;
    const sideCls=(c.side==="BUY"||c.side==="LONG")?'side-buy':'side-sell';
    const sideLabel=c.side==="LONG"?"LONG":(c.side==="SHORT"?"SHORT":c.side||"BUY");
    const pnlCls=c.win?"pnl-pos":"pnl-neg";
    return `<tr><td style="font-weight:600">${c.ticker}</td><td class="${sideCls}">${sideLabel}</td><td style="color:var(--text-dim)">${entryDate}</td><td>$${(c.entry_price||0).toFixed(6)}</td><td>$${c.entry_usd.toFixed(2)}</td>
      <td style="color:var(--text-dim)">${exitDate}</td><td>$${(c.exit_price||0).toFixed(6)}</td><td>$${c.exit_usd.toFixed(2)}</td>
      <td class="${pnlCls}">${c.pnl_mult.toFixed(2)}x (${sign}${((c.pnl_mult-1)*100).toFixed(1)}%)</td>
      <td class="${pnlCls}">${sign}$${c.pnl_usd.toFixed(2)}</td></tr>`;
  }).join("");
}

// === OKX Trade History (paginated) ===
let _okxTradePage=0;
const OKX_TRADE_PAGE_SIZE=10;
function renderOkxTrades(fills){
  const tbody=document.getElementById("swap-body");
  if(!tbody)return;
  if(!fills||!fills.length){
    tbody.innerHTML=`<tr><td colspan="7" class="empty-row">— ${lang==="zh"?"暂无OKX成交":"no OKX trades"} —</td></tr>`;return;
  }
  const total=fills.length;
  const totalPages=Math.max(1,Math.ceil(total/OKX_TRADE_PAGE_SIZE));
  if(_okxTradePage>=totalPages)_okxTradePage=0;
  const start=_okxTradePage*OKX_TRADE_PAGE_SIZE;
  const page=fills.slice(start,start+OKX_TRADE_PAGE_SIZE);
  let html=`<tr><td colspan="7" style="color:var(--text-dim);font-size:10px;padding:4px 12px;text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono)">— ${lang==="zh"?"OKX 真实成交 (最近 7 天)":"OKX Real Fills (Last 7d)"} (${total}) —</td></tr>`;
  page.forEach(t=>{
    const sideCls=(t.side||"").toUpperCase()==="BUY"?"side-buy":"side-sell";
    const feeSign=t.fee<0?"":"-";
    html+=`<tr>
      <td class="${sideCls}">${t.side||"—"}</td>
      <td style="font-weight:600">${t.ticker||"?"}</td>
      <td>$${(t.notional_usd||0).toFixed(2)}</td>
      <td style="color:var(--text-dim)">${feeSign}$${Math.abs(t.fee||0).toFixed(4)}</td>
      <td style="color:var(--text-dim)">@ $${(t.fill_px||0).toFixed(t.fill_px>100?2:4)}</td>
      <td style="color:var(--text-dim)">${(t.fill_sz||0).toFixed(t.fill_sz>100?2:4)} ${(t.inst_id||"").split("-")[0]||""}</td>
      <td style="color:var(--text-dim);font-family:var(--mono);font-size:10px">${t.ts_ms?new Date(t.ts_ms).toLocaleString():t.inst_id?.substring(0,16)||"—"}</td>
    </tr>`;
  });
  const pager=document.getElementById("swap-pager");
  if(pager){
    let btns='';
    for(let i=0;i<totalPages;i++){
      btns+=`<button class="page-btn${i===_okxTradePage?' active':''}" onclick="setOkxTradePage(${i})">${i+1}</button>`;
    }
    pager.innerHTML=`<span class="page-info">${total} ${lang==="zh"?"条记录":"records"}</span>${btns}`;
  }
  tbody.innerHTML=html;
}
function setOkxTradePage(p){_okxTradePage=p;const s=window.__lastDeskState||{};if(s.okx_trades_history)renderOkxTrades(s.okx_trades_history);}

// === Swaps table ===
let _swapVersions=0;
let _isOkxMode = false;
function renderSwaps(swapsData, isOkx){
  if(!swapsData) return;
  _isOkxMode = !!isOkx;
  const tbody=document.getElementById("swap-body");
  const badge=document.getElementById("swap-badge");
  const card=document.getElementById("swaps-card");
  if(!tbody||!badge) return;
  const pending=(swapsData.pending||[]).slice(-30);
  const submitted=(swapsData.submitted||[]).slice(-20);
  const failed=(swapsData.failed||[]).slice(-10);
  badge.textContent=`(${pending.length})`;

  if(card){
    const h2=card.querySelector(".panel-title");
    if(h2){
      h2.innerHTML = _isOkxMode
        ? '<span class="dot"></span><span data-i18n="swapsTitle">📊 OKX TRADE HISTORY</span> <span id="swap-badge" class="badge green">(auto)</span>'
        : `<span class="dot amber"></span><span data-i18n="swapsTitle">🔗 SWAP BUNDLES</span> <span id="swap-badge" class="badge">(${pending.length})</span>`;
    }
    const pHead=card.querySelector(".panel-title");
    if(pHead){
      pHead.setAttribute("data-tip", _isOkxMode
        ? "OKX 自动交易记录\n平台自动下单，无需 Phantom 签名"
        : "🔗 Swap Bundles — 待签名交易列表");
    }
  }

  let html="";
  if(!_isOkxMode){
    pending.forEach(s=>{
      const sideCls=s.side==="BUY"?"side-buy":"side-sell";
      html+=`<tr data-key="${s.key}">
        <td class="${sideCls}">${s.side}</td>
        <td style="font-weight:600">${s.ticker}</td>
        <td>$${s.amount_usd.toFixed(2)}</td>
        <td><span class="tag ${s.platform==='okx'?'tag-okx':'tag-jup'}">${s.platform==='okx'?'OKX':'JUP'}</span></td>
        <td style="color:var(--amber)">${Math.round(s.age_s||0)}s</td>
        <td>—</td>
        <td><button class="action-btn" onclick="submitSwap('${s.key}')" id="btn-${s.key}">▶ ${lang==="zh"?"提交":"SUBMIT"}</button></td>
      </tr>`;
    });
    if(submitted.length){
      html+=`<tr><td colspan="7" style="color:var(--text-dim);font-size:10px;padding:4px 12px;text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono)">— ${lang==="zh"?"最近提交":"Recent Submitted"} (${submitted.length}) —</td></tr>`;
      submitted.slice(-5).forEach(s=>{
        html+=`<tr><td class="side-buy">${s.side}</td><td style="font-weight:600">${s.ticker||"?"}</td>
          <td>$${s.amount_usd?.toFixed(2)||"—"}</td><td><span class="tag tag-okx">OKX</span></td><td colspan="3" style="color:var(--text-dim);font-size:9px;font-family:var(--mono)">${s.order_id?s.order_id.substring(0,16):"—"}</td></tr>`;
      });
    }
    if(failed.length){
      html+=`<tr><td colspan="7" style="color:var(--text-dim);font-size:10px;padding:4px 12px;text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono)">— ${lang==="zh"?"最近失败":"Recent Failed"} (${failed.length}) —</td></tr>`;
      failed.slice(-3).forEach(s=>{
        html+=`<tr><td class="side-sell">${s.side||"FAIL"}</td><td>${s.ticker||"?"}</td>
          <td colspan="5" style="color:var(--red);font-size:10px">${String(s.error||"").substring(0,60)}</td></tr>`;
      });
    }
    if(!html) html=`<tr><td colspan="7" class="empty-row">— ${lang==="zh"?"无待处理交易":"no pending swaps"} —</td></tr>`;
  } else {
    // OKX mode: prefer real fills from API, fallback to engine-tracked trades
    const realFills = (typeof window.__okxTradeHistory !== 'undefined') ? window.__okxTradeHistory : [];
    const fallbackTrades = [...failed, ...submitted].sort((a,b)=>(b.timestamp||0)-(a.timestamp||0));
    const primary = realFills.length > 0 ? realFills : fallbackTrades;
    const isReal = realFills.length > 0;
    if(primary.length){
      if(isReal){
        html+=`<tr><td colspan="7" style="color:var(--text-dim);font-size:10px;padding:4px 12px;text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono)">— ${lang==="zh"?"OKX 真实成交 (最近 7 天)":"OKX Real Fills (Last 7d)"} (${primary.length}) —</td></tr>`;
        primary.forEach(t=>{
          const sideCls = (t.side||"").toUpperCase()==="BUY"?"side-buy":"side-sell";
          const feeSign = t.fee < 0 ? "" : "-";
          html+=`<tr>
            <td class="${sideCls}">${t.side||"—"}</td>
            <td style="font-weight:600">${t.ticker||"?"}</td>
            <td>$${(t.notional_usd||0).toFixed(2)}</td>
            <td style="color:var(--text-dim)">${feeSign}$${Math.abs(t.fee||0).toFixed(4)}</td>
            <td style="color:var(--text-dim)">@ $${(t.fill_px||0).toFixed(t.fill_px>100?2:4)}</td>
            <td style="color:var(--text-dim)">${(t.fill_sz||0).toFixed(t.fill_sz>100?2:4)} ${(t.inst_id||"").split("-")[0]||""}</td>
            <td style="color:var(--text-dim);font-family:var(--mono);font-size:10px">${t.ts_ms?new Date(t.ts_ms).toLocaleString():t.inst_id?.substring(0,16)||"—"}</td>
          </tr>`;
        });
      } else {
        html+=`<tr><td colspan="7" style="color:var(--text-dim);font-size:10px;padding:4px 12px;text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono)">— ${lang==="zh"?"交易记录 (OKX 自动下单)":"Trade History (OKX Auto)"} (${primary.length}) —</td></tr>`;
        primary.forEach(s=>{
          const isFail = !!s.error;
          const sideCls = isFail ? "" : (s.side==="BUY"?"side-buy":"side-sell");
          html+=`<tr>
            <td class="${sideCls}">${s.side||"FAIL"}</td>
            <td style="font-weight:600">${s.ticker||"?"}</td>
            <td>$${(s.amount_usd||0).toFixed(2)}</td>
            <td style="color:var(--text-dim)">$${(s.fee_usd||0).toFixed(2)}</td>
            <td style="color:var(--text-dim)">—</td>
            <td style="color:var(--text-dim)">—</td>
            <td style="${isFail?'color:var(--red)':''}">${isFail?'✗ '+String(s.error||"").substring(0,20):(s.order_id||"—")}</td>
          </tr>`;
        });
      }
    } else {
      html=`<tr><td colspan="7" class="empty-row">— ${lang==="zh"?"暂无交易记录":"no trades yet"} —</td></tr>`;
    }
  }
  tbody.innerHTML=html;
}

// === Swap signing + submission via Phantom wallet ===
const SwapCodec = {
  base64ToBytes(s){return Uint8Array.from(atob(s),c=>c.charCodeAt(0));},
  bytesToBase64(b){let s="";for(const x of b)s+=String.fromCharCode(x);return btoa(s);},
};

async function ensurePhantom(){
  const w = window.phantom?.solana;
  if(!w){
    showToast(lang==="zh"?"❌ 未检测到 Phantom 钱包 - 请安装浏览器扩展":"❌ Phantom wallet not found - install browser extension",true);
    return null;
  }
  if(!w.isConnected){ try{ await w.connect(); }catch(e){ showToast(lang==="zh"?"❌ Phantom 连接失败":"❌ Phantom connect failed",true); return null; } }
  return w;
}

async function submitSwap(key){
  const btn=$("btn-"+key);
  if(!btn||btn.disabled)return;
  btn.disabled=true;
  btn.textContent=lang==="zh"?"连接 Phantom...":"Connecting Phantom...";

  let swapsResp, swapsData;
  try{
    swapsResp = await fetch("/api/swaps",{cache:"no-store"});
    swapsData = await swapsResp.json();
  }catch(e){ showToast(`❌ ${e.message}`,true); btn.disabled=false; btn.textContent=lang==="zh"?"▶ 提交":"▶ SUBMIT"; return; }

  const pending = (swapsData.pending||[]).find(s=>s.key===key);
  if(!pending||!pending.bundle){ btn.disabled=false; btn.textContent=lang==="zh"?"▶ 提交":"▶ SUBMIT"; showToast(lang==="zh"?"❌ Swap 已不在待处理列表":"❌ Swap no longer pending",true); return; }

  const txB64 = pending.bundle.transaction_base64;
  if(!txB64){ btn.disabled=false; btn.textContent=lang==="zh"?"▶ 提交":"▶ SUBMIT"; showToast(lang==="zh"?"❌ 空交易包":"❌ Empty tx bundle",true); return; }

  const phantom = await ensurePhantom();
  if(!phantom){ btn.disabled=false; btn.textContent=lang==="zh"?"▶ 提交":"▶ SUBMIT"; return; }

  btn.textContent=lang==="zh"?"钱包签名中...":"Signing...";
  let signedB64;
  try{
    const txBytes = SwapCodec.base64ToBytes(txB64);
    const signedOne = await phantom.signTransaction(txBytes);
    signedB64 = SwapCodec.bytesToBase64(signedOne);
  }catch(e){
    btn.disabled=false;
    btn.textContent=lang==="zh"?"▶ 提交":"▶ SUBMIT";
    showToast(`❌ ${lang==="zh"?"签名失败":"Sign failed"}: ${e.message||e}`,true);
    return;
  }

  btn.textContent=lang==="zh"?"链上广播中...":"Broadcasting...";
  try{
    const resp = await fetch("/api/swap/submit",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({key,signed_tx:signedB64})
    });
    const data = await resp.json();
    if(data.ok){
      btn.textContent="✓ "+(lang==="zh"?"已提交":"SUBMITTED");
      btn.style.background="var(--green)";
      showToast(`✅ ${lang==="zh"?"交易已提交":"Swap submitted"}: ${data.sig?.substring(0,16)}...`);
      refreshStatus();
    }else{
      btn.classList.add("failed");
      btn.textContent="✗ "+(lang==="zh"?"失败":"FAILED");
      showToast(`❌ ${data.error||"unknown error"}`,true);
      setTimeout(()=>{btn.disabled=false;btn.textContent="▶ "+(lang==="zh"?"提交":"SUBMIT");btn.classList.remove("failed");},3000);
    }
  }catch(e){
    btn.classList.add("failed");btn.textContent="✗ "+(lang==="zh"?"失败":"FAILED");
    showToast(`❌ ${e.message}`,true);
    setTimeout(()=>{btn.disabled=false;btn.textContent="▶ "+(lang==="zh"?"提交":"SUBMIT");btn.classList.remove("failed");},3000);
  }
}

// === Wallet connect ===
function connectWallet(){
  const pubkey=document.getElementById("walletInput").value.trim();
  if(!pubkey){showToast(lang==="zh"?"请粘贴 Solana 钱包地址":"Please paste your Solana wallet pubkey",true);return;}
  localStorage.setItem("rh_wallet",pubkey);
  document.getElementById("walletLabel").textContent="Wallet: "+pubkey.substring(0,8)+"..."+pubkey.substring(pubkey.length-6);
  document.getElementById("walletDot").classList.add("connected");
  showToast(lang==="zh"?"✅ 钱包已连接":"✅ Wallet connected");
  refreshStatus();
}
function loadWallet(){
  const w=localStorage.getItem("rh_wallet");
  if(w){
    const wi=document.getElementById("walletInput"); if(wi)wi.value=w;
    const wl=document.getElementById("walletLabel"); if(wl)wl.textContent="Wallet: "+w.substring(0,8)+"..."+w.substring(w.length-6);
    const wd=document.getElementById("walletDot"); if(wd)wd.classList.add("connected");
  }
}

// === Manual signal test ===
function manualTest(){
  const row=$("manualRow");
  row.style.display=row.style.display==="none"?"flex":"none";
}
async function sendManualSignal(){
  const ticker=$("manualTicker").value.trim().toUpperCase();
  const side=$("manualSide").value;
  const usd=parseFloat($("manualUsd").value)||50;
  try{
    const resp=await fetch("/api/manual/signal",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ticker,side,usd})});
    const data=await resp.json();
    if(data.ok)showToast(`✅ ${side} ${ticker} $${usd} queued`);
    else showToast(`❌ ${data.error}`,true);
  }catch(e){showToast(`❌ ${e.message}`,true);}
}

// === Toast notification ===
function showToast(msg,isError=false){
  const t=document.createElement("div");
  t.style.cssText=`position:fixed;bottom:20px;right:20px;padding:10px 18px;border-radius:4px;font-family:var(--mono);font-size:11px;z-index:9999;max-width:400px;word-break:break-all;text-transform:uppercase;letter-spacing:.04em;${isError?"background:var(--red);color:#fff":"background:var(--green);color:var(--bg)"}`;
  t.textContent=msg;document.body.appendChild(t);
  setTimeout(()=>t.remove(),4000);
}

// === Refresh server status ===
async function refreshStatus(){
  try{
    const r=await fetch("/api/status");
    const s=await r.json();
    if(s.error){showToast(s.error,true);return;}
    $("mon-sol").textContent="$"+(s.sol_usd||0).toFixed(2);
    $("mon-pending").textContent=s.pending_count||0;
    $("mon-submitted").textContent=s.submitted_count||0;
    if(s.recent_submitted?.length){
      const last=s.recent_submitted[s.recent_submitted.length-1];
      showToast(`📤 ${last.side} ${last.ticker}: ${last.sig?.substring(0,12)}...`);
    }
    const demoBadge = document.getElementById("mon-demo-badge");
    if (demoBadge) demoBadge.style.display = s.demo_mode ? "" : "none";
  }catch(e){}
}

// === SSE connection ===
function applyDeskState(s){
  if(!s)return;
  window.__lastDeskState = s;
  // ── OKX REAL ACCOUNT DATA: use real balance and skip simulator positions ──
  const okxBalance = s.okx_connected && s.okx_real_balance ? s.okx_real_balance : null;
  if(okxBalance){
    const pnlSub = document.getElementById('mon-pnl-sub');
    if(pnlSub) {
      const startBank = s.monitor && s.monitor.start_bankroll ? s.monitor.start_bankroll : 500;
      const realPnl = okxBalance.total_eq_usd - startBank;
      pnlSub.textContent = (realPnl >= 0 ? '+' : '') + '$' + Math.abs(realPnl).toFixed(2);
      pnlSub.style.color = realPnl >= 0 ? 'var(--green)' : 'var(--red)';
    }
  }
  if(s.equity&&Array.isArray(s.equity)&&s.equity.length>0){
    // OKX mode: build equity curve from OKX real balance (cached, not per-tick)
    if(okxBalance && okxBalance.total_eq_usd > 0){
      const nowSec = Math.floor(Date.now()/1000);
      const bal = okxBalance.total_eq_usd;
      // Add point every 30s to match OKX cache refresh rate
      if(nowSec - _eqOkxLastSec >= 30 || _eqOkxAccum.length === 0){
        _eqOkxAccum.push({t: nowSec, v: bal});
        _eqOkxLastSec = nowSec;
      }
      eqW = _eqOkxAccum.map(p=>p.t);
      eqV = _eqOkxAccum.map(p=>p.v);
    } else if (!okxBalance) {
      // No OKX balance available — fall back to simulator equity curve
      eqW = s.equity.map(p=>p[0]);
      eqV = s.equity.map(p=>p[1]);
    }
  } else if(s.equity && Array.isArray(s.equity) && s.equity.length > 0) {
    // Fallback: use raw equity data directly
    eqW = s.equity.map(p=>p[0]);
    eqV = s.equity.map(p=>p[1]);
  }
  try { if(s.monitor&&s.monitor.multiple!==undefined){updateMonitor(s.monitor, okxBalance);} } catch(e){ /* legacy IDs may be missing */ }
  // OKX mode: render perp positions, skip simulator positions
  if(s.perp_positions && s.perp_positions.length > 0){
    renderPerpPositions(s.perp_positions, s.perp_stats, s.perp_config);
    // Hide simulator Open Positions card (shows nothing but takes up space)
    const posCard = document.getElementById('positions-card');
    if(posCard) posCard.style.display = 'none';
  } else {
    // Non-OKX mode: use simulator positions
    if(s.positions)renderPositions(s.positions);
    const posCard = document.getElementById('positions-card');
    if(posCard) posCard.style.display = '';
  }
  if(s.feed){renderFeed(s.feed);}
  if(s.closed_trades){renderHistory(s.closed_trades);}
  // OKX mode: render real trades via paginated helper
  if(s.okx_connected && s.okx_trades_history && s.okx_trades_history.length > 0){
    renderOkxTrades(s.okx_trades_history);
  } else {
    if(s.swaps){renderSwaps(s.swaps, s.is_okx);}
  }
  if(s.snap){}
  if(s.okx_connected !== undefined) updateLandingState(s.okx_connected, s.data_source);
  if(s.decisions && Array.isArray(s.decisions)) renderDecisions(s.decisions);
  if(s.today_pnl !== undefined) updateTodayPnl(s.today_pnl);
  if(s.risk_pct !== undefined) updateRiskBar(s.risk_pct, s.risk_limit || 8);
  drawEquity();
}
let _pollTimer=null;
function startPolling(){
  if(_pollTimer)clearInterval(_pollTimer);
  console.log('[RH] polling /api/desk-state every 1s');
  _pollTimer=setInterval(async()=>{
    try{
      const r=await fetch('/api/desk-state',{cache:'no-store'});
      if(r.ok){const s=await r.json();applyDeskState(s);}
    }catch(e){}
  },1000);
}
function startSSE(){
  if(lastSSE){try{lastSSE.close();}catch(e){}lastSSE=null;}
  if(_pollTimer){clearInterval(_pollTimer);_pollTimer=null;}
  eqW=[];eqV=[];_eqOkxAccum=[];_eqOkxLastSec=0;
  let gotData=false;
  const ev=new EventSource("/events");
  lastSSE=ev;
  const fallbackTimer=setTimeout(()=>{
    if(!gotData){
      console.log('[RH] SSE no data after 3s, falling back to polling');
      try{ev.close();}catch(e){}
      lastSSE=null;
      startPolling();
    }
  },3000);
  ev.onmessage=e=>{
    gotData=true;
    clearTimeout(fallbackTimer);
    try{
      ev.close();
    }catch(e){}
    lastSSE=null;
    startPolling();
  };
  ev.onerror=()=>{};
}

// === Animation loop ===
let okxConnected = false;
let decisions = [];
let riskPreference = localStorage.getItem("risk_pref") || "balanced";
let tradeMode = localStorage.getItem("trade_mode") || "signal_only";
let todayPnlAccum = 0;
let _rafLastDraw = 0;
const RAF_THROTTLE_MS = 250; // ~4fps: equity data changes every ~2s, no need for 60fps redraw

function animLoop(now){
  if(now - _rafLastDraw >= RAF_THROTTLE_MS){
    _rafLastDraw = now;
    _pulsePhase=(now%1500)/1500; // 1.5s pulse cycle
    drawEquity();
  }
  animFrame=requestAnimationFrame(animLoop);
}

// === Resize equity canvas (content-area aware: fixes padding overflow) ===
function resizeEquityCanvas(){
  const canvas=document.getElementById("equity");
  if(!canvas)return;
  const wrap=canvas.parentElement;
  const cs=getComputedStyle(wrap);
  const rect=wrap.getBoundingClientRect();
  const padX=parseFloat(cs.paddingLeft)+parseFloat(cs.paddingRight);
  const padY=parseFloat(cs.paddingTop)+parseFloat(cs.paddingBottom);
  const cw=rect.width-padX;
  const ch=rect.height-padY;
  if(Math.abs(cw)>0 && Math.abs(ch)>0){
    const dpr=window.devicePixelRatio||1;
    canvas.width=Math.floor(cw*dpr);
    canvas.height=Math.floor(ch*dpr);
    canvas.style.width=cw+"px";
    canvas.style.height=ch+"px";
    eqCtx.setTransform(dpr,0,0,dpr,0,0);
  }
}
resizeEquityCanvas();
new ResizeObserver(resizeEquityCanvas).observe(document.getElementById("equity-card"));
window.addEventListener("resize",resizeEquityCanvas);

animLoop();
loadWallet();
applyLang();
(function restoreState(){
  const rp = localStorage.getItem("risk_pref");
  if(rp) riskPreference = rp;
  const tm = localStorage.getItem("trade_mode");
  if(tm) tradeMode = tm;
  const modeEl = document.getElementById("mon-mode");
  if(modeEl) modeEl.textContent = tradeMode === "auto"
    ? (lang === "zh" ? "全自动" : "AUTO")
    : (lang === "zh" ? "仅信号" : "SIGNS");
})();
window.addEventListener("load",()=>setTimeout(startSSE,300));

function updateLandingState(okxAuthReady, dataSource) {
  okxConnected = okxAuthReady;
  const landing = document.getElementById("landing-card");
  const riskBar = document.getElementById("risk-bar");
  const dsBadge = document.getElementById("mon-data-source");
  const demoBadge = document.getElementById("mon-demo-badge");
  const src = (dataSource || (okxConnected ? "OKX" : "JUP"));
  if (dsBadge) {
    dsBadge.style.display = "";
    if (src === "OKX") {
      dsBadge.className = "topbar-badge live";
      dsBadge.textContent = "● OKX LIVE";
      dsBadge.title = "Data source: OKX Demo Trading (locked)";
    } else {
      dsBadge.className = "topbar-badge demo";
      dsBadge.textContent = "● DEMO";
      dsBadge.title = "Data source: Jupiter / simulator";
    }
  }
  if (demoBadge) {
    demoBadge.style.display = (src === "OKX") ? "none" : "";
  }
  if (!okxConnected) {
    if (landing) landing.style.display = "";
    if (riskBar) riskBar.style.display = "none";
  } else {
    if (landing) landing.style.display = "none";
    if (riskBar) riskBar.style.display = "";
  }
}

// ── Decision Log ─────────────────────────────────────────────────
function renderDecisions(list) {
  if (!list || !list.length) {
    document.getElementById("decision-list").innerHTML =
      lang === "zh" ? "— 暂无 AI 决策记录 —" : "— no AI decisions yet —";
    const badge = document.getElementById("decision-badge");
    if (badge) badge.textContent = "(0)";
    return;
  }
  const badge = document.getElementById("decision-badge");
  if (badge) badge.textContent = `(${list.length})`;
  const html = list.map((d, i) => {
    const sideCls = d.side === "BUY" || d.side === "LONG" ? "buy" : "sell";
    const sideLabel = d.side === "BUY" || d.side === "LONG"
      ? (lang === "zh" ? "做多" : "LONG")
      : (lang === "zh" ? "做空" : "SHORT");
    const reasonsHtml = (d.reasons || []).map(r => `<li>${escHtml(r)}</li>`).join("");
    const actionsHtml = (tradeMode === "auto")
      ? `<span class="btn-auto-executed">${lang === "zh" ? "已自动执行" : "Auto-executed"}</span>`
      : `<button class="btn-confirm" onclick="confirmDecision(${i})">${I18N[lang].confirmBtn}</button>`
        + `<button class="btn-ignore" onclick="ignoreDecision(${i})">${I18N[lang].ignoreBtn}</button>`;
    return `<div class="decision-item">
      <div class="decision-header">
        <span class="decision-pair">${escHtml(d.pair)}</span>
        <span class="decision-side ${sideCls}">${sideLabel}</span>
        <span class="decision-time">${d.time || ""}</span>
      </div>
      <div class="decision-body">
        <span data-i18n="decisionPct" data-arg="${d.sizePct||''}">${lang==="zh"
          ? '账户 <span id="dec-pct-val">'+(d.sizePct||'')+'</span>'
          : 'Account <span id="dec-pct-val">'+(d.sizePct||'')+'</span>'
        }</span>
      </div>
      ${reasonsHtml ? `<ul class="decision-reasons">${reasonsHtml}</ul>` : ""}
      <div class="decision-risk">${escHtml(d.riskInfo || "")}</div>
      <div class="decision-actions">${actionsHtml}</div>
    </div>`;
  }).join("");
  document.getElementById("decision-list").innerHTML = html;
}

function confirmDecision(idx) {
  const d = decisions[idx];
  if (!d) return;
  showToast(lang === "zh" ? `✅ 已确认 ${d.pair}` : `✅ Confirmed ${d.pair}`);
  decisions.splice(idx, 1);
  renderDecisions(decisions);
}
function ignoreDecision(idx) {
  const d = decisions[idx];
  if (!d) return;
  showToast(lang === "zh" ? `🚫 已忽略 ${d.pair}` : `🚫 Ignored ${d.pair}`);
  decisions.splice(idx, 1);
  renderDecisions(decisions);
}

// ── Risk Bar ─────────────────────────────────────────────────────
function updateRiskBar(pct, limit) {
  const fill = document.getElementById("risk-fill");
  const text = document.getElementById("risk-text");
  if (!fill || !text) return;
  const ratio = Math.min(100, (pct / limit) * 100);
  fill.style.width = ratio + "%";
  if (ratio < 50) fill.style.background = "var(--green)";
  else if (ratio < 80) fill.style.background = "var(--amber)";
  else fill.style.background = "var(--red)";
  text.textContent = `${pct.toFixed(1)}% / ${limit}%`;
}

// ── Today P&L ────────────────────────────────────────────────────
function updateTodayPnl(val) {
  const el = document.getElementById("mon-pnl");
  if (!el) return;
  const v = parseFloat(val) || 0;
  el.textContent = (v >= 0 ? "+" : "") + "$" + v.toFixed(2);
  el.style.color = v > 0 ? "var(--green)" : v < 0 ? "var(--red)" : "var(--text-dim)";
  el.classList.toggle('up', v > 0);
  el.classList.toggle('down', v < 0);
}

// ── Settings Modal ───────────────────────────────────────────────
function openSettings() {
  let existing = document.querySelector(".modal-overlay");
  if (existing) existing.remove();

  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.id = "settings-modal";
  overlay.innerHTML = `
    <div class="modal-box">
      <button class="close-btn" onclick="closeSettings()">✕</button>
      <h2>${lang === "zh" ? "设置" : "SETTINGS"}</h2>

      <div class="modal-section">
        <label>${I18N[lang].riskPref}</label>
        <div class="risk-options">
          <div class="risk-option ${riskPreference==='conservative'?'active':''}" onclick="setRisk('conservative')">
            <div class="risk-name">${I18N[lang].conservative}</div>
            <div class="risk-desc">${I18N[lang].conservativeDesc}</div>
          </div>
          <div class="risk-option ${riskPreference==='balanced'?'active':''}" onclick="setRisk('balanced')">
            <div class="risk-name">${I18N[lang].balanced}</div>
            <div class="risk-desc">${I18N[lang].balancedDesc}</div>
          </div>
          <div class="risk-option ${riskPreference==='aggressive'?'active':''}" onclick="setRisk('aggressive')">
            <div class="risk-name">${I18N[lang].aggressive}</div>
            <div class="risk-desc">${I18N[lang].aggressiveDesc}</div>
          </div>
        </div>
      </div>

      <div class="modal-section">
        <label>${I18N[lang].tradeMode}</label>
        <div class="mode-options">
          <div class="mode-option ${tradeMode==='signal_only'?'active':''}" onclick="setMode('signal_only')">
            <div class="mode-name">${I18N[lang].signalOnly}</div>
            <div class="mode-desc">${I18N[lang].signalOnlyDesc}</div>
          </div>
          <div class="mode-option ${tradeMode==='auto'?'active':''}" onclick="setMode('auto')">
            <div class="mode-name">${I18N[lang].autoExec}</div>
            <div class="mode-desc">${I18N[lang].autoExecDesc}</div>
          </div>
        </div>
      </div>

      <div class="modal-section">
        <label>${I18N[lang].strategyGuide}</label>
        <div class="strat-content">
          <h3>${I18N[lang].strat_h1}</h3>
          <p><b>${I18N[lang].strat_s1}</b><br>${I18N[lang].strat_s1d}</p>
          <p><b>${I18N[lang].strat_s2}</b><br>${I18N[lang].strat_s2d}</p>
          <p><b>${I18N[lang].strat_s3}</b><br>${I18N[lang].strat_s3d}</p>
          <p><b>${I18N[lang].strat_s4}</b><br>${I18N[lang].strat_s4d}</p>
          <p><b>${I18N[lang].strat_s5}</b><br>${I18N[lang].strat_s5d}</p>
          <h3>${I18N[lang].strat_trust}</h3>
          <ul>
            <li>${I18N[lang].strat_t1}</li>
            <li>${I18N[lang].strat_t2}</li>
            <li>${I18N[lang].strat_t3}</li>
          </ul>
        </div>
      </div>

      <div class="modal-section">
        <label style="display:flex;align-items:center;gap:8px">
          ${lang==="zh"?"API 密钥管理":"API Key Management"}
          <span id="api-key-status" style="font-size:9px;padding:2px 6px;border-radius:10px;background:var(--surface2);color:var(--text-dim);border:1px solid var(--border)">—</span>
        </label>
        <div id="api-keys-section" style="margin-top:8px;display:none">
          <div style="margin-bottom:8px"><label style="font-size:10px;color:var(--text-dim)">${lang==="zh"?"API Key":""}</label><input id="api-key-input" type="text" readonly style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text-dim);padding:6px 10px;font-family:var(--mono);border-radius:2px;margin-top:4px;font-size:11px"></div>
          <div style="margin-bottom:8px"><label style="font-size:10px;color:var(--text-dim)">${lang==="zh"?"Secret":""}</label><input id="api-secret-input" type="password" readonly style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text-dim);padding:6px 10px;font-family:var(--mono);border-radius:2px;margin-top:4px;font-size:11px"></div>
          <div style="margin-bottom:12px"><label style="font-size:10px;color:var(--text-dim)">${lang==="zh"?"Passphrase":""}</label><input id="api-pass-input" type="password" readonly style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text-dim);padding:6px 10px;font-family:var(--mono);border-radius:2px;margin-top:4px;font-size:11px"></div>
          <button class="btn-primary" onclick="loadApiKeys()" style="font-size:11px;padding:6px 12px">${lang==="zh"?"刷新密钥状态":"Refresh Status"}</button>
          <button class="btn-secondary" onclick="openApiModal()" style="font-size:11px;padding:6px 12px;margin-left:8px">${lang==="zh"?"更新密钥":"Update Keys"}</button>
        </div>
      </div>

      <div class="modal-footer">
        <button class="btn-secondary" onclick="closeSettings()">${lang==="zh"?"关闭":"CLOSE"}</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  overlay.addEventListener("click", e => { if (e.target === overlay) closeSettings(); });
  // Auto-load API key status
  setTimeout(loadApiKeys, 300);
}

function closeSettings() {
  const m = document.getElementById("settings-modal");
  if (m) m.remove();
}

function setRisk(pref) {
  riskPreference = pref;
  localStorage.setItem("risk_pref", pref);
  document.querySelectorAll(".risk-option").forEach(el => el.classList.remove("active"));
  event.currentTarget.classList.add("active");
  showToast(lang === "zh" ? `风险偏好已设为：${pref}` : `Risk preference: ${pref}`);
  fetch("/api/settings", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({risk_preference: pref}),
  }).catch(() => {});
}

function setMode(mode) {
  if (mode === "auto" && tradeMode !== "auto") {
    if (!confirm(
      lang === "zh"
        ? "⚠️ 切换到「全自动」模式后将由 AI 自动下单，无法撤回。\n确认继续？"
        : "⚠️ Switching to Auto Execute will let AI place orders without confirmation.\nProceed?"
    )) return;
  }
  tradeMode = mode;
  localStorage.setItem("trade_mode", mode);
  document.querySelectorAll(".mode-option").forEach(el => el.classList.remove("active"));
  event.currentTarget.classList.add("active");
  const modeEl = document.getElementById("mon-mode");
  if (modeEl) modeEl.textContent = mode === "auto"
    ? (lang === "zh" ? "全自动" : "AUTO")
    : (lang === "zh" ? "仅信号" : "SIGNS");
  fetch("/api/settings", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({trade_mode: mode}),
  }).catch(() => {});
}

// ── OKX API connect modal ───────────────────────────────────────
function openApiModal() {
  let existing = document.querySelector("#api-modal");
  if (existing) existing.remove();

  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.id = "api-modal";
  overlay.innerHTML = `
    <div class="modal-box">
      <button class="close-btn" onclick="document.getElementById('api-modal').remove()">✕</button>
      <h2>${lang === "zh" ? "连接 OKX API" : "CONNECT OKX API"}</h2>
      <p style="font-size:11px;color:var(--red);margin-bottom:12px;font-family:var(--mono);line-height:1.6">${lang === "zh"
        ? "⚠️ 请仅开启「交易」权限，切勿开启提币权限。密钥仅在后端使用，不保存在前端。"
        : "⚠️ Grant 'Trading' permission only. Do NOT enable withdrawal. Keys are stored server-side only."
      }</p>
      <div style="margin-bottom:12px">
        <label>${lang==="zh"?"API Key":""}</label>
        <input id="okxApiKey" type="text" placeholder="OKX API Key" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px 10px;font-family:var(--mono);border-radius:2px;margin-top:6px;font-size:11px">
      </div>
      <div style="margin-bottom:12px">
        <label>${lang==="zh"?"API Secret":""}</label>
        <input id="okxApiSecret" type="password" placeholder="OKX API Secret" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px 10px;font-family:var(--mono);border-radius:2px;margin-top:6px;font-size:11px">
      </div>
      <div style="margin-bottom:16px">
        <label>${lang==="zh"?"Passphrase":""}</label>
        <input id="okxPassphrase" type="password" placeholder="OKX Passphrase" style="width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:8px 10px;font-family:var(--mono);border-radius:2px;margin-top:6px;font-size:11px">
      </div>
      <div class="modal-footer">
        <button class="btn-secondary" onclick="document.getElementById('api-modal').remove()">${lang==="zh"?"取消":"CANCEL"}</button>
        <button id="okxConnectBtn" class="btn-primary" onclick="connectOkxApi()">${lang==="zh"?"连接":"CONNECT"}</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  overlay.addEventListener("click", e => { if (e.target === overlay) overlay.remove(); });
}

async function connectOkxApi() {
  const key = document.getElementById("okxApiKey").value.trim();
  const secret = document.getElementById("okxApiSecret").value.trim();
  const passphrase = document.getElementById("okxPassphrase").value.trim();
  if (!key || !secret || !passphrase) {
    showToast(lang === "zh" ? "请填写所有字段" : "Please fill all fields", true);
    return;
  }
  const btn = document.getElementById("okxConnectBtn");
  btn.disabled = true;
  btn.textContent = lang === "zh" ? "连接中..." : "CONNECTING...";
  try {
    const resp = await fetch("/api/okx/connect", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({api_key: key, api_secret: secret, passphrase}),
    });
    const data = await resp.json();
    if (data.ok) {
      okxConnected = true;
      updateLandingState(true, "OKX");
      document.getElementById("api-modal").remove();
      showToast(lang === "zh" ? "✅ OKX API 连接成功" : "✅ OKX API connected");
      refreshStatus();
    } else {
      showToast((data.error || "unknown error"), true);
    }
  } catch(e) {
    showToast(e.message, true);
  }
  btn.disabled = false;
  btn.textContent = lang === "zh" ? "连接" : "CONNECT";
}

// ── Helpers ──────────────────────────────────────────────────────
function escHtml(s) {
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}



// Override renderFeed to use feed-list items (V1 Terminal style)
const _origRenderFeed = typeof renderFeed !== 'undefined' ? renderFeed : null;
function renderFeed(feed){
  const list = document.getElementById('feed-list');
  if(!list) return;
  const items = (feed||[]).map((f,i)=>{
    const cls = {ENTRY:'buy',EXIT:'sell',STOP:'sell',HALT:'halt',LEARN:'buy',REJECT:'reject',NOT_BUY:'reject'}[f.side] || 'all';
    const pnlCls = {ENTRY:'side-buy',EXIT:'side-sell',STOP:'pnl-neg',HALT:'side-sell'}[f.side] || 'side-sell';
    const pnlText = {ENTRY:'ENTRY',EXIT:'EXIT',STOP:'STOP',HALT:'HALTED',REJECT:'BLOCKED',NOT_BUY:'SKIP'}[f.side] || f.side;
    // Use original timestamp from feed, not current time
    let timeStr = '';
    if (f.t) {
      const d = new Date(f.t * 1000);
      timeStr = d.toLocaleTimeString('en-GB',{hour12:false,hour:'2-digit',minute:'2-digit'});
    }
    return `<div class="feed-item" data-type="${cls}"><div class="feed-time">${timeStr}</div>
      <div class="feed-body"><div class="feed-ticker"><span class="side-${cls==='buy'?'buy':'sell'}">${f.side}</span> ${f.ticker} · ${f.mult.toFixed(2)}x</div>
      <div class="feed-note">${f.note||''}</div></div><div class="feed-pnl ${pnlCls}">${pnlText}</div></div>`;
  }).join('');
  // Only update if content changed (avoid unnecessary DOM refresh)
  if (list.innerHTML !== items && items !== '<div class="empty-row">— no signals —</div>') {
    list.innerHTML = items || '<div class="empty-row">— no signals —</div>';
  }
}


// Update topbar stats from monitor
// In OKX mode, the V1 Terminal (applyDeskState wrapper) owns the topbar rendering.
// The legacy wrapper below only touches legacy IDs (mon-bank, mon-mult, etc.) to avoid
// breaking pages that still rely on those element IDs. It deliberately skips topbar-equity /
// topbar-trades / topbar-winrate to prevent flip-flop with the V1 Terminal renderer.
const _origUpdateMonitor = typeof updateMonitor !== 'undefined' ? updateMonitor : null;
if(typeof updateMonitor !== 'undefined'){
  const _origUM = updateMonitor;
  updateMonitor = function(m){
    _origUM(m);
    if(!m) return;
    // Only update legacy monitor IDs — do NOT touch topbar elements in OKX mode
    const bankLegacy = document.getElementById('mon-bank');
    if(bankLegacy){
      const bank = m.bankroll || 0;
      bankLegacy.textContent = '$'+bank.toFixed(0);
      bankLegacy.className = (m.multiple||1)>=1 ? 'up' : 'down';
    }
    const multLegacy = document.getElementById('mon-mult');
    if(multLegacy) multLegacy.textContent = (m.multiple||1).toFixed(2)+'x';
    const entryLegacy = document.getElementById('mon-entry');
    if(entryLegacy) entryLegacy.textContent = m.entries||0;
    const wlLegacy = document.getElementById('mon-wl');
    if(wlLegacy) wlLegacy.textContent = (m.wins||0)+' / '+(m.losses||0);
    const pendingLegacy = document.getElementById('mon-pending');
    if(pendingLegacy) pendingLegacy.textContent = m.pending_swaps||0;
    const submittedLegacy = document.getElementById('mon-submitted');
    if(submittedLegacy) submittedLegacy.textContent = m.submitted_swaps||0;
    const stateLegacy = document.getElementById('mon-state');
    if(stateLegacy) stateLegacy.textContent = m.state||'—';
  };
}


// ============================================================
// V1 TERMINAL: COMPLETE DATA WIRING (consumes new backend monitor fields)
// ============================================================
const _origApplyDeskState = typeof applyDeskState !== 'undefined' ? applyDeskState : null;
if(typeof applyDeskState !== 'undefined'){
  applyDeskState = function(s){
    if(_origApplyDeskState) { try { _origApplyDeskState(s); } catch(e){ /* legacy */ } }
    if(!s) return;
    const mon = s.monitor || {};
    const snap = s.snap || {};
    // ── OKX REAL ACCOUNT DATA: override topbar equity with real balance ──
    const okxBalance = s.okx_connected && s.okx_real_balance ? s.okx_real_balance : null;
    // Store OKX trades history globally so renderSwaps can consume it
    if(s.okx_trades_history) window.__okxTradeHistory = s.okx_trades_history;

    // ── 1. Topbar: EQUITY / 24h PnL / WIN RATE / TRADES / KELLY ──
    const eqEl = document.getElementById('topbar-equity');
    if(eqEl){
      const eqVal = okxBalance ? okxBalance.total_eq_usd : (mon.bankroll||0);
      eqEl.textContent = '$'+eqVal.toFixed(2);
      eqEl.className = 'topbar-stat-val '+(mon.multiple>=1?'up':'down');
      // Show spot + perp breakdown when in OKX mode
      const bkdnEl = document.getElementById('equity-breakdown');
      if(bkdnEl && okxBalance){
        const spotEq = (okxBalance.usdt_eq || 0).toFixed(2);
        const perpUpl = (okxBalance.perp_upl || 0).toFixed(2);
        bkdnEl.textContent = `SPOT $${spotEq}  UPNL ${perpUpl>=0?'+':''}${perpUpl}`;
        bkdnEl.style.display = 'inline';
      } else if(bkdnEl) {
        bkdnEl.style.display = 'none';
      }
    }

    // 24h PnL — consume mon.pnl_24h (backend-computed)
    const pnl24 = mon.pnl_24h !== undefined ? mon.pnl_24h : 0;
    const startBank = mon.start_bankroll && mon.start_bankroll>0 ? mon.start_bankroll : 500;
    const pnlPct = okxBalance
      ? ((okxBalance.total_eq_usd - startBank) / startBank * 100).toFixed(2)
      : ((mon.bankroll - startBank) / startBank * 100).toFixed(2);
    const pnlEl = document.getElementById('topbar-pnl');
    if(pnlEl){
      const realPnl = okxBalance ? okxBalance.total_eq_usd - startBank : pnl24;
      pnlEl.textContent = (realPnl>=0?'+':'-')+'$'+Math.abs(realPnl).toFixed(0)+' ('+(realPnl>=0?'+':'')+pnlPct+'%)';
      pnlEl.className = 'topbar-stat-val '+(realPnl>=0?'up':'down');
    }

    // Win Rate — consume mon.win_rate
    const wrEl = document.getElementById('topbar-winrate');
    if(wrEl){
      const wr = mon.win_rate;
      const total = mon.total_trades || 0;
      wrEl.textContent = total>0 ? wr.toFixed(1)+'%' : '—';
      wrEl.className = 'topbar-stat-val ' + (wr>=50?'up':(wr<50&&total>0?'down':''));
    }

    // Total trades
    const tradesEl = document.getElementById('topbar-trades');
    if(tradesEl) tradesEl.textContent = mon.total_trades !== undefined ? mon.total_trades : ((s.closed_trades||[]).length + (mon.open_count||0));

    // Kelly Used — consume mon.full_kelly / mon.used_kelly + atr_pct
    const kellyFull = mon.full_kelly !== undefined ? mon.full_kelly : (snap.full_kelly !== undefined ? snap.full_kelly : null);
    const kellyUsed = mon.used_kelly !== undefined ? mon.used_kelly : (snap.used_kelly !== undefined ? snap.used_kelly : null);
    const kellyEl = document.getElementById('topbar-kelly');
    const kellyCard = document.getElementById('mon-kelly-panel');
    const kellySub = document.getElementById('mon-kelly-sub');
    if(kellyEl) kellyEl.textContent = kellyFull !== null ? kellyFull.toFixed(3) : '—';
    if(kellyCard) kellyCard.textContent = kellyFull !== null ? kellyFull.toFixed(3) : '—';
    if(kellySub) {
      const atr = mon.atr_pct !== undefined ? mon.atr_pct*100 : 0;
      const vp = mon.vol_penalty !== undefined ? mon.vol_penalty : 1.0;
      kellySub.innerHTML = 'used: '+ (kellyUsed!==null?kellyUsed.toFixed(3):'—')
        + '  <span style="opacity:.5">· atr '+atr.toFixed(1)+'% · pen '+vp.toFixed(2)+'</span>';
    }

    // ── 2. Metrics Row sub-labels ──
    const multSub = document.getElementById('mon-mult-sub');
    if(multSub) multSub.textContent = (mon.multiple||1).toFixed(2)+'x';
    const pnlSub = document.getElementById('mon-pnl-sub');
    if(pnlSub) pnlSub.textContent = (pnl24>=0?'+':'')+'$'+Math.abs(pnl24).toFixed(0);
    const posSub = document.getElementById('mon-pos-sub');
    if(posSub) {
      const perpN = (s.perp_positions||[]).length;
      const spotN = mon.open_count || 0;
      posSub.textContent = spotN+' spot'+(perpN?' · '+perpN+' perp':'');
    }
    // Builder card sub-label — pseudo commission
    const builderComm = mon.comm_pseudo !== undefined ? mon.comm_pseudo : 0;
    const builderCard = document.querySelector('.metrics-row .metric-card:nth-child(5) .metric-sub');
    if(builderCard) builderCard.textContent = 'Comm pseudo: $'+builderComm.toFixed(2);

    // ── 3. Sidebar badge ──
    const navPos = document.getElementById('nav-positions-count');
    if(navPos) navPos.textContent = mon.open_count || (s.positions||[]).length || 0;

    // ── 4. Risk Bar ──
    updateRiskBar(mon.risk_pct !== undefined ? mon.risk_pct : (s.risk_pct||0), mon.risk_limit || 8);

    // ── 5. Legacy monitor IDs (fallback for old JS) ──
    const bankLegacy = document.getElementById('mon-bank');
    if(bankLegacy) bankLegacy.textContent = '$'+(okxBalance ? okxBalance.total_eq_usd : (mon.bankroll||0)).toFixed(2);
    const multLegacy = document.getElementById('mon-mult');
    if(multLegacy) multLegacy.textContent = (mon.multiple||1).toFixed(2)+'x';
    const entryLegacy = document.getElementById('mon-entry');
    if(entryLegacy) entryLegacy.textContent = mon.entries||0;
    const wlLegacy = document.getElementById('mon-wl');
    if(wlLegacy) wlLegacy.textContent = (mon.wins||0)+' / '+(mon.losses||0);
    const stLegacy = document.getElementById('mon-state');
    if(stLegacy){
      stLegacy.textContent = mon.state||'—';
      const pill = document.getElementById('pill');
      if(pill){ pill.textContent = (mon.state==='IN_POSITION'?'● LIVE':(mon.state==='HALTED'?'● HALTED':'● '+mon.state))+' '; }
    }

    // ── 6. OKX MODE: module visibility & equity curve alignment ──
    if(okxBalance){
      // 6a. Hide simulator-only History Trades (duplicate with OKX Trade History below)
      document.querySelectorAll('#positions-card .panel-header').forEach(el=>{
        if((el.textContent||'').includes('History')){
          const wrap = el.parentElement;
          if(wrap) wrap.style.display = 'none';
        }
      });

      // 6b. Strategy Signals already in title; skip rename

      // 6c. Hide AI Decisions (OKX模式下无独立决策流)
      const decCard = document.getElementById('decision-log-card');
      if(decCard) decCard.style.display = 'none';

      // 6d. Equity curve: already handled in applyDeskState (OKX mode resets to real balance anchor)

      // 6e. Bankroll sub-label → OKX Real
      const bankCardSub = document.querySelector('.metrics-row .metric-card:first-child .metric-sub');
      if(bankCardSub && bankCardSub.textContent !== 'OKX Real') bankCardSub.textContent = 'OKX Real';
    } else {
      // Non-OKX mode: restore simulator modules
      document.querySelectorAll('#positions-card .panel-header').forEach(el=>{
        if((el.textContent||'').includes('History')){
          const wrap = el.parentElement;
          if(wrap) wrap.style.display = '';
        }
      });
      const decCard = document.getElementById('decision-log-card');
      if(decCard) decCard.style.display = '';
    }
  };
}

// ── Signal Feed filter handlers ──
(function setupFeedFilters(){
  const state = { search: '' };
  const search = document.getElementById('feed-search');
  if(search){
    search.addEventListener('input', e=>{ state.search = e.target.value.toLowerCase(); applyFeedFilter(); });
  }
  function applyFeedFilter(){
    const items = document.querySelectorAll('#feed-list .feed-item');
    items.forEach(item=>{
      const text = item.textContent.toLowerCase();
      const showSearch = !state.search || text.includes(state.search);
      item.classList.toggle('hidden', !showSearch);
    });
  }
})();

// ── Equity Curve time window switch ──
(function setupEquitySwitch(){
  const windows = { '3H': 30, '4H': 45, '1D': 1440, 'ALL': Infinity };
  const els = document.querySelectorAll('#equity-card .panel-action');
  els.forEach(el=>{
    const label = el.textContent.trim();
    if(!windows[label]) return;
    el.classList.toggle('active', label==='ALL');
    el.addEventListener('click', ()=>{
      els.forEach(e=>e.classList.remove('active'));
      el.classList.add('active');
      window.__equity_window = windows[label];
      drawEquity();
    });
  });
})();

// ── Decision / Swap badge wiring ──
const _origRenderDecisions = typeof renderDecisions !== 'undefined' ? renderDecisions : null;
if(typeof renderDecisions !== 'undefined'){
  renderDecisions = function(list){
    if(_origRenderDecisions) _origRenderDecisions(list);
    const badge = document.getElementById('decision-badge');
    if(badge){ badge.textContent = '('+(list||[]).length+')'; }
  };
}
const _origRenderSwaps = typeof renderSwaps !== 'undefined' ? renderSwaps : null;
if(typeof renderSwaps !== 'undefined'){
  renderSwaps = function(data, isOkx){
    if(_origRenderSwaps) _origRenderSwaps(data, isOkx);
    const pending = (data&&data.pending) ? data.pending.length : 0;
    const badge = document.getElementById('swap-badge');
    if(badge){ badge.textContent = '('+pending+')'; }
  };
}

console.log('[V1] Full data wiring loaded — monitor.{pnl_24h,win_rate,risk_pct,atr_pct} consumed');

// ═══════════════════════════════════════════════════
// PAGE NAVIGATION
// ═══════════════════════════════════════════════════
(function setupPageNav(){
  document.querySelectorAll('.nav-item[data-page]').forEach(item=>{
    item.addEventListener('click', ()=>{
      const page = item.dataset.page;
      document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
      item.classList.add('active');
      document.querySelectorAll('.page-section').forEach(s=>s.classList.remove('active'));
      const target = document.getElementById('page-'+page);
      if(target) target.classList.add('active');
      // Lazy-load page data on first visit
      if(page==='trades' && !window.__trades_loaded){ window.__trades_loaded=true; loadTradesPage(); }
      if(page==='positions' && !window.__positions_loaded){ window.__positions_loaded=true; refreshPositionsPage(); }
      if(page==='spot' && !window.__spot_loaded){ window.__spot_loaded=true; loadSpotPage(); }
      if(page==='perp' && !window.__perp_loaded){ window.__perp_loaded=true; loadPerpPage(); }
      if(page==='kelly' && !window.__kelly_loaded){ window.__kelly_loaded=true; loadKellyPage(); }
      if(page==='reports' && !window.__reports_loaded){ window.__reports_loaded=true; loadReportsPage(); }
      if(page==='leaderboard' && !window.__lb_loaded){ window.__lb_loaded=true; loadLeaderboardPage(); }
      if(page==='members') loadMembersPage();
    });
  });
})();

function navigateToPage(page){
  const item = document.querySelector('.nav-item[data-page="'+page+'"]');
  if(item) item.click();
  else console.warn('navigateToPage: no nav-item for', page);
}

// ═══════════════════════════════════════════════════
// TRADES PAGE — DB-driven with pagination
// ═══════════════════════════════════════════════════
let _tradesPage = 0;
const TRADES_PAGE_SIZE = 25;
async function loadTradesPage(offset){
  if(offset!==undefined) _tradesPage = offset;
  try{
    const r = await fetch('/api/trades?action=list&limit='+TRADES_PAGE_SIZE+'&offset='+(_tradesPage*TRADES_PAGE_SIZE));
    const json = await r.json();
    const rows = json.trades||[];
    const total = json.total||0;
    // Stats
    const wins = rows.filter(t=>t.win).length;
    const totalPnl = rows.reduce((s,t)=>s+(t.pnl_usd||0),0);
    const elTotal = document.getElementById('tr-stat-total');
    const elWinrate = document.getElementById('tr-stat-winrate');
    const elPnl = document.getElementById('tr-stat-pnl');
    const elAvg = document.getElementById('tr-stat-avg');
    if(elTotal) elTotal.textContent = total;
    if(elWinrate) elWinrate.textContent = total>0?(wins/total*100).toFixed(1)+'%':'—';
    if(elPnl){ elPnl.textContent=(totalPnl>=0?'+':'')+'$'+totalPnl.toFixed(2); elPnl.className='metric-value '+(totalPnl>=0?'up':'down'); }
    if(elAvg) elAvg.textContent = total>0?'$'+(totalPnl/total).toFixed(2):'—';
    const statEl = document.getElementById('trades-stat');
    if(statEl) statEl.textContent = total+' trades total';
    // Table
    const tbody = document.getElementById('trades-body');
    if(!tbody) return;
    if(!rows.length){ tbody.innerHTML='<tr><td colspan="11" class="empty-row">— no trades yet —</td></tr>'; }
    else {
      tbody.innerHTML = rows.map(t=>{
        const entryTs = t.entry_ts?new Date(t.entry_ts*1000).toLocaleString():'—';
        const exitTs = t.exit_ts?new Date(t.exit_ts*1000).toLocaleString():'—';
        const pnlClass = (t.pnl_usd||0)>=0?'up':'down';
        return `<tr>
          <td style="color:var(--text-dim)">${t.id||'—'}</td>
          <td><b>${t.ticker||'—'}</b></td>
          <td><span class="badge ${t.side==='EXIT'?'red':'green'}">${t.side||'—'}</span></td>
          <td>$${(t.entry_usd||0).toFixed(2)}</td>
          <td>$${(t.exit_usd||0).toFixed(2)}</td>
          <td class="${pnlClass}">${(t.pnl_usd>=0?'+':'')}$${Math.abs(t.pnl_usd||0).toFixed(2)}</td>
          <td>${(t.pnl_mult||0).toFixed(3)}x</td>
          <td><span class="badge ${t.win?'green':'red'}">${t.win?'WIN':'LOSS'}</span></td>
          <td><span class="tag ${t.platform==='okx'?'tag-okx':'tag-jup'}">${t.platform==='okx'?'OKX':'JUP'}</span></td>
          <td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text-dim)" title="${t.why||''}">${(t.why||'').slice(0,30)}</td>
          <td style="color:var(--text-dim);font-size:10px">${exitTs}</td>
        </tr>`;
      }).join('');
    }
    // Pager
    const pager = document.getElementById('trades-pager');
    if(pager){
      const totalPages = Math.ceil(total/TRADES_PAGE_SIZE);
      if(totalPages<=1){ pager.innerHTML=''; }
      else {
        let btns='';
        for(let i=0;i<totalPages;i++){
          const active = i===_tradesPage?'style="background:var(--blue);color:#fff"':'';
          btns+=`<button ${active} onclick="loadTradesPage(${i})" style="padding:2px 6px;font-size:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);cursor:pointer;border-radius:2px">${i+1}</button>`;
        }
        pager.innerHTML=`<div style="display:flex;gap:4px;align-items:center;padding:8px 16px"><span style="font-size:10px;color:var(--text-dim)">Page </span>${btns}<span style="font-size:10px;color:var(--text-dim);margin-left:8px">of ${totalPages}</span></div>`;
      }
    }
  }catch(e){ console.error('loadTradesPage error:',e); }
}
function confirmClearTrades(){
  if(!confirm('Clear all trade history from database? This cannot be undone.')) return;
  fetch('/api/trades?action=clear',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}).then(r=>r.json()).then(j=>{
    if(j.ok){ _tradesPage=0; window.__trades_loaded=false; loadTradesPage(); }
  });
}

// ═══════════════════════════════════════════════════
// POSITIONS PAGE — OKX spot + perp
// ═══════════════════════════════════════════════════
function refreshPositionsPage(){
  window.__positions_loaded = true;
  const s = window.__lastDeskState || {};
  // Spot holdings
  const holdings = s.okx_spot_holdings || [];
  const spotBody = document.getElementById('spot-pos-body');
  const spotBadge = document.getElementById('spot-pos-badge');
  if(spotBadge) spotBadge.textContent = holdings.length;
  if(spotBody){
    if(!holdings.length){ spotBody.innerHTML='<tr><td colspan="6" class="empty-row">— no spot holdings —</td></tr>'; }
    else {
      spotBody.innerHTML = holdings.map(h=>{
        const coin = h.ccy||h.coin||h.asset||'—';
        const total = parseFloat(h.spot_bal||h.total||0).toFixed(6);
        const avail = parseFloat(h.available_bal||h.available||0).toFixed(6);
        const locked = parseFloat(h.locked||0).toFixed(6);
        const usdVal = parseFloat(h.eq_usd||h.usd_value||h.quote_cnt||0).toFixed(2);
        const pnl = h.upl!==undefined?((h.upl>=0?'+':'')+'$'+Math.abs(h.upl).toFixed(2)):'—';
        const pnlPct = h.pnl_pct!==undefined?((h.pnl_pct>=0?'+':'')+h.pnl_pct.toFixed(2)+'%'):'—';
        return `<tr><td><b>${coin}</b></td><td>${total}</td><td>${avail}</td><td>${locked}</td><td class="up">$${usdVal}</td><td>${pnl}</td></tr>`;
      }).join('');
    }
  }
  // Perp positions
  const perpPositions = s.perp_positions || [];
  const perpBody = document.getElementById('perp-pos-body-page');
  const perpBadge = document.getElementById('perp-pos-badge-page');
  if(perpBadge) perpBadge.textContent = perpPositions.length;
  if(perpBody){
    if(!perpPositions.length){ perpBody.innerHTML='<tr><td colspan="7" class="empty-row">— no perp positions —</td></tr>'; }
    else {
      perpBody.innerHTML = perpPositions.map(p=>{
        const pnlPct = p.pnl_pct !== undefined ? (p.pnl_pct>=0?'+':'')+p.pnl_pct.toFixed(2)+'%' : '—';
        const pnlClass = (p.pnl_usd||0)>=0?'up':'down';
        return `<tr>
          <td><b>${p.ticker||'—'}</b></td>
          <td><span class="badge ${p.side==='LONG'?'green':'red'}">${p.side||'—'}</span></td>
          <td>$${(p.entry_usd||0).toFixed(2)}</td>
          <td>$${(p.notional||0).toFixed(2)}</td>
          <td>${p.leverage||'—'}x</td>
          <td class="${pnlClass}">${(p.pnl_usd>=0?'+':'')}$${Math.abs(p.pnl_usd||0).toFixed(2)}</td>
          <td class="${pnlClass}">${pnlPct}</td>
        </tr>`;
      }).join('');
    }
  }
}

// ═══════════════════════════════════════════════════
// OKX SPOT PAGE
// ═══════════════════════════════════════════════════
function loadSpotPage(){
  window.__spot_loaded = true;
  const s = window.__lastDeskState || {};
  const bal = s.okx_real_balance || {};
  const eqEl = document.getElementById('okx-spot-equity');
  const availEl = document.getElementById('okx-spot-avail');
  const holdEl = document.getElementById('okx-spot-holdings');
  if(eqEl) eqEl.textContent = '$'+(bal.total_eq_usd||0).toFixed(2);
  if(availEl) availEl.textContent = '$'+(bal.usdt_avail||0).toFixed(2);
  if(holdEl) holdEl.textContent = '$'+(bal.usdt_eq||0).toFixed(2);
  const holdings = s.okx_spot_holdings || [];
  const body = document.getElementById('okx-spot-holdings-body');
  if(body){
    if(!holdings.length){ body.innerHTML='<tr><td colspan="5" class="empty-row">— no holdings —</td></tr>'; }
    else {
      body.innerHTML = holdings.map(h=>{
        const coin = h.ccy||h.coin||h.asset||'—';
        const total = parseFloat(h.spot_bal||h.total||0).toFixed(6);
        const avail = parseFloat(h.available_bal||h.available||0).toFixed(6);
        const locked = parseFloat(h.locked||0).toFixed(6);
        const usdVal = parseFloat(h.eq_usd||h.usd_value||h.quote_cnt||0).toFixed(2);
        return `<tr><td><b>${coin}</b></td><td>${total}</td><td>${avail}</td><td>${locked}</td><td class="up">$${usdVal}</td></tr>`;
      }).join('');
    }
  }
  // Recent spot trades
  const trades = s.okx_trades_history || [];
  const tBody = document.getElementById('okx-spot-trades-body');
  if(tBody){
    if(!trades.length){ tBody.innerHTML='<tr><td colspan="7" class="empty-row">No recent trades</td></tr>'; }
    else {
      tBody.innerHTML = trades.slice(0,20).map(tr=>{
        const ts = tr.fill_time ? new Date(parseInt(tr.fill_time)).toLocaleString() : '—';
        const side = (tr.side||'').toUpperCase();
        const price = parseFloat(tr.price_avg||tr.avg_px||0).toFixed(6);
        const qty = parseFloat(tr.orig_qty||tr.size||0).toFixed(6);
        const filled = parseFloat(tr.filled_qty||tr.cum_quote||0).toFixed(6);
        const fee = parseFloat(tr.realized_fee||tr.fee||0).toFixed(6);
        return `<tr><td style="color:var(--text-dim);font-size:10px">${ts}</td><td><b>${tr.inst_id||tr.symbol||'—'}</b></td><td><span class="badge ${side==='SELL'?'red':'green'}">${side}</span></td><td>$${price}</td><td>${qty}</td><td>${filled}</td><td style="color:var(--text-dim)">${fee}</td></tr>`;
      }).join('');
    }
  }
}

// ═══════════════════════════════════════════════════
// PERP ENGINE PAGE
// ═══════════════════════════════════════════════════
function loadPerpPage(){
  window.__perp_loaded = true;
  const s = window.__lastDeskState || {};
  const bal = s.okx_real_balance || {};
  const perpPositions = s.perp_positions || [];
  const eqEl = document.getElementById('okx-perp-equity');
  const uplEl = document.getElementById('okx-perp-upl');
  const countEl = document.getElementById('okx-perp-positions-count');
  if(eqEl) eqEl.textContent = '$'+(bal.total_eq_usd||0).toFixed(2);
  if(uplEl){
    const upl = bal.perp_upl||0;
    uplEl.textContent = (upl>=0?'+':'')+'$'+Math.abs(upl).toFixed(2);
    uplEl.className = 'metric-value '+(upl>=0?'up':'down');
  }
  if(countEl) countEl.textContent = perpPositions.length;
  // Update auto-close rules display from state
  const cfg = s.perp_config || {};
  const tpEl = document.getElementById('perp-rule-tp');
  const slEl = document.getElementById('perp-rule-sl');
  const holdEl = document.getElementById('perp-rule-hold');
  if(tpEl) tpEl.textContent = (cfg.tp_pct||3)+'%';
  if(slEl) slEl.textContent = (cfg.sl_pct||1.5)+'%';
  if(holdEl) holdEl.textContent = ((cfg.max_hold_sec||1800)/60).toFixed(0)+' min';
  const body = document.getElementById('okx-perp-body');
  if(body){
    if(!perpPositions.length){ body.innerHTML='<tr><td colspan="9" class="empty-row">— no perp positions —</td></tr>'; }
    else {
      body.innerHTML = perpPositions.map(p=>{
        const pnlPct = p.pnl_pct!==undefined?(p.pnl_pct>=0?'+':'')+p.pnl_pct.toFixed(2)+'%':'—';
        const pnlClass = (p.pnl_usd||0)>=0?'up':'down';
        return `<tr>
          <td><b>${p.ticker||'—'}</b></td>
          <td><span class="badge ${p.side==='LONG'?'green':'red'}">${p.side||'—'}</span></td>
          <td>$${(p.notional||p.entry_usd||0).toFixed(2)}</td>
          <td>$${(p.entry_usd||0).toFixed(2)}</td>
          <td>$${(p.mark_price||p.current_price||0).toFixed(6)}</td>
          <td>${p.leverage||'—'}x</td>
          <td class="${pnlClass}">${(p.pnl_usd>=0?'+':'')}$${Math.abs(p.pnl_usd||0).toFixed(2)}</td>
          <td class="${pnlClass}">${pnlPct}</td>
          <td>$${(p.margin||0).toFixed(2)}</td>
        </tr>`;
      }).join('');
    }
  }
}

// ═══════════════════════════════════════════════════
// KELLY PAGE — compute from DB
// ═══════════════════════════════════════════════════
async function loadKellyPage(){
  window.__kelly_loaded = true;
  try{
    const r = await fetch('/api/trades?action=stats');
    const stats = await r.json();
    const total = stats.total_trades||0;
    const wins = stats.wins||0;
    const losses = stats.losses||0;
    const winRate = total>0 ? wins/total : 0;
    const avgWin = stats.avg_win||0;
    const avgLoss = Math.abs(stats.avg_loss||0);
    const payoutRatio = avgLoss>0 ? avgWin/avgLoss : 0;
    const b = winRate; const q = 1-winRate;
    const kellyFull = payoutRatio>0 ? b - q/payoutRatio : 0;
    const kellyHalf = kellyFull/2;
    const kellyQuarter = kellyFull/4;
    const eq = window.__lastDeskState?.okx_real_balance;
    const bankroll = eq ? eq.total_eq_usd : 500;
    // Update Kelly display
    const setEl = (id,val,color)=>{const el=document.getElementById(id);if(el){el.textContent=val;el.style.color=color||'';}};
    setEl('kelly-winrate-display',(winRate*100).toFixed(1)+'%');
    setEl('kelly-payout-display',payoutRatio.toFixed(2)+'x');
    setEl('kelly-full-display',kellyFull.toFixed(3));
    setEl('kelly-half-display',kellyHalf.toFixed(3));
    setEl('kelly-quarter-display',kellyQuarter.toFixed(3));
    setEl('kelly-current-display','$'+bankroll.toFixed(2));
    const util = kellyHalf>0 ? Math.min(100, (bankroll*0.02/kellyHalf/bankroll*100)).toFixed(0) : '—';
    setEl('kelly-util-display',util+'%');
    // Trade stats
    const setStat = (id,val)=>{const el=document.getElementById(id);if(el)el.textContent=val;};
    setStat('kelly-stat-total',total);
    setStat('kelly-stat-wins',wins);
    setStat('kelly-stat-losses',losses);
    const grossProfit = stats.gross_profit||0;
    const grossLoss = Math.abs(stats.gross_loss||0);
    const pf = grossLoss>0 ? (grossProfit/grossLoss).toFixed(2) : '—';
    setStat('kelly-stat-pf',pf);
    setStat('kelly-stat-maxwin',stats.max_consec_wins||'—');
    setStat('kelly-stat-maxloss',stats.max_consec_losses||'—');
  }catch(e){ console.error('loadKellyPage error:',e); }
}

// ═══════════════════════════════════════════════════
// REPORTS PAGE
// ═══════════════════════════════════════════════════
let _rptPage = 0;
const RPT_PAGE_SIZE = 30;
async function loadReportsPage(offset){
  if(offset!==undefined) _rptPage = offset;
  const platformFilter = document.getElementById('rpt-filter-platform')?.value || 'all';
  try{
    const r = await fetch('/api/trades?action=list&limit='+RPT_PAGE_SIZE+'&offset='+(_rptPage*RPT_PAGE_SIZE));
    const json = await r.json();
    let rows = json.trades||[];
    if(platformFilter!=='all') rows = rows.filter(t=>t.platform===platformFilter);
    const total = rows.length;
    // Summary stats
    const wins = rows.filter(t=>t.win).length;
    const totalPnl = rows.reduce((s,t)=>s+(t.pnl_usd||0),0);
    const bestTrade = rows.reduce((m,t)=>t.pnl_usd>m?t.pnl_usd:m,-Infinity);
    const worstTrade = rows.reduce((m,t)=>t.pnl_usd<m?t.pnl_usd:m,Infinity);
    const grossProfit = rows.filter(t=>t.pnl_usd>0).reduce((s,t)=>s+t.pnl_usd,0);
    const grossLoss = Math.abs(rows.filter(t=>t.pnl_usd<0).reduce((s,t)=>s+t.pnl_usd,0));
    const setEl = (id,val)=>{const el=document.getElementById(id);if(el)el.textContent=val;};
    setEl('rpt-stat-total',total);
    setEl('rpt-stat-winrate',total>0?(wins/total*100).toFixed(1)+'%':'—');
    setEl('rpt-stat-pnl',(totalPnl>=0?'+':'')+'$'+totalPnl.toFixed(2));
    setEl('rpt-stat-avg',total>0?'$'+(totalPnl/total).toFixed(2):'—');
    setEl('rpt-stat-best',total>0?'$'+bestTrade.toFixed(2):'—');
    setEl('rpt-stat-worst',total>0?'$'+worstTrade.toFixed(2):'—');
    setEl('rpt-stat-pf',grossLoss>0?(grossProfit/grossLoss).toFixed(2):'—');
    // Trade log table
    const tbody = document.getElementById('rpt-trades-body');
    if(tbody){
      if(!rows.length){ tbody.innerHTML='<tr><td colspan="10" class="empty-row">— no trades —</td></tr>'; }
      else {
        tbody.innerHTML = rows.map(t=>{
          const ts = t.exit_ts?new Date(t.exit_ts*1000).toLocaleString():'—';
          const pnlClass = (t.pnl_usd||0)>=0?'up':'down';
          return `<tr>
            <td style="color:var(--text-dim)">${t.id||'—'}</td>
            <td style="color:var(--text-dim);font-size:10px">${ts}</td>
            <td><b>${t.ticker||'—'}</b></td>
            <td><span class="badge ${t.side==='EXIT'?'red':'green'}">${t.side||'—'}</span></td>
            <td>$${(t.entry_usd||0).toFixed(2)}</td>
            <td>$${(t.exit_usd||0).toFixed(2)}</td>
            <td class="${pnlClass}">${(t.pnl_usd>=0?'+':'')}$${Math.abs(t.pnl_usd||0).toFixed(2)}</td>
            <td>${(t.pnl_mult||0).toFixed(3)}x</td>
            <td style="color:var(--text-dim);font-size:10px">${t.platform||'JUP'}</td>
            <td style="max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text-dim)" title="${t.why||''}">${(t.why||'').slice(0,25)}</td>
          </tr>`;
        }).join('');
      }
    }
    // Pager
    const pager = document.getElementById('rpt-pager');
    if(pager){
      const totalPages = Math.ceil(json.total/RPT_PAGE_SIZE);
      if(totalPages<=1){ pager.innerHTML=''; }
      else {
        let btns='';
        for(let i=0;i<Math.min(totalPages,10);i++){
          const active=i===_rptPage?'style="background:var(--blue);color:#fff"':'';
          btns+=`<button ${active} onclick="loadReportsPage(${i})" style="padding:2px 6px;font-size:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);cursor:pointer;border-radius:2px">${i+1}</button>`;
        }
        pager.innerHTML=`<div style="display:flex;gap:4px;align-items:center;padding:8px 16px"><span style="font-size:10px;color:var(--text-dim)">Page </span>${btns}</div>`;
      }
    }
    // Equity curve on report page
    const eqCanvas = document.getElementById('equity-report');
    if(eqCanvas && window.__lastEquityCurve){
      drawEquityReport(eqCanvas, window.__lastEquityCurve);
    }
  }catch(e){ console.error('loadReportsPage error:',e); }
}

function drawEquityReport(canvas, equityData){
  if(!canvas||!equityData||!equityData.length) return;
  const dpr = window.devicePixelRatio||1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width*dpr; canvas.height = 220*dpr;
  canvas.style.width = rect.width+'px'; canvas.style.height = '220px';
  const ctx = canvas.getContext('2d'); ctx.scale(dpr,dpr);
  const W=rect.width, H=220, padL=10, padR=10, padT=10, padB=30;
  const chartW=W-padL-padR, chartH=H-padT-padB;
  const vals=equityData.map(p=>p[1]);
  const mn=Math.min(...vals)*0.99, mx=Math.max(...vals)*1.01;
  const n=vals.length;
  ctx.clearRect(0,0,W,H);
  ctx.strokeStyle='rgba(0,212,255,.2)';ctx.lineWidth=.5;
  for(let i=0;i<=4;i++){const y=padT+chartH*i/4;ctx.beginPath();ctx.moveTo(padL,y);ctx.lineTo(W-padR,y);ctx.stroke();}
  ctx.beginPath();
  equityData.forEach((p,i)=>{
    const x=padL+i/(n-1||1)*chartW, y=padT+(mx-p[1])/(mx-mn||1)*chartH;
    i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);
  });
  ctx.strokeStyle='#00d4ff';ctx.lineWidth=1.5;ctx.stroke();
  const lastY=padT+(mx-equityData[n-1][1])/(mx-mn)*chartH;
  ctx.beginPath();ctx.arc(W-padR,lastY,3,0,Math.PI*2);ctx.fillStyle='#00d4ff';ctx.fill();
  ctx.font='9px monospace';ctx.fillStyle='var(--text-dim)';
  ctx.fillText('$'+mx.toFixed(0),2,padT+4);ctx.fillText('$'+mn.toFixed(0),2,H-padB+2);
}

// ═══════════════════════════════════════════════════
// PERP AUTO-CLOSE SETTINGS MODAL
// ═══════════════════════════════════════════════════
function openPerpSettings(){
  const modal = document.getElementById('perp-settings-modal');
  if(!modal) return;
  modal.style.display='flex';
  // Load current settings from DB
  fetch('/api/settings').then(r=>r.json()).then(j=>{
    const s = j.settings||{};
    const setVal = (id,key)=>{const el=document.getElementById(id);if(el)el.value=s[key]||el.dataset.default||el.value;};
    setVal('perp-tp','tp_pct'); setVal('perp-sl','sl_pct');
    setVal('perp-max-hold','max_hold_sec'); setVal('perp-max-size','max_position_usd');
    setVal('perp-reopen','allow_reopen');
  });
}
function closePerpSettings(){
  const modal = document.getElementById('perp-settings-modal');
  if(modal) modal.style.display='none';
}
function savePerpSettings(){
  const vals = {
    tp_pct: document.getElementById('perp-tp')?.value || '3',
    sl_pct: document.getElementById('perp-sl')?.value || '1.5',
    max_hold_sec: document.getElementById('perp-max-hold')?.value || '1800',
    max_position_usd: document.getElementById('perp-max-size')?.value || '1000',
    allow_reopen: document.getElementById('perp-reopen')?.checked ? 'true' : 'false',
  };
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({method:'save',settings:vals})})
    .then(r=>r.json()).then(j=>{
      if(j.ok){ alert('Settings saved!'); closePerpSettings(); }
      else { alert('Failed to save: '+(j.error||'unknown error')); }
    });
}

// ═══════════════════════════════════════════════════
// API KEY SETTINGS MODAL (integrated into existing settings)
// ═══════════════════════════════════════════════════
function showApiSettings(){
  const section = document.getElementById('api-keys-section');
  if(section){ section.style.display = section.style.display==='none'?'block':'none'; }
}
async function loadApiKeys(){
  const statusEl = document.getElementById('api-key-status');
  const keyEl = document.getElementById('api-key-input');
  const secretEl = document.getElementById('api-secret-input');
  const passEl = document.getElementById('api-pass-input');
  const section = document.getElementById('api-keys-section');
  try{
    const r = await fetch('/api/settings').then(res=>res.json());
    if(r.ok && r.settings){
      const s = r.settings;
      const hasCfg = !!(s.okx_api_key && s.okx_api_secret && s.okx_passphrase);
      if(statusEl){
        statusEl.textContent = hasCfg ? 'CONFIGURED' : 'NOT SET';
        statusEl.style.color = hasCfg ? 'var(--green)' : 'var(--red)';
      }
      if(section) section.style.display = 'block';
      if(keyEl) keyEl.value = hasCfg ? (s.okx_api_key||'').slice(0,6)+'...'+(s.okx_api_key||'').slice(-4) : '—';
      if(secretEl) secretEl.value = hasCfg ? '••••••••' : '—';
      if(passEl) passEl.value = hasCfg ? '••••••••' : '—';
    }
  }catch(e){
    if(statusEl){ statusEl.textContent = 'ERROR'; statusEl.style.color = 'var(--red)'; }
  }
}

// ═══════════════════════════════════════════════════
// AUTH / MEMBERSHIP SYSTEM
// ═══════════════════════════════════════════════════
let _authToken = localStorage.getItem('session_token') || '';
let _currentUser = null;

async function checkAuth(){
  try{
    const r = await fetch('/api/auth/me').then(res=>res.json());
    if(r.ok && r.auth){
      _currentUser = r.auth;
      if(_authToken) localStorage.setItem('session_token', _authToken);
      updateAuthUI();
    } else {
      _currentUser = null;
      _authToken = '';
      localStorage.removeItem('session_token');
      updateAuthUI();
    }
  }catch(e){ updateAuthUI(); }
}

function updateAuthUI(){
  const el = document.getElementById('user-status');
  if(!el) return;
  if(_currentUser){
    el.innerHTML = `<span style="color:var(--green)">● ${_currentUser.username}</span> <span style="font-size:9px;color:var(--text-dim);margin-left:4px">[${_currentUser.tier}]</span> <a href="#" onclick="doLogout();return false;" style="font-size:9px;color:var(--text-dim);margin-left:4px">logout</a>`;
  } else {
    el.innerHTML = `<button class="btn-secondary" onclick="showAuthModal()" style="font-size:10px;padding:3px 8px">Login / Register</button>`;
  }
}

function showAuthModal(){
  const m = document.getElementById('auth-modal');
  if(m){ m.style.display='flex'; _authMsg(''); }
}
function closeAuthModal(){
  const m = document.getElementById('auth-modal');
  if(m) m.style.display='none';
  _authMsg('');
}
function switchAuthTab(tab){
  document.getElementById('tab-login').style.color = tab==='login' ? 'var(--green)' : 'var(--text-dim)';
  document.getElementById('tab-login').style.borderBottomColor = tab==='login' ? 'var(--green)' : 'var(--border)';
  document.getElementById('tab-register').style.color = tab==='register' ? 'var(--green)' : 'var(--text-dim)';
  document.getElementById('tab-register').style.borderBottomColor = tab==='register' ? 'var(--green)' : 'var(--border)';
  document.getElementById('auth-login-form').style.display = tab==='login' ? 'block' : 'none';
  document.getElementById('auth-register-form').style.display = tab==='register' ? 'block' : 'none';
  document.getElementById('auth-modal-title').textContent = tab==='login' ? 'Login' : 'Register';
  _authMsg('');
}
function _authMsg(msg){
  const el = document.getElementById('auth-msg');
  if(el) el.textContent = msg;
}
async function doLogin(){
  const u = document.getElementById('auth-login-user').value.trim();
  const p = document.getElementById('auth-login-pass').value;
  if(!u||!p){ _authMsg('Please fill in all fields'); return; }
  _authMsg('Connecting...');
  try{
    const resp = await fetch('/api/auth/login',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({username:u,password:p}),
      credentials:'same-origin'
    });
    if(!resp.ok){
      const text = await resp.text().catch(()=>'{},');
      _authMsg('HTTP '+resp.status+': '+text);
      return;
    }
    let r;
    try { r = await resp.json(); }
    catch(e){ _authMsg('Invalid server response'); return; }
    if(r.ok){
      _authToken = r.token;
      _currentUser = {username:r.username, role:r.role, tier:r.tier};
      localStorage.setItem('session_token', _authToken);
      updateAuthUI();
      closeAuthModal();
      _authMsg('Logged in as '+r.username);
      if(_currentUser&&_currentUser.role==='admin') loadMembersPage();
      else if(_currentUser) loadMembersPage();
      if(document.getElementById('page-leaderboard')?.classList.contains('active')) loadLeaderboardPage();
    } else { _authMsg((r.error||'Login failed')+' (check username/password)'); }
  }catch(e){ _authMsg('Network error: '+e.message); console.error('Login error:', e); }
}
async function doRegister(){
  const u = document.getElementById('auth-reg-user').value.trim();
  const p = document.getElementById('auth-reg-pass').value;
  const e = document.getElementById('auth-reg-email').value.trim();
  if(u.length<3){ _authMsg('Username must be ≥3 characters'); return; }
  if(p.length<4){ _authMsg('Password must be ≥4 characters'); return; }
  try{
    const r = await fetch('/api/auth/register',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u,password:p,email:e})}).then(res=>res.json());
    if(r.ok){
      _authMsg('Registered! Now login.');
      switchAuthTab('login');
      document.getElementById('auth-login-user').value = u;
    } else { _authMsg(r.error||'Registration failed'); }
  }catch(e){ _authMsg('Error: '+e.message); }
}
async function doLogout(){
  if(_authToken){
    try{ await fetch('/api/auth/logout',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); }catch(e){}
  }
  _authToken = ''; _currentUser = null;
  localStorage.removeItem('session_token');
  updateAuthUI();
}

// ═══════════════════════════════════════════════════
// LEADERBOARD PAGE
// ═══════════════════════════════════════════════════
async function loadLeaderboardPage(){
  const loginMsg = document.getElementById('leaderboard-login-msg');
  const content = document.getElementById('leaderboard-content');
  const loading = document.getElementById('leaderboard-loading');
  if(!loginMsg||!content||!loading) return;
  loading.style.display = 'block'; content.style.display = 'none'; loginMsg.style.display = 'none';
  // Check auth
  if(!_authToken){
    loading.style.display = 'none'; loginMsg.style.display = 'block'; return;
  }
  try{
    const r = await fetch('/api/leaderboard?limit=50').then(res=>res.json());
    loading.style.display = 'none';
    if(!r.ok){ loginMsg.style.display = 'block'; return; }
    content.style.display = 'block';
    const entries = r.entries || [];
    // Stats cards
    const cardsEl = document.getElementById('lb-stats-cards');
    if(cardsEl){
      const top = entries[0];
      const avgPnl = entries.length ? (entries.reduce((s,e)=>s+e.total_pnl,0)/entries.length).toFixed(2) : '0.00';
      const totalTrades = entries.reduce((s,e)=>s+e.trade_count,0);
      cardsEl.innerHTML = `
        <div class="panel" style="text-align:center;padding:16px"><div style="font-size:9px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em;margin-bottom:4px">Top Trader</div><div style="font-family:var(--mono);font-size:16px;color:var(--green)">${top?top.username:'—'}</div><div style="font-size:9px;color:var(--text-dim)">${top?('$'+top.total_pnl.toFixed(2)):'—'}</div></div>
        <div class="panel" style="text-align:center;padding:16px"><div style="font-size:9px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em;margin-bottom:4px">Average PnL</div><div style="font-family:var(--mono);font-size:16px;color:${parseFloat(avgPnl)>=0?'var(--green)':'var(--red)'}">$${avgPnl}</div><div style="font-size:9px;color:var(--text-dim)">across ${entries.length} traders</div></div>
        <div class="panel" style="text-align:center;padding:16px"><div style="font-size:9px;text-transform:uppercase;color:var(--text-dim);letter-spacing:.1em;margin-bottom:4px">Total Trades</div><div style="font-family:var(--mono);font-size:16px;color:var(--blue)">${totalTrades}</div><div style="font-size:9px;color:var(--text-dim)">combined volume</div></div>
      `;
    }
    // Table
    const tbody = document.getElementById('lb-tbody');
    if(tbody){
      tbody.innerHTML = entries.map(e=>{
        const pnlClass = e.total_pnl>=0?'var(--green)':'var(--red)';
        const rankDot = e.rank===1?'gold':e.rank===2?'silver':e.rank===3?'#cd7f32':'var(--text-dim)';
        return `<tr>
          <td><span style="display:inline-block;width:20px;height:20px;line-height:20px;text-align:center;border-radius:50%;background:${rankDot};color:#000;font-size:10px;font-weight:bold">${e.rank}</span></td>
          <td>${e.username}</td><td><span class="tier-badge ${e.tier}">${e.tier}</span></td>
          <td style="color:${pnlClass};font-family:var(--mono)">$${e.total_pnl.toFixed(2)}</td>
          <td>${e.trade_count}</td><td>${(e.win_rate*100).toFixed(1)}%</td>
        </tr>`;
      }).join('') || '<tr><td colspan="6" class="empty-row">No entries yet</td></tr>';
    }
  }catch(e){ loading.style.display = 'none'; loginMsg.style.display = 'block'; }
}

async function syncLeaderboard(){
  if(!_authToken){ showAuthModal(); return; }
  const btn = document.getElementById('sync-lb-btn');
  if(btn) btn.disabled = true;
  try{
    const r = await fetch('/api/leaderboard/sync',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}).then(res=>res.json());
    if(r.ok){
      if(btn) btn.textContent = 'Synced!';
      loadLeaderboardPage();
    } else { alert(r.error||'Sync failed'); }
  }catch(e){ alert('Error: '+e.message); }
  if(btn) btn.disabled = false;
}

// ═══════════════════════════════════════════════════
// MEMBERS / ADMIN PAGE
// ═══════════════════════════════════════════════════
async function loadMembersPage(){
  const loginMsg = document.getElementById('members-login-msg');
  const adminPanel = document.getElementById('members-admin-panel');
  const userPanel = document.getElementById('members-user-panel');
  const subtitle = document.getElementById('members-subtitle');
  if(!loginMsg) return;
  if(!_authToken){ loginMsg.style.display='block'; if(adminPanel) adminPanel.style.display='none'; if(userPanel) userPanel.style.display='none'; if(subtitle) subtitle.textContent='Log in to continue'; return; }
  if(! _currentUser){ loginMsg.style.display='block'; if(adminPanel) adminPanel.style.display='none'; if(userPanel) userPanel.style.display='none'; return; }
  loginMsg.style.display='none';
  // Show user center for all logged-in users
  if(userPanel) userPanel.style.display='block';
  if(subtitle) subtitle.textContent = `${_currentUser.username} [${_currentUser.tier}]`;
  // Show admin panel only for admins
  if(adminPanel) adminPanel.style.display = _currentUser.role === 'admin' ? 'block' : 'none';
  if(_currentUser.role === 'admin'){
    try{
      const r = await fetch('/api/admin/members').then(res=>res.json());
      const tbody = document.getElementById('members-tbody');
      if(!tbody) return;
      if(!r.ok||!r.members){ tbody.innerHTML='<tr><td colspan="8" class="empty-row">Failed to load</td></tr>'; return; }
      tbody.innerHTML = r.members.map(m=>{
        const regDate = m.created_at ? new Date(m.created_at*1000).toLocaleDateString() : '—';
        const loginDate = m.last_login ? new Date(m.last_login*1000).toLocaleDateString() : 'Never';
        return `<tr>
          <td>${m.id}</td><td>${m.username}</td><td>${m.email||'—'}</td>
          <td><span class="tier-badge ${m.role}">${m.role}</span></td>
          <td><span class="tier-badge ${m.tier}">${m.tier}</span></td>
          <td style="font-size:10px;color:var(--text-dim)">${regDate}</td>
          <td style="font-size:10px;color:var(--text-dim)">${loginDate}</td>
          <td><button class="btn-secondary" onclick="openMemberEdit(${m.id},'${m.role}','${m.tier}')" style="font-size:10px;padding:2px 6px">Edit</button></td>
        </tr>`;
      }).join('') || '<tr><td colspan="8" class="empty-row">No members</td></tr>';
    }catch(e){
      const tbody = document.getElementById('members-tbody');
      if(tbody) tbody.innerHTML='<tr><td colspan="8" class="empty-row">Error loading members</td></tr>';
    }
  }
  // Load user settings for the current user
  await loadUserSettings();
}

// ── User Center: Trade Settings & API Keys ─────────────────────────────────
let _userSettingsCache = null;

async function loadUserSettings(){
  if(!_authToken || !_currentUser) return;
  try{
    const r = await fetch('/api/user/settings').then(res=>res.json());
    if(!r.ok || !r.settings){
      _userSettingsCache = null;
      return;
    }
    _userSettingsCache = r.settings;
    // Populate form fields
    const setRisk = _userSettingsCache.risk_preference || 'balanced';
    const setMode = _userSettingsCache.trade_mode || 'signal_only';
    document.querySelectorAll('#members-user-panel .risk-option-member').forEach(el=>{
      el.classList.toggle('active', el.dataset.value === setRisk);
    });
    document.querySelectorAll('#members-user-panel .mode-option-member').forEach(el=>{
      el.classList.toggle('active', el.dataset.value === setMode);
    });
    const startTime = _userSettingsCache.run_start_time || '00:00';
    const endTime = _userSettingsCache.run_end_time || '23:59';
    const timeUnlimited = !startTime && !endTime;
    const startInput = document.getElementById('us-start-time');
    const endInput = document.getElementById('us-end-time');
    const unlimitedCheck = document.getElementById('us-time-unlimited');
    if(startInput) startInput.value = startTime || '00:00';
    if(endInput) endInput.value = endTime || '23:59';
    if(unlimitedCheck) unlimitedCheck.checked = timeUnlimited;
    if(timeUnlimited){ startInput.disabled=true; endInput.disabled=true; }
    const maxPos = document.getElementById('us-max-position');
    if(maxPos) maxPos.value = _userSettingsCache.max_position_usd || 100;
    const tickers = document.getElementById('us-allowed-tickers');
    if(tickers) tickers.value = _userSettingsCache.allowed_tickers || '';
    _memberSelectedTickers = (_userSettingsCache.allowed_tickers || '').split(',').filter(t=>t.trim()).map(t=>t.trim().toUpperCase());
    renderTickerTags();
    const tp = document.getElementById('us-take-profit');
    const sl = document.getElementById('us-stop-loss');
    if(tp) tp.value = _userSettingsCache.take_profit_pct || 3.0;
    if(sl) sl.value = _userSettingsCache.stop_loss_pct || 1.5;
    // API keys
    const apiKeyEl = document.getElementById('us-api-key');
    const apiSecretEl = document.getElementById('us-api-secret');
    const apiPassEl = document.getElementById('us-api-passphrase');
    if(apiKeyEl) apiKeyEl.value = _userSettingsCache.api_key || '';
    if(apiSecretEl) apiSecretEl.value = _userSettingsCache.api_secret || '';
    if(apiPassEl) apiPassEl.value = _userSettingsCache.api_passphrase || '';
    updateApiStatusDisplay();
  }catch(e){ console.error('loadUserSettings error:', e); }
}

function switchMemberTab(tab){
  const tradePanel = document.getElementById('panel-trade-settings');
  const apiPanel = document.getElementById('panel-api-keys');
  const tradeTab = document.getElementById('tab-trade-settings');
  const apiTab = document.getElementById('tab-api-keys');
  if(!tradePanel||!apiPanel||!tradeTab||!apiTab) return;
  if(tab==='trade-settings'){
    tradePanel.style.display='block'; apiPanel.style.display='none';
    tradeTab.style.color='var(--green)'; tradeTab.style.borderBottomColor='var(--green)';
    apiTab.style.color='var(--text-dim)'; apiTab.style.borderBottomColor='transparent';
  }else{
    tradePanel.style.display='none'; apiPanel.style.display='block';
    tradeTab.style.color='var(--text-dim)'; tradeTab.style.borderBottomColor='transparent';
    apiTab.style.color='var(--green)'; apiTab.style.borderBottomColor='var(--green)';
  }
}

function toggleTimeUnlimited(){
  const checked = document.getElementById('us-time-unlimited').checked;
  const startInput = document.getElementById('us-start-time');
  const endInput = document.getElementById('us-end-time');
  if(startInput) startInput.disabled = checked;
  if(endInput) endInput.disabled = checked;
}

function setRiskFromCenter(pref){
  document.querySelectorAll('#members-user-panel .risk-option-member').forEach(el=>{
    el.classList.toggle('active', el.dataset.value === pref);
  });
}

function setModeFromCenter(mode){
  document.querySelectorAll('#members-user-panel .mode-option-member').forEach(el=>{
    el.classList.toggle('active', el.dataset.value === mode);
  });
}

async function saveTradeSettings(){
  if(!_currentUser) return;
  const riskOption = document.querySelector('#members-user-panel .risk-option-member.active');
  const modeOption = document.querySelector('#members-user-panel .mode-option-member.active');
  const startInput = document.getElementById('us-start-time');
  const endInput = document.getElementById('us-end-time');
  const unlimitedCheck = document.getElementById('us-time-unlimited');
  const maxPosInput = document.getElementById('us-max-position');
  const tickersInput = document.getElementById('us-allowed-tickers');
  const tpInput = document.getElementById('us-take-profit');
  const slInput = document.getElementById('us-stop-loss');
  const settings = {
    risk_preference: riskOption ? riskOption.dataset.value : 'balanced',
    trade_mode: modeOption ? modeOption.dataset.value : 'signal_only',
    run_start_time: unlimitedCheck?.checked ? null : (startInput?.value || null),
    run_end_time: unlimitedCheck?.checked ? null : (endInput?.value || null),
    max_position_usd: parseFloat(maxPosInput?.value) || 100,
    allowed_tickers: (tickersInput?.value || '').trim(),
    take_profit_pct: parseFloat(tpInput?.value) || 3.0,
    stop_loss_pct: parseFloat(slInput?.value) || 1.5,
  };
  try{
    const r = await fetch('/api/user/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(settings)}).then(res=>res.json());
    if(r.ok){
      _userSettingsCache = {..._userSettingsCache,...settings};
      showToast(lang==='zh'?'交易设置已保存':'Trade settings saved');
    }else{
      alert(r.error||'Save failed');
    }
  }catch(e){ alert('Error: '+e.message); }
}

async function saveApiKeys(){
  if(!_currentUser) return;
  const key = document.getElementById('us-api-key')?.value || '';
  const secret = document.getElementById('us-api-secret')?.value || '';
  const passphrase = document.getElementById('us-api-passphrase')?.value || '';
  const settings = { api_key: key, api_secret: secret, api_passphrase: passphrase };
  try{
    const r = await fetch('/api/user/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(settings)}).then(res=>res.json());
    if(r.ok){
      _userSettingsCache = {..._userSettingsCache,...settings};
      updateApiStatusDisplay();
      showToast(lang==='zh'?'API密钥已保存':'API keys saved');
    }else{
      alert(r.error||'Save failed');
    }
  }catch(e){ alert('Error: '+e.message); }
}

function updateApiStatusDisplay(){
  const el = document.getElementById('us-api-status');
  if(!el) return;
  const hasKey = _userSettingsCache && _userSettingsCache.api_key;
  if(hasKey){
    el.innerHTML = `<span style="color:var(--green)">● <span data-i18n="configured">Configured</span></span>`;
  }else{
    el.innerHTML = `<span style="color:var(--text-dim)">— <span data-i18n="not_configured">Not configured</span></span>`;
  }
}

// ── Ticker Multi-Select Picker ─────────────────────────────────────────────
const MEMBER_TICKERS = [
  'BTC','ETH','SOL','XRP','DOGE','ADA','AVAX','DOT','LINK','MATIC',
  'LTC','BCH','UNI','ATOM','ETC','FIL','NEAR','APT','ARB','OP',
  'SUI','SEI','PEPE','WIF','ONDO','RNDR','FET','TAO','STRK','ALT',
  'TRU','PENDLE','TIA','INJ','SAND','AXS','IMX','GRT','RUNE','FTM',
  'ONE','MANA','ENJ','CHZ','GALA','ILV','PYR','APE','BLUR','GMX',
];
let _memberSelectedTickers = [];
let _tickerPickerOpen = false;

function toggleTickerPicker(){
  const dd = document.getElementById('us-ticker-dropdown');
  if(!dd) return;
  _tickerPickerOpen = !_tickerPickerOpen;
  dd.style.display = _tickerPickerOpen ? 'block' : 'none';
  if(_tickerPickerOpen) renderTickerList('');
}

function renderTickerList(filter=''){
  const list = document.getElementById('ticker-list');
  if(!list) return;
  const f = filter.toUpperCase();
  const filtered = MEMBER_TICKERS.filter(t=>t.includes(f));
  list.innerHTML = filtered.map(t=>{
    const sel = _memberSelectedTickers.includes(t);
    return `<div class="ticker-item${sel?' selected':''}" onclick="toggleTicker('${t}')">
      <input type="checkbox" ${sel?'checked':''} onclick="event.stopPropagation();toggleTicker('${t}')">
      <span>${t}</span>
    </div>`;
  }).join('') || '<div style="padding:12px;font-size:11px;color:var(--text-dim);text-align:center">No results</div>';
}

function filterTickers(val){ renderTickerList(val); }

function toggleTicker(ticker){
  const idx = _memberSelectedTickers.indexOf(ticker);
  if(idx>=0) _memberSelectedTickers.splice(idx,1);
  else _memberSelectedTickers.push(ticker);
  renderTickerList(document.getElementById('ticker-search')?.value||'');
  renderTickerTags();
}

function selectAllTickers(){
  _memberSelectedTickers = [...MEMBER_TICKERS];
  renderTickerList('');
  renderTickerTags();
}

function clearAllTickers(){
  _memberSelectedTickers = [];
  renderTickerList('');
  renderTickerTags();
}

function renderTickerTags(){
  const container = document.getElementById('us-ticker-input');
  const placeholder = document.getElementById('ticker-placeholder');
  if(!container) return;
  if(placeholder) placeholder.style.display = 'none';
  // Remove old tags
  container.querySelectorAll('.ticker-tag').forEach(t=>t.remove());
  if(_memberSelectedTickers.length === 0){
    if(placeholder) { placeholder.style.display=''; placeholder.textContent=lang==='zh'?'点击选择币种...':'Click to select coins...'; }
    return;
  }
  _memberSelectedTickers.forEach(t=>{
    const tag = document.createElement('span');
    tag.className = 'ticker-tag';
    tag.innerHTML = `${t}<button onclick="event.stopPropagation();removeTicker('${t}')">×</button>`;
    container.appendChild(tag);
  });
}

function removeTicker(t){
  const idx = _memberSelectedTickers.indexOf(t);
  if(idx>=0) _memberSelectedTickers.splice(idx,1);
  renderTickerList(document.getElementById('ticker-search')?.value||'');
  renderTickerTags();
}

// Close picker when clicking outside
document.addEventListener('click', function(e){
  const container = document.getElementById('ticker-picker-container');
  const dd = document.getElementById('us-ticker-dropdown');
  if(container && dd && _tickerPickerOpen && !container.contains(e.target)){
    _tickerPickerOpen = false;
    dd.style.display = 'none';
  }
});

let _editingMemberId = null;
function openMemberEdit(id, role, tier){
  _editingMemberId = id;
  document.getElementById('edit-member-id').value = id;
  document.getElementById('edit-member-role').value = role;
  document.getElementById('edit-member-tier').value = tier;
  const m = document.getElementById('member-edit-modal');
  if(m) m.style.display = 'flex';
}
function closeMemberEdit(){
  const m = document.getElementById('member-edit-modal');
  if(m) m.style.display = 'none';
  _editingMemberId = null;
}
async function saveMemberEdit(){
  if(!_editingMemberId) return;
  const role = document.getElementById('edit-member-role').value;
  const tier = document.getElementById('edit-member-tier').value;
  try{
    const r = await fetch('/api/admin/member/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:_editingMemberId,role,tier})}).then(res=>res.json());
    if(r.ok){ closeMemberEdit(); loadMembersPage(); }
    else alert(r.error||'Failed');
  }catch(e){ alert('Error: '+e.message); }
}
async function deleteMember(){
  if(!_editingMemberId) return;
  if(!confirm('Delete this member? This cannot be undone.')) return;
  try{
    const r = await fetch('/api/admin/member/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:_editingMemberId})}).then(res=>res.json());
    if(r.ok){ closeMemberEdit(); loadMembersPage(); }
    else alert(r.error||'Failed');
  }catch(e){ alert('Error: '+e.message); }
}

async function resetAllLeaderboard(){
  if(!confirm('Reset leaderboard? This will clear all entries.')) return;
  try{
    const r = await fetch('/api/admin/leaderboard/reset',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}).then(res=>res.json());
    if(r.ok){ alert('Leaderboard cleared. Members will need to re-sync.'); loadMembersPage(); }
    else alert(r.error||'Failed');
  }catch(e){ alert('Error: '+e.message); }
}

async function grantAllFreeToPremium(){
  if(!confirm('Upgrade all free tier members to premium?')) return;
  try{
    const r = await fetch('/api/admin/members').then(res=>res.json());
    if(!r.ok||!r.members) return;
    for(const m of r.members){
      if(m.tier==='free'){
        await fetch('/api/admin/member/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:m.id,tier:'premium'})}).catch(()=>{});
      }
    }
    loadMembersPage();
  }catch(e){}
}

// ═══════════════════════════════════════════════════
// INIT: check auth on load
// ═══════════════════════════════════════════════════
checkAuth();

</script>
</body>
</html>"""



# ──────────────────────────────────────────────────────────────────────
#  HTTP Handler
# ──────────────────────────────────────────────────────────────────────
class LiveHandler(BaseHTTPRequestHandler):
    """Serve dashboard + API for live trading."""

    def log_message(self, fmt, *args):
        return  # suppress access logs

    def _send_json(self, code, obj):
        data = json.dumps(obj, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json_with_cookie(self, code, obj, cookie_name, cookie_value):
        data = json.dumps(obj, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Set-Cookie", f"{cookie_name}={cookie_value}; Path=/; Max-Age=86400; SameSite=Lax")
        self.end_headers()
        self.wfile.write(data)

    def _get_cookie_token(self) -> str:
        val = self.headers.get("Cookie", "")
        if not val:
            return ""
        for part in val.split(";"):
            part = part.strip()
            if part.startswith("session_token="):
                return part.split("=", 1)[1]
        return ""

    def _set_cookie(self, name: str, value: str) -> None:
        self.send_header(f"Set-Cookie", f"{name}={value}; Path=/; Max-Age=86400; SameSite=Lax")

    def _clear_cookie(self, name: str) -> None:
        self.send_header(f"Set-Cookie", f"{name}=; Path=/; Max-Age=0")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(INDEX_HTML.encode())))
            self.end_headers()
            self.wfile.write(INDEX_HTML.encode("utf-8"))
        elif u.path == "/events":
            self._sse_handler(u)
        elif u.path == "/api/status":
            self._send_json(200, _server.status() if _server else {"error": "no server"})
        elif u.path == "/api/swaps":
            self._send_json(200, _server.swaps_snapshot() if _server else {"pending": [], "submitted": [], "failed": []})
        elif u.path == "/api/desk-state":
            if _runner is not None and hasattr(_runner, "_latest_state") and _runner._latest_state is not None:
                self._send_json(200, _runner._latest_state)
            elif _server is not None and _server.get_latest_state() is not None:
                self._send_json(200, _server.get_latest_state())
            else:
                self._send_json(404, {"error": "no desk state yet"})
        elif u.path.startswith("/api/trades"):
            qs = parse_qs(u.query)
            limit = min(int(qs.get("limit", ["100"])[0]), 500)
            offset = int(qs.get("offset", ["0"])[0])
            action = qs.get("action", ["list"])[0]
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            db: TradeDB = _runner.db
            if action == "stats":
                self._send_json(200, db.get_stats())
            elif action == "clear":
                token = self._get_cookie_token()
                info = _runner.db.validate_session(token) if token else None
                if not info:
                    self._send_json(401, {"error": "unauthorized"})
                    return
                cnt = db.clear_all()
                if _runner is not None:
                    _runner.closed_trades = []
                self._send_json(200, {"ok": True, "removed": cnt})
            else:
                rows = db.get_recent(limit=limit, offset=offset)
                total = db.get_total_count()
                self._send_json(200, {"trades": rows, "total": total, "limit": limit, "offset": offset})
        elif u.path == "/api/settings":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            db: TradeDB = _runner.db
            self._send_json(200, {"ok": True, "settings": db.get_all_settings()})
        elif u.path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
        elif u.path == "/api/auth/me":
            token = self._get_cookie_token()
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            info = _runner.db.validate_session(token) if token else None
            self._send_json(200, {"ok": True, "auth": info})
        elif u.path == "/api/user/settings":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            tok = self._get_cookie_token()
            info = _runner.db.validate_session(tok) if tok else None
            if not info:
                self._send_json(401, {"ok": False, "error": "unauthorized"})
                return
            settings = _runner.db.get_user_settings(info["user_id"])
            if not settings:
                _runner.db.create_user_settings_for_user(info["user_id"])
                settings = _runner.db.get_user_settings(info["user_id"])
            self._send_json(200, {"ok": True, "settings": settings or {}})
        elif u.path.startswith("/api/leaderboard"):
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            qs = parse_qs(u.query)
            limit = min(int(qs.get("limit", ["20"])[0]), 100)
            self._send_json(200, {"ok": True, "entries": _runner.db.get_leaderboard(limit=limit)})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        global _runner
        u = urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(body)
        except Exception:
            data = {}

        if u.path == "/api/swap/submit":
            key = data.get("key", "")
            signed = data.get("signed_tx", "") or None
            if not _server:
                self._send_json(500, {"ok": False, "error": "no server"})
                return
            result = _server.trader.submit_swap(key, signed_tx_b64=signed)
            self._send_json(200 if result.get("ok") else 400, result)
        elif u.path == "/api/manual/signal":
            if not _server:
                self._send_json(500, {"ok": False, "error": "no server"})
                return
            result = _server.manual_signal(
                ticker=data.get("ticker", "BONK"),
                side=data.get("side", "ENTRY"),
                usd=float(data.get("usd", 50)),
                note=f"manual signal via dashboard",
            )
            self._send_json(200, result)
        elif u.path == "/api/price/sol":
            if not _server:
                self._send_json(500, {"ok": False, "error": "no server"})
                return
            price = _server.refresh_sol_price()
            self._send_json(200, {"sol_usd": round(price, 2)})
        elif u.path == "/api/wallet/connect":
            pubkey = data.get("pubkey", "").strip()
            if len(pubkey) < 32:
                self._send_json(400, {"ok": False, "error": "invalid pubkey"})
                return
            if _server:
                _server.user_wallet = pubkey
                _server.trader.user_wallet = pubkey
            self._send_json(200, {"ok": True, "pubkey": pubkey[:8] + "..." + pubkey[-6:]})
        elif u.path == "/api/settings":
            method = data.get("method", "get")
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            db: TradeDB = _runner.db
            if method == "get":
                self._send_json(200, {"ok": True, "settings": db.get_all_settings()})
            elif method == "save":
                settings: dict = data.get("settings", {})
                for k, v in settings.items():
                    db.set_setting(str(k), str(v))
                # If perp settings changed, reload them in the runner
                if _runner is not None and hasattr(_runner, "_reload_perp_settings"):
                    _runner._reload_perp_settings()
                self._send_json(200, {"ok": True, "saved": list(settings.keys())})
            else:
                self._send_json(400, {"ok": False, "error": "unknown method"})
            # Handle direct trade_mode update (from frontend setMode)
            if "trade_mode" in data and data.get("trade_mode"):
                mode = data["trade_mode"]
                if _runner and hasattr(_runner, "update_trade_mode"):
                    _runner.update_trade_mode(mode)
                db.set_setting("trade_mode", mode)
                print(f"[LIVE_SERVER] trade_mode updated → {mode}")
                self._send_json(200, {"ok": True, "trade_mode": mode})
                return
        elif u.path == "/api/okx/spot":
            # Return cached OKX spot holdings for the Spot page
            if _runner is None:
                self._send_json(503, {"error": "no runner"})
                return
            holdings = getattr(_runner, "spot_holdings", []) or []
            self._send_json(200, {"ok": True, "holdings": holdings})
        elif u.path == "/api/okx/connect":
            # OKX API modal: persist credentials → rebuild executor → refresh DeskRunner
            try:
                new_key = data.get("api_key") or os.environ.get("OKX_API_KEY")
                new_secret = data.get("api_secret") or os.environ.get("OKX_API_SECRET")
                new_pass = data.get("passphrase") or os.environ.get("OKX_PASSPHRASE")
                if not (new_key and new_secret and new_pass):
                    self._send_json(400, {"ok": False, "error": "OKX credentials incomplete — all 3 fields required"})
                    return
                # Persist to os.environ (live) + .env (persistent across restarts)
                os.environ["OKX_API_KEY"] = new_key
                os.environ["OKX_API_SECRET"] = new_secret
                os.environ["OKX_PASSPHRASE"] = new_pass
                # Write to .env
                try:
                    env_path = Path(__file__).parent / ".env"
                    env_lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
                    seen = {"OKX_API_KEY": False, "OKX_API_SECRET": False, "OKX_PASSPHRASE": False}
                    new_lines = []
                    for ln in env_lines:
                        for k in seen:
                            if ln.startswith(k + "="):
                                new_lines.append(f"{k}={os.environ[k]}")
                                seen[k] = True
                                break
                        else:
                            new_lines.append(ln)
                    for k, v in seen.items():
                        if not v:
                            new_lines.append(f"{k}={os.environ[k]}")
                    env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
                    print(f"[LIVE_SERVER] .env updated — OKX credentials persisted")
                    # Also persist to DB for settings UI
                    if _runner and hasattr(_runner, "db"):
                        _runner.db.set_setting("okx_api_key", new_key)
                        _runner.db.set_setting("okx_api_secret", new_secret)
                        _runner.db.set_setting("okx_passphrase", new_pass)
                        print(f"[LIVE_SERVER] OKX API keys stored in DB for settings UI")
                except Exception as e:
                    print(f"[LIVE_SERVER] WARNING — .env write failed: {e}")

                # ── HOT-SWAP: rebuild executor + refresh DeskRunner ──
                result = {"ok": True, "msg": "OKX credentials saved", "auth": "ready"}
                if _server is not None:
                    try:
                        ex_new = OKXExecutor.from_env()
                        result["demo"] = ex_new.demo
                        result["auth"] = "ready" if ex_new._auth_ready else "keys-not-set"

                        # Swap executor on ServerState + LiveTrader
                        if ex_new._auth_ready:
                            print(f"[LIVE_SERVER] HOT-SWAP: rebuilding OKXExecutor (auth=ready, demo={ex_new.demo})")
                            _server.executor = ex_new
                            _server.executor_type = type(ex_new).__name__
                            _server.trader.executor = ex_new
                            _server.trader.ticker_inst_map.clear()  # force rebuild
                            result["msg"] = "OKX executor rebuilt with new credentials"

                            # Swap DeskRunner token source: pull fresh OKX tokens
                            if _runner is not None:
                                from rh_okx_data import OKXDataSource
                                _runner._okx_live_data = True
                                ds = OKXDataSource(ex_new)
                                fresh_tokens = ds.fetch_tokens(top_n=30)
                                if fresh_tokens:
                                    _runner.raw_tokens = fresh_tokens
                                    _runner.source = f"OKX-{len(fresh_tokens)}"
                                    _runner.events = sorted(
                                        [(tok.t_min, tok) for tok in fresh_tokens], key=lambda x: x[0]
                                    )
                                    # Update ticker map
                                    new_tmap = ds.get_ticker_map()
                                    _server.trader.ticker_inst_map.update(new_tmap)
                                    _server.ticker_mint_map.update(new_tmap)
                                    # Also refresh scatter display
                                    _runner.scatter_tokens = list(fresh_tokens)
                                    seen = set()
                                    unique_scatter = []
                                    for t in fresh_tokens:
                                        if t.ticker not in seen:
                                            unique_scatter.append(t); seen.add(t.ticker)
                                    _runner.scatter_tokens = unique_scatter
                                    result["tokens"] = len(fresh_tokens)
                                    result["source"] = _runner.source
                                    print(f"[LIVE_SERVER] HOT-SWAP: DeskRunner reloaded {len(fresh_tokens)} tokens from OKX")
                                else:
                                    result["msg"] = "OKX connected but no tokens returned"
                        else:
                            result["ok"] = False
                            result["auth"] = "keys-not-set"
                            result["msg"] = "OKX API keys rejected by OKX (auth check failed)"
                    except Exception as e:
                        print(f"[LIVE_SERVER] HOT-SWAP ERROR: {e}")
                        result["ok"] = False
                        result["error"] = str(e)
                self._send_json(200, result)
            except Exception as e:
                self._send_json(500, {"ok": False, "error": str(e)})
        elif u.path == "/api/auth/register":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            uname = data.get("username", "").strip()
            pw = data.get("password", "")
            email = data.get("email", "")
            if len(uname) < 3 or len(pw) < 4:
                self._send_json(400, {"ok": False, "error": "username ≥3 chars, password ≥4 chars"})
                return
            result = _runner.db.register(uname, pw, email)
            self._send_json(200, result)
        elif u.path == "/api/auth/login":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            uname = data.get("username", "").strip()
            pw = data.get("password", "")
            result = _runner.db.login(uname, pw)
            if result.get("ok"):
                self._send_json_with_cookie(200, result, "session_token", result["token"])
            else:
                self._send_json(200, result)
        elif u.path == "/api/auth/logout":
            token = self._get_cookie_token()
            if _runner and hasattr(_runner, "db") and token:
                _runner.db.logout(token)
            data = json.dumps({"ok": True}, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(data)))
            if token:
                self.send_header("Set-Cookie", "session_token=; Path=/; Max-Age=0")
            self.end_headers()
            self.wfile.write(data)
        elif u.path == "/api/admin/members":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            tok = self._get_cookie_token()
            info = _runner.db.validate_session(tok) if tok else None
            if not info or info["role"] != "admin":
                self._send_json(403, {"ok": False, "error": "forbidden"})
                return
            self._send_json(200, {"ok": True, "members": _runner.db.get_all_members()})
        elif u.path == "/api/admin/member/update":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            tok = self._get_cookie_token()
            info = _runner.db.validate_session(tok) if tok else None
            if not info or info["role"] != "admin":
                self._send_json(403, {"ok": False, "error": "forbidden"})
                return
            result = _runner.db.update_member(data.get("id"), role=data.get("role"), tier=data.get("tier"))
            self._send_json(200, result)
        elif u.path == "/api/admin/member/delete":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            tok = self._get_cookie_token()
            info = _runner.db.validate_session(tok) if tok else None
            if not info or info["role"] != "admin":
                self._send_json(403, {"ok": False, "error": "forbidden"})
                return
            result = _runner.db.delete_member(data.get("id"))
            self._send_json(200, result)
        elif u.path == "/api/leaderboard/sync":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            tok = self._get_cookie_token()
            auth_info = _runner.db.validate_session(tok) if tok else None
            if not auth_info:
                self._send_json(401, {"ok": False, "error": "unauthorized"})
                return
            stats = _runner.db.get_stats()
            _runner.db.update_leaderboard(auth_info["user_id"], stats["total_pnl"], stats["total"], stats["profit_rate"])
            self._send_json(200, {"ok": True, "stats": stats})
        elif u.path == "/api/user/settings":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            tok = self._get_cookie_token()
            info = _runner.db.validate_session(tok) if tok else None
            if not info:
                self._send_json(401, {"ok": False, "error": "unauthorized"})
                return
            # Filter allowed keys
            allowed_keys = ["api_key","api_secret","api_passphrase","risk_preference","trade_mode",
                            "run_start_time","run_end_time","max_position_usd","allowed_tickers",
                            "take_profit_pct","stop_loss_pct"]
            clean = {k: data.get(k, "") for k in allowed_keys}
            result = _runner.db.upsert_user_settings(info["user_id"], clean)
            self._send_json(200, result)
        elif u.path == "/api/admin/leaderboard/reset":
            if _runner is None or not hasattr(_runner, "db"):
                self._send_json(503, {"error": "no runner db"})
                return
            tok = self._get_cookie_token()
            info = _runner.db.validate_session(tok) if tok else None
            if not info or info["role"] != "admin":
                self._send_json(403, {"ok": False, "error": "forbidden"})
                return
            with _runner.db._lock:
                _runner.db._get_conn().execute("DELETE FROM leaderboard")
                _runner.db._get_conn().commit()
            self._send_json(200, {"ok": True})
        else:
            self._send_json(404, {"error": "not found"})

    def _sse_handler(self, u):
        """SSE stream — dual mode: DeskRunner push (full state) or manual poll (swaps+status)."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        print(f"[SSE] handler called — _live_queue={_live_queue is not None}, _server={_server is not None}", flush=True)

        if not _server:
            self.wfile.write(b'data: {"error":"no server running"}\n\n')
            return

        saved = os.environ.get("RH_USER_WALLET", "")
        if saved and not _server.user_wallet:
            _server.user_wallet = saved
            _server.trader.user_wallet = saved

        try:
            if _live_queue is not None:
                # ── DESKRUNNER PUSH MODE ──
                print("[SSE] DeskRunner push mode — reading from _live_queue")
                # Drain old stale data so we start live-consuming
                drained = 0
                while True:
                    try:
                        _live_queue.get_nowait()
                        drained += 1
                    except queue.Empty:
                        break
                if drained:
                    print(f"[SSE] drained {drained} stale states")
                while True:
                    try:
                        state = _live_queue.get(timeout=5)
                        payload = json.dumps(state, default=str)
                        self.wfile.write(f"data: {payload}\n\n".encode())
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b"event: ping\ndata: \n\n")
                        self.wfile.flush()
            else:
                # ── MANUAL POLL MODE ──
                print("[SSE] Manual poll mode — no DeskRunner")
                initial = _server.trader.status()
                init_payload = json.dumps({
                    "swaps": _server.swaps_snapshot(),
                    "status": initial,
                    "sol_usd": initial.get("sol_usd", 0),
                }, default=str)
                self.wfile.write(f"data: {init_payload}\n\n".encode())
                self.wfile.flush()
                while True:
                    try:
                        if not _server._sol_price_fetched:
                            _server.refresh_sol_price()
                    except Exception:
                        pass
                    swaps_data = _server.swaps_snapshot()
                    status_data = _server.trader.status()
                    payload = json.dumps({
                        "swaps": swaps_data, "status": status_data,
                        "sol_usd": status_data.get("sol_usd", 0),
                    }, default=str)
                    self.wfile.write(f"data: {payload}\n\n".encode())
                    self.wfile.flush()
                    time.sleep(2)
        except (BrokenPipeError, ConnectionResetError):
            pass


# ──────────────────────────────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────────────────────────────
def main(host: str = "127.0.0.1", port: int = 8765, csv_path: str | None = None,
         stake_usd: float = 50.0, slippage_bps: int = 100, tick_ms: int = 400,
         seed: int | None = 7, max_positions: int = 3, no_live_runner: bool = False,
         refresh_min: float | None = None):
    global _server

    # Load env (utf-8-sig strips BOM from Windows .env files)
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    # Ensure proxy env for external API calls (DexScreener, Jupiter)
    os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
    os.environ.setdefault("HTTP_PROXY", "http://127.0.0.1:7897")
    os.environ.setdefault("HTTPS_PROXY", "http://127.0.0.1:7897")

    fee_wallet = os.environ.get("RH_FEE_WALLET", "")
    user_wallet = os.environ.get("RH_USER_WALLET", "")
    fee_bps = int(os.environ.get("RH_FEE_BPS", "25"))

    # Load ticker_mint_map
    mint_map: dict[str, str] = {}
    mm_path = Path(__file__).parent / "ticker_mint_map.json"
    if mm_path.exists():
        try:
            mint_map = json.loads(mm_path.read_text())
        except Exception:
            pass
    # Also try building from CSV
    csv_file = csv_path or next(Path(__file__).parent.glob("reports/dex_trending_real.csv"), None)
    if csv_file:
        try:
            from rh_live_trader import build_ticker_mint_map_from_csv
            csv_map = build_ticker_mint_map_from_csv(str(csv_file))
            mint_map.update(csv_map)
        except Exception:
            pass

    print(f"[LIVE_SERVER] Fee wallet: {fee_wallet[:8]}...{fee_wallet[-6:] if len(fee_wallet)>6 else ''}")
    print(f"[LIVE_SERVER] User wallet: {user_wallet[:8] if user_wallet else '(not set)'}...")
    print(f"[LIVE_SERVER] Ticker map: {len(mint_map)} tokens")
    print(f"[LIVE_SERVER] CSV: {csv_file}")

    # Executor: RH_EXECUTOR=okx or jupiter (default: jupiter)
    executor_mode = os.environ.get("RH_EXECUTOR", "jupiter").lower()
    okx_live_data = False
    if executor_mode == "okx" and OKXExecutor is not None:
        ex = OKXExecutor.from_env()
        auth_str = "ready" if ex._auth_ready else "keys-not-set"
        print(f"[LIVE_SERVER] Executor: OKX (auth={auth_str}, demo={ex.demo})")
        bc = ex.ai_builder_code or "(not set - no commission tracking)"
        print(f"[LIVE_SERVER] Builder Code: {bc}")
        # Auto-enable OKX live data: pull real spot tickers from OKX public API
        if auth_str == "ready":
            okx_live_data = True
            print(f"[LIVE_SERVER] -> okx_live_data=True (skip CSV, pull from OKX)")
    else:
        ex = JupiterExecutor.from_env()
        print("[LIVE_SERVER] Executor: Jupiter (Solana)")
    if okx_live_data:
        csv_file = None  # DeskRunner OKXDataSource handles tokens

    _server = ServerState(
        executor=ex,
        user_wallet=user_wallet,
        ticker_mint_map=mint_map,
        sol_usd=float(os.environ.get("RH_SOL_USD", "200")),
        slippage_bps=slippage_bps,
        csv_path=str(csv_file) if csv_file else None,
        stake_usd=stake_usd,
    )

    # Save wallet to env for child processes
    if user_wallet:
        os.environ["RH_USER_WALLET"] = user_wallet

    runner = None
    if not no_live_runner:
        try:
            q = start_live_queue(maxsize=2000)
            from rh_desk_runner import DeskRunner
            runner = DeskRunner(_server, tick_ms=tick_ms,
                                max_positions=max_positions, seed=seed,
                                queue=q,
                                okx_live_data=okx_live_data,
                                refresh_interval_min=refresh_min if refresh_min is not None else (5.0 if okx_live_data else None))
            runner.start()
            src_tag = "OKX" if okx_live_data else (("CSV" if csv_file else "scenario"))
            print(f"[LIVE_SERVER] DeskRunner STARTED tick_ms={tick_ms} max_positions={max_positions} source={src_tag}")
            # Expose runner to module-level so /api/okx/connect can hot-swap
            global _runner
            _runner = runner
            _server.runner = runner
        except Exception as e:
            print(f"[LIVE_SERVER] WARNING — DeskRunner failed: {e}")
            runner = None
    else:
        print("[LIVE_SERVER] --no-live-runner: manual signal mode only")

    srv = ThreadingHTTPServer((host, port), LiveHandler)
    print(f"[LIVE_SERVER] Dashboard: http://{host}:{port}")
    print(f"[LIVE_SERVER] API:       http://{host}:{port}/api/status")
    print(f"[LIVE_SERVER] Ctrl+C to stop")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[LIVE_SERVER] stopping...")
        if runner:
            runner.stop()
        srv.shutdown()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="RH Trencher Live Trading Dashboard")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--csv", default=None)
    ap.add_argument("--stake", type=float, default=50.0)
    ap.add_argument("--slippage", type=int, default=100)
    ap.add_argument("--tick-ms", type=int, default=400,
                    help="DeskRunner tick interval ms (default 400, faster=more signals)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--max-positions", type=int, default=3)
    ap.add_argument("--no-live-runner", action="store_true",
                    help="Disable auto DeskRunner (manual signals only)")
    ap.add_argument("--refresh-min", type=float, default=None,
                    help="Auto-refresh tokens from DexScreener every N minutes (default: no refresh, use static CSV)")
    args = ap.parse_args()
    main(args.host, args.port, args.csv, args.stake, args.slippage,
         tick_ms=args.tick_ms, seed=args.seed,
         max_positions=args.max_positions, no_live_runner=args.no_live_runner,
         refresh_min=args.refresh_min)
