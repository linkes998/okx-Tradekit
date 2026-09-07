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
try:
    from rh_okx_executor import OKXExecutor
except ImportError:
    OKXExecutor = None
try:
    from rh_okx_executor import OKXExecutor
except ImportError:
    OKXExecutor = None  # noqa: N816

# ── Globals ──────────────────────────────────────────────────────
_server: "ServerState | None" = None

# ── Live-mode push pipeline ──────────────────────────────────────
_live_queue: queue.Queue[dict] | None = None


def push_state(state: dict) -> None:
    if _live_queue is None:
        return
    try:
        _live_queue.put_nowait(state)
    except queue.Full:
        pass


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
        return s

    def swaps_snapshot(self) -> dict:
        """JSON-serializable view of pending + submitted + failed swaps."""
        out: dict[str, list] = {"pending": [], "submitted": [], "failed": []}
        for key, ps in self.trader.pending_swaps.items():
            out["pending"].append({
                "key": key,
                "ticker": ps.ticker,
                "side": ps.side,
                "amount_usd": ps.amount_usd,
                "fee_usd": ps.fee_amount_usd,
                "note": ps.note,
                "tx_b64": ps.bundle.transaction_base64,
                "bundle": {"transaction_base64": ps.bundle.transaction_base64},
                "created_at": ps.created_at,
                "quote": {
                    "in_amount": ps.quote.in_amount,
                    "out_amount": ps.quote.out_amount,
                    "price_impact_pct": ps.quote.price_impact_pct,
                    "fee_bps": ps.quote.fee_bps,
                    "fee_amount": ps.quote.fee_amount,
                },
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
<title>RH Trencher · Live Trade</title>
<style>
:root{--bg:#0b0f0c;--fg:#cde8d0;--hi:#3dff8a;--warn:#ff5b5b;--gold:#ffd93d;--blue:#8ab4ff;--dim:#7ab080;--line:#2a4a32;--card:#0d120e}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:12px/1.5 ui-monospace,Menlo,Consolas,monospace}
header{padding:10px 18px;border-bottom:1px solid var(--line);display:flex;gap:14px;align-items:center;flex-wrap:wrap}
header h1{margin:0;font-size:14px;color:var(--hi);letter-spacing:.5px}
header label{display:flex;align-items:center;gap:6px;color:var(--dim);font-size:11px}
header input[type=text]{background:#151a16;border:1px solid var(--line);color:var(--fg);padding:4px 8px;width:180px;font-family:inherit;border-radius:3px}
header input[type=number]{background:#151a16;border:1px solid var(--line);color:var(--fg);padding:4px 6px;width:64px;font-family:inherit;border-radius:3px}
header button{background:var(--hi);color:var(--bg);border:0;padding:5px 14px;font-weight:bold;cursor:pointer;font-family:inherit;border-radius:3px;font-size:11px}
header button.secondary{background:var(--line);color:var(--fg)}
header button.danger{background:#ff5b5b;color:#fff}
header button:disabled{opacity:.4;cursor:not-allowed}
header .status-pill{padding:3px 10px;border-radius:20px;border:1px solid var(--line);color:var(--dim);font-size:11px}
header .status-pill.live{color:var(--hi);border-color:var(--hi)}
header .status-pill.done{color:var(--gold);border-color:var(--gold)}
.wallet-dot{width:8px;height:8px;border-radius:50%;background:var(--warn);display:inline-block;margin-right:4px}
.wallet-dot.connected{background:var(--hi);box-shadow:0 0 6px var(--hi)}
main{display:grid;grid-template-columns:1fr 1fr;gap:14px;padding:14px}
.card{border:1px solid var(--line);border-radius:6px;padding:10px;background:var(--card)}
.card h2{margin:0 0 8px;font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:1px;font-weight:600}
canvas{display:block;width:100%;height:auto}
#equity-card{grid-column:1/3}
#monitor-card{grid-column:1/3}
#swaps-card{grid-column:1/3;max-height:320px;overflow:auto}
#positions-card,#history-card{max-height:240px;overflow:auto}
#status-table,#pos-table,#hist-table,#swap-table{width:100%;border-collapse:collapse;font-size:11px}
#status-table td{padding:3px 6px}
#status-table td:first-child{color:var(--dim);width:45%}
#status-table td:last-child{text-align:right;color:var(--hi)}
#status-table tr.warn td:last-child{color:var(--warn)}
#status-table tr.gold td:last-child{color:var(--gold)}
#pos-table th,#hist-table th,#swap-table th{text-align:left;padding:4px 6px;background:var(--bg);color:var(--dim);font-weight:600;font-size:10px;text-transform:uppercase;letter-spacing:.5px;position:sticky;top:0;z-index:1}
#pos-table td,#hist-table td,#swap-table td{padding:4px 6px;border-top:1px solid var(--line);white-space:nowrap}
#swap-table .action-btn{background:var(--hi);color:var(--bg);border:0;padding:2px 8px;font-size:10px;cursor:pointer;font-family:inherit;font-weight:bold;border-radius:2px}
#swap-table .action-btn:disabled{opacity:.3;cursor:not-allowed}
#swap-table .action-btn.failed{background:var(--warn)}
#swap-table .tx-link{color:var(--blue);text-decoration:none;font-size:10px}
#feed-card{grid-column:1/3;max-height:200px;overflow:auto}
#feed-card pre{margin:0;white-space:pre-wrap;word-break:break-all;font-size:11px;line-height:1.45}
.entry{color:var(--hi)}.stop{color:var(--warn)}
.halt{color:var(--gold)}.learn{color:var(--blue)}
.reject{color:#777}.notbuy{color:#ff9c5b}
.badge-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;background:var(--warn);animation:pulse 1.2s infinite}
.badge-dot.live{background:var(--hi)}.badge-dot.done{background:var(--gold);animation:none}
.monitor-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px}
.metric{background:var(--bg);border:1px solid var(--line);border-radius:4px;padding:6px 8px}
.metric .label{color:var(--dim);font-size:10px;text-transform:uppercase;letter-spacing:.5px}
.metric .value{color:var(--hi);font-size:16px;font-weight:bold;font-family:ui-monospace,Menlo,monospace}
.metric .value.gold{color:var(--gold)}
.metric .value.blue{color:var(--blue)}
.monitor-footer{color:var(--dim);font-size:10px;margin-top:8px;display:flex;justify-content:space-between}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
.sig-row{display:flex;gap:8px;align-items:center;margin-top:8px;padding:8px;background:#0b0f0c;border:1px solid var(--line);border-radius:4px}
.sig-row input{flex:1;background:#151a16;border:1px solid var(--line);color:var(--fg);padding:4px 6px;font-size:10px;font-family:inherit;border-radius:3px}
.tx-short{color:var(--blue);font-size:10px}
.swap-quote-preview{font-size:10px;color:var(--dim);margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

/* ── Tooltip system: pure CSS, zero JS, zero layout impact ── */
.has-tip{position:relative;cursor:help;border-bottom:1px dotted var(--dim)}
.has-tip::after{content:attr(data-tip);position:absolute;bottom:calc(100% + 6px);left:50%;transform:translateX(-50%);background:#1a231c;color:#d6f5d8;border:1px solid var(--line);padding:6px 10px;font-size:11px;font-family:ui-monospace,Menlo,Consolas,monospace;white-space:pre-line;max-width:280px;width:max-content;border-radius:4px;box-shadow:0 4px 12px rgba(0,0,0,.5);pointer-events:none;opacity:0;transition:opacity .15s ease;z-index:999;line-height:1.5}
.has-tip::before{content:"";position:absolute;bottom:calc(100% + 2px);left:50%;transform:translateX(-50%);border:4px solid transparent;border-top-color:var(--line);pointer-events:none;opacity:0;transition:opacity .15s ease;z-index:999}
.has-tip:hover::after,.has-tip:hover::before{opacity:1}
/* Wide tooltips for h2 titles — let them expand */
h2.has-tip::after{width:max-content;max-width:380px}
/* Labels inside metric cards already small — use subtle underline only */
.metric .label.has-tip{border-bottom-style: dotted;border-bottom-width: 1px}
/* Table headers — no underline to keep it clean */
th.has-tip{border-bottom:none}
</style>
</head>
<body>
<header>
  <h1 class="has-tip" data-tip="RH TRENCHER LIVE — 自动化 memecoin 交易前端\n实时 Desk Runner 信号 · Jupiter Metis Swap 执行 · Phantom 钱包签名">RH TRENCHER LIVE</h1>
  <label class="has-tip" data-tip="钱包连接状态\n绿色圆点 = Phantom 已连接且 pubkey 匹配\n红色圆点 = 未连接或 pubkey 不匹配"><span class="wallet-dot" id="walletDot"></span><span id="walletLabel">Wallet: disconnected</span></label>
  <input id="walletInput" type="text" placeholder="Paste Solana pubkey..." class="has-tip" data-tip="输入你的 Solana 钱包地址 (base58)\nPhantom/Sollet → 设置 → 钱包地址 → 复制粘贴">
  <button id="btnConnect" onclick="connectWallet()" class="has-tip" data-tip="将上方钱包地址注册为当前用户钱包\nsubmit 签名时会校验是否与此地址一致">Connect</button>
  <button id="btnRefresh" class="secondary has-tip" data-tip="手动刷新所有面板数据\n正常每 1s 自动轮询一次" onclick="refreshStatus()">Refresh</button>
  <button id="btnManual" class="secondary has-tip" data-tip="手动注入一条交易信号\n用于测试完整 BUY/SELL 链路" onclick="manualTest()">Manual Signal</button>
  <button id="btnLangEN" class="secondary" style="font-size:10px" onclick="setLang('en')">EN</button>
  <button id="btnLangZH" class="secondary" style="font-size:10px" onclick="setLang('zh')">中文</button>
  <span class="status-pill has-tip" id="pill" data-tip="Desk Runner 当前状态\ni​dle = 未启动\nrunning = tick 循环中\nrefreshing = 正在从 DexScreener 拉新币\nclosed = replay 一轮结束自动重开">● idle</span>
  <span style="color:var(--dim);margin-left:auto;font-size:10px" data-i18n="hint">Live · SSE stream · connect wallet first</span>
</header>

<div class="sig-row" id="manualRow" style="display:none;margin:10px 18px">
  <label style="color:var(--dim);font-size:11px">Ticker:</label>
  <input id="manualTicker" type="text" value="BONK" style="width:80px" class="has-tip" data-tip="已在 ticker_mint_map.json 中的 ticker\n大小写不敏感">
  <label style="color:var(--dim);font-size:11px">Side:</label>
  <select id="manualSide" style="background:#151a16;border:1px solid var(--line);color:var(--fg);padding:4px;font-size:11px" class="has-tip" data-tip="ENTRY = 触发 BUY (SOL→TOKEN)\nEXIT = 触发 SELL (TOKEN→SOL)">
    <option value="ENTRY">ENTRY (BUY)</option>
    <option value="EXIT">EXIT (SELL)</option>
  </select>
  <label style="color:var(--dim);font-size:11px">USD:</label>
  <input id="manualUsd" type="number" value="50" min="1" step="1" style="width:64px" class="has-tip" data-tip="投入金额 (美元)\n会按当前 SOL 价格换算成 lamports">
  <button onclick="sendManualSignal()" style="background:var(--blue);color:var(--bg)">Send</button>
</div>

<main>
  <div class="card has-tip" id="monitor-card" data-tip="📡 实时监控面板\n每 1s 自动刷新\n显示 Desk Runner 的交易核心指标">
    <h2 class="has-tip" data-i18n="liveMonitor" data-tip="📡 Live Monitor — 实时监控面板\n展示 Desk Runner 运行时的核心指标\nMultiple / Bankroll / Entries / Wins / Pending Swaps ...">📡 Live Monitor <span id="mon-pill" class="badge-dot"></span></h2>
    <div class="monitor-grid">
      <div class="metric"><div class="label has-tip" data-i18n="multiple" data-tip="资金倍数 = 当前 bankroll / 初始 bankroll\n> 1 = 盈利, < 1 = 亏损">Multiple</div><div class="value" id="mon-mult">—</div></div>
      <div class="metric"><div class="label has-tip" data-i18n="bankroll" data-tip="当前可用总资金 (美元)\n= 初始 500 USD * multiple + 未实现盈亏">Bankroll</div><div class="value" id="mon-bank">—</div></div>
      <div class="metric"><div class="label has-tip" data-i18n="entries" data-tip="Desk Runner 发出的 BUY 信号总数">Entries</div><div class="value" id="mon-entry">—</div></div>
      <div class="metric"><div class="label has-tip" data-i18n="winsLosses" data-tip="已平仓交易数\n绿 = 盈利, 红 = 亏损">Wins / Losses</div><div class="value" id="mon-wl">—</div></div>
      <div class="metric"><div class="label has-tip" data-i18n="pendingSwaps" data-tip="已生成 swap bundle 等待 Phantom 签名的交易数\n点 Submit 按钮签名后链上广播">Pending Swaps</div><div class="value gold" id="mon-pending">—</div></div>
      <div class="metric"><div class="label has-tip" data-i18n="submittedSwaps" data-tip="已签名并广播上链的交易数\n可在历史记录中跟踪">Submitted</div><div class="value blue" id="mon-submitted">—</div></div>
      <div class="metric"><div class="label has-tip" data-i18n="solPrice" data-tip="当前 SOL/USD 价格\n从 Jupiter quote 返回或手动设置">SOL Price</div><div class="value" id="mon-sol">—</div></div>
      <div class="metric"><div class="label has-tip" data-i18n="deskState" data-tip="Desk 交易状态机\nSCANNING = 扫描新币\nENTERED = 有持仓\nHALTED = 暂停（连续亏损触发）">Desk State</div><div class="value blue" id="mon-state">—</div></div>
    </div>
  </div>

  <div class="card has-tip" id="equity-card" data-tip="📈 资金曲线 (对数刻度)\n横轴 = tick 时间, 纵轴 = bankroll 变化\n绿色向上 = 盈利">
    <h2 class="has-tip" data-i18n="equityTitle" data-tip="📈 Equity 资金曲线 (对数坐标)\n横轴 = Desk tick 时间\n纵轴 = bankroll 美元值\n绿色线 = 实时 bankroll, 对数刻度展示大增长">Equity (USD) · log scale · scroll window</h2>
    <canvas id="equity" width="1400" height="260"></canvas>
  </div>

  <div class="card has-tip" id="swaps-card" data-tip="🔗 Swap Bundles — 待签名交易列表\n每一条 = 一个 Desk 信号 → Jupiter build_swap 结果\n点 ▶ 提交 → Phantom 签名 → 链上广播">
    <h2 class="has-tip" data-i18n="swapsTitle" data-tip="🔗 Swap Bundles — 待签名交易列表\n操作流程:\n① 点 ▶ 提交按钮\n② Phantom 弹窗确认签名\n③ 签名后自动链上广播\n绿色 ✓ = 已提交\n红色 ✗ = 广播失败">🔗 Swap Bundles <span id="swap-badge" style="color:var(--gold)">(0)</span></h2>
    <table id="swap-table">
      <thead><tr>
        <th class="has-tip" data-i18n="side" data-tip="BUY = SOL→TOKEN (开多)\nSELL = TOKEN→SOL (平仓)">Side</th>
        <th class="has-tip" data-i18n="ticker" data-tip="币种 ticker (来自 DexScreener)\nmint 地址在 ticker_mint_map.json 中映射">Ticker</th>
        <th class="has-tip" data-i18n="amountUsd" data-tip="本次投入/退出的美元金额\nDesk 按 Kelly criterion 自动计算">Amount $</th>
        <th class="has-tip" data-i18n="feeUsd" data-tip="Jupiter platform fee (0.25%)\n以及 Solana 网络 gas 费合计">Fee $</th>
        <th class="has-tip" data-i18n="impact" data-tip="滑点 = 市价冲击\n越低越好, >2% 可能导致失败">Impact</th>
        <th class="has-tip" data-i18n="output" data-tip="预估可收到的 TOKEN 数量 (BUY)\n或预估可换回的 SOL 数量 (SELL)">Out (est)</th>
        <th class="has-tip" data-i18n="action" data-tip="▶ 提交 = Phantom 签名 + 链上广播\n✓ 已提交 = 广播成功 (点击可查看 Solana Explorer)\n✗ 失败 = RPC 错误, 可重试">Action</th>
      </tr></thead>
      <tbody id="swap-body"></tbody>
    </table>
  </div>

  <div class="card has-tip" id="positions-card" data-tip="持仓列表\nDesk 内部跟踪的 token 持仓\nPeak > 2x 可能触发止盈, 跌破 1x 止损">
    <h2 class="has-tip" data-i18n="positionsTitle" data-tip="Open Positions — 当前持仓\nDesk 内部维护, 尚未平仓的 token\nPeak = 历史最高倍数\nCurrent = 当前倍数\nSize% = 本次持仓占 bankroll 比例">Open Positions</h2>
    <table id="pos-table">
      <thead><tr>
        <th class="has-tip" data-i18n="ticker" data-tip="币种 ticker">Ticker</th>
        <th class="has-tip" data-i18n="entryTime" data-tip="开仓时间 (Desk tick)">Entry</th>
        <th class="has-tip" data-i18n="entryUsd" data-tip="开仓时美元价值">Entry $</th>
        <th class="has-tip" data-i18n="peakMult" data-tip="历史最高倍数\n≥ 2.0x 可能触发止盈">Peak</th>
        <th class="has-tip" data-i18n="currentMult" data-tip="当前倍数\n< 1.0x = 浮亏">Current</th>
        <th class="has-tip" data-i18n="sizeFrac" data-tip="占 bankroll 百分比">Size%</th>
      </tr></thead>
      <tbody id="pos-body"></tbody>
    </table>
  </div>

  <div class="card has-tip" id="history-card" data-tip="已平仓历史交易\nBUY → SELL 完整记录">
    <h2 class="has-tip" data-i18n="historyTitle" data-tip="History Trades — 已平仓历史\n每条 = 完整一轮 BUY → SELL\nP/L = 倍数, Realized = 实际盈亏">History Trades</h2>
    <table id="hist-table">
      <thead><tr>
        <th class="has-tip" data-i18n="ticker" data-tip="币种 ticker">Ticker</th>
        <th class="has-tip" data-i18n="entryTime" data-tip="开仓时间">Entry</th>
        <th class="has-tip" data-i18n="entryUsd" data-tip="买入金额 (美元)">Buy $</th>
        <th class="has-tip" data-i18n="exitTime" data-tip="平仓时间">Exit</th>
        <th class="has-tip" data-i18n="exitUsd" data-tip="卖出金额 (美元)">Sell $</th>
        <th class="has-tip" data-i18n="pnlMult" data-tip="盈亏倍数\n绿 > 1, 红 < 1">P/L</th>
        <th class="has-tip" data-i18n="pnlUsd" data-tip="实际盈亏 (美元)\n绿正, 红负">Realized</th>
      </tr></thead>
      <tbody id="hist-body"></tbody>
    </table>
  </div>

  <div class="card has-tip" data-tip="Desk 内部状态\n策略引擎实时参数">
    <h2 class="has-tip" data-i18n="deskStatus" data-tip="Desk Status — 策略引擎内部状态\nstate = SCANNING/ENTERED/HALTED\ntheme = 当前叙事主题 (AI/memecoin/DeFi...)\nexpectancy = 期望值, Kelly = 仓位建议">Desk status</h2>
    <table id="status-table">
      <tr><td class="has-tip" data-i18n="state" data-tip="Desk FSM 状态\nSCANNING = 扫描, ENTERED = 持仓, HALTED = 暂停">state</td><td id="st-state">—</td></tr>
      <tr><td class="has-tip" data-i18n="theme" data-tip="Narrative 叙事集群标签\nDesk 会聚合同主题新币一起炒">theme</td><td id="st-theme">—</td></tr>
      <tr><td class="has-tip" data-i18n="bankroll" data-tip="当前 bankroll (美元)">bankroll</td><td id="st-bankroll">—</td></tr>
      <tr><td class="has-tip" data-i18n="multiple" data-tip="bankroll / 初始 500USD">multiple</td><td id="st-mult">—</td></tr>
      <tr><td class="has-tip" data-i18n="enteredRejected" data-tip="发出 BUY 信号数 / 被风控拒单数">entered / rejected</td><td id="st-er">—</td></tr>
      <tr><td class="has-tip" data-i18n="expectancy" data-tip="期望值 = P_win * avg_win - P_loss * avg_loss">expectancy</td><td id="st-exp">—</td></tr>
      <tr><td class="has-tip" data-i18n="kelly" data-tip="Kelly criterion 仓位建议\nfull Kelly = 理论最优, used = 实际用 (乘以 0.15)">full / used Kelly</td><td id="st-kelly">—</td></tr>
      <tr><td class="has-tip" data-i18n="currentOpen" data-tip="当前同时持有的 token 数">current open</td><td id="st-open">—</td></tr>
    </table>
  </div>
  <div class="card has-tip" id="feed-card" data-tip="实时信号流\nDesk Runner tick 循环日志\n绿色=入场, 红色=止损, 金色=止盈">
    <h2 class="has-tip" data-i18n="feed" data-tip="Signal Feed — 实时信号日志\n绿色 = ENTRY (开多)\n金色 = HALT (止盈)\n红色 = STOP (止损)\n蓝色 = LEARN (策略学习)\n灰色 = REJECT (风控拒单)">Signal Feed</h2>
    <pre id="feed-log"></pre>
  </div>
</main>

<script>
// === i18n ===
const I18N = {
  en: {
    hint:"Live · SSE stream · connect wallet first",
    liveMonitor:"📡 Live Monitor", multiple:"Multiple", bankroll:"Bankroll",
    entries:"Entries", winsLosses:"Wins / Losses", pendingSwaps:"Pending Swaps",
    submittedSwaps:"Submitted", solPrice:"SOL Price", deskState:"Desk State",
    equityTitle:"Equity (USD) · log scale · scroll window",
    swapsTitle:"🔗 Swap Bundles", side:"Side", ticker:"Ticker",
    amountUsd:"Amount $", feeUsd:"Fee $", impact:"Impact", output:"Out (est)", action:"Action",
    positionsTitle:"Open Positions", entryTime:"Entry", entryUsd:"Entry $",
    peakMult:"Peak", currentMult:"Current", sizeFrac:"Size%",
    historyTitle:"History Trades", exitTime:"Exit", exitUsd:"Sell $",
    pnlMult:"P/L", pnlUsd:"Realized", why:"Why",
    deskStatus:"Desk status", state:"state", theme:"theme",
    enteredRejected:"entered / rejected", expectancy:"expectancy",
    kelly:"full / used Kelly", currentOpen:"current open",
    feed:"Signal Feed", idle:"idle", running:"running", done:"done",
    noData:"No data", noOpen:"— no open positions —", noHistory:"— no closed trades yet —",
    submit:"Submit", submitted:"Submitted", failed:"Failed", pending:"Pending",
    connectWallet:"Connect Wallet", disconnect:"Disconnect", connected:"Connected",
    manualSignal:"Manual Signal Test",
  },
  zh: {
    hint:"实时模式 · SSE 流 · 先连接钱包",
    liveMonitor:"📡 实时监视器", multiple:"倍数", bankroll:"可用资金",
    entries:"入场次数", winsLosses:"胜 / 负", pendingSwaps:"待签名",
    submittedSwaps:"已提交", solPrice:"SOL 价格", deskState:"运行状态",
    equityTitle:"净值曲线 (USD) · 对数刻度 · 滚动窗口",
    swapsTitle:"🔗 Swap 交易包", side:"方向", ticker:"币种",
    amountUsd:"金额 $", feeUsd:"手续费 $", impact:"滑点影响", output:"预期输出", action:"操作",
    positionsTitle:"当前持仓", entryTime:"入场", entryUsd:"入场 $",
    peakMult:"峰值", currentMult:"当前", sizeFrac:"仓位%",
    historyTitle:"历史交易", exitTime:"出场", exitUsd:"卖出 $",
    pnlMult:"盈亏倍率", pnlUsd:"已实现", why:"原因",
    deskStatus:"交易台状态", state:"状态", theme:"主题",
    enteredRejected:"入场 / 拒绝", expectancy:"期望值",
    kelly:"Kelly 满 / 实", currentOpen:"当前持仓详情",
    feed:"信号流", idle:"待机", running:"运行中", done:"完成",
    noData:"无数据", noOpen:"— 暂无持仓 —", noHistory:"— 暂无历史交易 —",
    submit:"提交", submitted:"已提交", failed:"失败", pending:"待签名",
    connectWallet:"连接钱包", disconnect:"断开", connected:"已连接",
    manualSignal:"手动信号测试",
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
let animFrame=null;
let lastSSE=null;
let _sseThrottle=100;
let _lastSSETime=0;

// === Equity draw ===
function drawEquity(){
  const W=eqCanvas.width, H=eqCanvas.height;
  eqCtx.fillStyle="#0d120e"; eqCtx.fillRect(0,0,W,H);
  if(eqW.length<2) return;
  const WINDOW_T_MIN=200, CURSOR_POS=0.75;
  let xmin=eqW[eqW.length-1]-WINDOW_T_MIN*CURSOR_POS;
  xmin=Math.max(0,xmin);
  let xmax=xmin+WINDOW_T_MIN;
  let iStart=0;
  while(iStart<eqW.length&&eqW[iStart]<xmin)iStart++;
  const visT=eqW.slice(iStart), visV=eqV.slice(iStart);
  if(visT.length<2)return;
  const start=500;
  const vmin=Math.max(30,Math.min(start*0.35,Math.min(...visV)*0.8));
  const vmax=Math.max(start*10,Math.max(...visV)*1.2);
  const padL=54,padR=14,padT=14,padB=26;
  const pw=W-padL-padR, ph=H-padT-padB;
  const logVmin=Math.log10(vmin), logVmax=Math.log10(vmax);
  const sx=t=>padL+pw*((t-xmin)/(xmax-xmin));
  const sy=v=>padT+ph*(1-(Math.log10(Math.max(vmin,v))-logVmin)/(logVmax-logVmin));
  eqCtx.strokeStyle="#1a3a22"; eqCtx.lineWidth=0.5;
  eqCtx.font="10px ui-monospace,monospace"; eqCtx.fillStyle="#7ab080";
  for(let v=Math.pow(10,Math.ceil(logVmin));v<=vmax;v*=10){
    const y=sy(v); eqCtx.beginPath(); eqCtx.moveTo(padL,y); eqCtx.lineTo(W-padR,y); eqCtx.stroke();
    eqCtx.fillText("$"+v.toLocaleString(),6,y+3);
  }
  eqCtx.strokeStyle="#3dff8a"; eqCtx.lineWidth=2;
  eqCtx.beginPath();
  for(let i=0;i<visT.length;i++){
    const x=sx(visT[i]), y=sy(visV[i]);
    i===0?eqCtx.moveTo(x,y):eqCtx.lineTo(x,y);
  }
  eqCtx.stroke();
  const cx=sx(visT[visT.length-1]), cy=sy(visV[visV.length-1]);
  eqCtx.fillStyle="#3dff8a"; eqCtx.beginPath(); eqCtx.arc(cx,cy,4,0,Math.PI*2); eqCtx.fill();
  eqCtx.strokeStyle="#3dff8a80"; eqCtx.beginPath(); eqCtx.arc(cx,cy,9,0,Math.PI*2); eqCtx.stroke();
  eqCtx.fillStyle="#cde8d0"; eqCtx.fillText("$"+visV[visV.length-1].toFixed(2),cx+10,cy-4);
}

// === Status & Monitor ===
function updateStatus(s){
  const snap=s.snap;
  $("st-state").textContent=snap.state;
  $("st-theme").textContent=snap.theme||"—";
  $("st-bankroll").textContent="$"+snap.bankroll.toFixed(2);
  $("st-mult").textContent=snap.multiple.toFixed(2)+"x";
  $("st-er").textContent=snap.entered+" / "+snap.rejected;
  $("st-exp").textContent=snap.expectancy.toFixed(3);
  $("st-kelly").textContent=snap.full_kelly.toFixed(3)+" / "+snap.used_kelly.toFixed(3);
  const open=(snap.open_positions&&snap.open_positions[0])||null;
  $("st-open").textContent=open?`${open.ticker} @ ${open.entry_min}m peak=${open.peak_mult}x`:"—";
}
function updateMonitor(m){
  if(!m)return;
  $("mon-mult").textContent=(m.multiple||1).toFixed(2)+"x";
  $("mon-bank").textContent="$"+(m.bankroll||0).toFixed(0);
  $("mon-entry").textContent=m.entries||0;
  $("mon-wl").textContent=(m.wins||0)+" / "+(m.losses||0);
  $("mon-pending").textContent=m.pending_swaps||0;
  $("mon-submitted").textContent=m.submitted_swaps||0;
  $("mon-state").textContent=m.state||"—";
  document.getElementById("mon-pill").className="badge-dot live";
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
    tbody.innerHTML=`<tr><td colspan="6" style="text-align:center;color:#7ab080;padding:14px">— ${lang==="zh"?"暂无持仓":"no open positions"} —</td></tr>`;return;
  }
  tbody.innerHTML=positions.map(p=>{
    const nowColor=(p.current_mult||1)>=1?'var(--hi)':'var(--warn)';
    return `<tr><td>${p.ticker}</td><td>${p.entry_min}m</td><td>$${p.entry_usd.toFixed(2)}</td>
      <td>${(p.peak_mult||0).toFixed(2)}x</td><td style="color:${nowColor}">${(p.current_mult||1).toFixed(2)}x</td><td>${p.size_frac}%</td></tr>`;
  }).join("");
}

// === History ===
function renderHistory(closed){
  const tbody=$("hist-body");
  if(!closed||!closed.length){
    tbody.innerHTML=`<tr><td colspan="7" style="text-align:center;color:#7ab080;padding:14px">— ${lang==="zh"?"暂无历史":"no history yet"} —</td></tr>`;return;
  }
  tbody.innerHTML=closed.map(c=>{
    const cls=c.win?"win":"loss";
    const sign=c.pnl_usd>0?"+":"";
    return `<tr class="${cls}"><td>${c.ticker}</td><td>${c.entry_min}m</td><td>$${c.entry_usd.toFixed(2)}</td>
      <td>${c.exit_min}m</td><td>$${c.exit_usd.toFixed(2)}</td>
      <td class="pnl">${c.pnl_mult.toFixed(2)}x (${sign}${((c.pnl_mult-1)*100).toFixed(1)}%)</td>
      <td class="pnl">${sign}$${c.pnl_usd.toFixed(2)}</td></tr>`;
  }).join("");
}

// === Swaps table (new!) ===
let _swapVersions=0;
function renderSwaps(swapsData){
  if(!swapsData) return;
  const tbody=document.getElementById("swap-body");
  const badge=document.getElementById("swap-badge");
  if(!tbody||!badge) return;
  const pending=swapsData.pending||[];
  const submitted=swapsData.submitted||[];
  const failed=swapsData.failed||[];
  badge.textContent=`(${pending.length})`;
  let html="";
  pending.forEach(s=>{
    const impact=s.quote?`${s.quote.price_impact_pct?.toFixed(3)||0}%`:"—";
    const out=s.quote?`${(s.quote.out_amount/1e9).toFixed(4)} SOL`:`—`;
    html+=`<tr data-key="${s.key}">
      <td style="color:${s.side==="BUY"?"var(--hi)":"var(--warn)"}">${s.side}</td>
      <td>${s.ticker}</td>
      <td>$${s.amount_usd.toFixed(2)}</td>
      <td>$${s.fee_usd.toFixed(4)}</td>
      <td style="color:var(--gold)">${impact}</td>
      <td class="tx-short">${out}</td>
      <td><button class="action-btn" onclick="submitSwap('${s.key}')" id="btn-${s.key}">▶ ${lang==="zh"?"提交":"Submit"}</button></td>
    </tr>`;
  });
  submitted.forEach(s=>{
    html+=`<tr><td style="color:var(--hi)">${s.side}</td><td>${s.ticker}</td>
      <td>$${s.amount_usd.toFixed(2)}</td><td colspan="3" class="tx-short">${s.sig||""}</td>
      <td><span style="color:var(--hi)">✓ ${lang==="zh"?"已提交":"Submitted"}</span></td></tr>`;
  });
  failed.forEach(s=>{
    html+=`<tr><td style="color:var(--warn)">${s.side}</td><td>${s.ticker}</td>
      <td colspan="5" style="color:var(--warn);font-size:10px">${s.error||""}</td>
      <td><span class="action-btn failed">✗ ${lang==="zh"?"失败":"Failed"}</span></td></tr>`;
  });
  if(!html) html=`<tr><td colspan="7" style="text-align:center;color:#7ab080;padding:14px">— ${lang==="zh"?"无待处理交易":"no pending swaps"} —</td></tr>`;
  tbody.innerHTML=html;
}

// === Swap signing + submission via Phantom wallet ===
// Transaction encoding helpers (no deps, pure JS)
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

  // 1. Find the pending swap from current swapsData — need its transaction_base64
  // We re-fetch fresh swaps to be safe
  let swapsResp, swapsData;
  try{
    swapsResp = await fetch("/api/swaps",{cache:"no-store"});
    swapsData = await swapsResp.json();
  }catch(e){ showToast(`❌ ${e.message}`,true); btn.disabled=false; btn.textContent=lang==="zh"?"▶ 提交":"▶ Submit"; return; }

  const pending = (swapsData.pending||[]).find(s=>s.key===key);
  if(!pending||!pending.bundle){ btn.disabled=false; btn.textContent=lang==="zh"?"▶ 提交":"▶ Submit"; showToast(lang==="zh"?"❌ Swap 已不在待处理列表":"❌ Swap no longer pending",true); return; }

  const txB64 = pending.bundle.transaction_base64;
  if(!txB64){ btn.disabled=false; btn.textContent=lang==="zh"?"▶ 提交":"▶ Submit"; showToast(lang==="zh"?"❌ 空交易包":"❌ Empty tx bundle",true); return; }

  // 2. Connect Phantom
  const phantom = await ensurePhantom();
  if(!phantom){ btn.disabled=false; btn.textContent=lang==="zh"?"▶ 提交":"▶ Submit"; return; }

  // 3. Sign the transaction
  btn.textContent=lang==="zh"?"钱包签名中...":"Signing with wallet...";
  let signedB64;
  try{
    const txBytes = SwapCodec.base64ToBytes(txB64);
    const signed = await phantom.signAllTransactions([txBytes]);
    // signed is [{publicKey, signature}] — we need signed tx bytes
    // Phantom returns {signature} as base58; we must attach it to the tx
    // Simpler: signTransaction returns the full signed transaction bytes
    btn.textContent=lang==="zh"?"重新签名中...":"Re-signing...";
    const signedOne = await phantom.signTransaction(txBytes);
    signedB64 = SwapCodec.bytesToBase64(signedOne);
  }catch(e){
    btn.disabled=false;
    btn.textContent=lang==="zh"?"▶ 提交":"▶ Submit";
    showToast(`❌ ${lang==="zh"?"签名失败":"Sign failed"}: ${e.message||e}`,true);
    return;
  }

  // 4. Submit signed tx to backend for RPC broadcast
  btn.textContent=lang==="zh"?"链上广播中...":"Broadcasting...";
  try{
    const resp = await fetch("/api/swap/submit",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({key,signed_tx:signedB64})
    });
    const data = await resp.json();
    if(data.ok){
      btn.textContent="✓ "+(lang==="zh"?"已提交":"Submitted");
      btn.style.background="var(--hi)";
      showToast(`✅ ${lang==="zh"?"交易已提交":"Swap submitted"}: ${data.sig?.substring(0,16)}...`);
      refreshStatus();
    }else{
      btn.classList.add("failed");
      btn.textContent="✗ "+(lang==="zh"?"失败":"Failed");
      showToast(`❌ ${data.error||"unknown error"}`,true);
      setTimeout(()=>{btn.disabled=false;btn.textContent="▶ "+(lang==="zh"?"提交":"Submit");btn.classList.remove("failed");},3000);
    }
  }catch(e){
    btn.classList.add("failed");btn.textContent="✗ "+(lang==="zh"?"失败":"Failed");
    showToast(`❌ ${e.message}`,true);
    setTimeout(()=>{btn.disabled=false;btn.textContent="▶ "+(lang==="zh"?"提交":"Submit");btn.classList.remove("failed");},3000);
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
    document.getElementById("walletInput").value=w;
    document.getElementById("walletLabel").textContent="Wallet: "+w.substring(0,8)+"..."+w.substring(w.length-6);
    document.getElementById("walletDot").classList.add("connected");
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
  t.style.cssText=`position:fixed;bottom:20px;right:20px;padding:10px 18px;border-radius:6px;font-size:12px;z-index:9999;max-width:400px;word-break:break-all;${isError?"background:#ff5b5b;color:#fff":"background:#3dff8a;color:#0b0f0c"}`;
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
  }catch(e){}
}

// === SSE connection ===
function applyDeskState(s){
  if(!s)return;
  if(s.equity&&Array.isArray(s.equity)&&s.equity.length>0){eqW=s.equity.map(r=>r[0]);eqV=s.equity.map(r=>r[1]);}
  if(s.monitor&&s.monitor.multiple!==undefined){updateMonitor(s.monitor);}
  if(s.feed){renderFeed(s.feed);}
  if(s.positions){renderPositions(s.positions);}
  if(s.closed_trades){renderHistory(s.closed_trades);}
  if(s.swaps){renderSwaps(s.swaps);}
  if(s.snap){updateStatus(s);}
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
  eqW=[];eqV=[];
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
function animLoop(){
  drawEquity();
  animFrame=requestAnimationFrame(animLoop);
}
animLoop();
loadWallet();
applyLang();
window.addEventListener("load",()=>setTimeout(startSSE,300));
</script>
</body>
</html>
"""


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
            state = getattr(_server, "_desk_state", None) if _server else None
            if state is None:
                self._send_json(404, {"error": "no desk state yet"})
            else:
                self._send_json(200, state)
        elif u.path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
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
    if executor_mode == "okx" and OKXExecutor is not None:
        ex = OKXExecutor.from_env()
        auth_str = "ready" if ex._auth_ready else "keys-not-set"
        print(f"[LIVE_SERVER] Executor: OKX (auth={auth_str}, demo={ex.demo})")
        bc = ex.ai_builder_code or "(not set - no commission tracking)"
        print(f"[LIVE_SERVER] Builder Code: {bc}")
    else:
        ex = JupiterExecutor.from_env()
        print("[LIVE_SERVER] Executor: Jupiter (Solana)")

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
                                queue=q, refresh_interval_min=refresh_min)
            runner.start()
            print(f"[LIVE_SERVER] DeskRunner STARTED tick_ms={tick_ms} max_positions={max_positions}")
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
