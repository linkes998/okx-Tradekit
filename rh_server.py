#!/usr/bin/env python3
"""RH Trencher — Web live dashboard. Pure stdlib (http.server + SSE)."""
from __future__ import annotations

import json
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import numpy as np
from rh_trencher import Desk, DeskState, scenario

# ── Live-mode push pipeline (Task 6/7) ───────────────────────────────────────
_live_queue: queue.Queue[dict] | None = None


def push_state(state: dict) -> None:
    """Called by TickEngine at the end of each tick_loop iteration.

    Pushes a desk.snapshot() dict (with optional fx/usd extras) into the SSE
    queue so the frontend receives live-mode updates instead of replay steps.
    """
    if _live_queue is None:
        return
    try:
        _live_queue.put_nowait(state)
    except queue.Full:
        pass  # drop oldest if client is slow


def start_live_queue(maxsize: int = 30) -> queue.Queue:
    global _live_queue
    _live_queue = queue.Queue(maxsize=maxsize)
    return _live_queue


# ──────────────────────────────────────────────────────────────────────
#  引擎：generator 版本的 run_replay —— 每 step yield (t, desk, event_dict)
# ──────────────────────────────────────────────────────────────────────
def step_replay(seed: int = 7, loss_pause_n: int = 3, from_csv: str | None = None,
                max_positions: int = 3, thin_cut: float | None = None,
                live_mode: bool = False, realistic: bool = False,
                max_slippage_pct: float = 0.02, peak_proxy_coef: float = 0.6):
    rng = np.random.default_rng(seed)
    desk = Desk(narrative_seed=seed, loss_pause_n=loss_pause_n,
                max_positions=max_positions, thin_cut_override=thin_cut,
                live_mode=live_mode, realistic=realistic,
                max_slippage_pct=max_slippage_pct, peak_proxy_coef=peak_proxy_coef)
    if from_csv:
        from fetch_dexscreener import load_csv_as_tokenlaunches
        raw_tokens = load_csv_as_tokenlaunches(from_csv)
    else:
        raw_tokens = scenario(rng)
    events = sorted(raw_tokens, key=lambda t: (t.t_min, t.ticker))
    i, pending_marks, extra_dump_armed = 0, [], False

    def schedule_marks(tok, entry_t):
        for j, m in enumerate(tok.true_multiple_path):
            pending_marks.append((entry_t + 1 + j * 2, tok, m))

    seen, scatter_tokens = set(), []
    for t in raw_tokens:
        if t.ticker not in seen:
            scatter_tokens.append(t)
            seen.add(t.ticker)
    scatter_meta = [{"ticker": t.ticker, "theme": t.theme_hint,
                     "desc": (t.description or "")[:40]} for t in scatter_tokens]

    # Track closed trades by matching ENTRY/EXIT fills
    open_trades = {}   # ticker -> {entry_min, entry_usd}
    closed_trades = []

    def make_state(t):
        nonlocal open_trades, closed_trades
        # Recompute from desk.feed to catch new closes
        for f in desk.feed[-50:]:
            if f.side == "ENTRY" and f.ticker not in open_trades:
                # Find matching entry position data
                pos = next((p for p in desk.positions if p.ticker == f.ticker), None)
                entry_usd = pos.entry_usd if pos else f.usd
                open_trades[f.ticker] = {"entry_min": f.t_min, "entry_usd": round(entry_usd, 2)}
            elif f.side in ("EXIT", "STOP") and f.ticker in open_trades:
                ot = open_trades.pop(f.ticker)
                exit_usd = round(ot["entry_usd"] * f.multiple, 2)
                pnl_mult = round(f.multiple, 3)
                pnl_usd = round(exit_usd - ot["entry_usd"], 2)
                closed_trades.append({
                    "ticker": f.ticker,
                    "entry_min": ot["entry_min"],
                    "entry_usd": ot["entry_usd"],
                    "exit_min": f.t_min,
                    "exit_usd": exit_usd,
                    "pnl_mult": pnl_mult,
                    "pnl_usd": pnl_usd,
                    "why": f.note or "",
                    "win": f.multiple >= 1.0,
                })

        df = desk.narrative.project_2d(scatter_tokens)
        pts = df[["x", "y"]].values.tolist()
        centroid_xy = None
        c = desk.narrative.cluster_centroid
        if c is not None:
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
        # Monitor stats (single-replay aggregates)
        wins = sum(1 for ct in closed_trades if ct["win"])
        losses = len(closed_trades) - wins
        monitor = {
            "multiple": round(desk.bankroll / desk.start, 2),
            "bankroll": round(desk.bankroll, 2),
            "total_pools": len(raw_tokens),
            "entries": desk.entered,
            "wins": wins,
            "losses": losses,
            "dominant_theme": desk.narrative.cluster_label,
            "profit_rate": round(wins / len(closed_trades), 3) if closed_trades else 0.0,
            "open_count": len(desk.positions),
            "state": desk.state.name,
        }
        # Live-mode extras (Task 7: SSE push pipeline)
        extras: dict = {}
        if getattr(desk, "eth_usd", None):
            extras["eth_usd"] = round(desk.eth_usd, 2)
        if getattr(desk, "fx_feed", None):
            try:
                extras["eth_usd"] = round(desk.fx_feed.get_eth_usd(), 2)
                extras["sol_usd"] = round(desk.fx_feed.get_sol_usd(), 2)
            except Exception:
                pass
        state_dict = {
            "t": int(t),
            "snap": snap,
            "equity": equity,
            "scatter": {"points": pts, "centroid": centroid_xy, "tokens": scatter_meta},
            "feed": feed,
            "positions": positions,
            "closed_trades": list(reversed(closed_trades[-50:])),  # newest first, capped at 50
            "monitor": monitor,
            "bankroll": round(desk.bankroll, 2),
            "state": desk.state.name,
            "narrative": {"label": desk.narrative.cluster_label, "min_match": desk.narrative.min_match},
            "done": False,
            **extras,
        }
        return state_dict

    yield make_state(0)

    while i < len(events) or pending_marks:
        nxt_event = events[i].t_min if i < len(events) else 10**9
        nxt_mark = min(pending_marks, key=lambda z: z[0])[0] if pending_marks else 10**9
        if nxt_event <= nxt_mark and i < len(events):
            tok = events[i]
            t = tok.t_min
            i += 1
            if max(tok.true_multiple_path or [1]) >= 1.5:
                desk.observe_runner(tok, max(tok.true_multiple_path))
            before = len(desk.positions)
            desk.on_launch(tok)
            if len(desk.positions) > before:
                schedule_marks(tok, t)
        else:
            t, tok, mult = min(pending_marks, key=lambda z: z[0])
            pending_marks.remove((t, tok, mult))
            if mult >= 1.3:
                desk.observe_runner(tok, mult)
            last = not any(pm[1].ticker == tok.ticker for pm in pending_marks)
            pos_match = next((p for p in desk.positions if p.ticker == tok.ticker), None)
            forced = None
            if last and pos_match and mult >= 1.5:
                forced = f"EXIT rotate off last mark {mult:.1f}x"
            desk.mark_and_maybe_exit(tok, t, mult, forced=forced)
        # Append tick snapshot to equity curve for dense scrolling
        desk.equity_curve.append((t, desk.bankroll, "tick"))
        yield make_state(t)

    s = make_state(500)
    s["done"] = True
    yield s


# ──────────────────────────────────────────────────────────────────────
#  Server
# ──────────────────────────────────────────────────────────────────────
INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>RH Trencher · Live</title>
<style>
  :root {
    --bg:#0b0f0c; --fg:#cde8d0; --hi:#3dff8a; --warn:#ff5b5b;
    --gold:#ffd93d; --blue:#8ab4ff; --dim:#7ab080; --line:#2a4a32;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);
       font:12px/1.5 ui-monospace,Menlo,Consolas,monospace}
  header{padding:10px 18px;border-bottom:1px solid var(--line);
         display:flex;gap:14px;align-items:center;flex-wrap:wrap}
  header h1{margin:0;font-size:14px;color:var(--hi);letter-spacing:.5px}
  header label{display:flex;align-items:center;gap:6px;color:var(--dim)}
  header input{background:#151a16;border:1px solid var(--line);color:var(--fg);
               padding:4px 6px;width:72px;font-family:inherit;border-radius:3px}
  header button{background:var(--hi);color:var(--bg);border:0;padding:5px 14px;
                font-weight:bold;cursor:pointer;font-family:inherit;border-radius:3px}
  header button.secondary{background:var(--line);color:var(--fg)}
  header button.lang-btn{background:#151a16;border:1px solid var(--line);color:var(--dim);
                         padding:4px 10px;font-size:11px}
  header button.lang-btn.active{color:var(--hi);border-color:var(--hi)}
  header .status-pill{padding:3px 10px;border-radius:20px;border:1px solid var(--line);
                      color:var(--dim);font-size:11px}
  header .status-pill.live{color:var(--hi);border-color:var(--hi)}
  header .status-pill.done{color:var(--gold);border-color:var(--gold)}
  main{display:grid;grid-template-columns:1fr 1fr;gap:14px;padding:14px}
  .card{border:1px solid var(--line);border-radius:6px;padding:10px;background:#0d120e}
  .card h2{margin:0 0 8px;font-size:11px;color:var(--dim);text-transform:uppercase;
           letter-spacing:1px;font-weight:600}
  canvas{display:block;width:100%;height:auto}
  #equity-card{grid-column:1/3}
  #feed-card{grid-column:1/3;max-height:260px;overflow:auto}
  #monitor-card{grid-column:1/3}
  #positions-card, #history-card{max-height:240px;overflow:auto}
  #status-table,#pos-table,#hist-table{width:100%;border-collapse:collapse;font-size:11px}
  #status-table td{padding:3px 6px}
  #status-table td:first-child{color:var(--dim);width:45%}
  #status-table td:last-child{text-align:right;color:var(--hi)}
  #status-table tr.warn td:last-child{color:var(--warn)}
  #status-table tr.gold td:last-child{color:var(--gold)}
  #pos-table th,#hist-table th{text-align:left;padding:4px 6px;background:#0b0f0c;
               color:var(--dim);font-weight:600;font-size:10px;
               text-transform:uppercase;letter-spacing:.5px;
               position:sticky;top:0;z-index:1}
  #pos-table td,#hist-table td{padding:4px 6px;border-top:1px solid var(--line);white-space:nowrap}
  #pos-table tr.exit td{background:#0a0d0a;color:#7ab080}
  #hist-table tr.win td.pnl{color:var(--hi)}
  #hist-table tr.loss td.pnl{color:var(--warn)}
  #feed-card pre{margin:0;white-space:pre-wrap;word-break:break-all;font-size:11px;line-height:1.45}
  .entry{color:var(--hi)}.stop{color:var(--warn)}
  .halt{color:var(--gold)}.learn{color:var(--blue)}
  .reject{color:#777}.notbuy{color:#ff9c5b}
  .badge-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;
             background:var(--warn);animation:pulse 1.2s infinite}
  .badge-dot.live{background:var(--hi)}.badge-dot.done{background:var(--gold);animation:none}
  #monitor-card .monitor-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px}
  #monitor-card .metric{background:#0b0f0c;border:1px solid var(--line);border-radius:4px;padding:6px 8px}
  #monitor-card .metric .label{color:var(--dim);font-size:10px;text-transform:uppercase;letter-spacing:.5px}
  #monitor-card .metric .value{color:var(--hi);font-size:16px;font-weight:bold;font-family:ui-monospace,Menlo,monospace}
  #monitor-card .metric .value.gold{color:var(--gold)}
  #monitor-card .metric .value.blue{color:var(--blue)}
  #monitor-card .monitor-footer{color:var(--dim);font-size:10px;margin-top:8px;display:flex;justify-content:space-between}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
</style>
</head>
<body>
<header>
  <h1>RH TRENCHER</h1>
  <label>Seed <input id="seed" type="number" value="2" min="0" max="999"></label>
  <label>LPN <input id="lpn" type="number" value="3" min="1" max="6"></label>
  <label data-i18n="stepMs">Step ms</label>
  <input id="stepms" type="number" value="180" min="0" max="2000" step="10">
  <label data-i18n="maxPos">Max Pos</label>
  <input id="maxpos" type="number" value="10" min="1" max="20">
  <label>LG-Proxy <input id="lgproxy" type="checkbox" checked></label>
  <label data-i18n="realistic">Realistic <input id="realistic" type="checkbox" checked></label>
  <button id="btnRun" data-i18n="run">Run</button>
  <button id="btnReset" class="secondary" data-i18n="reset">Reset</button>
  <button id="btnLangEN" class="lang-btn active">EN</button>
  <button id="btnLangZH" class="lang-btn">中文</button>
  <span class="status-pill" id="pill">● idle</span>
  <span style="color:var(--dim);margin-left:auto" data-i18n="hint">SSE 实时流 · 零额外依赖</span>
</header>
<main>
  <div class="card" id="monitor-card">
    <h2 data-i18n="liveMonitor">📡 Live Monitor <span id="mon-pill" class="badge-dot"></span></h2>
    <div class="monitor-grid">
      <div class="metric"><div class="label" data-i18n="multiple">Multiple</div><div class="value" id="mon-mult">—</div></div>
      <div class="metric"><div class="label" data-i18n="bankroll">Bankroll</div><div class="value" id="mon-bank">—</div></div>
      <div class="metric"><div class="label" data-i18n="totalPools">Total Pools</div><div class="value blue" id="mon-pools">—</div></div>
      <div class="metric"><div class="label" data-i18n="entries">Entries</div><div class="value" id="mon-entry">—</div></div>
      <div class="metric"><div class="label" data-i18n="winsLosses">Wins / Losses</div><div class="value" id="mon-wl">—</div></div>
      <div class="metric"><div class="label" data-i18n="dominantTheme">Dominant Theme</div><div class="value gold" id="mon-theme">—</div></div>
      <div class="metric"><div class="label" data-i18n="profitRate">Profit Rate</div><div class="value" id="mon-profit">—</div></div>
      <div class="metric"><div class="label" data-i18n="openPositions">Open Pos</div><div class="value" id="mon-open">—</div></div>
      <div class="metric"><div class="label" data-i18n="deskState">Desk State</div><div class="value blue" id="mon-state">—</div></div>
    </div>
  </div>

  <div class="card" id="equity-card">
    <h2 data-i18n="equityTitle">Equity (USD) · log scale · scroll window</h2>
    <canvas id="equity" width="1400" height="260"></canvas>
  </div>

  <div class="card" id="positions-card">
    <h2 data-i18n="positionsTitle">Open Positions</h2>
    <table id="pos-table">
      <thead><tr>
        <th data-i18n="ticker">Ticker</th>
        <th data-i18n="entryTime">Entry</th>
        <th data-i18n="entryUsd">Entry $</th>
        <th data-i18n="peakMult">Peak</th>
        <th data-i18n="currentMult">Current</th>
        <th data-i18n="sizeFrac">Size%</th>
      </tr></thead>
      <tbody id="pos-body"></tbody>
    </table>
  </div>

  <div class="card" id="history-card">
    <h2 data-i18n="historyTitle">History Trades</h2>
    <table id="hist-table">
      <thead><tr>
        <th data-i18n="ticker">Ticker</th>
        <th data-i18n="entryTime">Entry</th>
        <th data-i18n="entryUsd">Buy $</th>
        <th data-i18n="exitTime">Exit</th>
        <th data-i18n="exitUsd">Sell $</th>
        <th data-i18n="pnlMult">P/L</th>
        <th data-i18n="pnlUsd">Realized</th>
        <th data-i18n="why">Why</th>
      </tr></thead>
      <tbody id="hist-body"></tbody>
    </table>
  </div>

  <div class="card">
    <h2 data-i18n="scatterTitle">Narrative cluster · animated</h2>
    <canvas id="scatter" width="700" height="300"></canvas>
  </div>
  <div class="card">
    <h2 data-i18n="deskStatus">Desk status</h2>
    <table id="status-table">
      <tr><td data-i18n="state">state</td><td id="st-state">—</td></tr>
      <tr><td data-i18n="theme">theme</td><td id="st-theme">—</td></tr>
      <tr><td data-i18n="bankroll">bankroll</td><td id="st-bankroll">—</td></tr>
      <tr><td data-i18n="multiple">multiple</td><td id="st-mult">—</td></tr>
      <tr><td data-i18n="enteredRejected">entered / rejected</td><td id="st-er">—</td></tr>
      <tr><td data-i18n="consecLosses">consecutive losses</td><td id="st-loss">—</td></tr>
      <tr><td data-i18n="expectancy">expectancy</td><td id="st-exp">—</td></tr>
      <tr><td data-i18n="kelly">full / used Kelly</td><td id="st-kelly">—</td></tr>
      <tr><td data-i18n="ruinProb">ruin probability</td><td id="st-ruin">—</td></tr>
      <tr><td data-i18n="marketSurvivors">market survivors</td><td id="st-surv">—</td></tr>
      <tr><td data-i18n="currentOpen">current open</td><td id="st-open">—</td></tr>
    </table>
  </div>
  <div class="card" id="feed-card">
    <h2 data-i18n="feed">Feed (last 20 events)</h2>
    <pre id="feed-log"></pre>
  </div>
</main>

<script>
// === i18n ===
const I18N = {
  en: {
    stepMs:"Step ms", maxPos:"Max Pos", realistic:"Realistic",
    run:"Run", reset:"Reset", hint:"SSE live stream · zero extra deps",
    liveMonitor:"📡 Live Monitor", multiple:"Multiple",
    bankroll:"Bankroll", totalPools:"Total Pools", entries:"Entries",
    winsLosses:"Wins / Losses", dominantTheme:"Dominant Theme",
    profitRate:"Profit Rate", openPositions:"Open Pos", deskState:"Desk State",
    equityTitle:"Equity (USD) · log scale · scroll window",
    positionsTitle:"Open Positions", historyTitle:"History Trades",
    ticker:"Ticker", entryTime:"Entry", entryUsd:"Buy $",
    peakMult:"Peak", currentMult:"Current", sizeFrac:"Size%",
    exitTime:"Exit", exitUsd:"Sell $", pnlMult:"P/L", pnlUsd:"Realized", why:"Why",
    scatterTitle:"Narrative cluster · animated",
    deskStatus:"Desk status", state:"state", theme:"theme",
    multiple:"multiple", enteredRejected:"entered / rejected", consecLosses:"consecutive losses",
    expectancy:"expectancy", kelly:"full / used Kelly",
    ruinProb:"ruin probability", marketSurvivors:"market survivors",
    currentOpen:"current open",
    feed:"Feed (last 20 events)", idle:"idle", running:"running", done:"done",
    noData:"No data", noOpen:"— no open positions —", noHistory:"— no closed trades yet —"
  },
  zh: {
    stepMs:"步进 ms", maxPos:"最大持仓", realistic:"真实模式",
    run:"运行", reset:"重置", hint:"SSE 实时流 · 零额外依赖",
    liveMonitor:"📡 实时监视器", multiple:"倍数",
    bankroll:"可用资金", totalPools:"池子总数", entries:"入场次数",
    winsLosses:"胜 / 负", dominantTheme:"主导主题",
    profitRate:"胜率", openPositions:"当前持仓", deskState:"运行状态",
    equityTitle:"净值曲线 (USD) · 对数刻度 · 滚动窗口",
    positionsTitle:"当前持仓", historyTitle:"历史交易",
    ticker:"币种", entryTime:"入场", entryUsd:"买入 $",
    peakMult:"峰值", currentMult:"当前", sizeFrac:"仓位%",
    exitTime:"出场", exitUsd:"卖出 $", pnlMult:"盈亏倍率", pnlUsd:"已实现", why:"原因",
    scatterTitle:"叙事聚类 · 粒子动画",
    deskStatus:"交易台状态", state:"状态", theme:"主题",
    multiple:"倍数", enteredRejected:"入场 / 拒绝", consecLosses:"连续亏损",
    expectancy:"期望值", kelly:"Kelly 满 / 实",
    ruinProb:"破产概率", marketSurvivors:"幸存者",
    currentOpen:"当前持仓详情",
    feed:"事件流 (最近 20 条)", idle:"待机", running:"运行中", done:"完成",
    noData:"无数据", noOpen:"— 暂无持仓 —", noHistory:"— 暂无历史交易 —"
  }
};
let lang = "en";
function applyLang() {
  document.querySelectorAll("[data-i18n]").forEach(el=>{
    const k = el.getAttribute("data-i18n");
    if(!I18N[lang][k]) return;
    const childEls = el.children.length;
    if(childEls === 0){
      el.textContent = I18N[lang][k];
    } else {
      const tn = Array.from(el.childNodes).find(n=>n.nodeType===Node.TEXT_NODE && n.textContent.trim());
      if(tn) tn.textContent = I18N[lang][k] + " ";
    }
  });
  document.getElementById("btnLangEN").classList.toggle("active", lang==="en");
  document.getElementById("btnLangZH").classList.toggle("active", lang==="zh");
}
document.getElementById("btnLangEN").addEventListener("click", ()=>{lang="en";applyLang()});
document.getElementById("btnLangZH").addEventListener("click", ()=>{lang="zh";applyLang()});

// === Core state ===
const $ = id => document.getElementById(id);
const eqCanvas=$("equity"), eqCtx=eqCanvas.getContext("2d");
const scCanvas=$("scatter"), scCtx=scCanvas.getContext("2d");
let currentT=0, eqW=[], eqV=[];
let scatterBase=null;
let animFrame=null;

// === Equity (auto-scroll log scale) ===
function drawEquity(){
  const W=eqCanvas.width, H=eqCanvas.height;
  eqCtx.fillStyle="#0d120e"; eqCtx.fillRect(0,0,W,H);
  if(eqW.length<2) return;

  // Auto-scroll: fixed-width window, latest point anchored at 75% of view
  const WINDOW_T_MIN = 200;
  const CURSOR_POS = 0.75;
  let xmin = eqW[eqW.length - 1] - WINDOW_T_MIN * CURSOR_POS;
  xmin = Math.max(0, xmin);
  let xmax = xmin + WINDOW_T_MIN;

  // Trim to visible window
  let iStart=0;
  while(iStart<eqW.length && eqW[iStart]<xmin) iStart++;
  const visT = eqW.slice(iStart);
  const visV = eqV.slice(iStart);
  if(visT.length<2) return;

  const start=500;
  const vmin=Math.max(30, Math.min(start*0.35, Math.min(...visV)*0.8));
  const vmax=Math.max(start*10, Math.max(...visV)*1.2);
  const padL=54, padR=14, padT=14, padB=26;
  const pw=W-padL-padR, ph=H-padT-padB;
  const logVmin=Math.log10(vmin), logVmax=Math.log10(vmax);
  const sx=t=>padL+pw*((t-xmin)/(xmax-xmin));
  const sy=v=>padT+ph*(1-(Math.log10(Math.max(vmin,v))-logVmin)/(logVmax-logVmin));

  // grid
  eqCtx.strokeStyle="#1a3a22"; eqCtx.lineWidth=0.5;
  eqCtx.font="10px ui-monospace,monospace"; eqCtx.fillStyle="#7ab080";
  for(let v=Math.pow(10,Math.ceil(logVmin)); v<=vmax; v*=10){
    const y=sy(v); eqCtx.beginPath(); eqCtx.moveTo(padL,y); eqCtx.lineTo(W-padR,y); eqCtx.stroke();
    eqCtx.fillText("$"+v.toLocaleString(),6,y+3);
  }
  // x ticks
  for(let t=Math.ceil(xmin/50)*50; t<=xmax; t+=50){
    const x=sx(t); eqCtx.beginPath(); eqCtx.moveTo(x,padT); eqCtx.lineTo(x,H-padB);
    eqCtx.strokeStyle="#153018"; eqCtx.stroke();
    eqCtx.fillText(t+"m",x-8,H-10);
  }
  // start line
  const sy_s=sy(start);
  eqCtx.strokeStyle="#555"; eqCtx.setLineDash([4,4]);
  eqCtx.beginPath(); eqCtx.moveTo(padL,sy_s); eqCtx.lineTo(W-padR,sy_s); eqCtx.stroke();
  eqCtx.setLineDash([]);

  // curve
  eqCtx.strokeStyle="#3dff8a"; eqCtx.lineWidth=2;
  eqCtx.beginPath();
  for(let i=0;i<visT.length;i++){
    const x=sx(visT[i]), y=sy(visV[i]);
    i===0?eqCtx.moveTo(x,y):eqCtx.lineTo(x,y);
  }
  eqCtx.stroke();

  // current marker
  const cx=sx(visT[visT.length-1]), cy=sy(visV[visV.length-1]);
  eqCtx.fillStyle="#3dff8a"; eqCtx.beginPath(); eqCtx.arc(cx,cy,4,0,Math.PI*2); eqCtx.fill();
  eqCtx.strokeStyle="#3dff8a80"; eqCtx.beginPath(); eqCtx.arc(cx,cy,9,0,Math.PI*2); eqCtx.stroke();
  eqCtx.fillStyle="#cde8d0"; eqCtx.fillText("$"+visV[visV.length-1].toFixed(2), cx+10, cy-4);
}

// === Scatter animation ===
function drawScatter(){
  if(!scatterBase || !scatterBase.points) return;
  const W=scCanvas.width, H=scCanvas.height;
  scCtx.fillStyle="#0d120e"; scCtx.fillRect(0,0,W,H);
  if(scatterBase.points.length===0) return;

  const pts=scatterBase.points;
  let xmin=Infinity, xmax=-Infinity, ymin=Infinity, ymax=-Infinity;
  pts.forEach(p=>{xmin=Math.min(xmin,p[0]);xmax=Math.max(xmax,p[0]);ymin=Math.min(ymin,p[1]);ymax=Math.max(ymax,p[1])});
  const xpad=(xmax-xmin)*0.15||0.2, ypad=(ymax-ymin)*0.15||0.2;
  xmin-=xpad; xmax+=xpad; ymin-=ypad; ymax+=ypad;
  const padL=10, padR=10, padT=10, padB=22;
  const pw=W-padL-padR, ph=H-padT-padB;
  const sx=x=>padL+pw*(x-xmin)/(xmax-xmin);
  const sy=y=>padT+ph*(1-(y-ymin)/(ymax-ymin));

  const t = performance.now()/1000;
  scCtx.lineWidth = 1;

  pts.forEach((p,i)=>{
    const phase = (i*0.37)%6.28;
    const jitterX = Math.sin(t*1.3+phase)*0.025*(xmax-xmin);
    const jitterY = Math.cos(t*1.1+phase*1.3)*0.025*(ymax-ymin);
    const x = sx(p[0]+jitterX), y = sy(p[1]+jitterY);

    const theme = scatterBase.tokens[i] && scatterBase.tokens[i].theme;
    let color;
    if(theme==="hood") color="#3dff8a";
    else if(theme==="pet") color="#8ab4ff";
    else if(theme==="ai-craze" || theme==="ai") color="#ffd93d";
    else if(theme==="political") color="#ff5b5b";
    else color="#cde8d0";

    const pulse = 0.6 + 0.4*Math.sin(t*2+phase);
    scCtx.fillStyle = color;
    scCtx.globalAlpha = 0.25*pulse;
    scCtx.beginPath(); scCtx.arc(x,y,8,0,Math.PI*2); scCtx.fill();
    scCtx.globalAlpha = 0.9;
    scCtx.beginPath(); scCtx.arc(x,y,4.5,0,Math.PI*2); scCtx.fill();
    scCtx.globalAlpha = 1;
  });

  // labels
  scCtx.fillStyle="#7ab080"; scCtx.font="9px ui-monospace,monospace";
  pts.forEach((p,i)=>{
    const x=sx(p[0])+7, y=sy(p[1])+3;
    scCtx.fillText(scatterBase.tokens[i].ticker, x, y);
  });

  if(scatterBase.centroid){
    const cx=sx(scatterBase.centroid[0]), cy=sy(scatterBase.centroid[1]);
    const pulse2 = 0.5+0.5*Math.sin(t*2.5);
    scCtx.strokeStyle = "#ffd93d30"; scCtx.lineWidth = 10+pulse2*6;
    scCtx.beginPath(); scCtx.moveTo(cx-12,cy); scCtx.lineTo(cx+12,cy);
    scCtx.moveTo(cx,cy-12); scCtx.lineTo(cx,cy+12); scCtx.stroke();
    scCtx.strokeStyle = "#ffd93d"; scCtx.lineWidth = 2.5;
    scCtx.beginPath(); scCtx.moveTo(cx-10,cy); scCtx.lineTo(cx+10,cy);
    scCtx.moveTo(cx,cy-10); scCtx.lineTo(cx,cy+10); scCtx.stroke();
    scCtx.fillStyle="#ffd93d"; scCtx.font="bold 10px ui-monospace,monospace";
    scCtx.fillText("CLUSTER", cx-22, cy-18);
  }

  // legend
  const themes = [
    {k:"hood", c:"#3dff8a"},{k:"pet", c:"#8ab4ff"},
    {k:"ai-craze", c:"#ffd93d"},{k:"political", c:"#ff5b5b"},
  ];
  let lx = W-155;
  scCtx.font = "9px ui-monospace,monospace";
  themes.forEach((t,idx)=>{
    const ly = padT+14+idx*14;
    scCtx.fillStyle=t.c;
    scCtx.beginPath(); scCtx.arc(lx, ly, 3.5, 0, Math.PI*2); scCtx.fill();
    scCtx.fillStyle="#7ab080";
    scCtx.fillText(t.k, lx+7, ly+3);
  });
}

// Performance-optimized animation loop
const RENDER_INTERVAL_EQ = 33;   // ~30fps for equity (more than enough)
const RENDER_INTERVAL_SC = 67;   // ~15fps for scatter particles (humans can't tell 60fps particles)
let _lastEqRender = 0, _lastScRender = 0;
let _pageHidden = false;
document.addEventListener('visibilitychange', () => {
  _pageHidden = document.hidden;
  if(!_pageHidden && !animFrame) animFrame = requestAnimationFrame(animLoop);
});

function animLoop(){
  if(_pageHidden){ animFrame=null; return; }   // ← ZERO cost when page hidden
  const now = performance.now();
  // Throttled draws
  if(now - _lastEqRender >= RENDER_INTERVAL_EQ){
    drawEquity(); _lastEqRender = now;
  }
  if(now - _lastScRender >= RENDER_INTERVAL_SC){
    drawScatter(); _lastScRender = now;
  }
  animFrame = requestAnimationFrame(animLoop);
}

// === Status table ===
function updateStatus(s){
  const snap=s.snap;
  $("st-state").textContent=snap.state;
  $("st-theme").textContent=snap.theme||"—";
  $("st-bankroll").textContent="$"+snap.bankroll.toFixed(2);
  $("st-mult").textContent=snap.multiple.toFixed(2)+"x";
  const open=(snap.open_positions && snap.open_positions[0]) || null;
  $("st-open").textContent=open?`${open.ticker} @ ${open.entry_min}m  peak=${open.peak_mult}x`:"—";
  $("st-er").textContent=snap.entered+" / "+snap.rejected;
  $("st-loss").textContent=(snap.state==="DRAWDOWN"||snap.state==="SELF_PAUSE")?snap.state:"0";
  $("st-exp").textContent=snap.expectancy.toFixed(3);
  $("st-kelly").textContent=snap.full_kelly.toFixed(3)+" / "+snap.used_kelly.toFixed(3);
  $("st-ruin").textContent=snap.ruin.toFixed(3);
  $("st-surv").textContent=Array.isArray(snap.market_survivors)?snap.market_survivors.length:snap.market_survivors;
}

// === Monitor panel (driven by SSE, no polling) ===
function updateMonitor(m){
  if(!m) return;
  $("mon-mult").textContent = m.multiple.toFixed(2) + "x";
  $("mon-bank").textContent = "$" + fmt(m.bankroll, 0);
  $("mon-pools").textContent = m.total_pools;
  $("mon-entry").textContent = m.entries;
  $("mon-wl").textContent = m.wins + " / " + m.losses;
  $("mon-theme").textContent = m.dominant_theme || "(none)";
  $("mon-profit").textContent = (m.profit_rate*100).toFixed(0) + "%";
  $("mon-open").textContent = m.open_count;
  $("mon-state").textContent = m.state;
  document.getElementById("mon-pill").className = "badge-dot live";
}

function fmt(v,d){if(v>=1e9)return(v/1e9).toFixed(d)+"B";if(v>=1e6)return(v/1e6).toFixed(d)+"M";if(v>=1e3)return(v/1e3).toFixed(d)+"K";return v.toFixed(d);}

// === Feed ===
function renderFeed(feed){
  const lines=feed.map(f=>{
    const cls=({ENTRY:"entry",EXIT:"entry",STOP:"stop",HALT:"halt",
                LEARN:"learn",RULE:"halt",REJECT:"reject",
                NOT_BUY:"notbuy"})[f.side]||"";
    return `<span class="${cls}">[${String(f.t).padStart(3,"0")}] ${f.side.padEnd(7)} ${String(f.ticker).padEnd(10)} ${String(f.mult).padStart(6)}x  ${f.note}</span>`;
  });
  $("feed-log").innerHTML=lines.join("\n") || `<span class="reject">— ${lang==="zh"?"无数据":"no data"} —</span>`;
}

// === Open Positions table ===
function renderOpenPositions(positions){
  const tbody=$("pos-body");
  if(!positions || positions.length===0){
    tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;color:#7ab080;padding:14px">— ${lang==="zh"?"暂无持仓":"no open positions"} —</td></tr>`;
    return;
  }
  let html="";
  positions.forEach(p=>{
    const now = p.current_mult || 1;
    const isWin = now >= 1.0;
    const nowColor = isWin ? 'var(--hi)' : 'var(--warn)';
    html += `<tr data-ticker="${p.ticker}">
      <td>${p.ticker}</td>
      <td>${p.entry_min}m</td>
      <td>$${p.entry_usd.toFixed(2)}</td>
      <td>${p.peak_mult.toFixed(2)}x</td>
      <td style="color:${nowColor}">${now.toFixed(2)}x</td>
      <td>${p.size_frac}%</td>
    </tr>`;
  });
  tbody.innerHTML = html;
}

// === History Trades table ===
function renderHistoryTrades(closed){
  const tbody=$("hist-body");
  if(!closed || closed.length===0){
    tbody.innerHTML = `<tr><td colspan="8" style="text-align:center;color:#7ab080;padding:14px">— ${lang==="zh"?"暂无历史交易":"no closed trades yet"} —</td></tr>`;
    return;
  }
  let html="";
  closed.forEach(c=>{
    const cls = c.win ? "win" : "loss";
    const sign = c.pnl_usd > 0 ? "+" : "";
    const pnlUsdStr = sign + "$" + c.pnl_usd.toFixed(2);
    html += `<tr class="${cls}">
      <td>${c.ticker}</td>
      <td>${c.entry_min}m</td>
      <td>$${c.entry_usd.toFixed(2)}</td>
      <td>${c.exit_min}m</td>
      <td>$${c.exit_usd.toFixed(2)}</td>
      <td class="pnl">${c.pnl_mult.toFixed(2)}x (${sign}${((c.pnl_mult-1)*100).toFixed(1)}%)</td>
      <td class="pnl">${pnlUsdStr}</td>
      <td style="color:#7ab080;font-size:10px">${(c.why||"").substring(0,40)}</td>
    </tr>`;
  });
  tbody.innerHTML = html;
}

// === SSE (throttled + incremental DOM) ===
let lastSSE=null, _sseLastRender=0, _sseThrottle=80;  // 80ms throttle → ~12Hz DOM updates
let _closedTradeRenderedCount = 0;
let _lastOpenHash = '';

function startRun(){
  if(lastSSE){try{lastSSE.close()}catch(e){}lastSSE=null;}
  eqW=[]; eqV=[]; scatterBase=null; currentT=0;
  _closedTradeRenderedCount = 0; _lastOpenHash = '';

  const seed=$("seed").value, lpn=$("lpn").value;
  const stepms=Math.max(0,$("stepms").value);
  const maxpos=$("maxpos").value;
  const lgproxy=$("lgproxy").checked?"1":"0";
  const realistic=$("realistic").checked?"1":"0";
  $("pill").textContent="● running"; $("pill").className="status-pill live";

  const ev=new EventSource(`/events?seed=${seed}&lpn=${lpn}&stepms=${stepms}&maxpos=${maxpos}&lgproxy=${lgproxy}&realistic=${realistic}`);
  lastSSE=ev;
  ev.onmessage=(e)=>{
    const s=JSON.parse(e.data);
    currentT=s.t;
    eqW=s.equity.map(r=>r[0]);
    eqV=s.equity.map(r=>r[1]);
    scatterBase={points:s.scatter.points, centroid:s.scatter.centroid, tokens:s.scatter.tokens};
    
    // Throttle DOM updates — expensive parts batch every _sseThrottle ms
    const now = performance.now();
    if(now - _sseLastRender >= _sseThrottle || s.done){
      _sseLastRender = now;
      updateStatus(s);
      updateMonitor(s.monitor);
      renderFeed(s.feed);
      renderOpenPositions(s.positions);
      renderHistoryTradesIncremental(s.closed_trades);
    }
    if(s.done){
      $("pill").textContent="● done  mult="+s.snap.multiple.toFixed(2)+"x";
      $("pill").className="status-pill done";
      ev.close(); lastSSE=null;
      // Stop rAF — no more data coming
      if(animFrame){cancelAnimationFrame(animFrame); animFrame=null;}
      drawEquity(); drawScatter();  // final render
    }
  };
  ev.onerror = () => {};
}

function resetView(){
  if(lastSSE){try{lastSSE.close()}catch(e){}lastSSE=null;}
  if(animFrame){cancelAnimationFrame(animFrame); animFrame=null;}
  eqW=[]; eqV=[]; scatterBase=null;
  _closedTradeRenderedCount = 0; _lastOpenHash = '';
  renderOpenPositions([]);
  renderHistoryTradesIncremental([]);
  drawEquity(); drawScatter();
  $("feed-log").innerHTML="";
  ["st-state","st-theme","st-bankroll","st-mult","st-er",
   "st-loss","st-exp","st-kelly","st-ruin","st-surv","st-open"].forEach(id=>{const el=$(id); if(el) el.textContent="—";});
  ["mon-mult","mon-bank","mon-pools","mon-entry","mon-wl",
   "mon-theme","mon-profit","mon-open","mon-state"].forEach(id=>{const el=$(id); if(el) el.textContent="—";});
  document.getElementById("mon-pill").className = "badge-dot";
  $("pill").textContent="● idle"; $("pill").className="status-pill";
}

// Incremental history trades render — only append new rows, O(1) per tick
function renderHistoryTradesIncremental(closed){
  if(!closed || closed.length === 0){
    if(_closedTradeRenderedCount > 0){
      $("hist-body").innerHTML = '';
      _closedTradeRenderedCount = 0;
    }
    return;
  }
  // capped at 50 by backend — do FULL rerender when near limit (simple & correct)
  if(closed.length <= 30 || Math.abs(closed.length - _closedTradeRenderedCount) > 5){
    // Full rerender (cheap — max 50 rows)
    renderHistoryTrades(closed);
    _closedTradeRenderedCount = closed.length;
  } else {
    // Incremental append
    const tbody = $("hist-body");
    const newOnes = closed.slice(_closedTradeRenderedCount);
    for(const c of newOnes){
      const cls = c.win ? "win" : "loss";
      const sign = c.pnl_usd > 0 ? "+" : "";
      const pnlUsdStr = sign + "$" + c.pnl_usd.toFixed(2);
      const tr = document.createElement("tr");
      tr.className = cls;
      tr.innerHTML = `<td>${c.ticker}</td>
        <td>${c.entry_min}m</td>
        <td>$${c.entry_usd.toFixed(2)}</td>
        <td>${c.exit_min}m</td>
        <td>$${c.exit_usd.toFixed(2)}</td>
        <td class="pnl">${c.pnl_mult.toFixed(2)}x (${sign}${((c.pnl_mult-1)*100).toFixed(1)}%)</td>
        <td class="pnl">${pnlUsdStr}</td>
        <td style="color:#7ab080;font-size:10px">${(c.why||"").substring(0,40)}</td>`;
      tbody.prepend(tr);  // newest on top
    }
    _closedTradeRenderedCount = closed.length;
  }
}

// Init (rAF starts here)
applyLang();
animLoop();
window.addEventListener("load", ()=>{setTimeout(startRun,200);});
// Init
applyLang();
animLoop();
window.addEventListener("load", ()=>{setTimeout(startRun,200);});
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    """Serve / for static HTML, /events for SSE stream."""

    def log_message(self, fmt, *args):
        return

    def _send(self, code: int, content: bytes, ctype: str = "text/html"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            self._send(200, INDEX_HTML.encode("utf-8"), "text/html")
        elif u.path == "/favicon.ico":
            self._send(204, b"")
        elif u.path == "/events":
            self._handle_sse(u)
        elif u.path == "/api/stats":
            self._handle_stats()
        else:
            self._send(404, b"not found", "text/plain")

    def _handle_stats(self):
        import json as _json
        stats_path = SERVER_DEFAULTS.get("stats_json")
        if stats_path and Path(stats_path).exists():
            try:
                data = _json.loads(Path(stats_path).read_text())
                self._send(200, _json.dumps(data).encode("utf-8"), "application/json")
                return
            except Exception:
                pass
        self._send(200, b'{"error":"no stats file"}', "application/json")

    def _handle_sse(self, u):
        q = parse_qs(u.query)
        seed = int(q.get("seed", ["2"])[0])
        lpn = int(q.get("lpn", ["3"])[0])
        step_ms = int(q.get("stepms", ["180"])[0])
        from_csv = q.get("csv", [None])[0] or SERVER_DEFAULTS.get("csv")
        max_pos = int(q.get("maxpos", ["3"])[0])
        thin_cut = q.get("thincut", [None])[0]
        thin_cut = float(thin_cut) if thin_cut is not None else None
        live_mode = q.get("lgproxy", ["0"])[0] in ("1", "true", "True")
        realistic = q.get("realistic", ["0"])[0] in ("1", "true", "True")
        if from_csv and not q.get("realistic") and not q.get("norealistic"):
            realistic = True
        if q.get("norealistic"):
            realistic = False
        peak_coef = float(q.get("peakcoef", ["0.6"])[0])
        slip_pct = float(q.get("slip", ["0.02"])[0])

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        try:
            if live_mode and _live_queue is not None:
                # Live mode: read states pushed by TickEngine.run_tick()
                self.wfile.write(b"event: ping\ndata: \n\n")
                self.wfile.flush()
                while True:
                    try:
                        state = _live_queue.get(timeout=30)
                        payload = json.dumps(state, default=str)
                        self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                        self.wfile.flush()
                    except queue.Empty:
                        # Keep-alive ping so client doesn't timeout
                        self.wfile.write(b"event: ping\ndata: \n\n")
                        self.wfile.flush()
            else:
                for state in step_replay(seed=seed, loss_pause_n=lpn, from_csv=from_csv,
                                         max_positions=max_pos, thin_cut=thin_cut,
                                         live_mode=live_mode, realistic=realistic,
                                         max_slippage_pct=slip_pct, peak_proxy_coef=peak_coef):
                    payload = json.dumps(state, default=str)
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                    if step_ms > 0:
                        time.sleep(step_ms / 1000.0)
                    if state["done"]:
                        break
        except (BrokenPipeError, ConnectionResetError):
            pass


SERVER_DEFAULTS: dict = {}


def main(host: str = "127.0.0.1", port: int = 8765, csv_path: str | None = None,
         stats_json: str | None = None, live_mode: bool = False):
    srv = ThreadingHTTPServer((host, port), Handler)
    SERVER_DEFAULTS["csv"] = csv_path
    SERVER_DEFAULTS["stats_json"] = stats_json
    if live_mode:
        start_live_queue(maxsize=30)
        print(f"[rh_server] LIVE MODE — accepting push_state() from TickEngine")
    else:
        print(f"[rh_server] REPLAY MODE — default CSV: {csv_path or 'scenario'}")
    print(f"[rh_server] dashboard on http://{host}:{port}")
    if stats_json:
        print(f"[rh_server] live stats JSON: {stats_json}")
    print("[rh_server] Ctrl+C to stop")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[rh_server] stopped")
        srv.shutdown()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--csv", default=None,
                    help="Default CSV path for /events?csv=<path> (auto-used by INDEX_HTML)")
    ap.add_argument("--stats", default=None,
                    help="Path to rh_monitor JSON stats file (enables /api/stats + Live Monitor panel)")
    args = ap.parse_args()
    main(args.host, args.port, args.csv, args.stats)
