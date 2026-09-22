#!/usr/bin/env python3
"""DeskRunner — background thread that drives Desk tick loop → auto ENTRY/EXIT → LiveTrader swaps."""
from __future__ import annotations
import json
import os
import queue
import threading
import time
from pathlib import Path
from typing import Any
import numpy as np
from rh_trencher import Desk, Fill, TokenLaunch
from db_trades import TradeDB, DEFAULT_TRADE_USD


def okx_position_open_seconds(pos: dict) -> int:
    """Position open time from an OKX ``/account/positions`` payload, UNIX seconds.

    OKX reports ``cTime`` (position created) and ``uTime`` (last adjusted) as
    MILLISECOND values. The original code read a ``cups`` key, which OKX never
    returns, so ``entry_time`` stayed 0 for every position and the max-hold time
    stop could never arm. 0 means "unknown" and makes the caller skip the time
    stop rather than guess.
    """
    for key in ("cTime", "uTime", "pTime", "ts"):
        raw = pos.get(key)
        if not raw:
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if val > 1e11:              # milliseconds -> seconds
            val /= 1000.0
        if val > 0:
            return int(val)
    return 0


class DeskRunner:
    """后台线程驱动 Desk tick loop.

    完整链路:
      CSV tokens → events queue → DeskRunner tick loop
        → Desk.on_launch() → Desk.log(Fill) → signal_callback → LiveTrader.on_desk_fill → pending_swaps
        → Desk.mark_and_maybe_exit() → Desk.log(Fill) → signal_callback → LiveTrader.on_desk_fill → pending_swaps
        → 每 tick push 完整 state → SSE → 前端

    OKX 模式下，真实账户数据按 OKX_ACCOUNT_REFRESH_SEC（默认 30s）节奏拉取一次，
    tick loop 内只消费缓存，避免每 400ms 打爆 OKX API 导致 okx_connected 抖动。
    """

    def __init__(self, server: "ServerState", tick_ms: int = 400,
                 max_positions: int = 3, seed: int | None = 7,
                 queue: queue.Queue | None = None,
                 refresh_interval_min: float | None = None,
                 okx_live_data: bool = False):
        self.server = server
        self.tick_ms = tick_ms
        self._running = threading.Event()
        self._thread: threading.Thread | None = None
        self._tick_count = 0
        self._refresh_interval_min = refresh_interval_min
        self._last_fetch_ts: float = 0  # 0 = always refresh on first loop reset
        self._okx_live_data = okx_live_data
        self._last_auto_submit_ts: float = 0  # track last auto-submit time

        # ── OKX ACCOUNT CACHE: stabilize SSE payload so front-end doesn't flash ──
        # P0 修复：持仓缓存 30s 意味着开仓防重检查有 30s 盲区（同标的可连开）。
        # 合约侧无需 30s 级精度，降到 10s；API 调用量仍远低于 OKX 限速。
        self._okx_account_refresh_sec = float(os.environ.get("OKX_ACCOUNT_REFRESH_SEC", "10"))
        self._okx_last_account_ts: float = 0.0
        self._okx_cached_balance: dict | None = None
        self._okx_cached_positions: list[dict] = []
        self._okx_cached_trades: list[dict] = []
        # Realized round trips derived from OKX fills (FIFO). This — not the desk
        # replay ledger — backs the Trade History page while signal_mode == "real".
        self._okx_cached_round_trips: list[dict] = []
        self._okx_executor_ref: object | None = None   # 统一引用，避免 executor / trader.executor 不同步
        self._okx_mode_locked: bool = False             # 一旦 OKX auth 成功，锁死模式不再回落到 demo
        # OKX PnL 基准：首次成功取到账户权益时锁定，避免用写死的 500 (DEMO) 做基准
        self._okx_start_equity: float | None = None
        # Final source-of-truth platform tag (OKX / JUP), 启动时冻结
        self._platform_tag: str = self._detect_platform_tag()

        # ── OKX PERP AUTO-EXIT CONFIG ──
        # P1 修复：原 TP=3%/SL=1.5% 在随机游走下恰为零期望（胜率 33% → EV=0，
        # 扣费必负）。改为 TP=6%/SL=1%，盈亏比 6:1，breakeven 胜率降至 14.3%，
        # 扣费后才有正期望空间。
        self._perp_tp_pct: float = float(os.environ.get("OKX_PERP_TP_PCT", "6.0"))
        self._perp_sl_pct: float = float(os.environ.get("OKX_PERP_SL_PCT", "1.0"))
        # ── ATR-ADAPTIVE SL/TP (方案 B) ──
        # 固定百分比止损在高波动币上会落进噪音带（1% ≈ 0.3–0.7×ATR），
        # 被随机波动反复扫出 → 高频止损 → 手续费/滑点持续磨损。
        # 改为按入场时 15m ATR% 归一化：
        #   SL% = clamp(k_sl × ATR%, sl_floor, sl_cap)
        #   TP% = clamp(R   × SL%,  tp_floor, tp_cap)
        # atr_pct 由 rh_okx_data.evaluate_signal 计算并写入 TokenLaunch.atr_pct。
        # ATR 未知（< atr_use_min）时回退到固定 _perp_tp_pct / _perp_sl_pct。
        self._atr_adaptive: bool = os.environ.get(
            "OKX_ATR_ADAPTIVE", "1").lower() not in ("0", "false", "no")
        self._atr_k_sl: float = float(os.environ.get("OKX_ATR_K_SL", "1.5"))
        self._atr_r: float = float(os.environ.get("OKX_ATR_R", "3.0"))
        self._atr_sl_floor: float = float(os.environ.get("OKX_ATR_SL_FLOOR", "0.8"))
        self._atr_sl_cap: float = float(os.environ.get("OKX_ATR_SL_CAP", "4.0"))
        self._atr_tp_floor: float = float(os.environ.get("OKX_ATR_TP_FLOOR", "2.0"))
        self._atr_tp_cap: float = float(os.environ.get("OKX_ATR_TP_CAP", "12.0"))
        self._atr_use_min: float = float(os.environ.get("OKX_ATR_USE_MIN", "0.05"))
        # ── PER-TRADE AUTO-EXEC NOTIONAL ──
        # Single source of truth for "how much each auto trade buys", resolved in
        # _reload_perp_settings() from: ops DB settings.trade_usd → env
        # OKX_SPOT_ORDER_USD / OKX_PERP_ORDER_USD → DEFAULT_TRADE_USD (50).
        self._perp_trade_usd: float = float(DEFAULT_TRADE_USD)
        # inst_id → (sl_pct, tp_pct) / atr%：开仓时冻结，平仓时按 inst_id 取用
        self._perp_sl_tp: dict[str, tuple[float, float]] = {}
        self._perp_atr: dict[str, float] = {}
        # Time stop: a position held longer than this is force-closed. Default
        # 12h — at 30 minutes the clock, not the edge, was deciding exits long
        # before a +6% take-profit target could realistically be reached.
        self._perp_max_hold_sec: int = int(os.environ.get("OKX_PERP_MAX_HOLD_SEC", "43200"))
        # P0 修复：入场节流。原 30s cooldown 被 allow_reopen=True 完全绕过
        # （冷却形同虚设），且 400ms tick 下同标的每轮 replay 都会重新满足入场条件。
        # 统一硬冷却：任一 ticker 自上次平仓/开仓失败起 120s 内禁止再入场。
        self._perp_entry_cooldown_sec: int = int(os.environ.get("OKX_PERP_ENTRY_COOLDOWN_SEC", "120"))
        self._perp_last_exit: dict[str, float] = {}     # ticker → timestamp of last exit
        self._perp_last_entry: dict[str, float] = {}    # ticker → timestamp of last entry attempt
        self._perp_close_log: list[dict] = []           # recent closes for UI display
        # Delisted / non-existent SWAP instruments (51001 / 51087) — never retry these
        self._delisted_insts: set[str] = set()
        self._perp_allow_reopen: bool = True             # default: allow re-entry
        self._perp_close_cooldown_sec: int = 90          # cooldown after ANY close attempt (win or fail)
        self._perp_max_position_usd: float = 1000.0      # default max position size
        self._perp_min_trade_usd: float = 0.0            # min notional per entry (0 = OKX's floor)
        self._trade_mode: str = "signal_only"            # "signal_only" or "auto"
        self._signal_mode: str = "real"                  # "real" (OKX candles) or "legacy"
        self._mode_changed: threading.Event = threading.Event()  # signalled when trade_mode is updated from UI

        # Load tokens
        if okx_live_data:
            from rh_okx_data import OKXDataSource
            executor = getattr(server, "okx_executor", None) or server.executor
            if executor is not None:
                data_source = OKXDataSource(executor)
                self.raw_tokens = data_source.fetch_tokens(top_n=30)
                self.source = f"OKX-{len(self.raw_tokens)}"
                print(f"[DeskRunner] OKX live data mode: {len(self.raw_tokens)} tokens, source={self.source}")
                if self.raw_tokens:
                    sample = [(t.ticker, t.name) for t in self.raw_tokens[:5]]
                    print(f"[DeskRunner] Sample tokens: {sample}")
                # Populate ticker_inst_map immediately so LiveTrader can resolve all OKX tickers
                ticker_map = data_source.get_ticker_map()
                if ticker_map:
                    self.server.trader.ticker_inst_map.update(ticker_map)
                    self.server.ticker_mint_map.update(ticker_map)
                    print(f"[DeskRunner] OKX ticker_inst_map loaded: {len(ticker_map)} entries")
            else:
                raise RuntimeError("OKX live data mode requires OKX executor")
            # P0 修复: OKX 实盘模式无需 400ms 级 tick（真实市场数据 10s 才刷一次，
            # 入场/平仓已有 90-120s 硬冷却），高速空转只会放大 churn。
            # 下限 2000ms，保持 UI SSE 每 2s 一帧。
            self.tick_ms = max(self.tick_ms, 2000)
            print(f"[DeskRunner] OKX mode tick clamped to {self.tick_ms}ms")
        elif server.csv_path:
            from fetch_dexscreener import load_csv_as_tokenlaunches
            self.raw_tokens = load_csv_as_tokenlaunches(server.csv_path)
            self.source = "CSV"
        else:
            from rh_trencher import scenario
            rng = np.random.default_rng(seed if seed is not None else 7)
            self.raw_tokens = scenario(rng)
            self.source = "scenario"
        print(f"[DeskRunner] loaded {len(self.raw_tokens)} tokens from {self.source}")

        self.events = sorted([(tok.t_min, tok) for tok in self.raw_tokens], key=lambda x: x[0])
        self.pending_marks: list[tuple[int, TokenLaunch, float]] = []
        self.i = 0
        self.desk = server.desk
        # 方案 C：OKX 模式下只保留合约交易路径（perp-only）。
        # 合约开/平仓由 _auto_exec_entry / _check_and_close_perp_positions 直接打到
        # "-USDT-SWAP"（submit_market / close_swap），不依赖信号回调。而
        # server.desk.signal_callback → trader.on_desk_fill → _okx_build 会把同一信号解析成
        # 现货单（如 "SOL-USDT"）并反复提交（正是手续费流失的碎单来源）。
        # 故 OKX 模式下把 Desk 信号回调置为 no-op，从源头掐断自动现货下单。
        self._perp_only: bool = self._platform_tag == "OKX"

        def _noop_signal_cb(fill):  # noqa: F841
            return None

        if self._perp_only:
            self._desk_callback = _noop_signal_cb
            # 初始 Desk（server.desk）自带 trader.on_desk_fill，第一轮 replay 就是用它跑的，
            # 必须一并摘掉，否则首轮仍会下现货单。
            self.desk.signal_callback = None
        else:
            self._desk_callback = server.desk.signal_callback
        self._desk_start = server.desk.start
        self._desk_max_pos = server.desk.max_positions
        self._queue = queue
        # Also keep latest state snapshot on server for polling fallback
        self._latest_state: dict | None = None
        if self._queue is not None:
            print(f"[DeskRunner] queue wired at construction: {id(self._queue)}")
        else:
            print("[DeskRunner] WARNING — no queue passed at construction!")

        self.db = TradeDB(str(Path(__file__).parent / "trades.db"))
        print(f"[DeskRunner] DB initialized: {self.db}")

        # Load perp auto-close settings from DB (override env vars if present)
        self._reload_perp_settings()
        print(f"[DeskRunner] After reload: trade_mode={self._trade_mode}")

        self.closed_trades: list[dict] = []
        self.open_trades: dict[str, dict] = {}
        self.scatter_tokens = []
        seen = set()
        for t in self.raw_tokens:
            if t.ticker not in seen:
                self.scatter_tokens.append(t)
                seen.add(t.ticker)

    def _refresh_live_tokens(self) -> None:
        """Fetch fresh tokens from configured data source.

        In OKX live data mode, refreshes from OKX public API.
        Otherwise falls back to DexScreener (legacy behavior).
        """
        if self._okx_live_data:
            self._refresh_okx_tokens()
        else:
            self._refresh_dexscreener_tokens()

    def _refresh_okx_tokens(self) -> None:
        """Refresh tokens from OKX public market API."""
        try:
            from rh_okx_data import OKXDataSource
            executor = getattr(self.server, "okx_executor", None) or self.server.executor
            if executor is None:
                print("[DeskRunner] OKX executor not available for refresh")
                return
            data_source = OKXDataSource(executor)
            new_tokens = data_source.fetch_tokens(top_n=30, force_refresh=True)
            if len(new_tokens) < 3:
                print("[DeskRunner] OKX refresh: too few tokens, keeping old")
                return
            self.raw_tokens = new_tokens
            self.events = sorted([(tok.t_min, tok) for tok in new_tokens], key=lambda x: x[0])
            self.scatter_tokens = list(new_tokens)
            # Update ticker map for LiveTrader
            ticker_map = data_source.get_ticker_map()
            if ticker_map:
                before = len(self.server.trader.ticker_inst_map)
                self.server.ticker_mint_map.update(ticker_map)
                self.server.trader.ticker_inst_map.update(ticker_map)
                after = len(self.server.trader.ticker_inst_map)
                print(f"[DeskRunner] OKX ticker_inst_map: {before} -> {after} entries")
            self._last_fetch_ts = time.time()
            print(f"[DeskRunner] OKX REFRESH OK — {len(new_tokens)} tokens, {len(ticker_map)} instIds mapped")
        except Exception as e:
            print(f"[DeskRunner] OKX refresh FAILED: {e}")
            import traceback
            traceback.print_exc()

    def _refresh_dexscreener_tokens(self) -> None:
        """Legacy: refresh tokens from DexScreener (Solana DEX).

        Uses HTTP(S)_PROXY env if set (local dev). On production the
        environment is not expected to have a local Clash, so fall back to
        a direct opener when no proxy is configured.
        """
        try:
            import os
            import urllib.request

            # Build proxy handler from env (empty → direct connect, no global side-effect)
            hp = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
            sp = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
            if hp or sp:
                proxy = urllib.request.ProxyHandler({
                    "http": hp or sp,
                    "https": sp or hp,
                })
                opener = urllib.request.build_opener(proxy)
                urllib.request.install_opener(opener)
            else:
                # Direct connection — avoid global install_opener side-effect
                # on a server without a local proxy.
                _direct_proxy = urllib.request.ProxyHandler({})
                urllib.request.install_opener(urllib.request.build_opener(_direct_proxy))

            import fetch_dex_top
            trending = self._fetch_with_retry(fetch_dex_top.fetch_trending, "solana", 20)
            vol = self._fetch_with_retry(fetch_dex_top.fetch_top_volume, "solana", 15)
            if not trending and not vol:
                print("[DeskRunner] refresh: both fetch failed, keeping old tokens")
                return
            print(f"[DeskRunner] fetch: {len(trending)} trending + {len(vol)} vol")

            raw_pairs = trending + vol
            seen_addr = set()
            pairs_dedup = []
            for p in raw_pairs:
                addr = p.get("pairAddress")
                if addr and addr not in seen_addr:
                    seen_addr.add(addr)
                    pairs_dedup.append(p)

            from fetch_dex_top import pair_to_row
            from rh_trencher import TokenLaunch

            new_tokens: list[TokenLaunch] = []
            mint_updates: dict[str, str] = {}
            ticker_seen: set[str] = set()
            for i, p in enumerate(pairs_dedup):
                row = pair_to_row(p, t_min=(i + 1) * 5)
                bt = p.get("baseToken") or {}
                token_addr = bt.get("address", "")
                ticker = (row.get("ticker") or "?").strip().upper()
                if ticker in ticker_seen:
                    continue
                ticker_seen.add(ticker)
                path_str = row.get("true_multiple_path", "[]")
                try:
                    path = json.loads(path_str)
                except Exception:
                    path = []
                tok = TokenLaunch(
                    t_min=int(row.get("t_min", i * 5)),
                    ticker=ticker,
                    name=row.get("name", ticker),
                    description=row.get("description", "") or "",
                    launchpad=row.get("launchpad", "dexscreener"),
                    liquidity_eth=float(row.get("liquidity_eth", 0)),
                    liq_growth=float(row.get("liq_growth", 1.0)),
                    deployer=row.get("deployer", "") or "",
                    holders=[],
                    linked_groups=[],
                    selling_linked=int(row.get("selling_linked", 0)),
                    true_multiple_path=path,
                    theme_hint=row.get("theme_hint", ""),
                )
                new_tokens.append(tok)
                if token_addr:
                    mint_updates[ticker] = token_addr

            if len(new_tokens) < 3:
                print("[DeskRunner] refresh: too few tokens, skipping")
                return

            self.raw_tokens = new_tokens
            self.events = sorted([(tok.t_min, tok) for tok in new_tokens], key=lambda x: x[0])
            self.scatter_tokens = list(new_tokens)

            import json as _json
            mm_path = Path(__file__).parent / "ticker_mint_map.json"
            existing = {}
            if mm_path.exists():
                try:
                    existing = _json.loads(mm_path.read_text())
                except Exception:
                    pass
            existing.update(mint_updates)
            mm_path.write_text(_json.dumps(existing, indent=2))
            self.server.ticker_mint_map = existing
            self.server.trader.ticker_mint_map = existing

            self._last_fetch_ts = time.time()
            print(f"[DeskRunner] REFRESH OK — {len(new_tokens)} tokens, {len(mint_updates)} mints saved")

        except Exception as e:
            print(f"[DeskRunner] refresh FAILED: {e}")
            import traceback
            traceback.print_exc()

    def _fetch_with_retry(self, func, *args, max_retries: int = 3, **kwargs) -> list:
        """Call a fetch function with retries, return [] on total failure."""
        for attempt in range(1, max_retries + 1):
            try:
                result = func(*args, **kwargs)
                if result:
                    return result
            except Exception as e:
                print(f"[DeskRunner] fetch retry {attempt}/{max_retries}: {e}")
                time.sleep(1.0 * attempt)
        return []

    def _schedule_marks(self, tok: TokenLaunch, entry_t: int) -> None:
        for j, m in enumerate(tok.true_multiple_path or []):
            self.pending_marks.append((entry_t + 1 + j * 2, tok, m))

    def _detect_platform_tag(self) -> str:
        """Determine platform ONCE at startup.

        Priority: OKX_LIVE_MODE env → server.trader.executor → server.executor → JUP fallback.
        """
        if os.environ.get("OKX_LIVE_MODE", "").lower() in ("1", "true", "yes"):
            return "OKX"
        # Prefer trader.executor (the one actually used for orders)
        try:
            trader_exe = getattr(getattr(self.server, "trader", None), "executor", None)
            if trader_exe and trader_exe.__class__.__name__.startswith("OKX") and getattr(trader_exe, "_auth_ready", False):
                return "OKX"
        except Exception:
            pass
        try:
            exe = getattr(self.server, "executor", None)
            if exe and exe.__class__.__name__.startswith("OKX") and getattr(exe, "_auth_ready", False):
                return "OKX"
        except Exception:
            pass
        return "JUP"

    def _resolve_okx_executor(self):
        """Return OKXExecutor instance or None.

        Uses the unified reference stored once at startup, avoiding the
        server.executor vs server.trader.executor split that caused flashing.
        """
        if self._okx_executor_ref is not None:
            return self._okx_executor_ref
        # First time — try all possible sources, cache the first auth-ready one
        candidates = [
            getattr(getattr(self.server, "trader", None), "executor", None),
            getattr(self.server, "executor", None),
            getattr(self.server, "okx_executor", None),
        ]
        for exe in candidates:
            if exe is None:
                continue
            name = exe.__class__.__name__
            if name.startswith("OKX") and getattr(exe, "_auth_ready", False):
                from rh_okx_executor import OKXExecutor
                if isinstance(exe, OKXExecutor):
                    self._okx_executor_ref = exe
                    self._okx_mode_locked = True
                    print(f"[DeskRunner] OKX executor LOCKED: {name} (auth_ready=True)")
                    return exe
        return None

    def _refresh_okx_account_cache(self, force: bool = False) -> bool:
        """Pull OKX account snapshot and refresh the cache.

        Returns True if fresh data was fetched, False if skipped/throttled.
        Throttled to OKX_ACCOUNT_REFRESH_SEC (default 30s) between pulls.
        """
        now = time.time()
        if not force and (now - self._okx_last_account_ts) < self._okx_account_refresh_sec:
            return False

        exe = self._resolve_okx_executor()
        if exe is None:
            # Keep existing cache if any — DO NOT reset to None on transient failure
            return False
        try:
            raw_positions = exe.get_positions()
            summary = exe.get_account_summary(perp_positions=raw_positions)
            total_eq_now = summary["total_eq_usd"]
            self._okx_cached_balance = {
                "total_eq_usd": total_eq_now,
                "usdt_avail": summary["usdt_avail"],
                "usdt_eq": summary["usdt_eq"],
                "holdings": summary["holdings"],
                "perp_upl": summary["perp_upl"],
            }
            # Lock PnL baseline on first successful account fetch (not hardcoded 500)
            if self._okx_start_equity is None and total_eq_now > 0:
                self._okx_start_equity = total_eq_now
                print(f"[DeskRunner] OKX PnL baseline locked: ${total_eq_now:.2f}")
            perp: list[dict] = []
            for pos in raw_positions:
                inst_id = pos.get("instId", "")
                if "-SWAP" not in inst_id:
                    continue
                pos_size = float(pos.get("pos", 0) or 0)
                if abs(pos_size) < 0.0001:
                    continue
                avg_px = float(pos.get("avgPx", 0) or 0)
                last_px = float(pos.get("last", 0) or pos.get("markPx", 0) or avg_px or 1)
                upl = float(pos.get("upl", 0) or 0)
                inst = inst_id.replace("-USDT-SWAP", "")
                side = "LONG" if pos_size > 0 else "SHORT"
                # OKX reports SWAP `pos` in CONTRACTS (张), not base coins.
                # notional = contracts × ctVal × price. Omitting ctVal understated
                # NEAR (ctVal=10) by 10x — UI showed $20 for a real $202 position.
                ct_val = 1.0
                get_ct = getattr(exe, "_get_ct_val", None)
                if callable(get_ct):
                    try:
                        ct_val = get_ct(inst_id) or 1.0
                    except Exception:
                        ct_val = 1.0
                notional = abs(pos_size) * ct_val * last_px
                # Position open time — OKX reports cTime/uTime in ms. There is no
                # "cups" key, which is why entry_time used to be 0 for every
                # position and the max-hold time stop never fired.
                entry_time = okx_position_open_seconds(pos)
                # PnL% = upl / margin (margin = notional / leverage)
                lever = float(pos.get("lever", 0) or 0) or 1.0
                margin = notional / lever if lever else 0
                pnl_pct = (upl / margin * 100) if margin > 0 else 0.0
                perp.append({
                    "inst_id": inst_id,
                    "ticker": inst,
                    "side": side,
                    "size": pos_size,
                    "size_usd": round(notional, 2),
                    "avg_px": avg_px,
                    "last_px": last_px,
                    "mark_px": float(pos.get("markPx", 0) or 0),
                    "upl": round(upl, 2),
                    "lever": pos.get("lever", "?"),
                    "margin_usd": round(margin, 2) if margin > 0 else 0,
                    "pnl_pct": round(pnl_pct, 4),
                    "entry_time": entry_time,
                    "ts": int(pos.get("ts", 0) or 0),
                    # OKX posSide: "long" / "short" (hedge mode) or "net" (net mode).
                    # Only forward it when hedge mode — "net" must NOT be sent.
                    "pos_side": pos.get("posSide", "net"),
                })
            perp.sort(key=lambda x: abs(x["size_usd"]), reverse=True)
            self._okx_cached_positions = perp
            # Prune frozen ATR thresholds for instruments no longer held
            # (closed manually on OKX, liquidated, or expired out-of-band).
            live_insts = {p["inst_id"] for p in perp}
            for _k in list(self._perp_sl_tp.keys()):
                if _k not in live_insts:
                    self._perp_sl_tp.pop(_k, None)
                    self._perp_atr.pop(_k, None)

            try:
                self._okx_cached_trades = exe.get_order_history(limit=50)
            except Exception as he:
                print(f"[DeskRunner] OKX order history fetch failed (kept cached): {he}")

            # Realized round trips (FIFO-paired fills) — the Trade History page
            # source in OKX mode. Pure derivation from the exchange, no local
            # bookkeeping, so it can never drift from the account.
            get_rt = getattr(exe, "get_round_trips", None)
            if callable(get_rt):
                try:
                    self._okx_cached_round_trips = get_rt(limit=100)
                except Exception as rte:
                    print(f"[DeskRunner] OKX round-trip build failed (kept cached): {rte}")

            self._okx_last_account_ts = now
            self._okx_mode_locked = True
            print(f"[DeskRunner] OKX account CACHED: eq=${self._okx_cached_balance['total_eq_usd']:.2f} "
                  f"perp={len(self._okx_cached_positions)} trades={len(self._okx_cached_trades)} "
                  f"(next refresh in {self._okx_account_refresh_sec:.0f}s)")
            return True
        except Exception as e:
            # Transient OKX API failure — KEEP existing cache, don't let UI flash back to demo
            print(f"[DeskRunner] OKX account fetch FAILED (kept cached): {e}")
            return False

    def start(self) -> None:
        if self._running.is_set():
            return
        # ── Platform banner ──
        src = "OKX LIVE" if self._platform_tag == "OKX" else "DEMO / JUPITER"
        locked = "(LOCKED)" if self._platform_tag == "OKX" else ""
        print(f"\n{'='*60}")
        print(f"[DeskRunner] DATA SOURCE: {src} {locked}")
        print(f"[DeskRunner] OKX_ACCOUNT_REFRESH_SEC = {self._okx_account_refresh_sec}")
        print(f"{'='*60}\n")
        # ── Pre-warm OKX account cache on startup ──
        if self._platform_tag == "OKX":
            for attempt in range(3):
                ok = self._refresh_okx_account_cache(force=True)
                if ok:
                    break
                print(f"[DeskRunner] OKX pre-warm retry {attempt+1}/3 in 2s...")
                time.sleep(2)
        self._running.set()
        self._thread = threading.Thread(target=self._run_loop, name="DeskRunner", daemon=True)
        self._thread.start()
        print(f"[DeskRunner] STARTED tick_ms={self.tick_ms}")

    def stop(self) -> None:
        self._running.clear()
        if self._thread:
            self._thread.join(timeout=2)
            self._thread = None
        print(f"[DeskRunner] STOPPED after {self._tick_count} ticks")

    def _run_loop(self) -> None:
        while self._running.is_set():
            # ── Inner tick loop: one full CSV replay ──
            while self._running.is_set() and (self.i < len(self.events) or self.pending_marks):
                try:
                    self._do_one_tick()
                    # Periodic OKX account cache refresh (every ~30s, NOT every tick)
                    if self._platform_tag == "OKX":
                        self._refresh_okx_account_cache(force=False)
                        self._check_and_close_perp_positions()
                        # P2: real-candle entry scan (no-op in legacy mode)
                        if self._signal_mode == "real":
                            self._scan_real_signals()
                        self._housekeep_swaps()
                        self._auto_submit_pending_swaps()
                    self._push_current_state()
                    self._tick_count += 1
                except Exception as e:
                    print(f"[DeskRunner] ERROR at tick {self._tick_count}: {e}")
                    import traceback; traceback.print_exc()
                time.sleep(self.tick_ms / 1000.0)
                # Check if trade_mode was updated from UI (non-blocking)
                if self._mode_changed.is_set():
                    self._mode_changed.clear()
                    self._reload_perp_settings()
                    print(f"[DeskRunner] trade_mode updated → {self._trade_mode}")

            # Close any remaining open positions
            self._close_remaining()
            if self._platform_tag == "OKX":
                self._refresh_okx_account_cache(force=True)
            self._push_current_state()
            print(f"[DeskRunner] replay done — {self._tick_count} ticks, {len(self.closed_trades)} closed")

            # ── Loop reset: rebuild Desk, reset pointers ──
            if not self._running.is_set():
                break
            time.sleep(3)  # 3s cooldown before next replay

            # ── Auto-refresh tokens from configured data source ──
            if (self._refresh_interval_min is not None and
                    time.time() - self._last_fetch_ts > self._refresh_interval_min * 60):
                self._refresh_live_tokens()

            self.desk = Desk(
                start_usd=self._desk_start,
                max_positions=self._desk_max_pos,
                live_mode=True, realistic=True,
                signal_callback=self._desk_callback,
            )
            self.server.desk = self.desk
            self.i = 0
            self.pending_marks = []
            self.open_trades = {}
            # Reload DB history so SSE payload is complete from tick 0.
            # "real" mode never writes the paper ledger, so there is nothing to
            # restore there — the Trade History page reads OKX fills instead.
            if self._signal_mode == "real":
                self.closed_trades = []
            else:
                self.closed_trades = self.db.get_recent(limit=500)
                print(f"[DeskRunner] loop reset — {len(self.closed_trades)} trades restored from DB")

    def _do_one_tick(self) -> None:
        nxt_event = self.events[self.i][0] if self.i < len(self.events) else 10**9
        nxt_mark = min((pm[0] for pm in self.pending_marks), default=10**9)

        if nxt_event <= nxt_mark and self.i < len(self.events):
            t, tok = self.events[self.i]
            self.i += 1
            if max(tok.true_multiple_path or [1]) >= 1.5:
                self.desk.observe_runner(tok, max(tok.true_multiple_path))
            before = len(self.desk.positions)
            self.desk.on_launch(tok)
            after = len(self.desk.positions)
            if after > before:
                self._schedule_marks(tok, t)
                self.desk.equity_curve.append((t, self.desk.bankroll, f"launch {tok.ticker}"))
                # ── AUTO-EXECUTE: open real OKX position if mode == auto ──
                # P2: only in legacy signal mode. In "real" mode entries come
                # from _scan_real_signals() off real OKX candles.
                if (self._trade_mode == "auto" and self._platform_tag == "OKX"
                        and self._signal_mode == "legacy"):
                    self._auto_exec_entry(tok, before, after)
        elif self.pending_marks:
            t, tok, mult = min(self.pending_marks, key=lambda z: z[0])
            self.pending_marks.remove((t, tok, mult))
            if mult >= 1.3:
                self.desk.observe_runner(tok, mult)
            pos_match = next((p for p in self.desk.positions if p.ticker == tok.ticker), None)
            forced = None
            last = not any(pm[1].ticker == tok.ticker for pm in self.pending_marks)
            if last and pos_match and mult >= 1.5:
                if not self.desk.wallets.score(tok)["force_exit"]:
                    forced = f"EXIT rotate off last mark {mult:.1f}x"
            self.desk.mark_and_maybe_exit(tok, t, mult, forced=forced)
            self.desk.equity_curve.append((t, self.desk.bankroll, f"mark {tok.ticker} {mult:.1f}x"))

        self._track_trades()

    def _track_trades(self) -> None:
        # In "real" mode the desk is a paper replay that no longer drives any
        # order, so persisting its fills would fill the Trade History page with
        # trades the exchange never saw. Realized trades come from OKX fills
        # instead (_okx_cached_round_trips). The replay still runs for the
        # scatter/feed panels — it just must not touch the ledger.
        if self._signal_mode == "real":
            self.open_trades.clear()
            return
        for f in self.desk.feed[-30:]:
            if f.side == "ENTRY" and f.ticker not in self.open_trades:
                pos = next((p for p in self.desk.positions if p.ticker == f.ticker), None)
                entry_usd = pos.entry_usd if pos else f.usd
                trade_rec = {"ticker": f.ticker, "entry_min": f.t_min, "entry_usd": round(entry_usd, 2),
                             "platform": self._platform_tag}
                self.open_trades[f.ticker] = trade_rec
                self.db.add_open_trade(trade_rec)
            elif f.side in ("EXIT", "STOP") and f.ticker in self.open_trades:
                ot = self.open_trades.pop(f.ticker)
                exit_usd = round(ot["entry_usd"] * f.multiple, 2)
                pnl_usd = round(exit_usd - ot["entry_usd"], 2)
                record = {
                    "ticker": f.ticker, "side": "EXIT",
                    "entry_usd": ot["entry_usd"], "exit_usd": exit_usd,
                    "pnl_usd": pnl_usd, "pnl_mult": round(f.multiple, 3),
                    "win": f.multiple >= 1.0,
                    "entry_min": ot["entry_min"], "exit_min": f.t_min,
                    "entry_ts": ot.get("entry_ts"), "exit_ts": time.time(),
                    "why": f.note or "",
                    # P2: this ledger is the desk's paper replay, NOT OKX fills.
                    # Tagging it "OKX" made the UI claim orders the exchange never
                    # saw. Real fills live in _okx_cached_trades (/trade/fills).
                    "platform": "PAPER",
                }
                self.closed_trades.append(record)
                self.db.add_trade(record)
                self.db.remove_open_trade(f.ticker)

    def _close_remaining(self) -> None:
        if not self.desk.positions:
            return
        last_t = max((f.t_min for f in self.desk.feed), default=0)
        remaining = list(self.desk.positions)
        for pos in remaining:
            orig_tok = next((ev[1] for ev in self.events if ev[1].ticker == pos.ticker), None)
            self.desk.mark_and_maybe_exit(
                orig_tok or self.events[0][1],
                last_t + 1, pos.current_mult,
                forced=f"END_OF_REPLAY close {pos.current_mult:.2f}x",
            )
        self._track_trades()

    def _housekeep_swaps(self) -> None:
        """Drop expired pending swaps. Runs in every mode — only the
        *submission* is gated on trade_mode, not the bookkeeping."""
        trader = self.server.trader
        if not trader:
            return
        try:
            trader._cleanup_expired_swaps()
        except Exception as e:
            print(f"[DeskRunner] swap cleanup failed: {e}")

    def _auto_submit_pending_swaps(self) -> None:
        """Auto-submit OKX pending swaps to avoid accumulation.

        Submits pending swaps in batches, at most every 5 seconds,
        to prevent flooding the OKX API while ensuring timely execution.
        Also cleans up expired pending swaps.

        方案 C：OKX perp-only 模式下本函数是纯粹给"自动现货下单"兜底的入口 ——
        pending_swaps 里存的是现货单（on_desk_fill 解析成 “SOL-USDT”）。合约侧
        （submit_market / close_swap）不经过这里。故 perp-only 时直接返回，确保
        任何（包括历史残留 / 手动注入的）现货 pending 都不会被自动提交。
        """
        if self._trade_mode != "auto":
            # "Signal Only" is the documented default: the desk proposes and the
            # member confirms in the UI, so never push those orders ourselves.
            return
        if getattr(self, "_perp_only", False):
            # 仍要清过期 pending，否则手动注入的残留单会永久滞留在 UI 里
            trader = self.server.trader
            if trader:
                trader._cleanup_expired_swaps()
            return
        trader = self.server.trader
        if not trader:
            return

        # Clean up expired pending swaps first
        trader._cleanup_expired_swaps()

        if len(trader.pending_swaps) == 0:
            return
        # Throttle: submit at most once every 5 seconds
        now = time.time()
        if now - self._last_auto_submit_ts < 5.0:
            return
        self._last_auto_submit_ts = now

        # Get all pending swap keys
        pending_keys = list(trader.pending_swaps.keys())
        if not pending_keys:
            return

        submitted_count = 0
        for key in pending_keys[:10]:  # Process at most 10 per batch
            try:
                result = trader.submit_swap(key)
                if result.get("ok"):
                    submitted_count += 1
                    print(f"[DeskRunner] Auto-submitted OKX swap: {key} → {result.get('order_id', 'ok')}")
                else:
                    print(f"[DeskRunner] Auto-submit failed for {key}: {result.get('error', 'unknown')}")
            except Exception as e:
                print(f"[DeskRunner] Auto-submit exception for {key}: {e}")

        if submitted_count > 0:
            print(f"[DeskRunner] Auto-submit batch: {submitted_count}/{len(pending_keys)} swaps submitted")

    def _reload_perp_settings(self) -> None:
        """Reload perp auto-close settings from DB, falling back to env vars.

        Priority: OKX_PERP_MAX_POSITION_USD env > DB max_position_usd > 1000 default.
        This lets operators pin a hard cap via .env without touching the UI.
        """
        try:
            s = self.db.get_all_settings()
            if "tp_pct" in s: self._perp_tp_pct = float(s["tp_pct"])
            if "sl_pct" in s: self._perp_sl_pct = float(s["sl_pct"])
            if "max_hold_sec" in s: self._perp_max_hold_sec = int(s["max_hold_sec"])
            self._perp_allow_reopen = s.get("allow_reopen", "true") == "true"
            # OKX_PERP_MAX_POSITION_USD env wins over DB
            env_max = os.environ.get("OKX_PERP_MAX_POSITION_USD")
            if env_max:
                try:
                    self._perp_max_position_usd = float(env_max)
                    print(f"[DeskRunner] Using OKX_PERP_MAX_POSITION_USD env: ${self._perp_max_position_usd}")
                except ValueError:
                    self._perp_max_position_usd = float(s.get("max_position_usd", "1000"))
            else:
                self._perp_max_position_usd = float(s.get("max_position_usd", "1000"))
            # Minimum notional for a single entry (0 = no floor beyond OKX's)
            self._perp_min_trade_usd = float(s.get("min_trade_usd", "0") or 0)
            # ── Per-trade auto-exec notional (single source of truth) ──
            # ops DB 「trade_usd」 > env OKX_SPOT_ORDER_USD/OKX_PERP_ORDER_USD > $50
            env_trade = (os.environ.get("OKX_SPOT_ORDER_USD")
                         or os.environ.get("OKX_PERP_ORDER_USD") or "")
            try:
                self._perp_trade_usd = float(s.get("trade_usd") or env_trade)
            except (TypeError, ValueError):
                self._perp_trade_usd = float(DEFAULT_TRADE_USD)
            if self._perp_trade_usd <= 0:
                self._perp_trade_usd = float(DEFAULT_TRADE_USD)
            # Auto-trade mode from DB (default: signal_only)
            self._trade_mode = s.get("trade_mode", "signal_only")
            # P2: entry-signal source. "real" = OKX candles (trend/RSI/ATR/
            # breakout) via _scan_real_signals; "legacy" = the meme-era desk
            # narrative path. env wins so operators can pin it in .env.
            self._signal_mode = (os.environ.get("OKX_SIGNAL_MODE")
                                 or s.get("signal_mode", "real")).lower()
            _adapt = (f"adaptive(k_sl={self._atr_k_sl},R={self._atr_r},"
                      f"sl=[{self._atr_sl_floor},{self._atr_sl_cap}]%,"
                      f"tp=[{self._atr_tp_floor},{self._atr_tp_cap}]%)"
                      if self._atr_adaptive else "fixed")
            print(f"[DeskRunner] Perp settings reloaded: TP={self._perp_tp_pct}% SL={self._perp_sl_pct}% hold={self._perp_max_hold_sec}s reopen={self._perp_allow_reopen} size_band=${self._perp_min_trade_usd:.0f}-${self._perp_max_position_usd:.0f} trade_usd={self._perp_trade_usd} mode={self._trade_mode} signal_mode={self._signal_mode} sltp={_adapt}")
        except Exception as e:
            print(f"[DeskRunner] WARNING — perp settings reload failed: {e}")

    def update_trade_mode(self, mode: str) -> None:
        """Update trade mode directly from server - no DB round-trip."""
        if self._trade_mode != mode:
            self._trade_mode = mode
            # Persist to DB so it survives restarts
            self.db.set_setting("trade_mode", mode)
            print(f"[DeskRunner] trade_mode updated → {mode}")

    def _auto_exec_entry(self, tok: Any, before_count: int, after_count: int) -> None:
        """Desk-signal driven entry — only used when signal_mode == "legacy".

        The desk replay fabricates its own price path, so it cannot be trusted
        to decide real entries. In signal_mode == "real" (the default) this path
        is disabled and _scan_real_signals() opens positions off real OKX
        candles instead.
        """
        try:
            # Only open new positions (not exits)
            if after_count <= before_count:
                return

            # Find newly opened position
            new_pos = next((p for p in self.desk.positions
                          if p.ticker == tok.ticker and (p.entry_min >= self.events[self.i - 1][0] if self.i > 0 else True)),
                          None)
            if new_pos is None:
                return

            # Determine position size from desk position
            stake_usd = new_pos.entry_usd
            # Fixed per-trade notional — the same single resolved value the real
            # signal path uses, so desk-sizer drift can never change the amount.
            if self._perp_trade_usd > 0:
                print(f"[DeskRunner] AUTO-EXEC {tok.ticker}: fixed order "
                      f"${self._perp_trade_usd:.2f} (override desk ${new_pos.entry_usd:.2f})")
                stake_usd = self._perp_trade_usd

            self._open_perp(tok.ticker, stake_usd, "desk-signal",
                            atr_pct=float(getattr(tok, "atr_pct", 0.0) or 0.0))

        except Exception as e:
            print(f"[DeskRunner] AUTO-EXEC ERROR {tok.ticker}: {e}")
            import traceback; traceback.print_exc()

    def _sl_tp_for_atr(self, atr_pct: float | None) -> tuple[float, float]:
        """Return the ATR-adaptive (sl_pct, tp_pct) for one entry.

        SL% = clamp(k_sl × ATR%, sl_floor, sl_cap)
        TP% = clamp(R   × SL%,  tp_floor, tp_cap)

        Falls back to the fixed _perp_sl_pct / _perp_tp_pct when adaptive mode is
        off or the ATR is unknown/too small — so a missing atr_pct can never
        produce a zero-width stop.
        """
        if (not self._atr_adaptive) or atr_pct is None or atr_pct < self._atr_use_min:
            return self._perp_sl_pct, self._perp_tp_pct
        sl = min(max(self._atr_k_sl * float(atr_pct), self._atr_sl_floor), self._atr_sl_cap)
        tp = min(max(self._atr_r * sl, self._atr_tp_floor), self._atr_tp_cap)
        return round(sl, 4), round(tp, 4)

    def _open_perp(self, ticker: str, stake_usd: float, reason: str,
                   atr_pct: float | None = None) -> None:
        """Open a real OKX perpetual position, subject to every safety gate.

        Shared by the legacy desk-driven path (_auto_exec_entry) and the
        real-candle path (_scan_real_signals) so both honour the same position
        lock, delisting blacklist, hard entry cooldown and size cap.

        `atr_pct` (15m ATR%, from TokenLaunch.atr_pct) selects the ATR-adaptive
        SL/TP frozen for this position; None falls back to the fixed defaults.
        """
        from rh_okx_executor import OKX_MIN_ORDER_USD
        exe = self._resolve_okx_executor()
        if exe is None:
            return

        inst_id = f"{ticker}-USDT-SWAP"
        sl_pct, tp_pct = self._sl_tp_for_atr(atr_pct)

        # Skip instruments OKX already reported as delisted/non-existent
        if inst_id in self._delisted_insts:
            return

        # Skip if a position already exists in OKX (prevent double-open)
        existing = next((p for p in self._okx_cached_positions
                        if p.get("ticker", "").upper() == ticker.upper()),
                       None)
        if existing:
            return

        # P0 修复: 入场节流 —— 硬冷却独立于 allow_reopen。
        # 旧逻辑 cooldown 只在 allow_reopen=False 时才 return（默认 True → 完全失效），
        # 400ms tick 下同标的每轮 replay 都会重新开仓，churn 手续费。
        # 必须置于所有 print 之前：否则被冷却拦下的尝试仍会打日志，运维会误判为
        # "还在反复重试"。此处一并消耗冷却窗口，使尺寸不足/杠杆失败等也受节流。
        now = time.time()
        last_blocker = max(self._perp_last_exit.get(ticker, 0.0),
                           self._perp_last_entry.get(ticker, 0.0))
        if now - last_blocker < self._perp_entry_cooldown_sec:
            return
        # allow_reopen 只管"平过仓的标的是否还允许再入场"
        if not self._perp_allow_reopen and ticker in self._perp_last_exit:
            return
        self._perp_last_entry[ticker] = now

        # ── Per-trade amount band (min_trade_usd … max_position_usd) ──
        # Clamp instead of reject — an entry below the configured floor is raised
        # to it and one above the cap is trimmed, matching the manual path.
        band_lo = max(self._perp_min_trade_usd, OKX_MIN_ORDER_USD)
        band_hi = self._perp_max_position_usd
        if band_hi > 0 and stake_usd > band_hi:
            print(f"[DeskRunner] AUTO-EXEC SIZE {ticker}: ${stake_usd:.2f} capped to max ${band_hi:.2f}")
            stake_usd = band_hi
        if stake_usd < band_lo:
            print(f"[DeskRunner] AUTO-EXEC SIZE {ticker}: ${stake_usd:.2f} raised to min ${band_lo:.2f}")
            stake_usd = band_lo
        if stake_usd < OKX_MIN_ORDER_USD:
            print(f"[DeskRunner] AUTO-EXEC SKIP {ticker}: stake ${stake_usd:.2f} below min ${OKX_MIN_ORDER_USD}")
            return

        # Set leverage first (required before perp orders)
        leverage = int(os.environ.get("OKX_PERP_LEVERAGE", "5"))
        try:
            exe.set_leverage(inst_id, leverage, mgn_mode="cross")
        except Exception as e:
            print(f"[DeskRunner] AUTO-EXEC WARN set_leverage {inst_id}: {e}")
            if "51001" in str(e) or "51087" in str(e):
                self._delisted_insts.add(inst_id)
                print(f"[DeskRunner] AUTO-EXEC BLACKLIST {inst_id}: instrument delisted/nonexistent")
                return

        # Submit market order (LONG by default for ENTRY signals)
        cl_ord_id = f"ae{ticker}{int(time.time()*1000)}"  # OKX clOrdId: alnum only, no underscore
        try:
            result = exe.submit_market(inst_id, "buy", stake_usd, cl_ord_id=cl_ord_id, td_mode="cross")
        except ValueError as e:
            # Notional rounds below one contract (ctVal/lotSz) — a config-level
            # skip, not a failure; do not spam a traceback for it.
            print(f"[DeskRunner] AUTO-EXEC SKIP {ticker}: {e}")
            return

        if result.get("ok"):
            # Freeze the ATR-adaptive thresholds for THIS position (keyed by instId),
            # so a later ATR change can never move an already-open position's stop.
            self._perp_sl_tp[inst_id] = (sl_pct, tp_pct)
            self._perp_atr[inst_id] = float(atr_pct or 0.0)
            print(f"[DeskRunner] AUTO-EXEC ENTRY {ticker} {stake_usd:.2f}USD @ {inst_id} "
                  f"order_id={result.get('order_id','')} "
                  f"SL={sl_pct:.2f}% TP={tp_pct:.2f}% ATR={float(atr_pct or 0.0):.2f}% [{reason}]")
        else:
            msg = str(result.get("msg", result))
            print(f"[DeskRunner] AUTO-EXEC FAILED {ticker}: {msg}")
            if "51001" in msg or "51087" in msg:
                self._delisted_insts.add(inst_id)
                print(f"[DeskRunner] AUTO-EXEC BLACKLIST {inst_id}: instrument delisted/nonexistent")

    def _scan_real_signals(self) -> None:
        """Entry scan used in signal_mode == "real" (the default).

        Iterates the OKX tokens refreshed by OKXDataSource; each one carries a
        signal evaluated on real candles (trend + RSI + ATR + breakout) in
        `signal_ok` / `signal_reason`. The meme-era narrative and wallet-map
        scoring is never consulted here.
        """
        if self._platform_tag != "OKX" or self._trade_mode != "auto":
            return
        # Per-trade notional: use the single resolved value — never re-read the
        # env here, or the value shown/saved in the UI could diverge from what
        # actually gets executed.
        stake_usd = self._perp_trade_usd
        for tok in self.raw_tokens:
            if not getattr(tok, "signal_ok", False):
                continue
            self._open_perp(tok.ticker, stake_usd, getattr(tok, "signal_reason", ""),
                            atr_pct=float(getattr(tok, "atr_pct", 0.0) or 0.0))

    def _check_and_close_perp_positions(self) -> None:
        """Auto-close OKX perpetual positions based on TP/SL/time limits.

        Reads real OKX positions from cache, computes entry_price from raw data,
        and closes via submit_market when thresholds are breached.
        """
        if not self._okx_cached_positions or not self._okx_cached_positions:
            return
        exe = self._resolve_okx_executor()
        if exe is None:
            return
        from rh_okx_executor import OKX_MIN_ORDER_USD
        now = time.time()
        to_close = []
        # 诊断日志: 为什么这个仓位没被关
        skipped = 0
        for pos in self._okx_cached_positions:
            ticker = pos.get("ticker", "")
            avg_px = pos.get("avg_px", 0)
            last_px = pos.get("last_px", 0)
            side = pos.get("side", "")
            size_usd = abs(pos.get("size_usd", 0))
            upl = pos.get("upl", 0)
            if avg_px <= 0 or last_px <= 0:
                skipped += 1
                continue
            # NOTE: no OKX_MIN_ORDER_USD floor here. That $10 threshold is an
            # OPENING constraint; applying it to CLOSES silently stranded every
            # position below $10 notional, which is why positions never moved.
            # Closing is always a risk-reducing action — let OKX enforce its own
            # per-instrument minSz and surface any rejection in the log.
            pos_qty_chk = abs(pos.get("size", 0))
            if pos_qty_chk <= 0:
                skipped += 1
                continue
            # Skip if exceeds max position size — print why we skip it
            if size_usd > self._perp_max_position_usd:
                skipped += 1
                # Diagnostic (throttle: only once per 5 min per ticker)
                key = f"diag_{ticker}"
                if now - getattr(self, "_perp_diag_ts", {}).get(key, 0) > 300:
                    setattr(self, "_perp_diag_ts", getattr(self, "_perp_diag_ts", {}))
                    self._perp_diag_ts[key] = now
                    print(f"[DeskRunner] AUTO-CLOSE SKIP {ticker} notional=${size_usd:.2f} "
                          f"> max_position=${self._perp_max_position_usd:.2f} (override via OKX_PERP_MAX_POSITION_USD)")
                continue
            # Cooldown after any close attempt. The OKX position cache is up to
            # 30s stale, so without this the same already-closed position would
            # be retried every tick and spam 51169 ("no position to reduce").
            if ticker in self._perp_last_exit and now - self._perp_last_exit[ticker] < self._perp_close_cooldown_sec:
                continue
            # Compute entry vs current price percentage change
            if side == "LONG":
                pct_change = (last_px - avg_px) / avg_px * 100
            else:
                pct_change = (avg_px - last_px) / avg_px * 100
            # ATR-adaptive thresholds frozen at entry (fallback: fixed defaults)
            inst_id = pos.get("inst_id") or f"{ticker}-USDT-SWAP"
            sl_pct, tp_pct = self._perp_sl_tp.get(
                inst_id, (self._perp_sl_pct, self._perp_tp_pct))
            reason = None
            if pct_change >= tp_pct:
                reason = f"TAKE_PROFIT +{pct_change:.1f}% (TP {tp_pct:.2f}%)"
            elif pct_change <= -sl_pct:
                reason = f"STOP_LOSS {pct_change:.1f}% (SL {sl_pct:.2f}%)"
            # P1 修复: MAX_HOLD 仅在未触发 TP/SL 时生效（旧代码是独立 if，
            # 时间止损会无条件覆盖 reason，把已到 TP 的盈利单也强制砍掉）。
            # Time stop — only when no TP/SL already fired (an in-profit trade must
            # not be force-closed by the clock) and only with a plausible open time,
            # so a missing or garbage timestamp can never mass-close positions.
            entry_time = pos.get("entry_time", 0) or 0
            age = now - entry_time if entry_time else 0
            if reason is None and 0 < age <= 7 * 86400 and age > self._perp_max_hold_sec:
                reason = f"MAX_HOLD_EXCEEDED ({age/60:.0f}min)"
            if reason:
                close_side = "sell" if side == "LONG" else "buy"
                pos_qty = abs(pos.get("size", 0))  # base coin quantity (NOT USD)
                # posSide only valid in hedge mode; "net" would be rejected
                raw_pos_side = pos.get("pos_side", "net")
                close_pos_side = raw_pos_side if raw_pos_side in ("long", "short") else None
                to_close.append((ticker, close_side, size_usd, pos_qty, side, reason, upl, close_pos_side))
        # Execute closes
        for ticker, close_side, size_usd, pos_qty, orig_side, reason, upl, close_pos_side in to_close:
            try:
                inst_id = f"{ticker}-USDT-SWAP"
                # OKX clOrdId: alphanumeric only (1-32 chars) — underscore is rejected (51000)
                cl_ord = f"ac{close_side}{ticker}{int(now*1000)}"
                # Use close_swap (pos=quantity) for closing SWAPs — submit_market
                # with nominal USD would attempt to OPEN a new position and is
                # rejected when margin is insufficient (51008).
                if hasattr(exe, "close_swap"):
                    result = exe.close_swap(inst_id, close_side, pos_qty,
                                            cl_ord_id=cl_ord,
                                            pos_side=close_pos_side)
                else:
                    # Fallback for older executor
                    result = exe.submit_market(inst_id, close_side, size_usd, cl_ord_id=cl_ord)
                if result.get("ok"):
                    self._perp_last_exit[ticker] = now
                    # Capture the frozen thresholds for the log, then release them.
                    sl_used, tp_used = self._perp_sl_tp.get(
                        inst_id, (self._perp_sl_pct, self._perp_tp_pct))
                    atr_used = self._perp_atr.get(inst_id, 0.0)
                    log_entry = {"ticker": ticker, "side": close_side, "reason": reason,
                                 "size_usd": round(size_usd, 2), "pnl_usd": round(upl, 2),
                                 "sl_pct": sl_used, "tp_pct": tp_used,
                                 "atr_pct": round(float(atr_used or 0.0), 3),
                                 "ts": int(now), "order_id": result.get("order_id", "")}
                    self._perp_close_log.append(log_entry)
                    if len(self._perp_close_log) > 50:
                        self._perp_close_log = self._perp_close_log[-50:]
                    self._perp_sl_tp.pop(inst_id, None)
                    self._perp_atr.pop(inst_id, None)
                    print(f"[DeskRunner] AUTO-CLOSE {ticker} {close_side} ({reason}): qty={pos_qty:.4f} upl=${upl:.2f} order_id={result.get('order_id','')}")
                else:
                    print(f"[DeskRunner] AUTO-CLOSE FAILED {ticker}: {result}")
                    self._perp_last_exit[ticker] = now
            except Exception as e:
                print(f"[DeskRunner] AUTO-CLOSE ERROR {ticker}: {e}")
                self._perp_last_exit[ticker] = now

    def _push_current_state(self) -> None:
        """Build full state dict (desk + swaps) and push to SSE queue."""
        t = self._tick_count * 2
        snap = self.desk.snapshot(t)
        equity = [(int(ts), float(eq)) for ts, eq, _ in self.desk.equity_curve[-800:]]

        positions = [{
            "ticker": p.ticker, "entry_min": p.entry_min,
            "entry_usd": round(p.entry_usd, 2),
            "peak_mult": round(p.peak_mult, 3),
            "current_mult": round(p.current_mult, 3),
            "size_frac": round(p.size_frac * 100, 1),
        } for p in self.desk.positions]

        # ── OKX LIVE DATA: consume CACHED account snapshot (NOT per-tick API call) ──
        # self._platform_tag is frozen at startup → prevents mode flip-flop
        is_okx = self._platform_tag == "OKX"
        okx_real_balance = self._okx_cached_balance if (is_okx and self._okx_cached_balance) else None
        okx_perp_positions = list(self._okx_cached_positions) if is_okx else []
        # Annotate each open perp with its frozen ATR-adaptive SL/TP (for the UI).
        # Falls back to the fixed defaults when the position predates this upgrade.
        for _p in okx_perp_positions:
            _iid = _p.get("inst_id", "")
            _sl, _tp = self._perp_sl_tp.get(
                _iid, (self._perp_sl_pct, self._perp_tp_pct))
            _p["sl_pct"] = _sl
            _p["tp_pct"] = _tp
            _p["atr_pct"] = round(float(self._perp_atr.get(_iid, 0.0) or 0.0), 3)
        okx_trades_history = list(self._okx_cached_trades) if is_okx else []
        # Whether OKX sim-account data is actually available for display.
        # When False, top-level metrics must NOT fall back to hardcoded 500/0.453.
        okx_data_available = bool(okx_real_balance)
        # ── OKX account fetch diagnostics (for UI empty-state messaging) ──
        okx_data_source = "okx_sim" if okx_data_available else ("okx_unfetched" if is_okx else "simulator")
        okx_cache_age = round(time.time() - self._okx_last_account_ts, 1) if self._okx_last_account_ts else -1

        # ── Compute wins/losses/win_rate universally (needed by both branches) ──
        # Source of truth for the ledger: OKX realized round trips in OKX mode,
        # the desk's paper replay otherwise. Mixing them made the top-bar win
        # rate reflect 207k replay ticks instead of the account's real trades.
        ledger = (list(self._okx_cached_round_trips) if is_okx
                  else list(self.closed_trades))
        closed = list(reversed(ledger[-50:]))
        wins = sum(1 for ct in closed if ct.get("win"))
        losses = len(closed) - wins
        win_rate_pct = round(wins / len(ledger), 3) if ledger else 0.0

        # Detect which live executor platform is active so we tag feed entries
        # NOTE: use self._platform_tag (frozen at startup), not executor snapshot, to avoid flip-flop
        plat_tag = self._platform_tag
        feed = [{"t": f.t_min, "side": f.side, "ticker": f.ticker or "-",
                 "mult": round(f.multiple, 2), "note": f.note,
                 "platform": plat_tag}
                for f in self.desk.feed[-20:]]

        # ── Kelly ATR sub-metrics (from sizer atr_pct → vol_penalty) ──
        k = self.desk.sizer.kelly()
        vol_penalty = max(0.3, 1.0 - self.desk.sizer.atr_pct * 10.0) if self.desk.sizer.atr_pct > 0 else 1.0

        # ── Builder commission pseudo-total (simulator) ──
        comm_pseudo = round(sum(p.entry_usd for p in self.desk.positions) * 0.0015, 2)

        # ── OKX REAL VS SIMULATOR: source routing for top-level metrics ──
        # In OKX mode, top account-level fields use OKX real balance/positions.
        # Simulator-only fields are prefixed "sim_" and go to strategy panels.
        if is_okx and okx_real_balance:
            total_eq = okx_real_balance["total_eq_usd"]
            # PnL baseline: first fetched OKX account equity (NOT hardcoded 500)
            okx_base = self._okx_start_equity if self._okx_start_equity else total_eq
            multiple = round(total_eq / okx_base, 2) if okx_base > 0 else 1.0
            bankroll = round(total_eq, 2)

            # Open positions count = spot holdings (non-USDT, non-zero) + perp
            spot_hot = [h for h in okx_real_balance["holdings"]
                        if h["ccy"] != "USDT" and h["spot_bal"] > 1e-8]
            open_count = len(spot_hot) + len(okx_perp_positions)

            # OKX account risk exposure = (perp notional + spot eq) / total_eq
            perp_notional = sum(p["size_usd"] for p in okx_perp_positions)
            spot_eq_usd = sum(h["eq_usd"] for h in okx_real_balance["holdings"])
            risk_pct = round(
                (perp_notional + spot_eq_usd) / total_eq * 100, 1
            ) if total_eq > 0 else 0.0
            risk_limit = 8.0

            # Realized round trips + currently-open positions = real activity
            total_trades = len(ledger) + len(okx_perp_positions)

            # OKX PnL = current equity vs first-fetched baseline (not hardcoded 500)
            pnl_24h = round(total_eq - okx_base, 2)
            okx_start_bankroll = round(okx_base, 2)

            # used_kelly: real risk / real bankroll. full_kelly: 1 (OKX doesn't have kelly cap)
            used_kelly = round(perp_notional / total_eq * 100, 3) if total_eq > 0 else 0.0
            full_kelly = 1.0  # OKX-unified account has no single-asset kelly cap

            # entries/wins/losses stay as simulator counters — they reflect the
            # strategy's internal signal engine, not OKX ledger activity
        else:
            multiple = round(self.desk.bankroll / self.desk.start, 2)
            bankroll = round(self.desk.bankroll, 2)
            open_count = len(self.desk.positions)
            last_eq = self.desk.equity_curve[-1][1] if self.desk.equity_curve else self.desk.bankroll
            window_ticks = 1440
            if len(self.desk.equity_curve) > window_ticks:
                baseline = self.desk.equity_curve[-window_ticks][1]
            else:
                baseline = self.desk.start
            pnl_24h = round(last_eq - baseline, 2)
            risk_pct = round(sum(p.size_frac * 100 for p in self.desk.positions), 1)
            risk_limit = 8.0
            used_kelly = round(k["used"], 3)
            full_kelly = round(k["full_kelly"], 3)
            total_trades = wins + losses + len(self.desk.positions)
            # Non-OKX: PnL baseline is the simulator desk start (kept for the
            # pure-simulator path; OKX pages never use this value)
            okx_start_bankroll = self.desk.start

        monitor = {
            "multiple": multiple,
            "bankroll": bankroll,
            "total_pools": len(self.raw_tokens),
            "entries": self.desk.entered,
            "wins": wins, "losses": losses,
            "dominant_theme": self.desk.narrative.cluster_label,
            "profit_rate": round(wins / len(ledger), 3) if ledger else 0.0,
            "open_count": open_count,
            "state": "LIVE" if is_okx else self.desk.state.name,
            "pending_swaps": len(self.server.trader.pending_swaps),
            "submitted_swaps": len(self.server.trader.submitted_swaps),
            # OKX pages: PnL% baseline = first-fetched account equity, not hardcoded 500
            "start_bankroll": (okx_start_bankroll if (is_okx and okx_real_balance) else self.desk.start),
            "pnl_24h": pnl_24h,
            "win_rate": win_rate_pct,
            "total_trades": total_trades,
            "risk_pct": risk_pct,
            "risk_limit": risk_limit,
            "atr_pct": round(self.desk.sizer.atr_pct, 4),
            "vol_penalty": round(vol_penalty, 3),
            "full_kelly": full_kelly,
            "used_kelly": used_kelly,
            "comm_pseudo": comm_pseudo,
            # ── Simulator-only strategy counters (preserved for Engines panel) ──
            "sim_entries": self.desk.entered,
            "sim_wins": wins,
            "sim_losses": losses,
            "sim_open_count": len(self.desk.positions),
        }

        pts, centroid_xy = [], None
        try:
            df = self.desk.narrative.project_2d(self.scatter_tokens)
            pts = df[["x", "y"]].values.tolist() if hasattr(df, "values") else []
            c = self.desk.narrative.cluster_centroid
            if c is not None and self.scatter_tokens:
                X = np.vstack([self.desk.narrative.embed_token(t) for t in self.scatter_tokens] + [c])
                X = X - X.mean(axis=0)
                U, S, _ = np.linalg.svd(X, full_matrices=False)
                xy_all = (U[:, :2] * S[:2]).tolist()
                pts = xy_all[:-1]
                centroid_xy = xy_all[-1]
        except Exception:
            pass

        scatter_meta = [{"ticker": t.ticker, "theme": t.theme_hint,
                         "desc": (t.description or "")[:40]} for t in self.scatter_tokens]

        state = {
            "t": t, "snap": snap, "equity": equity,
            "scatter": {"points": pts, "centroid": centroid_xy, "tokens": scatter_meta},
            "feed": feed, "positions": positions, "closed_trades": closed,
            "monitor": monitor,
            "bankroll": round(self.desk.bankroll, 2),
            "state": "LIVE" if is_okx else self.desk.state.name,
            "narrative": {"label": self.desk.narrative.cluster_label,
                          "min_match": self.desk.narrative.min_match},
            "swaps": self.server.swaps_snapshot(),
            "sol_usd": self.server._sol_usd,
            "done": not (self.i < len(self.events) or self.pending_marks),
            # ── V1 TERMINAL: top-level convenience fields ──
            # okx_connected = TRUE when platform locked to OKX AND we have a non-empty cache
            # → prevents UI from flashing back to demo on transient API failures
            "okx_connected": bool(is_okx and (self._okx_cached_balance is not None or self._okx_mode_locked)),
            "today_pnl": pnl_24h,
            "risk_pct": risk_pct,
            "risk_limit": risk_limit,
            "decisions": list(getattr(self, "_ai_decisions", [])),
            "perp_positions": okx_perp_positions if is_okx else list(getattr(self, "_perp_positions", [])),
            "perp_stats": getattr(self, "_perp_stats", {}),
            "is_okx": is_okx,
            # ── OKX REAL ACCOUNT DATA (cached, NOT per-tick) ──
            "okx_real_balance": okx_real_balance,
            "okx_trades_history": okx_trades_history,
            # Realized round trips derived from those fills — what the Trade
            # History page renders while signal_mode == "real".
            "okx_round_trips": list(self._okx_cached_round_trips) if is_okx else [],
            "okx_spot_holdings": okx_real_balance.get("holdings", []) if okx_real_balance else [],
            # ── Perp auto-close settings (current) ──
            "perp_config": {
                "tp_pct": self._perp_tp_pct,
                "sl_pct": self._perp_sl_pct,
                "max_hold_sec": self._perp_max_hold_sec,
                "max_position_usd": self._perp_max_position_usd,
                "allow_reopen": self._perp_allow_reopen,
                # Per-trade auto-exec notional actually used by the trader.
                "trade_usd": self._perp_trade_usd,
                # ── ATR-adaptive SL/TP (方案 B) ──
                "atr_adaptive": self._atr_adaptive,
                "atr_k_sl": self._atr_k_sl,
                "atr_r": self._atr_r,
                "atr_sl_floor": self._atr_sl_floor,
                "atr_sl_cap": self._atr_sl_cap,
                "atr_tp_floor": self._atr_tp_floor,
                "atr_tp_cap": self._atr_tp_cap,
            },
            # ── Data Source metadata for front-end status badge ──
            "data_source": plat_tag,   # "OKX" | "JUP"
            "data_source_locked": is_okx,
            "okx_data_available": okx_data_available,
            "okx_data_source": okx_data_source,
            "okx_cache_age_sec": okx_cache_age,
            # ── OKX auto-close log for UI display ──
            "perp_close_log": self._perp_close_log,
            # ── Trade mode for UI display ──
            "trade_mode": self._trade_mode,
            "demo_mode": not is_okx,
        }
        self._latest_state = state
        self.server._desk_state = state
        # Update ServerState poll cache so /api/desk-state always serves fresh data
        self.server.push_state(state)
        if self._queue is not None:
            try:
                self._queue.put_nowait(state)
            except queue.Full:
                if self._tick_count % 50 == 0:
                    print(f"[DeskRunner] queue.FULL at tick {self._tick_count}")
        elif self._tick_count % 30 == 0:
            print(f"[DeskRunner] self._queue is None at tick {self._tick_count}")
