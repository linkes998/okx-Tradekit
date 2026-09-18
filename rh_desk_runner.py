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
from db_trades import TradeDB


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
        self._okx_account_refresh_sec = float(os.environ.get("OKX_ACCOUNT_REFRESH_SEC", "30"))
        self._okx_last_account_ts: float = 0.0
        self._okx_cached_balance: dict | None = None
        self._okx_cached_positions: list[dict] = []
        self._okx_cached_trades: list[dict] = []
        self._okx_executor_ref: object | None = None   # 统一引用，避免 executor / trader.executor 不同步
        self._okx_mode_locked: bool = False             # 一旦 OKX auth 成功，锁死模式不再回落到 demo
        # Final source-of-truth platform tag (OKX / JUP), 启动时冻结
        self._platform_tag: str = self._detect_platform_tag()

        # ── OKX PERP AUTO-EXIT CONFIG ──
        self._perp_tp_pct: float = float(os.environ.get("OKX_PERP_TP_PCT", "3.0"))
        self._perp_sl_pct: float = float(os.environ.get("OKX_PERP_SL_PCT", "1.5"))
        self._perp_max_hold_sec: int = int(os.environ.get("OKX_PERP_MAX_HOLD_SEC", "1800"))
        self._perp_last_exit: dict[str, float] = {}     # ticker → timestamp of last exit
        self._perp_close_log: list[dict] = []           # recent closes for UI display
        self._perp_allow_reopen: bool = True             # default: allow re-entry
        self._perp_max_position_usd: float = 1000.0      # default max position size
        self._trade_mode: str = "signal_only"            # "signal_only" or "auto"
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
        """Legacy: refresh tokens from DexScreener (Solana DEX)."""
        try:
            import os
            os.environ.setdefault("HTTP_PROXY", "http://127.0.0.1:7897")
            os.environ.setdefault("HTTPS_PROXY", "http://127.0.0.1:7897")

            import urllib.request
            proxy = urllib.request.ProxyHandler({
                "http": "http://127.0.0.1:7897",
                "https": "http://127.0.0.1:7897",
            })
            opener = urllib.request.build_opener(proxy)
            urllib.request.install_opener(opener)

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
            self._okx_cached_balance = {
                "total_eq_usd": summary["total_eq_usd"],
                "usdt_avail": summary["usdt_avail"],
                "usdt_eq": summary["usdt_eq"],
                "holdings": summary["holdings"],
                "perp_upl": summary["perp_upl"],
            }
            perp: list[dict] = []
            for pos in raw_positions:
                inst_id = pos.get("instId", "")
                if "-SWAP" not in inst_id:
                    continue
                pos_size = float(pos.get("pos", 0) or 0)
                if abs(pos_size) < 0.0001:
                    continue
                avg_px = float(pos.get("avgPx", 0) or 0)
                last_px = float(pos.get("last", 0) or avg_px or 1)
                upl = float(pos.get("upl", 0) or 0)
                inst = inst_id.replace("-USDT-SWAP", "")
                side = "LONG" if pos_size > 0 else "SHORT"
                notional = abs(pos_size) * last_px
                perp.append({
                    "inst_id": inst_id,
                    "ticker": inst,
                    "side": side,
                    "size": pos_size,
                    "size_usd": round(notional, 2),
                    "avg_px": avg_px,
                    "last_px": last_px,
                    "upl": round(upl, 2),
                    "lever": pos.get("lever", "?"),
                })
            perp.sort(key=lambda x: abs(x["size_usd"]), reverse=True)
            self._okx_cached_positions = perp

            try:
                self._okx_cached_trades = exe.get_order_history(limit=50)
            except Exception as he:
                print(f"[DeskRunner] OKX order history fetch failed (kept cached): {he}")

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
            # Reload DB history so SSE payload is complete from tick 0
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
                if self._trade_mode == "auto" and self._platform_tag == "OKX":
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
                    "why": f.note or "", "platform": self._platform_tag,
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

    def _auto_submit_pending_swaps(self) -> None:
        """Auto-submit OKX pending swaps to avoid accumulation.

        Submits pending swaps in batches, at most every 5 seconds,
        to prevent flooding the OKX API while ensuring timely execution.
        Also cleans up expired pending swaps.
        """
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
        """Reload perp auto-close settings from DB, falling back to env vars."""
        try:
            s = self.db.get_all_settings()
            if "tp_pct" in s: self._perp_tp_pct = float(s["tp_pct"])
            if "sl_pct" in s: self._perp_sl_pct = float(s["sl_pct"])
            if "max_hold_sec" in s: self._perp_max_hold_sec = int(s["max_hold_sec"])
            self._perp_allow_reopen = s.get("allow_reopen", "true") == "true"
            self._perp_max_position_usd = float(s.get("max_position_usd", "1000"))
            # Auto-trade mode from DB (default: signal_only)
            self._trade_mode = s.get("trade_mode", "signal_only")
            print(f"[DeskRunner] Perp settings reloaded: TP={self._perp_tp_pct}% SL={self._perp_sl_pct}% hold={self._perp_max_hold_sec}s reopen={self._perp_allow_reopen} max_usd={self._perp_max_position_usd} mode={self._trade_mode}")
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
        """Auto-execute OKX perpetual entry when trade_mode == 'auto'.

        When the strategy desk generates an ENTRY signal, this method submits
        a real market order to OKX if:
          1. _trade_mode == "auto"
          2. _platform_tag == "OKX"
          3. No existing position for this ticker (position lock)
        """
        try:
            from rh_okx_executor import OKX_MIN_ORDER_USD
            exe = self._resolve_okx_executor()
            if exe is None:
                return

            # Only open new positions (not exits)
            if after_count <= before_count:
                return

            # Find newly opened position
            new_pos = next((p for p in self.desk.positions
                          if p.ticker == tok.ticker and (p.entry_min >= self.events[self.i - 1][0] if self.i > 0 else True)),
                          None)
            if new_pos is None:
                return

            # Skip if position already exists in OKX (prevent double-open)
            existing = next((p for p in self._okx_cached_positions
                            if p.get("ticker", "").upper() == tok.ticker.upper()),
                           None)
            if existing:
                return

            # Build OKX instrument ID for SWAP
            inst_id = f"{tok.ticker}-USDT-SWAP"

            # Determine position size from desk position
            stake_usd = new_pos.entry_usd
            if stake_usd < OKX_MIN_ORDER_USD:
                print(f"[DeskRunner] AUTO-EXEC SKIP {tok.ticker}: stake ${stake_usd:.2f} below min ${OKX_MIN_ORDER_USD}")
                return

            # Check max position size
            if stake_usd > self._perp_max_position_usd:
                print(f"[DeskRunner] AUTO-EXEC SKIP {tok.ticker}: stake ${stake_usd:.2f} exceeds max ${self._perp_max_position_usd}")
                return

            # Check cooldown (30s)
            now = time.time()
            if tok.ticker in self._perp_last_exit and now - self._perp_last_exit[tok.ticker] < 30:
                if not self._perp_allow_reopen:
                    return

            # Set leverage first (required before perp orders)
            leverage = int(os.environ.get("OKX_PERP_LEVERAGE", "5"))
            try:
                exe.set_leverage(inst_id, leverage, mgn_mode="cross")
            except Exception as e:
                print(f"[DeskRunner] AUTO-EXEC WARN set_leverage {inst_id}: {e}")

            # Submit market order (LONG by default for ENTRY signals)
            cl_ord_id = f"auto_entry_{tok.ticker}_{int(now)}"
            result = exe.submit_market(inst_id, "buy", stake_usd, cl_ord_id=cl_ord_id, td_mode="cross")

            if result.get("ok"):
                print(f"[DeskRunner] AUTO-EXEC ENTRY {tok.ticker} {stake_usd:.2f}USD @ {inst_id} order_id={result.get('order_id','')}")
            else:
                print(f"[DeskRunner] AUTO-EXEC FAILED {tok.ticker}: {result.get('msg', result)}")

        except Exception as e:
            print(f"[DeskRunner] AUTO-EXEC ERROR {tok.ticker}: {e}")
            import traceback; traceback.print_exc()

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
        for pos in self._okx_cached_positions:
            ticker = pos.get("ticker", "")
            avg_px = pos.get("avg_px", 0)
            last_px = pos.get("last_px", 0)
            side = pos.get("side", "")
            size_usd = abs(pos.get("size_usd", 0))
            upl = pos.get("upl", 0)
            if avg_px <= 0 or last_px <= 0 or size_usd < 1:
                continue
            # Skip if below OKX minimum order size
            if size_usd < OKX_MIN_ORDER_USD:
                continue
            # Skip if exceeds max position size
            if size_usd > self._perp_max_position_usd:
                continue
            # Skip if recently closed (cooldown) — only if reopen not allowed
            if ticker in self._perp_last_exit and now - self._perp_last_exit[ticker] < 30:
                if not self._perp_allow_reopen:
                    continue
            # Compute entry vs current price percentage change
            if side == "LONG":
                pct_change = (last_px - avg_px) / avg_px * 100
            else:
                pct_change = (avg_px - last_px) / avg_px * 100
            reason = None
            if pct_change >= self._perp_tp_pct:
                reason = f"TAKE_PROFIT +{pct_change:.1f}%"
            elif pct_change <= -self._perp_sl_pct:
                reason = f"STOP_LOSS {pct_change:.1f}%"
            # Also check time limit
            entry_time = pos.get("entry_time", 0)
            if entry_time > 0 and (now - entry_time) > self._perp_max_hold_sec:
                reason = f"MAX_HOLD_EXCEEDED ({(now - entry_time)/60:.0f}min)"
            if reason:
                close_side = "sell" if side == "LONG" else "buy"
                to_close.append((ticker, close_side, size_usd, reason, upl))
        # Execute closes
        for ticker, close_side, size_usd, reason, upl in to_close:
            try:
                inst_id = f"{ticker}-USDT-SWAP"
                result = exe.submit_market(inst_id, close_side, size_usd,
                                           cl_ord_id=f"auto_{close_side}_{ticker}_{int(now)}")
                if result.get("ok"):
                    self._perp_last_exit[ticker] = now
                    log_entry = {"ticker": ticker, "side": close_side, "reason": reason,
                                 "size_usd": round(size_usd, 2), "pnl_usd": round(upl, 2),
                                 "ts": int(now), "order_id": result.get("order_id", "")}
                    self._perp_close_log.append(log_entry)
                    if len(self._perp_close_log) > 50:
                        self._perp_close_log = self._perp_close_log[-50:]
                    print(f"[DeskRunner] AUTO-CLOSE {ticker} {close_side} ({reason}): upl=${upl:.2f}")
                else:
                    print(f"[DeskRunner] AUTO-CLOSE FAILED {ticker}: {result}")
            except Exception as e:
                print(f"[DeskRunner] AUTO-CLOSE ERROR {ticker}: {e}")
                import traceback; traceback.print_exc()

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
        okx_trades_history = list(self._okx_cached_trades) if is_okx else []

        # ── Compute wins/losses/win_rate universally (needed by both branches) ──
        closed = list(reversed(self.closed_trades[-50:]))
        wins = sum(1 for ct in closed if ct.get("win"))
        losses = len(closed) - wins
        win_rate_pct = round(wins / len(self.closed_trades), 3) if self.closed_trades else 0.0

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
            multiple = round(total_eq / self.desk.start, 2)
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

            # OKX trades history size + open desk entries as total activity counter
            total_trades = len(okx_trades_history) + len(self.desk.positions)

            # OKX 24h PnL = real equity vs start (no intraday baseline yet)
            pnl_24h = round(total_eq - self.desk.start, 2)

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

        monitor = {
            "multiple": multiple,
            "bankroll": bankroll,
            "total_pools": len(self.raw_tokens),
            "entries": self.desk.entered,
            "wins": wins, "losses": losses,
            "dominant_theme": self.desk.narrative.cluster_label,
            "profit_rate": round(wins / len(self.closed_trades), 3) if self.closed_trades else 0.0,
            "open_count": open_count,
            "state": "LIVE" if is_okx else self.desk.state.name,
            "pending_swaps": len(self.server.trader.pending_swaps),
            "submitted_swaps": len(self.server.trader.submitted_swaps),
            "start_bankroll": self.desk.start,
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
            "okx_spot_holdings": okx_real_balance.get("holdings", []) if okx_real_balance else [],
            # ── Perp auto-close settings (current) ──
            "perp_config": {
                "tp_pct": self._perp_tp_pct,
                "sl_pct": self._perp_sl_pct,
                "max_hold_sec": self._perp_max_hold_sec,
                "max_position_usd": self._perp_max_position_usd,
                "allow_reopen": self._perp_allow_reopen,
            },
            # ── Data Source metadata for front-end status badge ──
            "data_source": plat_tag,   # "OKX" | "JUP"
            "data_source_locked": is_okx,
            "okx_cache_age_sec": round(time.time() - self._okx_last_account_ts, 1) if self._okx_last_account_ts else -1,
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
