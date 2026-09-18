#!/usr/bin/env python3
"""OKXLiveEngine — real-time SPOT trading engine (no replay/scenario).

Reads OKX SPOT market data and executes real trades based on
price movement, volume, and momentum signals. Trades existing
altcoin holdings (not USDT-based).

Usage:
    engine = OKXLiveEngine(trader, executor, tick_ms=5000)
    engine.start()
"""
from __future__ import annotations

import queue
import threading
import time
from typing import Any

from rh_trencher import Fill
from rh_live_trader import LiveTrader
from rh_okx_executor import OKXExecutor


# Position state tracking
class OKXPosition:
    """Tracks an open SPOT position with real-time P&L."""
    __slots__ = ("ticker", "side", "entry_price", "entry_usd",
                 "entry_time", "peak_price", "peak_usd", "inst_id",
                 "size_base", "current_price")

    def __init__(self, ticker: str, side: str, entry_price: float,
                 entry_usd: float, inst_id: str,
                 size_base: float = 0.0, current_price: float = 0.0):
        self.ticker = ticker
        self.side = side  # "BUY" or "SELL"
        self.entry_price = entry_price
        self.entry_usd = entry_usd
        self.entry_time = time.time()
        self.peak_price = entry_price
        self.peak_usd = entry_usd
        self.inst_id = inst_id
        self.size_base = size_base       # actual token quantity held
        self.current_price = current_price  # latest market price

    @property
    def current_mult(self) -> float:
        if self.side == "BUY":
            return self.peak_price / self.entry_price if self.entry_price > 0 else 1.0
        return self.entry_price / self.peak_price if self.peak_price > 0 else 1.0

    @property
    def pnl_usd(self) -> float:
        # P&L = current equity - original equity
        current_eq = self.size_base * self.peak_price
        original_eq = self.size_base * self.entry_price if self.entry_price > 0 else self.entry_usd
        return current_eq - original_eq

    @property
    def pnl_pct(self) -> float:
        if self.entry_usd <= 0:
            return 0.0
        return self.pnl_usd / self.entry_usd * 100


class OKXLiveEngine:
    """Real-time SPOT OKX trading engine.

    Replaces DeskRunner's deterministic replay with live price-based decisions.
    Fetches OKX SPOT tickers every tick_ms, tracks positions, and submits orders
    when conditions are met. Trades existing altcoin holdings.
    """

    # Entry/exit thresholds (tunable)
    EXIT_TAKE_PROFIT_PCT = 5.0         # take profit at +5%
    EXIT_STOP_LOSS_PCT = -2.0          # stop loss at -2%
    EXIT_TIME_LIMIT_SEC = 300          # force exit after 5 minutes
    MAX_POSITIONS = 5
    STAKE_USD = 50.0                   # target stake per trade (needs >= $10 OKX min)
    MIN_VOLUME_USD = 500_000           # minimum 24h volume
    TICK_INTERVAL_SEC = 5              # seconds between ticks for price delta
    MOMENTUM_THRESHOLD_PCT = 0.05      # minimum % change to trigger entry

    # OKX 模拟盘可交易 USDT 现货白名单 (已验证能下单)
    # 自动从 OKX 拉取；如果拉取失败则用这个保守列表
    OKX_DEMO_TICKERS = {
        "BTC", "ETH", "SOL", "DOGE", "ADA", "LINK", "UNI", "NEAR", "APT", "LTC", "DOT",
        "MATIC", "AVAX", "LINK", "LDO", "FIL", "ICP", "GAS", "RAY", "GRAM", "ETHFI",
        "NEAR", "ICP", "DOGE", "SOL", "RAY", "GAS", "UNI", "GRAM", "LINK", "LTC",
        "ADA", "APT", "DOT", "FIL", "LDO", "ETHFI",
    }

    def __init__(self, trader: LiveTrader, executor: OKXExecutor,
                 tick_ms: int = 5000, queue: queue.Queue | None = None,
                 server=None):
        self.trader = trader
        self.executor = executor
        self.tick_ms = tick_ms
        self._queue = queue
        self._server = server
        self._running = threading.Event()
        self._thread: threading.Thread | None = None

        # Position tracking
        self.positions: dict[str, OKXPosition] = {}
        self._traded_set: set[str] = set()

        # Stats
        self.buys_triggered = 0
        self.sells_triggered = 0
        self._last_ticker_ts = 0.0
        self._last_tickers: list[dict] = []

        # Price history for momentum detection
        self._prev_prices: dict[str, float] = {}
        self._prev_tick_ts: dict[str, float] = {}

        # Equity tracking (starts at total account value)
        self.start_usd = 10000.0
        self.current_usd = self.start_usd

        # OKX account sync (every 30s)
        self._last_sync_ts = 0.0
        self.SYNC_INTERVAL_SEC = 30

        # Cooldown: don't re-enter a coin too soon after exit
        self._exit_timestamps: dict[str, float] = {}
        self.COOLDOWN_SEC = 120  # 2 minute cooldown after exiting

        # Per-ticker entry cooldown to avoid spam
        self._entry_cooldowns: dict[str, float] = {}
        self.ENTRY_COOLDOWN_SEC = 120  # 120s between entries per coin

        # Track which coins we already hold (from account balance)
        self._held_coins: set[str] = set()
        self._okx_holdings: set[str] = set()  # Real OKX holdings (synced every 30s)

        # Cumulative data for frontend display
        self._equity_history: list[list] = []
        self._feed_log: list[dict] = []
        self._closed_trades: list[dict] = []
        self._decision_log: list[dict] = []

    def start(self) -> None:
        if self._running.is_set():
            return
        # Initial sync from OKX account before starting
        self._sync_from_okx()
        self._running.set()
        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                         name="OKXLiveEngine")
        self._thread.start()
        print(f"[OKXLive] SPOT Engine STARTED tick_ms={self.tick_ms} bankroll=${self.current_usd:.2f}")

    def stop(self) -> None:
        self._running.clear()
        if self._thread:
            self._thread.join(timeout=3)
            self._thread = None
        print(f"[OKXLive] Engine STOPPED after {self.buys_triggered} buys, {self.sells_triggered} sells")

    def _run_loop(self) -> None:
        while self._running.is_set():
            now = time.time()
            # Periodic sync from OKX account (every 30s)
            if now - self._last_sync_ts >= self.SYNC_INTERVAL_SEC:
                self._sync_from_okx()
                self._last_sync_ts = now
            try:
                self._tick()
            except Exception as e:
                print(f"[OKXLive] ERROR in tick: {e}")
                import traceback; traceback.print_exc()
            for _ in range(int(self.tick_ms / 100)):
                if not self._running.is_set():
                    return
                time.sleep(0.1)

    def _sync_from_okx(self) -> None:
        """Sync bankroll and holdings from OKX account API.

        This is the SINGLE source of truth for what positions we hold.
        Every 30s we reconcile internal state with OKX reality:
          1. Import coins OKX has (spotBal > 0) into self.positions
          2. Update existing positions with OKX's latest spot_bal
          3. Remove internal positions that OKX no longer has
        """
        try:
            summary = self.executor.get_account_summary()
            usdt_avail = summary["usdt_avail"]
            total_eq = summary["total_eq_usd"]

            # Update bankroll (always use OKX truth)
            self.current_usd = total_eq if total_eq > 0 else usdt_avail
            if self.start_usd == 10000.0 and usdt_avail > 0:
                self.start_usd = usdt_avail
                print(f"[OKXLive] Initial sync: start_usd=${self.start_usd:.2f} (USDT avail)")

            okx_ccys = {h["ccy"] for h in summary["holdings"]}
            self._okx_holdings = okx_ccys
            tracked_ccys = set(self.positions.keys())

            # ── 1. IMPORT coins OKX has but we don't track ──
            untracked = okx_ccys - tracked_ccys
            imported = []
            for h in summary["holdings"]:
                ccy = h["ccy"]
                if ccy not in untracked:
                    continue
                spot_bal = h["spot_bal"]
                eq_usd = h["eq_usd"]
                if spot_bal <= 0.0000001 or eq_usd < 5.0:
                    continue  # skip dust (< $5 USD)
                inst_id = f"{ccy}-USDT"
                try:
                    price = float(self.executor.public_ticker(inst_id).get("data", [{}])[0].get("last", 0))
                except Exception:
                    price = 0
                # Fallback: derive price from eq_usd / spot_bal if ticker failed
                if price <= 0 and spot_bal > 0:
                    price = eq_usd / spot_bal
                pos = OKXPosition(
                    ticker=ccy, side="BUY",
                    entry_price=price,
                    entry_usd=eq_usd,
                    inst_id=inst_id,
                    size_base=spot_bal,
                    current_price=price,
                )
                pos.peak_price = price or 0
                pos.peak_usd = eq_usd
                self.positions[ccy] = pos
                imported.append(f"{ccy}({spot_bal:.4f}@${price:.4f}, ${eq_usd:.2f})")
                print(f"[OKXLive] IMPORT {ccy}: spotBal={spot_bal:.4f} eqUsd=${eq_usd:.2f}")

            if imported:
                print(f"[OKXLive] Sync: imported {len(imported)} OKX positions: {', '.join(imported)}")

            # ── 2. UPDATE existing positions with OKX spot_bal ──
            for h in summary["holdings"]:
                ccy = h["ccy"]
                if ccy in self.positions:
                    pos = self.positions[ccy]
                    new_bal = h["spot_bal"]
                    old_bal = pos.size_base
                    if abs(new_bal - old_bal) / max(old_bal, 0.000001) > 0.01:
                        # Bal changed significantly — update size and equity
                        pos.size_base = new_bal
                        try:
                            price = float(self.executor.public_ticker(pos.inst_id).get("data", [{}])[0].get("last", 0))
                            pos.current_price = price
                            pos.peak_price = max(pos.peak_price, price) if pos.side == "BUY" else min(pos.peak_price, price)
                            pos.entry_usd = h["eq_usd"] / (price / pos.entry_price) if price > 0 else pos.entry_usd
                            pos.peak_usd = new_bal * pos.peak_price
                        except Exception:
                            pass

            # ── 3. REMOVE positions OKX sold ──
            drifted = tracked_ccys - okx_ccys
            drifted.discard("USDT")
            for ccy in drifted:
                if ccy in self.positions:
                    pos = self.positions.pop(ccy)
                    print(f"[OKXLive] REMOVE {ccy}: vanished from OKX (sold externally or drift)")
                    self._closed_trades.append({
                        "ticker": ccy, "side": pos.side,
                        "entry_ts": pos.entry_time, "entry_usd": pos.entry_usd,
                        "entry_price": pos.entry_price,
                        "exit_ts": time.time(), "exit_usd": pos.entry_usd,
                        "exit_price": pos.entry_price,
                        "pnl_mult": 1.0, "pnl_usd": 0.0, "win": True, "drift": True,
                    })
        except Exception as e:
            print(f"[OKXLive] _sync_from_okx error: {e}")

    def _tick(self) -> None:
        """One iteration: fetch prices, check positions, generate signals."""
        now = time.time()

        # Fetch fresh SPOT tickers every tick
        try:
            tickers = self.executor.public_tickers(inst_type="SPOT")
            if not tickers:
                print(f"[OKXLive] No tickers returned at {now:.0f}")
                return
            self._last_tickers = [
                t for t in tickers
                if t.get("instId", "").endswith("-USDT")
                and not t.get("instId", "").endswith("-SWAP")
                and float(t.get("vol24h") or 0) >= self.MIN_VOLUME_USD
                and float(t.get("last") or 0) > 0
            ]
            self._last_ticker_ts = now
        except Exception as e:
            print(f"[OKXLive] Failed to fetch tickers: {e}")
            return

        # Always push state first (equity accumulation must never be skipped)
        try:
            self._push_state()
        except Exception as e:
            print(f"[OKXLive] _push_state error: {e}")

        try:
            self._update_positions()
        except Exception as e:
            print(f"[OKXLive] _update_positions error: {e}")

        try:
            self._check_exits()
        except Exception as e:
            print(f"[OKXLive] _check_exits error: {e}")

        try:
            self._check_entries()
        except Exception as e:
            print(f"[OKXLive] _check_entries error: {e}")

        # Update price history
        for t in self._last_tickers:
            inst_id = t.get("instId", "")
            base = inst_id.split("-")[0].upper()
            price = float(t.get("last") or 0)
            if price > 0:
                self._prev_prices[base] = price
                self._prev_tick_ts[base] = now

    def _get_current_price(self, inst_id: str) -> float | None:
        """Get current price for an inst_id from cached tickers."""
        for t in self._last_tickers:
            if t.get("instId") == inst_id:
                return float(t.get("last") or 0)
        return None

    def _get_intraday_change_pct(self, ticker: str, current_price: float) -> float:
        """Calculate price change percentage since last observation tick."""
        if ticker in self._prev_prices:
            prev = self._prev_prices[ticker]
            if prev > 0:
                return ((current_price - prev) / prev) * 100
        return 0.0

    def _update_positions(self) -> None:
        """Update peak price/USD for all open positions using OKX spot_bal as truth."""
        for ticker, pos in list(self.positions.items()):
            price = self._get_current_price(pos.inst_id)
            if price is None:
                continue
            pos.current_price = price
            if pos.side == "BUY":
                pos.peak_price = max(pos.peak_price, price)
            else:
                pos.peak_price = min(pos.peak_price, price)
            # Always compute peak_usd from size_base (OKX truth) * peak_price
            pos.peak_usd = pos.size_base * pos.peak_price

    def _check_exits(self) -> None:
        """Check if any position should be closed."""
        for ticker, pos in list(self.positions.items()):
            pnl_pct = pos.pnl_pct
            elapsed = time.time() - pos.entry_time

            reason = None
            if pnl_pct >= self.EXIT_TAKE_PROFIT_PCT:
                reason = f"TAKE_PROFIT {pnl_pct:.1f}%"
            elif pnl_pct <= self.EXIT_STOP_LOSS_PCT:
                reason = f"STOP_LOSS {pnl_pct:.1f}%"
            elif elapsed >= self.EXIT_TIME_LIMIT_SEC:
                reason = f"TIME_EXIT {elapsed:.0f}s"

            if reason:
                self._close_position(ticker, pos, reason)

    def _close_position(self, ticker: str, pos: OKXPosition, reason: str) -> None:
        """Close a position and submit the opposite order."""
        side = "SELL" if pos.side == "BUY" else "BUY"
        exit_usd = pos.peak_usd
        pnl = pos.pnl_usd
        pnl_mult = pos.current_mult

        swap_key = f"{side}_{ticker}_{int(time.time())}"
        ps = self.trader.build_swap(swap_key, ticker, side, exit_usd, pos.inst_id)
        submit_ok = False
        if ps:
            self.trader.pending_swaps[swap_key] = ps
            result = self.trader.submit_swap(swap_key)
            submit_ok = result.get("ok", False)
            if submit_ok:
                self.sells_triggered += 1
                print(f"[OKXLive] {reason}: {ticker} {pos.side}->{side} "
                      f"entry=${pos.entry_usd:.2f} exit=${exit_usd:.2f} "
                      f"pnl=${pnl:.2f} ({pnl_mult:.2f}x)")
            else:
                print(f"[OKXLive] SUBMIT_FAILED {reason}: {ticker} result={result}")
        else:
            print(f"[OKXLive] BUILD_FAILED {reason}: {ticker}")

        # Always record to closed_trades and delete position (regardless of submit result)
        self._push_trade_log(ticker, side, exit_usd, pnl_mult, reason)
        self._closed_trades.append({
            "ticker": ticker,
            "side": pos.side,
            "entry_min": int((time.time() - pos.entry_time) / 60),
            "entry_ts": pos.entry_time,
            "entry_usd": pos.entry_usd,
            "entry_price": pos.entry_price,
            "exit_min": int((time.time() - pos.entry_time) / 60),
            "exit_ts": time.time(),
            "exit_usd": exit_usd,
            "exit_price": pos.peak_price,
            "pnl_mult": pnl_mult,
            "pnl_usd": pnl,
            "win": pnl > 0,
            "submit_ok": submit_ok,
        })
        self._exit_timestamps[ticker] = time.time()
        del self.positions[ticker]
        self.current_usd += pnl

    def _check_entries(self) -> None:
        """Check for new entry opportunities using real-time price changes."""
        if len(self.positions) >= self.MAX_POSITIONS:
            return

        for t in self._last_tickers:
            inst_id = t.get("instId", "")
            base = inst_id.split("-")[0].upper()

            # Skip if already in position
            if base in self.positions:
                continue

            # Skip if not in OKX demo tradeable whitelist
            if base not in self.OKX_DEMO_TICKERS:
                continue

            # Check cooldown after exit
            if base in self._exit_timestamps:
                elapsed = time.time() - self._exit_timestamps[base]
                if elapsed < self.COOLDOWN_SEC:
                    continue

            # Check per-coin entry cooldown
            if base in self._entry_cooldowns:
                elapsed = time.time() - self._entry_cooldowns[base]
                if elapsed < self.ENTRY_COOLDOWN_SEC:
                    continue

            price = float(t.get("last") or 0)
            if price <= 0:
                continue

            # Get 24h open price to calculate intraday change
            open24h = float(t.get("open24h") or 0)
            if open24h > 0:
                change_24h = (price - open24h) / open24h * 100
            else:
                change_24h = self._get_intraday_change_pct(base, price)

            # On first tick for this coin, use 24h change as momentum signal
            is_first_tick = base not in self._prev_tick_ts
            if is_first_tick:
                change_pct = change_24h
            else:
                change_pct = self._get_intraday_change_pct(base, price)

            # Enter on momentum: significant price movement
            if abs(change_pct) >= self.MOMENTUM_THRESHOLD_PCT:
                direction = "BUY" if change_pct > 0 else "SELL"

                # ── Key guard: respect OKX reality ──
                if direction == "BUY" and base in self.positions:
                    print(f"[OKXLive] SKIP BUY {base}: already holds {self.positions[base].size_base:.4f}")
                    continue
                if direction == "SELL" and base not in self._okx_holdings:
                    print(f"[OKXLive] SKIP SELL {base}: no holdings in OKX")
                    continue

                # Avoid chasing extreme moves
                if abs(change_pct) > 15:
                    print(f"[OKXLive] SKIP {base}: extreme move {change_pct:+.2f}%")
                    continue

                # Log signal
                if is_first_tick:
                    print(f"[OKXLive] FIRST-TICK signal {base}: {direction} 24h_change={change_24h:+.2f}%")
                else:
                    print(f"[OKXLive] DELTA signal {base}: {direction} delta={change_pct:+.3f}% 24h={change_24h:+.2f}%")

                # Submit entry order
                swap_key = f"{direction}_{base}_{int(time.time())}"
                ps = self.trader.build_swap(swap_key, base, direction,
                                             self.STAKE_USD, inst_id)
                if ps:
                    self.trader.pending_swaps[swap_key] = ps
                    result = self.trader.submit_swap(swap_key)
                    if result.get("ok"):
                        self.buys_triggered += 1
                        self._traded_set.add(base)
                        self._entry_cooldowns[base] = time.time()

                        pos = OKXPosition(
                            ticker=base,
                            side=direction,
                            entry_price=price,
                            entry_usd=self.STAKE_USD,
                            inst_id=inst_id,
                        )
                        self.positions[base] = pos
                        print(f"[OKXLive] ENTRY {direction} {base}: ${self.STAKE_USD:.2f} "
                              f"@ ${price:.6f} (delta: {change_pct:+.3f}%, 24h: {change_24h:+.2f}%)")

                        self._push_trade_log(base, direction, self.STAKE_USD, 1.0, "ENTRY")
                        self._decision_log.append({
                            "pair": base,
                            "side": direction,
                            "time": time.strftime("%H:%M:%S"),
                            "sizePct": "0.5%",
                            "reasons": [f"Momentum {change_pct:+.2f}%", f"24h change {change_24h:+.2f}%"],
                            "riskInfo": f"Stop: -2% | TP: +5% | Max: 5min",
                        })
                    else:
                        print(f"[OKXLive] BUILD_FAILED {direction} {base}: build_swap returned None")
            else:
                # silence: momentum too low (first_tick={is_first_tick})
                pass

    def _push_trade_log(self, ticker: str, side: str, usd: float,
                        mult: float, reason: str) -> None:
        """Push a trade log entry for frontend display."""
        fill = Fill(
            t_min=int(time.time() / 60),
            ticker=ticker,
            side=side,
            usd=usd,
            multiple=mult,
            note=reason,
        )
        self.trader.on_desk_fill(fill)
        self._feed_log.append({
            "t": fill.t_min,
            "ticker": fill.ticker,
            "side": fill.side,
            "usd": fill.usd,
            "mult": fill.multiple,
            "note": fill.note or "",
        })
        if len(self._feed_log) > 200:
            self._feed_log = self._feed_log[-150:]

    def _push_state(self) -> None:
        """Push state update to queue if available."""
        # Always accumulate equity history independently (never skip)
        equity_val = self.current_usd
        try:
            for pos in self.positions.values():
                try:
                    current_price = self._get_current_price(pos.inst_id)
                    if current_price and pos.entry_price > 0:
                        if pos.side == "BUY":
                            pos.peak_price = max(pos.peak_price, current_price)
                            mult = pos.peak_price / pos.entry_price
                        else:
                            pos.peak_price = min(pos.peak_price, current_price)
                            mult = pos.entry_price / pos.peak_price
                        pos.peak_usd = pos.entry_usd * mult
                        equity_val += pos.entry_usd * (mult - 1)
                except Exception:
                    pass
            now_ts = int(time.time())
            if not self._equity_history or now_ts - self._equity_history[-1][0] >= 2:
                self._equity_history.append([now_ts, equity_val])
                if len(self._equity_history) > 600:
                    self._equity_history = self._equity_history[-500:]
        except Exception:
            pass

        if self._queue is None:
            return

        now_min = int(time.time() / 60)
        try:
            trader = self.trader

            # Build real swap history from trader's actual data
            pending_items = list(trader.pending_swaps.items())[-30:]
            submitted_items = list(trader.submitted_swaps.items())[-20:]
            failed_items = list(trader.failed_swaps.items())[-5:]

            pending_list = []
            for key, ps in pending_items:
                entry = {
                    "key": key, "ticker": ps.ticker, "side": ps.side,
                    "platform": ps.platform, "amount_usd": ps.amount_usd,
                    "fee_usd": ps.fee_amount_usd, "note": ps.note,
                    "created_at": ps.created_at,
                }
                if ps.okx_order:
                    entry["okx_order"] = {
                        "inst_id": ps.okx_order.inst_id,
                        "side": ps.okx_order.side,
                        "sz": getattr(ps.okx_order, 'sz', ''),
                    }
                pending_list.append(entry)

            submitted_list = []
            for key, info in submitted_items:
                submitted_list.append({
                    "key": key,
                    "ticker": info.get("inst_id", "").split("-")[0],
                    "side": info.get("side", ""),
                    "platform": "okx",
                    "amount_usd": info.get("amount_usd", 0),
                    "fee_usd": info.get("fee_usd", 0),
                    "order_id": info.get("order_id", ""),
                    "timestamp": info.get("timestamp", 0),
                })

            failed_list = []
            for key, info in failed_items:
                failed_list.append({
                    "key": key, "error": info.get("error", ""),
                    "platform": "okx", "timestamp": info.get("timestamp", 0),
                })

            wins = self.sells_triggered
            losses = 0

            state = {
                "snap": {
                    "t": now_min,
                    "state": "LIVE" if self._running.is_set() else "STOP",
                    "bankroll": round(equity_val, 2),
                    "multiple": round(equity_val / self.start_usd, 3),
                    "seen": self.buys_triggered + self.sells_triggered,
                    "entered": self.buys_triggered,
                    "rejected": 0,
                    "theme": "okx-live-spot",
                    "expectancy": 0.0,
                    "full_kelly": 0.5,
                    "used_kelly": 0.35,
                    "ruin": 0.01,
                    "open": self.positions[list(self.positions.keys())[0]].ticker if self.positions else None,
                    "open_positions": [
                        {
                            "ticker": p.ticker,
                            "entry_min": now_min - int((time.time() - p.entry_time) / 60),
                            "size_frac": round(p.entry_usd / self.current_usd, 3) if self.current_usd > 0 else 0,
                            "peak_mult": round(p.peak_price / p.entry_price, 3) if (p.side == "BUY" and p.entry_price > 0) else (round(p.entry_price / p.peak_price, 3) if p.peak_price > 0 else 1.0),
                            "current_mult": round(p.current_mult, 3),
                        }
                        for p in self.positions.values()
                    ],
                },
                "monitor": {
                    "multiple": round(equity_val / self.start_usd, 3),
                    "bankroll": round(equity_val, 2),
                    "entries": self.buys_triggered,
                    "wins": wins,
                    "losses": losses,
                    "open_count": len(self.positions),
                    "state": "LIVE" if self._running.is_set() else "STOP",
                    "pending_swaps": len(pending_list),
                    "submitted_swaps": len(submitted_list),
                },
                "equity": self._equity_history[-300:],
                "closed_trades": self._closed_trades[-50:],
                "feed": self._feed_log[-100:],
                "swaps": {
                    "pending": pending_list,
                    "submitted": submitted_list,
                    "failed": failed_list,
                    "submitted_count": len(submitted_list),
                    "failed_count": len(failed_list),
                },
                "status": {
                    "buys_triggered": self.buys_triggered,
                    "sells_triggered": self.sells_triggered,
                    "pending_count": len(pending_list),
                    "submitted_count": len(submitted_list),
                    "failed_count": len(failed_list),
                    "balance_usd": round(self.current_usd, 2),
                },
                "okx_connected": True,
                "trade_mode": "live",
                "risk_preference": "balanced",
                "decisions": self._decision_log[-50:],
                "is_okx": True,
                "positions": [
                    {
                        "ticker": p.ticker,
                        "side": p.side,
                        "entry_min": now_min - int((time.time() - p.entry_time) / 60),
                        "entry_ts": p.entry_time,
                        "entry_usd": p.entry_usd,
                        "entry_price": p.entry_price,
                        "peak_mult": round(p.peak_price / p.entry_price, 3) if p.entry_price > 0 else 1.0,
                        "current_mult": round(p.current_mult, 3),
                        "size_frac": round(p.entry_usd / self.start_usd * 100, 1) if self.start_usd > 0 else 0.0,
                    }
                    for p in self.positions.values()
                ],
            }
            # Preserve perp engine data if available
            if self._server is not None and hasattr(self._server, "_perp_engine") and self._server._perp_engine is not None:
                pe = self._server._perp_engine
                state["perp_positions"] = [
                    {
                        "ticker": p.ticker,
                        "side": p.side,
                        "entry_price": p.entry_price,
                        "current_price": p.peak_price,
                        "notional_usd": p.notional_usd,
                        "leverage": p.leverage,
                        "margin_mode": p.margin_mode,
                        "peak_mult": round(p.current_mult, 4),
                        "pnl_usd": round(p.pnl_usd, 2),
                        "pnl_pct": round(p.pnl_pct, 2),
                        "elapsed_min": round(p.elapsed_sec / 60, 1),
                        "stop_loss": p.stop_loss,
                        "take_profit": p.take_profit,
                    }
                    for p in pe.positions.values()
                ]
                state["perp_stats"] = {
                    "longs_triggered": pe.buys_triggered,
                    "shorts_triggered": pe.sells_triggered,
                    "open_count": len(pe.positions),
                }
                state["perp_config"] = {
                    "leverage": pe.config.leverage,
                    "margin_mode": pe.config.margin_mode,
                    "position_size": pe.config.position_size_usd,
                    "max_positions": pe.config.max_positions,
                }
            self._queue.put_nowait(state)
            # Also sync to server._desk_state for /api/desk-state endpoint
            if self._server is not None:
                self._server._desk_state = state
        except queue.Full:
            pass

    def status(self) -> dict[str, Any]:
        """Return current engine status."""
        return {
            "running": self._running.is_set(),
            "positions": len(self.positions),
            "balances": {
                "current_usd": round(self.current_usd, 2),
                "start_usd": self.start_usd,
                "pnl_usd": round(self.current_usd - self.start_usd, 2),
                "pnl_pct": round((self.current_usd / self.start_usd - 1) * 100, 2),
            },
            "stats": {
                "buys_triggered": self.buys_triggered,
                "sells_triggered": self.sells_triggered,
                "open_positions": len(self.positions),
                "traded_coins": list(self._traded_set),
            },
            "open_positions": [
                {
                    "ticker": p.ticker,
                    "side": p.side,
                    "entry_usd": p.entry_usd,
                    "current_usd": round(p.peak_usd, 2),
                    "mult": round(p.current_mult, 3),
                    "pnl_pct": round(p.pnl_pct, 2),
                    "elapsed_sec": round(time.time() - p.entry_time, 0),
                }
                for p in self.positions.values()
            ],
        }
