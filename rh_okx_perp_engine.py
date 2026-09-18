#!/usr/bin/env python3
"""OKXPerpEngine — real-time SWAP (perpetual) trading engine.

Supports long/short perpetual futures with configurable leverage and
margin mode (cross/isolated). Trades all available USDT-margined
perpetual contracts on OKX.

Usage:
    engine = OKXPerpEngine(trader, executor, tick_ms=5000)
    engine.start()
"""
from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from typing import Any

from rh_trencher import Fill
from rh_live_trader import LiveTrader
from rh_okx_executor import OKXExecutor, OKX_MIN_ORDER_USD


# ── Position & Config dataclasses ────────────────────────────────────────────

@dataclass
class PerpConfig:
    """Configuration for perpetual futures trading."""
    leverage: int = 5                    # default 5x leverage
    margin_mode: str = "cross"           # "cross" or "isolated"
    position_size_usd: float = 100.0     # notional size per position
    max_positions: int = 8               # max concurrent positions
    take_profit_pct: float = 3.0         # take profit threshold
    stop_loss_pct: float = -1.5          # stop loss threshold
    max_hold_sec: int = 600              # max 10 minutes per trade
    momentum_threshold_pct: float = 0.1  # minimum % change to trigger
    cooldown_sec: int = 180              # cooldown after exit
    entry_cooldown_sec: int = 300        # per-coin entry cooldown


class PerpPosition:
    """Tracks an open perpetual position with leverage-adjusted P&L."""
    __slots__ = ("ticker", "side", "inst_id", "entry_price", "notional_usd",
                 "leverage", "margin_mode", "entry_time", "peak_price",
                 "peak_notional", "stop_loss", "take_profit")

    def __init__(self, ticker: str, side: str, inst_id: str,
                 entry_price: float, notional_usd: float, leverage: int,
                 margin_mode: str):
        self.ticker = ticker
        self.side = side  # "LONG" or "SHORT"
        self.inst_id = inst_id
        self.entry_price = entry_price
        self.notional_usd = notional_usd
        self.leverage = leverage
        self.margin_mode = margin_mode
        self.entry_time = time.time()
        self.peak_price = entry_price
        self.peak_notional = notional_usd

        # Calculate stop loss and take profit levels
        if side == "LONG":
            self.stop_loss = entry_price * (1 - abs(PerpConfig.stop_loss_pct) / 100)
            self.take_profit = entry_price * (1 + PerpConfig.take_profit_pct / 100)
        else:  # SHORT
            self.stop_loss = entry_price * (1 + abs(PerpConfig.stop_loss_pct) / 100)
            self.take_profit = entry_price * (1 - PerpConfig.take_profit_pct / 100)

    @property
    def current_mult(self) -> float:
        """Current multiplier including leverage effect."""
        if self.side == "LONG":
            return self.peak_price / self.entry_price if self.entry_price > 0 else 1.0
        else:  # SHORT
            return self.entry_price / self.peak_price if self.peak_price > 0 else 1.0

    @property
    def pnl_usd(self) -> float:
        """Unrealized P&L in USD (leverage-adjusted)."""
        leverage_adj = (self.current_mult - 1) * self.leverage
        return self.notional_usd * leverage_adj

    @property
    def pnl_pct(self) -> float:
        """P&L percentage (leverage-adjusted)."""
        return (self.current_mult - 1) * self.leverage * 100

    @property
    def elapsed_sec(self) -> float:
        return time.time() - self.entry_time

    def update_price(self, current_price: float) -> None:
        """Update peak price tracking."""
        if self.side == "LONG":
            self.peak_price = max(self.peak_price, current_price)
        else:
            self.peak_price = min(self.peak_price, current_price)
        self.peak_notional = self.notional_usd * self.current_mult


# ── OKX Perpetual Trading Engine ────────────────────────────────────────────

class OKXPerpEngine:
    """Real-time SWAP (perpetual) OKX trading engine.

    Supports long/short positions with configurable leverage and margin mode.
    Trades only SWAP contracts verified to work on OKX demo.
    """

    # Verified tradeable SWAP contracts on OKX demo (tested set_leverage + submit_order)
    # Any instrument NOT in this set is filtered out BEFORE generating signals,
    # avoiding 51001 (Instrument doesn't exist) and 51087 (listing canceled) errors.
    DEMO_SWAP_WHITELIST: set[str] = {
        "BTC", "ETH", "SOL", "DOGE", "ADA", "AVAX", "LINK", "DOT", "LTC",
        "UNI", "NEAR", "APT", "SUI", "ARB", "OP", "MATIC", "ATOM", "ETC",
        "BCH", "XLM", "FIL", "ICP", "LDO", "AAVE", "MKR", "RUNE", "INJ",
    }

    # Cache of lotSz per instrument (OKX minimum order size increment)
    _lot_sz_cache: dict[str, float] = {}

    def __init__(self, trader: LiveTrader, executor: OKXExecutor,
                 config: PerpConfig | None = None,
                 tick_ms: int = 3000, queue: queue.Queue | None = None,
                 server=None):
        self.trader = trader
        self.executor = executor
        self.config = config or PerpConfig()
        self.tick_ms = tick_ms
        self._queue = queue
        self._server = server
        self._running = threading.Event()
        self._thread: threading.Thread | None = None

        # Position tracking
        self.positions: dict[str, PerpPosition] = {}
        self._traded_set: set[str] = set()

        # Stats
        self.buys_triggered = 0  # LONG positions
        self.sells_triggered = 0  # SHORT positions
        self._last_ticker_ts = 0.0
        self._last_tickers: list[dict] = []

        # Price history for momentum detection
        self._prev_prices: dict[str, float] = {}
        self._prev_tick_ts: dict[str, float] = {}

        # Account tracking
        self.start_usd = 10000.0
        self.current_usd = self.start_usd

        # Cooldown tracking
        self._exit_timestamps: dict[str, float] = {}
        self._entry_cooldowns: dict[str, float] = {}
        self._fail_cooldowns: dict[str, float] = {}  # base → when we can retry

        # Logging
        self._equity_history: list[list] = []
        self._feed_log: list[dict] = []
        self._closed_trades: list[dict] = []
        self._decision_log: list[dict] = []

    def start(self) -> None:
        if self._running.is_set():
            return

        # ── Pre-flight: check account mode — Simple Spot (type=0) can't trade SWAP ──
        acct_type, can_perp = self.executor.check_account_mode()
        if not can_perp:
            msg = (f"[OKXPerp] ABORT START: account type={acct_type} "
                   f"('Simple Spot' mode). OKX requires switching to "
                   f"Margin/Unified mode on the OKX website before "
                   f"perpetual trading is enabled.")
            print(msg)
            # Don't start — leave _running=False so engine is effectively disabled
            self._disabled_reason = msg
            return

        self._running.set()
        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                         name="OKXPerpEngine")
        self._thread.start()
        print(f"[OKXPerp] SWAP Engine STARTED (leverage={self.config.leverage}x, "
              f"margin={self.config.margin_mode}, tick_ms={self.tick_ms})")

    def stop(self) -> None:
        self._running.clear()
        if self._thread:
            self._thread.join(timeout=3)
            self._thread = None
        print(f"[OKXPerp] Engine STOPPED: {self.buys_triggered} LONGs, "
              f"{self.sells_triggered} SHORTs")

    def _run_loop(self) -> None:
        while self._running.is_set():
            try:
                self._tick()
            except Exception as e:
                print(f"[OKXPerp] ERROR in tick: {e}")
                import traceback; traceback.print_exc()
            for _ in range(int(self.tick_ms / 100)):
                if not self._running.is_set():
                    return
                time.sleep(0.1)

    def _tick(self) -> None:
        """One iteration: fetch prices, check positions, generate signals."""
        now = time.time()

        # Fetch SWAP tickers
        try:
            tickers = self.executor.public_tickers(inst_type="SWAP")
            if not tickers:
                return

            # Filter to USDT-margined perpetuals on our demo whitelist
            self._last_tickers = [
                t for t in tickers
                if t.get("instId", "").endswith("-USDT-SWAP")
                and float(t.get("last") or 0) > 0
                and t.get("instId", "").split("-")[0].upper() in self.DEMO_SWAP_WHITELIST
            ]
            self._last_ticker_ts = now
            print(f"[OKXPerp] Tick {now:.0f}: {len(self._last_tickers)} tradeable SWAPs loaded (whitelist)")
        except Exception as e:
            print(f"[OKXPerp] Failed to fetch tickers: {e}")
            return

        # Update positions with current prices
        self._update_positions()

        # Check exit conditions
        self._check_exits()

        # Generate entry signals
        self._check_entries()

        # Update price history
        for t in self._last_tickers:
            inst_id = t.get("instId", "")
            base = inst_id.split("-")[0].upper()
            price = float(t.get("last") or 0)
            self._prev_prices[base] = price
            self._prev_tick_ts[base] = now

        # Push state
        self._push_state()

    def _update_positions(self) -> None:
        """Update open positions with current prices."""
        for ticker, pos in list(self.positions.items()):
            # Find ticker in current tickers
            inst_id = pos.inst_id
            ticker_data = next(
                (t for t in self._last_tickers if t.get("instId") == inst_id),
                None
            )
            if ticker_data:
                current_price = float(ticker_data.get("last") or 0)
                if current_price > 0:
                    pos.update_price(current_price)

    def _check_exits(self) -> None:
        """Check exit conditions for all open positions."""
        for ticker, pos in list(self.positions.items()):
            # Time limit check
            if pos.elapsed_sec >= self.config.max_hold_sec:
                self._close_position(ticker, "TIME_LIMIT")
                continue

            # Stop loss check
            if pos.side == "LONG" and pos.peak_price <= pos.stop_loss:
                self._close_position(ticker, "STOP_LOSS")
            elif pos.side == "SHORT" and pos.peak_price >= pos.stop_loss:
                self._close_position(ticker, "STOP_LOSS")
                continue

            # Take profit check
            if pos.side == "LONG" and pos.peak_price >= pos.take_profit:
                self._close_position(ticker, "TAKE_PROFIT")
            elif pos.side == "SHORT" and pos.peak_price <= pos.take_profit:
                self._close_position(ticker, "TAKE_PROFIT")

    def _close_position(self, ticker: str, reason: str) -> None:
        """Close an open position."""
        pos = self.positions.get(ticker)
        if not pos:
            return

        # Get current price
        inst_id = pos.inst_id
        ticker_data = next(
            (t for t in self._last_tickers if t.get("instId") == inst_id),
            None
        )
        if not ticker_data:
            return

        current_price = float(ticker_data.get("last") or 0)
        if current_price <= 0:
            return

        # Calculate exit notional
        exit_mult = pos.current_mult
        exit_notional = pos.notional_usd * exit_mult
        pnl_usd = pos.pnl_usd

        # Submit closing order directly via executor
        try:
            close_side = "sell" if pos.side == "LONG" else "buy"
            base = pos.ticker  # derive from position to avoid undefined-variable bug
            close_qty_usd = pos.notional_usd + abs(pos.pnl_usd) if pos.pnl_usd > 0 else pos.notional_usd
            result = self.executor.submit_market(
                inst_id, close_side, close_qty_usd,
                cl_ord_id=f"close_{pos.side}_{base}_{int(time.time())}"
            )
            if result.get("ok"):
                print(f"[OKXPerp] EXIT {ticker} {pos.side} ({reason}): "
                      f"entry=${pos.notional_usd:.2f} @ ${pos.entry_price:.4f} "
                      f"exit=${close_qty_usd:.2f} @ ${current_price:.4f} "
                      f"pnl=${pos.pnl_usd:.2f} ({pos.current_mult:.3f}x {pos.leverage}x lev)")

            self._push_trade_log(ticker, close_side, exit_notional,
                                 exit_mult, reason)
            self._closed_trades.append({
                "ticker": ticker,
                "side": pos.side,
                "entry_price": pos.entry_price,
                "exit_price": current_price,
                "notional_usd": pos.notional_usd,
                "pnl_usd": pnl_usd,
                "leverage": pos.leverage,
                "reason": reason,
                "elapsed_sec": pos.elapsed_sec,
            })

            # Update account balance
            self.current_usd += pnl_usd

            # Track exit time
            self._exit_timestamps[ticker] = time.time()

            # Remove position
            del self.positions[ticker]
        except Exception as e:
            print(f"[OKXPerp] ERROR closing {ticker}: {e}")
            if ticker in self.positions:
                del self.positions[ticker]
            self.current_usd += pnl_usd

    def _check_entries(self) -> None:
        """Check for new entry opportunities."""
        for t in self._last_tickers:
            # ⚠️ Guard: check limits EVERY iteration (not just once at top)
            if len(self.positions) >= self.config.max_positions:
                break

            # Guard: enough margin? (notional / leverage = required margin)
            margin_needed = self.config.position_size_usd / self.config.leverage
            if self.current_usd < margin_needed * 2:  # 2x buffer
                break

            inst_id = t.get("instId", "")
            base = inst_id.split("-")[0].upper()

            # Skip if already in position
            if base in self.positions:
                continue

            # Check cooldown after exit
            if base in self._exit_timestamps:
                elapsed = time.time() - self._exit_timestamps[base]
                if elapsed < self.config.cooldown_sec:
                    continue

            # Check per-coin entry cooldown
            if base in self._entry_cooldowns:
                elapsed = time.time() - self._entry_cooldowns[base]
                if elapsed < self.config.entry_cooldown_sec:
                    continue

            # Check fail cooldown (instrument-level back-off after errors)
            if base in self._fail_cooldowns:
                if time.time() < self._fail_cooldowns[base]:
                    continue

            price = float(t.get("last") or 0)
            if price <= 0:
                continue

            # Calculate momentum
            open24h = float(t.get("open24h") or 0)
            if open24h > 0:
                change_24h = (price - open24h) / open24h * 100
            else:
                change_24h = self._get_intraday_change_pct(base, price)

            # On first tick, use 24h change
            is_first_tick = base not in self._prev_tick_ts
            if is_first_tick:
                change_pct = change_24h
            else:
                change_pct = self._get_intraday_change_pct(base, price)

            # Determine direction based on momentum
            if abs(change_pct) >= self.config.momentum_threshold_pct:
                direction = "LONG" if change_pct > 0 else "SHORT"

                # Avoid extreme moves
                if abs(change_pct) > 20:
                    print(f"[OKXPerp] SKIP {base}: extreme move {change_pct:+.2f}%")
                    continue

                # Log signal
                log_msg = f"[OKXPerp] SIGNAL {base}: {direction} "
                log_msg += f"delta={change_pct:+.2f}% 24h={change_24h:+.2f}%"
                if is_first_tick:
                    log_msg += " (first tick)"
                print(log_msg)

                # Set leverage BEFORE placing any SWAP order (OKX defaults to 1x)
                try:
                    self.executor.set_leverage(
                        inst_id, self.config.leverage,
                        mgn_mode=self.config.margin_mode,
                    )
                    print(f"[OKXPerp] Leverage set: {self.config.leverage}x {self.config.margin_mode} for {inst_id}")
                except Exception as e:
                    print(f"[OKXPerp] WARNING: set_leverage failed for {inst_id}: {e}")
                    # Continue anyway — leverage may already be set

                # Submit entry order directly via executor
                okx_side = "buy" if direction == "LONG" else "sell"
                try:
                    result = self.executor.submit_market(
                        inst_id, okx_side, self.config.position_size_usd,
                        cl_ord_id=f"{direction}_{base}_{int(time.time())}",
                        td_mode=self.config.margin_mode,
                    )
                except Exception as e:
                    err_str = str(e)
                    # Error-specific handling: different cooldowns
                    if "sCode=51001" in err_str:
                        # Instrument doesn't exist → blacklist for 10 min
                        cooldown = 600
                        print(f"[OKXPerp] BLACKLIST {base}: 51001 instrument not found (cool {cooldown}s)")
                    elif "sCode=51121" in err_str or "lot size" in err_str.lower():
                        # Lot size mismatch → skip 1 min
                        cooldown = 60
                        print(f"[OKXPerp] SKIP {base}: 51121 lot size mismatch (cool {cooldown}s)")
                    elif "sCode=51008" in err_str or "insufficient" in err_str.lower():
                        # Insufficient margin → cool 2 min
                        cooldown = 120
                        print(f"[OKXPerp] SKIP {base}: 51008 insufficient margin (cool {cooldown}s)")
                    elif "sCode=51087" in err_str:
                        # Listing canceled → blacklist
                        cooldown = 600
                        print(f"[OKXPerp] BLACKLIST {base}: 51087 listing canceled (cool {cooldown}s)")
                    elif "SSL" in err_str or "Connection" in err_str or "timeout" in err_str.lower():
                        # Network/SSL issue → brief cool
                        cooldown = 10
                        print(f"[OKXPerp] NET {base}: network error (cool {cooldown}s)")
                    else:
                        cooldown = 120
                        print(f"[OKXPerp] ERR {base}: {err_str[:120]} (cool {cooldown}s)")
                    self._fail_cooldowns[base] = time.time() + cooldown
                    continue
                if result.get("ok"):
                    if direction == "LONG":
                        self.buys_triggered += 1
                    else:
                        self.sells_triggered += 1

                    self._traded_set.add(base)
                    self._entry_cooldowns[base] = time.time()

                    # Create position
                    pos = PerpPosition(
                        ticker=base,
                        side=direction,
                        inst_id=inst_id,
                        entry_price=price,
                        notional_usd=self.config.position_size_usd,
                        leverage=self.config.leverage,
                        margin_mode=self.config.margin_mode,
                    )
                    self.positions[base] = pos

                    print(f"[OKXPerp] ENTRY {direction} {base}: "
                          f"${self.config.position_size_usd:.2f} @ ${price:.4f} "
                          f"({change_pct:+.2f}%, {self.config.leverage}x lev)")

                    self._push_trade_log(base, direction,
                                         self.config.position_size_usd,
                                         1.0, "ENTRY")
                    self._decision_log.append({
                        "pair": base,
                        "side": direction,
                        "time": time.strftime("%H:%M:%S"),
                        "sizePct": f"{self.config.leverage}x",
                        "notional": self.config.position_size_usd,
                        "reasons": [
                            f"Momentum {change_pct:+.2f}%",
                            f"24h change {change_24h:+.2f}%"
                        ],
                        "riskInfo": (
                            f"Stop: -{abs(self.config.stop_loss_pct)}% | "
                            f"TP: +{self.config.take_profit_pct}% | "
                            f"Max: {self.config.max_hold_sec//60}min | "
                            f"Leverage: {self.config.leverage}x"
                        ),
                    })
                else:
                    print(f"[OKXPerp] BUILD_FAILED {direction} {base}")

    def _get_intraday_change_pct(self, ticker: str, current_price: float) -> float:
        """Calculate intraday price change percentage."""
        prev_price = self._prev_prices.get(ticker)
        if prev_price and prev_price > 0:
            prev_ts = self._prev_tick_ts.get(ticker, 0)
            elapsed = time.time() - prev_ts
            if elapsed >= self.tick_ms / 1000:
                return (current_price - prev_price) / prev_price * 100
        return 0.0

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
            "note": fill.note,
        })

    def _push_state(self) -> None:
        """Push engine state to queue and server."""
        try:
            now_min = int(time.time())

            # Calculate equity with unrealized P&L
            equity_val = self.current_usd
            for pos in self.positions.values():
                equity_val += pos.pnl_usd

            # Accumulate equity history
            if not self._equity_history or now_min - self._equity_history[-1][0] >= 2:
                self._equity_history.append([now_min, equity_val])
                if len(self._equity_history) > 600:
                    self._equity_history = self._equity_history[-500:]

            # Prepare state
            state = {
                "t": now_min,
                "state": "LIVE",
                "bankroll": round(equity_val, 2),
                "multiple": round(equity_val / self.start_usd, 4),
                "seen": len(self._last_tickers),
                "entered": len(self.positions),
                "rejected": 0,
                "theme": "okx-live-swap",
                "expectancy": 0.0,
                "full_kelly": 0.5,
                "used_kelly": 0.35,
                "ruin": 0.01,
                "open": list(self.positions.keys())[0] if self.positions else "",
                "open_positions": [
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
                    for p in self.positions.values()
                ],
                # Include equity history so the equity chart updates via SSE too
                "equity": self._equity_history[-300:],
                # Include perp positions directly so SSE recipients see them
                "perp_positions": [
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
                    for p in self.positions.values()
                ],
                "perp_stats": {
                    "longs_triggered": self.buys_triggered,
                    "shorts_triggered": self.sells_triggered,
                    "open_count": len(self.positions),
                },
                "perp_config": {
                    "leverage": self.config.leverage,
                    "margin_mode": self.config.margin_mode,
                    "position_size": self.config.position_size_usd,
                    "max_positions": self.config.max_positions,
                },
            }
            self._queue.put_nowait(state)
            if self._server is not None:
                # Preserve existing SPOT engine fields (monitor, equity, feed, etc.)
                # Only update perp-specific fields to avoid wiping SPOT data
                existing = getattr(self._server, "_desk_state", None) or {}
                merged = dict(existing)
                # Perp-specific keys that should override
                for k in ("perp_positions", "perp_stats", "perp_config",
                          "open_positions", "equity"):
                    if k in state:
                        merged[k] = state[k]
                self._server._desk_state = merged
        except queue.Full:
            pass

    def status(self) -> dict[str, Any]:
        """Return current engine status."""
        # Calculate total equity with unrealized P&L
        equity = self.current_usd
        for pos in self.positions.values():
            equity += pos.pnl_usd

        return {
            "running": self._running.is_set(),
            "positions": len(self.positions),
            "balances": {
                "current_usd": round(equity, 2),
                "start_usd": self.start_usd,
                "pnl_usd": round(equity - self.start_usd, 2),
                "pnl_pct": round((equity / self.start_usd - 1) * 100, 2),
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
                    "entry_usd": p.notional_usd,
                    "current_usd": round(p.peak_notional, 2),
                    "mult": round(p.current_mult, 4),
                    "pnl_pct": round(p.pnl_pct, 2),
                    "leverage": p.leverage,
                    "margin_mode": p.margin_mode,
                    "elapsed_sec": round(p.elapsed_sec, 0),
                }
                for p in self.positions.values()
            ],
            "config": {
                "leverage": self.config.leverage,
                "margin_mode": self.config.margin_mode,
                "position_size_usd": self.config.position_size_usd,
                "max_positions": self.config.max_positions,
            },
        }
