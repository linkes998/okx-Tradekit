#!/usr/bin/env python3
"""Live Tick Engine — discovers new DexScreener pairs and ticks open positions.

Architecture:
  - discover_loop (15s):  polls DexScreener → constructs TokenLaunch → desk.on_launch()
  - tick_loop    (15s):  fetches current prices → desk.mark_and_maybe_exit() + observe_runner()
  - Both loops run as daemon threads inside a single TickEngine instance.

Desk.core contract (AC-L6):
  - on_launch / mark_and_maybe_exit / observe_runner / snapshot signatures are unchanged.
  - TokenLaunch gains 3 optional fields: _pairCreatedAt, _launch_price_usd, _pair_address.
"""
from __future__ import annotations

import logging
import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rh_trencher import Desk
    from fx_feed import FXFeed
    from dex_client import DexClient
else:
    from rh_trencher import Desk, TokenLaunch
    from fx_feed import FXFeed
    from dex_client import DexClient

logger = logging.getLogger(__name__)


# ── TickEngine ───────────────────────────────────────────────────────────────

class TickEngine:
    def __init__(self, desk: "Desk", dex: "DexClient", fx: "FXFeed",
                 interval: int = 15, max_age_min: int = 30,
                 min_liquidity_usd: float = 500.0):
        self.desk = desk
        self.dex = dex
        self.fx = fx
        self.interval = interval
        self.max_age_min = max_age_min
        self.min_liquidity_usd = min_liquidity_usd

        # Dedup: pairAddress → True (already seen / already in traded_pools)
        self._seen_pairs: set[str] = set()

        # Open positions: pairAddress → (TokenLaunch, launch_price_usd)
        self._open_positions: dict[str, tuple["TokenLaunch", float]] = {}

        self._stop_event = __import__("threading").Event()

    # ── discover_loop ───────────────────────────────────────────────────────

    def run_discover(self) -> None:
        """Poll DexScreener every `interval` seconds for new tokens.

        For each new pair:
          1. Skip if already seen or already in desk.traded_pools.
          2. Build TokenLaunch with live-mode fields.
          3. Call desk.on_launch(tok) → ENTRY or REJECT.
          4. On ENTRY: record in _open_positions.
        """
        logger.info("[discover] started (interval=%ds, chain=%s)",
                     self.interval, self.dex.chain)
        while not self._stop_event.wait(timeout=self.interval):
            try:
                self._discover_once()
            except Exception as exc:
                logger.warning("[discover] error: %s", exc, exc_info=True)

    def _discover_once(self) -> None:
        pairs = self.dex.search_new_tokens(
            max_age_min=self.max_age_min,
            min_liquidity_usd=self.min_liquidity_usd,
        )
        logger.info("[discover] found %d candidate pairs", len(pairs))

        fx_usd = self.fx.get_eth_usd()
        now_ms = int(time.time() * 1000)

        from rh_poll import estimate_proxy_path

        for p in pairs:
            addr = p.get("pairAddress")
            if not addr:
                continue
            if addr in self._seen_pairs:
                continue
            if addr in self.desk.traded_pools:
                self._seen_pairs.add(addr)
                continue

            # Skip if pool already exists in traded_pools by ticker (fallback dedup)
            ticker_cand = (p.get("baseToken") or {}).get("symbol") or ""

            pc_at = p.get("pairCreatedAt") or 0
            age_min = max(0, (now_ms - int(pc_at)) / 60000) if pc_at else 0
            t_min = int(max(0, age_min))  # real clock minutes, NOT replay window

            liq_usd = float((p.get("liquidity") or {}).get("usd") or 0)
            price_usd_raw = p.get("priceUsd")
            try:
                launch_price_usd = float(price_usd_raw) if price_usd_raw else 0.0
            except (ValueError, TypeError):
                launch_price_usd = 0.0

            liq_eth = liq_usd / fx_usd if fx_usd > 0 else 0.0

            # estimate_proxy_path → (proxy_path, liq_growth, sell_linked, shape)
            proxy_path, liq_growth, sell_linked, shape = estimate_proxy_path(p)

            tok = TokenLaunch(
                t_min=t_min,
                ticker=ticker_cand or "UNKNOWN",
                name=(p.get("baseToken") or {}).get("name") or "",
                description="",
                launchpad=f"DexScreener·{shape}",
                liquidity_eth=liq_eth,
                liq_growth=liq_growth,
                deployer="",
                holders=[],
                linked_groups=[],
                selling_linked=sell_linked,
                true_multiple_path=proxy_path,
                theme_hint="",
                _pool_id=addr,
                _pairCreatedAt=int(pc_at) if pc_at else 0,
                _launch_price_usd=launch_price_usd,
                _pair_address=addr,
            )

            self._seen_pairs.add(addr)
            vote = self.desk.on_launch(tok)
            if vote is None:
                logger.info("[discover] REJECT %s (t_min=%d)", tok.ticker, tok.t_min)
                continue
            logger.info(
                "[discover] ENTRY %s t_min=%d liq_eth=%.2f lg=%.1f v.narr=%.2f v.liq=%.2f",
                tok.ticker, tok.t_min, tok.liquidity_eth, tok.liq_growth,
                vote.narrative, vote.liquidity,
            )
            if launch_price_usd > 0:
                self._open_positions[addr] = (tok, launch_price_usd)

    # ── tick_loop ─────────────────────────────────────────────────────────────

    def run_tick(self) -> None:
        """Every `interval` seconds: refresh prices of open positions, check exits.

        For each open position:
          1. Batch-fetch current priceUsd via dex.batch_price().
          2. Compute current_multiple = priceUsd / launch_price_usd.
          3. Call desk.mark_and_maybe_exit(tok, t_min, current_multiple).
          4. Call desk.observe_runner(tok, current_multiple).
          5. Remove from _open_positions if position was closed.
        """
        logger.info("[tick] started (interval=%ds)", self.interval)
        while not self._stop_event.wait(timeout=self.interval):
            try:
                self._tick_once()
            except Exception as exc:
                logger.warning("[tick] error: %s", exc, exc_info=True)

    def _tick_once(self) -> None:
        if not self._open_positions:
            return

        addrs = list(self._open_positions.keys())
        price_map = self.dex.batch_price(addrs)

        to_remove: list[str] = []
        for addr, (tok, launch_price) in self._open_positions.items():
            pair_data = price_map.get(addr)
            if not pair_data:
                continue
            price_raw = pair_data.get("priceUsd")
            if price_raw is None or launch_price <= 0:
                continue
            try:
                current_multiple = float(price_raw) / launch_price
            except (ValueError, TypeError):
                continue

            now_ms = int(time.time() * 1000)
            pc_at = tok._pairCreatedAt or now_ms
            t_min = int(max(0, (now_ms - pc_at) / 60000))

            self.desk.mark_and_maybe_exit(tok, t_min, current_multiple)
            self.desk.observe_runner(tok, current_multiple)

            logger.info(
                "[tick] %s mult=%.2f t_min=%d open=%d bankroll=%.0f",
                tok.ticker, current_multiple, t_min,
                len(self.desk.positions), self.desk.bankroll,
            )

            # Clean up closed positions
            if addr not in self._open_positions:
                to_remove.append(addr)
            elif not any(p.ticker == tok.ticker for p in self.desk.positions):
                to_remove.append(addr)

        for addr in to_remove:
            self._open_positions.pop(addr, None)

        # Push state to SSE queue (Task 7)
        try:
            from rh_server import push_state
            push_state(self.desk.snapshot(int(time.time() / 60)))
        except Exception:
            pass

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def stop(self) -> None:
        self._stop_event.set()
        logger.info("[TickEngine] stop signaled")


# ── CLI entry point (standalone smoke test) ───────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )
    from rh_trencher import Desk
    from fx_feed import FXFeed
    from dex_client import DexClient

    desk = Desk(start_usd=500, live_mode=True, realistic=True, max_positions=3)
    fx = FXFeed()
    fx.start()
    dex = DexClient(chain="solana")
    engine = TickEngine(desk, dex, fx, interval=15, max_age_min=30,
                        min_liquidity_usd=300)

    print("\n═══ Live Tick Engine — running 2 cycles then stopping ═══\n", flush=True)

    # Run one discover + one tick manually (don't use threads for this test)
    engine._discover_once()
    engine._tick_once()
    engine._discover_once()
    engine._tick_once()

    print(f"\n═══ Final state ═══")
    print(f"  positions  : {len(desk.positions)}")
    print(f"  open_pos   : {len(engine._open_positions)}")
    print(f"  traded_pools: {len(desk.traded_pools)}")
    print(f"  feed entries : {len(desk.feed)}")
    print(f"  closed trades: {len(desk.closed)}")
    print(f"  bankroll     : ${desk.bankroll:.0f}")
    print(f"  multiple     : {desk.bankroll / desk.start:.2f}x")

    fx.stop()
    print("\nDone.")
