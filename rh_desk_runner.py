#!/usr/bin/env python3
"""DeskRunner — background thread that drives Desk tick loop → auto ENTRY/EXIT → LiveTrader swaps."""
from __future__ import annotations
import json
import queue
import threading
import time
from pathlib import Path
from typing import Any
import numpy as np
from rh_trencher import Desk, Fill, TokenLaunch


class DeskRunner:
    """后台线程驱动 Desk tick loop.

    完整链路:
      CSV tokens → events queue → DeskRunner tick loop
        → Desk.on_launch() → Desk.log(Fill) → signal_callback → LiveTrader.on_desk_fill → pending_swaps
        → Desk.mark_and_maybe_exit() → Desk.log(Fill) → signal_callback → LiveTrader.on_desk_fill → pending_swaps
        → 每 tick push 完整 state → SSE → 前端
    """

    def __init__(self, server: "ServerState", tick_ms: int = 400,
                 max_positions: int = 3, seed: int | None = 7,
                 queue: queue.Queue | None = None,
                 refresh_interval_min: float | None = None):
        self.server = server
        self.tick_ms = tick_ms
        self._running = threading.Event()
        self._thread: threading.Thread | None = None
        self._tick_count = 0
        self._refresh_interval_min = refresh_interval_min
        self._last_fetch_ts: float = 0  # 0 = always refresh on first loop reset

        # Load tokens
        if server.csv_path:
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

        self.closed_trades: list[dict] = []
        self.open_trades: dict[str, dict] = {}
        self.scatter_tokens = []
        seen = set()
        for t in self.raw_tokens:
            if t.ticker not in seen:
                self.scatter_tokens.append(t)
                seen.add(t.ticker)

    def _refresh_live_tokens(self) -> None:
        """Fetch fresh trending + top-volume from DexScreener via proxy.

        Updates self.raw_tokens / self.events / self.scatter_tokens and
        refreshes ticker_mint_map.json for LiveTrader.
        """
        try:
            import os
            os.environ.setdefault("HTTP_PROXY", "http://127.0.0.1:7897")
            os.environ.setdefault("HTTPS_PROXY", "http://127.0.0.1:7897")

            # Monkey-patch urllib opener to use proxy explicitly
            import urllib.request
            proxy = urllib.request.ProxyHandler({
                "http": "http://127.0.0.1:7897",
                "https": "http://127.0.0.1:7897",
            })
            no_proxy = urllib.request.ProxyHandler({})
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
                    continue  # dedup by ticker (PUMP was 8x!)
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

            # Update token lists
            self.raw_tokens = new_tokens
            self.events = sorted([(tok.t_min, tok) for tok in new_tokens], key=lambda x: x[0])
            self.scatter_tokens = list(new_tokens)

            # Refresh ticker_mint_map
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

    def start(self) -> None:
        if self._running.is_set():
            return
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
                    self._push_current_state()
                    self._tick_count += 1
                except Exception as e:
                    print(f"[DeskRunner] ERROR at tick {self._tick_count}: {e}")
                    import traceback; traceback.print_exc()
                time.sleep(self.tick_ms / 1000.0)

            # Close any remaining open positions
            self._close_remaining()
            self._push_current_state()
            print(f"[DeskRunner] replay done — {self._tick_count} ticks, {len(self.closed_trades)} closed")

            # ── Loop reset: rebuild Desk, reset pointers ──
            if not self._running.is_set():
                break
            time.sleep(3)  # 3s cooldown before next replay

            # ── Auto-refresh tokens from DexScreener ──
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
            self.closed_trades = []
            print(f"[DeskRunner] loop reset — next replay starting")

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
                self.open_trades[f.ticker] = {"entry_min": f.t_min, "entry_usd": round(entry_usd, 2)}
            elif f.side in ("EXIT", "STOP") and f.ticker in self.open_trades:
                ot = self.open_trades.pop(f.ticker)
                exit_usd = round(ot["entry_usd"] * f.multiple, 2)
                pnl_usd = round(exit_usd - ot["entry_usd"], 2)
                self.closed_trades.append({
                    "ticker": f.ticker, "entry_min": ot["entry_min"], "entry_usd": ot["entry_usd"],
                    "exit_min": f.t_min, "exit_usd": exit_usd,
                    "pnl_mult": round(f.multiple, 3), "pnl_usd": pnl_usd,
                    "why": f.note or "", "win": f.multiple >= 1.0,
                })

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
        closed = list(reversed(self.closed_trades[-50:]))
        feed = [{"t": f.t_min, "side": f.side, "ticker": f.ticker or "-",
                 "mult": round(f.multiple, 2), "note": f.note}
                for f in self.desk.feed[-20:]]

        wins = sum(1 for ct in self.closed_trades if ct["win"])
        losses = len(self.closed_trades) - wins
        monitor = {
            "multiple": round(self.desk.bankroll / self.desk.start, 2),
            "bankroll": round(self.desk.bankroll, 2),
            "total_pools": len(self.raw_tokens),
            "entries": self.desk.entered,
            "wins": wins, "losses": losses,
            "dominant_theme": self.desk.narrative.cluster_label,
            "profit_rate": round(wins / len(self.closed_trades), 3) if self.closed_trades else 0.0,
            "open_count": len(self.desk.positions),
            "state": self.desk.state.name,
            "pending_swaps": len(self.server.trader.pending_swaps),
            "submitted_swaps": len(self.server.trader.submitted_swaps),
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
            "state": self.desk.state.name,
            "narrative": {"label": self.desk.narrative.cluster_label,
                          "min_match": self.desk.narrative.min_match},
            "swaps": self.server.swaps_snapshot(),
            "sol_usd": self.server._sol_usd,
            "done": not (self.i < len(self.events) or self.pending_marks),
        }
        self._latest_state = state
        self.server._desk_state = state
        if self._queue is not None:
            try:
                self._queue.put_nowait(state)
            except queue.Full:
                if self._tick_count % 50 == 0:
                    print(f"[DeskRunner] queue.FULL at tick {self._tick_count}")
        elif self._tick_count % 30 == 0:
            print(f"[DeskRunner] self._queue is None at tick {self._tick_count}")
