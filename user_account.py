#!/usr/bin/env python3
"""Per-member OKX account context.

Why this module exists
----------------------
The dashboard used to render one shared account snapshot pulled with the
server's own OKX credentials. Members can now bring their own API key, and
once they do, the dashboard must show *their* balance, *their* positions and
*their* fills — while everyone else keeps seeing the system default data.

Performance contract
--------------------
The HTTP request path must never block on OKX. Every read goes through
:meth:`UserAccountManager.snapshot`, which only ever returns an in-memory
cache populated by a background worker thread. A cold cache returns ``None``
and the caller falls back to the shared desk state, so the UI never blanks.

Trading contract
----------------
``min_trade_usd`` / ``max_trade_usd`` clamp the notional of every *entry*
(manual signal or auto-exec). Exits — including one-click close and the
automatic TP/SL/time-limit closes — are deliberately exempt: a size band must
never be able to trap a member in a losing position.
"""
from __future__ import annotations

import hashlib
import threading
import time
from typing import Any

try:
    from rh_okx_executor import OKXExecutor, OKX_MIN_ORDER_USD, TD_MODE_SPOT
except ImportError:  # pragma: no cover - OKX is optional at import time
    OKXExecutor = None
    OKX_MIN_ORDER_USD = 10.0
    TD_MODE_SPOT = "cross"


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


class UserAccountManager:
    """Owns one :class:`OKXExecutor` per member plus its cached snapshot.

    Parameters
    ----------
    db:
        ``TradeDB`` used to enumerate members that finished their setup.
    refresh_sec:
        Minimum age of a snapshot before the worker refreshes it.
    demo:
        When the whole site runs against OKX demo trading, member executors
        must use the same header, otherwise orders are silently rejected.
    """

    def __init__(self, db, refresh_sec: float = 30.0, demo: bool = False,
                 builder_code: str = ""):
        self._db = db
        self._refresh_sec = max(5.0, float(refresh_sec))
        self._demo = demo
        self._builder_code = builder_code

        self._lock = threading.RLock()
        # user_id → {"exe": OKXExecutor, "fp": credentials fingerprint}
        self._executors: dict[int, dict] = {}
        # user_id → {"balance":…, "positions":…, "trades":…, "ts":…, "error":…}
        self._snapshots: dict[int, dict] = {}
        self._settings: dict[int, dict] = {}

        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ── lifecycle ──────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._worker, name="user-account-refresher", daemon=True
        )
        self._thread.start()
        print(f"[UserAccount] refresher started (interval={self._refresh_sec:.0f}s)")

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def _worker(self) -> None:
        """Refresh one member per wake-up so a burst of members never spams OKX.

        The worker prioritises members whose snapshot is missing or oldest, so
        a newly connected member sees their own data within one tick.
        """
        while not self._stop.is_set():
            try:
                members = self._db.get_users_with_api_keys()
            except Exception as e:  # DB hiccup must not kill the thread
                print(f"[UserAccount] member scan failed: {e}")
                members = []

            now = time.time()
            with self._lock:
                self._settings = {int(m["user_id"]): m for m in members}
                # Drop snapshots for members that removed their keys.
                for uid in list(self._snapshots):
                    if uid not in self._settings:
                        self._snapshots.pop(uid, None)
                        self._executors.pop(uid, None)

            # Oldest / never-fetched first
            due = [
                m for m in members
                if now - self._snapshots.get(int(m["user_id"]), {}).get("ts", 0.0)
                >= self._refresh_sec
            ]
            due.sort(key=lambda m: self._snapshots.get(int(m["user_id"]), {}).get("ts", 0.0))

            if due:
                self._refresh_one(int(due[0]["user_id"]))
            else:
                self._wake.wait(timeout=min(self._refresh_sec, 10.0))
                self._wake.clear()

    def _refresh_one(self, user_id: int) -> None:
        with self._lock:
            settings = self._settings.get(user_id)
        if not settings:
            # Not loaded yet this cycle — pull it directly.
            try:
                settings = next(
                    (m for m in self._db.get_users_with_api_keys()
                     if int(m["user_id"]) == user_id), None
                )
            except Exception:
                settings = None
            if not settings:
                return
            with self._lock:
                self._settings[user_id] = settings

        exe = self._executor_for(user_id, settings)
        if exe is None:
            return
        snap = self._fetch_snapshot(exe)
        bal = snap.get("balance")
        if bal:
            # PnL must be measured against THIS member's own equity, never the
            # shared desk's starting bankroll.
            baseline, baseline_day = self._baseline_for(
                user_id, _f(bal.get("total_eq_usd"))
            )
            snap["baseline_eq"] = baseline
            snap["baseline_day"] = baseline_day
        snap["ts"] = time.time()
        with self._lock:
            self._snapshots[user_id] = snap

    def _baseline_for(self, user_id: int, current_eq: float) -> tuple[float, str]:
        """Daily equity baseline, captured on the first snapshot of each day.

        Persisted in the settings table so a server restart does not wipe the
        member's "today" reference point.
        """
        key = f"acct_baseline_{user_id}"
        today = time.strftime("%Y-%m-%d")
        raw = ""
        try:
            raw = self._db.get_setting(key, "")
        except Exception:
            raw = ""
        if raw:
            parts = raw.split("|", 1)
            if len(parts) == 2 and parts[0] == today:
                try:
                    return float(parts[1]), today
                except ValueError:
                    pass
        if current_eq > 0:
            try:
                self._db.set_setting(key, f"{today}|{current_eq}")
            except Exception:
                pass
        return current_eq, today

    # ── executors ──────────────────────────────────────────────────────────

    @staticmethod
    def _fingerprint(settings: dict) -> str:
        raw = "|".join(str(settings.get(k, "")) for k in
                       ("api_key", "api_secret", "api_passphrase"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _executor_for(self, user_id: int, settings: dict):
        """Build (or reuse) the member's executor. Rebuilds when keys change."""
        if OKXExecutor is None:
            return None
        fp = self._fingerprint(settings)
        with self._lock:
            rec = self._executors.get(user_id)
            if rec and rec["fp"] == fp:
                return rec["exe"]
        try:
            exe = OKXExecutor(
                api_key=settings.get("api_key"),
                api_secret=settings.get("api_secret"),
                passphrase=settings.get("api_passphrase"),
                ai_builder_code=self._builder_code or None,
                demo=self._demo,
            )
        except Exception as e:
            print(f"[UserAccount] executor build failed for user {user_id}: {e}")
            return None
        if not getattr(exe, "_auth_ready", False):
            return None
        with self._lock:
            self._executors[user_id] = {"exe": exe, "fp": fp}
        print(f"[UserAccount] executor ready for user {user_id} (demo={exe.demo})")
        return exe

    def executor_for_user(self, user_id: int, settings: dict | None = None):
        """Public accessor used by the request path for order placement."""
        if settings is None:
            with self._lock:
                settings = self._settings.get(user_id)
        if settings is None:
            try:
                settings = self._db.get_user_settings(user_id)
            except Exception:
                settings = None
        if not settings or not settings.get("api_key"):
            return None
        with self._lock:
            self._settings.setdefault(user_id, settings)
        return self._executor_for(user_id, settings)

    # ── snapshots ──────────────────────────────────────────────────────────

    def _fetch_snapshot(self, exe) -> dict:
        """Pull balance + positions + fills once. Never raises."""
        out: dict[str, Any] = {
            "balance": None, "positions": [], "trades": [],
            "error": None, "ts": 0.0,
        }
        try:
            raw_positions = exe.get_positions()
            summary = exe.get_account_summary(perp_positions=raw_positions)
            out["balance"] = {
                "total_eq_usd": summary["total_eq_usd"],
                "usdt_avail": summary["usdt_avail"],
                "usdt_eq": summary["usdt_eq"],
                "holdings": summary["holdings"],
                "perp_upl": summary["perp_upl"],
            }
            perp: list[dict] = []
            for pos in raw_positions or []:
                inst_id = pos.get("instId", "")
                if "-SWAP" not in inst_id:
                    continue
                pos_size = _f(pos.get("pos"))
                if abs(pos_size) < 0.0001:
                    continue
                avg_px = _f(pos.get("avgPx"))
                last_px = _f(pos.get("last")) or avg_px or 1.0
                upl = _f(pos.get("upl"))
                inst = inst_id.replace("-USDT-SWAP", "")
                perp.append({
                    "inst_id": inst_id,
                    "ticker": inst,
                    "side": "LONG" if pos_size > 0 else "SHORT",
                    "size": pos_size,
                    "size_usd": round(abs(pos_size) * last_px, 2),
                    "avg_px": avg_px,
                    "last_px": last_px,
                    "upl": round(upl, 2),
                    "lever": pos.get("lever", "?"),
                    "entry_usd": round(abs(pos_size) * avg_px, 2),
                    "notional": round(abs(pos_size) * last_px, 2),
                    "mark_price": last_px,
                    "margin": round(_f(pos.get("margin")) or _f(pos.get("imr")), 2),
                })
            perp.sort(key=lambda x: abs(x["size_usd"]), reverse=True)
            out["positions"] = perp
        except Exception as e:
            out["error"] = f"positions/balance: {e}"

        try:
            out["trades"] = exe.get_order_history(limit=50)
        except Exception as e:
            if not out["error"]:
                out["error"] = f"history: {e}"
        return out

    def snapshot(self, user_id: int) -> dict | None:
        """Cache-only read used by /api/desk-state. Never performs I/O."""
        with self._lock:
            snap = self._snapshots.get(user_id)
            if snap is not None:
                return snap
        # Cold cache: ask the worker to prioritise this member, then bail so
        # the caller can serve the default state for this tick.
        self.request_refresh(user_id)
        return None

    def request_refresh(self, user_id: int) -> None:
        """Nudge the worker to refresh soon (non-blocking)."""
        self._wake.set()

    def force_refresh(self, user_id: int, settings: dict | None = None) -> dict | None:
        """Blocking refresh — used after a trade action so the UI updates now."""
        if settings is not None:
            with self._lock:
                self._settings[user_id] = settings
        self._refresh_one(user_id)
        with self._lock:
            return self._snapshots.get(user_id)

    def has_keys(self, user_id: int, settings: dict | None = None) -> bool:
        if settings is None:
            with self._lock:
                settings = self._settings.get(user_id)
        if settings is None:
            try:
                settings = self._db.get_user_settings(user_id)
            except Exception:
                settings = None
        if not settings:
            return False
        return bool(
            str(settings.get("api_key") or "").strip()
            and str(settings.get("api_secret") or "").strip()
            and str(settings.get("api_passphrase") or "").strip()
        )

    def status(self, user_id: int) -> dict:
        with self._lock:
            snap = self._snapshots.get(user_id)
        return {
            "keys_configured": self.has_keys(user_id),
            "cache_age_sec": round(time.time() - snap["ts"], 1) if snap else -1,
            "error": (snap or {}).get("error"),
            "connected": bool(snap and snap.get("balance")),
        }

    # ── per-trade amount band ──────────────────────────────────────────────

    def clamp_stake(self, user_id: int, usd: float, settings: dict | None = None) -> tuple[float, str]:
        """Clamp an *entry* notional into the member's allowed band.

        Returns ``(amount, reason)`` — ``reason`` is empty when untouched so
        callers can surface a precise message in the UI/feed.
        """
        if settings is None:
            with self._lock:
                settings = self._settings.get(user_id)
        if settings is None:
            try:
                settings = self._db.get_user_settings(user_id)
            except Exception:
                settings = None

        band = self.amount_band(settings)
        lo, hi = band["min"], band["max"]
        amt = _f(usd)
        if amt <= 0:
            return lo, f"stake ${amt:.2f} invalid, floored to ${lo:.2f}"
        if amt < lo:
            return lo, f"stake ${amt:.2f} below min ${lo:.2f}, raised"
        if amt > hi:
            return hi, f"stake ${amt:.2f} above max ${hi:.2f}, capped"
        return amt, ""

    @staticmethod
    def amount_band(settings: dict | None) -> dict:
        """Normalise min/max from a settings row, tolerating legacy rows."""
        s = settings or {}
        lo = _f(s.get("min_trade_usd"), 0.0) or 0.0
        hi = _f(s.get("max_trade_usd"), 0.0) or 0.0
        # Legacy rows only carried max_position_usd.
        if hi <= 0:
            hi = _f(s.get("max_position_usd"), 100.0) or 100.0
        lo = max(lo, 0.0)
        if lo > hi:
            lo = hi
        return {"min": round(lo, 2), "max": round(hi, 2),
                "okx_floor": OKX_MIN_ORDER_USD}

    # ── one-click close ────────────────────────────────────────────────────

    def close_position(self, user_id: int, payload: dict,
                       settings: dict | None = None) -> dict:
        """Market-close one position on the member's own account.

        ``payload`` keys:
            kind     : "perp" | "spot"
            inst_id  : e.g. "BTC-USDT-SWAP" / "BTC-USDT"
            ticker   : display ticker (for logging)
            side     : "LONG" | "SHORT" (perp only)
            size_usd : notional to close; omit/0 → close in full

        Closes are intentionally exempt from the min/max band: exiting must
        always be possible.
        """
        exe = self.executor_for_user(user_id, settings)
        if exe is None:
            return {"ok": False, "error": "no_api_key",
                    "msg": "No OKX API key configured for this account"}

        kind = (payload.get("kind") or "perp").lower()
        inst_id = (payload.get("inst_id") or "").strip()
        ticker = (payload.get("ticker") or inst_id).strip()
        side = (payload.get("side") or "").upper()
        size_usd = _f(payload.get("size_usd"))
        cl_ord_id = f"mclose{user_id}{kind[0]}{ticker}{int(time.time())}"

        # Resolve against the freshest snapshot so a stale UI can't send a
        # nonsense size.
        snap = None
        with self._lock:
            snap = self._snapshots.get(user_id)

        try:
            if kind == "spot":
                if not inst_id and ticker:
                    inst_id = f"{ticker}-USDT"
                qty_base = 0.0
                if snap and snap.get("balance"):
                    for h in snap["balance"].get("holdings", []):
                        if (h.get("ccy") or "").upper() == ticker.upper():
                            qty_base = _f(h.get("spot_bal"))
                            break
                if qty_base <= 0 and size_usd <= 0:
                    return {"ok": False, "error": "no_position",
                            "msg": f"No spot balance for {ticker}"}
                return self._close_spot(exe, user_id, inst_id, ticker,
                                        qty_base, size_usd, cl_ord_id)

            # ── perpetual ──
            if not inst_id:
                inst_id = f"{ticker}-USDT-SWAP"
            pos = None
            if snap:
                pos = next((p for p in snap.get("positions", [])
                            if p.get("inst_id") == inst_id), None)
            if not side and pos:
                side = pos.get("side", "")
            close_side = "sell" if side == "LONG" else "buy"
            if size_usd <= 0:
                size_usd = _f((pos or {}).get("size_usd"))
            if size_usd <= 0:
                return {"ok": False, "error": "no_position",
                        "msg": f"No open position for {ticker}"}
            if size_usd < OKX_MIN_ORDER_USD:
                return {"ok": False, "error": "below_min",
                        "msg": f"Position ${size_usd:.2f} below OKX minimum "
                               f"${OKX_MIN_ORDER_USD:.0f} — close it manually in OKX"}
            result = exe.submit_market(inst_id, close_side, size_usd,
                                       cl_ord_id=cl_ord_id)
        except Exception as e:
            return {"ok": False, "error": "exception", "msg": str(e)}

        if not result.get("ok"):
            return {"ok": False, "error": "rejected",
                    "msg": result.get("msg") or "OKX rejected the close order"}

        self.force_refresh(user_id, settings)
        try:
            self._db.remove_open_trade(ticker, user_id=user_id)
        except Exception:
            pass
        return {
            "ok": True, "kind": "perp", "ticker": ticker, "inst_id": inst_id,
            "side": close_side, "size_usd": round(size_usd, 2),
            "order_id": result.get("order_id", ""),
            "msg": f"Closed {ticker} {close_side} ${size_usd:.2f}",
        }

    def _close_spot(self, exe, user_id: int, inst_id: str, ticker: str,
                    qty_base: float, size_usd: float, cl_ord_id: str) -> dict:
        """Sell a spot holding. Prefers the exact base quantity to avoid dust."""
        notional = size_usd
        if qty_base > 0:
            try:
                px = _f((exe.public_ticker(inst_id).get("data") or [{}])[0].get("last"))
                if px > 0:
                    notional = qty_base * px
            except Exception:
                pass
        if notional <= 0:
            return {"ok": False, "error": "no_position",
                    "msg": f"No spot balance for {ticker}"}

        # get_quote/build_order enforce OKX's floor, so probe with at least the
        # minimum and then pin the order to the exact base quantity.
        probe_usd = max(notional, OKX_MIN_ORDER_USD)
        quote = exe.get_quote(inst_id, "sell", probe_usd)
        order = exe.build_order(quote, cl_ord_id=cl_ord_id, td_mode=TD_MODE_SPOT)
        if qty_base > 0:
            sz = f"{qty_base:.8f}".rstrip("0").rstrip(".")
            if sz and sz != "0":
                order.sz = sz
        result = exe.submit_order(order)
        if not result.get("ok"):
            return {"ok": False, "error": "rejected",
                    "msg": result.get("msg") or "OKX rejected the close order"}

        self.force_refresh(user_id)
        try:
            self._db.remove_open_trade(ticker, user_id=user_id)
        except Exception:
            pass
        return {
            "ok": True, "kind": "spot", "ticker": ticker, "inst_id": inst_id,
            "side": "sell", "size_usd": round(notional, 2),
            "order_id": result.get("order_id", ""),
            "msg": f"Closed spot {ticker} (sell {order.sz})",
        }
