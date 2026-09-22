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
import os
import threading
import time
from typing import Any

try:
    from rh_okx_executor import OKXExecutor, OKX_MIN_ORDER_USD, TD_MODE_SPOT
except ImportError:  # pragma: no cover - OKX is optional at import time
    OKXExecutor = None
    OKX_MIN_ORDER_USD = 10.0
    TD_MODE_SPOT = "cross"

try:
    from db_trades import (
        DEFAULT_TRADE_USD, DEFAULT_RISK, DEFAULT_MAX_HOLD_SEC, risk_profile,
    )
except ImportError:  # pragma: no cover - keeps this module importable alone
    DEFAULT_TRADE_USD = 50.0
    DEFAULT_RISK = "balanced"
    DEFAULT_MAX_HOLD_SEC = 43200

    _FALLBACK_PROFILES = {
        "conservative": {"max_pct": 0.02, "total_pct": 0.05, "k_sl": 1.2, "r": 3.5, "max_pos": 2},
        "aggressive": {"max_pct": 0.06, "total_pct": 0.12, "k_sl": 2.0, "r": 2.5, "max_pos": 5},
    }
    _FALLBACK_BALANCED = {"max_pct": 0.04, "total_pct": 0.08, "k_sl": 1.5, "r": 3.0, "max_pos": 3}

    def risk_profile(pref):  # type: ignore[misc]
        return _FALLBACK_PROFILES.get(
            str(pref or DEFAULT_RISK).strip().lower(), _FALLBACK_BALANCED)


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

    def cache_settings(self, user_id: int, settings: dict) -> None:
        """Push a freshly saved settings row into the cache so it applies now.

        The worker re-reads every member's row at least every 10s; this makes a
        just-saved per-trade amount effective immediately instead of waiting for
        that cycle to come round.
        """
        if not settings:
            return
        with self._lock:
            self._settings[int(user_id)] = dict(settings)

    def any_executor(self):
        """Any ready member executor — used for public market data only.

        The signal scanner needs an executor to call the public candles
        endpoint; it does not matter whose, since no credentials are involved.
        Returns ``None`` when no member has connected yet, in which case the
        scanner falls back to whatever the server already computed.
        """
        with self._lock:
            for rec in self._executors.values():
                exe = rec.get("exe")
                if exe is not None:
                    return exe
        return None

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
    def _band_bounds(settings: dict | None) -> tuple[float, float]:
        """The member's (min, max) entry band, tolerating legacy rows."""
        s = settings or {}
        lo = _f(s.get("min_trade_usd"), 0.0) or 0.0
        hi = _f(s.get("max_trade_usd"), 0.0) or 0.0
        # Legacy rows only carried max_position_usd.
        if hi <= 0:
            hi = _f(s.get("max_position_usd"), 100.0) or 100.0
        lo = max(lo, 0.0)
        if lo > hi:
            lo = hi
        return lo, hi

    @classmethod
    def resolve_trade_usd(cls, settings: dict | None,
                          fallback: float | None = None) -> float:
        """The one effective per-trade notional (USD) for an entry.

        Single source of truth — the default value, the member's saved value in
        会员中心 → 交易设置 and the amount actually sent to OKX all resolve here,
        so they can never disagree:

          1. the member's saved ``trade_usd``
          2. ``fallback`` (ops/system default: DB ``settings`` or env)
          3. :data:`DEFAULT_TRADE_USD` (50)

        The result is clamped into the member's ``[min, max]`` band and floored
        at OKX's own minimum, so it is safe to hand straight to the executor.
        """
        s = settings or {}
        amt = _f(s.get("trade_usd"))
        if amt <= 0:
            amt = _f(fallback) if fallback else DEFAULT_TRADE_USD
        lo, hi = cls._band_bounds(s)
        amt = min(max(amt, lo), hi)
        return max(amt, OKX_MIN_ORDER_USD)

    @classmethod
    def amount_band(cls, settings: dict | None) -> dict:
        """Normalise the entry band + effective per-trade size for one member."""
        s = settings or {}
        lo, hi = cls._band_bounds(s)
        return {"min": round(lo, 2), "max": round(hi, 2),
                "trade_usd": round(cls.resolve_trade_usd(s), 2),
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


# ── per-member auto trader ────────────────────────────────────────────────────

def in_run_window(start: str | None, end: str | None, now: float | None = None) -> bool:
    """True when *now* falls inside ``[start, end]``; empty/invalid = 24h.

    Handles windows that wrap past midnight (e.g. 22:00 → 06:00).
    """
    s = (start or "").strip()
    e = (end or "").strip()
    if not s or not e:
        return True
    try:
        sh, sm = (int(x) for x in s.split(":")[:2])
        eh, em = (int(x) for x in e.split(":")[:2])
    except (TypeError, ValueError):
        return True
    cur = time.localtime(now if now is not None else time.time())
    mins = cur.tm_hour * 60 + cur.tm_min
    lo, hi = sh * 60 + sm, eh * 60 + em
    if lo <= hi:
        return lo <= mins <= hi
    return mins >= lo or mins <= hi


def okx_position_open_seconds(pos: dict) -> int:
    """Position open time from an OKX ``/account/positions`` row, UNIX seconds.

    OKX reports ``cTime`` (created) / ``uTime`` (last adjusted) in MILLISECONDS;
    there is no ``cups`` key. ``0`` means unknown and makes the caller skip the
    time stop rather than guess.
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


class MemberTrader:
    """Auto-trades each opted-in member's OWN OKX account by their OWN rules.

    Why this exists
    ---------------
    :class:`~rh_desk_runner.DeskRunner` auto-trades the *shared system* account
    only. A member who brought their own API key expects their own account to
    trade by their own rules — the very values 会员中心 → 交易设置 writes:

      * ``trade_usd``            per-trade notional  (default $50)
      * ``take_profit_pct`` / ``stop_loss_pct``      TP/SL thresholds
      * ``allowed_tickers``      optional ticker filter
      * ``run_start_time`` / ``run_end_time``        trading window (empty = 24h)
      * ``trade_mode == "auto"``  master opt-in

    Because the amount is read through :meth:`UserAccountManager.resolve_trade_usd`
    the value a member saves is exactly the value executed — the default, the
    saved value and the order can never disagree.

    Safety gates (all opt-in, AND-ed)
    ---------------------------------
      * complete API credentials on file,
      * ``trade_mode == "auto"``,
      * inside the run window,
      * ``OKX_MEMBER_AUTO_TRADE`` env not disabled,
      * per-ticker entry cooldown, delisting blacklist, OKX minimum size,
      * max concurrent positions cap.

    The engine never touches the shared system account: every order goes through
    :meth:`UserAccountManager.executor_for_user`.
    """

    def __init__(self, db, account_mgr, default_trade_usd: float | None = None,
                 signal_cfg: dict | None = None, candidates_fn=None,
                 interval: float | None = None):
        self._db = db
        self._mgr = account_mgr
        self._default_trade_usd = _f(default_trade_usd) or DEFAULT_TRADE_USD
        self._signal_cfg = signal_cfg or {}
        # Optional callable -> [{"ticker","atr_pct","reason"}]; when wired to the
        # shared desk it reuses tokens it already scored (no extra candle calls).
        self._candidates_fn = candidates_fn

        self._interval = float(interval if interval is not None
                               else os.environ.get("OKX_MEMBER_TRADE_INTERVAL_SEC", "60"))
        self._interval = max(10.0, self._interval)
        # Max concurrent positions: env pins it, otherwise the risk profile decides.
        _mp_env = os.environ.get("OKX_MEMBER_MAX_POSITIONS")
        self._max_positions_env: int | None = int(_mp_env) if _mp_env else None
        self._max_positions: int = self._max_positions_env or int(
            risk_profile(DEFAULT_RISK)["max_pos"])
        # ── ATR-adaptive SL/TP bounds (mirror the system desk) ──
        self._atr_sl_floor = float(os.environ.get("OKX_ATR_SL_FLOOR", "0.8"))
        self._atr_sl_cap = float(os.environ.get("OKX_ATR_SL_CAP", "4.0"))
        self._atr_tp_floor = float(os.environ.get("OKX_ATR_TP_FLOOR", "2.0"))
        self._atr_tp_cap = float(os.environ.get("OKX_ATR_TP_CAP", "12.0"))
        self._atr_use_min = float(os.environ.get("OKX_ATR_USE_MIN", "0.05"))
        # Fallback holding time when the member never set one (12h, same as desk).
        self._member_max_hold_sec = int(os.environ.get(
            "OKX_MEMBER_MAX_HOLD_SEC", str(int(DEFAULT_MAX_HOLD_SEC))))
        self._entry_cooldown = float(os.environ.get("OKX_MEMBER_ENTRY_COOLDOWN_SEC", "300"))
        self._close_cooldown = float(os.environ.get("OKX_MEMBER_CLOSE_COOLDOWN_SEC", "90"))
        self._leverage = int(os.environ.get("OKX_PERP_LEVERAGE", "5"))
        self._tickers = [t.strip().upper() for t in (
            os.environ.get("OKX_MEMBER_TICKERS")
            or "BTC,ETH,SOL,DOGE,ADA,LINK,UNI,NEAR,APT,LTC,DOT"
        ).split(",") if t.strip()]

        # (user_id, inst_id|ticker) keyed state
        self._sl_tp: dict[tuple[int, str], tuple[float, float]] = {}
        self._last_entry: dict[tuple[int, str], float] = {}
        self._last_exit: dict[tuple[int, str], float] = {}
        self._delisted: set[str] = set()
        self._sig_cache: list[dict] = []
        self._sig_ts: float = 0.0
        self._sig_ttl: float = 120.0
        self._cycles = 0
        self._orders = 0

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ── lifecycle ──────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="member-trader", daemon=True)
        self._thread.start()
        print(f"[MemberTrader] started (interval={self._interval:.0f}s "
              f"max_pos={self._max_positions} entry_cooldown={self._entry_cooldown:.0f}s "
              f"leverage={self._leverage}x)")

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(timeout=self._interval):
            try:
                self.cycle()
            except Exception as e:
                print(f"[MemberTrader] cycle error: {e}")
                import traceback
                traceback.print_exc()

    # ── one cycle over every member ────────────────────────────────────────

    def cycle(self) -> None:
        try:
            members = self._db.get_users_with_api_keys()
        except Exception as e:
            print(f"[MemberTrader] member scan failed: {e}")
            return
        self._cycles += 1
        active = 0
        for m in members or []:
            try:
                if self._trade_member(m):
                    active += 1
            except Exception as e:
                print(f"[MemberTrader] member {m.get('user_id')} error: {e}")
        if members:
            print(f"[MemberTrader] cycle #{self._cycles}: {len(members)} member(s), "
                  f"{active} auto-trading, {self._orders} order(s) since start")

    def _trade_member(self, m: dict) -> bool:
        uid = int(m.get("user_id") or 0)
        if uid <= 0:
            return False
        exe = self._mgr.executor_for_user(uid, m)
        if exe is None:
            return False
        try:
            raw = exe.get_positions()
        except Exception as e:
            print(f"[MemberTrader] user {uid}: positions fetch failed: {e}")
            return False

        positions = self._normalize(raw)
        self._manage_closes(uid, m, exe, positions)

        # Master opt-in — anything other than "auto" means hands off.
        if str(m.get("trade_mode") or "").lower() != "auto":
            return False
        if not in_run_window(m.get("run_start_time"), m.get("run_end_time")):
            return False
        held = {p["ticker"].upper() for p in positions}
        self._scan_entries(uid, m, exe, held, positions)
        return True

    # ── helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize(raw: list) -> list[dict]:
        """OKX ``/account/positions`` rows → SWAP-only normalised dicts."""
        out: list[dict] = []
        for pos in raw or []:
            inst_id = pos.get("instId", "")
            if "-SWAP" not in inst_id:
                continue
            size = _f(pos.get("pos"))
            if abs(size) < 1e-8:
                continue
            avg_px = _f(pos.get("avgPx"))
            last_px = _f(pos.get("last")) or _f(pos.get("markPx")) or avg_px
            out.append({
                "inst_id": inst_id,
                "ticker": inst_id.replace("-USDT-SWAP", ""),
                "side": "LONG" if size > 0 else "SHORT",
                "size": size,
                "avg_px": avg_px,
                "last_px": last_px,
                "upl": round(_f(pos.get("upl")), 2),
                "pos_side": pos.get("posSide", "net"),
                # Drives the member time stop; 0 = unknown → skip the time stop.
                "open_time": okx_position_open_seconds(pos),
                "size_usd": round(abs(size) * last_px, 2),
            })
        return out

    @staticmethod
    def _default_sl_tp(settings: dict) -> tuple[float, float]:
        """The member's own TP/SL from 会员中心 → 交易设置."""
        sl = abs(_f(settings.get("stop_loss_pct"), 1.5)) or 1.5
        tp = abs(_f(settings.get("take_profit_pct"), 3.0)) or 3.0
        return max(0.1, sl), max(0.1, tp)

    def _sl_tp_for(self, settings: dict, atr_pct: float | None) -> tuple[float, float]:
        """ATR-adaptive SL/TP, scaled by the member's risk preference.

        A fixed 50/10 style pair does not survive noisy coins, so the adaptive
        path is primary; the member's saved pair is only the fallback for when
        the ATR is unknown.
        """
        a = _f(atr_pct)
        if a < self._atr_use_min:
            return self._default_sl_tp(settings)
        prof = risk_profile(settings.get("risk_preference"))
        sl = min(max(_f(prof["k_sl"]) * a, self._atr_sl_floor), self._atr_sl_cap)
        tp = min(max(_f(prof["r"]) * sl, self._atr_tp_floor), self._atr_tp_cap)
        return round(sl, 4), round(tp, 4)

    def _equity_for(self, uid: int, exe) -> float:
        """Member account equity — prefers the manager's cached snapshot."""
        bal: dict = {}
        try:
            snap = self._mgr.snapshot(uid)
            bal = ((snap or {}).get("balance") or {})
        except Exception:
            bal = {}
        eq = _f(bal.get("total_eq_usd"))
        if eq <= 0:
            try:
                eq = _f(exe.get_account_summary().get("total_eq_usd"))
            except Exception:
                eq = 0.0
        return max(0.0, eq)

    # ── exits ──────────────────────────────────────────────────────────────

    def _manage_closes(self, uid: int, m: dict, exe, positions: list[dict]) -> None:
        now = time.time()
        # Per-member holding limit; falls back to 12h, same as the system desk.
        max_hold = int(_f(m.get("max_hold_sec")) or self._member_max_hold_sec)
        changed = False
        for p in positions:
            avg_px, last_px = p["avg_px"], p["last_px"]
            if avg_px <= 0 or last_px <= 0:
                continue
            key = (uid, p["ticker"])
            if now - self._last_exit.get(key, 0.0) < self._close_cooldown:
                continue
            pct = ((last_px - avg_px) if p["side"] == "LONG"
                   else (avg_px - last_px)) / avg_px * 100
            sl, tp = self._sl_tp.get((uid, p["inst_id"])) or self._sl_tp_for(m, None)
            reason = None
            if pct >= tp:
                reason = f"TAKE_PROFIT +{pct:.1f}% (TP {tp:.2f}%)"
            elif pct <= -sl:
                reason = f"STOP_LOSS {pct:.1f}% (SL {sl:.2f}%)"
            # ── time stop: never let a member position ride forever ──
            if reason is None:
                opened = int(p.get("open_time") or 0)
                if opened > 0:
                    age = now - opened
                    if 0 < age <= 7 * 86400 and age > max_hold:
                        reason = f"MAX_HOLD_EXCEEDED ({age / 60:.0f}min)"
            if reason is None:
                continue
            qty = abs(p["size"])
            if qty <= 0:
                continue
            close_side = "sell" if p["side"] == "LONG" else "buy"
            pos_side = p["pos_side"] if p["pos_side"] in ("long", "short") else None
            cl = f"mc{uid}{close_side}{p['ticker']}{int(now * 1000)}"
            try:
                if hasattr(exe, "close_swap"):
                    res = exe.close_swap(p["inst_id"], close_side, qty,
                                         cl_ord_id=cl, pos_side=pos_side)
                else:  # pragma: no cover - older executor fallback
                    res = exe.submit_market(p["inst_id"], close_side,
                                            qty * last_px, cl_ord_id=cl)
            except Exception as e:
                print(f"[MemberTrader] user {uid} close {p['ticker']} failed: {e}")
                self._last_exit[key] = now
                continue
            self._last_exit[key] = now
            if res.get("ok"):
                changed = True
                self._orders += 1
                self._sl_tp.pop((uid, p["inst_id"]), None)
                print(f"[MemberTrader] user {uid} CLOSE {p['ticker']} {close_side} "
                      f"({reason}) qty={qty:.6f} upl=${p['upl']:.2f} "
                      f"order={res.get('order_id', '')}")
            else:
                print(f"[MemberTrader] user {uid} CLOSE FAILED {p['ticker']}: "
                      f"{res.get('msg') or res}")

        # Drop frozen thresholds for instruments the account no longer holds.
        live = {(uid, p["inst_id"]) for p in positions}
        for k in list(self._sl_tp.keys()):
            if k[0] == uid and k not in live:
                self._sl_tp.pop(k, None)
        if changed:
            try:
                self._mgr.force_refresh(uid, m)
            except Exception:
                pass

    # ── entries ────────────────────────────────────────────────────────────

    def _scan_entries(self, uid: int, m: dict, exe, held: set[str],
                      positions: list[dict]) -> None:
        prof = risk_profile(m.get("risk_preference"))
        max_pos = self._max_positions_env or int(prof["max_pos"])
        if len(held) >= max_pos:
            return
        allowed = {t.strip().upper() for t in
                   str(m.get("allowed_tickers") or "").split(",") if t.strip()}
        now = time.time()
        # Same resolver the settings UI/user_account use — one source of truth.
        size = UserAccountManager.resolve_trade_usd(m, fallback=self._default_trade_usd)
        # ── Risk-preference gates (the preference now drives real limits) ──
        equity = self._equity_for(uid, exe)
        exposure = sum(abs(_f(p.get("size_usd"))) for p in positions or [])
        if equity > 0:
            entry_cap = equity * _f(prof["max_pct"])
            if entry_cap < OKX_MIN_ORDER_USD:
                print(f"[MemberTrader] user {uid} SKIP: risk "
                      f"{m.get('risk_preference')} caps an entry at "
                      f"${entry_cap:.2f} (< OKX min ${OKX_MIN_ORDER_USD:.0f})")
                return
            size = min(size, entry_cap)
        if size < OKX_MIN_ORDER_USD:
            return
        if equity > 0 and exposure + size > equity * _f(prof["total_pct"]):
            print(f"[MemberTrader] user {uid} SKIP: exposure "
                  f"${exposure + size:.2f} exceeds {_f(prof['total_pct']):.0%} of "
                  f"equity ${equity:.2f} (risk={m.get('risk_preference')})")
            return
        for sig in self._signals():
            if len(held) >= max_pos:
                break
            ticker = str(sig.get("ticker") or "").upper()
            if not ticker or ticker in held:
                continue
            if allowed and ticker not in allowed:
                continue
            key = (uid, ticker)
            if now - self._last_entry.get(key, 0.0) < self._entry_cooldown:
                continue
            inst_id = f"{ticker}-USDT-SWAP"
            if inst_id in self._delisted:
                continue
            self._last_entry[key] = now
            try:
                exe.set_leverage(inst_id, self._leverage, mgn_mode="cross")
            except Exception as e:
                msg = str(e)
                if "51001" in msg or "51087" in msg:
                    self._delisted.add(inst_id)
                print(f"[MemberTrader] user {uid} set_leverage {inst_id}: {e}")
                continue
            cl = f"me{uid}{ticker}{int(now * 1000)}"
            try:
                res = exe.submit_market(inst_id, "buy", size,
                                        cl_ord_id=cl, td_mode="cross")
            except ValueError as e:
                print(f"[MemberTrader] user {uid} SKIP {ticker}: {e}")
                continue
            except Exception as e:
                msg = str(e)
                if "51001" in msg or "51087" in msg:
                    self._delisted.add(inst_id)
                print(f"[MemberTrader] user {uid} ENTRY {ticker} failed: {e}")
                continue
            if not res.get("ok"):
                print(f"[MemberTrader] user {uid} ENTRY {ticker} rejected: "
                      f"{res.get('msg') or res}")
                continue
            sl, tp = self._sl_tp_for(m, sig.get("atr_pct"))
            self._sl_tp[(uid, inst_id)] = (sl, tp)
            held.add(ticker)
            self._orders += 1
            print(f"[MemberTrader] user {uid} ENTRY {ticker} ${size:.2f} @ {inst_id} "
                  f"SL={sl:.2f}% TP={tp:.2f}% order={res.get('order_id', '')} "
                  f"[{str(sig.get('reason', ''))[:60]}]")

    # ── signal source ──────────────────────────────────────────────────────

    def _signals(self) -> list[dict]:
        """Entry candidates: shared-desk tokens when wired, else self-computed."""
        if self._candidates_fn is not None:
            try:
                out = self._candidates_fn() or []
                if out:
                    return out
            except Exception as e:
                print(f"[MemberTrader] candidates_fn failed: {e}")
        now = time.time()
        if self._sig_cache and now - self._sig_ts < self._sig_ttl:
            return self._sig_cache
        from rh_okx_data import fetch_signal
        exe = self._mgr.any_executor()
        out: list[dict] = []
        if exe is not None:
            for t in self._tickers:
                try:
                    sig = fetch_signal(exe, f"{t}-USDT-SWAP", self._signal_cfg)
                except Exception:
                    continue
                if sig.get("ok"):
                    out.append({"ticker": t,
                                "atr_pct": _f(sig.get("atr_pct")),
                                "reason": str(sig.get("reason", ""))})
        self._sig_cache = out
        self._sig_ts = now
        return out

    # ── introspection ──────────────────────────────────────────────────────

    def status(self) -> dict:
        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "interval_sec": self._interval,
            "max_positions": self._max_positions,
            "leverage": self._leverage,
            "cycles": self._cycles,
            "orders": self._orders,
            "open_thresholds": len(self._sl_tp),
            "delisted": sorted(self._delisted),
        }
