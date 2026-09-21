#!/usr/bin/env python3
"""RH OKX V5 API Executor — AI Builder Program integration.

Pure stdlib (urllib + json + hmac + base64). No extra deps.

OKX AI Builder attribution: every order carries `tag = <AI_BUILDER_CODE>`
so OKX tracks commissions back to the builder.

Usage:
  from rh_okx_executor import OKXExecutor
  ex = OKXExecutor.from_env()
  quote = ex.get_quote("BTC-USDT", "buy", size_usd=75.0)
  order = ex.submit_order(quote, side="buy", size_usd=75.0)
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone

import requests

# ── Constants ──────────────────────────────────────────────────────────
OKX_BASE_PROD = "https://www.okx.com"
OKX_BASE_DEMO = "https://www.okx.com"  # demo uses same domain + header
OKX_V5 = "/api/v5"

# Spot order types (OKX V5 enum)
ORD_TYPE_MARKET = "market"
ORD_TYPE_LIMIT = "limit"

# Trade mode: "cross" for both SPOT and PERP on Margin/Unified accounts
# OKX Simple Spot used "cash" but after upgrading to cross-currency margin,
# ALL orders (spot + swap) must use "cross".
TD_MODE_SPOT = "cross"

# OKX minimum order amounts (USD) — conservative floor
# Per-coin limits vary; use a safe default of $10 to avoid "minimum order amount" errors
# Actual min per coin: SOL~$0.10, DOGE~$0.09, ADA~$0.002, UNI~$6.36, LINK~$1.15
OKX_MIN_ORDER_USD = 10.0

# ── Result dataclasses (aligned with JupiterExecutor interface) ───────
@dataclass
class QuoteResult:
    """Price/slippage estimate — mirrors Jupiter QuoteResult for LiveTrader."""
    raw: dict                    # raw OKX market ticker response
    inst_id: str                 # e.g. "BTC-USDT"
    side: str                    # "buy" or "sell"
    price_usd: float             # last price (USD per 1 base coin)
    size_usd: float              # input size in USD
    size_base: float             # input size in base coin (e.g. BTC amount)
    out_amount_base: float      # estimated output (same as size_base for market, may differ on slippage)
    fee_amount_usd: float       # estimated OKX taker fee (0.08%)
    fee_bps: int                 # fee in basis points
    price_impact_pct: float      # estimated slippage (0 for market order, computed from depth if available)
    ticker: str = ""            # alias

@dataclass
class OKXOrder:
    """Built order payload — server-side signed via API key.

    OKX spot market order rules:
      BUY  → send tdSz (quote notional, e.g. "10" = spend 10 USDT)
      SELL → send sz   (base qty, e.g. "0.000126" = sell 0.000126 BTC)
    Limit orders always send sz + px.
    """
    quote: QuoteResult
    inst_id: str
    side: str                     # "buy" / "sell"
    ord_type: str                 # "market" / "limit"
    td_mode: str                  # "spot" / "cross" / "isolated"
    sz: str | None = None         # base asset quantity (sell + limit)
    td_sz: str | None = None      # quote notional (market buy)
    px: str | None = None         # price (limit orders only)
    tag: str | None = None        # AI Builder Code for commission attribution
    cl_ord_id: str | None = None  # client order ID (optional)

# ── HTTP helpers (requests + explicit proxy) ──────────────────────────
# Clash/Mihomo proxy on 127.0.0.1:7897 works with requests but NOT with
# urllib.request (gets 403). We read proxy from HTTP_PROXY env and pass it
# explicitly to every requests call.
def _get_proxies() -> dict | None:
    """Return requests-compatible proxy dict, or None if no proxy."""
    hp = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy") or os.environ.get("ALL_PROXY")
    sp = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or hp
    if hp or sp:
        return {"http": hp or sp, "https": sp or hp}
    return None


# ── Connection reuse ─────────────────────────────────────────────────
# Every call used to go through `requests.get/post`, opening a fresh
# TCP+TLS connection each time. With one account snapshot per member every
# 30s — three calls each — the TLS handshake dominated the cost. A pooled
# Session keeps the connection alive and reuses it.
#
# The Session is thread-local because requests.Session is not fully
# thread-safe and this module is called from several server threads.
_SESSION_LOCAL = threading.local()


def _get_session() -> requests.Session:
    """Return this thread's pooled HTTP session."""
    sess = getattr(_SESSION_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=8,
            pool_maxsize=16,
            max_retries=0,          # retry policy lives in _post_with_backoff
            pool_block=False,
        )
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)
        _SESSION_LOCAL.session = sess
    return sess


def _http_get(url, params=None, headers=None, timeout=10.0):
    proxies = _get_proxies()
    hdrs = {"Accept": "application/json", "User-Agent": "RH-OKX-Executor/1.0"}
    if headers:
        hdrs.update(headers)
    try:
        r = _get_session().get(url, params=params, headers=hdrs,
                               proxies=proxies, timeout=timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code} GET {url.split('?')[0]}: {r.text[:400]}")
        return r.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"HTTP GET {url.split('?')[0]} failed: {e}") from e


def _http_post(url, payload, headers=None, timeout=15.0):
    proxies = _get_proxies()
    hdrs = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "RH-OKX-Executor/1.0"}
    if headers:
        hdrs.update(headers)
    try:
        r = _get_session().post(url, json=payload, headers=hdrs,
                                proxies=proxies, timeout=timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code} POST {url}: {r.text[:500]}")
        return r.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"HTTP POST {url} failed: {e}") from e


# ── Exponential backoff retry wrapper ─────────────────────────────────
def _post_with_backoff(url, payload, headers, timeout=15.0, max_retries=4):
    """POST with exponential backoff for rate-limit (429) and transient errors."""
    import time as _time
    wait = 1.0
    last_err = None
    for attempt in range(max_retries + 1):
        try:
            return _http_post(url, payload, headers, timeout=timeout)
        except (RuntimeError, requests.exceptions.Timeout) as e:
            last_err = e
            msg = str(e).lower()
            is_rate_limit = ("429" in msg) or ("rate" in msg) or ("limit" in msg)
            if not is_rate_limit and attempt >= 2:
                break  # non-rate-limit errors fail fast after 2 tries
            _time.sleep(wait)
            wait = min(wait * 2, 30.0)  # cap at 30s
    raise last_err or RuntimeError("POST backoff exhausted")

# ── OKX V5 signature ──────────────────────────────────────────────────
def _iso_timestamp_ms() -> str:
    """OKX wants ISO8601 with milliseconds + Z."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

def _okx_sign(api_secret: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """OKX V5 REST signature.

    prehash = timestamp + method.upper() + requestPath + body
    sign = Base64(HMAC-SHA256(api_secret, prehash))
    """
    prehash = timestamp + method.upper() + request_path + body
    digest = hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")

# ── OKX Executor ──────────────────────────────────────────────────────
class OKXExecutor:
    """OKX V5 REST executor with AI Builder attribution.

    Implements the SAME public interface as JupiterExecutor so LiveTrader
    can swap executors with a single import change.

    Required env vars (API mode):
      OKX_API_KEY        — your OKX API key (read-only + trade permissions)
      OKX_API_SECRET     — API secret
      OKX_PASSPHRASE     — API passphrase
      OKX_AI_BUILDER_CODE — AI Builder Code for commission tracking (put in tag)

    Optional:
      OKX_DEMO=1         — use demo trading mode (no real money)
      OKX_BASE_URL       — override OKX domain (defaults to prod)
    """

    def __init__(
        self,
        api_key=None,
        api_secret=None,
        passphrase=None,
        ai_builder_code=None,
        base_url=None,
        demo=False,
        max_retries=2,
        timeout=15.0,
        default_fee_bps=8,          # OKX spot taker = 0.08%
        slippage_bps=100,           # 1% default
    ):
        self.api_key = api_key or os.environ.get("OKX_API_KEY", "")
        self.api_secret = api_secret or os.environ.get("OKX_API_SECRET", "")
        self.passphrase = passphrase or os.environ.get("OKX_PASSPHRASE", "")
        self.ai_builder_code = ai_builder_code or os.environ.get("OKX_AI_BUILDER_CODE", "")
        self.base_url = base_url or os.environ.get("OKX_BASE_URL", OKX_BASE_PROD)
        self.demo = demo or os.environ.get("OKX_DEMO", "").lower() in ("1", "true", "yes")
        self.max_retries = max_retries
        self.timeout = timeout
        self.default_fee_bps = default_fee_bps
        self.slippage_bps = slippage_bps

        self._auth_ready = bool(self.api_key and self.api_secret and self.passphrase)

        # lotSz cache: inst_id → minimum order size increment
        self._lot_sz_cache: dict[str, float] = {}

        # Account mode check (lazy)
        self._acct_type: str | None = None
        self._acct_can_trade_perp: bool | None = None

    def check_account_mode(self) -> tuple[str, bool]:
        """Query OKX account config and cache whether we can trade perps.

        OKX real account: type != "0" means margin mode (can trade perps).
        OKX demo account: type is often stuck at "0" even after upgrading,
                          but acctLv >= 3 means contract trading is enabled.
        """
        if self._acct_type is not None:
            return self._acct_type, self._acct_can_trade_perp or False
        try:
            r = self._signed_get("/api/v5/account/config")
            cfg = (r.get("data") or [{}])[0]
            self._acct_type = str(cfg.get("type", "?"))
            acct_lv = int(cfg.get("acctLv", 0) or 0)
            # Real account: type must be 1/2/6 (margin/unified)
            # Demo account fallback: type=0 but acctLv>=3 means SWAP enabled
            self._acct_can_trade_perp = (
                self._acct_type != "0"
                or acct_lv >= 3
            )
            print(f"[OKXExecutor] Account check: type={self._acct_type} acctLv={acct_lv} "
                  f"can_trade_perp={self._acct_can_trade_perp}")
        except Exception as e:
            print(f"[OKXExecutor] account config check failed: {e}")
            self._acct_type = "?"
            self._acct_can_trade_perp = False
        return self._acct_type, self._acct_can_trade_perp

    @property
    def can_trade_perp(self) -> bool:
        """Shortcut: can this account trade SWAP/perpetuals?"""
        _, ok = self.check_account_mode()
        return ok

    def _get_lot_sz(self, inst_id: str) -> float:
        """Get OKX lotSz for an instrument (cached). Returns 0 if unknown."""
        if inst_id in self._lot_sz_cache:
            return self._lot_sz_cache[inst_id]
        try:
            inst_type = "SPOT" if "SPOT" in inst_id else "SWAP" if inst_id.endswith("-SWAP") else "SPOT"
            insts = self.public_instruments(inst_type=inst_type)
            for inst in insts:
                if inst.get("instId") == inst_id:
                    sz = float(inst.get("lotSz") or "1")
                    self._lot_sz_cache[inst_id] = sz
                    return sz
        except Exception:
            pass
        # Default to 0.0001 if unknown (won't hurt since it's tiny)
        self._lot_sz_cache[inst_id] = 0.0001
        return 0.0001

    @classmethod
    def from_env(cls):
        return cls(
            api_key=os.environ.get("OKX_API_KEY"),
            api_secret=os.environ.get("OKX_API_SECRET"),
            passphrase=os.environ.get("OKX_PASSPHRASE"),
            ai_builder_code=os.environ.get("OKX_AI_BUILDER_CODE"),
            demo=os.environ.get("OKX_DEMO", "").lower() in ("1", "true", "yes"),
        )

    # ── Internal: signed request ────────────────────────────────────
    def _signed_headers(self, method: str, request_path: str, body: str = "") -> dict:
        timestamp = _iso_timestamp_ms()
        sign = _okx_sign(self.api_secret, timestamp, method, request_path, body)
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
        }
        if self.demo:
            headers["x-simulated-trading"] = "1"
        return headers

    def _signed_get(self, request_path: str, params=None) -> dict:
        qs = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
        full_path = f"{request_path}?{qs}" if qs else request_path
        headers = self._signed_headers("GET", full_path)
        return _http_get(self.base_url + full_path, headers=headers, timeout=self.timeout)

    def _signed_post(self, request_path: str, payload: dict) -> dict:
        body_str = json.dumps(payload)
        headers = self._signed_headers("POST", request_path, body_str)
        headers["Content-Type"] = "application/json"
        # P0-3: use exponential backoff for rate-limit resilience
        return _post_with_backoff(self.base_url + request_path, payload, headers=headers, timeout=self.timeout)

    def _check_ok(self, data: dict, action: str = "request") -> dict:
        """OKX returns {"code": "0", "data": [...]} on success.

        Raises RuntimeError with the REAL OKX sub-code (sCode/sMsg) embedded
        so callers can distinguish instrument-not-found vs lot-size vs SSL etc.
        """
        code = data.get("code", "?")
        if code != "0":
            msg = data.get("msg", "unknown")
            scode = ""
            smsg = ""
            if data.get("data"):
                first = data["data"][0] if data["data"] else {}
                scode = first.get("sCode", "")
                smsg = first.get("sMsg", "")
            detail = smsg or msg
            full_msg = f"OKX {action} failed: code={code} sCode={scode or '-'} msg={detail}"
            print(f"[{full_msg}]")
            raise RuntimeError(full_msg)
        arr = data.get("data", [])
        return arr[0] if arr else {}

    # ── Public market endpoints (no sign needed) ────────────────────
    def public_ticker(self, inst_id: str) -> dict:
        """GET /api/v5/market/ticker?instId=XXX — public, no auth."""
        return _http_get(
            self.base_url + OKX_V5 + "/market/ticker",
            params={"instId": inst_id},
            timeout=self.timeout,
        )

    def public_candles(self, inst_id: str, bar: str = "1m", limit: int = 100) -> list:
        """GET /api/v5/market/candles — public OHLCV."""
        data = _http_get(
            self.base_url + OKX_V5 + "/market/candles",
            params={"instId": inst_id, "bar": bar, "limit": str(limit)},
            timeout=self.timeout,
        )
        return data.get("data", [])

    def public_books(self, inst_id: str, sz: int = 5) -> dict:
        """GET /api/v5/market/books — orderbook (for slippage estimate)."""
        data = _http_get(
            self.base_url + OKX_V5 + "/market/books",
            params={"instId": inst_id, "sz": str(sz)},
            timeout=self.timeout,
        )
        arr = data.get("data", [])
        return arr[0] if arr else {}

    def public_instruments(self, inst_type: str = "SPOT") -> list:
        """GET /api/v5/public/instruments — list available spot pairs."""
        data = _http_get(
            self.base_url + OKX_V5 + "/public/instruments",
            params={"instType": inst_type},
            timeout=self.timeout,
        )
        return data.get("data", [])

    def get_tradable_pairs(self, quote_ccy: str = "USDT", limit: int = 50) -> list[dict]:
        """Get top tradable pairs sorted by 24h volume.

        Returns list of dicts with keys:
          inst_id, base_ccy, quote_ccy, last_price, vol_24h, vol_ccy, change_pct
        """
        try:
            tickers = self.public_tickers()
            # Filter for USDT pairs only
            pairs = []
            for t in tickers:
                inst_id = t.get("instId", "")
                if not inst_id.endswith(f"-{quote_ccy}"):
                    continue
                try:
                    last = float(t.get("last") or 0)
                    vol = float(t.get("vol24h") or 0)
                    open24h = float(t.get("open24h") or last)
                    if last > 0 and vol > 0:
                        pairs.append({
                            "inst_id": inst_id,
                            "base_ccy": inst_id.split("-")[0],
                            "quote_ccy": quote_ccy,
                            "last_price": last,
                            "vol_24h": vol,
                            "vol_ccy": t.get("volCcy24h", "0"),
                            "change_pct": round((last - open24h) / open24h * 100, 2) if open24h > 0 else 0,
                        })
                except (ValueError, TypeError):
                    continue
            # Sort by 24h volume descending
            pairs.sort(key=lambda x: x["vol_24h"], reverse=True)
            # Truncate to limit first, then ensure demo-compatible pairs are included
            top_pairs = pairs[:limit]
            from rh_okx_data import DEMO_COMPATIBLE_TICKERS
            demo_set = DEMO_COMPATIBLE_TICKERS
            existing = {p["base_ccy"].upper() for p in top_pairs}
            missing_demo = [d for d in demo_set if d not in existing]
            # Append missing demo coins (preserve sort order for remaining items)
            for t in tickers:
                inst_id = t.get("instId", "")
                if not inst_id.endswith(f"-{quote_ccy}"):
                    continue
                base = inst_id.split("-")[0].upper()
                if base not in demo_set or base in existing:
                    continue
                try:
                    last = float(t.get("last") or 0)
                    vol = float(t.get("vol24h") or 0)
                    open24h = float(t.get("open24h") or last)
                    if last > 0 and vol > 0:
                        top_pairs.append({
                            "inst_id": inst_id,
                            "base_ccy": base,
                            "quote_ccy": quote_ccy,
                            "last_price": last,
                            "vol_24h": vol,
                            "vol_ccy": t.get("volCcy24h", "0"),
                            "change_pct": round((last - open24h) / open24h * 100, 2) if open24h > 0 else 0,
                        })
                        existing.add(base)
                except (ValueError, TypeError):
                    continue
            return top_pairs
        except Exception as e:
            print(f"[OKX] get_tradable_pairs failed: {e}")
            return []

    def public_tickers(self, inst_type: str = "SPOT") -> list:
        """GET /api/v5/market/tickers — all spot tickers."""
        data = _http_get(
            self.base_url + OKX_V5 + "/market/tickers",
            params={"instType": inst_type},
            timeout=self.timeout,
        )
        return data.get("data", [])

    # ── Account (signed) ────────────────────────────────────────────
    def get_balance(self, ccy: str | None = None) -> dict:
        """GET /api/v5/account/balance."""
        if not self._auth_ready:
            raise RuntimeError("OKX API credentials not configured")
        params = {"ccy": ccy} if ccy else None
        resp = self._signed_get(OKX_V5 + "/account/balance", params)
        return self._check_ok(resp, "get_balance")

    def get_positions(self) -> list:
        """GET /api/v5/account/positions — mostly for derivatives."""
        if not self._auth_ready:
            raise RuntimeError("OKX API credentials not configured")
        resp = self._signed_get(OKX_V5 + "/account/positions")
        return resp.get("data", [])

    def get_account_summary(self, perp_positions: list = None) -> dict:
        """Parse OKX balance into a usable summary for the trading engine.

        Args:
            perp_positions: Optional list of perpetual positions from /account/positions.
                           If provided, perp unrealized PnL is merged into total_eq_usd.

        Returns:
            {
                "total_eq_usd": float,   # spot totalEq + perp upl (if positions provided)
                "usdt_avail": float,
                "usdt_eq": float,
                "holdings": [...],
                "perp_upl": float,       # sum of unrealized PnL from all positions
                "raw": dict,
            }
        """
        raw = self.get_balance()
        details = raw.get("details", [])
        usdt_avail = 0.0
        usdt_eq = 0.0
        holdings = []
        for d in details:
            ccy = d.get("ccy", "")
            avail = float(d.get("availBal", 0) or 0)
            spot_bal = float(d.get("spotBal", 0) or d.get("cashBal", 0) or 0)
            eq_usd = float(d.get("eqUsd", 0) or 0)
            upl = float(d.get("spotUpl", 0) or 0)
            if ccy == "USDT":
                usdt_avail = avail
                usdt_eq = eq_usd
            elif spot_bal > 0.0000001:
                holdings.append({
                    "ccy": ccy,
                    "spot_bal": spot_bal,
                    "eq_usd": eq_usd,
                    "upl": upl,
                })

        spot_total = float(raw.get("totalEq", 0) or 0)

        # Merge perpetual position unrealized PnL into total equity
        perp_upl = 0.0
        if perp_positions:
            perp_upl = sum(float(p.get("upl", 0) or 0) for p in perp_positions)

        # totalEq from OKX balance API covers spot only.
        # Add perp unrealized PnL to get unified account equity.
        total_eq_usd = spot_total + perp_upl

        return {
            "total_eq_usd": round(total_eq_usd, 2),
            "usdt_avail": usdt_avail,
            "usdt_eq": usdt_eq,
            "holdings": holdings,
            "perp_upl": round(perp_upl, 2),
            "raw": raw,
        }

    # ── Quote (uses public market data, builds QuoteResult) ──────────
    def get_quote(self, inst_id: str, side: str = "buy", size_usd: float = 75.0) -> QuoteResult:
        """Get price + slippage estimate for a spot market order.

        Mirrors JupiterExecutor.get_quote interface for LiveTrader compatibility.

        Args:
            inst_id:  OKX instrument e.g. "BTC-USDT", "ETH-USDT", "SOL-USDT"
            side:     "buy" or "sell"
            size_usd: order notional in USD (quote currency)
        """
        raw = self.public_ticker(inst_id)
        # OKX ticker response: {"code": "0", "data": [{"last": "12345.0", "bidPx": "12344.5", ...}]}
        arr = raw.get("data", [])
        if not arr:
            raise RuntimeError(f"OKX ticker empty for {inst_id}")
        t = arr[0]

        try:
            last_price = float(t.get("last") or t.get("lastPx") or 0)
            bid = float(t.get("bidPx") or 0)
            ask = float(t.get("askPx") or 0)
        except (ValueError, TypeError):
            raise RuntimeError(f"OKX ticker bad price for {inst_id}: {t}")

        if last_price <= 0:
            raise RuntimeError(f"OKX ticker zero price for {inst_id}")

        # For buy we pay ask, for sell we receive bid
        exec_price = ask if side == "buy" else bid
        if exec_price <= 0:
            exec_price = last_price  # fallback

        size_base = size_usd / exec_price

        # ── Align to lotSz (OKX requires order quantity be a multiple of lot size) ──
        # Try to get lotSz from instrument metadata (cached)
        lot_sz = self._get_lot_sz(inst_id)
        if lot_sz and lot_sz > 0:
            # Round DOWN to nearest multiple of lotSz
            size_base = (size_base // lot_sz) * lot_sz
            if size_base < lot_sz:
                size_base = lot_sz  # minimum 1 lot
            # Ensure enough USD notional
            min_usd = size_base * exec_price
            if min_usd < 5:  # skip absurdly small orders
                size_base = 0

        out_amount = size_base

        # Fee estimate: OKX spot taker = 0.08% of notional
        fee_usd = size_usd * (self.default_fee_bps / 10000)

        # Simple slippage estimate using top-of-book spread
        spread_bps = ((ask - bid) / last_price) * 10000 if ask > 0 and bid > 0 else 0
        price_impact = spread_bps / 100  # convert bps to percent

        return QuoteResult(
            raw=t,
            inst_id=inst_id,
            side=side,
            price_usd=exec_price,
            size_usd=size_usd,
            size_base=size_base,
            out_amount_base=out_amount,
            fee_amount_usd=fee_usd,
            fee_bps=self.default_fee_bps,
            price_impact_pct=price_impact,
            ticker=inst_id.split("-")[0],
        )

    # ── Build order (server-side signed via API key) ────────────────
    def build_order(
        self,
        quote: QuoteResult,
        tag: str | None = None,
        ord_type: str = ORD_TYPE_MARKET,
        td_mode: str = TD_MODE_SPOT,
        cl_ord_id: str | None = None,
    ) -> OKXOrder:
        """Translate quote into a signed OKX order payload.

        OKX V5 spot market orders use sz (base asset quantity) for BOTH
        buy and sell — tdSz is NOT accepted for spot market orders.
        """
        use_tag = tag or self.ai_builder_code or None

        # Validate minimum order amount (OKX requires different minimums per coin)
        if quote.size_usd < OKX_MIN_ORDER_USD:
            raise ValueError(
                f"Order size ${quote.size_usd:.2f} below OKX minimum ${OKX_MIN_ORDER_USD:.0f} "
                f"for {quote.inst_id}. Increase stake or skip."
            )

        # Use appropriate precision based on price level
        price = quote.price_usd
        if price >= 100:
            sz = f"{quote.size_base:.6f}".rstrip("0").rstrip(".")
        elif price >= 1:
            sz = f"{quote.size_base:.4f}".rstrip("0").rstrip(".")
        else:
            # Low-price coins need more precision
            sz = f"{quote.size_base:.2f}".rstrip("0").rstrip(".")
            if sz in ("0", ".0"):
                sz = f"{quote.size_base:.8f}".rstrip("0").rstrip(".")
        if not sz or sz == "0":
            sz = "0.0001"

        return OKXOrder(
            quote=quote, inst_id=quote.inst_id, side=quote.side,
            ord_type=ord_type, td_mode=td_mode,
            sz=sz, td_sz=None, px=None,
            tag=use_tag, cl_ord_id=cl_ord_id,
        )

    # ── Submit order (signed REST) ───────────────────────────────────
    def submit_order(self, order: OKXOrder) -> dict:
        """POST /api/v5/trade/order — server-side signed broadcast.

        Returns OKX response with orderId:
          {"orderId": "123456", "clOrdId": "...", "msg": ""}

        For AI Builder attribution, `tag` field MUST contain your Builder Code.
        """
        if not self._auth_ready:
            raise RuntimeError("OKX API credentials not configured — set OKX_API_KEY/SECRET/PASSPHRASE")

        payload = {
            "instId": order.inst_id,
            "tdMode": order.td_mode,
            "side": order.side,
            "ordType": order.ord_type,
        }
        if order.sz:
            payload["sz"] = order.sz
        if order.td_sz:
            payload["tdSz"] = order.td_sz
        if order.px:
            payload["px"] = order.px
        if order.tag:
            payload["tag"] = order.tag
        if order.cl_ord_id:
            # OKX clOrdId: alphanumeric ONLY (no underscores, no hyphens).
            # Strip any non-alphanumeric chars; if nothing left, omit it.
            clean = "".join(c for c in order.cl_ord_id if c.isalnum())
            if clean:
                payload["clOrdId"] = clean[:32]  # max 32 chars per OKX spec

        resp = self._signed_post(OKX_V5 + "/trade/order", payload)
        result = self._check_ok(resp, "submit_order")

        # OKX response uses ordId (lowercase o) and clOrdId
        return {
            "ok": True,
            "order_id": result.get("ordId", ""),
            "cl_ord_id": result.get("clOrdId", ""),
            "inst_id": order.inst_id,
            "side": order.side,
            "sz": order.sz,
            "tag": order.tag,
            "msg": result.get("msg", ""),
        }

    def get_commission(self, inst_id: str, begin_ms: int | None = None, end_ms: int | None = None, limit: int = 100) -> dict:
        """GET /api/v5/trade/fills — query filled orders + commission data.

        Returns total_commission_usd, builder_hits, and list of recent fills.
        begin_ms/end_ms are epoch milliseconds (OKX format). None = last 7 days.
        """
        params = {"instId": inst_id, "limit": str(limit)}
        if begin_ms:
            params["beginMs"] = str(begin_ms)
        if end_ms:
            params["endMs"] = str(end_ms)
        resp = self._signed_get(OKX_V5 + "/trade/fills", params)
        fills = resp.get("data", [])
        total_comm_usd = 0.0
        builder_hits = 0
        for f in fills:
            comm = float(f.get("fee", 0) or 0)
            cc = f.get("ccy", "")
            if cc == "USDT":
                total_comm_usd += comm
            tag_hit = bool(f.get("tag")) and f.get("tag") == self.ai_builder_code
            if tag_hit:
                builder_hits += 1
        return {
            "total_commission_usd": round(total_comm_usd, 6),
            "builder_hits": builder_hits,
            "total_fills": len(fills),
            "fills": [
                {
                    "inst_id": f.get("instId", ""),
                    "side": f.get("side", ""),
                    "fill_px": f.get("fillPx", ""),
                    "fill_sz": f.get("fillSz", ""),
                    "fee": f.get("fee", ""),
                    "ccy": f.get("ccy", ""),
                    "tag": f.get("tag", ""),
                    "ts": f.get("fillTime", ""),
                }
                for f in fills[-20:]
            ],
        }

    def get_order_history(self, limit: int = 50) -> list:
        """GET /api/v5/trade/fills — ALL recent fills across every instrument.

        Unlike get_commission which filters by instId, this returns the full
        account-wide fill history (last 7 days, up to 100 fills from OKX;
        we slice to `limit` newest).

        Returns a list of normalized dicts ready for the frontend trade log:
          {side, ticker, inst_id, fill_px, fill_sz, fee, fee_ccy, ts_ms}
        """
        params = {"limit": str(min(limit * 4, 100))}  # fetch more in case of pagination, we slice later
        resp = self._signed_get(OKX_V5 + "/trade/fills", params)
        fills = resp.get("data", [])
        result = []
        for f in fills:
            inst_id = f.get("instId", "")
            # Extract ticker (e.g. "BTC-USDT-SWAP" → "BTC", "ETH-USDT" → "ETH")
            ticker = inst_id.split("-")[0] if inst_id else inst_id
            fill_px = float(f.get("fillPx", 0) or 0)
            fill_sz = float(f.get("fillSz", 0) or 0)
            fee = float(f.get("fee", 0) or 0)
            fee_ccy = f.get("ccy", "")
            side = f.get("side", "").upper()  # "buy"/"sell" → "BUY"/"SELL"
            ts_ms = int(f.get("fillTime", 0) or 0)
            # Convert fill_sz to USD notional (best-effort)
            notional_usd = round(fill_sz * fill_px, 4)
            result.append({
                "side": side,
                "ticker": ticker,
                "inst_id": inst_id,
                "fill_px": fill_px,
                "fill_sz": fill_sz,
                "notional_usd": notional_usd,
                "fee": fee,
                "fee_ccy": fee_ccy,
                "ts_ms": ts_ms,
            })
        # Newest first (OKX returns newest first already, but sort defensively)
        result.sort(key=lambda x: x["ts_ms"], reverse=True)
        return result[:limit]

    def submit_market(self, inst_id: str, side: str, size_usd: float, cl_ord_id: str | None = None, td_mode: str = "cross") -> dict:
        """One-shot: quote → build → submit. Convenience for LiveTrader."""
        quote = self.get_quote(inst_id, side, size_usd)
        order = self.build_order(quote, cl_ord_id=cl_ord_id, td_mode=td_mode)
        return self.submit_order(order)

    def set_leverage(self, inst_id: str, leverage: int, mgn_mode: str = "cross", pos_side: str | None = None) -> dict:
        """POST /api/v5/account/set-leverage — set leverage for a SWAP contract.

        Required BEFORE submitting perpetual orders, otherwise OKX defaults
        to 1x leverage.

        Args:
            inst_id: e.g. "BTC-USDT-SWAP"
            leverage: e.g. 5
            mgn_mode: "cross" or "isolated"
            pos_side: "long" or "short" (required when hedge mode enabled)
        """
        if not self._auth_ready:
            raise RuntimeError("OKX API credentials not configured")
        payload = {
            "instId": inst_id,
            "lever": str(leverage),
            "mgnMode": mgn_mode,
        }
        if pos_side:
            payload["posSide"] = pos_side
        resp = self._signed_post(OKX_V5 + "/account/set-leverage", payload)
        return self._check_ok(resp, "set_leverage")

    # ── Query / cancel orders ────────────────────────────────────────
    def get_order(self, inst_id: str, order_id: str | None = None, cl_ord_id: str | None = None) -> dict:
        """GET /api/v5/trade/order/{orderId} — signed."""
        if not self._auth_ready:
            raise RuntimeError("OKX API credentials not configured")
        if not order_id and not cl_ord_id:
            raise ValueError("need order_id or cl_ord_id")
        if order_id:
            path = f"{OKX_V5}/trade/order/{order_id}"
        else:
            path = f"{OKX_V5}/trade/order/{cl_ord_id}"
        params = {"instId": inst_id}
        resp = self._signed_get(path, params)
        return self._check_ok(resp, "get_order")

    def cancel_order(self, inst_id: str, order_id: str | None = None, cl_ord_id: str | None = None) -> dict:
        """POST /api/v5/trade/cancel-order."""
        if not self._auth_ready:
            raise RuntimeError("OKX API credentials not configured")
        payload = {"instId": inst_id}
        if order_id:
            payload["orderId"] = order_id
        if cl_ord_id:
            payload["clOrdId"] = cl_ord_id
        resp = self._signed_post(OKX_V5 + "/trade/cancel-order", payload)
        return self._check_ok(resp, "cancel_order")

    # ── PING / connectivity test ─────────────────────────────────────
    def ping(self) -> dict:
        """Quick connectivity + auth check."""
        result = {"endpoint": self.base_url, "auth_ready": self._auth_ready, "demo": self.demo}
        try:
            # Public ticker test — always works
            t = self.public_ticker("BTC-USDT")
            arr = t.get("data", [])
            result["ok"] = True
            result["btc_price"] = float(arr[0]["last"]) if arr else 0
            result["mode"] = "public_ok"
        except Exception as e:
            result["ok"] = False
            result["mode"] = "public_fail"
            result["error"] = str(e)
            return result

        # Auth test — get balance (lightweight)
        if self._auth_ready:
            try:
                bal = self.get_balance()
                ccy_list = bal.get("details", [])
                result["balance_ok"] = True
                result["ccy_count"] = len(ccy_list)
                result["builder_code"] = self.ai_builder_code or "(not set — no commission tracking)"
            except Exception as e:
                result["balance_ok"] = False
                result["balance_error"] = str(e)

        return result

    # ── Convenience: matches LiveTrader call patterns ────────────────
    def estimate_buy(self, inst_id: str, size_usd: float = 75.0) -> QuoteResult:
        return self.get_quote(inst_id, side="buy", size_usd=size_usd)

    def estimate_sell(self, inst_id: str, size_usd: float = 75.0) -> QuoteResult:
        return self.get_quote(inst_id, side="sell", size_usd=size_usd)


# ── Quick self-test ───────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="RH OKX Executor - connectivity test")
    ap.add_argument("--demo", action="store_true", help="use OKX demo trading")
    args = ap.parse_args()

    if args.demo:
        os.environ["OKX_DEMO"] = "1"

    ex = OKXExecutor.from_env()
    print(f"[rh_okx_executor] base_url    = {ex.base_url}")
    print(f"[rh_okx_executor] demo        = {ex.demo}")
    print(f"[rh_okx_executor] api_key     = {'SET' if ex.api_key else 'NOT SET'}")
    print(f"[rh_okx_executor] passphrase  = {'SET' if ex.passphrase else 'NOT SET'}")
    print(f"[rh_okx_executor] builder_code= {ex.ai_builder_code or '(not set)'}")
    print()

    result = ex.ping()
    print(json.dumps(result, indent=2))

    if result.get("ok"):
        print("\n[OK] Public market API reachable.")
        if result.get("balance_ok"):
            print("[OK] Authenticated trading API reachable. Ready for live orders.")
        elif result.get("auth_ready"):
            print("[WARN] Auth keys present but balance query failed — check permissions.")
        else:
            print("[INFO] No API keys configured — public-only mode. Set OKX_API_KEY/SECRET/PASSPHRASE to trade.")
    else:
        print(f"\n[FAIL] {result.get('error')}")
