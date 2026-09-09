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
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone

# ── Constants ──────────────────────────────────────────────────────────
OKX_BASE_PROD = "https://www.okx.com"
OKX_BASE_DEMO = "https://www.okx.com"  # demo uses same domain + header
OKX_V5 = "/api/v5"

# Spot order types (OKX V5 enum)
ORD_TYPE_MARKET = "market"
ORD_TYPE_LIMIT = "limit"

# Trade mode: "spot" for spot, "cross"/"isolated" for derivatives
TD_MODE_SPOT = "spot"

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

# ── HTTP helpers ──────────────────────────────────────────────────────
def _http_get(url, params=None, headers=None, timeout=10.0):
    qs = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
    full = f"{url}?{qs}" if qs else url
    req = urllib.request.Request(full)
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "RH-OKX-Executor/1.0")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = ""
        try: body = e.read().decode(errors="replace")
        except Exception: pass
        raise RuntimeError(f"HTTP {e.code} GET {url.split('?')[0]}: {body[:400]}") from e

def _http_post(url, payload, headers=None, timeout=15.0):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "RH-OKX-Executor/1.0")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        resp_body = ""
        try: resp_body = e.read().decode(errors="replace")
        except Exception: pass
        raise RuntimeError(f"HTTP {e.code} POST {url}: {resp_body[:500]}") from e

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
        return _http_post(self.base_url + request_path, payload, headers=headers, timeout=self.timeout)

    def _check_ok(self, data: dict, action: str = "request") -> dict:
        """OKX returns {"code": "0", "data": [...]} on success."""
        code = data.get("code", "?")
        if code != "0":
            msg = data.get("msg", "unknown")
            raise RuntimeError(f"OKX {action} failed: code={code} msg={msg}")
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
        out_amount = size_base  # market order: ~1:1, slippage included in price diff

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

        OKX V5 spot market order rules:
          BUY  → tdSz (quote notional, e.g. "10" USD)
          SELL → sz   (base qty, e.g. "0.000126" BTC)
        Limit orders: sz + px always.
        """
        use_tag = tag or self.ai_builder_code or None
        side = quote.side  # "buy" / "sell" from quote

        if ord_type == ORD_TYPE_MARKET and side == "buy":
            # BUY: send quote notional — spend this many USDT
            td_sz = f"{quote.size_usd:.2f}".rstrip("0").rstrip(".")
            if not td_sz:
                td_sz = "0.01"
            return OKXOrder(
                quote=quote, inst_id=quote.inst_id, side=side,
                ord_type=ord_type, td_mode=td_mode,
                sz=None, td_sz=td_sz, px=None,
                tag=use_tag, cl_ord_id=cl_ord_id,
            )
        else:
            # SELL (market) or LIMIT: send base asset quantity
            sz = f"{quote.size_base:.6f}".rstrip("0").rstrip(".")
            if not sz:
                sz = "0.000001"
            return OKXOrder(
                quote=quote, inst_id=quote.inst_id, side=side,
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
            payload["clOrdId"] = order.cl_ord_id

        resp = self._signed_post(OKX_V5 + "/trade/order", payload)
        result = self._check_ok(resp, "submit_order")

        # OKX orderId = response["orderId"], clOrdId = client ref
        return {
            "ok": True,
            "order_id": result.get("orderId", ""),
            "cl_ord_id": result.get("clOrdId", ""),
            "inst_id": order.inst_id,
            "side": order.side,
            "sz": order.sz,
            "tag": order.tag,
            "msg": result.get("msg", ""),
        }

    def submit_market(self, inst_id: str, side: str, size_usd: float, cl_ord_id: str | None = None) -> dict:
        """One-shot: quote → build → submit. Convenience for LiveTrader."""
        quote = self.get_quote(inst_id, side, size_usd)
        order = self.build_order(quote, cl_ord_id=cl_ord_id)
        return self.submit_order(order)

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
