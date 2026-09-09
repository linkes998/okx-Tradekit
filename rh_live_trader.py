#!/usr/bin/env python3
"""RH Live Trader — platform-agnostic bridge between Desk fills and execution.

This module translates Desk ENTRY/EXIT/STOP signals into executable orders
via whichever executor is configured:

  JupiterExecutor (Solana) — AMM swap aggregator, requires wallet signature
  OKXExecutor (CEX)        — REST API with server-side signing, tag=Builder Code

Both executors expose a compatible get_quote() interface; LiveTrader routes
build/submit accordingly based on platform auto-detection.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable

from rh_trencher import Fill

# ── Platform detection ──────────────────────────────────────────────
def _detect_platform(executor) -> str:
    cls_name = type(executor).__name__
    if "OKX" in cls_name:
        return "okx"
    if "Jupiter" in cls_name:
        return "jupiter"
    if hasattr(executor, "build_order") and hasattr(executor, "submit_order"):
        return "okx"
    if hasattr(executor, "build_swap") and hasattr(executor, "fee_wallet"):
        return "jupiter"
    return "unknown"


# ── Default OKX spot instrument mapping (ticker → instId) ──────────
# Generated from OKX /api/v5/market/tickers (top 50 by 24h USDT volume)
# Only tickers present here will be traded in OKX mode — unknown tickers
# from DexScreener will be SKIPped to avoid 404 / connection errors.
DEFAULT_OKX_INST_MAP: dict[str, str] = {
    # Top volume
    "BTC": "BTC-USDT", "ETH": "ETH-USDT", "SOL": "SOL-USDT",
    "USDC": "USDC-USDT", "USDT": "USDT-TRY",
    "XRP": "XRP-USDT", "DOGE": "DOGE-USDT", "BNB": "BNB-USDT",
    "ADA": "ADA-USDT", "ARB": "ARB-USDT", "DOT": "DOT-USDT",
    "SUI": "SUI-USDT", "TRX": "TRX-TRY", "NEAR": "NEAR-USDT",
    # Mid volume
    "UNI": "UNI-USDT", "LINK": "LINK-USDT", "LTC": "LTC-USDT",
    "AVAX": "AVAX-USDT", "OP": "OP-USDT", "APT": "APT-USDT",
    "SEI": "SEI-USDT", "TIA": "TIA-USDT", "WLD": "WLD-USDT",
    "HYPE": "HYPE-USDT", "SOPH": "SOPH-USDT", "LIT": "LIT-USDT",
    "MATIC": "MATIC-USDT", "PEPE": "PEPE-USDT", "SHIB": "SHIB-USDT",
    # ETC + stable pairs
    "ETC": "ETC-USDT", "AAVE": "AAVE-USDT", "FIL": "FIL-USDT",
    "THETA": "THETA-USDT", "ICP": "ICP-USDT", "ATOM": "ATOM-USDT",
    "LDO": "LDO-USDT", "RPL": "RPL-USDT", "CHZ": "CHZ-USDT",
}

SOL_LAMPORTS = 1_000_000_000
SOL_USD_FALLBACK = 200.0


@dataclass
class PendingSwap:
    ticker: str
    side: str                       # "BUY" or "SELL"
    platform: str                   # "okx" or "jupiter"
    amount_usd: float
    quote: Any = None
    bundle: Any = None              # Jupiter SwapBundle (base64 tx for wallet signing)
    okx_order: Any = None           # OKX OKXOrder (server-side signed, ready to submit)
    inst_id: str = ""               # OKX: "BTC-USDT", Jupiter: mint address
    fee_amount_usd: float = 0.0
    created_at: float = 0.0
    note: str = ""
    requires_wallet_sign: bool = False


class LiveTrader:
    def __init__(
        self,
        executor,
        user_wallet: str | None = None,
        ticker_mint_map: dict[str, str] | None = None,
        ticker_inst_map: dict[str, str] | None = None,
        sol_usd: float = SOL_USD_FALLBACK,
        slippage_bps: int = 100,
        enable_buys: bool = True,
        enable_sells: bool = True,
        log_fn: Callable[[str], None] | None = print,
        rpc_url: str | None = None,
    ):
        self.executor = executor
        self.platform = _detect_platform(executor)
        self.user_wallet = user_wallet or ""
        self.sol_usd = sol_usd
        self.slippage_bps = slippage_bps
        self.enable_buys = enable_buys
        self.enable_sells = enable_sells
        self.log_fn = log_fn
        self.rpc_url = rpc_url or os.environ.get("RH_SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com"

        self.ticker_mint_map: dict[str, str] = dict(ticker_mint_map or {})
        self.ticker_inst_map: dict[str, str] = {**DEFAULT_OKX_INST_MAP, **(ticker_inst_map or {})}
        self.builder_code: str = getattr(executor, "ai_builder_code", "") or os.environ.get("OKX_AI_BUILDER_CODE", "")

        self.pending_swaps: dict[str, PendingSwap] = {}
        self.submitted_swaps: dict[str, dict] = {}
        self.failed_swaps: dict[str, dict] = {}
        self.buys_triggered = 0
        self.sells_triggered = 0
        self.errors: list[dict[str, Any]] = []
        self.swap_id_counter = 0

        self._log(f"[LIVE] LiveTrader initialized — platform={self.platform.upper()}, "
                  f"executor={type(executor).__name__}, builder_code={self.builder_code or '(not set)'}")

    # ── Main entry ─────────────────────────────────────────────────

    def on_desk_fill(self, fill: Fill) -> PendingSwap | None:
        if fill.side == "ENTRY" and self.enable_buys:
            return self._handle_entry(fill)
        if fill.side in ("EXIT", "STOP") and self.enable_sells:
            return self._handle_exit(fill)
        return None

    def _handle_entry(self, fill: Fill) -> PendingSwap | None:
        if self.platform == "okx":
            return self._okx_build(fill.ticker, "buy", fill.usd, fill.note or "ENTRY")
        if self.platform == "jupiter":
            return self._jupiter_build(fill.ticker, "BUY", fill.usd, fill.note or "ENTRY")
        self._log(f"[LIVE] SKIP {fill.ticker}: unknown platform={self.platform}")
        return None

    def _handle_exit(self, fill: Fill) -> PendingSwap | None:
        if self.platform == "okx":
            return self._okx_build(fill.ticker, "sell", fill.usd, fill.note or f"EXIT ({fill.side})")
        if self.platform == "jupiter":
            return self._jupiter_build(fill.ticker, "SELL", fill.usd, fill.note or f"EXIT ({fill.side})")
        self._log(f"[LIVE] SKIP {fill.ticker}: unknown platform={self.platform}")
        return None

    # ── OKX build path ────────────────────────────────────────────

    def _okx_build(self, ticker: str, okx_side: str, usd: float, note: str) -> PendingSwap | None:
        inst_id = self._resolve_okx_inst(ticker)
        if not inst_id:
            self._log(f"[LIVE] SKIP OKX {okx_side} {ticker}: no OKX instId")
            return None
        if usd < 1.0:
            self._log(f"[LIVE] SKIP OKX {okx_side} {ticker}: stake too small ({usd:.2f} USD)")
            return None
        try:
            quote = self.executor.get_quote(inst_id, side=okx_side, size_usd=usd)
            order = self.executor.build_order(quote, cl_ord_id=f"rh_{ticker}_{okx_side}_{int(time.time())}")
        except Exception as e:
            self._log(f"[LIVE] ERROR OKX {okx_side.upper()} {ticker}: {e}")
            self.errors.append({"platform": "okx", "side": okx_side.upper(), "ticker": ticker, "error": str(e)})
            return None

        pending = PendingSwap(
            ticker=ticker, side=okx_side.upper(), platform="okx",
            amount_usd=usd, quote=quote, bundle=None, okx_order=order,
            inst_id=inst_id, fee_amount_usd=quote.fee_amount_usd,
            created_at=time.time(), note=note, requires_wallet_sign=False,
        )
        self.swap_id_counter += 1
        key = f"{ticker}_{pending.side}_{self.swap_id_counter}"
        self.pending_swaps[key] = pending
        if okx_side == "buy":
            self.buys_triggered += 1
        else:
            self.sells_triggered += 1
        self._log(
            f"[LIVE] OKX {okx_side.upper()} {ticker}: {usd:.2f} USD → {inst_id} "
            f"@ , fee=, "
            f"tag={order.tag or '(none)'}"
        )
        return pending

    # ── Jupiter build path ─────────────────────────────────────────

    def _jupiter_build(self, ticker: str, jp_side: str, usd: float, note: str) -> PendingSwap | None:
        from rh_jupiter_executor import resolve_mint

        mint = self._resolve_mint(ticker, resolve_mint)
        if not mint:
            self._log(f"[LIVE] SKIP JUPITER {jp_side} {ticker}: no mint address")
            return None

        sol_amount = usd / max(self.sol_usd, 0.01)
        lamports = int(sol_amount * SOL_LAMPORTS)
        if lamports < 10_000:
            self._log(f"[LIVE] SKIP JUPITER {jp_side} {ticker}: stake too small")
            return None

        try:
            if jp_side == "BUY":
                quote = self.executor.get_quote("SOL", mint, lamports, self.slippage_bps)
                bundle = self.executor.build_swap(quote, self.user_wallet)
            else:
                rev = self.executor.get_quote("SOL", mint, lamports, self.slippage_bps)
                if rev.out_amount <= 0:
                    self._log(f"[LIVE] SKIP JUPITER SELL {ticker}: zero token amount")
                    return None
                quote = self.executor.get_quote(mint, "SOL", rev.out_amount, self.slippage_bps)
                bundle = self.executor.build_swap(quote, self.user_wallet)
        except Exception as e:
            self._log(f"[LIVE] ERROR JUPITER {jp_side} {ticker}: {e}")
            self.errors.append({"platform": "jupiter", "side": jp_side, "ticker": ticker, "error": str(e)})
            return None

        pending = PendingSwap(
            ticker=ticker, side=jp_side, platform="jupiter",
            amount_usd=usd, quote=quote, bundle=bundle, okx_order=None,
            inst_id=mint, fee_amount_usd=self._estimate_jupiter_fee(quote),
            created_at=time.time(), note=note, requires_wallet_sign=True,
        )
        self.swap_id_counter += 1
        key = f"{ticker}_{jp_side}_{self.swap_id_counter}"
        self.pending_swaps[key] = pending
        if jp_side == "BUY":
            self.buys_triggered += 1
        else:
            self.sells_triggered += 1
        self._log(
            f"[LIVE] JUPITER {jp_side} {ticker}: {usd:.2f} USD, fee="
        )
        return pending

    # ── Resolvers ──────────────────────────────────────────────────

    def _resolve_okx_inst(self, ticker: str) -> str | None:
        """Return OKX instId only if ticker is explicitly mapped.

        No fallback — unknown tickers (e.g. DexScreener memecoins not on
        OKX) must return None so the caller SKIPs them instead of
        constructing a fake `SOLFONE-USDT` that 404s.
        """
        for t in (ticker, ticker.upper(), ticker.lower()):
            if t in self.ticker_inst_map:
                return self.ticker_inst_map[t]
        return None  # not on OKX — SKIP silently

    def _resolve_mint(self, ticker: str, resolve_mint_fn) -> str | None:
        for t in (ticker, ticker.upper(), ticker.lower()):
            if t in self.ticker_mint_map:
                return self.ticker_mint_map[t]
        known = resolve_mint_fn(ticker)
        if known != ticker:
            return known
        for t in (ticker.upper(), ticker.lower()):
            k = resolve_mint_fn(t)
            if k != t:
                return k
        return None

    def _estimate_jupiter_fee(self, quote) -> float:
        if not getattr(quote, "fee_amount", None):
            return 0.0
        mint = getattr(quote, "platform_fee_mint", None)
        USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
        if mint in (USDC, USDT):
            return quote.fee_amount / 1e6
        if mint and "SOL" in str(mint).upper():
            return (quote.fee_amount / SOL_LAMPORTS) * self.sol_usd
        return 0.0

    def _log(self, msg: str) -> None:
        if self.log_fn:
            self.log_fn(msg)

    # ── Submit (platform-aware) ────────────────────────────────────

    def submit_swap(self, swap_key: str, signed_payload: str | None = None) -> dict:
        ps = self.pending_swaps.get(swap_key)
        if not ps:
            return {"ok": False, "error": f"no pending swap for key: {swap_key}"}

        if ps.platform == "okx":
            return self._submit_okx(swap_key, ps)
        if ps.platform == "jupiter":
            return self._submit_jupiter(swap_key, ps, signed_payload)
        return {"ok": False, "error": f"unknown platform: {ps.platform}"}

    def _submit_okx(self, swap_key: str, ps: PendingSwap) -> dict:
        if not ps.okx_order:
            return {"ok": False, "error": "OKX order not built"}
        try:
            result = self.executor.submit_order(ps.okx_order)
            self.submitted_swaps[swap_key] = {
                "platform": "okx",
                "order_id": result.get("order_id", ""),
                "inst_id": result.get("inst_id", ps.inst_id),
                "side": ps.side,
                "amount_usd": ps.amount_usd,
                "fee_usd": ps.fee_amount_usd,
                "tag": result.get("tag", ""),
                "timestamp": time.time(),
            }
            del self.pending_swaps[swap_key]
            self._log(f"[LIVE] OKX SUBMITTED {ps.side} {ps.ticker}: orderId={result.get('order_id','?')}")
            return {"ok": True, "order_id": result.get("order_id", ""), "platform": "okx"}
        except Exception as e:
            self.failed_swaps[swap_key] = {"platform": "okx", "error": str(e), "timestamp": time.time()}
            return {"ok": False, "error": str(e), "platform": "okx"}

    def _submit_jupiter(self, swap_key: str, ps: PendingSwap, signed_payload: str | None) -> dict:
        import urllib.request as _ur
        tx_b64 = signed_payload if signed_payload else getattr(ps.bundle, "transaction_base64", None)
        if not tx_b64:
            return {"ok": False, "error": "empty transaction bundle"}
        if not signed_payload:
            return {"ok": False, "error": "transaction not signed — use Phantom wallet Sign & Submit"}

        try:
            payload = json.dumps({
                "jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
                "params": [tx_b64, {"encoding": "base64", "preflightCommitment": "confirmed"}],
            }).encode("utf-8")
            req = _ur.Request(self.rpc_url, data=payload, headers={"Content-Type": "application/json"})
            with _ur.urlopen(req, timeout=15) as resp:
                rpc_result = json.loads(resp.read())

            if "result" not in rpc_result:
                err = rpc_result.get("error", {}).get("message", str(rpc_result))
                return {"ok": False, "error": str(err), "platform": "jupiter"}

            sig = rpc_result["result"]
            self.submitted_swaps[swap_key] = {
                "platform": "jupiter", "sig": sig, "ticker": ps.ticker,
                "side": ps.side, "amount_usd": ps.amount_usd,
                "fee_usd": ps.fee_amount_usd, "timestamp": time.time(),
            }
            del self.pending_swaps[swap_key]
            self._log(f"[LIVE] JUPITER SUBMITTED {ps.side} {ps.ticker}: sig={sig[:20]}...")
            return {"ok": True, "sig": sig, "platform": "jupiter"}
        except Exception as e:
            self.failed_swaps[swap_key] = {"platform": "jupiter", "error": str(e), "timestamp": time.time()}
            return {"ok": False, "error": str(e), "platform": "jupiter"}

    # ── Dashboard outputs ─────────────────────────────────────────

    def status(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "executor": type(self.executor).__name__,
            "user_wallet": self.user_wallet,
            "rpc_url": self.rpc_url,
            "builder_code": self.builder_code,
            "sol_usd": round(self.sol_usd, 2),
            "buys_triggered": self.buys_triggered,
            "sells_triggered": self.sells_triggered,
            "pending_count": len(self.pending_swaps),
            "pending_tickers": list(self.pending_swaps.keys()),
            "submitted_count": len(self.submitted_swaps),
            "failed_count": len(self.failed_swaps),
            "errors": self.errors[-10:],
        }

    def swaps_snapshot(self) -> list[dict]:
        out = []
        for key, ps in self.pending_swaps.items():
            entry = {
                "key": key, "ticker": ps.ticker, "side": ps.side,
                "platform": ps.platform, "inst_id": ps.inst_id,
                "amount_usd": round(ps.amount_usd, 2),
                "fee_usd": round(ps.fee_amount_usd, 4),
                "requires_wallet_sign": ps.requires_wallet_sign,
                "note": ps.note, "age_s": round(time.time() - ps.created_at, 1),
            }
            if ps.platform == "okx" and ps.okx_order:
                order = ps.okx_order
                entry["order_payload"] = {
                    "instId": order.inst_id, "side": order.side,
                    "ordType": order.ord_type, "sz": order.sz,
                    "tag": order.tag, "tdMode": order.td_mode,
                }
                entry["quote_price"] = round(ps.quote.price_usd, 4) if ps.quote else 0
            elif ps.platform == "jupiter" and ps.bundle:
                entry["transaction_base64"] = ps.bundle.transaction_base64
                entry["tx_len"] = len(ps.bundle.transaction_base64)
            out.append(entry)
        return out
