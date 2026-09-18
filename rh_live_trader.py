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
        self._max_history = 50
        self._pending_swap_timeout = 300  # 5 minutes — swaps older than this are dropped
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
            self._log(f"[LIVE] SKIP OKX {okx_side} {ticker!r}: no OKX instId (map_size={len(self.ticker_inst_map)}, map_keys_sample={list(self.ticker_inst_map.keys())[:5]})")
            return None
        if usd < 1.0:
            self._log(f"[LIVE] SKIP OKX {okx_side} {ticker}: stake too small ({usd:.2f} USD)")
            return None
        try:
            quote = self.executor.get_quote(inst_id, side=okx_side, size_usd=usd)
            order = self.executor.build_order(quote, cl_ord_id=f"rh_{ticker}_{okx_side}_{int(time.time())}")
        except ValueError as e:
            # Order below minimum — log and skip (don't add to pending)
            self._log(f"[LIVE] SKIP OKX {okx_side} {ticker}: {e}")
            return None
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
        """Return OKX instId for a ticker.

        Lookup order:
          1. Exact match in ticker_inst_map (e.g. "SOL" -> "SOL-USDT")
          2. Fallback: if ticker already looks like OKX instId (contains '-'),
             return it directly (e.g. "SOL-USDT" stays "SOL-USDT")
          3. Try appending "-USDT" as a heuristic — ONLY when map is populated
             from OKX live data (non-default map), trust the heuristic.
          4. Return None for unknown tickers not on OKX.
        """
        # 1. Map lookup (covers both DEFAULT_OKX_INST_MAP and live-updated entries)
        for t in (ticker, ticker.upper(), ticker.lower()):
            if t in self.ticker_inst_map:
                return self.ticker_inst_map[t]
        # 2. Already OKX instId format (contains '-')
        if "-" in ticker:
            return ticker.upper()
        # 3. Heuristic: try -USDT append.
        #    When the map has more than just the default entries, trust the heuristic
        #    because OKX live data has already populated the map with real pairs.
        upper = ticker.upper()
        okx_guess = f"{upper}-USDT"
        # Trust if map was extended beyond defaults (i.e. live data loaded)
        if len(self.ticker_inst_map) > len(DEFAULT_OKX_INST_MAP):
            return okx_guess
        # Fallback: only trust well-known base currencies when map is default-only
        well_known = {"BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA",
                      "AVAX", "LINK", "DOT", "MATIC", "UNI", "LTC",
                      "ARB", "OP", "APT", "SUI", "TIA", "SEI", "WLD",
                      "NEAR", "TRX"}
        if upper in well_known:
            return okx_guess
        return None  # unknown — SKIP silently

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

    def build_swap(self, swap_key: str, ticker: str, side: str,
                   amount_usd: float, inst_id: str) -> PendingSwap | None:
        """Build a pending swap without adding to pending_swaps (for external engines)."""
        okx_side = "buy" if side.upper() in ("BUY", "LONG") else "sell"
        print(f"[LIVE_DEBUG] build_swap called: key={swap_key}, ticker={ticker}, side={side}, amount_usd={amount_usd}, inst_id={inst_id}")
        print(f"[LIVE_DEBUG] executor type={type(self.executor).__name__}, auth_ready={getattr(self.executor, '_auth_ready', 'N/A')}")
        try:
            print(f"[LIVE_DEBUG] Calling get_quote for {inst_id} {okx_side} ${amount_usd}")
            quote = self.executor.get_quote(inst_id, side=okx_side, size_usd=amount_usd)
            print(f"[LIVE_DEBUG] get_quote OK: price=${quote.price_usd:.6f} size={quote.size_base:.6f}")
            # P0-2: slippage guard — skip order if price impact is too high
            slippage_pct = getattr(quote, 'price_impact_pct', 0.0)
            SLIPPAGE_LIMIT_PCT = 1.5  # hard reject orders with >1.5% estimated slippage
            if slippage_pct > SLIPPAGE_LIMIT_PCT:
                print(f"[LIVE_DEBUG] REJECT {ticker}: slippage {slippage_pct:.2f}% > {SLIPPAGE_LIMIT_PCT}% limit")
                self._log(f"[LIVE] SKIP {ticker}: slippage {slippage_pct:.2f}% exceeds {SLIPPAGE_LIMIT_PCT}% limit")
                return None
            order = self.executor.build_order(quote, cl_ord_id=f"{swap_key}")
            print(f"[LIVE_DEBUG] build_order OK")
        except ValueError as e:
            print(f"[LIVE_DEBUG] SKIP OKX {okx_side} {ticker}: {e}")
            self._log(f"[LIVE] SKIP OKX {okx_side} {ticker}: {e}")
            return None
        except Exception as e:
            print(f"[LIVE_DEBUG] ERROR OKX {okx_side.upper()} {ticker}: {e}")
            import traceback; traceback.print_exc()
            self._log(f"[LIVE] ERROR OKX {okx_side.upper()} {ticker}: {e}")
            return None
        pending = PendingSwap(
            ticker=ticker, side=side.upper(), platform="okx",
            amount_usd=amount_usd, quote=quote, bundle=None, okx_order=order,
            inst_id=inst_id, fee_amount_usd=quote.fee_amount_usd,
            created_at=time.time(), note=side, requires_wallet_sign=False,
        )
        print(f"[LIVE_DEBUG] PendingSwap created for {ticker} {side}")
        return pending

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
                "ticker": ps.ticker,
                "order_id": result.get("order_id", ""),
                "inst_id": result.get("inst_id", ps.inst_id),
                "side": ps.side,
                "amount_usd": ps.amount_usd,
                "fee_usd": ps.fee_amount_usd,
                "tag": result.get("tag", ""),
                "timestamp": time.time(),
            }
            del self.pending_swaps[swap_key]
            self._trim_history()
            self._log(f"[LIVE] OKX SUBMITTED {ps.side} {ps.ticker}: orderId={result.get('order_id','?')}")
            return {"ok": True, "order_id": result.get("order_id", ""), "platform": "okx", "ticker": ps.ticker}
        except Exception as e:
            self.failed_swaps[swap_key] = {"platform": "okx", "ticker": ps.ticker, "side": ps.side, "amount_usd": ps.amount_usd, "error": str(e), "timestamp": time.time()}
            del self.pending_swaps[swap_key]
            self._trim_history()
            return {"ok": False, "error": str(e), "platform": "okx", "ticker": ps.ticker}

    def _submit_jupiter(self, swap_key: str, ps: PendingSwap, signed_payload: str | None) -> dict:
        import urllib.request as _ur
        tx_b64 = signed_payload if signed_payload else getattr(ps.bundle, "transaction_base64", None)
        if not tx_b64:
            return {"ok": False, "error": "empty transaction bundle"}

        # P2-8: server-side signing — if executor has a private key, sign automatically
        if not signed_payload and hasattr(self.executor, "server_signing_ready") and self.executor.server_signing_ready:
            try:
                tx_b64 = self.executor.sign_transaction(tx_b64)
            except Exception as e:
                return {"ok": False, "error": f"server sign failed: {e}", "platform": "jupiter"}

        if not signed_payload and not (hasattr(self.executor, "server_signing_ready") and self.executor.server_signing_ready):
            return {"ok": False, "error": "transaction not signed — use Phantom wallet Sign & Submit or set RH_JUPITER_PRIVATE_KEY"}

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
            self._trim_history()
            self._log(f"[LIVE] JUPITER SUBMITTED {ps.side} {ps.ticker}: sig={sig[:20]}...")
            return {"ok": True, "sig": sig, "platform": "jupiter"}
        except Exception as e:
            self.failed_swaps[swap_key] = {"platform": "jupiter", "ticker": ps.ticker, "side": ps.side, "amount_usd": ps.amount_usd, "error": str(e), "timestamp": time.time()}
            del self.pending_swaps[swap_key]
            self._trim_history()
            return {"ok": False, "error": str(e), "platform": "jupiter", "ticker": ps.ticker}

    # ── Dashboard outputs ─────────────────────────────────────────

    def _trim_history(self) -> None:
        """Keep only the most recent N entries in submitted/failed to prevent unbounded growth."""
        if len(self.submitted_swaps) > self._max_history:
            sorted_keys = sorted(self.submitted_swaps, key=lambda k: self.submitted_swaps[k].get("timestamp", 0), reverse=True)
            for k in sorted_keys[self._max_history:]:
                del self.submitted_swaps[k]
        if len(self.failed_swaps) > self._max_history:
            sorted_keys = sorted(self.failed_swaps, key=lambda k: self.failed_swaps[k].get("timestamp", 0), reverse=True)
            for k in sorted_keys[self._max_history:]:
                del self.failed_swaps[k]

    def _cleanup_expired_swaps(self) -> None:
        """Remove pending swaps older than _pending_swap_timeout seconds."""
        now = time.time()
        expired = [(k, v.created_at) for k, v in self.pending_swaps.items()
                   if now - v.created_at > self._pending_swap_timeout]
        for k, created_at in expired:
            del self.pending_swaps[k]
            print(f"[Trader] Cleaned up expired pending swap: {k} (age={now - created_at:.0f}s)")

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
        # Cap pending to most recent 100 to prevent massive payload
        pending_items = list(self.pending_swaps.items())[-100:]
        for key, ps in pending_items:
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
