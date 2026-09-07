#!/usr/bin/env python3
"""RH Live Trader — bridges Desk signal_callback to Jupiter Metis Swap execution.

This module is the glue between RH Trencher's strategy engine and Jupiter's
swap API. Desk produces ENTRY/EXIT/STOP signals via signal_callback; LiveTrader
picks them up and calls Jupiter to build real swap transactions.

Pure Python stdlib + rh_jupiter_executor (no solana SDK needed for building).
User wallet signs the transaction — LiveTrader never holds private keys.

Usage pattern:
    import rh_trencher, rh_live_trader, rh_jupiter_executor

    ex = rh_jupiter_executor.JupiterExecutor.from_env()
    trader = rh_live_trader.LiveTrader(
        executor=ex,
        user_wallet=os.environ["USER_WALLET_PUBKEY"],
        ticker_mint_map={"BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", ...}
    )
    desk = rh_trencher.Desk(signal_callback=trader.on_desk_fill)

    # Feed tokens through desk normally; each ENTRY/EXIT triggers Jupiter quote + build
    for tok in tokens:
        desk.on_launch(tok)
        # ... mark_and_maybe_exit etc.

    # trader.pending_swaps now holds SwapBundle objects awaiting user signature
    for ticker, bundle in trader.pending_swaps.items():
        print(f"[PENDING] {ticker}: {bundle.transaction_base64[:80]}...")
"""
from __future__ import annotations

import base64
import csv
import json
import os
import time
import threading
import urllib.request
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable

from rh_jupiter_executor import (
    JupiterExecutor,
    QuoteResult,
    SwapBundle,
    resolve_mint,
)
from rh_trencher import Fill

# ── Solana / USD constants ──────────────────────────────────────
SOL_LAMPORTS = 1_000_000_000          # 1 SOL in lamports
DEFAULT_ETH_USD = 3000.0
SOL_USD_FALLBACK = 200.0               # fallback for estimate (replace with real feed)
SOLANA_RPC_URL_DEFAULT = "https://api.mainnet-beta.solana.com"


@dataclass
class PendingSwap:
    """A swap awaiting user wallet signature."""
    ticker: str
    side: str                  # "BUY" or "SELL"
    quote: QuoteResult
    bundle: SwapBundle
    amount_usd: float
    fee_amount_raw: int | None = None
    fee_amount_usd: float = 0.0
    created_at: float = 0.0
    note: str = ""


class LiveTrader:
    """Translates Desk fill signals into Jupiter swap bundles.

    The strategy engine (Desk) decides WHAT to trade; this layer
    decides HOW to execute via Jupiter's Metis Swap API.

    Thread safety:
        on_desk_fill() runs inside Desk's main tick loop (single-threaded).
        _signal_queue + pending_swaps are also updated on that thread.
        External readers can poll pending_swaps safely (GIL-protected dict).
    """

    def __init__(
        self,
        executor: JupiterExecutor,
        user_wallet: str | None = None,
        ticker_mint_map: dict[str, str] | None = None,
        sol_usd: float = SOL_USD_FALLBACK,
        eth_usd: float = DEFAULT_ETH_USD,
        slippage_bps: int = 100,
        enable_buys: bool = True,
        enable_sells: bool = True,
        log_fn: Callable[[str], None] | None = print,
        rpc_url: str | None = None,
    ):
        self.executor = executor
        self.user_wallet = user_wallet or executor.fee_wallet or ""
        self.ticker_mint_map: dict[str, str] = dict(ticker_mint_map or {})
        self.sol_usd = sol_usd
        self.eth_usd = eth_usd
        self.slippage_bps = slippage_bps
        self.enable_buys = enable_buys
        self.enable_sells = enable_sells
        self.log_fn = log_fn
        self.rpc_url = rpc_url or os.environ.get("RH_SOLANA_RPC_URL") or SOLANA_RPC_URL_DEFAULT

        # Output: swaps awaiting user signature
        self.pending_swaps: dict[str, PendingSwap] = {}
        # Track submitted swaps and their results
        self.submitted_swaps: dict[str, dict] = {}
        self.failed_swaps: dict[str, dict] = {}

        # Statistics
        self.buys_triggered = 0
        self.sells_triggered = 0
        self.errors: list[dict[str, Any]] = []
        self.swap_id_counter = 0

    # ── main entry point: called by Desk.signal_callback ──────────

    def on_desk_fill(self, fill: Fill) -> PendingSwap | None:
        """Handle a Desk fill event. Only ENTRY / EXIT / STOP produce swaps.

        Returns a PendingSwap if a Jupiter bundle was built, else None.
        """
        if fill.side == "ENTRY" and self.enable_buys:
            return self._handle_entry(fill)
        if fill.side in ("EXIT", "STOP") and self.enable_sells:
            return self._handle_exit(fill)
        return None

    # ── ENTRY → BUY via Jupiter (SOL → TOKEN) ─────────────────────

    def _handle_entry(self, fill: Fill) -> PendingSwap | None:
        """Turn an ENTRY signal into a Jupiter BUY swap bundle.

        Desk says "I want to buy $X worth of TICKER". We need:
          1. Resolve ticker → mint address (DexScreener CSV has mint via _pool_id,
             or ticker_mint_map provides it upfront).
          2. Convert fill.usd (stake USD) → SOL lamports using sol_usd rate.
          3. Get quote SOL → TOKEN via Jupiter Metis.
          4. Build swap transaction bundle for user to sign.
        """
        ticker = fill.ticker
        mint = self._resolve_mint(ticker)
        if not mint:
            self._log(f"[LIVE] SKIP ENTRY {ticker}: no mint address (add to ticker_mint_map)")
            return None

        # USD → SOL → lamports
        sol_amount = fill.usd / max(self.sol_usd, 0.01)
        lamports = int(sol_amount * SOL_LAMPORTS)
        if lamports < 10_000:  # < 0.00001 SOL = dust
            self._log(f"[LIVE] SKIP ENTRY {ticker}: stake too small ({fill.usd:.2f} USD)")
            return None

        try:
            quote = self.executor.get_quote(
                input_mint="SOL",
                output_mint=mint,
                amount_lamports=lamports,
                slippage_bps=self.slippage_bps,
            )
            bundle = self.executor.build_swap(quote, self.user_wallet)
        except Exception as e:
            self._log(f"[LIVE] ERROR ENTRY {ticker}: {e}")
            self.errors.append({"side": "BUY", "ticker": ticker, "error": str(e)})
            return None

        swap = PendingSwap(
            ticker=ticker,
            side="BUY",
            quote=quote,
            bundle=bundle,
            amount_usd=fill.usd,
            fee_amount_raw=quote.fee_amount,
            fee_amount_usd=self._estimate_fee_usd(quote),
            created_at=__import__("time").time(),
            note=fill.note or "ENTRY signal from Desk",
        )
        self.swap_id_counter += 1
        key = f"{ticker}_BUY_{self.swap_id_counter}"
        self.pending_swaps[key] = swap
        self.buys_triggered += 1
        self._log(
            f"[LIVE] BUY {ticker}: {fill.usd:.2f} USD → SOL→TOKEN, "
            f"fee={swap.fee_amount_usd:.4f} USD, impact={quote.price_impact_pct:.3f}%"
        )
        return swap

    # ── EXIT → SELL via Jupiter (TOKEN → SOL) ─────────────────────

    def _handle_exit(self, fill: Fill) -> PendingSwap | None:
        """Turn an EXIT or STOP signal into a Jupiter SELL swap bundle.

        IMPORTANT: Desk's mark_and_maybe_exit computes `multiple * entry_usd`
        but doesn't track the raw token amount held. For a real sell we need
        the actual TOKEN balance (atomic units). We derive it from entry_usd
        and the entry price proxy.

        For live trading, the caller should pre-populate position_token_amounts
        with actual on-chain balances. Without it, we estimate from Desk state.
        """
        ticker = fill.ticker
        mint = self._resolve_mint(ticker)
        if not mint:
            self._log(f"[LIVE] SKIP EXIT {ticker}: no mint address")
            return None

        # Desk reports usd_out = entry_usd * multiple * 0.985 (slippage already baked in)
        usd_out_est = fill.usd
        sol_amount = usd_out_est / max(self.sol_usd, 0.01)
        # We want to RECEIVE ~sol_amount SOL → quote TOKEN → SOL
        # For ExactIn we need raw token amount to sell.
        # Strategy: first get a reverse quote (SOL → TOKEN) at the equivalent SOL amount,
        # then use the returned out_amount as our token sell amount.
        sell_lamports = max(int(sol_amount * SOL_LAMPORTS), 10_000)

        try:
            # Get TOKEN amount that ~ equals sell_lamports SOL value
            rev_quote = self.executor.get_quote(
                input_mint="SOL",
                output_mint=mint,
                amount_lamports=sell_lamports,
                slippage_bps=self.slippage_bps,
            )
            token_amount = rev_quote.out_amount
            if token_amount <= 0:
                self._log(f"[LIVE] SKIP EXIT {ticker}: zero token amount from reverse quote")
                return None

            # Now quote SELL and build swap
            quote = self.executor.get_quote(
                input_mint=mint,
                output_mint="SOL",
                amount_lamports=token_amount,
                slippage_bps=self.slippage_bps,
            )
            bundle = self.executor.build_swap(quote, self.user_wallet)
        except Exception as e:
            self._log(f"[LIVE] ERROR EXIT {ticker}: {e}")
            self.errors.append({"side": "SELL", "ticker": ticker, "error": str(e)})
            return None

        swap = PendingSwap(
            ticker=ticker,
            side="SELL",
            quote=quote,
            bundle=bundle,
            amount_usd=usd_out_est,
            fee_amount_raw=quote.fee_amount,
            fee_amount_usd=self._estimate_fee_usd(quote),
            created_at=__import__("time").time(),
            note=fill.note or f"EXIT ({fill.side}) signal from Desk",
        )
        self.swap_id_counter += 1
        key = f"{ticker}_SELL_{self.swap_id_counter}"
        self.pending_swaps[key] = swap
        self.sells_triggered += 1
        self._log(
            f"[LIVE] SELL {ticker}: token→SOL est {usd_out_est:.2f} USD, "
            f"fee={swap.fee_amount_usd:.4f} USD, impact={quote.price_impact_pct:.3f}%"
        )
        return swap

    # ── helpers ────────────────────────────────────────────────────

    def _resolve_mint(self, ticker: str) -> str | None:
        # Case-insensitive ticker resolution
        if ticker in self.ticker_mint_map:
            return self.ticker_mint_map[ticker]
        upper = ticker.upper()
        if upper in self.ticker_mint_map:
            return self.ticker_mint_map[upper]
        lower = ticker.lower()
        if lower in self.ticker_mint_map:
            return self.ticker_mint_map[lower]
        known = resolve_mint(ticker)
        if known != ticker:
            return known
        for t in (upper, lower):
            k = resolve_mint(t)
            if k != t:
                return k
        return None

    def _estimate_fee_usd(self, quote: QuoteResult) -> float:
        """Rough USD estimate of platform fee (for logging only)."""
        if not quote.fee_amount or not quote.platform_fee_mint:
            return 0.0
        mint = quote.platform_fee_mint
        # Known USD-pegged tokens
        if mint in ("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"):  # USDT
            return quote.fee_amount / 1e6
        if mint == resolve_mint("SOL"):
            return (quote.fee_amount / SOL_LAMPORTS) * self.sol_usd
        # memecoin fee → hard to value, skip
        return 0.0

    def _log(self, msg: str) -> None:
        if self.log_fn:
            self.log_fn(msg)

    # ── RPC helpers ───────────────────────────────────────────────

    @staticmethod
    def _get_sol_price() -> float:
        """Fetch current SOL price from CoinGecko (fallback: env or default)."""
        try:
            import urllib.request
            url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
            req = urllib.request.Request(url, headers={"User-Agent": "RH-LiveTrader/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                return float(data["solana"]["usd"])
        except Exception:
            pass
        return float(os.environ.get("RH_SOL_USD", SOL_USD_FALLBACK))

    @staticmethod
    def _send_rpc_request(rpc_url: str, method: str, params: list) -> dict:
        """Send a raw JSON-RPC call to Solana node."""
        payload = json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        }).encode("utf-8")
        req = urllib.request.Request(
            rpc_url, data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())

    def submit_swap(self, swap_key: str, signed_tx_b64: str | None = None) -> dict:
        """Submit a pending swap on-chain via RPC.

        Two modes:
          1. signed_tx_b64 provided → use the pre-signed tx directly from frontend.
             This is the normal Phantom/Solflare flow: frontend signs the
             Jupiter-provided base64 tx, sends it here, we broadcast.
          2. signed_tx_b64 None → try submitting the raw Jupiter output as-is.
             Fails on mainnet (missing user signature) — only useful for offline
             testing. We warn and return an error if no signature given.

        Returns {"ok": True, "sig": "base58_sig"} or {"ok": False, "error": "..."}
        """
        ps = self.pending_swaps.get(swap_key)
        if not ps:
            return {"ok": False, "error": f"no pending swap for key: {swap_key}"}

        tx_b64 = signed_tx_b64 if signed_tx_b64 else ps.bundle.transaction_base64
        if not tx_b64:
            return {"ok": False, "error": "empty transaction bundle"}

        if not signed_tx_b64:
            return {"ok": False, "error": "transaction not signed — connect Phantom wallet and click Sign & Submit"}

        try:
            rpc_result = self._send_rpc_request(
                self.rpc_url,
                "sendTransaction",
                [tx_b64, {"encoding": "base64", "preflightCommitment": "confirmed"}],
            )
            if "result" not in rpc_result:
                err = rpc_result.get("error", {}).get("message", str(rpc_result))
                return {"ok": False, "error": str(err)}

            sig = rpc_result["result"]
            self.submitted_swaps[swap_key] = {
                "sig": sig,
                "side": ps.side,
                "ticker": ps.ticker,
                "amount_usd": ps.amount_usd,
                "fee_usd": ps.fee_amount_usd,
                "timestamp": time.time(),
            }
            del self.pending_swaps[swap_key]
            self._log(f"[LIVE] SUBMITTED {ps.side} {ps.ticker}: sig={sig[:20]}...")
            return {"ok": True, "sig": sig}
        except Exception as e:
            self.failed_swaps[swap_key] = {
                "error": str(e),
                "side": ps.side,
                "ticker": ps.ticker,
                "timestamp": time.time(),
            }
            return {"ok": False, "error": str(e)}

    def confirm_tx(self, signature: str, commitment: str = "confirmed") -> dict:
        """Poll for transaction confirmation status."""
        try:
            rpc_result = self._send_rpc_request(
                self.rpc_url,
                "getSignatureStatuses",
                [[signature]],
            )
            result = rpc_result.get("result", {})
            if result and result.get("value"):
                statuses = result["value"]
                if statuses and statuses[0]:
                    return {"ok": True, "status": statuses[0].get("confirmationStatus", "unknown")}
            return {"ok": True, "status": "not_confirmed_yet"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def refresh_sol_price(self) -> float:
        """Update sol_usd from live price feed."""
        self.sol_usd = self._get_sol_price()
        self._log(f"[LIVE] SOL price updated: ${self.sol_usd:.2f}")
        return self.sol_usd

    def status(self) -> dict[str, Any]:
        return {
            "user_wallet": self.user_wallet,
            "rpc_url": self.rpc_url,
            "sol_usd": round(self.sol_usd, 2),
            "buys_triggered": self.buys_triggered,
            "sells_triggered": self.sells_triggered,
            "pending_count": len(self.pending_swaps),
            "pending_tickers": list(self.pending_swaps.keys()),
            "submitted_count": len(self.submitted_swaps),
            "submitted_tickers": list(self.submitted_swaps.keys()),
            "failed_count": len(self.failed_swaps),
            "errors": self.errors[-10:],
            "recent_submitted": [
                {**v, "sig": v.get("sig", "")[:20] + "..."}
                for v in list(self.submitted_swaps.values())[-5:]
            ],
        }


# ── DexScreener CSV → ticker_mint_map helper ─────────────────────

def build_ticker_mint_map_from_csv(csv_path: str) -> dict[str, str]:
    """Extract ticker → mint pubkey mapping from a DexScreener CSV.

    DexScreener CSV 中可能有 _token_address / _mint 等字段。
    这个 helper 先读 header，然后尝试各可能字段来找到 mint。
    """
    import csv
    import os

    if not os.path.exists(csv_path):
        return {}

    result: dict[str, str] = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        # Find the mint column — try known names
        mint_col = None
        for candidate in ("_token_address", "_mint", "mint_address", "token_address",
                          "_pool_id", "baseTokenAddress", "quoteTokenAddress"):
            if candidate in headers:
                mint_col = candidate
                break

        for row in reader:
            ticker = row.get("ticker", "").strip()
            mint = (row.get(mint_col) or "").strip() if mint_col else ""
            if ticker and mint and mint != "pool1":
                result[ticker.upper()] = mint
    return result


# ── CLI: smoke test ──────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import time as _time

    ap = argparse.ArgumentParser(description="RH LiveTrader smoke test")
    ap.add_argument("--fee-wallet", required=True, help="Your Solana wallet pubkey (receives platform fees)")
    ap.add_argument("--user-wallet", required=True, help="Trader's Solana wallet pubkey (signs swaps)")
    ap.add_argument("--fee-bps", type=int, default=25)
    ap.add_argument("--sol-usd", type=float, default=200.0)
    args = ap.parse_args()

    os.environ["RH_FEE_WALLET"] = args.fee_wallet
    os.environ["RH_FEE_BPS"] = str(args.fee_bps)

    from rh_jupiter_executor import JupiterExecutor
    from rh_trencher import Desk, TokenLaunch

    print("=" * 60)
    print("RH LiveTrader Smoke Test")
    print("=" * 60)

    ex = JupiterExecutor.from_env()
    trader = LiveTrader(
        executor=ex,
        user_wallet=args.user_wallet,
        ticker_mint_map={"BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
                         "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"},
        sol_usd=args.sol_usd,
    )
    desk = Desk(
        start_usd=500,
        max_positions=2,
        live_mode=True,
        realistic=True,
        signal_callback=trader.on_desk_fill,
    )

    # Simulate Desk lifecycle: one ENTRY (BONK) → some ticks → one EXIT
    print("\n[TEST 1] ENTRY BONK...")
    bonk = TokenLaunch(
        t_min=10, ticker="BONK", name="Bonk", description="Bonk memecoin",
        launchpad="raydium", liquidity_eth=1.0, liq_growth=5.0,
        deployer="x", holders=[], linked_groups=[], selling_linked=0,
        true_multiple_path=[3.0, 5.0, 8.0, 4.0],
        theme_hint="pet", _pool_id="pool_bonk",
    )
    desk.on_launch(bonk)

    print(f"\n[TEST 2] Ticking BONK up to 5x...")
    for mult in [1.5, 2.5, 4.0, 5.5, 7.0]:
        desk.observe_runner(bonk, mult)
        desk.mark_and_maybe_exit(bonk, 10 + int(mult), mult)
        _time.sleep(0.1)

    print("\n" + "=" * 60)
    print("FINAL STATUS")
    print("=" * 60)
    print(json.dumps(trader.status(), indent=2))
    print("\nPending swaps:")
    for key, ps in trader.pending_swaps.items():
        print(f"  {ps.side} {ps.ticker}: ${ps.amount_usd:.2f}, fee=${ps.fee_amount_usd:.4f}")
        print(f"    tx_b64: {ps.bundle.transaction_base64[:60]}...")

    print("\n[DONE] LiveTrader smoke test complete.")

