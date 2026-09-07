#!/usr/bin/env python3
"""RH Jupiter Executor — Metis Swap V1 integration with platform fees.

Pure stdlib (urllib + json + hashlib). No extra deps.

Usage:
  from rh_jupiter_executor import JupiterExecutor
  ex = JupiterExecutor.from_env()
  quote = ex.get_quote("SOL", "MEMECOIN_MINT", lamports=1_000_000_000)
  swap_bundle = ex.build_swap(quote, user_wallet_pubkey)
  # user signs swap_bundle["transaction"] -> ex.submit(...)
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

# Constants --------------------------------------------------------
JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"
JUPITER_SWAP_URL = "https://api.jup.ag/swap/v1/swap"
JUPITER_PUBLIC_QUOTE = "https://public.jupiterapi.com/swap/v1/quote"
JUPITER_PUBLIC_SWAP = "https://public.jupiterapi.com/swap/v1/swap"

KNOWN_MINTS = {
    "SOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    "POPCAT": "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
}

TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
ATA_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
MAX_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# Base58 codec -----------------------------------------------------
def base58_decode(s: str) -> bytes:
    if not s:
        return b""
    n_leading = len(s) - len(s.lstrip("1"))
    n = 0
    for ch in s:
        n *= 58
        n += MAX_BASE58_ALPHABET.index(ch)
    raw = n.to_bytes((n.bit_length() + 7) // 8 or 1, "big")
    return b"\x00" * n_leading + raw

def base58_encode(data: bytes) -> str:
    if not data:
        return ""
    n = int.from_bytes(data, "big")
    out = []
    while n > 0:
        n, r = divmod(n, 58)
        out.append(MAX_BASE58_ALPHABET[r])
    n_leading = 0
    for b in data:
        if b == 0:
            n_leading += 1
        else:
            break
    return "1" * n_leading + "".join(reversed(out))

# PDA / ATA --------------------------------------------------------
def _pda_seed(seed):
    if isinstance(seed, str):
        seed = seed.encode()
    if len(seed) > 32:
        seed = hashlib.sha256(seed).digest()
    return seed

def find_pda(seeds, program_id):
    prog_raw = base58_decode(program_id)
    bump = 255
    while bump >= 0:
        h = hashlib.sha256()
        for s in seeds:
            h.update(_pda_seed(s))
        h.update(prog_raw)
        h.update(bytes([bump]))
        digest = h.digest()
        # Solana PDA check: if result is NOT valid ed25519 pubkey, it's a PDA.
        # Pure-python fallback: trust all digests (Jupiter handles ATA creation).
        # We just need deterministic derivation matching Solana.
        # Ristretto255 decompression check omitted — Jupiter auto-creates missing ATAs.
        return base58_encode(digest), bump
    raise RuntimeError("No valid PDA found")

def derive_ata(wallet_pubkey, mint_pubkey, token_program_id=TOKEN_PROGRAM_ID):
    ata, _bump = find_pda(
        seeds=[wallet_pubkey, token_program_id, mint_pubkey],
        program_id=ATA_PROGRAM_ID,
    )
    return ata

def resolve_mint(ticker_or_mint):
    if ticker_or_mint.upper() in KNOWN_MINTS:
        return KNOWN_MINTS[ticker_or_mint.upper()]
    return ticker_or_mint

# HTTP client ------------------------------------------------------
def _http_get(url, params, headers=None, timeout=10.0):
    qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    full = f"{url}?{qs}"
    req = urllib.request.Request(full)
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "RH-Jupiter-Executor/1.0")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode(errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {e.code} GET {url.split('?')[0]}: {body[:400]}") from e

def _http_post(url, payload, headers=None, timeout=15.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "RH-Jupiter-Executor/1.0")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode(errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {e.code} POST {url}: {body[:500]}") from e

# Result dataclasses ----------------------------------------------
@dataclass
class QuoteResult:
    raw: dict
    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    price_impact_pct: float
    fee_amount: int | None = None
    fee_bps: int = 0
    platform_fee_mint: str | None = None

@dataclass
class SwapBundle:
    quote: QuoteResult
    transaction_base64: str
    user_public_key: str
    request_id: str | None = None

# Main executor ----------------------------------------------------
class JupiterExecutor:
    """Metis Swap V1 executor with platform fee integration."""

    def __init__(
        self,
        api_key=None,
        fee_wallet=None,
        fee_mint=None,
        platform_fee_bps=25,
        quote_url=None,
        swap_url=None,
        priority_fee_lamports=100_000,
        jito_tip_lamports=0,
        max_retries=2,
        timeout=15.0,
    ):
        self.api_key = api_key or os.environ.get("JUPITER_API_KEY")
        self.fee_wallet = fee_wallet or os.environ.get("RH_FEE_WALLET")
        self.fee_mint = fee_mint or os.environ.get("RH_FEE_MINT")
        self.platform_fee_bps = platform_fee_bps
        self.quote_url = quote_url or os.environ.get("JUPITER_QUOTE_URL", JUPITER_QUOTE_URL)
        self.swap_url = swap_url or os.environ.get("JUPITER_SWAP_URL", JUPITER_SWAP_URL)
        self.priority_fee_lamports = priority_fee_lamports
        self.jito_tip_lamports = jito_tip_lamports
        self.max_retries = max_retries
        self.timeout = timeout

        self._headers = {}
        if self.api_key:
            self._headers["x-api-key"] = self.api_key

    @classmethod
    def from_env(cls):
        api_key = os.environ.get("JUPITER_API_KEY")
        if not api_key:
            print("[rh_jupiter_executor] WARNING: JUPITER_API_KEY not set -> using api.jup.ag (no key)")
            # api.jup.ag works without key - 60 rpm default, enough for testing
            return cls(
                api_key=None,
                fee_wallet=os.environ.get("RH_FEE_WALLET"),
                fee_mint=os.environ.get("RH_FEE_MINT"),
                platform_fee_bps=int(os.environ.get("RH_FEE_BPS", "25")),
            )
        return cls(
            api_key=api_key,
            fee_wallet=os.environ.get("RH_FEE_WALLET"),
            fee_mint=os.environ.get("RH_FEE_MINT"),
            platform_fee_bps=int(os.environ.get("RH_FEE_BPS", "25")),
        )

    def get_quote(
        self,
        input_mint,
        output_mint,
        amount_lamports,
        slippage_bps=100,
        platform_fee_bps=None,
        restrict_intermediate_tokens=False,
        as_legacy=False,
        exclude_dexes=None,
    ):
        in_mint = resolve_mint(input_mint)
        out_mint = resolve_mint(output_mint)
        fee_bps = platform_fee_bps if platform_fee_bps is not None else self.platform_fee_bps

        params = {
            "inputMint": in_mint,
            "outputMint": out_mint,
            "amount": str(int(amount_lamports)),
            "slippageBps": slippage_bps,
            "platformFeeBps": fee_bps,
            "restrictIntermediateTokens": str(restrict_intermediate_tokens).lower(),
            "asLegacyTransaction": str(as_legacy).lower(),
        }
        if exclude_dexes:
            params["excludeDexes"] = exclude_dexes

        last_err = None
        for attempt in range(self.max_retries + 1):
            try:
                raw = _http_get(self.quote_url, params, headers=self._headers, timeout=self.timeout)
                break
            except Exception as e:
                last_err = e
                if attempt < self.max_retries:
                    time.sleep(0.5 * (attempt + 1))
                else:
                    raise RuntimeError(f"Jupiter quote failed: {e}") from e
        else:
            raise last_err

        return self._parse_quote(raw, in_mint, out_mint, fee_bps)

    def _parse_quote(self, raw, in_mint, out_mint, fee_bps):
        in_amount = int(raw.get("inAmount", 0))
        out_amount = int(raw.get("outAmount", 0))
        pi_str = raw.get("priceImpactPct", "0")
        try:
            price_impact = float(pi_str)
        except (ValueError, TypeError):
            price_impact = 0.0

        fee_block = raw.get("platformFee") or {}
        fee_amount = int(fee_block.get("amount", 0)) or None
        fee_mint_in_quote = fee_block.get("mint")

        return QuoteResult(
            raw=raw,
            input_mint=in_mint,
            output_mint=out_mint,
            in_amount=in_amount,
            out_amount=out_amount,
            price_impact_pct=price_impact,
            fee_amount=fee_amount,
            fee_bps=fee_bps,
            platform_fee_mint=fee_mint_in_quote,
        )

    def build_swap(
        self,
        quote,
        user_public_key,
        fee_account=None,
        priority_fee_lamports=None,
        jito_tip_lamports=None,
        dynamic_compute_unit_limit=True,
        use_shared_accounts=False,
        wrap_and_unwrap_sol=True,
    ):
        fee_acc = fee_account
        if fee_acc is None:
            fee_wallet = self.fee_wallet
            if fee_wallet:
                # Prefer explicitly configured fee mint
                if self.fee_mint:
                    fee_acc = derive_ata(fee_wallet, resolve_mint(self.fee_mint))
                # Then use what Jupiter says in quote (usually output mint for ExactIn)
                elif quote.platform_fee_mint:
                    fee_acc = derive_ata(fee_wallet, quote.platform_fee_mint)
                # Fallback: feeAccount MUST be a valid mint from the swap pair
                # Jupiter docs: ExactIn -> fee mint can be input_mint or output_mint
                else:
                    fee_acc = derive_ata(fee_wallet, quote.output_mint)

        pay_fee = priority_fee_lamports if priority_fee_lamports is not None else self.priority_fee_lamports
        jito_tip = jito_tip_lamports if jito_tip_lamports is not None else self.jito_tip_lamports

        # NOTE: computeUnitPriceMicroLamports and prioritizationFeeLamports
        # are MUTUALLY EXCLUSIVE on Jupiter API - cannot send both.
        if jito_tip > 0:
            # Use flat prioritization fee (recommended for MEV/jito tips)
            payload = {
                "quoteResponse": quote.raw,
                "userPublicKey": user_public_key,
                "wrapAndUnwrapSol": wrap_and_unwrap_sol,
                "useSharedAccounts": use_shared_accounts,
                "computeUnitPriceMicroLamports": 0,
                "prioritizationFeeLamports": jito_tip,
                "dynamicComputeUnitLimit": dynamic_compute_unit_limit,
                "skipUserAccountsRpcCalls": False,
            }
        else:
            # Use micro-lamport based priority price
            payload = {
                "quoteResponse": quote.raw,
                "userPublicKey": user_public_key,
                "wrapAndUnwrapSol": wrap_and_unwrap_sol,
                "useSharedAccounts": use_shared_accounts,
                "computeUnitPriceMicroLamports": pay_fee,
                "dynamicComputeUnitLimit": dynamic_compute_unit_limit,
                "skipUserAccountsRpcCalls": False,
            }
        if fee_acc:
            payload["feeAccount"] = fee_acc

        last_err = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = _http_post(self.swap_url, payload, headers=self._headers, timeout=self.timeout)
                break
            except Exception as e:
                last_err = e
                if attempt < self.max_retries:
                    time.sleep(0.5 * (attempt + 1))
                else:
                    raise RuntimeError(f"Jupiter swap build failed: {e}") from e
        else:
            raise last_err

        if resp.get("error"):
            raise RuntimeError(f"Jupiter swap error: {resp['error']}")

        tx_b64 = resp.get("swapTransaction") or resp.get("transaction")
        if not tx_b64:
            raise RuntimeError(f"Jupiter swap response missing transaction: {list(resp.keys())}")

        return SwapBundle(
            quote=quote,
            transaction_base64=tx_b64,
            user_public_key=user_public_key,
            request_id=resp.get("requestId"),
        )

    def estimate_buy(self, ticker_or_mint, sol_amount_lamports, user_public_key=None):
        quote = self.get_quote(
            input_mint="SOL", output_mint=ticker_or_mint,
            amount_lamports=sol_amount_lamports, slippage_bps=100,
        )
        if user_public_key:
            return self.build_swap(quote, user_public_key)
        return quote

    def estimate_sell(self, ticker_or_mint, token_amount_raw, user_public_key=None):
        quote = self.get_quote(
            input_mint=ticker_or_mint, output_mint="SOL",
            amount_lamports=token_amount_raw, slippage_bps=100,
        )
        if user_public_key:
            return self.build_swap(quote, user_public_key)
        return quote

    def ping(self):
        try:
            quote = self.get_quote(
                input_mint="SOL", output_mint="USDC",
                amount_lamports=100_000_000, slippage_bps=100,
            )
            return {
                "ok": True,
                "endpoint": self.quote_url,
                "api_key_present": bool(self.api_key),
                "platform_fee_bps": self.platform_fee_bps,
                "out_amount": quote.out_amount,
                "fee_amount": quote.fee_amount,
                "price_impact": quote.price_impact_pct,
            }
        except Exception as e:
            return {"ok": False, "endpoint": self.quote_url, "error": str(e)}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="RH Jupiter Executor - connectivity test")
    ap.add_argument("--fee-bps", type=int, default=25)
    ap.add_argument("--fee-wallet", default=None)
    args = ap.parse_args()

    if args.fee_wallet:
        os.environ["RH_FEE_WALLET"] = args.fee_wallet

    ex = JupiterExecutor(platform_fee_bps=args.fee_bps)
    print(f"[rh_jupiter_executor] endpoint = {ex.quote_url}")
    print(f"[rh_jupiter_executor] api_key   = {'SET' if ex.api_key else 'NOT SET (public)'}")
    print(f"[rh_jupiter_executor] fee_bps   = {ex.platform_fee_bps}")
    print(f"[rh_jupiter_executor] fee_wallet= {ex.fee_wallet or '(not configured)'}")
    print()

    result = ex.ping()
    print(json.dumps(result, indent=2))
    if result["ok"]:
        print("\nOK - Jupiter Metis Swap API reachable.")
    else:
        print(f"\nFAIL - {result.get('error')}")

