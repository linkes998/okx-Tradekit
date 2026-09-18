#!/usr/bin/env python3
"""OKX Data Source — provides real-time market data from OKX for Desk trading.

Replaces DexScreener/Solana-based data with OKX spot market data:
  - Fetch top USDT pairs by volume from OKX public API
  - Convert OKX instruments to TokenLaunch objects for Desk strategy
  - Support periodic refresh from live OKX data

Usage:
  from rh_okx_data import OKXDataSource
  source = OKXDataSource(executor)
  tokens = source.fetch_tokens(top_n=30)
"""
from __future__ import annotations

import time
from typing import Any

from rh_trencher import TokenLaunch


# ── Theme mapping for OKX tickers ────────────────────────────────────
# Map common coin categories to narrative themes
COIN_THEMES: dict[str, str] = {
    # AI / Tech
    "ETH": "ai-craze", "LINK": "ai-craze", "UNI": "ai-craze",
    "RNDR": "ai-craze", "FET": "ai-craze", "NEAR": "ai-craze",
    # Meme / Animal
    "DOGE": "animal", "SHIB": "animal", "PEPE": "animal",
    "WIF": "animal", "BONK": "animal", "FLOKI": "animal",
    # Solana ecosystem
    "SOL": "sol",
    # DeFi
    "AAVE": "hood", "MKR": "hood", "SNX": "hood",
    "CRV": "hood", "SUSHI": "hood", "COMP": "hood",
    # Layer 1
    "ADA": "generic", "DOT": "generic", "AVAX": "generic",
    "MATIC": "generic", "ARB": "generic", "OP": "generic",
    "APT": "generic", "SUI": "generic", "SEI": "generic",
    # Trade / Finance
    "BNB": "hood", "TRX": "generic", "XRP": "generic",
    # Other popular
    "LTC": "generic", "BCH": "generic", "ETC": "generic",
    "FIL": "generic", "ATOM": "generic", "ICP": "generic",
    "INJ": "ai-craze", "TIA": "generic", "WLD": "ai-craze",
    "PYTH": "generic", "JUP": "generic",
}

# Default themes for unknown coins
DEFAULT_THEME = "generic"

# OKX demo trading only supports a limited set of coins.
# Coins not in this list will be skipped to avoid 403/code=1 errors.
DEMO_COMPATIBLE_TICKERS: set[str] = {
    "SOL", "DOGE", "ADA", "LINK", "UNI", "NEAR", "APT", "LTC", "DOT",
}


def _inst_to_tokenlaunch(inst: dict, t_min: int, idx: int) -> TokenLaunch:
    """Convert an OKX instrument dict to a TokenLaunch for Desk.

    OKX provides real price/volume data; we synthesize entry/exit paths
    based on realistic volatility patterns for the asset class.
    """
    inst_id = inst.get("instId") or inst.get("inst_id", "")
    base_ccy = inst_id.split("-")[0] if "-" in inst_id else inst_id
    ticker = base_ccy.upper()

    # Get current price and 24h change for path generation
    last_price = float(inst.get("last") or 0)
    open24h = float(inst.get("open24h") or last_price)
    change_pct = float(inst.get("change_pct") or 0)

    # Determine theme from coin mapping
    theme = COIN_THEMES.get(ticker, DEFAULT_THEME)

    # Generate realistic multiple path based on coin type and volatility
    # Major coins (BTC, ETH, SOL): moderate volatility
    # Altcoins: higher volatility
    # Meme coins: extreme volatility
    if ticker in ("BTC", "ETH"):
        # Blue chips: small moves, realistic for demo
        path_mults = [1.02, 1.05, 1.08, 1.12, 1.18]
    elif ticker in ("SOL", "BNB", "XRP"):
        # Major alts: moderate volatility
        path_mults = [1.05, 1.12, 1.22, 1.35, 1.55]
    elif theme == "animal":
        # Meme coins: high volatility
        path_mults = [1.10, 1.25, 1.50, 2.0, 3.0, 5.0]
    elif theme == "hood":
        # DeFi/finance: steady growth
        path_mults = [1.03, 1.08, 1.15, 1.25, 1.40]
    elif theme == "ai-craze":
        # AI coins: speculative growth
        path_mults = [1.08, 1.18, 1.35, 1.60, 2.2, 3.5]
    else:
        # Generic: moderate
        path_mults = [1.04, 1.10, 1.18, 1.28, 1.42]

    # Apply current trend bias (if 24h change is positive, shift path up)
    if change_pct > 5:
        path_mults = [m * (1 + change_pct / 200) for m in path_mults]
    elif change_pct < -5:
        path_mults = [m * (1 + change_pct / 200) for m in path_mults]

    # Convert to TokenLaunch fields
    # OKX major pairs have real deep liquidity — use realistic ETH-based estimates
    vol_24h = float(inst.get("vol_24h") or 0)
    base_ccy = inst_id.split("-")[0].upper() if "-" in inst_id else inst_id
    if base_ccy in ("BTC", "ETH"):
        liquidity_eth = 500.0 + abs(change_pct) * 2.0  # deep blue-chip books
    elif base_ccy in ("SOL", "BNB", "XRP", "ADA"):
        liquidity_eth = 80.0 + abs(change_pct) * 3.0   # major altcoins
    elif base_ccy in ("DOGE", "DOT", "AVAX", "MATIC"):
        liquidity_eth = 40.0 + abs(change_pct) * 4.0   # mid caps
    else:
        liquidity_eth = 20.0 + abs(change_pct) * 5.0   # smaller caps
    # liq_growth: OKX coins are mature, use modest growth proxy from 24h momentum
    liq_growth = max(1.05, min(3.0, 1.0 + abs(change_pct) / 50.0))

    return TokenLaunch(
        t_min=t_min,
        ticker=ticker,
        name=ticker,
        description=f"{ticker} on OKX spot",
        launchpad="okx",
        liquidity_eth=liquidity_eth,
        liq_growth=liq_growth,
        deployer=f"okx_{ticker.lower()}",
        holders=[],
        linked_groups=[],
        selling_linked=0,
        true_multiple_path=path_mults,
        theme_hint=theme,
    )


class OKXDataSource:
    """Fetch and serve OKX market data as TokenLaunch objects for Desk."""

    def __init__(self, executor):
        self.executor = executor
        self._cache: list[TokenLaunch] = []
        self._cache_ts: float = 0
        self._cache_ttl: float = 300.0  # 5 minute cache

    def fetch_tokens(self, top_n: int = 30, force_refresh: bool = False) -> list[TokenLaunch]:
        """Fetch top N USDT pairs from OKX and convert to TokenLaunch.

        Args:
            top_n: number of top-volume pairs to fetch
            force_refresh: bypass cache and fetch fresh data

        Returns:
            list of TokenLaunch objects ready for Desk
        """
        # Return cached data if fresh enough
        if not force_refresh and self._cache and (time.time() - self._cache_ts) < self._cache_ttl:
            return self._cache

        # Fetch pairs from OKX
        try:
            pairs = self.executor.get_tradable_pairs(quote_ccy="USDT", limit=top_n)
        except Exception as e:
            print(f"[OKXData] Failed to fetch pairs: {e}")
            return self._cache if self._cache else []

        if not pairs:
            print("[OKXData] No pairs returned from OKX")
            return []

        # Convert to TokenLaunch with staggered timestamps
        now_min = int(time.time() / 60)
        tokens = []
        skipped = 0
        for i, pair in enumerate(pairs):
            ticker = pair["base_ccy"].upper()
            if ticker not in DEMO_COMPATIBLE_TICKERS:
                skipped += 1
                continue
            t_min = now_min + len(tokens) * 3  # 3-minute spacing for simulation
            token = _inst_to_tokenlaunch(pair, t_min, len(tokens))
            tokens.append(token)

        print(f"[OKXData] Loaded {len(tokens)} demo-compatible tokens from OKX (skipped {skipped} non-demo coins)")
        if tokens:
            print(f"[OKXData] Available coins: {[t.ticker for t in tokens]}")

        self._cache = tokens
        self._cache_ts = time.time()

        return tokens

    def get_ticker_map(self) -> dict[str, str]:
        """Return ticker -> instId mapping for LiveTrader (demo-compatible only)."""
        try:
            pairs = self.executor.get_tradable_pairs(quote_ccy="USDT", limit=100)
            return {
                p["base_ccy"].upper(): p["inst_id"]
                for p in pairs
                if p["base_ccy"].upper() in DEMO_COMPATIBLE_TICKERS
            }
        except Exception:
            return {}
