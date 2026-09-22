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

import os
import time
from typing import Any

from rh_trencher import TokenLaunch


# ── Real-market entry signal ─────────────────────────────────────────
# The legacy path fabricated `true_multiple_path` from hardcoded volatility
# buckets (meme → 5x, blue chip → 1.18x), so every ticker "launched" on every
# replay no matter what the market was doing, and the paper ledger filled up
# with trades that never existed on OKX. The signal below is computed from real
# OKX candles instead, and drives real perpetual entries.
SIGNAL_BAR = os.environ.get("OKX_SIGNAL_BAR", "15m")
SIGNAL_CANDLES = int(os.environ.get("OKX_SIGNAL_CANDLES", "100"))
# Realized window replayed by the paper desk (real past closes, not a forecast)
SIGNAL_PATH_BARS = int(os.environ.get("OKX_SIGNAL_PATH_BARS", "8"))


def _f(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _sma(vals: list[float], n: int) -> float:
    return sum(vals[-n:]) / n if len(vals) >= n else 0.0


def _rsi(closes: list[float], n: int = 14) -> float:
    """Wilder RSI over chronological closes. 50.0 when there is not enough data."""
    if len(closes) < n + 1:
        return 50.0
    gain = loss = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gain += max(d, 0.0)
        loss += max(-d, 0.0)
    gain /= n
    loss /= n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = (gain * (n - 1) + max(d, 0.0)) / n
        loss = (loss * (n - 1) + max(-d, 0.0)) / n
    if loss <= 0:
        return 100.0
    rs = gain / loss
    return 100.0 - 100.0 / (1.0 + rs)


def _atr_pct(highs: list[float], lows: list[float], closes: list[float], n: int = 14) -> float:
    """ATR as a percentage of the last close — the position's natural noise scale."""
    if len(closes) < n + 1:
        return 0.0
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    last = closes[-1]
    return (sum(trs[-n:]) / n) / last * 100 if last > 0 else 0.0


def evaluate_signal(candles: list, cfg: dict | None = None) -> dict:
    """Score the REAL OKX candles and return an entry decision.

    Rule (deliberately simple and explainable):
      trend  : close > SMA20
      RSI    : within [rsi_min, rsi_max]  (not oversold-broken, not blow-off)
      vol    : ATR% within [atr_min, atr_max]  (enough movement, not chaos)
      trigger: 20-bar breakout OR 10-bar ROC >= roc_min

    Returns {"ok", "reason", "close", "rsi", "sma20", "atr_pct", "path"}.
    `path` is the REALIZED multiple series of the last `path_bars` closes — the
    paper desk replays it, so the ledger reflects real prices, not a forecast.
    """
    cfg = cfg or {}
    rsi_min = cfg.get("rsi_min", _f(os.environ.get("OKX_SIGNAL_RSI_MIN"), 40.0))
    rsi_max = cfg.get("rsi_max", _f(os.environ.get("OKX_SIGNAL_RSI_MAX"), 72.0))
    atr_min = cfg.get("atr_min", _f(os.environ.get("OKX_SIGNAL_ATR_MIN"), 0.15))
    atr_max = cfg.get("atr_max", _f(os.environ.get("OKX_SIGNAL_ATR_MAX"), 8.0))
    roc_min = cfg.get("roc_min", _f(os.environ.get("OKX_SIGNAL_ROC_MIN"), 1.0))

    # OKX returns candles newest-first; indicators need chronological order.
    rows = sorted(candles, key=lambda c: int(c[0])) if candles else []
    if len(rows) < 30:
        return {"ok": False, "reason": f"insufficient candles ({len(rows)})", "path": []}

    closes = [_f(r[4]) for r in rows]
    highs = [_f(r[2]) for r in rows]
    lows = [_f(r[3]) for r in rows]
    last = closes[-1]
    if last <= 0:
        return {"ok": False, "reason": "bad last close", "path": []}

    sma20 = _sma(closes, 20)
    rsi = _rsi(closes, 14)
    atr = _atr_pct(highs, lows, closes, 14)
    prior_high = max(highs[-21:-1])
    roc10 = (last / closes[-11] - 1) * 100 if len(closes) >= 11 and closes[-11] > 0 else 0.0

    breakout = last >= prior_high
    trend = last > sma20
    rsi_ok = rsi_min <= rsi <= rsi_max
    vol_ok = atr_min <= atr <= atr_max
    trigger = breakout or roc10 >= roc_min
    ok = trend and rsi_ok and vol_ok and trigger

    trigger_txt = f"BREAKOUT>={prior_high:.6g}" if breakout else f"ROC10={roc10:+.2f}%"
    reason = (f"{'PASS' if ok else 'SKIP'} close={last:.6g} SMA20={sma20:.6g} "
              f"RSI={rsi:.1f} ATR%={atr:.2f} {trigger_txt}"
              f"{'' if trend else ' [below SMA20]'}{'' if rsi_ok else ' [RSI out]'}"
              f"{'' if vol_ok else ' [ATR out]'}")

    base = closes[-1 - SIGNAL_PATH_BARS] if len(closes) > SIGNAL_PATH_BARS else closes[0]
    path = [round(c / base, 4) for c in closes[-SIGNAL_PATH_BARS:]] if base > 0 else []

    return {
        "ok": ok, "reason": reason, "close": last, "rsi": round(rsi, 2),
        "sma20": round(sma20, 8), "atr_pct": round(atr, 3),
        "breakout": breakout, "roc10": round(roc10, 3), "path": path,
    }


def fetch_signal(executor, inst_id: str, cfg: dict | None = None) -> dict:
    """Fetch candles for inst_id and evaluate the entry signal."""
    try:
        candles = executor.public_candles(inst_id, bar=SIGNAL_BAR, limit=SIGNAL_CANDLES)
    except Exception as e:
        return {"ok": False, "reason": f"candle fetch failed: {e}", "path": []}
    return evaluate_signal(candles, cfg)


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


def _inst_to_tokenlaunch(inst: dict, t_min: int, idx: int, signal: dict | None = None) -> TokenLaunch:
    """Convert an OKX instrument dict to a TokenLaunch for Desk.

    `signal` is the real-candle evaluation from evaluate_signal(); its realized
    multiple path replaces the old fabricated per-theme volatility buckets.
    """
    signal = signal or {}
    inst_id = inst.get("instId") or inst.get("inst_id", "")
    base_ccy = inst_id.split("-")[0] if "-" in inst_id else inst_id
    ticker = base_ccy.upper()

    change_pct = _f(inst.get("change_pct"))

    theme = COIN_THEMES.get(ticker, DEFAULT_THEME)

    # Realized multiple path (real candles) — paper replay only. Empty when the
    # signal could not be computed, in which case the desk opens nothing.
    path_mults = list(signal.get("path") or [])

    # Convert to TokenLaunch fields
    # OKX major pairs have real deep liquidity — use realistic ETH-based estimates
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
        signal_ok=bool(signal.get("ok")),
        signal_reason=str(signal.get("reason", "")),
        signal_ts=time.time(),
        atr_pct=float(signal.get("atr_pct") or 0.0),
    )


class OKXDataSource:
    """Fetch and serve OKX market data as TokenLaunch objects for Desk."""

    def __init__(self, executor, signal_cfg: dict | None = None):
        self.executor = executor
        self._cache: list[TokenLaunch] = []
        self._cache_ts: float = 0
        self._cache_ttl: float = 300.0  # 5 minute cache
        self._signal_cfg: dict = signal_cfg or {}

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
            # Real-candle entry signal: trend/RSI/ATR/breakout on the SWAP chart
            sig = fetch_signal(self.executor, f"{ticker}-USDT-SWAP", self._signal_cfg)
            t_min = now_min + len(tokens) * 3  # 3-minute spacing for simulation
            token = _inst_to_tokenlaunch(pair, t_min, len(tokens), sig)
            tokens.append(token)

        print(f"[OKXData] Loaded {len(tokens)} demo-compatible tokens from OKX (skipped {skipped} non-demo coins)")
        for t in tokens:
            print(f"[OKXData] SIGNAL {t.ticker}: {'PASS' if t.signal_ok else 'SKIP'} — {t.signal_reason}")
        passing = [t.ticker for t in tokens if t.signal_ok]
        print(f"[OKXData] Signal summary: {len(passing)}/{len(tokens)} pass → {passing}")

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
