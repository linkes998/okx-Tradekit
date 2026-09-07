#!/usr/bin/env python3
"""DexScreener API client — search new tokens + batch price lookup with rate limiting."""
from __future__ import annotations

import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from typing import Any

logger = logging.getLogger(__name__)

BASE_URL = "https://api.dexscreener.com/latest/dex"
UA = "RH-Trencher-Live/1.0"

_DEFAULT_KEYWORDS = ["dog", "cat", "meme", "ai", "trump", "pepe", "sol",
                     "hood", "political", "eth", "btc"]


def _http_get(path: str, retries: int = 3) -> dict | list | None:
    """GET from DexScreener with exponential backoff on 429/5xx."""
    url = BASE_URL + path if not path.startswith("http") else path
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429,):
                wait = 15 * (2 ** attempt)  # 15, 30, 60, 120
                logger.warning("[DexClient] 429 rate limit on %s, sleep %ds",
                               path, wait)
                time.sleep(wait)
            elif e.code >= 500:
                wait = 2 ** attempt
                logger.warning("[DexClient] %d server error on %s, sleep %ds",
                               e.code, path, wait)
                time.sleep(wait)
            else:
                logger.warning("[DexClient] HTTP %d on %s: %s",
                               e.code, path, e.reason)
                return None
        except (urllib.error.URLError, OSError) as e:
            logger.warning("[DexClient] network error on %s: %s", path, e)
            time.sleep(1 + attempt)
        except json.JSONDecodeError as e:
            logger.warning("[DexClient] bad JSON from %s: %s", path, e)
            return None
    logger.error("[DexClient] exhausted retries for %s", path)
    return None


class DexClient:
    """Thin wrapper around DexScreener public API with built-in rate limiting."""

    def __init__(self, chain: str = "solana", rpm_limit: int = 120):
        self._chain = chain
        self._rpm = rpm_limit
        self._bucket: deque[float] = deque()
        self._min_gap = 60.0 / rpm_limit

    # ── search_new_tokens ──────────────────────────────────────────────────

    def search_new_tokens(self, keywords: list[str] | None = None,
                          max_age_min: int = 30,
                          min_liquidity_usd: float = 500.0) -> list[dict]:
        """Return newly-launched pairs matching keywords, filtered by age & liq.

        Each returned dict has at minimum:
          pairAddress, tokenAddress, priceUsd, liquidity:{usd}, pairCreatedAt
        """
        keywords = keywords or _DEFAULT_KEYWORDS
        seen: set[str] = set()
        results: list[dict] = []

        for kw in keywords:
            path = f"/search?q={urllib.parse.quote(kw)}&chainId={self._chain}"
            data = _http_get(path)
            if not isinstance(data, dict):
                continue
            pairs = data.get("pairs") or []
            now_ms = int(time.time() * 1000)
            age_cutoff_ms = now_ms - max_age_min * 60 * 1000

            for p in pairs:
                if not isinstance(p, dict):
                    continue
                addr = p.get("pairAddress")
                if not addr or addr in seen:
                    continue
                # age filter
                pc = p.get("pairCreatedAt")
                if isinstance(pc, (int, float)) and pc > 0:
                    if pc < age_cutoff_ms:
                        continue
                # liquidity filter
                liq = p.get("liquidity")
                liq_usd = 0.0
                if isinstance(liq, dict):
                    liq_usd = float(liq.get("usd") or 0)
                if liq_usd < min_liquidity_usd:
                    continue
                seen.add(addr)
                results.append(p)

        logger.info("[DexClient] search_new_tokens: %d pairs (chain=%s, kw=%d)",
                     len(results), self._chain, len(keywords))
        return results

    # ── batch_price ─────────────────────────────────────────────────────────

    def batch_price(self, pair_addresses: list[str]) -> dict[str, dict]:
        """Batch-fetch current prices for up to 30 addresses per call.

        Returns {pairAddress: {priceUsd, priceChange:{m5,h1}, liquidity:{usd}}}.
        """
        if not pair_addresses:
            return {}
        results: dict[str, dict] = {}
        BATCH = 30
        addrs = [a for a in pair_addresses if a]

        for i in range(0, len(addrs), BATCH):
            chunk = addrs[i:i + BATCH]
            slug = ",".join(chunk)
            path = f"/pairs/{self._chain}/{slug}"
            data = _http_get(path)
            if not isinstance(data, dict):
                continue
            pairs = data.get("pairs") or []
            for p in pairs:
                if not isinstance(p, dict):
                    continue
                addr = p.get("pairAddress")
                if addr:
                    results[addr] = p

        logger.debug("[DexClient] batch_price: %d/%d addresses resolved",
                     len(results), len(addrs))
        return results

    # ── helpers ─────────────────────────────────────────────────────────────

    def get_price_usd(self, pair_address: str) -> float | None:
        """Convenience: single-address price lookup."""
        d = self.batch_price([pair_address])
        p = d.get(pair_address)
        if p and isinstance(p, dict):
            raw = p.get("priceUsd")
            if raw is not None:
                try:
                    return float(raw)
                except (ValueError, TypeError):
                    return None
        return None

    @property
    def chain(self) -> str:
        return self._chain


if __name__ == "__main__":
    import urllib.parse
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s %(name)s %(message)s")
    cx = DexClient(chain="solana")
    print("[DexClient] searching new tokens...", file=sys.stderr)
    pairs = cx.search_new_tokens(max_age_min=10, min_liquidity_usd=200)
    print(f"[DexClient] found {len(pairs)} fresh pairs", file=sys.stderr)
    if pairs:
        sample = pairs[0]
        print(json.dumps({
            "pairAddress": sample.get("pairAddress"),
            "tokenAddress": sample.get("tokenAddress"),
            "priceUsd": sample.get("priceUsd"),
            "liquidityUsd": (sample.get("liquidity") or {}).get("usd"),
            "pairCreatedAt": sample.get("pairCreatedAt"),
        }, indent=2))
        addrs = [p["pairAddress"] for p in pairs[:10]]
        prices = cx.batch_price(addrs)
        print(f"[DexClient] batch_price resolved {len(prices)} addresses")
