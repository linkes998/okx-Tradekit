#!/usr/bin/env python3
"""FX Rate Feed — thread-safe live ETH/SOL USD price cache from CoinGecko."""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

_COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=ethereum,solana&vs_currencies=usd"
)

_FALLBACK_ETH = 3000.0
_FALLBACK_SOL = 150.0
_REFRESH_INTERVAL = 300  # 5 minutes


class FXFeed:
    def __init__(self, interval_seconds: int = _REFRESH_INTERVAL):
        self._lock = threading.Lock()
        self._eth_usd: float = _FALLBACK_ETH
        self._sol_usd: float = _FALLBACK_SOL
        self._last_update: float = 0.0
        self._interval = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ── public API ────────────────────────────────────────────────────────

    def get_eth_usd(self) -> float:
        with self._lock:
            return self._eth_usd

    def get_sol_usd(self) -> float:
        with self._lock:
            return self._sol_usd

    def start(self) -> "FXFeed":
        if self._thread is not None and self._thread.is_alive():
            return self
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="fx-feed")
        self._thread.start()
        logger.info("FXFeed started (interval=%ds)", self._interval)
        return self

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        logger.info("FXFeed stopped")

    # ── internal ───────────────────────────────────────────────────────────

    def _fetch_once(self) -> bool:
        try:
            req = urllib.request.Request(_COINGECKO_URL, headers={
                "Accept": "application/json",
                "User-Agent": "RH-Trencher-FXFeed/1.0",
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            eth = data.get("ethereum", {}).get("usd")
            sol = data.get("solana", {}).get("usd")
            if eth and isinstance(eth, (int, float)) and eth > 0:
                self._eth_usd = float(eth)
            if sol and isinstance(sol, (int, float)) and sol > 0:
                self._sol_usd = float(sol)
            self._last_update = time.time()
            logger.debug("FXFeed fetched: ETH=$%.2f SOL=$%.2f",
                         self._eth_usd, self._sol_usd)
            return True
        except Exception as exc:
            logger.warning("FXFeed fetch failed (%s), using cached values", exc)
            return False

    def _run(self) -> None:
        # Initial fetch before entering loop
        self._fetch_once()
        while not self._stop_event.wait(timeout=self._interval):
            self._fetch_once()


if __name__ == "__main__":
    import json as _json
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    fx = FXFeed()
    fx.start()
    time.sleep(6)
    print(_json.dumps({
        "eth_usd": fx.get_eth_usd(),
        "sol_usd": fx.get_sol_usd(),
        "last_update": time.ctime(fx._last_update),
    }, indent=2))
    fx.stop()
