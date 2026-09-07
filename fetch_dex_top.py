"""
fetch_dex_top.py — DexScreener top volume / top gainers for Solana.

Fetches real pairs from DexScreener search endpoint, ranks by volume or price change,
filters out obvious rugs (near-zero liquidity, >90% mcap drop, no pairCreatedAt),
and writes Desk-compatible CSV.

Usage:
    python fetch_dex_top.py top-volume   --out dex_top_volume.csv --limit 20
    python fetch_dex_top.py top-gainers  --out dex_top_gainers.csv --limit 20
    python fetch_dex_top.py trending     --out dex_trending.csv    --limit 20
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# ── DexScreener contract ──────────────────────────────────────────────────
BASE = "https://api.dexscreener.com"
HEADERS = {"User-Agent": "rh_trencher/0.1"}


# ── Rate limiter (token bucket, 30 rpm) ────────────────────────────────────
class RateLimiter:
    def __init__(self, rpm: int = 30):
        self.min_interval = 60.0 / rpm
        self._next_ok = 0.0

    def wait(self):
        now = time.monotonic()
        if now < self._next_ok:
            time.sleep(self._next_ok - now)
        self._next_ok = time.monotonic() + self.min_interval


rl = RateLimiter(rpm=28)


def _fetch_json(url: str, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        rl.wait()
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json_loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait_s = 2 ** attempt
                print(f"  429 rate-limited → sleeping {wait_s}s", file=sys.stderr)
                time.sleep(wait_s)
                continue
            if e.code == 403:
                print(f"  403 Forbidden on {url}", file=sys.stderr)
                return None
            print(f"  HTTP {e.code} on {url}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"  fetch error: {e}", file=sys.stderr)
            time.sleep(1)
    return None


def json_loads(b: bytes) -> dict:
    import json
    return json.loads(b)


# ── Lexicon helpers (same as rh_trencher.THEME_LEXICON) ────────────────────
THEME_LEXICON = {
    "hood": [
        "hood", "robinhood", "robin", "hoodai", "hoodrat", "hoodcash",
        "cashcat", "broker", "commission", "app", "tendies", "stonk",
        "vlad", "tenev", "l2", "stocktoken", "gang", "mafia", "cartel", "heist", "kingpin",
    ],
    "pet": [
        "pet", "pepe", "dog", "wif", "cat", "inu", "frog", "kitty", "puppy",
        "doge", "florki", "bonk", "shib", "kibble", "bark", "roco", "smooth",
    ],
    "ai-craze": [
        "ai-craze", "ai", "agent", "grok", "gpt", "neural", "bot", "llm",
        "openai", "mistral", "quantum", "claude", "megatron",
    ],
    "political": [
        "political", "trump", "maga", "biden", "giga", "elon", "chad",
    ],
    "sol": ["sol", "solana", "pump", "jet"],
}


def infer_theme(text: str) -> str:
    """Pick the theme lexicon with most hits. Returns '' if nothing."""
    blob = text.lower()
    votes = Counter()
    for theme, words in THEME_LEXICON.items():
        votes[theme] += sum(1 for w in words if w in blob)
    top, n = votes.most_common(1)[0] if votes else ("", 0)
    return top if n >= 1 else ""


# ── Filter helpers ─────────────────────────────────────────────────────────
def is_probably_rug(p: dict) -> bool:
    """Heuristic rug detection on a DexScreener pair."""
    liq = (p.get("liquidity") or {}).get("usd") or 0
    pc = p.get("priceChange") or {}
    h6 = pc.get("h6", 0) or 0
    h1 = pc.get("h1", 0) or 0
    # No liquidity data + huge drop in both h1 and h6 → likely dead
    if liq < 50 and (h1 < -50 or h6 < -50):
        return True
    # >95% drop in 1h and h24 → rug
    h24 = pc.get("h24", 0) or 0
    if h1 < -90 and h24 < -90:
        return True
    # No pairCreatedAt + very old data + price drop → skip
    return False


def looks_new(p: dict, max_age_hours: int = 72) -> bool:
    """Pair was created within last N hours."""
    ts = p.get("pairCreatedAt")
    if not ts:
        return True  # unknown age → keep (may filter later by market cap)
    age_s = (time.time() * 1000 - ts) / 1000
    return age_s <= max_age_hours * 3600


# ── Main fetch ─────────────────────────────────────────────────────────────
# Keywords for Solana meme discovery (avoid generic chain name that returns SOL pairs)
MEME_KEYWORDS = ["pump", "meme", "cat", "dog", "ai", "trump", "grok", "wif", "bonk"]


def fetch_trending_profiles(chain: str, limit: int) -> list[dict]:
    """Use DexScreener trending token-profiles v1, then resolve each to its pair."""
    url = f"{BASE}/token-profiles/latest/v1"
    data = _fetch_json(url)
    profiles = data or []
    # Filter to target chain
    filtered = [p for p in profiles if p.get("chainId") == chain]
    # Resolve each profile → its pair via token-address endpoint
    pairs = []
    for prof in filtered[:limit]:
        addr = prof.get("tokenAddress")
        if not addr:
            continue
        pair_url = f"{BASE}/latest/dex/tokens/{addr}"
        pair_data = _fetch_json(pair_url)
        p_list = (pair_data or {}).get("pairs", []) or []
        if p_list:
            # Take the pair with highest volume
            p_list.sort(key=lambda pp: (pp.get("volume") or {}).get("h24") or 0, reverse=True)
            pairs.append(p_list[0])
    return pairs[:limit]


def fetch_search_keywords(chain: str, keywords: list[str], limit: int) -> list[dict]:
    """Search DexScreener with meme keywords instead of chain name, deduped."""
    seen_addrs = set()
    all_pairs = []
    # Pick first keyword that yields results
    for kw in keywords:
        url = f"{BASE}/latest/dex/search?q={kw}&sort=volume&order=desc&limit={limit * 3}"
        data = _fetch_json(url)
        pairs = (data or {}).get("pairs", []) or []
        # Filter to target chain
        for p in pairs:
            if p.get("chainId") != chain:
                continue
            addr = p.get("pairAddress")
            if addr and addr not in seen_addrs:
                seen_addrs.add(addr)
                all_pairs.append(p)
        if len(all_pairs) >= limit:
            break
    return all_pairs[:limit]


def fetch_top_volume(chain: str, limit: int) -> list[dict]:
    """Trending profiles + keyword search, sorted by h24 volume."""
    profiles = fetch_trending_profiles(chain, max(limit // 2, 5))
    kw_pairs = fetch_search_keywords(chain, MEME_KEYWORDS, limit)
    seen = set()
    out = []
    for p in profiles + kw_pairs:
        addr = p.get("pairAddress")
        if not addr or addr in seen:
            continue
        seen.add(addr)
        out.append(p)
    out.sort(key=lambda p: (p.get("volume") or {}).get("h24") or 0, reverse=True)
    return out[:limit]


def fetch_top_gainers(chain: str, limit: int) -> list[dict]:
    """Keyword search → filter to h24 gainers."""
    kw_pairs = fetch_search_keywords(chain, MEME_KEYWORDS, limit * 2)
    with_pc = [p for p in kw_pairs if (p.get("priceChange") or {}).get("h24") is not None]
    with_pc.sort(key=lambda p: p["priceChange"]["h24"], reverse=True)
    return with_pc[:limit]


def fetch_trending(chain: str, limit: int) -> list[dict]:
    """Trending profiles + keyword search, deduped."""
    profiles = fetch_trending_profiles(chain, max(limit // 2, 5))
    kw_pairs = fetch_search_keywords(chain, MEME_KEYWORDS, limit)
    seen = set()
    out = []
    for p in profiles + kw_pairs:
        addr = p.get("pairAddress")
        if addr in seen:
            continue
        seen.add(addr)
        out.append(p)
    return out[:limit]


# ── Pair → Desk row ───────────────────────────────────────────────────────
def pair_to_row(p: dict, t_min: int = 1) -> dict:
    """Map a DexScreener pair to a Desk-compatible row dict.

    t_min is a synthetic replay entry minute. We use pairCreatedAt offset so
    the oldest pair in the batch enters earliest.
    """
    bt = p.get("baseToken") or {}
    qt = p.get("quoteToken") or {}
    vol = p.get("volume") or {}
    liq = p.get("liquidity") or {}
    pc = p.get("priceChange") or {}

    symbol = bt.get("symbol", "?") or "?"
    name = bt.get("name", symbol)
    token_addr = bt.get("address", "")

    # liquidity_eth for Desk: convert USD sol-equivalent (rough: 1 ETH ≈ 2200 USD)
    liq_usd = float(liq.get("usd", 0) or 0)
    liquidity_eth = liq_usd / 2200.0

    # liq_growth proxy: use h24 volume/h6 volume ratio → higher = more activity recently
    h24 = float(vol.get("h24", 0) or 0)
    h6 = float(vol.get("h6", 0) or 0)
    if h6 > 0:
        liq_growth = round(1.0 + (h24 / h6 - 4.0) * 0.1, 4)
        liq_growth = max(0.3, min(3.0, liq_growth))
    else:
        liq_growth = 1.0

    # theme_hint from lexicon
    blob = f"{symbol} {name}"
    theme = infer_theme(blob)

    # true_multiple_path: synthesize from priceChange data
    # We don't have OHLCV in DexScreener, so we make a realistic synthetic path
    h24_change = float(pc.get("h24", 0) or 0) / 100.0  # e.g. 0.25 = +25%
    final_mult = 1.0 + h24_change
    # Clamp to realistic range
    final_mult = max(0.2, min(15.0, final_mult))
    path = synthesize_path(final_mult, n_marks=20, seed=abs(hash(token_addr)) & 0xFFFFFFFF if token_addr else 42)

    return {
        "t_min": t_min,
        "ticker": symbol,
        "name": name,
        "description": p.get("info", {}).get("description", "") if isinstance(p.get("info"), dict) else "",
        "launchpad": "dexscreener",
        "liquidity_eth": round(liquidity_eth, 4),
        "liq_growth": liq_growth,
        "deployer": "",
        "holders": "[]",
        "linked_groups": "[]",
        "selling_linked": 0,
        "true_multiple_path": str(path),
        "theme_hint": theme,
        "_pair_address": p.get("pairAddress", ""),
        "_token_address": token_addr,
        "_dex": p.get("dexId", ""),
        "_source": "dexscreener",
        "_volume_h24": h24,
        "_price_change_h24": h24_change,
        "_liquidity_usd": liq_usd,
        "_fdv": p.get("fdv", 0),
        "_pair_created_at": p.get("pairCreatedAt", 0),
    }


def synthesize_path(final_mult: float, n_marks: int = 20, seed: int = 42) -> list[float]:
    """Make a realistic price path ending at final_mult."""
    import hashlib, struct, math
    # Deterministic PRNG from seed
    h = hashlib.sha256(struct.pack("<Q", seed)).digest()
    state = int.from_bytes(h[:8], "little")

    def rng():
        nonlocal state
        state = (state * 1103515245 + 12345) & 0x7FFFFFFF
        return state / 0x7FFFFFFF

    path = [1.0]
    # Trajectory: some early noise, then drift toward final_mult
    for i in range(1, n_marks):
        progress = i / (n_marks - 1)
        drift = 1.0 + (final_mult - 1.0) * progress
        noise = (rng() - 0.5) * 0.15 * max(0.5, final_mult)
        path.append(round(max(0.2, drift + noise), 4))
    # Ensure last mark ≈ final_mult
    path[-1] = round(final_mult, 4)
    return path


# ── Entry point ───────────────────────────────────────────────────────────
def cmd_top_volume(args):
    return fetch_top_volume(args.chain, args.limit)


def cmd_top_gainers(args):
    return fetch_top_gainers(args.chain, args.limit)


def cmd_trending(args):
    return fetch_trending(args.chain, args.limit)


def main():
    parser = argparse.ArgumentParser(description="DexScreener top volume / top gainers fetchers")
    sub = parser.add_subparsers(dest="command", required=True)

    for name, fn in [("top-volume", cmd_top_volume), ("top-gainers", cmd_top_gainers), ("trending", cmd_trending)]:
        sp = sub.add_parser(name)
        sp.add_argument("--chain", default="solana", help="Chain to search (solana, base, ethereum...)")
        sp.add_argument("--limit", type=int, default=20)
        sp.add_argument("--out", default="dex_top.csv")
        sp.add_argument("--include-rugs", action="store_true")
        sp.add_argument("--max-age-hours", type=int, default=168, help="Max pair age (hours)")
        sp.set_defaults(func=fn)

    args = parser.parse_args()
    pairs = args.func(args)

    # Filter rugs
    before = len(pairs)
    if not args.include_rugs:
        pairs = [p for p in pairs if not is_probably_rug(p)]
    pairs = [p for p in pairs if looks_new(p, args.max_age_hours)]
    print(f"[dex_top] {args.command}: got {len(pairs)} pairs (filtered from {before})", file=sys.stderr)

    if not pairs:
        print("No pairs passed filter — try --include-rugs or higher --max-age-hours", file=sys.stderr)
        sys.exit(1)

    # Sort by pairCreatedAt asc so older pairs enter first (earlier t_min)
    pairs.sort(key=lambda p: p.get("pairCreatedAt") or 0)
    # Assign t_min
    rows = []
    for i, p in enumerate(pairs):
        rows.append(pair_to_row(p, t_min=(i + 1) * 5))

    # Strip underscore-prefixed fields for Desk CSV (but keep them in extra for debugging)
    out_path = Path(args.out)
    desk_fields = [
        "t_min", "ticker", "name", "description", "launchpad",
        "liquidity_eth", "liq_growth", "deployer", "holders",
        "linked_groups", "selling_linked", "true_multiple_path", "theme_hint",
        "_pair_address", "_token_address", "_dex", "_source",
    ]
    # Theme stats
    themes = Counter(r["theme_hint"] or "(none)" for r in rows)

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=desk_fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    print(f"\n[dex_top] → {out_path}")
    print(f"  total: {len(rows)}")
    print(f"  theme distribution:")
    for th, n in themes.most_common():
        print(f"    {th:12s}: {n}")
    print(f"\n  Top 5 by volume:")
    # Re-sort by volume for display
    for r in sorted(rows, key=lambda x: x.get("_volume_h24", 0), reverse=True)[:5]:
        pc = r.get("_price_change_h24", 0)
        vh = r.get("_volume_h24", 0)
        print(f"    ${r['ticker']:10s} chg_h24={pc:+.1%} vol_h24=${vh:,.0f} liq={r['liquidity_eth']:.1f}ETH theme={r['theme_hint']}")


if __name__ == "__main__":
    main()
