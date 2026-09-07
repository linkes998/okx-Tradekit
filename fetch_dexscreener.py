#!/usr/bin/env python3
"""Fetch real token-launch data from DexScreener → CSV that rh_trencher.Desk can consume.

Pure stdlib, no extra deps. Rate-limited per DexScreener tiers (300 rpm DEX / 60 rpm profiles).

Usage examples:
  python fetch_dexscreener.py --chain solana --out recent_sol.csv
  python fetch_dexscreener.py --chain base  --out recent_base.csv --max-pairs 300
  python fetch_dexscreener.py --import-csv recent_sol.csv    # round-trip: load → print TokenLaunch summary

Discovery strategy (DexScreener has no wildcard endpoint):
  1. Pull /token-profiles/latest/v1 — get 30 recently-enhanced tokens (slow tier, 60 rpm)
  2. Pull /latest/dex/search?q={dog,cat,meme,ai,trump,pepe,sol} — keyword searches (fast tier, 300 rpm)
  3. Merge + dedup by pairAddress
  4. For each pair, optionally call /token-pairs/v1/{chain}/{tokenAddress} for extra pools +
     fresh liquidity snapshot (configurable, default OFF to keep request count low)

Output CSV columns (aligned with rh_trencher.TokenLaunch fields + provenance hints):
  t_min               pairs[REAL] pairCreatedAt converted to minutes before newest observed launch
  ticker              [REAL] baseToken.symbol
  name                [REAL] baseToken.name
  description         [REAL] token-profile.description (search-result pairs almost never have info.description)
  launchpad           [REAL] dexId (raydium / orca / meteora / pump.fun ...)
  liquidity_eth       [REAL] liquidity.usd / ETH_USD (approx, ETH=$3000 fixed)
  liq_growth          [PROXY] priceChange.h6 as a rough momentum proxy
  deployer            [MISSING] DexScreener doesn't expose mint signer — needs Solana RPC
  holders             [MISSING] DexScreener has no holder list — needs Solana RPC
  linked_groups       [MISSING] wallet clustering requires RPC + flow analysis
  selling_linked      [MISSING] scene-only ground truth
  true_multiple_path  [PROXY] linear interpolation from priceChange snapshots (NOT historical OHLCV)
  theme_hint          [EMPTY] let Desk.NarrativeEngine decide from description + ticker + launchpad
  # columns starting with _ are provenance helpers (pairAddress, tokenAddress, url, priceUsd, ...)

See MIGRATION.md or chat history for full provenance legend.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── constants ──────────────────────────────────────────────────────────────────
ETH_USD = 3000.0
BASE_URL = "https://api.dexscreener.com"
UA = "rh_trencher/0.1 (personal research bot)"
RATE_DEX = 300 / 60       # fast tier: 5 rps
RATE_PROFILE = 60 / 60    # slow tier: 1 rps
DEFAULT_KEYWORDS = ["dog", "cat", "meme", "ai", "trump", "pepe", "sol"]
PROVENANCE_FIELDS = [
    "_pair_address", "_token_address", "_price_usd", "_fdv", "_mcap", "_url", "_dex",
]


# ── token-bucket rate limiter ──────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, permits_per_sec: float):
        self._q: deque[float] = deque()
        self._interval = 1.0 / permits_per_sec

    def acquire(self):
        now = time.monotonic()
        while self._q and now - self._q[0] > 1.0:
            self._q.popleft()
        if self._q:
            gap = self._interval - (now - self._q[-1])
            if gap > 0:
                time.sleep(max(0, gap))
        self._q.append(time.monotonic())


def http_get(path: str, rl: RateLimiter, retries: int = 3) -> dict | list | None:
    rl.acquire()
    url = BASE_URL + path
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read()
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (403, 429):
                wait = 2 ** attempt
                print(f"  [rate-limit {e.code}] {path} → sleep {wait}s", file=sys.stderr)
                time.sleep(wait)
                rl.acquire()
            elif e.code >= 500:
                time.sleep(1 + attempt)
            else:
                print(f"  [http {e.code}] {path}: {e.reason}", file=sys.stderr)
                return None
        except Exception as e:
            print(f"  [network] {path}: {e}", file=sys.stderr)
            time.sleep(1 + attempt)
    return None


# ── Discovery ─────────────────────────────────────────────────────────────────
def discover_pairs(chain: str, rl_dex: RateLimiter, rl_prof: RateLimiter,
                   keywords: list[str] | None = None) -> list[dict]:
    """Multi-source discovery: profiles + keyword searches → deduped pair list.

    All returned pairs share the DexScreener pair schema
    (pairCreatedAt, priceUsd, liquidity, baseToken, etc.).
    Profiles contribute description when a search-result pair's info is empty.
    """
    keywords = keywords or DEFAULT_KEYWORDS

    # 1) Profiles (slow tier — don't need descriptions; we still get tokenAddress list)
    profiles = http_get("/token-profiles/latest/v1", rl_prof) or []
    profile_map = {
        p["tokenAddress"]: p
        for p in profiles
        if isinstance(p, dict) and p.get("chainId") == chain and "tokenAddress" in p
    }
    print(f"  [profiles] chain={chain} → {len(profile_map)} token-address→description lookups")

    # 2) Keyword searches (fast tier)
    seen_tokens: set[str] = set()
    pairs: list[dict] = []
    total_raw = 0
    for kw in keywords:
        data = http_get(f"/latest/dex/search?q={kw}", rl_dex) or {}
        for p in data.get("pairs", []):
            if not isinstance(p, dict) or p.get("chainId") != chain:
                continue
            total_raw += 1
            bt = p.get("baseToken") or {}
            token_addr = bt.get("address", "")

            # Skip pairs whose base is a wrapped native (SOL / WETH / USDC / etc.)
            if bt.get("symbol", "").upper() in {"SOL", "WSOL", "WETH", "ETH", "WBTC",
                                                 "USDC", "USDT", "BUSD", "DAI"}:
                continue

            # Enrich description from profile feed if pair.info is empty
            info = dict(p.get("info") or {})
            if not info.get("description") and token_addr in profile_map:
                desc = profile_map[token_addr].get("description")
                if desc:
                    info["description"] = desc
            p = dict(p)
            if info:
                p["info"] = info

            pairs.append(p)
        print(f"  [search q={kw}] kept={sum(1 for _ in [])} raw", file=sys.stderr)

    # Dedup by pairAddress (not tokenAddress — a token can have multiple pools)
    dedup: dict[str, dict] = {}
    for p in pairs:
        key = p.get("pairAddress", "")
        if key and key not in dedup:
            dedup[key] = p
        elif key and dedup[key]:
            cur_info = dedup[key].get("info") or {}
            new_info = p.get("info") or {}
            if len(new_info) > len(cur_info):
                dedup[key] = p
    unique = list(dedup.values())

    # CRITICAL: DexScreener /latest/dex/search sorts by LIQUIDITY (big pairs first),
    # not by recency. We MUST re-sort by pairCreatedAt DESC to surface new launches.
    unique.sort(key=lambda p: -(p.get("pairCreatedAt") or 0))

    print(f"  [search] {total_raw} raw keyword hits → {len(unique)} unique pairs "
          f"(after wrapped-native filter + dedup, sorted newest-first)")
    return unique


# ── TokenLaunch mapping ───────────────────────────────────────────────────────
def _estimate_multiple_path(pair: dict) -> list[float]:
    """Proxy for true_multiple_path: priceChange snapshots → linear interpolation.

    DexScreener only gives current % change over m5/h1/h6/h24 windows, not historical
    OHLCV. We treat priceChange.x = (price_now - price_x_min_ago) / price_x_min_ago,
    so price_x_min_ago = price_now / (1 + priceChange.x / 100). We build anchors
    oldest→newest→now and interpolate 5 waypoints.

    This is a coarse proxy — historical OHLCV from a source like GeckoTerminal or
    Jupiter API is needed for a proper true_multiple_path.
    """
    pc = pair.get("priceChange") or {}

    def mult_at_window(pct_key: str) -> float | None:
        v = pc.get(pct_key)
        if v is None or (isinstance(v, float) and v != v):
            return None
        return max(0.01, 1.0 + float(v) / 100.0)

    anchors: list[tuple[int, float]] = []  # (minutes_ago, mult)
    # Order matters: oldest → newest → now
    for win, mins in [("h24", 1440), ("h6", 360), ("h1", 60), ("m5", 5)]:
        m = mult_at_window(win)
        if m is not None:
            anchors.append((mins, m))

    if not anchors:
        return [1.0]

    # Drop anchors that are *older than* pairCreatedAt (impossible — pair didn't exist then)
    created_ms = pair.get("pairCreatedAt") or 0
    age_min = max(0, (time.time() * 1000 - created_ms) // 60000) if created_ms else 1440
    anchors = [(mins, m) for mins, m in anchors if mins <= age_min + 30]

    if not anchors:
        return [1.0]

    # Oldest anchor → now (now = 1.0 by definition of priceChange)
    oldest_mins, _ = anchors[0]
    n = 5
    path = [round(1.0 + (oldest_mult - 1.0) * i / (n - 1), 3)
            for i, oldest_mult in enumerate([anchors[0][1]] * n)]
    return path


def pair_to_row(pair: dict, t_anchor_ms: int) -> dict[str, Any]:
    """Map a DexScreener pair dict → TokenLaunch-shaped row dict + provenance fields.

    t_anchor_ms = pairCreatedAt of the newest pair in the batch (unix ms).
    t_min = (t_anchor_ms - pairCreatedAt) // 60000  → so newest pair gets t_min ≈ 0.
    """
    bt = pair.get("baseToken") or {}
    info = pair.get("info") or {}
    liq = pair.get("liquidity") or {}
    pc = pair.get("priceChange") or {}

    created_ms = pair.get("pairCreatedAt") or 0
    t_min = max(0, (t_anchor_ms - created_ms) // 60000) if created_ms else 0

    liq_usd = float(liq.get("usd") or 0)
    liquidity_eth = liq_usd / ETH_USD

    row = {
        "t_min": int(t_min),
        "ticker": bt.get("symbol", "") or "",
        "name": bt.get("name", "") or "",
        "description": info.get("description", "") or "",
        "launchpad": pair.get("dexId", "") or "",
        "liquidity_eth": round(liquidity_eth, 4),
        "liq_growth": round(float(pc.get("h6") or 0) / 100.0, 4),
        "deployer": "",                       # MISSING — RPC
        "holders": "[]",                      # MISSING — RPC
        "linked_groups": "[]",                # MISSING — RPC clustering
        "selling_linked": 0,                  # scene-only ground truth
        "true_multiple_path": json.dumps(_estimate_multiple_path(pair)),
        "theme_hint": "",                     # let NarrativeEngine decide
        # provenance helpers
        "_pair_address": pair.get("pairAddress", ""),
        "_token_address": bt.get("address", ""),
        "_price_usd": pair.get("priceUsd", ""),
        "_fdv": pair.get("fdv", ""),
        "_mcap": pair.get("marketCap", ""),
        "_url": pair.get("url", ""),
        "_dex": pair.get("dexId", ""),
    }
    return row


# ── CSV → TokenLaunch round-trip ──────────────────────────────────────────────
def load_csv_as_tokenlaunches(path: str | Path) -> list:
    """Load CSV → rh_trencher.TokenLaunch objects (ready to feed Desk)."""
    from rh_trencher import TokenLaunch  # lazy import — only needed on round-trip
    path = Path(path)
    rows = list(csv.DictReader(path.open(encoding="utf-8-sig")))
    out = []
    for r in rows:
        tl = TokenLaunch(
            t_min=int(r["t_min"]),
            ticker=r["ticker"],
            name=r["name"],
            description=r["description"],
            launchpad=r["launchpad"],
            liquidity_eth=float(r["liquidity_eth"]),
            liq_growth=float(r["liq_growth"]),
            deployer=r.get("deployer", ""),
            holders=json.loads(r.get("holders", "[]")),
            linked_groups=json.loads(r.get("linked_groups", "[]")),
            selling_linked=int(r.get("selling_linked", 0)),
            true_multiple_path=json.loads(r["true_multiple_path"]),
            theme_hint=r.get("theme_hint", ""),
            _pool_id=r.get("_pair_address", "") or r.get("_token_address", "") or "",
        )
        out.append(tl)
    return out


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="DexScreener → TokenLaunch CSV for rh_trencher backtesting"
    )
    ap.add_argument("--chain", default="solana",
                    help="chainId (solana/ethereum/base/bsc/...)")
    ap.add_argument("--out", default="dexscreener_pairs.csv",
                    help="output CSV path")
    ap.add_argument("--keywords", default=",".join(DEFAULT_KEYWORDS),
                    help="comma-separated search keywords")
    ap.add_argument("--age-min", type=int, default=0,
                    help="ignore pairs whose launch is NEWER than this many minutes "
                         "(gives them time to have liquidity data populate)")
    ap.add_argument("--age-max", type=int, default=2880,
                    help="ignore pairs older than N minutes (default 2880 = 2 days)")
    ap.add_argument("--min-liq-usd", type=float, default=500,
                    help="minimum liquidity.usd (DexScreener sometimes reports 0 for fresh pairs)")
    ap.add_argument("--max-pairs", type=int, default=200,
                    help="max pairs to keep after all filters")
    ap.add_argument("--skip-search", action="store_true",
                    help="skip keyword search — use ONLY profiles feed (fewer pairs, all have descriptions)")
    ap.add_argument("--import-csv", default=None,
                    help="round-trip: load CSV → TokenLaunch → print summary (no network calls)")
    args = ap.parse_args()

    # ── Round-trip mode ──────────────────────────────────────────────────────
    if args.import_csv:
        path = Path(args.import_csv)
        toks = load_csv_as_tokenlaunches(path)
        print(f"Loaded {len(toks)} TokenLaunch objects from {path}")
        if not toks:
            print("  (empty CSV — filters killed all rows)")
            return
        for t in toks[:5]:
            print(f"  t={t.t_min:>4}  {t.ticker:12}  liq={t.liquidity_eth:>8.3f}ETH  "
                  f"lp={str(t.true_multiple_path[:3]):>30}  desc={bool(t.description)}  "
                  f"launchpad={t.launchpad}  theme={t.theme_hint or '(auto)'}")
        print(f"\n  first token description preview: {toks[0].description[:80] if toks[0].description else '(empty)'}")
        return

    # ── Fetch mode ──────────────────────────────────────────────────────────
    rl_dex = RateLimiter(RATE_DEX)
    rl_prof = RateLimiter(RATE_PROFILE)
    keywords = args.keywords.split(",") if not args.skip_search else []

    print(f"[fetch] DexScreener chain={args.chain}  "
          f"min_liq=${args.min_liq_usd}  age=[{args.age_min},{args.age_max}]min  "
          f"max_pairs={args.max_pairs}  keywords={keywords}")

    pairs: list[dict] = []

    if args.skip_search:
        print("  skipping keyword search (profile-only mode)")
        profiles = http_get("/token-profiles/latest/v1", rl_prof) or []
        for p in profiles:
            if p.get("chainId") != args.chain:
                continue
            # profile feed alone has no pair-level data — skip (kept as a documented option)
    else:
        pairs = discover_pairs(args.chain, rl_dex, rl_prof, keywords)

    if not pairs:
        print("\n[WARN] Zero pairs found. Try more keywords or remove --skip-search.")
        return

    # ── Anchor = newest pairCreatedAt (ms) ────────────────────────────────────
    timestamps = [p.get("pairCreatedAt") or 0 for p in pairs]
    t_anchor = max(timestamps) if timestamps else int(time.time() * 1000)
    anchor_human = datetime.fromtimestamp(t_anchor / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC")
    print(f"\n[filter] anchor (newest launch on chain) = {anchor_human}  "
          f"(t_anchor_ms={t_anchor})")

    # ── Apply filters ─────────────────────────────────────────────────────────
    kept: list[dict] = []
    for p in pairs:
        created = p.get("pairCreatedAt") or 0
        age_min = max(0, (t_anchor - created) // 60000)
        if age_min < args.age_min or age_min > args.age_max:
            continue
        liq_usd = float((p.get("liquidity") or {}).get("usd") or 0)
        if liq_usd < args.min_liq_usd:
            continue
        # Final wrap-native guard (redundant with discover_pairs but belt-and-suspenders)
        bt = (p.get("baseToken") or {}).get("symbol", "").upper()
        if bt in {"SOL", "WSOL", "WETH", "ETH", "WBTC", "USDC", "USDT", "BUSD", "DAI"}:
            continue
        kept.append(p)

    kept.sort(key=lambda p: p.get("pairCreatedAt") or 0, reverse=True)
    kept = kept[: args.max_pairs]
    print(f"[filter] kept {len(kept)} pairs after age [{args.age_min},{args.age_max}]min "
          f"+ liq >= ${args.min_liq_usd} + non-native base + cap {args.max_pairs}")

    if not kept:
        print("\n[WARN] All pairs filtered out. Relax --min-liq-usd or --age-max.")
        return

    # ── Map → rows ────────────────────────────────────────────────────────────
    rows = [pair_to_row(p, t_anchor) for p in kept]

    # ── Write CSV ─────────────────────────────────────────────────────────────
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    main_fields = [
        "t_min", "ticker", "name", "description", "launchpad",
        "liquidity_eth", "liq_growth", "deployer", "holders",
        "linked_groups", "selling_linked", "true_multiple_path", "theme_hint",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=main_fields + PROVENANCE_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    desc_count = sum(1 for r in rows if r["description"])
    sol_equiv = sum(r["liquidity_eth"] for r in rows)
    print(f"\n✓ wrote {len(rows)} rows → {out_path.resolve()}")
    print(f"  {desc_count}/{len(rows)} have description  total liq={sol_equiv:.1f} ETH-equivalent")
    print("\nProvenance split per TokenLaunch field:")
    for f in main_fields:
        if f in {"t_min", "ticker", "name", "launchpad", "liquidity_eth"}:
            mark = "REAL"
        elif f in {"true_multiple_path", "liq_growth"}:
            mark = "PROXY"
        elif f == "description":
            mark = "REAL (profile feed)" if desc_count else "EMPTY"
        elif f == "theme_hint":
            mark = "EMPTY → let Desk decide"
        else:
            mark = "MISSING → needs Solana RPC"
        print(f"    {f:22s} [{mark}]")
    print(f"\n  Round-trip verify:  python fetch_dexscreener.py --import-csv {args.out}")


if __name__ == "__main__":
    main()
