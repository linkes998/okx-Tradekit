"""Pipeline: GeckoTerminal trending_pools → Desk CSV (rich OHLCV).

Trending pools 已经有足够交易量和 OHLCV 历史，适合 Desk 做 real-data replay。
"""
import urllib.request, urllib.error, json, csv, time, argparse, sys
from pathlib import Path
from typing import Any

# Reuse from existing modules
sys.path.insert(0, str(Path(__file__).parent))
from enrich_ohlcv import candles_to_path_and_growth  # type: ignore
from fetch_gt_pipeline import infer_theme  # type: ignore

BASE = "https://api.geckoterminal.com/api/v2"
HEADERS = {"User-Agent": "rh_trencher/0.1", "Accept": "application/json;version=20230303"}


class RateLimiter:
    def __init__(self, rpm: int = 28):
        self.min_interval = 60.0 / rpm
        self.last = 0.0

    def wait(self):
        now = time.time()
        dt = now - self.last
        if dt < self.min_interval:
            time.sleep(self.min_interval - dt)
        self.last = time.time()


def get(path: str, limiter: RateLimiter | None = None) -> dict:
    if limiter:
        limiter.wait()
    req = urllib.request.Request(f"{BASE}{path}", headers=HEADERS)
    resp = urllib.request.urlopen(req, timeout=15)
    return json.loads(resp.read())


def fetch_trending(network: str = "solana") -> list[dict]:
    """拉 trending pools —— 20 个有足够交易量 + OHLCV 丰富的池。"""
    data = get(f"/networks/{network}/trending_pools")
    pools = []
    for p in data.get("data", []):
        a = p.get("attributes", {})
        name = a.get("name", "?")
        addr = a.get("address", "")
        liq = float(a.get("reserve_in_usd", 0))
        vol = float(a.get("volume_in_usd", 0))
        # 从 name 拆 symbol
        if " / " in name:
            base_symbol = name.split(" / ", 1)[0].strip()
            quote_symbol = name.split(" / ", 1)[1].strip()
        else:
            base_symbol = name.strip()
            quote_symbol = ""
        pools.append({
            "name": name,
            "pair_address": addr,
            "base_symbol": base_symbol,
            "quote_symbol": quote_symbol,
            "reserve_in_usd": liq,
            "volume_in_usd": vol,
            "pair_created_at": a.get("pair_created_at", ""),
        })
    return pools


def fetch_ohlcv(network: str, pool_addr: str, limiter: RateLimiter,
                timeframe: str = "minute", limit: int = 200,
                max_retries: int = 3) -> list[list[float]]:
    """拉 OHLCV candles, return list of [ts, open, high, low, close, volume]."""
    url = f"/networks/{network}/pools/{pool_addr}/ohlcv/{timeframe}?limit={limit}"
    for attempt in range(max_retries + 1):
        try:
            data = get(url, limiter)
            return data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 5 * (2 ** attempt)  # 5s, 10s, 20s
                if attempt < max_retries:
                    print(f"    [429] rate-limited, wait {wait}s then retry...")
                    time.sleep(wait)
                    continue
            print(f"  [WARN] OHLCV failed for {pool_addr[:10]}...: {e}")
            return []
        except Exception as e:
            print(f"  [WARN] OHLCV failed for {pool_addr[:10]}...: {e}")
            return []
    return []


def build_csv(network: str = "solana", out: str = "gt_trending.csv",
              rpm: int = 28, min_liq: float = 500, t_min_offset: int = 3) -> None:
    """完整 pipeline: trending → OHLCV → CSV for Desk。

    t_min_offset: 每个池的起始 t_min (分钟), 让 Desk 按顺序看到。
    """
    print(f"[gt_trending] network={network}  rpm={rpm}  min_liq=${min_liq:.0f}")

    limiter = RateLimiter(rpm)

    # Step 1: discover
    print("\n--- Step 1: fetch trending pools ---")
    pools = fetch_trending(network)
    print(f"  got {len(pools)} pools")

    # Filter
    pools = [p for p in pools if p["reserve_in_usd"] >= min_liq
             and p["quote_symbol"].upper() not in ("USDC", "USDT", "BUSD", "DAI")]
    print(f"  after filter (liq>=${min_liq:.0f}, no stable quote): {len(pools)}")

    if not pools:
        print("  no pools passed filter — try lowering --min-liq")
        return

    # Step 2: enrich OHLCV
    print(f"\n--- Step 2: enrich OHLCV ({len(pools)} pools) ---")
    t0 = time.time()
    rows = []
    ETH_USD = 2300.0

    for i, pool in enumerate(pools):
        t_min = (i + 1) * t_min_offset  # 3, 6, 9, ... 分钟间隔

        candles = fetch_ohlcv(network, pool["pair_address"], limiter)
        path, growth = candles_to_path_and_growth(candles, marks=20, mark_step_min=2, warmup_min=3)
        theme_hint = infer_theme(pool["base_symbol"], pool["name"])

        if not path:
            path = [1.0] * 5

        liq_eth = pool["reserve_in_usd"] / ETH_USD

        row = {
            "t_min": t_min,
            "ticker": pool["base_symbol"],
            "name": pool["name"],
            "description": "",
            "launchpad": "raydium",
            "liquidity_eth": round(liq_eth, 4),
            "liq_growth": str(growth),
            "deployer": "",
            "holders": "[]",
            "linked_groups": "[]",
            "selling_linked": 0,
            "true_multiple_path": json.dumps(path),
            "theme_hint": theme_hint,
            "_pair_address": pool["pair_address"],
            "_token_address": "",
            "_price_usd": "",
            "_fdv": "",
            "_mcap": "",
            "_url": f"https://www.geckoterminal.com/{network}/pools/{pool['pair_address']}",
            "_dex": "raydium",
            "_gt_candles": str(len(candles)),
            "_source": "geckoterminal_trending",
        }
        rows.append(row)

        elapsed = time.time() - t0
        status = "OK" if len(path) > 0 and max(path) != min(path) else "flat"
        print(f"  [{i+1}/{len(pools):>2}] {pool['base_symbol']:12s} "
              f"liq=${pool['reserve_in_usd']:>10,.0f} growth={growth:.4f} "
              f"candles={len(candles):>3d} path={status} "
              f"elapsed={elapsed:.0f}s")

    # Step 3: write CSV
    fields = list(rows[0].keys())
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  written={len(rows)} → {out}")

    # quick stats
    n_with_var = sum(1 for r in rows if "[1.0, 1.0, 1.0, 1.0, 1.0]" not in r["true_multiple_path"])
    themes = {}
    for r in rows:
        t = r["theme_hint"] or "(none)"
        themes[t] = themes.get(t, 0) + 1
    print(f"  rows with price variety: {n_with_var}/{len(rows)}")
    print(f"  theme distribution:")
    for k, v in sorted(themes.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v}")


def main():
    ap = argparse.ArgumentParser(description="GT trending pools → Desk CSV")
    ap.add_argument("--network", default="solana")
    ap.add_argument("--out", default="gt_trending.csv")
    ap.add_argument("--rpm", type=int, default=28)
    ap.add_argument("--min-liq", type=float, default=500)
    ap.add_argument("--t-min-offset", type=int, default=3,
                    help="Minutes between successive LAUNCH events")
    args = ap.parse_args()

    build_csv(args.network, args.out, args.rpm, args.min_liq, args.t_min_offset)


if __name__ == "__main__":
    main()
