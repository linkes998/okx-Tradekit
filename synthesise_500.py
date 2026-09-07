"""synthesise_500.py — build a 500-pool mixed dataset for Desk replay.

真实 pool (22, from pool_lake) + synthetic pools (~478, realistic distributions) → Desk CSV.

目的：让 Desk 在大样本里跑 self-pause → rebuild，看能不能学到 pet/hood/ai 主题。
"""
import csv, json, time, random, hashlib, argparse, sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from fetch_gt_pipeline import infer_theme  # type: ignore

# ── Synthetic generator ────────────────────────────────────────────────────
# 真实 pool 的 liq_eth 分布: 大部分 <5, 少数 >10
# path 分布: ~35% flat, ~65% variety (min 0.8x, max 1.2x)
# theme_hint 分布: ~15% pet, ~5% ai-craze, ~3% hood, ~3% political, ~74% none

THEME_KEYWORDS = {
    "pet": ["dog", "cat", "kitty", "puppy", "frog", "pepe", "shiba", "wolf",
            "panda", "koala", "pig", "cow", "chihuahua", "wojak", "bonk", "wif", "bark"],
    "ai-craze": ["ai", "gpt", "llm", "agent", "grok", "neural", "bot", "sora", "claude", "mistral"],
    "hood": ["cartel", "mafia", "gang", "drug", "heist", "money", "cash", "kingpin"],
    "political": ["trump", "maga", "biden", "obama", "president", "vote"],
}

# 真实 memecoin ticker pool (从 DexScreener search 收集)
REAL_TICKERS = {
    "pet": ["DOGE", "SHIB", "FLOKI", "WIF", "BONK", "PEPE", "FROG", "WOJAK",
            "BARK", "KIBBLE", "TURBO", "GIGA", "PIPEINU", "ROCO", "SMOOTH",
            "CATGIRL", "KITTY", "PUPPY", "BULLY", "DOGE-1", "BITCAT", "SOLCAT"],
    "ai-craze": ["OPENAI", "GPT", "CLAUDE", "GROK", "LLM", "AGENT", "NEURAL",
                 "BOT", "SORA", "MISTRAL", "JARVIS", "OMEGA", "QUANTUM"],
    "hood": ["CARTEL", "GANG", "MAFIA", "HEIST", "DRUG", "CASH", "KINGPIN",
             "BOSSMAN", "HUSTLE", "GRIND", "CHEF"],
    "political": ["TRUMP", "BIDEN", "MAGA", "VOTE", "PRESIDENT", "AMERICA",
                  "USA", "ELECTION", "TRUMP1", "TRUMP2"],
    "none": [f"TOKEN{i:03d}" for i in range(200)],
}


def gen_liq_eth(rng: np.random.Generator) -> float:
    """Realistic liquidity distribution."""
    # 80% < 5 ETH, 15% 5-20, 5% > 20
    r = rng.random()
    if r < 0.8:
        return float(rng.uniform(0.3, 5.0))
    elif r < 0.95:
        return float(rng.uniform(5.0, 20.0))
    else:
        return float(rng.uniform(20.0, 100.0))


def gen_path(rng: np.random.Generator, n_marks: int = 20) -> list[float]:
    """Realistic price path with variety.

    ~40% flat-ish (max-min < 0.05), ~60% real variety with random walk.
    """
    if rng.random() < 0.4:
        # flat-ish
        noise = rng.normal(0, 0.005, n_marks)
        return [round(max(0.95, min(1.05, 1.0 + x)), 4) for x in noise]

    # Random walk with drift + occasional jump
    drift = rng.normal(0, 0.02)  # per-step drift
    vol = abs(rng.normal(0.01, 0.02))  # per-step volatility

    prices = [1.0]
    for i in range(1, n_marks):
        jump = 0
        if rng.random() < 0.05:  # 5% chance of a pump/rug
            jump = rng.choice([-0.15, -0.1, 0.05, 0.1, 0.15, 0.2, 0.3])
        ret = rng.normal(drift, vol) + jump
        prices.append(prices[-1] * (1 + ret))

    # Normalise so path[0] = 1.0
    baseline = prices[0]
    return [round(max(0.5, p / baseline), 4) for p in prices]


def gen_pool(rng: np.random.Generator, idx: int, theme_dist: dict[str, float]) -> dict:
    """Generate one synthetic pool row."""
    # Pick theme
    r = rng.random()
    cum = 0.0
    theme = "none"
    for t, p in theme_dist.items():
        cum += p
        if r < cum:
            theme = t
            break

    tickers = REAL_TICKERS.get(theme, REAL_TICKERS["none"])
    ticker = str(rng.choice(tickers))
    name = ticker  # No "/ SOL" suffix — that leaks platform noise into theme learning
    liq_eth = gen_liq_eth(rng)
    path = gen_path(rng, 20)
    growth = round(path[-1], 4)

    # Deterministic fake address from ticker
    addr_hash = hashlib.sha256(f"{ticker}-{idx}".encode()).hexdigest()[:32]
    pair_addr = addr_hash + addr_hash[:8]
    mint_addr = addr_hash[:16] + addr_hash[16:32]

    return {
        "t_min": idx * 3 + 3,
        "ticker": ticker,
        "name": name,
        "description": "",
        "launchpad": "raydium",
        "liquidity_eth": round(liq_eth, 4),
        "liq_growth": str(growth),
        "deployer": "",
        "holders": "[]",
        "linked_groups": "[]",
        "selling_linked": 0,
        "true_multiple_path": json.dumps(path),
        "theme_hint": "" if theme == "none" else theme,
        "_pair_address": pair_addr,
        "_token_address": mint_addr,
        "_dex": "raydium",
        "_source": "synthetic",
        "_gt_candles": "200",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool-lake", default=None,
                    help="Path to pool_lake_22.csv (real pools). If omitted, all synthetic.")
    ap.add_argument("--n", type=int, default=500, help="Total pools in output")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="desk_500.csv")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)

    rows = []

    # Load real pools if available
    real_count = 0
    if args.pool_lake and Path(args.pool_lake).exists():
        with open(args.pool_lake, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append(dict(r))
                real_count += 1
        print(f"Loaded {real_count} real pools from {args.pool_lake}")

    # Theme distribution (target)
    theme_dist = {
        "pet": 0.18,
        "ai-craze": 0.06,
        "hood": 0.04,
        "political": 0.04,
        "none": 0.68,
    }

    synthetic_target = args.n - real_count
    print(f"Generating {synthetic_target} synthetic pools (total target: {args.n})...")

    # Sort real pools by t_min (they already have reasonable spacing)
    if rows:
        rows.sort(key=lambda r: int(r["t_min"]))
        last_t = int(rows[-1]["t_min"])
    else:
        last_t = 0

    for i in range(synthetic_target):
        row = gen_pool(rng, i, theme_dist)
        row["t_min"] = last_t + (i + 1) * 3
        rows.append(row)

    # Write
    if not rows:
        print("No rows!")
        return

    # Union of all fieldnames across rows
    field_set = set()
    for r in rows:
        field_set.update(r.keys())
    fields = sorted(field_set)

    # Make sure every row has all fields (fill missing with "")
    for r in rows:
        for f_ in fields:
            if f_ not in r:
                r[f_] = ""

    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    # Stats
    n_var = sum(1 for r in rows if max(json.loads(r["true_multiple_path"])) != min(json.loads(r["true_multiple_path"])))
    themes = {}
    for r in rows:
        t = r.get("theme_hint", "") or "(none)"
        themes[t] = themes.get(t, 0) + 1

    print(f"\nDone → {args.out}")
    print(f"  total: {len(rows)} ({real_count} real + {synthetic_target} synthetic)")
    print(f"  with price variety: {n_var}/{len(rows)} ({100*n_var//len(rows)}%)")
    print(f"  theme distribution:")
    for k, v in sorted(themes.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v}")


if __name__ == "__main__":
    main()
