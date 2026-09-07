"""pool_lake.py — persistent pool accumulation system.

每天 08:00 跑一次 GT new_pools (4 pages = ~80 pools/day)，存 SQLite。
N 天后回来补 OHLCV → 自动变成 Desk CSV。

Design:
  pool_lake.db (SQLite)
  ├── raw_pools      : pool addresses + 元数据 (pair, mint, sym, dex, discovered_at)
  ├── ohlcv_cache    : OHLCV candles by (pool_address, timeframe)
  └── enrich_queue   : 待 enrich 的 pool (status: discovered → ohlcv_done → csv_ready)

Usage:
  python pool_lake.py ingest             # 每天跑: discover + insert new
  python pool_lake.py fill --age-days 3  # 补 OHLCV: 池龄 >=3 天的池有足够蜡烛
  python pool_lake.py export --out       # 导出 Desk CSV (only ohlcv_done)
  python pool_lake.py status             # 看积累情况
"""
import sqlite3, json, time, csv, argparse, sys, hashlib, random
from pathlib import Path
from typing import Any
from datetime import datetime, timedelta

import urllib.request
import urllib.error

# Reuse existing modules
sys.path.insert(0, str(Path(__file__).parent))
from fetch_gt_trending import fetch_ohlcv  # type: ignore
from fetch_gt_pipeline import infer_theme, RateLimiter  # type: ignore
from enrich_ohlcv import candles_to_path_and_growth  # type: ignore

BASE = "https://api.geckoterminal.com/api/v2"
HEADERS = {"User-Agent": "rh_trencher/0.1", "Accept": "application/json;version=20230303"}
DB_PATH = Path(__file__).parent / "pool_lake.db"
ETH_USD = 2300.0


# ── DB setup ───────────────────────────────────────────────────────────────
def ensure_schema(db: sqlite3.Connection):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS raw_pools (
            pair_address TEXT PRIMARY KEY,
            mint_address TEXT,
            base_symbol TEXT,
            base_name TEXT,
            quote_symbol TEXT,
            dex_id TEXT,
            reserve_usd REAL,
            volume_24h REAL,
            discovered_at INTEGER,
            first_seen_at TEXT,
            source TEXT
        );
        CREATE TABLE IF NOT EXISTS ohlcv_cache (
            pair_address TEXT,
            timeframe TEXT,
            fetched_at INTEGER,
            candles_json TEXT,
            PRIMARY KEY (pair_address, timeframe)
        );
        CREATE TABLE IF NOT EXISTS pool_rows (
            pair_address TEXT PRIMARY KEY,
            t_min INTEGER,
            ticker TEXT,
            name TEXT,
            launchpad TEXT,
            liquidity_eth REAL,
            liq_growth REAL,
            true_multiple_path TEXT,
            theme_hint TEXT,
            ohlcv_fetched_at INTEGER,
            n_candles INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_raw_discovered ON raw_pools(discovered_at);
    """)
    db.commit()


def get_db() -> sqlite3.Connection:
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    ensure_schema(db)
    return db


# ── GT discovery ───────────────────────────────────────────────────────────
def gt_get(path: str, limiter: RateLimiter | None = None) -> dict:
    if limiter:
        limiter.wait()
    req = urllib.request.Request(f"{BASE}{path}", headers=HEADERS)
    resp = urllib.request.urlopen(req, timeout=15)
    return json.loads(resp.read())


def discover_new_pools(network: str, pages: int, limiter: RateLimiter) -> list[dict]:
    """Fetch GT new_pools across N pages."""
    pools = []
    for page in range(1, pages + 1):
        try:
            data = gt_get(f"/networks/{network}/new_pools?page={page}", limiter)
            page_pools = data.get("data", [])
            if not page_pools:
                break
            pools.extend(page_pools)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print(f"  [WARN] page {page}: 429 rate-limited, stopping")
                break
            raise
    return pools


def discover_trending_pools(network: str, limiter: RateLimiter) -> list[dict]:
    data = gt_get(f"/networks/{network}/trending_pools", limiter)
    return data.get("data", [])


def pool_to_dict(p: dict, source: str) -> dict:
    """Map GT pool JSON → normalised dict for DB insert."""
    a = p.get("attributes", {})
    rels = p.get("relationships", {})
    name = a.get("name", "?")
    pair_address = a.get("address", "")

    # Symbol from name
    if " / " in name:
        base_symbol = name.split(" / ", 1)[0].strip()
        quote_symbol = name.split(" / ", 1)[1].strip()
    else:
        base_symbol = name.strip()
        quote_symbol = ""

    # Mint from relationships
    bt_id = rels.get("base_token", {}).get("data", {}).get("id", "")
    mint_address = ""
    if bt_id.startswith("solana_"):
        mint_address = bt_id[len("solana_"):]

    return {
        "pair_address": pair_address,
        "mint_address": mint_address,
        "base_symbol": base_symbol,
        "base_name": name,
        "quote_symbol": quote_symbol,
        "dex_id": a.get("dex_id", ""),
        "reserve_usd": float(a.get("reserve_in_usd", 0)),
        "volume_24h": float(a.get("volume_in_usd", 0)),
        "discovered_at": int(time.time()),
        "first_seen_at": a.get("pair_created_at", ""),
        "source": source,
    }


def dex_pair_to_dict(p: dict, source: str) -> dict:
    """Map a DexScreener pair dict → normalised dict for raw_pools insert."""
    bt = p.get("baseToken") or {}
    qt = p.get("quoteToken") or {}
    liq = p.get("liquidity") or {}
    vol = p.get("volume") or {}

    name = bt.get("name", bt.get("symbol", "?"))
    if qt.get("symbol"):
        name = f"{name} / {qt['symbol']}"

    return {
        "pair_address": p.get("pairAddress", ""),
        "mint_address": bt.get("address", ""),
        "base_symbol": bt.get("symbol", ""),
        "base_name": name,
        "quote_symbol": qt.get("symbol", ""),
        "dex_id": p.get("dexId", ""),
        "reserve_usd": float(liq.get("usd", 0) or 0),
        "volume_24h": float(vol.get("h24", 0) or 0),
        "discovered_at": int(time.time()),
        "first_seen_at": p.get("pairCreatedAt") or "",
        "source": source,
    }


# ── Commands ───────────────────────────────────────────────────────────────
def cmd_ingest(args):
    """Discover new pools and insert into DB."""
    limiter = RateLimiter(args.rpm)
    db = get_db()
    t0 = time.time()

    existing = set(r["pair_address"] for r in db.execute("SELECT pair_address FROM raw_pools").fetchall())
    print(f"[ingest] existing pools in DB: {len(existing)}")

    new_discovered = 0
    sources = []

    # 1. new_pools (多页)
    print(f"\n--- new_pools ({args.pages} pages) ---")
    new_pools_json = discover_new_pools(args.network, args.pages, limiter)
    new_pools_json = [p for p in new_pools_json
                      if float(p.get("attributes", {}).get("reserve_in_usd") or 0) >= args.min_liq]
    for p in new_pools_json:
        d = pool_to_dict(p, "gt_new_pools")
        if d["pair_address"] and d["pair_address"] not in existing:
            db.execute("""INSERT INTO raw_pools VALUES (:pair_address,:mint_address,:base_symbol,
                :base_name,:quote_symbol,:dex_id,:reserve_usd,:volume_24h,:discovered_at,
                :first_seen_at,:source)""", d)
            existing.add(d["pair_address"])
            new_discovered += 1
    print(f"  new pools added: {new_discovered}  (total fetched {len(new_pools_json)})")

    # 2. trending_pools
    if args.include_trending:
        print(f"\n--- trending_pools ---")
        trend_json = discover_trending_pools(args.network, limiter)
        trend_json = [p for p in trend_json
                      if float(p.get("attributes", {}).get("reserve_in_usd", 0)) >= args.min_liq]
        added = 0
        for p in trend_json:
            d = pool_to_dict(p, "gt_trending")
            if d["pair_address"] and d["pair_address"] not in existing:
                db.execute("""INSERT INTO raw_pools VALUES (:pair_address,:mint_address,:base_symbol,
                    :base_name,:quote_symbol,:dex_id,:reserve_usd,:volume_24h,:discovered_at,
                    :first_seen_at,:source)""", d)
                existing.add(d["pair_address"])
                added += 1
        print(f"  trending added: {added}")
        new_discovered += added

    db.commit()
    print(f"\n[ingest] done in {time.time()-t0:.1f}s  total pools: {len(existing)}  new: {new_discovered}")


def cmd_fill(args):
    """Fetch OHLCV for pools that are old enough, write into pool_rows."""
    limiter = RateLimiter(args.rpm)
    db = get_db()
    t0 = time.time()

    age_sec = args.age_days * 86400
    now = time.time()
    # Select pools whose OHLCV hasn't been fetched, and which are "old enough"
    rows = db.execute("""
        SELECT rp.* FROM raw_pools rp
        LEFT JOIN pool_rows pr ON rp.pair_address = pr.pair_address
        WHERE pr.pair_address IS NULL
          AND rp.discovered_at <= ?
          AND rp.reserve_usd >= ?
    """, (now - age_sec, args.min_liq)).fetchall()

    # Also include pools that already have partial cache but no final pool_rows
    old_cache = db.execute("""
        SELECT rp.*, oc.candles_json FROM raw_pools rp
        JOIN ohlcv_cache oc ON rp.pair_address = oc.pair_address
        LEFT JOIN pool_rows pr ON rp.pair_address = pr.pair_address
        WHERE pr.pair_address IS NULL AND oc.timeframe = 'minute'
    """).fetchall()

    # Dedupe by pair_address
    seen = set()
    targets = []
    for r in rows:
        if r["pair_address"] not in seen:
            targets.append(dict(r))
            seen.add(r["pair_address"])
    for r in old_cache:
        if r["pair_address"] not in seen:
            targets.append(dict(r))
            seen.add(r["pair_address"])

    print(f"[fill] targets: {len(targets)} (age>={args.age_days}d, liq>=${args.min_liq:.0f})")

    done = 0
    for i, tgt in enumerate(targets[: args.max]):
        pair = tgt["pair_address"]
        sym = tgt["base_symbol"]
        mint = tgt["mint_address"] or ""
        t_min = (done + 1) * 3

        # Fetch OHLCV (with retry + 429 handling)
        candles = fetch_ohlcv(args.network, pair, limiter)

        # Cache
        db.execute("""INSERT OR REPLACE INTO ohlcv_cache VALUES (?,?,?,?)""",
                   (pair, "minute", int(time.time()), json.dumps(candles)))

        # Derive path
        path, growth = candles_to_path_and_growth(
            candles, marks=20, mark_step_min=2, warmup_min=3)
        if not path:
            path = [1.0] * 5

        liq_eth = tgt["reserve_usd"] / ETH_USD
        theme = infer_theme(tgt["base_symbol"], tgt["base_name"])

        db.execute("""INSERT OR REPLACE INTO pool_rows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (pair, t_min, sym, tgt["base_name"],
                     tgt["dex_id"], round(liq_eth, 4), round(growth, 6),
                     json.dumps(path), theme,
                     int(time.time()), len(candles),
                     "[]", "[]", 0, ""))

        db.commit()
        done += 1
        elapsed = time.time() - t0
        n_var = "var" if max(path) != min(path) else "flat"
        print(f"  [{done}/{min(len(targets), args.max):>3}] {sym:15s} "
              f"candles={len(candles):>3d} path={n_var} "
              f"elapsed={elapsed:.0f}s")

    print(f"\n[fill] done in {time.time()-t0:.1f}s  filled={done}")


def cmd_export(args):
    """Export pool_rows → Desk CSV."""
    db = get_db()
    t_min_base = args.t_min_offset

    rows = db.execute("""
        SELECT pr.*, rp.mint_address, rp.reserve_usd, rp.source
        FROM pool_rows pr JOIN raw_pools rp ON pr.pair_address = rp.pair_address
        ORDER BY rp.reserve_usd DESC
    """).fetchall()

    if not rows:
        print("[export] no pool_rows — run 'fill' first")
        return

    out_path = args.out or "pool_lake_export.csv"
    fields = ["t_min", "ticker", "name", "description", "launchpad",
              "liquidity_eth", "liq_growth", "deployer", "holders",
              "linked_groups", "selling_linked", "true_multiple_path", "theme_hint",
              "_pair_address", "_token_address", "_dex", "_source"]

    min_swing = getattr(args, "min_swing", 0.0)
    with_theme = getattr(args, "with_theme", False)

    filtered, skipped_flat, skipped_theme = [], 0, 0
    for r in rows:
        # Filter flat pools
        if min_swing > 0:
            try:
                p = json.loads(r["true_multiple_path"])
                if p and (max(p) - min(p)) < min_swing:
                    skipped_flat += 1
                    continue
            except:
                skipped_flat += 1
                continue
        # Filter by theme_hint
        if with_theme and not r["theme_hint"]:
            skipped_theme += 1
            continue
        filtered.append(r)

    max_peak = getattr(args, "max_peak", 50.0)
    if max_peak and max_peak > 0:
        print(f"  capping peaks at {max_peak:.0f}x")

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for i, r in enumerate(filtered):
            t_min = t_min_base + i * args.t_min_step
            raw_path = r["true_multiple_path"]
            if max_peak and max_peak > 0:
                try:
                    p = json.loads(raw_path)
                    if p and max(p) > max_peak:
                        scale = max_peak / max(p)
                        p = [max(1.0, x * scale) for x in p]
                        raw_path = json.dumps(p)
                except:
                    pass
            # 从 DB 拿已 enrich 的钱包数据
            holders = r["holders"] or "[]"
            linked = r["linked_groups"] or "[]"
            selling = r["selling_linked"] or 0
            deployer = r["deployer"] or ""
            w.writerow({
                "t_min": t_min,
                "ticker": r["ticker"],
                "name": r["name"],
                "description": "",
                "launchpad": r["launchpad"],
                "liquidity_eth": r["liquidity_eth"],
                "liq_growth": str(r["liq_growth"]),
                "deployer": deployer,
                "holders": holders,
                "linked_groups": linked,
                "selling_linked": selling,
                "true_multiple_path": raw_path,
                "theme_hint": r["theme_hint"],
                "_pair_address": r["pair_address"],
                "_token_address": r["mint_address"],
                "_dex": r["launchpad"],
                "_source": r["source"],
            })

    n_with_mint = sum(1 for r in filtered if r["mint_address"])
    n_with_var = sum(1 for r in filtered
                     if max(json.loads(r["true_multiple_path"])) != min(json.loads(r["true_multiple_path"])))
    print(f"[export] {len(rows)} raw → {len(filtered)} filtered → {out_path}")
    if min_swing:
        print(f"  skipped flat (swing<{min_swing:.0%}): {skipped_flat}")
    if with_theme:
        print(f"  skipped no-theme: {skipped_theme}")
    print(f"  with mint_address: {n_with_mint}/{len(filtered)}")
    print(f"  with price variety: {n_with_var}/{len(filtered)}")


def _make_runner_path(peak: float, rise_ticks: int | None = None, total: int = 20) -> list[float]:
    """Generate realistic runner multiple path (20 OHLCV marks).

    Shape: entry flat → exponential rise to peak → hold/gradual decline
    """
    if rise_ticks is None:
        rise_ticks = random.randint(4, 8)
    path: list[float] = []
    entry_flat = max(1, rise_ticks // 3)
    for _ in range(entry_flat):
        path.append(1.0 + random.uniform(0, 0.1))
    remaining_rise = rise_ticks - entry_flat
    for j in range(remaining_rise):
        frac = (j + 1) / remaining_rise
        path.append(1.0 * (peak ** frac) * random.uniform(0.95, 1.05))
    path.append(peak * random.uniform(0.97, 1.0))
    post = total - len(path)
    for j in range(post):
        frac = (j + 1) / max(1, post)
        mult = peak * (0.80 + 0.20 * (1 - frac)) * random.uniform(0.95, 1.05)
        path.append(max(1.0, mult))
    while len(path) < total:
        path.append(1.0)
    return [round(p, 3) for p in path[:total]]


def _make_rug_path(peak: float, total: int = 20) -> list[float]:
    """Generate rug pull path: rise to peak → sudden dump to near-zero.

    The rug happens 1-2 ticks after peak — exit must catch it before total loss.
    """
    rise_ticks = random.randint(4, 8)
    path: list[float] = []
    entry_flat = max(1, rise_ticks // 3)
    for _ in range(entry_flat):
        path.append(1.0 + random.uniform(0, 0.1))
    remaining_rise = rise_ticks - entry_flat
    for j in range(remaining_rise):
        frac = (j + 1) / remaining_rise
        path.append(1.0 * (peak ** frac) * random.uniform(0.95, 1.05))
    path.append(peak * random.uniform(0.97, 1.0))
    # Peak held for only 1-2 ticks before rug
    hold = random.randint(1, 2)
    for _ in range(hold):
        path.append(peak * random.uniform(0.90, 1.0))
    # Sudden dump to 5-20% of peak
    dump_tick = peak * random.uniform(0.05, 0.20)
    path.append(max(0.5, dump_tick))
    # Remaining ticks stay near bottom
    post = total - len(path)
    for _ in range(post):
        path.append(max(0.3, peak * random.uniform(0.01, 0.08)))
    return [round(p, 3) for p in path[:total]]


def cmd_synthesize(args):
    """Inject synthetic runner pools into an exported CSV, for scalability testing."""
    src = Path(args.input) if args.input else None
    out_path = args.out or "pool_lake_synthesized.csv"

    fields = ["t_min", "ticker", "name", "description", "launchpad",
              "liquidity_eth", "liq_growth", "deployer", "holders",
              "linked_groups", "selling_linked", "true_multiple_path", "theme_hint",
              "_pair_address", "_token_address", "_dex", "_source"]

    random.seed(args.seed)
    synthetic: list[dict] = []

    # Pre-defined runner profile templates — cover varied peaks & themes
    themes = ["pet", "ai-craze", "political", ""]
    n = args.count
    peak_range = args.peak_range or "5-50"
    lo_peak, hi_peak = [float(x) for x in peak_range.split("-")]
    lg_range = args.lg_range or "3-300"
    lo_lg, hi_lg = [float(x) for x in lg_range.split("-")]
    t_start = args.t_min_start
    t_gap = args.t_min_gap

    for i in range(n):
        # Peak distribution: uniform (flat) or bimodal (few extreme, many moderate — realistic)
        dist = getattr(args, "peak_dist", "uniform")
        if dist == "bimodal":
            rr = random.random()
            if rr < 0.10:
                # 10% extreme
                peak = round(random.uniform(max(lo_peak, hi_peak * 0.7), hi_peak), 1)
            elif rr < 0.45:
                # 35% high-mid
                peak = round(random.uniform(lo_peak + (hi_peak - lo_peak) * 0.5, hi_peak * 0.75), 1)
            else:
                # 55% moderate
                peak = round(random.uniform(lo_peak, lo_peak + (hi_peak - lo_peak) * 0.5), 1)
        else:
            peak = round(random.uniform(lo_peak, hi_peak), 1)
        lg = round(random.uniform(lo_lg, hi_lg), 1)
        theme = random.choice(themes)
        t_min = t_start + i * t_gap + random.randint(-5, 5)
        # Synth runner risk distribution: 25% safe, 25% whale, 25% extreme, 25% rug
        # Rug runners (selling_linked=3) use _make_rug_path with sudden dump after peak
        sell_linked = random.choice([0, 0, 1, 1, 2, 2, 3, 3])
        ticker = f"SYNRUN_{t_min}_{int(peak)}"
        if sell_linked == 3:
            path = _make_rug_path(peak)
        else:
            path = _make_runner_path(peak)
        synthetic.append({
            "t_min": t_min,
            "ticker": ticker,
            "name": f"Synthetic Runner {ticker}",
            "description": "",
            "launchpad": "pump.fun",
            "liquidity_eth": round(random.uniform(0.05, 0.3), 4),  # pump.fun new token: $100-$600 base liquidity
            "liq_growth": str(lg),
            "deployer": "SYNTHETIC_DEPLOYER",
            "holders": json.dumps([f"synth_{j}_{random.randint(1000,9999)}" for j in range(8)]),
            "linked_groups": "[]",
            "selling_linked": sell_linked,
            "true_multiple_path": str(path),
            "theme_hint": theme,
            "_pair_address": f"synth_pair_{random.randint(100000,999999)}",
            "_token_address": f"synth_tok_{random.randint(100000,999999)}",
            "_dex": "synthetic",
            "_source": "synthetic_injected",
        })

    if src and src.exists():
        rows = list(csv.DictReader(open(src, encoding="utf-8")))
        # Ensure same fieldnames
        actual_fields = list(rows[0].keys()) if rows else fields
        # Merge + sort by t_min
        all_rows = rows + synthetic
        all_rows.sort(key=lambda r: int(r["t_min"]))
    else:
        all_rows = synthetic

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=actual_fields if src and src.exists() else fields,
                           extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)

    print(f"[synthesize] {len(synthetic)} synthetic runners injected "
          f"({len(all_rows)} total rows → {out_path})")
    print(f"  peak range: {lo_peak:.0f}-{hi_peak:.0f}x, liq_growth: {lo_lg:.0f}-{hi_lg:.0f}x")
    print(f"  t_min range: {t_start} - {t_start + n * t_gap}")
    print(f"  seed={args.seed}")
    # Sample peaks from generated data
    sample_peaks = []
    for r in synthetic[:3]:
        try:
            p = eval(r["true_multiple_path"])
            sample_peaks.append(max(p))
        except Exception:
            pass
    if sample_peaks:
        print(f"  sample peaks: {[round(p, 1) for p in sample_peaks]}")


def cmd_status(args):
    db = get_db()
    n_raw = db.execute("SELECT COUNT(*) FROM raw_pools").fetchone()[0]
    n_rows = db.execute("SELECT COUNT(*) FROM pool_rows").fetchone()[0]
    n_cache = db.execute("SELECT COUNT(*) FROM ohlcv_cache").fetchone()[0]

    # By source
    by_source = db.execute("""SELECT source, COUNT(*) FROM raw_pools GROUP BY source""").fetchall()
    by_theme = db.execute("""SELECT theme_hint, COUNT(*) FROM pool_rows GROUP BY theme_hint""").fetchall()
    with_var = db.execute("""SELECT COUNT(*) FROM pool_rows""").fetchone()[0]
    # Approximate: count rows with path containing variety
    by_age = db.execute("""
        SELECT CASE
            WHEN discovered_at > strftime('%s', 'now') - 86400 THEN '<1d'
            WHEN discovered_at > strftime('%s', 'now') - 604800 THEN '1-7d'
            ELSE '>7d'
        END as bucket, COUNT(*) FROM raw_pools GROUP BY bucket
    """).fetchall()

    print(f"[status] pool_lake.db")
    print(f"  raw_pools:  {n_raw}")
    print(f"  pool_rows:  {n_rows}  (ready for Desk)")
    print(f"  ohlcv_cache:{n_cache}")
    print(f"\n  by source:")
    for src, cnt in by_source:
        print(f"    {src}: {cnt}")
    print(f"\n  by theme_hint (in pool_rows):")
    for th, cnt in by_theme:
        label = th if th else "(none)"
        print(f"    {label}: {cnt}")
    print(f"\n  by age:")
    for bucket, cnt in by_age:
        print(f"    {bucket}: {cnt}")


def cmd_ingest_dex(args):
    """Pull DexScreener trending/top pools and insert into DB."""
    # Lazy import to avoid heavy dependency when GT is the primary source
    from fetch_dex_top import (
        fetch_trending_profiles, fetch_search_keywords, MEME_KEYWORDS, is_probably_rug
    )

    db = get_db()
    t0 = time.time()
    existing = set(r["pair_address"] for r in db.execute("SELECT pair_address FROM raw_pools").fetchall())
    print(f"[ingest-dex] existing: {len(existing)}")

    # Strategy 1: trending token profiles (resolve each to its pair)
    print(f"\n--- DexScreener trending profiles (chain={args.chain}) ---")
    profiles = fetch_trending_profiles(args.chain, max(args.limit // 2, 5))
    print(f"  resolved {len(profiles)} profiles → pairs")

    # Strategy 2: keyword search
    print(f"\n--- keyword search ({args.chain}) ---")
    kw_pairs = fetch_search_keywords(args.chain, MEME_KEYWORDS, args.limit)
    print(f"  {len(kw_pairs)} keyword pairs")

    # Dedup + filter rugs + insert
    seen = set()
    new_added = 0
    sources = [("dex_trending", profiles), ("dex_keyword", kw_pairs)]
    for source_label, pairs in sources:
        for p in pairs:
            addr = p.get("pairAddress")
            if not addr or addr in seen:
                continue
            seen.add(addr)
            if is_probably_rug(p):
                continue
            d = dex_pair_to_dict(p, source_label)
            if d["pair_address"] in existing:
                continue
            db.execute("""INSERT INTO raw_pools VALUES
                (:pair_address,:mint_address,:base_symbol,:base_name,
                 :quote_symbol,:dex_id,:reserve_usd,:volume_24h,
                 :discovered_at,:first_seen_at,:source)""", d)
            existing.add(d["pair_address"])
            new_added += 1

    db.commit()
    print(f"\n[ingest-dex] done in {time.time()-t0:.1f}s  new pools: {new_added}  total: {len(existing)}")


# ── CLI ────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Pool Lake — persistent pool accumulation for rh_trencher")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # ingest
    pi = sub.add_parser("ingest", help="Discover new pools and insert into DB")
    pi.add_argument("--network", default="solana")
    pi.add_argument("--pages", type=int, default=4, help="GT new_pools pages (4 pages = ~80 pools)")
    pi.add_argument("--include-trending", action="store_true", default=True)
    pi.add_argument("--min-liq", type=float, default=300)
    pi.add_argument("--rpm", type=int, default=12)

    # fill
    pf = sub.add_parser("fill", help="Fetch OHLCV and build pool_rows")
    pf.add_argument("--network", default="solana")
    pf.add_argument("--age-days", type=int, default=3, help="Only fetch pools discovered >= N days ago")
    pf.add_argument("--min-liq", type=float, default=300)
    pf.add_argument("--rpm", type=int, default=12)
    pf.add_argument("--max", type=int, default=500, help="Max pools to fill in one run")

    # export
    pe = sub.add_parser("export", help="Export pool_rows → Desk CSV")
    pe.add_argument("--out", default=None)
    pe.add_argument("--t-min-offset", type=int, default=3)
    pe.add_argument("--t-min-step", type=int, default=3)
    pe.add_argument("--min-swing", type=float, default=0.0, help="Min peak-to-trough swing (e.g. 0.05 = 5%)")
    pe.add_argument("--with-theme", action="store_true", help="Only keep pools with non-empty theme_hint")
    pe.add_argument("--max-peak", type=float, default=50.0, help="Cap true_multiple_path peak (default 50x, 0=no cap)")

    # status
    sub.add_parser("status", help="Show pool accumulation status")

    # ingest-dex
    pd = sub.add_parser("ingest-dex", help="Pull DexScreener trending/top pools")
    pd.add_argument("--chain", default="solana")
    pd.add_argument("--limit", type=int, default=20)

    # synthesize
    ps = sub.add_parser("synthesize", help="Inject synthetic runner pools for scalability testing")
    ps.add_argument("--input", default=None, help="Existing CSV to append to (default: create new)")
    ps.add_argument("--out", default=None, help="Output CSV path")
    ps.add_argument("--count", type=int, default=15, help="Number of synthetic runners to generate")
    ps.add_argument("--peak-range", default="5-50", help="Peak multiple range (e.g. '5-50')")
    ps.add_argument("--lg-range", default="3-300", help="liq_growth range (e.g. '3-300')")
    ps.add_argument("--t-min-start", type=int, default=400, help="First synthetic runner t_min")
    ps.add_argument("--t-min-gap", type=int, default=15, help="Gap between consecutive runners")
    ps.add_argument("--peak-dist", default="uniform", choices=["uniform", "bimodal"],
                    help="Peak distribution: uniform (flat) or bimodal (10%% extreme, 35%% high, 55%% moderate — realistic)")
    ps.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")

    args = ap.parse_args()

    if args.cmd == "ingest":
        cmd_ingest(args)
    elif args.cmd == "fill":
        cmd_fill(args)
    elif args.cmd == "export":
        cmd_export(args)
    elif args.cmd == "status":
        cmd_status(args)
    elif args.cmd == "ingest-dex":
        cmd_ingest_dex(args)
    elif args.cmd == "synthesize":
        cmd_synthesize(args)


if __name__ == "__main__":
    main()
