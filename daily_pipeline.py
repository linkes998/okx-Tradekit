#!/usr/bin/env python3
"""
Daily data pipeline: fetch → enrich → backtest → report.

Usage:
    python daily_pipeline.py [--skip-dex] [--skip-gt] [--skip-holders] [--skip-backtest]

Pipeline:
    1. fetch_dex_top.py trending → pool_lake ingest-dex
    2. fetch_gt_trending.py → pool_lake fill
    3. enrich_holders.py (QuickNode)
    4. pool_lake.py export → CSV
    5. rh_trencher.py --from-csv → 30-seed backtest
    6. Daily report (JSON + summary)
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).parent.resolve()
DB = BASE / "pool_lake.db"
REPORT_DIR = BASE / "reports"


def run(cmd: list[str], desc: str) -> int:
    """Run a subprocess, return exit code. Logs output."""
    print(f"\n{'='*60}")
    print(f"[{desc}] {' '.join(cmd)}")
    print("=" * 60)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=str(BASE), timeout=300
        )
        if result.stdout:
            print(result.stdout[-800:])
        if result.stderr:
            print(result.stderr[-400:], file=sys.stderr)
        if result.returncode != 0:
            print(f"[WARN] {desc} exited with code {result.returncode}")
        return result.returncode
    except subprocess.TimeoutExpired:
        print(f"[WARN] {desc} timed out after 300s")
        return -1


def step_dex() -> bool:
    """Fetch DexScreener trending pools."""
    print("\n--- Step 1: DexScreener ingest ---")
    r1 = run(
        ["python", "fetch_dex_top.py", "trending", "--chain", "solana",
         "--limit", "30", "--out", "_tmp_trending.csv"],
        "DexScreener trending",
    )
    if r1 != 0:
        return False
    r2 = run(
        ["python", "pool_lake.py", "ingest-dex", "--chain", "solana", "--limit", "60"],
        "pool_lake ingest-dex",
    )
    return r2 == 0


def step_gt() -> bool:
    """Fetch GeckoTerminal trending pools + OHLCV fill."""
    print("\n--- Step 2: GeckoTerminal fill ---")
    # Just fill existing pools that need OHLCV (skip new discovery to avoid rate limits)
    r = run(
        ["python", "pool_lake.py", "fill", "--age-days", "7",
         "--min-liq", "100", "--max", "40", "--rpm", "22"],
        "pool_lake fill",
    )
    return r == 0 or r == -1  # tolerate timeout


def step_holders() -> bool:
    """Enrich holders via QuickNode RPC."""
    print("\n--- Step 3: Enrich holders ---")
    r = run(
        ["python", "enrich_holders.py", "--limit", "100"],
        "enrich_holders",
    )
    return r == 0 or r == 1  # tolerate partial failures


def step_synthesize(csv_in: str, count: int = 20,
                    peak_range: str = "5-50", lg_range: str = "3-300",
                    peak_dist: str = "bimodal") -> str | None:
    """Inject synthetic runner pools into the exported CSV for scalability testing."""
    print(f"\n--- Step 4.5: Synthesize {count} runners (peak_dist={peak_dist}) ---")
    out = csv_in.replace(".csv", "_synth.csv")
    r = run(
        ["python", "pool_lake.py", "synthesize",
         "--input", csv_in, "--out", out,
         "--count", str(count),
         "--peak-range", peak_range,
         "--lg-range", lg_range,
         "--peak-dist", peak_dist,
         "--seed", "2026"],
        "pool_lake synthesize",
    )
    if r == 0:
        return out
    return None


def step_export() -> str | None:
    """Export CSV from pool_lake."""
    print("\n--- Step 4: Export CSV ---")
    date = dt.date.today().isoformat()
    out = f"reports/pool_lake_{date}.csv"
    Path(out).parent.mkdir(exist_ok=True)
    r = run(
        ["python", "pool_lake.py", "export", "--out", out,
         "--min-swing", "0.0", "--max-peak", "50"],
        "pool_lake export",
    )
    if r == 0:
        return out
    return None


def step_backtest(csv_path: str, lg_proxy: bool = False,
                  max_positions: int = 10, multi: int = 30, lpn: int = 2,
                  realistic: bool = False, slip: float = 0.02) -> dict | None:
    """Run Desk backtest (N seeds) and extract results.

    Args:
        csv_path: CSV file to run on
        lg_proxy: If True, run in live-compatible mode (no future peak leak)
        max_positions: Max concurrent positions
        multi: Number of seeds
        lpn: Consecutive losses before self-pause
        realistic: If True, enforce liquidity-aware stake capping
        slip: Max stake as fraction of pool liquidity when realistic
    """
    extra = ["--lg-proxy"] if lg_proxy else []
    extra_title = " [LIVE]" if lg_proxy else ""
    if realistic:
        extra += ["--realistic", "--slip", str(slip)]
        extra_title += f" [REALISTIC slip={slip}]"
    print(f"\n--- Step 5: Backtest{extra_title} ({csv_path}, pos={max_positions}, lpn={lpn}, multi={multi}) ---")
    r = run(
        ["python", "rh_trencher.py", "--from-csv", csv_path,
         "--multi", str(multi), "--thin-cut", "0.0", "--lpn", str(lpn),
         "--max-positions", str(max_positions)] + extra,
        f"rh_trencher backtest{extra_title}",
    )
    if r != 0:
        return None

    # Quick re-run just the summary lines for clean report
    realistic_flags = f'"--realistic", "--slip", "{slip}"' if realistic else '[]'
    summary = subprocess.run(
        ["python", "-c", f"""
import subprocess
extra = {extra}
out = subprocess.run(["python", "rh_trencher.py", "--from-csv", "{csv_path}",
                       "--multi", "{multi}", "--thin-cut", "0.0", "--lpn", "{lpn}",
                       "--max-positions", "{max_positions}"] + extra,
                      capture_output=True, text=True).stdout
lines = [l for l in out.splitlines() if l.startswith("mult") or l.startswith("halt")]
print("\\n".join(lines))
"""],
        capture_output=True, text=True, cwd=str(BASE), timeout=120
    )
    print(summary.stdout)
    return summary.stdout.strip() if summary.stdout else None


def step_robustness_scan(csv_path: str, lg_proxy: bool = False, max_positions: int = 10, multi: int = 10,
                         jitter_pcts: list[float] | None = None,
                         jitter_min: int = 2,
                         realistic: bool = False, slip: float = 0.02) -> list[dict]:
    """Scan jitter levels to evaluate strategy robustness to real-world noise.

    Returns list of {jitter_pct, mean_mult, std_mult, cv}.
    """
    if jitter_pcts is None:
        jitter_pcts = [0.0, 0.01, 0.02, 0.03, 0.05]

    extra = ["--lg-proxy"] if lg_proxy else []
    if realistic:
        extra += ["--realistic", "--slip", str(slip)]
    mode_label = "LG-PROXY" if lg_proxy else "BACKTEST"
    if realistic:
        mode_label += "+REALISTIC"
    results = []

    print(f"\n--- Step 6: Robustness scan [{mode_label}] ---")
    print(f"  jitter_pct    mean_mult    cv       range")
    print(f"  {'-'*50}")

    for jt in jitter_pcts:
        cmd = ["python", "rh_trencher.py", "--from-csv", csv_path,
               "--multi", str(multi), "--thin-cut", "0.0", "--lpn", "3",
               "--max-positions", str(max_positions),
               "--jitter-pct", str(jt),
               "--jitter-min", str(jitter_min)] + extra
        r = subprocess.run(cmd, capture_output=True, text=True, cwd=str(BASE), timeout=120)
        mult_val, std_val = None, None
        seed_mults = []
        for l in r.stdout.splitlines():
            if "mult" in l and "mean=" in l:
                for tok in l.split():
                    if "mean=" in tok:
                        mult_val = float(tok.split("=")[1])
                    elif "std=" in tok:
                        std_val = float(tok.split("=")[1])
            parts = l.split()
            if len(parts) >= 2 and parts[0].isdigit():
                try:
                    seed_mults.append(float(parts[1]))
                except:
                    pass

        cv = (std_val / mult_val) if (mult_val and std_val) else 0
        rg = f"[{min(seed_mults):.1f}, {max(seed_mults):.1f}]" if seed_mults else "[?]"
        print(f"  {jt*100:>5.1f}%         {mult_val:>8.2f}    {cv:>6.2%}   {rg}")

        results.append({
            "jitter_pct": jt, "mean": mult_val, "std": std_val,
            "cv": cv, "range": rg,
        })

    return results


def main():
    ap = argparse.ArgumentParser(description="Daily data pipeline")
    ap.add_argument("--skip-dex", action="store_true")
    ap.add_argument("--skip-gt", action="store_true")
    ap.add_argument("--skip-holders", action="store_true")
    ap.add_argument("--skip-backtest", action="store_true")
    ap.add_argument("--skip-synth", action="store_true", help="Skip synthetic runner injection")
    ap.add_argument("--synth-count", type=int, default=20, help="Number of synthetic runners to inject")
    ap.add_argument("--synth-peak-range", default="5-50", help="Peak range for synth runners (e.g. '5-50')")
    ap.add_argument("--synth-lg-range", default="3-300", help="liq_growth range for synth runners")
    ap.add_argument("--synth-peak-dist", default="bimodal", choices=["uniform", "bimodal"],
                    help="Synth runner peak distribution (default: bimodal — realistic long-tail)")
    ap.add_argument("--lg-proxy", action="store_true", help="Live-compatible mode: no future peak leak")
    ap.add_argument("--max-positions", type=int, default=10, help="Max concurrent positions (lg-proxy opt: 10)")
    ap.add_argument("--lpn", type=int, default=2, help="Consecutive losses before self-pause (lg-proxy opt: 2)")
    ap.add_argument("--multi", type=int, default=30, help="Number of seeds")
    ap.add_argument("--robustness", action="store_true",
                    help="Run jitter robustness scan (Step 6 after backtest)")
    ap.add_argument("--robustness-seeds", type=int, default=10,
                    help="Number of seeds per jitter level in robustness scan")
    ap.add_argument("--realistic", action="store_true",
                    help="Enforce liquidity-aware stake capping (on by default with --lg-proxy)")
    ap.add_argument("--no-realistic", action="store_true",
                    help="Disable realistic mode even when --lg-proxy is set")
    ap.add_argument("--slip", type=float, default=0.02,
                    help="Max stake as fraction of pool liquidity when realistic (default 0.02 = 2%%)")
    ap.add_argument("--csv-input", default=None,
                    help="Use an existing CSV instead of running export (skips fetch+export steps)")
    args = ap.parse_args()

    # Auto-enable realistic mode when lg_proxy is set (unless explicitly disabled)
    use_realistic = args.realistic or (args.lg_proxy and not args.no_realistic)
    if args.lg_proxy and not args.no_realistic and not args.realistic:
        print("[auto] enabling --realistic with --lg-proxy (use --no-realistic to disable)")

    date = dt.date.today().isoformat()
    report = {
        "date": date,
        "start_time": dt.datetime.now().isoformat(),
        "steps": {},
    }

    print(f"\n{'#'*60}")
    print(f"#  RH Trencher Daily Pipeline  {date}")
    print(f"#  lg-proxy={args.lg_proxy}  realistic={use_realistic}  slip={args.slip}  max-positions={args.max_positions}")
    print(f"{'#'*60}")

    # 1. DexScreener
    if not args.skip_dex:
        report["steps"]["dex"] = step_dex()
    # 2. GT fill
    if not args.skip_gt:
        report["steps"]["gt"] = step_gt()
    # 3. Holders
    if not args.skip_holders:
        report["steps"]["holders"] = step_holders()
    # 4. Export (or use provided CSV)
    if args.csv_input:
        csv_path = args.csv_input
        print(f"\n--- Step 4: Using provided CSV ---")
        print(f"  {csv_path}")
    else:
        csv_path = step_export()
    report["export_csv"] = csv_path
    # 4.5 Synthesize
    if csv_path and not args.skip_synth and args.synth_count > 0:
        synth_csv = step_synthesize(
            csv_path, count=args.synth_count,
            peak_range=args.synth_peak_range, lg_range=args.synth_lg_range,
            peak_dist=args.synth_peak_dist,
        )
        if synth_csv:
            csv_path = synth_csv
            report["synthesized_csv"] = synth_csv
            report["steps"]["synthesize"] = args.synth_count
    # 5. Backtest
    if csv_path and not args.skip_backtest:
        result = step_backtest(
            csv_path, lg_proxy=args.lg_proxy,
            max_positions=args.max_positions, multi=args.multi,
            lpn=args.lpn,
            realistic=use_realistic, slip=args.slip,
        )
        report["backtest"] = result

    # 6. Robustness scan (optional)
    if csv_path and args.robustness:
        rob = step_robustness_scan(
            csv_path, lg_proxy=args.lg_proxy,
            max_positions=args.max_positions, multi=args.robustness_seeds,
            realistic=use_realistic, slip=args.slip,
        )
        report["robustness"] = rob
        # Also run backtest-mode robustness for comparison
        if args.lg_proxy:
            rob_bt = step_robustness_scan(
                csv_path, lg_proxy=False,
                max_positions=args.max_positions, multi=args.robustness_seeds,
                realistic=use_realistic, slip=args.slip,
            )
            report["robustness_backtest"] = rob_bt

    # Save report
    report["end_time"] = dt.datetime.now().isoformat()
    REPORT_DIR.mkdir(exist_ok=True)
    report_path = REPORT_DIR / f"report_{date}.json"
    report_path.write_text(json.dumps(report, indent=2))

    print(f"\n{'#'*60}")
    print(f"#  Pipeline complete!")
    print(f"#  Report: {report_path}")
    print(f"{'#'*60}")

    # Quick summary
    if report.get("backtest"):
        for line in report["backtest"].splitlines():
            print(f"  → {line}")


if __name__ == "__main__":
    main()

