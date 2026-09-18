"""OKX 模拟盘清仓脚本 — 卖出所有非 USDT 持仓，恢复 USDT 余额。"""
import os, sys, json, time
from pathlib import Path
sys.path.insert(0, os.path.dirname(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env", encoding="utf-8-sig")
except Exception:
    pass

from rh_okx_executor import OKXExecutor

def main():
    ex = OKXExecutor()
    print(f"Auth ready: {ex._auth_ready}, Demo: {ex.demo}")
    
    # Read SPOT account balance
    balance = ex.get_balance()
    details = balance.get("details", [])
    
    print(f"\n=== Account Total ===")
    print(f"Total eq: {balance.get('totalEq', '?')} USDT")
    
    print(f"\n=== Coin Holdings (spotBal > 0) ===")
    to_sell = []
    for d in details:
        ccy = d.get("ccy", "")
        spot_bal = float(d.get("spotBal", 0) or d.get("cashBal", 0) or 0)
        avail_bal = float(d.get("availBal", 0) or 0)
        eq_usd = d.get("eqUsd", "0")
        spot_upl = d.get("spotUpl", "")
        
        if spot_bal > 0.0000001 or avail_bal > 0.0000001:
            print(f"  {ccy:10s} spotBal={spot_bal:>14.6f}  availBal={avail_bal:>14.6f}  eqUsd={eq_usd}  upl={spot_upl}")
            # Sell available balance (not frozen)
            if ccy != "USDT" and avail_bal > 0.0000001:
                to_sell.append((ccy, avail_bal))
    
    if not to_sell:
        print("\n✅ Nothing to sell! All holdings cleared.")
        return
    
    print(f"\n=== Selling {len(to_sell)} coins (using availBal) ===")
    sold = 0
    failed = 0
    for ccy, bal in to_sell:
        inst_id = f"{ccy}-USDT"
        try:
            # Use size_usd = token quantity (market sell with sz=base asset)
            # We need to submit a sell order with exact token amount
            quote = ex.get_quote(inst_id, side="sell", size_usd=bal)
            order = ex.build_order(quote)
            # Override sz to actual token balance (OKX market sell sz=base asset)
            order.sz = str(bal)
            result = ex.submit_order(order)
            order_id = result.get("orderId", "?")
            print(f"  ✅ SOLD {ccy:10s} {bal:.6f} tokens -> orderId={order_id[:16] if order_id else '?'}")
            sold += 1
        except Exception as e:
            print(f"  ❌ FAIL {ccy:10s} {bal:.6f} -> {str(e)[:100]}")
            failed += 1
        time.sleep(0.5)  # rate limit

    print(f"\n=== Done: {sold} sold, {failed} failed ===")
    time.sleep(2)
    
    # Verify
    b2 = ex.get_balance()
    print(f"\n=== Post-Clear Balance ===")
    print(f"Total eq: {b2.get('totalEq', '?')} USDT")
    for d in b2.get("details", []):
        ccy = d.get("ccy")
        eq_usd = d.get("eqUsd", "0")
        spot_bal = d.get("spotBal", "0")
        avail = d.get("availBal", "0")
        print(f"  {ccy:10s} eqUsd={eq_usd:>10s}  availBal={avail}  spotBal={spot_bal}")

if __name__ == "__main__":
    main()
