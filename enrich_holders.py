"""Enrich pool_lake with real holder data + on-chain linked_groups.

Two-tier RPC:
- QuickNode demo   → getTokenLargestAccounts for holders + amounts (100/day)
- Public Solana    → getSignaturesForAddress + getTransaction for funder clustering

Wallet clustering logic:
1. For each top holder, fetch recent SPL transfers where they were destination
2. Filter transfers within [mint_created, mint_created + WINDOW_HOURS]
3. Group holders by their first transfer source (funder wallet)
4. Groups of ≥2 holders sharing the same funder → linked_groups (likely coordinated)
5. selling_linked combines top1% concentration + linked_groups size
"""
from __future__ import annotations

import json
import sqlite3
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

import base58

# ── RPC endpoints ──────────────────────────────────────────────────────────
HOLDERS_RPC = "https://docs-demo.solana-mainnet.quiknode.pro/"
CHAIN_RPC = "https://api.mainnet-beta.solana.com"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# Mint window: only consider funders within 1 hour of mint creation
WINDOW_SECONDS = 3600

# Rate limits
SLEEP_HOLDERS = 0.3   # between getTokenLargestAccounts calls
SLEEP_CHAIN   = 0.5   # between getTransaction calls on public RPC


def rpc_call(rpc_url: str, method: str, params: list | None = None, retries: int = 2) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                rpc_url,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            resp = urllib.request.urlopen(req, timeout=25)
            return json.loads(resp.read())
        except Exception as e:
            if attempt == retries - 1:
                return {"error": str(e)}
            time.sleep(1.0 * (attempt + 1))
    return {"error": "max retries"}


# ── Tier 1: Holder fetching (QuickNode) ──────────────────────────────────
def get_top_holders_with_amounts(mint: str, n: int = 8) -> list[tuple[str, float]]:
    resp = rpc_call(HOLDERS_RPC, "getTokenLargestAccounts", [mint, {"commitment": "finalized"}])
    if "error" in resp:
        return []
    result = resp.get("result", {})
    if not result:
        return []
    value = result.get("value", [])
    holders = []
    for item in value[:n]:
        addr = item.get("address", "")
        amount = float(item.get("uiAmount", 0) or 0)
        holders.append((addr, amount))
    return holders


def compute_concentration(holders: list[tuple[str, float]]) -> tuple[float, float, float]:
    if not holders:
        return 0.0, 0.0, 0.0
    total = sum(a for _, a in holders)
    if total <= 0:
        return 0.0, 0.0, 0.0
    top1 = holders[0][1] / total if len(holders) >= 1 else 0.0
    top3 = sum(a for _, a in holders[:3]) / total if len(holders) >= 3 else sum(a for _, a in holders) / total
    return top1, top3, total


# ── Tier 2: On-chain funder clustering (Public RPC) ────────────────────────

# Cache for ATA → owner lookups (avoid repeated RPCs)
_ATA_CACHE: dict[str, str | None] = {}


def ata_to_owner(ata: str) -> str | None:
    """Convert an Associated Token Account to its owner wallet."""
    if ata in _ATA_CACHE:
        return _ATA_CACHE[ata]
    resp = rpc_call(CHAIN_RPC, "getAccountInfo",
                    [ata, {"encoding": "jsonParsed"}])
    result = resp.get("result")
    owner = None
    if result and isinstance(result, dict):
        val = result.get("value", {})
        if isinstance(val, dict):
            parsed = val.get("data", {}).get("parsed", {})
            owner = parsed.get("info", {}).get("owner")
    _ATA_CACHE[ata] = owner
    time.sleep(0.2)
    return owner


def get_mint_transfers(mint: str, limit: int = 30,
                       mint_bt: int | None = None) -> list[dict]:
    """
    Fetch SPL Token Transfer transactions for this mint.
    Returns list of {src_ata, dest_ata, blockTime, amount}.
    
    Strategy: query mint's signatures in chronological order, then
    parse each transaction's instructions for Transfer type=3 where
    pre/postTokenBalances include our mint.
    """
    # Get mint's own signatures (newest first by default, 30 most recent)
    resp = rpc_call(CHAIN_RPC, "getSignaturesForAddress",
                    [mint, {"limit": limit, "commitment": "finalized"}])
    sigs = resp.get("result")
    if not isinstance(sigs, list):
        return []
    
    transfers = []
    for s in sigs:
        sig = s.get("signature")
        sig_bt = s.get("blockTime")
        if not sig:
            continue
        
        # Skip pre-mint transactions
        if mint_bt is not None and sig_bt is not None and sig_bt < mint_bt - 60:
            continue
        # Skip way-too-old transactions (before mint + window)
        if mint_bt is not None and sig_bt is not None and sig_bt > mint_bt + WINDOW_SECONDS * 2:
            # We're past the 2x window — still scan but mark as post-mint
            pass
        
        tx_resp = rpc_call(CHAIN_RPC, "getTransaction",
                           [sig, {"commitment": "finalized",
                                  "maxSupportedTransactionVersion": 0}])
        tx = tx_resp.get("result")
        if not isinstance(tx, dict):
            time.sleep(SLEEP_CHAIN)
            continue
        
        msg = tx.get("transaction", {}).get("message", {})
        accts_raw = msg.get("accountKeys", [])
        acct_pks = [a if isinstance(a, str) else a.get("pubkey", "")
                    for a in accts_raw]
        
        # Token mint filter via meta
        meta = tx.get("meta", {})
        mints_in_tx: set[str] = set()
        for bal in (meta.get("preTokenBalances") or []):
            if isinstance(bal, dict):
                mints_in_tx.add(bal.get("mint", ""))
        for bal in (meta.get("postTokenBalances") or []):
            if isinstance(bal, dict):
                mints_in_tx.add(bal.get("mint", ""))
        
        if mint not in mints_in_tx:
            time.sleep(SLEEP_CHAIN)
            continue
        
        ins = msg.get("instructions", [])
        for ins_item in ins:
            if not isinstance(ins_item, dict):
                continue
            pid_idx = ins_item.get("programIdIndex", -1)
            if pid_idx < 0 or pid_idx >= len(acct_pks):
                continue
            if acct_pks[pid_idx] != TOKEN_PROGRAM:
                continue
            
            data_b58 = ins_item.get("data", "")
            acct_indices = ins_item.get("accounts", [])
            try:
                raw = base58.b58decode(data_b58)
            except Exception:
                continue
            
            instr_type = raw[0] if raw else -1
            if instr_type != 3:  # Transfer
                continue
            if len(acct_indices) < 3:
                continue
            
            src_idx = acct_indices[0]
            dest_idx = acct_indices[1]
            src_ata = acct_pks[src_idx] if src_idx < len(acct_pks) else ""
            dest_ata = acct_pks[dest_idx] if dest_idx < len(acct_pks) else ""
            amount = int.from_bytes(raw[1:9], "little") if len(raw) >= 9 else 0
            
            if src_ata and dest_ata:
                transfers.append({
                    "src_ata": src_ata,
                    "dest_ata": dest_ata,
                    "blockTime": sig_bt,
                    "amount": amount,
                })
        
        time.sleep(SLEEP_CHAIN)
    
    return transfers


def get_mint_creation_time(mint: str) -> int | None:
    """Get earliest known blockTime for this mint (≈ mint creation)."""
    resp = rpc_call(CHAIN_RPC, "getSignaturesForAddress",
                    [mint, {"limit": 3, "commitment": "finalized"}])
    sigs = resp.get("result")
    if not isinstance(sigs, list) or not sigs:
        return None
    # Public RPC returns newest-first; the last element is oldest
    oldest = sigs[-1]
    return oldest.get("blockTime")


# ── Linked group builder ──────────────────────────────────────────────────
def build_linked_groups(mint: str, holder_atas: list[str],
                        mint_bt: int | None, verbose: bool = False
                        ) -> tuple[list[set], dict[str, str | None], list[str]]:
    """
    New approach: query all SPL Transfers for the mint, then cluster
    holder-owners by their common funder (src wallet in initial transfers).

    Returns:
      linked_groups: list[set[str]]  — sets of owner wallets sharing a funder
      funder_of:     dict[owner] = funder_wallet or None
      owner_list:    list[str]       — owner wallets (replaces ATA addresses)
    """
    # Step 1: ATA → owner for all holders
    owner_of_ata: dict[str, str | None] = {}
    owners: list[str] = []
    for ata in holder_atas:
        owner = ata_to_owner(ata)
        owner_of_ata[ata] = owner
        if owner and owner not in owners:
            owners.append(owner)
        if verbose:
            label = owner[:12] + "..." if owner else "?"
            print(f"    ATA→owner: {ata[:12]}... → {label}")

    if len(owners) < 2:
        return [], {}, owners

    # Step 2: Fetch all SPL Transfers involving this mint
    transfers = get_mint_transfers(mint, limit=30, mint_bt=mint_bt)
    if verbose:
        print(f"    mint transfers: {len(transfers)} found")

    # Step 3: For each transfer, map dest_ata → owner → funder = src_ata
    #   First-transfer-wins policy: we track earliest funder per owner
    funder_of: dict[str, str | None] = {o: None for o in owners}
    first_transfer_bt: dict[str, int] = {}

    holder_atas_set = set(holder_atas)

    for t in transfers:
        src_ata = t["src_ata"]
        dest_ata = t["dest_ata"]
        bt = t["blockTime"] or 0

        # Skip transfers where dest is not one of our holder ATAs
        if dest_ata not in holder_atas_set:
            continue

        dest_owner = owner_of_ata.get(dest_ata)
        if not dest_owner or dest_owner not in funder_of:
            continue

        # Time window filter: within mint_bt → mint_bt + WINDOW
        if mint_bt is not None:
            if bt < mint_bt - 60 or bt > mint_bt + WINDOW_SECONDS:
                continue

        # Convert src_ata to owner too (for clustering by owner)
        src_owner = ata_to_owner(src_ata) or src_ata

        # First-transfer-wins (earliest in window)
        if dest_owner not in first_transfer_bt or bt < first_transfer_bt[dest_owner]:
            first_transfer_bt[dest_owner] = bt
            funder_of[dest_owner] = src_owner

        if verbose:
            print(f"    Transfer match: {src_owner[:12]}... → {dest_owner[:12]}... "
                  f"bt={bt}")

    # Step 4: Cluster owners by funder
    groups: dict[str, set] = defaultdict(set)
    for owner, funder in funder_of.items():
        if funder:
            groups[funder].add(owner)

    linked = [g for g in groups.values() if len(g) >= 2]

    if verbose:
        print(f"    owners with funder: "
              f"{sum(1 for v in funder_of.values() if v is not None)}/{len(owners)}")
        print(f"    linked groups (≥2): {len(linked)}")
        for i, g in enumerate(linked):
            print(f"      group[{i}]: {[o[:12]+'...' for o in g]}")

    return linked, funder_of, owners


# ── selling_linked calculation ─────────────────────────────────────────────
def compute_selling_linked(holders: list[tuple[str, float]],
                           linked_groups: list[set]) -> int:
    """
    selling_linked risk levels:
      0 = safe
      1 = top1 ≥ 30%  (whale risk)
      2 = top1 ≥ 50%  OR linked ≥ 3 holders OR top3 ≥ 70%  (extreme)
      3 = top1 ≥ 70%  (single-holder rug)
    """
    if not holders:
        return 0
    
    top1_share, top3_share, _ = compute_concentration(holders)
    n_linked = sum(len(g) for g in linked_groups)
    
    # Single-holder rug
    if top1_share >= 0.70:
        return 3
    # Extreme: whale + coordinated selling
    if top1_share >= 0.50 or top3_share >= 0.70 or n_linked >= 3:
        return 2
    # Whale risk
    if top1_share >= 0.30:
        return 1
    
    return 0


# ── Main pipeline ─────────────────────────────────────────────────────────
def enrich_pool_lake(db_path: str, limit: int = 20,
                     re_enrich: bool = False,
                     skip_linked: bool = False,
                     dry_run: bool = False) -> None:
    """
    Enrich pool_rows with holder data + on-chain linked_groups.
    
    Pipeline per pool:
      1. Fetch top-8 holders via QuickNode getTokenLargestAccounts
      2. Skip linked detection if skip_linked or holders=[]
      3. Get mint creation time via getSignaturesForAddress(mint)
      4. For each holder: getTransaction → SPL Transfer → funder source
      5. Cluster funders → linked_groups
      6. Compute selling_linked from concentration + linked_groups
      7. UPDATE pool_rows
    """
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    
    # Ensure columns exist
    cols = [c[1] for c in db.execute("PRAGMA table_info(pool_rows)").fetchall()]
    if "holders" not in cols:
        db.execute("ALTER TABLE pool_rows ADD COLUMN holders TEXT DEFAULT '[]'")
    if "linked_groups" not in cols:
        db.execute("ALTER TABLE pool_rows ADD COLUMN linked_groups TEXT DEFAULT '[]'")
    if "selling_linked" not in cols:
        db.execute("ALTER TABLE pool_rows ADD COLUMN selling_linked INTEGER DEFAULT 0")
    
    # Select pools
    if re_enrich:
        where = "rp.mint_address IS NOT NULL AND rp.mint_address != ''"
    else:
        # Prefer pools with existing holders but missing real linked_groups
        where = ("(pr.holders IS NULL OR pr.holders = '[]' "
                 "OR pr.linked_groups = '[]') "
                 "AND rp.mint_address IS NOT NULL AND rp.mint_address != ''")
    
    rows = db.execute(
        f"""SELECT pr.pair_address, rp.mint_address, pr.ticker, pr.holders,
                   rp.discovered_at
            FROM pool_rows pr
            JOIN raw_pools rp ON pr.pair_address = rp.pair_address
            WHERE {where}
            ORDER BY LENGTH(pr.holders) ASC  -- pools with no holders first
            LIMIT ?""",
        (limit,),
    ).fetchall()
    
    print(f"[enrich] targets: {len(rows)} pools "
          f"(re_enrich={re_enrich}, skip_linked={skip_linked}, dry_run={dry_run})")
    
    updated = 0
    linked_new = 0
    risk_dist = {0: 0, 1: 0, 2: 0, 3: 0}
    
    for i, r in enumerate(rows):
        mint = r["mint_address"]
        ticker = r["ticker"]
        pair = r["pair_address"]
        existing_holders = r["holders"]
        discovered_at = r["discovered_at"]
        
        print(f"\n{'='*60}")
        print(f"[{i+1}/{len(rows)}] {ticker}  mint={mint[:20]}...")
        
        # Step 1: Fetch holders (if missing)
        if not existing_holders or existing_holders == "[]":
            holder_tuples = get_top_holders_with_amounts(mint, n=8)
            time.sleep(SLEEP_HOLDERS)
        else:
            # We have addresses but no amounts; re-fetch to be safe
            holder_tuples = get_top_holders_with_amounts(mint, n=8)
            time.sleep(SLEEP_HOLDERS)
            if not holder_tuples:
                # Fallback to existing addresses without amounts
                addrs = json.loads(existing_holders)
                holder_tuples = [(a, 0.0) for a in addrs]
        
        if not holder_tuples:
            print(f"  ✗ NO HOLDER DATA (RPC failed)")
            continue
        
        addresses = [addr for addr, _ in holder_tuples]
        top1, top3, total = compute_concentration(holder_tuples)
        print(f"  holders={len(addresses)}  top1={top1:.0%}  top3={top3:.0%}")
        
        # Step 2: Linked group detection
        linked_groups: list[set] = []
        if not skip_linked and len(addresses) >= 2:
            # Get mint creation time
            mint_bt = get_mint_creation_time(mint)
            if mint_bt is None:
                mint_bt = discovered_at or int(time.time())
                print(f"  mint_bt: unknown, using discovered_at={mint_bt}")
            else:
                print(f"  mint_creation_bt: {mint_bt} "
                      f"(age={int(time.time()) - mint_bt}s)")
            
            linked_groups, funder_map, owner_list = build_linked_groups(
                mint, addresses, mint_bt, verbose=True)
            
            n_linked = sum(len(g) for g in linked_groups)
            if n_linked > 0:
                linked_new += 1
                print(f"  ✓ LINKED {n_linked} owners across "
                      f"{len(linked_groups)} funder groups")
            else:
                print(f"  - no linked funders found")
            
            # Use owner wallets for storage (not ATA)
            storage_addresses = owner_list if owner_list else addresses
        elif skip_linked:
            print(f"  (linked_groups skipped)")
            storage_addresses = addresses
        else:
            print(f"  (too few holders for clustering)")
            storage_addresses = addresses
        
        # Step 3: selling_linked
        selling = compute_selling_linked(holder_tuples, linked_groups)
        risk_dist[selling] += 1
        risk_label = {0: "safe", 1: "whale", 2: "extreme", 3: "single"}[selling]
        print(f"  → selling_linked={selling} ({risk_label})")
        
        # Step 4: Commit
        if dry_run:
            print(f"  [dry-run] would UPDATE pool_rows SET "
                  f"holders=[{len(storage_addresses)} owners] "
                  f"linked_groups=[{len(linked_groups)} groups] "
                  f"selling_linked={selling}")
        else:
            db.execute(
                "UPDATE pool_rows SET holders=?, linked_groups=?, selling_linked=? "
                "WHERE pair_address=?",
                (json.dumps(storage_addresses),
                 json.dumps([sorted(list(g)) for g in linked_groups]),
                 selling, pair),
            )
            updated += 1
            if updated % 10 == 0:
                db.commit()  # periodic checkpoint
    
    db.commit()
    
    print(f"\n{'='*60}")
    print(f"[enrich] done: {updated}/{len(rows)} pools updated")
    print(f"  pools with real linked_groups: {linked_new}")
    print(f"  risk distribution: "
          f"safe={risk_dist[0]} whale={risk_dist[1]} "
          f"extreme={risk_dist[2]} single={risk_dist[3]}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Enrich pool_lake with on-chain holder data")
    ap.add_argument("--db", default="pool_lake.db")
    ap.add_argument("--limit", type=int, default=20,
                    help="Max pools to enrich in this run")
    ap.add_argument("--re-enrich", action="store_true",
                    help="Re-enrich ALL pools even if already have holder data")
    ap.add_argument("--skip-linked", action="store_true",
                    help="Only fetch holders, skip expensive linked_groups detection")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print what would be done, don't write to DB")
    args = ap.parse_args()
    enrich_pool_lake(args.db, args.limit, args.re_enrich,
                     args.skip_linked, args.dry_run)
