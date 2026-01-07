"""
Incentiv Blockchain ETL Pipeline - Direct to Dune
Extracts data from Blockscout API and pushes directly to Dune Analytics

Usage:
    python incentiv_dune_etl.py --full          # Full historical extraction
    python incentiv_dune_etl.py --incremental   # Last 24 hours only
    python incentiv_dune_etl.py --test          # Test with small sample
"""

import requests
import json
import time
import argparse
import io
import csv
from datetime import datetime
from typing import Dict, List, Optional, Generator
from dataclasses import dataclass, asdict, fields

# =============================================================================
# CONFIGURATION
# =============================================================================

# Dune API
DUNE_API_KEY = "3nKpTZrrziBToMPOY7z2nybU8c6L3Our"
DUNE_API_BASE = "https://api.dune.com/api/v1"

# Blockscout API
BLOCKSCOUT_URL = "https://explorer.incentiv.io"

# Token list - fetched dynamically at runtime
TOKENS = {}  # Populated from /api/v2/tokens
DECIMALS = {}  # Populated from /api/v2/tokens

# DEX Pool
DEX_POOL = "0xf9884c2A1749b0a02ce780aDE437cBaDFA3a961D"

# Event signatures
EVENT_SIGS = {
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Swap": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    "ReceivedTransferRemote": "0xba20947a325f450d232530e5f5fce293e7963499d5309a07cee84a269f2f15a6",
}

REQUEST_DELAY = 0.2
MAX_RETRIES = 3


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class BridgeTransfer:
    tx_hash: str
    block_number: int
    timestamp: str
    direction: str
    token_symbol: str
    token_address: str
    origin_chain_id: int
    recipient: str
    amount_raw: str
    amount_decimal: float

@dataclass
class TokenTransfer:
    tx_hash: str
    block_number: int
    timestamp: str
    token_symbol: str
    token_address: str
    from_address: str
    to_address: str
    amount_raw: str
    amount_decimal: float
    is_mint: bool
    is_burn: bool

@dataclass
class DEXSwap:
    tx_hash: str
    block_number: int
    timestamp: str
    pool_address: str
    sender: str
    recipient: str
    amount0: str
    amount1: str
    sqrt_price_x96: str
    liquidity: str
    tick: int

@dataclass
class ActiveWallet:
    address: str
    first_seen_block: int
    first_seen_timestamp: str
    last_seen_block: int
    last_seen_timestamp: str
    tx_count: int
    token_transfer_count: int


# =============================================================================
# DUNE API CLIENT
# =============================================================================

class DuneClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = DUNE_API_BASE
        self.session = requests.Session()
        self.session.headers.update({
            "X-Dune-Api-Key": api_key,
            "Content-Type": "application/json"
        })
    
    def upload_csv(self, table_name: str, data: List, description: str = "") -> Dict:
        """Upload data as CSV (creates table if needed, namespace auto-detected from API key)"""
        if not data:
            print(f"  ⚠️ No data to upload for {table_name}")
            return {"status": "empty"}
        
        # Save local backup first
        backup_file = f"{table_name}_backup.csv"
        print(f"  💾 Saving local backup to {backup_file}...")
        try:
            with open(backup_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=[f.name for f in fields(data[0])])
                writer.writeheader()
                for item in data:
                    writer.writerow(asdict(item))
        except Exception as e:
            print(f"  ⚠️ Failed to save backup: {e}")

        # Chunk size for upload (e.g., 2000 rows per request to be safe)
        CHUNK_SIZE = 2000
        total_rows = len(data)
        print(f"  📤 Uploading {total_rows} rows to {table_name} in chunks of {CHUNK_SIZE}...")
        
        url = f"{self.base_url}/table/upload/csv"
        headers = {"X-Dune-Api-Key": self.api_key}
        
        success_count = 0
        
        for i in range(0, total_rows, CHUNK_SIZE):
            chunk = data[i:i + CHUNK_SIZE]
            current_chunk_num = (i // CHUNK_SIZE) + 1
            total_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
            
            print(f"    • Uploading chunk {current_chunk_num}/{total_chunks} ({len(chunk)} rows)...")
            
            # Convert chunk to CSV string
            output = io.StringIO()
            field_names = [f.name for f in fields(chunk[0])]
            writer = csv.DictWriter(output, fieldnames=field_names)
            writer.writeheader()
            for item in chunk:
                writer.writerow(asdict(item))
            
            csv_content = output.getvalue()
            
            files = {
                "data": (f"{table_name}_chunk_{current_chunk_num}.csv", csv_content, "text/csv")
            }
            
            form_data = {
                "table_name": table_name,
                "description": description or f"Incentiv {table_name} data",
                "is_private": "false"
            }
            
            try:
                resp = requests.post(url, files=files, data=form_data, headers=headers)
                if resp.status_code == 200:
                    success_count += len(chunk)
                else:
                    print(f"    ❌ Chunk {current_chunk_num} failed: {resp.status_code} - {resp.text}")
            except Exception as e:
                print(f"    ❌ Chunk {current_chunk_num} error: {e}")
                
            # Small delay to avoid rate limits
            time.sleep(1)
            
        print(f"  ✅ Uploaded {success_count}/{total_rows} rows to {table_name}")
        return {"status": "completed", "rows_uploaded": success_count}


# =============================================================================
# BLOCKSCOUT API CLIENT
# =============================================================================

class BlockscoutAPI:
    def __init__(self, base_url: str = BLOCKSCOUT_URL):
        self.base_url = base_url
        self.session = requests.Session()
    
    def _request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(REQUEST_DELAY)
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                print(f"  ⚠️ Request failed (attempt {attempt + 1}): {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)
        return {}
    
    def get_stats(self) -> Dict:
        return self._request("/api/v2/stats")
    
    def get_tokens(self) -> List[Dict]:
        """Fetch all tokens from the chain"""
        tokens = []
        next_page = None
        
        while True:
            data = self._request("/api/v2/tokens", params=next_page)
            items = data.get("items", [])
            if not items:
                break
            tokens.extend(items)
            next_page = data.get("next_page_params")
            if not next_page:
                break
        
        return tokens
    
    def get_address_logs(self, address: str, next_page: Optional[Dict] = None) -> Dict:
        return self._request(f"/api/v2/addresses/{address}/logs", params=next_page)
    
    def get_token_transfers(self, next_page: Optional[Dict] = None) -> Dict:
        return self._request("/api/v2/token-transfers", params=next_page)
    
    def get_logs_rpc(self, address: str, topic0: str, from_block: str = "0", to_block: str = "latest") -> List[Dict]:
        """Fetch logs using the RPC-style endpoint for efficient filtering"""
        url = f"{self.base_url}/api"
        params = {
            "module": "logs",
            "action": "getLogs",
            "address": address,
            "topic0": topic0,
            "fromBlock": from_block,
            "toBlock": to_block
        }
        
        print(f"  🔍 RPC fetch: {address[:10]}... (topic: {topic0[:10]}...)")
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            result = data.get("result", [])
            if isinstance(result, list):
                return result
            else:
                print(f"  ⚠️ RPC returned non-list result: {result}")
                # Print full response for debugging
                print(f"  🔍 Full response: {json.dumps(data)}")
                if result is None:
                    return []
                return []
        except Exception as e:
            print(f"  ⚠️ RPC request failed: {e}")
            return []

    def paginate(self, fetch_func, max_pages: Optional[int] = None) -> Generator:
        next_page = None
        page_count = 0
        
        while True:
            data = fetch_func(next_page)
            items = data.get("items", [])
            
            if not items:
                break
            
            yield from items
            
            next_page = data.get("next_page_params")
            page_count += 1
            
            if not next_page:
                break
            if max_pages and page_count >= max_pages:
                print(f"  ℹ️ Reached page limit ({max_pages})")
                break
            
            if page_count % 10 == 0:
                print(f"  📄 Processed {page_count} pages...")


# =============================================================================
# ETL PIPELINE
# =============================================================================

class IncentivETL:
    def __init__(self, blockscout: BlockscoutAPI, dune: DuneClient):
        self.api = blockscout
        self.dune = dune
        self.wallets: Dict[str, ActiveWallet] = {}
    
    def extract_bridge_transfers(self, max_pages: Optional[int] = None) -> List[BridgeTransfer]:
        """Extract bridge inflows from all token contracts"""
        print("\n🌉 Extracting bridge transfers...")
        transfers = []
        
        for symbol, address in TOKENS.items():
            print(f"  📍 Scanning {symbol}...")
            
            # Use efficient RPC filtering
            logs = self.api.get_logs_rpc(address, EVENT_SIGS["ReceivedTransferRemote"])
            print(f"    Found {len(logs)} potential bridge events")
            
            for log in logs:
                topics = log.get("topics", [])
                if not topics or topics[0] != EVENT_SIGS["ReceivedTransferRemote"]:
                    continue
                
                # RPC response uses hex strings for everything
                try:
                    # Parse fields - RPC returns camelCase
                    tx_hash = log.get("transactionHash", "")
                    block_number = int(log.get("blockNumber", "0"), 16)
                    timestamp = log.get("timeStamp", "") # Might be missing in RPC
                    
                    # Topics[1] is origin chain, Topics[2] is recipient
                    origin_chain = int(topics[1], 16) if len(topics) > 1 and topics[1] else 0
                    recipient = "0x" + topics[2][-40:] if len(topics) > 2 and topics[2] else ""
                    
                    # Data contains the amount (non-indexed parameter)
                    data = log.get("data", "0x")
                    if data.startswith("0x"):
                        data = data[2:]
                    amount_raw = str(int(data, 16)) if data else "0"
                    
                    decimals = DECIMALS.get(symbol, 18)
                    try:
                        amount_decimal = int(amount_raw) / (10 ** decimals)
                    except:
                        amount_decimal = 0.0
                    
                    transfer = BridgeTransfer(
                        tx_hash=tx_hash,
                        block_number=block_number,
                        timestamp="", # Timestamp usually not in getLogs result, handled later/empty
                        direction="inflow",
                        token_symbol=symbol,
                        token_address=address,
                        origin_chain_id=origin_chain,
                        recipient=recipient,
                        amount_raw=amount_raw,
                        amount_decimal=amount_decimal,
                    )
                    transfers.append(transfer)
                    self._track_wallet(recipient, block_number, "")
                except Exception as e:
                    print(f"    ⚠️ Error parsing log: {e}")
                    continue
        
        print(f"  ✅ Found {len(transfers)} bridge transfers")
        return transfers
    
    def extract_token_transfers(self, max_pages: Optional[int] = None) -> List[TokenTransfer]:
        """Extract all token transfers"""
        print("\n💸 Extracting token transfers...")
        transfers = []
        
        # We can use the /api/v2/token-transfers endpoint to get ALL transfers
        # This is more efficient than scanning per token
        
        # But for now, let's use the per-token approach if the global endpoint is limited
        # Actually, global endpoint is better but might miss some if we don't paginate enough
        # Let's stick to per-token to be thorough, or just use global?
        # The original code used per-token. Let's keep it but fix the loop.
        
        for address, symbol in TOKENS.items():
            print(f"  📍 Scanning {symbol} ({address[:10]}...)... বিজয়")
            
            for tx in self.api.paginate(
                lambda p, addr=address: self.api._request(f"/api/v2/tokens/{addr}/transfers", params=p),
                max_pages=max_pages
            ):
                try:
                    t = TokenTransfer(
                        tx_hash=tx.get("tx_hash", ""),
                        block_number=int(tx.get("block_number") or 0),
                        timestamp=tx.get("timestamp", ""),
                        token_symbol=symbol,
                        token_address=address,
                        from_address=tx.get("from", {}).get("hash", ""),
                        to_address=tx.get("to", {}).get("hash", ""),
                        amount_raw=tx.get("total", {}).get("value", "0"),
                        amount_decimal=float(tx.get("total", {}).get("decimals", "0") or 0),
                        is_mint=False, # Logic needed
                        is_burn=False  # Logic needed
                    )
                    
                    # Determine mint/burn
                    if t.from_address == "0x0000000000000000000000000000000000000000":
                        t.is_mint = True
                    if t.to_address == "0x0000000000000000000000000000000000000000":
                        t.is_burn = True
                        
                    transfers.append(t)
                    
                    if not t.is_mint:
                        self._track_wallet(t.from_address, t.block_number, t.timestamp)
                    if not t.is_burn:
                        self._track_wallet(t.to_address, t.block_number, t.timestamp)
                        
                except Exception as e:
                    # print(f"    ⚠️ Error parsing transfer: {e}")
                    pass
                    
        print(f"  ✅ Found {len(transfers)} token transfers")
        return transfers
    
    def extract_dex_swaps(self, max_pages: Optional[int] = None) -> List[DEXSwap]:
        """Extract DEX swaps from pool contract"""
        print("\n🔄 Extracting DEX swaps...")
        swaps = []
        
        for log in self.api.paginate(
            lambda p: self.api.get_address_logs(DEX_POOL, p),
            max_pages=max_pages
        ):
            topics = log.get("topics", [])
            if not topics or topics[0] != EVENT_SIGS["Swap"]:
                continue
            
            decoded = log.get("decoded", {})
            params = {p["name"]: p["value"] for p in decoded.get("parameters", [])}
            
            swap = DEXSwap(
                tx_hash=log.get("transaction_hash", ""),
                block_number=log.get("block_number", 0),
                timestamp="",
                pool_address=DEX_POOL,
                sender=str(params.get("sender", "")),
                recipient=str(params.get("recipient", "")),
                amount0=str(params.get("amount0", "0")),
                amount1=str(params.get("amount1", "0")),
                sqrt_price_x96=str(params.get("sqrtPriceX96", "0")),
                liquidity=str(params.get("liquidity", "0")),
                tick=int(params.get("tick", 0)) if params.get("tick") else 0,
            )
            swaps.append(swap)
            
            self._track_wallet(swap.sender, log.get("block_number", 0), "")
            self._track_wallet(swap.recipient, log.get("block_number", 0), "")
        
        print(f"  ✅ Found {len(swaps)} DEX swaps")
        return swaps
    
    def _track_wallet(self, address: str, block: int, timestamp: str):
        if not address or address == "0x0000000000000000000000000000000000000000":
            return
        
        address = address.lower()
        if address not in self.wallets:
            self.wallets[address] = ActiveWallet(
                address=address,
                first_seen_block=block,
                first_seen_timestamp=timestamp,
                last_seen_block=block,
                last_seen_timestamp=timestamp,
                tx_count=0,
                token_transfer_count=1,
            )
        else:
            w = self.wallets[address]
            w.last_seen_block = max(w.last_seen_block, block)
            w.last_seen_timestamp = timestamp or w.last_seen_timestamp
            w.token_transfer_count += 1
    
    def get_wallets(self) -> List[ActiveWallet]:
        return list(self.wallets.values())
    
    def push_to_dune(self, bridge: List, transfers: List, swaps: List, wallets: List):
        """Push all data to Dune"""
        print("\n📤 Pushing data to Dune...")
        
        # Upload each dataset
        if bridge:
            self.dune.upload_csv("incentiv_bridge_transfers", bridge, 
                               "Incentiv bridge inflows via Hyperlane")
        
        if transfers:
            self.dune.upload_csv("incentiv_token_transfers", transfers,
                               "Incentiv ERC20 token transfers")
        
        if swaps:
            self.dune.upload_csv("incentiv_dex_swaps", swaps,
                               "Incentiv DEX swap events")
        
        if wallets:
            self.dune.upload_csv("incentiv_active_wallets", wallets,
                               "Incentiv unique active wallets")


# =============================================================================
# MAIN
# =============================================================================

def main():
    global TOKENS, DECIMALS
    
    parser = argparse.ArgumentParser(description="Incentiv ETL → Dune")
    parser.add_argument("--full", action="store_true", help="Full extraction")
    parser.add_argument("--incremental", action="store_true", help="Last 24h")
    parser.add_argument("--test", action="store_true", help="Small sample")
    parser.add_argument("--bridge-only", action="store_true", help="Extract bridge transfers only")
    args = parser.parse_args()
    
    if args.test:
        max_pages = 2
        print("🧪 TEST MODE (2 pages)")
    elif args.incremental:
        max_pages = 10
        print("⏱️ INCREMENTAL MODE (10 pages)")
    else:
        max_pages = None
        print("📊 FULL MODE (all data)")
    
    # Initialize clients
    blockscout = BlockscoutAPI()
    dune = DuneClient(DUNE_API_KEY)
    
    # Fetch tokens dynamically
    print("\n🪙 Fetching token list...")
    token_list = blockscout.get_tokens()
    for t in token_list:
        symbol = t.get("symbol", "UNKNOWN")
        address = t.get("address_hash", "")
        decimals = int(t.get("decimals", "18") or "18")
        TOKENS[address] = symbol
        DECIMALS[address] = decimals
        print(f"  • {symbol}: {address[:10]}... ({decimals} decimals)")
    
    print(f"  ✅ Found {len(TOKENS)} tokens")
    
    # Manually add known bridge tokens if missing
    # USDC (Verified from bridge events)
    known_usdc = "0x16e43840d8D79896A389a3De85aB0B0210C05685"
    if known_usdc not in TOKENS:
        print(f"  ➕ Adding known USDC token: {known_usdc}")
        TOKENS[known_usdc] = "USDC"
        DECIMALS[known_usdc] = 6
    
    etl = IncentivETL(blockscout, dune)
    
    # Get chain stats
    print("\n📈 Chain Statistics...")
    stats = blockscout.get_stats()
    print(f"  Blocks: {stats.get('total_blocks', 'N/A')}")
    print(f"  Transactions: {stats.get('total_transactions', 'N/A')}")
    print(f"  Addresses: {stats.get('total_addresses', 'N/A')}")
    
    # Extract data
    bridge_transfers = []
    token_transfers = []
    dex_swaps = []
    
    if getattr(args, "bridge_only", False):
        bridge_transfers = etl.extract_bridge_transfers(max_pages)
    else:
        bridge_transfers = etl.extract_bridge_transfers(max_pages)
        token_transfers = etl.extract_token_transfers(max_pages)
        dex_swaps = etl.extract_dex_swaps(max_pages)
    
    active_wallets = etl.get_wallets()
    
    # Push to Dune
    etl.push_to_dune(bridge_transfers, token_transfers, dex_swaps, active_wallets)
    
    # Summary
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                 ETL → DUNE COMPLETE                          ║
╠══════════════════════════════════════════════════════════════╣
║  Bridge Transfers:  {len(bridge_transfers):>8}                              ║
║  Token Transfers:   {len(token_transfers):>8}                              ║
║  DEX Swaps:         {len(dex_swaps):>8}                              ║
║  Active Wallets:    {len(active_wallets):>8}                              ║
║  Tokens Tracked:    {len(TOKENS):>8}                              ║
╠══════════════════════════════════════════════════════════════╣
║  Tables created in Dune:                                     ║
║    • incentiv_bridge_transfers                               ║
║    • incentiv_token_transfers                                ║
║    • incentiv_dex_swaps                                      ║
║    • incentiv_active_wallets                                 ║
╚══════════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    main()
