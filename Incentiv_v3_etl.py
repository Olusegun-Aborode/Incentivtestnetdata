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

# Token list - core bridged tokens (hardcoded) + dynamic tokens
BRIDGED_TOKENS = {
    "USDC": "0x16e43840d8D79896A389a3De85aB0B0210C05685",
    "USDT": "0x39b076b5d23F588690D480af3Bf820edad31a4bB",
    "WETH": "0x3e425317dB7BaC8077093117081b40d9b46F29cb",
    "SOL": "0xfaC24134dbc4b00Ee11114eCDFE6397f389203E3",
    "WBTC": "0x0292593D416Cb765E0e8FF77b32fA7e465958FEE",
}

BRIDGED_DECIMALS = {
    "USDC": 6,
    "USDT": 6,
    "WETH": 18,
    "SOL": 9,
    "WBTC": 8,
}

TOKENS = {}  # Populated from BRIDGED_TOKENS + /api/v2/tokens
DECIMALS = {}  # Populated from BRIDGED_DECIMALS + /api/v2/tokens

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
        self.chunk_size = 5000  # Rows per upload batch
    
    def _save_local_backup(self, table_name: str, data: List) -> str:
        """Save data to local CSV as backup"""
        import os
        from pathlib import Path
        
        output_dir = Path("./incentiv_data")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = output_dir / f"{table_name}_{timestamp}.csv"
        
        with open(filepath, "w", newline="") as f:
            field_names = [field.name for field in fields(data[0])]
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            for item in data:
                writer.writerow(asdict(item))
        
        print(f"  💾 Saved local backup: {filepath} ({len(data)} rows)")
        return str(filepath)
    
    def _upload_chunk(self, table_name: str, chunk_data: List, chunk_num: int, is_first: bool) -> Dict:
        """Upload a single chunk to Dune"""
        url = f"{self.base_url}/table/upload/csv"
        
        # Convert to CSV string
        output = io.StringIO()
        field_names = [f.name for f in fields(chunk_data[0])]
        writer = csv.DictWriter(output, fieldnames=field_names)
        writer.writeheader()
        for item in chunk_data:
            writer.writerow(asdict(item))
        
        csv_content = output.getvalue()
        
        files = {
            "data": (f"{table_name}.csv", csv_content, "text/csv")
        }
        
        form_data = {
            "table_name": table_name,
            "description": f"Incentiv {table_name} data",
            "is_private": "false"
        }
        
        headers = {
            "X-Dune-Api-Key": self.api_key,
            "Accept-Encoding": "identity"
        }
        
        for attempt in range(3):
            try:
                resp = requests.post(url, files=files, data=form_data, headers=headers, timeout=120)
                
                if resp.status_code == 200:
                    return {"status": "success", "chunk": chunk_num}
                else:
                    print(f"    ⚠️ Chunk {chunk_num} attempt {attempt + 1} failed: {resp.status_code}")
                    time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    ⚠️ Chunk {chunk_num} attempt {attempt + 1} error: {e}")
                time.sleep(2 ** attempt)
        
        return {"status": "failed", "chunk": chunk_num}
    
    def upload_csv(self, table_name: str, data: List, description: str = "") -> Dict:
        """Upload data as CSV with chunking and local backup"""
        if not data:
            print(f"  ⚠️ No data to upload for {table_name}")
            return {"status": "empty"}
        
        # Step 1: Save local backup first
        print(f"\n  📦 Processing {table_name} ({len(data)} rows)...")
        backup_path = self._save_local_backup(table_name, data)
        
        # Step 2: Upload in chunks
        total_chunks = (len(data) + self.chunk_size - 1) // self.chunk_size
        print(f"  📤 Uploading to Dune in {total_chunks} chunk(s)...")
        
        successful = 0
        failed = 0
        
        for i in range(0, len(data), self.chunk_size):
            chunk_num = (i // self.chunk_size) + 1
            chunk = data[i:i + self.chunk_size]
            
            # For subsequent chunks, append to table name to avoid overwrite
            if chunk_num == 1:
                upload_name = table_name
            else:
                upload_name = f"{table_name}_part{chunk_num}"
            
            result = self._upload_chunk(upload_name, chunk, chunk_num, is_first=(chunk_num == 1))
            
            if result["status"] == "success":
                successful += 1
                print(f"    ✅ Chunk {chunk_num}/{total_chunks} uploaded ({len(chunk)} rows)")
            else:
                failed += 1
                print(f"    ❌ Chunk {chunk_num}/{total_chunks} failed")
        
        print(f"  📊 Upload complete: {successful}/{total_chunks} chunks successful")
        
        if failed > 0:
            print(f"  ℹ️ Failed chunks can be manually uploaded from: {backup_path}")
        
        return {"successful": successful, "failed": failed, "backup": backup_path}


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
    
    def get_transactions(self, next_page: Optional[Dict] = None) -> Dict:
        """Fetch all transactions"""
        return self._request("/api/v2/transactions", params=next_page)
    
    def get_block(self, block_number: int) -> Dict:
        """Fetch block details including timestamp"""
        return self._request(f"/api/v2/blocks/{block_number}")
    
    def get_block_timestamps(self, block_numbers: List[int]) -> Dict[int, str]:
        """Fetch timestamps for multiple blocks"""
        timestamps = {}
        unique_blocks = list(set(block_numbers))
        print(f"  ⏰ Fetching timestamps for {len(unique_blocks)} blocks...")
        
        for i, block_num in enumerate(unique_blocks):
            if block_num == 0:
                continue
            try:
                block_data = self.get_block(block_num)
                timestamps[block_num] = block_data.get("timestamp", "")
            except:
                timestamps[block_num] = ""
            
            if (i + 1) % 100 == 0:
                print(f"    📦 Fetched {i + 1}/{len(unique_blocks)} block timestamps...")
        
        print(f"  ✅ Retrieved {len(timestamps)} block timestamps")
        return timestamps
    
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
        self.block_timestamps: Dict[int, str] = {}  # Cache for block timestamps
    
    def _get_timestamp(self, block_number: int) -> str:
        """Get timestamp for a block, fetching if not cached"""
        return self.block_timestamps.get(block_number, "")
    
    def populate_timestamps(self, bridge_transfers: List[BridgeTransfer], dex_swaps: List[DEXSwap]):
        """Fetch and populate timestamps for bridge transfers and DEX swaps"""
        print("\n⏰ Populating timestamps...")
        
        # Collect all unique block numbers
        block_numbers = set()
        for t in bridge_transfers:
            if t.block_number:
                block_numbers.add(t.block_number)
        for s in dex_swaps:
            if s.block_number:
                block_numbers.add(int(s.block_number))
        
        if not block_numbers:
            print("  ℹ️ No blocks to fetch timestamps for")
            return
        
        # Fetch timestamps
        self.block_timestamps = self.api.get_block_timestamps(list(block_numbers))
        
        # Update bridge transfers
        updated_bridge = 0
        for t in bridge_transfers:
            ts = self.block_timestamps.get(t.block_number, "")
            if ts:
                t.timestamp = ts
                updated_bridge += 1
            else:
                print(f"    ⚠️ Missing timestamp for block {t.block_number} (Tx: {t.tx_hash[:10]}...)")
        
        # Update DEX swaps
        updated_swaps = 0
        for s in dex_swaps:
            ts = self.block_timestamps.get(int(s.block_number), "")
            if ts:
                s.timestamp = ts
                updated_swaps += 1
            else:
                print(f"    ⚠️ Missing timestamp for block {s.block_number} (Tx: {s.tx_hash[:10]}...)")
        
        print(f"  ✅ Updated {updated_bridge}/{len(bridge_transfers)} bridge transfers and {updated_swaps}/{len(dex_swaps)} DEX swaps with timestamps")
    
    def extract_bridge_transfers(self, max_pages: Optional[int] = None) -> List[BridgeTransfer]:
        """Extract bridge inflows from all token contracts using RPC"""
        print("\n🌉 Extracting bridge transfers (RPC method)...")
        transfers = []
        
        for symbol, address in TOKENS.items():
            print(f"  📍 Scanning {symbol}...")
            
            # Use efficient RPC filtering
            logs = self.api.get_logs_rpc(address, EVENT_SIGS["ReceivedTransferRemote"])
            if not logs:
                continue
                
            print(f"    Found {len(logs)} bridge events")
            
            for log in logs:
                topics = log.get("topics", [])
                if not topics or topics[0] != EVENT_SIGS["ReceivedTransferRemote"]:
                    continue
                
                # RPC response uses camelCase and hex values
                try:
                    # Parse block number (hex to int)
                    block_number_hex = log.get("blockNumber", "0")
                    block_number = int(block_number_hex, 16) if block_number_hex.startswith("0x") else int(block_number_hex)
                    
                    # Parse origin chain (topic 1)
                    origin_chain = int(topics[1], 16) if len(topics) > 1 and topics[1] else 0
                    
                    # Parse recipient (topic 2)
                    recipient = "0x" + topics[2][-40:] if len(topics) > 2 and topics[2] else ""
                    
                    # Parse amount (data)
                    data_hex = log.get("data", "0")
                    amount_raw = str(int(data_hex, 16))
                    
                    decimals = DECIMALS.get(symbol, 18)
                    try:
                        amount_decimal = int(amount_raw) / (10 ** decimals)
                    except:
                        amount_decimal = 0.0
                    
                    transfer = BridgeTransfer(
                        tx_hash=log.get("transactionHash", ""),
                        block_number=block_number,
                        timestamp="", # Will be populated later
                        direction="inflow",
                        token_symbol=symbol,
                        token_address=address,
                        origin_chain_id=origin_chain,
                        recipient=recipient,
                        amount_raw=amount_raw,
                        amount_decimal=amount_decimal,
                    )
                    transfers.append(transfer)
                    self._track_wallet(recipient, block_number, "", is_token_transfer=True)
                except Exception as e:
                    print(f"    ⚠️ Error parsing log: {e}")
                    continue
        
        print(f"  ✅ Found {len(transfers)} bridge transfers")
        return transfers
    
    def extract_token_transfers(self, max_pages: Optional[int] = None) -> List[TokenTransfer]:
        """Extract all token transfers"""
        print("\n💸 Extracting token transfers...")
        transfers = []
        
        # Build reverse lookup: address -> symbol
        addr_to_symbol = {addr.lower(): sym for sym, addr in TOKENS.items()}
        
        for item in self.api.paginate(
            lambda p: self.api.get_token_transfers(p),
            max_pages=max_pages
        ):
            token_info = item.get("token", {})
            token_address = token_info.get("address_hash", "")
            
            # Look up symbol by address
            token_symbol = addr_to_symbol.get(token_address.lower(), "UNKNOWN")
            
            from_addr = item.get("from", {}).get("hash", "")
            to_addr = item.get("to", {}).get("hash", "")
            
            zero = "0x0000000000000000000000000000000000000000"
            is_mint = from_addr.lower() == zero
            is_burn = to_addr.lower() == zero
            
            total = item.get("total", {})
            amount_raw = total.get("value", "0")
            decimals = int(total.get("decimals", "18"))
            try:
                amount_decimal = int(amount_raw) / (10 ** decimals)
            except:
                amount_decimal = 0.0
            
            transfer = TokenTransfer(
                tx_hash=item.get("transaction_hash", ""),
                block_number=item.get("block_number", 0),
                timestamp=item.get("timestamp", ""),
                token_symbol=token_symbol,
                token_address=token_address,
                from_address=from_addr,
                to_address=to_addr,
                amount_raw=amount_raw,
                amount_decimal=amount_decimal,
                is_mint=is_mint,
                is_burn=is_burn,
            )
            transfers.append(transfer)
            
            if not is_mint:
                self._track_wallet(from_addr, item.get("block_number", 0), item.get("timestamp", ""), is_token_transfer=True)
            if not is_burn:
                self._track_wallet(to_addr, item.get("block_number", 0), item.get("timestamp", ""), is_token_transfer=True)
        
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
            
            self._track_wallet(swap.sender, log.get("block_number", 0), "", is_token_transfer=True)
            self._track_wallet(swap.recipient, log.get("block_number", 0), "", is_token_transfer=True)
        
        print(f"  ✅ Found {len(swaps)} DEX swaps")
        return swaps
    
    def extract_transactions(self, max_pages: Optional[int] = None):
        """Extract all transactions to capture missing wallets"""
        print("\n🧾 Extracting transactions to capture all wallets...")
        tx_count = 0
        
        for tx in self.api.paginate(
            lambda p: self.api.get_transactions(p),
            max_pages=max_pages
        ):
            tx_count += 1
            try:
                from_addr = tx.get("from", {}).get("hash", "")
                
                # Handle "to" address (can be None for contract creation)
                to_data = tx.get("to")
                to_addr = to_data.get("hash", "") if to_data else ""
                
                block = tx.get("block", 0)
                timestamp = tx.get("timestamp", "")
                
                # Track sender (increment tx count)
                if from_addr:
                    self._track_wallet(from_addr, block, timestamp, is_tx=True)
                
                # Track recipient (just seen, not sender)
                if to_addr:
                    self._track_wallet(to_addr, block, timestamp, is_tx=False)
                
                # Track created contract
                created_contract = tx.get("created_contract", {})
                if created_contract:
                    contract_addr = created_contract.get("hash", "")
                    if contract_addr:
                         self._track_wallet(contract_addr, block, timestamp, is_tx=False)

            except Exception as e:
                # Log error but don't crash
                # print(f"    ⚠️ Error processing tx: {e}") 
                pass
                
        print(f"  ✅ Processed {tx_count} transactions")

    def _track_wallet(self, address: str, block: int, timestamp: str, is_token_transfer: bool = False, is_tx: bool = False):
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
                tx_count=1 if is_tx else 0,
                token_transfer_count=1 if is_token_transfer else 0,
            )
        else:
            w = self.wallets[address]
            w.last_seen_block = max(w.last_seen_block, block)
            w.last_seen_timestamp = timestamp or w.last_seen_timestamp
            if is_token_transfer:
                w.token_transfer_count += 1
            if is_tx:
                w.tx_count += 1
    
    def get_wallets(self) -> List[ActiveWallet]:
        return list(self.wallets.values())
    
    def include_known_contracts(self, tokens: Dict[str, str], dex_pool: str):
        """Add known contracts (tokens, pools) to wallet list"""
        print(f"\n🏭 Adding {len(tokens) + 1} known contracts to wallet list...")
        
        # Add DEX Pool
        self._track_wallet(dex_pool, 0, "", is_tx=False)
        
        # Add Tokens
        for symbol, address in tokens.items():
             self._track_wallet(address, 0, "", is_tx=False)

    def save_local_only(self, bridge: List, transfers: List, swaps: List, wallets: List):
        """Save all data to local CSV files only"""
        if bridge:
            self.dune._save_local_backup("incentiv_bridge_transfers", bridge)
        if transfers:
            self.dune._save_local_backup("incentiv_token_transfers", transfers)
        if swaps:
            self.dune._save_local_backup("incentiv_dex_swaps_v2", swaps)
        if wallets:
            self.dune._save_local_backup("incentiv_active_wallets_v2", wallets)
    
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
            self.dune.upload_csv("incentiv_dex_swaps_v2", swaps,
                               "Incentiv DEX swap events")
        
        if wallets:
            self.dune.upload_csv("incentiv_active_wallets_v2", wallets,
                               "Incentiv unique active wallets")


# =============================================================================
# MAIN
# =============================================================================

def main():
    global TOKENS, DECIMALS
    
    parser = argparse.ArgumentParser(description="Incentiv ETL → Dune")
    parser.add_argument("--full", action="store_true", help="Full extraction (use --pages to limit)")
    parser.add_argument("--incremental", action="store_true", help="Last 10 pages only")
    parser.add_argument("--test", action="store_true", help="Small sample (2 pages)")
    parser.add_argument("--pages", type=int, default=100, help="Max pages per run (default: 100)")
    parser.add_argument("--local-only", action="store_true", help="Save to CSV only, skip Dune upload")
    parser.add_argument("--bridge-only", action="store_true", help="Only extract bridge transfers")
    parser.add_argument("--transactions", action="store_true", help="Extract all transactions to fix wallet counts")
    args = parser.parse_args()
    
    if args.test:
        max_pages = 2
        print("🧪 TEST MODE (2 pages)")
    elif args.incremental:
        max_pages = 10
        print("⏱️ INCREMENTAL MODE (10 pages)")
    elif args.full:
        max_pages = None
        print("🚀 FULL EXTRACTION MODE (Unlimited pages)")
    else:
        max_pages = args.pages
        print(f"📊 EXTRACTION MODE ({max_pages} pages max)")
    
    # Initialize clients
    blockscout = BlockscoutAPI()
    dune = DuneClient(DUNE_API_KEY)
    
    # Fetch tokens dynamically
    print("\n🪙 Fetching token list...")
    
    # Start with hardcoded bridged tokens
    TOKENS.update(BRIDGED_TOKENS)
    DECIMALS.update(BRIDGED_DECIMALS)
    print(f"  📌 Added {len(BRIDGED_TOKENS)} bridged tokens (USDC, USDT, WETH, SOL, WBTC)")
    
    # Add dynamic tokens from API
    token_list = blockscout.get_tokens()
    for t in token_list:
        symbol = t.get("symbol", "UNKNOWN")
        address = t.get("address_hash", "")
        decimals = int(t.get("decimals", "18") or "18")
        if symbol not in TOKENS:  # Don't override bridged tokens
            TOKENS[symbol] = address
            DECIMALS[symbol] = decimals
            print(f"  • {symbol}: {address[:10]}... ({decimals} decimals)")
    
    print(f"  ✅ Found {len(TOKENS)} tokens")
    
    etl = IncentivETL(blockscout, dune)
    
    # Get chain stats
    print("\n📈 Chain Statistics...")
    stats = blockscout.get_stats()
    print(f"  Blocks: {stats.get('total_blocks', 'N/A')}")
    print(f"  Transactions: {stats.get('total_transactions', 'N/A')}")
    print(f"  Addresses: {stats.get('total_addresses', 'N/A')}")
    
    # Extract data
    bridge_transfers = etl.extract_bridge_transfers(max_pages)
    
    if args.bridge_only:
        token_transfers = []
        dex_swaps = []
        active_wallets = [] # We still track wallets from bridge transfers but won't export full list if requested? 
        # Actually, extract_bridge_transfers tracks wallets.
        # But if we want strictly bridge only, maybe we don't care about token/dex/wallets.
        # Let's keep it simple: empty lists for others.
    else:
        token_transfers = etl.extract_token_transfers(max_pages)
        dex_swaps = etl.extract_dex_swaps(max_pages)
        
        # Extract transactions if requested or in full mode (but maybe optional in full if too slow?)
        # Let's make it explicit or part of full if manageable.
        if args.transactions or args.full:
             etl.extract_transactions(max_pages)
    
    # Populate timestamps for bridge and DEX data
    etl.populate_timestamps(bridge_transfers, dex_swaps)
    
    if not args.bridge_only:
        etl.include_known_contracts(TOKENS, DEX_POOL)
        active_wallets = etl.get_wallets()
    else:
        active_wallets = [] # Skip wallet upload for bridge only run to save time/confusion
    
    # Push to Dune (or local only)
    if args.local_only:
        print("\n💾 Saving to local CSV only (--local-only mode)...")
        etl.save_local_only(bridge_transfers, token_transfers, dex_swaps, active_wallets)
    else:
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
║  Tables in Dune:                                             ║
║    • incentiv_bridge_transfers                               ║
║    • incentiv_token_transfers                                ║
║    • incentiv_dex_swaps_v2                                   ║
║    • incentiv_active_wallets_v2                              ║
╠══════════════════════════════════════════════════════════════╣
║  Local backups saved in: ./incentiv_data/                    ║
╚══════════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    main()