"""
Incentiv Blockchain ETL Pipeline
Extracts: Bridge Inflows/Outflows, DEX Swaps, Token Transfers, Active Wallets
Outputs: CSV files ready for Dune Analytics upload

Usage:
    python incentiv_etl.py --full          # Full historical extraction
    python incentiv_etl.py --incremental   # Last 24 hours only
    python incentiv_etl.py --test          # Test with small sample
"""

import requests
import json
import csv
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Generator
from dataclasses import dataclass, asdict
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = "https://explorer.incentiv.io"

# Token contracts (all are Hyperlane bridged tokens)
TOKENS = {
    "USDC": "0x16e43840d8D79896A389a3De85aB0B0210C05685",
    "USDT": "0x39b076b5d23F588690D480af3Bf820edad31a4bB",
    "WETH": "0x3e425317dB7BaC8077093117081b40d9b46F29cb",
    "SOL": "0xfaC24134dbc4b00Ee11114eCDFE6397f389203E3",
    "WBTC": "0x0292593D416Cb765E0e8FF77b32fA7e465958FEE",
}

# Token decimals
DECIMALS = {
    "USDC": 6,
    "USDT": 6,
    "WETH": 18,
    "SOL": 9,
    "WBTC": 8,
}

# Key infrastructure contracts
CONTRACTS = {
    "router": "0x4a66A8bA9704DD06fE52A027f2B16a3F5D11B048",
    "paymaster": "0x43000f785EB43BcB4961C5c70276eD00e088972c",
    "entrypoint": "0x3eC61c5633BBD7Afa9144C6610930489736a72d4",
}

# Event signatures (topic0)
EVENT_SIGS = {
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
    "Swap": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    "ReceivedTransferRemote": "0xba20947a325f450d232530e5f5fce293e7963499d5309a07cee84a269f2f15a6",
    "SentTransferRemote": "0xd229aacb94204188f2e40a67c6b20b7cd5c8e1f8e2dce6e1a3d2c3c4c3e5d6a7",  # Need to verify
    "Process": "0x0d381c2a574ae8f04e213db7cfb4df8df712cdbd427d9868ffef380660ca6574",
}

# Rate limiting
REQUEST_DELAY = 0.2  # seconds between requests
MAX_RETRIES = 3

# Output directory
OUTPUT_DIR = Path("./incentiv_data")


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class BridgeTransfer:
    tx_hash: str
    block_number: int
    timestamp: str
    direction: str  # 'inflow' or 'outflow'
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
    token0_address: str
    token1_address: str
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
# API CLIENT
# =============================================================================

class IncentivAPI:
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "IncentivETL/1.0"
        })
    
    def _request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make API request with retry logic"""
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(REQUEST_DELAY)
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                print(f"  ⚠️ Request failed (attempt {attempt + 1}): {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return {}
    
    def get_stats(self) -> Dict:
        """Get chain statistics"""
        return self._request("/api/v2/stats")
    
    def get_address_logs(self, address: str, next_page_params: Optional[Dict] = None) -> Dict:
        """Get logs for a specific address with pagination"""
        endpoint = f"/api/v2/addresses/{address}/logs"
        return self._request(endpoint, params=next_page_params)
    
    def get_token_transfers(self, next_page_params: Optional[Dict] = None) -> Dict:
        """Get all token transfers with pagination"""
        return self._request("/api/v2/token-transfers", params=next_page_params)
    
    def get_transactions(self, next_page_params: Optional[Dict] = None) -> Dict:
        """Get transactions with pagination"""
        return self._request("/api/v2/transactions", params=next_page_params)
    
    def get_address_transactions(self, address: str, next_page_params: Optional[Dict] = None) -> Dict:
        """Get transactions for specific address"""
        endpoint = f"/api/v2/addresses/{address}/transactions"
        return self._request(endpoint, params=next_page_params)
    
    def paginate(self, fetch_func, max_pages: Optional[int] = None) -> Generator[Dict, None, None]:
        """Generator that handles pagination automatically"""
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
                print(f"  ℹ️ Reached max pages limit ({max_pages})")
                break
            
            print(f"  📄 Page {page_count} complete, {len(items)} items")


# =============================================================================
# DATA EXTRACTION
# =============================================================================

class IncentivETL:
    def __init__(self, api: IncentivAPI):
        self.api = api
        self.wallets: Dict[str, ActiveWallet] = {}
        
    def extract_bridge_transfers(self, token_address: str, token_symbol: str, 
                                  max_pages: Optional[int] = None) -> List[BridgeTransfer]:
        """Extract bridge inflows (ReceivedTransferRemote events)"""
        print(f"\n🌉 Extracting bridge transfers for {token_symbol}...")
        
        transfers = []
        
        for log in self.api.paginate(
            lambda p: self.api.get_address_logs(token_address, p),
            max_pages=max_pages
        ):
            topics = log.get("topics", [])
            if not topics:
                continue
            
            topic0 = topics[0]
            
            # ReceivedTransferRemote = bridge inflow
            if topic0 == EVENT_SIGS["ReceivedTransferRemote"]:
                decoded = log.get("decoded", {})
                params = {p["name"]: p["value"] for p in decoded.get("parameters", [])}
                
                # Parse origin chain from topic1 (uint32)
                origin_chain = int(topics[1], 16) if len(topics) > 1 else 0
                
                # Parse recipient from topic2
                recipient = "0x" + topics[2][-40:] if len(topics) > 2 else ""
                
                # Amount from decoded params or data field
                amount_raw = params.get("amount", "0")
                if isinstance(amount_raw, str) and amount_raw.startswith("0x"):
                    amount_raw = str(int(amount_raw, 16))
                
                decimals = DECIMALS.get(token_symbol, 18)
                amount_decimal = int(amount_raw) / (10 ** decimals)
                
                transfer = BridgeTransfer(
                    tx_hash=log.get("transaction_hash", ""),
                    block_number=log.get("block_number", 0),
                    timestamp=self._get_timestamp_from_block(log.get("block_number", 0)),
                    direction="inflow",
                    token_symbol=token_symbol,
                    token_address=token_address,
                    origin_chain_id=origin_chain,
                    recipient=recipient,
                    amount_raw=amount_raw,
                    amount_decimal=amount_decimal,
                )
                transfers.append(transfer)
                self._track_wallet(recipient, log.get("block_number", 0), "")
        
        print(f"  ✅ Found {len(transfers)} bridge inflows")
        return transfers
    
    def extract_token_transfers(self, max_pages: Optional[int] = None) -> List[TokenTransfer]:
        """Extract all token transfers"""
        print(f"\n💸 Extracting token transfers...")
        
        transfers = []
        
        for item in self.api.paginate(
            lambda p: self.api.get_token_transfers(p),
            max_pages=max_pages
        ):
            token_info = item.get("token", {})
            token_address = token_info.get("address_hash", "")
            
            # Find token symbol
            token_symbol = "UNKNOWN"
            for symbol, addr in TOKENS.items():
                if addr.lower() == token_address.lower():
                    token_symbol = symbol
                    break
            
            from_addr = item.get("from", {}).get("hash", "")
            to_addr = item.get("to", {}).get("hash", "")
            
            # Check if mint (from zero address) or burn (to zero address)
            zero_addr = "0x0000000000000000000000000000000000000000"
            is_mint = from_addr.lower() == zero_addr.lower()
            is_burn = to_addr.lower() == zero_addr.lower()
            
            total_info = item.get("total", {})
            amount_raw = total_info.get("value", "0")
            decimals = int(total_info.get("decimals", "18"))
            amount_decimal = int(amount_raw) / (10 ** decimals)
            
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
            
            # Track wallets
            if not is_mint:
                self._track_wallet(from_addr, item.get("block_number", 0), item.get("timestamp", ""))
            if not is_burn:
                self._track_wallet(to_addr, item.get("block_number", 0), item.get("timestamp", ""))
        
        print(f"  ✅ Found {len(transfers)} token transfers")
        return transfers
    
    def extract_dex_swaps(self, pool_addresses: List[str], 
                          max_pages: Optional[int] = None) -> List[DEXSwap]:
        """Extract DEX swap events from pool contracts"""
        print(f"\n🔄 Extracting DEX swaps...")
        
        swaps = []
        
        for pool_addr in pool_addresses:
            print(f"  📍 Scanning pool {pool_addr[:10]}...")
            
            for log in self.api.paginate(
                lambda p: self.api.get_address_logs(pool_addr, p),
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
                    timestamp=self._get_timestamp_from_block(log.get("block_number", 0)),
                    pool_address=pool_addr,
                    sender=params.get("sender", ""),
                    recipient=params.get("recipient", ""),
                    token0_address="",  # Would need pool contract read
                    token1_address="",
                    amount0=str(params.get("amount0", "0")),
                    amount1=str(params.get("amount1", "0")),
                    sqrt_price_x96=str(params.get("sqrtPriceX96", "0")),
                    liquidity=str(params.get("liquidity", "0")),
                    tick=int(params.get("tick", 0)),
                )
                swaps.append(swap)
                
                # Track wallets
                self._track_wallet(swap.sender, log.get("block_number", 0), "")
                self._track_wallet(swap.recipient, log.get("block_number", 0), "")
        
        print(f"  ✅ Found {len(swaps)} DEX swaps")
        return swaps
    
    def _track_wallet(self, address: str, block_number: int, timestamp: str):
        """Track unique wallets"""
        if not address or address == "0x0000000000000000000000000000000000000000":
            return
            
        address = address.lower()
        
        if address not in self.wallets:
            self.wallets[address] = ActiveWallet(
                address=address,
                first_seen_block=block_number,
                first_seen_timestamp=timestamp,
                last_seen_block=block_number,
                last_seen_timestamp=timestamp,
                tx_count=0,
                token_transfer_count=1,
            )
        else:
            wallet = self.wallets[address]
            wallet.last_seen_block = max(wallet.last_seen_block, block_number)
            wallet.last_seen_timestamp = timestamp or wallet.last_seen_timestamp
            wallet.token_transfer_count += 1
    
    def _get_timestamp_from_block(self, block_number: int) -> str:
        """Get timestamp for a block (simplified - could be enhanced with caching)"""
        # For now, return empty - could implement block timestamp lookup
        return ""
    
    def get_active_wallets(self) -> List[ActiveWallet]:
        """Return tracked wallets"""
        return list(self.wallets.values())


# =============================================================================
# OUTPUT
# =============================================================================

def write_csv(data: List, filename: str, output_dir: Path):
    """Write dataclass list to CSV"""
    if not data:
        print(f"  ⚠️ No data to write for {filename}")
        return
    
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename
    
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=asdict(data[0]).keys())
        writer.writeheader()
        for item in data:
            writer.writerow(asdict(item))
    
    print(f"  💾 Wrote {len(data)} rows to {filepath}")


def write_json(data: List, filename: str, output_dir: Path):
    """Write dataclass list to JSON"""
    if not data:
        return
    
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename
    
    with open(filepath, "w") as f:
        json.dump([asdict(item) for item in data], f, indent=2)
    
    print(f"  💾 Wrote {len(data)} records to {filepath}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Incentiv Blockchain ETL")
    parser.add_argument("--full", action="store_true", help="Full historical extraction")
    parser.add_argument("--incremental", action="store_true", help="Last 24h only")
    parser.add_argument("--test", action="store_true", help="Test with small sample")
    parser.add_argument("--output", type=str, default="./incentiv_data", help="Output directory")
    args = parser.parse_args()
    
    # Set page limits based on mode
    if args.test:
        max_pages = 2
        print("🧪 Running in TEST mode (2 pages max)")
    elif args.incremental:
        max_pages = 10
        print("⏱️ Running in INCREMENTAL mode (10 pages max)")
    else:
        max_pages = None
        print("📊 Running in FULL mode (all data)")
    
    output_dir = Path(args.output)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Initialize
    api = IncentivAPI()
    etl = IncentivETL(api)
    
    # Get chain stats
    print("\n📈 Fetching chain statistics...")
    stats = api.get_stats()
    print(f"  Total Blocks: {stats.get('total_blocks', 'N/A')}")
    print(f"  Total Transactions: {stats.get('total_transactions', 'N/A')}")
    print(f"  Total Addresses: {stats.get('total_addresses', 'N/A')}")
    
    # Extract bridge transfers for each token
    all_bridge_transfers = []
    for symbol, address in TOKENS.items():
        transfers = etl.extract_bridge_transfers(address, symbol, max_pages=max_pages)
        all_bridge_transfers.extend(transfers)
    
    # Extract token transfers
    token_transfers = etl.extract_token_transfers(max_pages=max_pages)
    
    # Extract DEX swaps (need to identify pool addresses first)
    # For now, we'll scan the router contract logs
    # dex_swaps = etl.extract_dex_swaps([CONTRACTS["router"]], max_pages=max_pages)
    
    # Get active wallets
    active_wallets = etl.get_active_wallets()
    
    # Write outputs
    print("\n💾 Writing output files...")
    write_csv(all_bridge_transfers, f"bridge_transfers_{timestamp}.csv", output_dir)
    write_csv(token_transfers, f"token_transfers_{timestamp}.csv", output_dir)
    # write_csv(dex_swaps, f"dex_swaps_{timestamp}.csv", output_dir)
    write_csv(active_wallets, f"active_wallets_{timestamp}.csv", output_dir)
    
    # Also write JSON for flexibility
    write_json(all_bridge_transfers, f"bridge_transfers_{timestamp}.json", output_dir)
    write_json(token_transfers, f"token_transfers_{timestamp}.json", output_dir)
    write_json(active_wallets, f"active_wallets_{timestamp}.json", output_dir)
    
    # Summary
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    ETL COMPLETE                              ║
╠══════════════════════════════════════════════════════════════╣
║  Bridge Transfers:  {len(all_bridge_transfers):>8}                              ║
║  Token Transfers:   {len(token_transfers):>8}                              ║
║  Active Wallets:    {len(active_wallets):>8}                              ║
║                                                              ║
║  Output Directory: {str(output_dir):<40} ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    print("\n📤 Next steps for Dune upload:")
    print("   1. Go to dune.com → My Creations → Upload Data")
    print("   2. Upload the CSV files")
    print("   3. Create queries joining with on-chain data")


if __name__ == "__main__":
    main()
