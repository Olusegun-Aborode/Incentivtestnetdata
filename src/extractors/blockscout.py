import requests
import time
from typing import Dict, List, Optional, Generator
from dataclasses import dataclass

# Constant Event Signatures (Moving global constants here)
EVENT_SIGS = {
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Swap": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    "ReceivedTransferRemote": "0xba20947a325f450d232530e5f5fce293e7963499d5309a07cee84a269f2f15a6",
}

# Data Classes
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

class BlockscoutAPI:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.max_retries = 3
        self.request_delay = 0.2
    
    def _request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.request_delay)
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                print(f"  ⚠️ Request failed (attempt {attempt + 1}): {e}")
                if attempt == self.max_retries - 1:
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
        return self._request("/api/v2/transactions", params=next_page)
        
    def get_block(self, block_number: int) -> Dict:
        return self._request(f"/api/v2/blocks/{block_number}")
    
    def get_block_timestamps(self, block_numbers: List[int]) -> Dict[int, str]:
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


class IncentivETL:
    def __init__(self, blockscout: BlockscoutAPI, dune_client):
        self.api = blockscout
        self.dune = dune_client
        self.wallets: Dict[str, ActiveWallet] = {}
        self.block_timestamps: Dict[int, str] = {}
        
        # Will be populated by pipeline
        self.tokens = {}
        self.decimals = {}
        self.dex_pool = ""
    
    def set_config(self, tokens: Dict, decimals: Dict, dex_pool: str):
        self.tokens = tokens
        self.decimals = decimals
        self.dex_pool = dex_pool

    def populate_timestamps(self, bridge_transfers: List[BridgeTransfer], dex_swaps: List[DEXSwap]):
        """Fetch and populate timestamps for bridge transfers and DEX swaps"""
        print("\n⏰ Populating timestamps...")
        
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
        
        self.block_timestamps = self.api.get_block_timestamps(list(block_numbers))
        
        updated_bridge = 0
        for t in bridge_transfers:
            ts = self.block_timestamps.get(t.block_number, "")
            if ts:
                t.timestamp = ts
                updated_bridge += 1
        
        updated_swaps = 0
        for s in dex_swaps:
            ts = self.block_timestamps.get(int(s.block_number), "")
            if ts:
                s.timestamp = ts
                updated_swaps += 1
        
        print(f"  ✅ Updated {updated_bridge}/{len(bridge_transfers)} bridge transfers and {updated_swaps}/{len(dex_swaps)} DEX swaps")
    
    def extract_bridge_transfers(self, max_pages: Optional[int] = None) -> List[BridgeTransfer]:
        print("\n🌉 Extracting bridge transfers (RPC method)...")
        transfers = []
        
        for symbol, address in self.tokens.items():
            print(f"  📍 Scanning {symbol}...")
            
            logs = self.api.get_logs_rpc(address, EVENT_SIGS["ReceivedTransferRemote"])
            if not logs:
                continue
                
            print(f"    Found {len(logs)} bridge events")
            
            for log in logs:
                topics = log.get("topics", [])
                if not topics or topics[0] != EVENT_SIGS["ReceivedTransferRemote"]:
                    continue
                
                try:
                    block_number_hex = log.get("blockNumber", "0")
                    block_number = int(block_number_hex, 16) if block_number_hex.startswith("0x") else int(block_number_hex)
                    
                    origin_chain = int(topics[1], 16) if len(topics) > 1 and topics[1] else 0
                    recipient = "0x" + topics[2][-40:] if len(topics) > 2 and topics[2] else ""
                    
                    data_hex = log.get("data", "0")
                    amount_raw = str(int(data_hex, 16))
                    
                    decimals = self.decimals.get(symbol, 18)
                    try:
                        amount_decimal = int(amount_raw) / (10 ** decimals)
                    except:
                        amount_decimal = 0.0
                    
                    transfer = BridgeTransfer(
                        tx_hash=log.get("transactionHash", ""),
                        block_number=block_number,
                        timestamp="",
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
        print("\n💸 Extracting token transfers...")
        transfers = []
        
        addr_to_symbol = {addr.lower(): sym for sym, addr in self.tokens.items()}
        
        for item in self.api.paginate(
            lambda p: self.api.get_token_transfers(p),
            max_pages=max_pages
        ):
            token_info = item.get("token", {})
            token_address = token_info.get("address_hash", "")
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
        print("\n🔄 Extracting DEX swaps...")
        swaps = []
        
        for log in self.api.paginate(
            lambda p: self.api.get_address_logs(self.dex_pool, p),
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
                pool_address=self.dex_pool,
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
        print("\n🧾 Extracting transactions to capture all wallets...")
        tx_count = 0
        
        for tx in self.api.paginate(
            lambda p: self.api.get_transactions(p),
            max_pages=max_pages
        ):
            tx_count += 1
            try:
                from_addr = tx.get("from", {}).get("hash", "")
                to_data = tx.get("to")
                to_addr = to_data.get("hash", "") if to_data else ""
                block = tx.get("block", 0)
                timestamp = tx.get("timestamp", "")
                
                if from_addr:
                    self._track_wallet(from_addr, block, timestamp, is_tx=True)
                if to_addr:
                    self._track_wallet(to_addr, block, timestamp, is_tx=False)
                
                created_contract = tx.get("created_contract", {})
                if created_contract:
                     self._track_wallet(created_contract.get("hash", ""), block, timestamp, is_tx=False)
            except Exception:
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
        print(f"\n🏭 Adding {len(tokens) + 1} known contracts to wallet list...")
        self._track_wallet(dex_pool, 0, "", is_tx=False)
        for symbol, address in tokens.items():
             self._track_wallet(address, 0, "", is_tx=False)

    def push_to_dune(self, bridge: List, transfers: List, swaps: List, wallets: List):
        print("\n📤 Pushing data to Dune...")
        if bridge:
            self.dune.upload_csv("incentiv_bridge_transfers", bridge, "Incentiv bridge inflows via Hyperlane")
        if transfers:
            self.dune.upload_csv("incentiv_token_transfers", transfers, "Incentiv ERC20 token transfers")
        if swaps:
            self.dune.upload_csv("incentiv_dex_swaps_v2", swaps, "Incentiv DEX swap events")
        if wallets:
            self.dune.upload_csv("incentiv_active_wallets_v2", wallets, "Incentiv unique active wallets")
