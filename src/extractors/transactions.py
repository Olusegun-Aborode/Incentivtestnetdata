from typing import Any, Dict, Iterable, List
from src.extractors.blockscout import BlockscoutExtractor

class TransactionsExtractor:
    def __init__(self, blockscout: BlockscoutExtractor) -> None:
        self.blockscout = blockscout

    def get_blocks_with_transactions(self, from_block: int, to_block: int) -> List[Dict[str, Any]]:
        block_numbers = list(range(from_block, to_block + 1))
        # Batch size for this is handled inside get_blocks_by_number
        blocks_map = self.blockscout.get_blocks_by_number(block_numbers, include_transactions=True)
        return [blocks_map[number] for number in block_numbers if number in blocks_map]

    def get_transaction_receipts(self, tx_hashes: Iterable[str], batch_size: int = 50) -> Dict[str, Dict[str, Any]]:
        tx_hashes_list = list(tx_hashes)
        receipts: Dict[str, Dict[str, Any]] = {}
        for start in range(0, len(tx_hashes_list), batch_size):
            chunk = tx_hashes_list[start : start + batch_size]
            payloads = [
                {
                    "id": tx_hash,
                    "jsonrpc": "2.0",
                    "method": "eth_getTransactionReceipt",
                    "params": [tx_hash],
                }
                for tx_hash in chunk
            ]
            responses = self.blockscout.rpc_client.post_batch("", payloads)
            for response in responses:
                receipt = response.get("result")
                if receipt and receipt.get("transactionHash"):
                    receipts[receipt["transactionHash"].lower()] = receipt
        return receipts
