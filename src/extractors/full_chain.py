"""
Full chain extractor — orchestrates capturing ALL on-chain activity
(blocks, transactions, and unfiltered logs) for the Incentiv blockchain.
"""

import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.extractors.blockscout import BlockscoutExtractor


class FullChainExtractor:
    """
    High-level orchestrator for extracting complete blockchain data.
    Wraps BlockscoutExtractor with full-chain-specific logic.
    """

    def __init__(self, extractor: BlockscoutExtractor) -> None:
        self.extractor = extractor

    def extract_block_range(
        self,
        from_block: int,
        to_block: int,
        include_transactions: bool = True,
        progress_callback: Optional[Callable] = None,
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Extract blocks and their transactions for a range.
        Returns (blocks_list, transactions_list).
        """
        blocks_list = []
        transactions_list = []

        block_numbers = list(range(from_block, to_block + 1))
        blocks_map = self.extractor.get_blocks_by_number(
            block_numbers, include_transactions=include_transactions
        )

        for block_num in sorted(blocks_map.keys()):
            block = blocks_map[block_num]
            blocks_list.append(block)

            if include_transactions and isinstance(block.get("transactions"), list):
                for tx in block["transactions"]:
                    if isinstance(tx, dict):
                        # Enrich tx with block timestamp
                        tx["block_timestamp"] = block.get("timestamp")
                        transactions_list.append(tx)

        if progress_callback:
            progress_callback(len(blocks_list), len(transactions_list))

        return blocks_list, transactions_list

    def extract_all_logs(
        self,
        from_block: int,
        to_block: int,
        blocks_map: Optional[Dict[int, Dict]] = None,
        progress_callback: Optional[Callable] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract ALL event logs (no address/topic filter) for a block range.
        Enriches logs with block timestamps if blocks_map is provided.
        """
        logs = self.extractor.get_all_logs(from_block, to_block)

        if logs and blocks_map:
            self._enrich_logs_with_timestamps(logs, blocks_map)
        elif logs and not blocks_map:
            # Fetch timestamps for log blocks
            block_numbers = sorted(set(int(log["blockNumber"], 16) for log in logs))
            fetched_blocks = self.extractor.get_blocks_by_number(block_numbers)
            self._enrich_logs_with_timestamps(logs, fetched_blocks)

        if progress_callback:
            progress_callback(len(logs))

        return logs

    def extract_full_batch(
        self,
        from_block: int,
        to_block: int,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Extract everything for a block range: blocks, transactions, and all logs.

        Returns:
            {
                "blocks": [...],
                "transactions": [...],
                "logs": [...],
                "block_range": (from_block, to_block),
                "extracted_at": datetime
            }
        """
        start_time = time.time()

        # Step 1: Extract blocks with transactions
        blocks_list, transactions_list = self.extract_block_range(
            from_block, to_block, include_transactions=True
        )

        # Build blocks map for timestamp enrichment
        blocks_map = {}
        for block in blocks_list:
            block_num = int(block["number"], 16)
            blocks_map[block_num] = block

        # Step 2: Extract all logs (unfiltered) from transaction receipts
        tx_hashes = [tx["hash"] for tx in transactions_list]
        receipts_map = self.extractor.get_transaction_receipts_parallel(tx_hashes)
        
        logs = []
        for tx_hash in tx_hashes:
            receipt = receipts_map.get(tx_hash.lower())
            if not receipt:
                continue
                
            tx_logs = receipt.get("logs", [])
            for log in tx_logs:
                block_number_hex = log.get("blockNumber", "0x0")
                block_number = int(block_number_hex, 16) if block_number_hex else 0
                
                block = blocks_map.get(block_number)
                if block:
                    log["block_timestamp"] = datetime.utcfromtimestamp(
                        int(block["timestamp"], 16)
                    )
                else:
                    log["block_timestamp"] = datetime.utcfromtimestamp(0)
                logs.append(log)

        elapsed = time.time() - start_time

        result = {
            "blocks": blocks_list,
            "transactions": transactions_list,
            "logs": logs,
            "block_range": (from_block, to_block),
            "extracted_at": datetime.utcnow(),
            "elapsed_seconds": elapsed,
        }

        if progress_callback:
            progress_callback(result)

        return result

    def discover_contracts(self, logs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Extract unique contract addresses from logs with metadata.
        Returns {address: {first_seen_block, last_activity_block, event_count}}.
        """
        contracts: Dict[str, Dict[str, Any]] = {}

        for log in logs:
            address = log.get("address", "").lower()
            if not address:
                continue

            block_num = int(log["blockNumber"], 16)

            if address not in contracts:
                contracts[address] = {
                    "address": address,
                    "first_seen_block": block_num,
                    "last_activity_block": block_num,
                    "event_count": 1,
                }
            else:
                contracts[address]["last_activity_block"] = max(
                    contracts[address]["last_activity_block"], block_num
                )
                contracts[address]["event_count"] += 1

        return contracts

    @staticmethod
    def _enrich_logs_with_timestamps(
        logs: List[Dict[str, Any]], blocks_map: Dict[int, Dict]
    ) -> None:
        """Add block_timestamp to each log from the blocks map."""
        for log in logs:
            block_number = int(log["blockNumber"], 16)
            block = blocks_map.get(block_number)
            if block:
                log["block_timestamp"] = datetime.utcfromtimestamp(
                    int(block["timestamp"], 16)
                )
            else:
                log["block_timestamp"] = datetime.utcfromtimestamp(0)
