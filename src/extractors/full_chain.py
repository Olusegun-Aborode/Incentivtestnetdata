"""
Full chain extractor — orchestrates capturing ALL on-chain activity
(blocks, transactions, and unfiltered logs) for the Incentiv blockchain.

v2: Uses REST v2 API as primary path (more reliable, richer data).
    Falls back to RPC if REST v2 fails.
"""

import concurrent.futures
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.extractors.blockscout import BlockscoutExtractor


class FullChainExtractor:
    """
    High-level orchestrator for extracting complete blockchain data.
    Wraps BlockscoutExtractor with full-chain-specific logic.

    v2: Primary path uses REST v2 API for reliability.
    """

    def __init__(self, extractor: BlockscoutExtractor) -> None:
        self.extractor = extractor

    def extract_full_batch(
        self,
        from_block: int,
        to_block: int,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Extract everything for a block range: blocks, transactions, and all logs.

        Strategy (REST v2):
          1. Fetch blocks via REST v2 (parallelized)
          2. Fetch transactions per block via REST v2 (parallelized)
          3. Fetch logs per transaction via REST v2 (parallelized)
          4. All data returned in RPC-compatible format for existing transformers

        Returns:
            {
                "blocks": [...],
                "transactions": [...],
                "logs": [...],
                "block_range": (from_block, to_block),
                "extracted_at": datetime,
                "elapsed_seconds": float,
            }
        """
        start_time = time.time()

        try:
            result = self._extract_via_rest_v2(from_block, to_block)
        except Exception as e:
            print(f"  [REST v2] Failed, falling back to RPC: {e}")
            result = self._extract_via_rpc(from_block, to_block)

        result["block_range"] = (from_block, to_block)
        result["extracted_at"] = datetime.utcnow()
        result["elapsed_seconds"] = time.time() - start_time

        if progress_callback:
            progress_callback(result)

        return result

    def _extract_via_rest_v2(
        self, from_block: int, to_block: int
    ) -> Dict[str, Any]:
        """
        Primary extraction path using REST v2 API.
        More reliable than RPC — no 413 errors, no batch JSON-RPC issues.
        """
        # Step 1: Fetch all blocks in parallel
        blocks_map = self.extractor.get_blocks_rest(from_block, to_block)
        blocks_list = [blocks_map[bn] for bn in sorted(blocks_map.keys())]

        # Step 2: Fetch transactions for each block in parallel
        transactions_list = []
        block_numbers_with_data = sorted(blocks_map.keys())

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            future_to_block = {
                executor.submit(
                    self.extractor.get_block_transactions_rest, bn
                ): bn
                for bn in block_numbers_with_data
            }
            block_txs = {}
            for future in concurrent.futures.as_completed(future_to_block):
                bn = future_to_block[future]
                try:
                    txs = future.result()
                    block_txs[bn] = txs
                except Exception as e:
                    print(f"  [REST v2] Failed to get txs for block {bn}: {e}")
                    block_txs[bn] = []

        # Assemble transactions in block order and attach to blocks
        for bn in sorted(block_txs.keys()):
            txs = block_txs[bn]
            block = blocks_map.get(bn)
            if block:
                block["transactions"] = txs
            for tx in txs:
                # Enrich tx with block timestamp
                tx["block_timestamp"] = block.get("timestamp") if block else "0x0"
                transactions_list.append(tx)

        # Step 3: Fetch logs for each transaction in parallel
        logs = []
        tx_hashes = [tx["hash"] for tx in transactions_list if tx.get("hash")]

        if tx_hashes:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                future_to_hash = {
                    executor.submit(
                        self.extractor.get_transaction_logs_rest, tx_hash
                    ): tx_hash
                    for tx_hash in tx_hashes
                }
                tx_logs_map = {}
                for future in concurrent.futures.as_completed(future_to_hash):
                    tx_hash = future_to_hash[future]
                    try:
                        tx_logs = future.result()
                        tx_logs_map[tx_hash] = tx_logs
                    except Exception as e:
                        print(f"  [REST v2] Failed to get logs for tx {tx_hash[:16]}...: {e}")
                        tx_logs_map[tx_hash] = []

            # Enrich logs with block timestamps
            for tx_hash in tx_hashes:
                for log in tx_logs_map.get(tx_hash, []):
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

        return {
            "blocks": blocks_list,
            "transactions": transactions_list,
            "logs": logs,
        }

    def _extract_via_rpc(
        self, from_block: int, to_block: int
    ) -> Dict[str, Any]:
        """
        Fallback extraction path using RPC (original method).
        Used when REST v2 is unavailable.
        """
        # Step 1: Extract blocks with transactions
        blocks_list, transactions_list = self.extract_block_range(
            from_block, to_block, include_transactions=True
        )

        # Build blocks map for timestamp enrichment
        blocks_map = {}
        for block in blocks_list:
            block_num = int(block["number"], 16)
            blocks_map[block_num] = block

        # Step 2: Extract all logs from transaction receipts
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

        return {
            "blocks": blocks_list,
            "transactions": transactions_list,
            "logs": logs,
        }

    # ══════════════════════════════════════════════════════════════
    # Helper methods (preserved for backward compatibility)
    # ══════════════════════════════════════════════════════════════

    def extract_block_range(
        self,
        from_block: int,
        to_block: int,
        include_transactions: bool = True,
        progress_callback: Optional[Callable] = None,
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Extract blocks and their transactions for a range (RPC path).
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
            block_numbers = sorted(set(int(log["blockNumber"], 16) for log in logs))
            fetched_blocks = self.extractor.get_blocks_by_number(block_numbers)
            self._enrich_logs_with_timestamps(logs, fetched_blocks)

        if progress_callback:
            progress_callback(len(logs))

        return logs

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
