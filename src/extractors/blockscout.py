import concurrent.futures
from typing import Any, Dict, Iterable, List, Optional

from src.utils.http import HttpClient

# Maximum recursion depth for 413 splitting to prevent infinite loops
MAX_SPLIT_DEPTH = 12


class BlockscoutExtractor:
    def __init__(
        self,
        base_url: str,
        rpc_url: str,
        confirmations: int,
        batch_size: int,
        rate_limit_per_second: float,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.rpc_url = rpc_url.rstrip("/")
        self.confirmations = confirmations
        self.batch_size = batch_size
        self.rest_client = HttpClient(self.base_url, rate_limit_per_second=rate_limit_per_second)
        self.rpc_client = HttpClient(self.rpc_url, rate_limit_per_second=rate_limit_per_second)

    def get_latest_block_number(self) -> int:
        payload = {"id": 1, "jsonrpc": "2.0", "method": "eth_blockNumber", "params": []}
        response = self.rpc_client.post("", payload)
        return int(response["result"], 16)

    def get_safe_block_number(self) -> int:
        latest = self.get_latest_block_number()
        return max(latest - self.confirmations, 0)

    def get_logs(
        self,
        address: str,
        topics: List[Any],
        from_block: int,
        to_block: int,
    ) -> List[Dict[str, Any]]:
        address = address.lower()
        normalized_topics = []
        for t in topics:
            if isinstance(t, str):
                normalized_topics.append(t.lower())
            elif isinstance(t, list):
                normalized_topics.append([x.lower() for x in t])
            else:
                normalized_topics.append(t)
        topics = normalized_topics

        logs: List[Dict[str, Any]] = []
        for start in range(from_block, to_block + 1, self.batch_size):
            end = min(start + self.batch_size - 1, to_block)
            logs.extend(self._get_logs_recursive(address, topics, start, end))
        return logs

    def get_all_logs(
        self,
        from_block: int,
        to_block: int,
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL event logs across ALL contracts with no address or topic filtering.
        This captures the complete on-chain activity for the given block range.
        Uses the same recursive 413-splitting strategy as get_logs().
        """
        logs: List[Dict[str, Any]] = []
        for start in range(from_block, to_block + 1, self.batch_size):
            end = min(start + self.batch_size - 1, to_block)
            logs.extend(self._get_all_logs_recursive(start, end))
        return logs

    def _get_all_logs_recursive(
        self, from_block: int, to_block: int, depth: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch all logs with no filters. Recursively splits on 413 errors."""
        if depth > MAX_SPLIT_DEPTH:
            print(f"  Max split depth ({MAX_SPLIT_DEPTH}) reached for blocks {from_block}-{to_block}. Skipping.")
            return []

        try:
            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [
                    {
                        "fromBlock": hex(from_block),
                        "toBlock": hex(to_block),
                    }
                ],
            }
            response = self.rpc_client.post("", payload)
            result = response.get("result", [])
            if result is None:
                return []
            return result
        except Exception as e:
            if "413" in str(e) and from_block < to_block:
                mid = (from_block + to_block) // 2
                if depth == 0:
                    print(f"  Range {from_block}-{to_block} too large (413). Splitting (depth={depth+1})...")
                return self._get_all_logs_recursive(
                    from_block, mid, depth + 1
                ) + self._get_all_logs_recursive(mid + 1, to_block, depth + 1)
            raise e

    def _get_logs_recursive(
        self, address: str, topics: List[Any], from_block: int, to_block: int, depth: int = 0
    ) -> List[Dict[str, Any]]:
        if depth > MAX_SPLIT_DEPTH:
            print(f"  Max split depth ({MAX_SPLIT_DEPTH}) reached for blocks {from_block}-{to_block}. Skipping.")
            return []

        try:
            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [
                    {
                        "address": address,
                        "fromBlock": hex(from_block),
                        "toBlock": hex(to_block),
                        "topics": topics,
                    }
                ],
            }
            response = self.rpc_client.post("", payload)
            return response.get("result", [])
        except Exception as e:
            if "413" in str(e) and from_block < to_block:
                mid = (from_block + to_block) // 2
                if depth == 0:
                    print(f"  Range {from_block}-{to_block} too large (413). Splitting...")
                return self._get_logs_recursive(
                    address, topics, from_block, mid, depth + 1
                ) + self._get_logs_recursive(address, topics, mid + 1, to_block, depth + 1)
            raise e

    def get_blocks_by_number(
        self, block_numbers: Iterable[int], include_transactions: bool = False
    ) -> Dict[int, Dict[str, Any]]:
        unique_numbers = sorted(set(block_numbers))
        results: Dict[int, Dict[str, Any]] = {}
        
        # Extremely restrictive RPC (413 Payload Too Large Even for 10 blocks)
        # We'll use chunk_size=1, but parallelize via ThreadPoolExecutor
        chunk_size = 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_chunk = {}
            for i in range(0, len(unique_numbers), chunk_size):
                chunk = unique_numbers[i : i + chunk_size]
                future = executor.submit(self._get_blocks_recursive, chunk, include_transactions)
                future_to_chunk[future] = chunk

            for future in concurrent.futures.as_completed(future_to_chunk):
                results.update(future.result())

        return results

    def _get_blocks_recursive(self, chunk: List[int], include_transactions: bool) -> Dict[int, Dict[str, Any]]:
        try:
            payloads = [
                {
                    "id": block_number,
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [hex(block_number), include_transactions],
                }
                for block_number in chunk
            ]
            responses = self.rpc_client.post_batch("", payloads)
            results = {}
            for response in responses:
                block = response.get("result")
                if not block:
                    continue
                results[int(block["number"], 16)] = block
            return results
        except Exception as e:
            if "413" in str(e) and len(chunk) > 1:
                mid = len(chunk) // 2
                print(f"  ⚠️ Block batch {len(chunk)} too large (413). Splitting...")
                res = self._get_blocks_recursive(chunk[:mid], include_transactions)
                res.update(self._get_blocks_recursive(chunk[mid:], include_transactions))
                return res
            raise e

    def get_transaction_receipts_parallel(self, tx_hashes: Iterable[str]) -> Dict[str, Dict[str, Any]]:
        unique_hashes = list(set(tx_hashes))
        results: Dict[str, Dict[str, Any]] = {}

        chunk_size = 10

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_chunk = {}
            for i in range(0, len(unique_hashes), chunk_size):
                chunk = unique_hashes[i : i + chunk_size]
                future = executor.submit(self._get_receipts_recursive, chunk)
                future_to_chunk[future] = chunk

            for future in concurrent.futures.as_completed(future_to_chunk):
                results.update(future.result())

        return results

    def _get_receipts_recursive(self, chunk: List[str]) -> Dict[str, Dict[str, Any]]:
        try:
            payloads = [
                {
                    "id": i,
                    "jsonrpc": "2.0",
                    "method": "eth_getTransactionReceipt",
                    "params": [tx_hash],
                }
                for i, tx_hash in enumerate(chunk)
            ]
            responses = self.rpc_client.post_batch("", payloads)
            results = {}
            for response in responses:
                receipt = response.get("result")
                if receipt and receipt.get("transactionHash"):
                    results[receipt["transactionHash"].lower()] = receipt
            return results
        except Exception as e:
            if "413" in str(e) and len(chunk) > 1:
                mid = len(chunk) // 2
                res = self._get_receipts_recursive(chunk[:mid])
                res.update(self._get_receipts_recursive(chunk[mid:]))
                return res
            raise e
