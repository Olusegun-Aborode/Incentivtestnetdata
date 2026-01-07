from typing import Any, Dict, Iterable, List, Optional

from src.utils.http import HttpClient


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
        topics: List[str],
        from_block: int,
        to_block: int,
    ) -> List[Dict[str, Any]]:
        logs: List[Dict[str, Any]] = []
        address = address.lower()
        topics = [topic.lower() for topic in topics]
        for start in range(from_block, to_block + 1, self.batch_size):
            end = min(start + self.batch_size - 1, to_block)
            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [
                    {
                        "address": address,
                        "fromBlock": hex(start),
                        "toBlock": hex(end),
                        "topics": topics,
                    }
                ],
            }
            response = self.rpc_client.post("", payload)
            logs.extend(response.get("result", []))
        return logs

    def get_blocks_by_number(
        self, block_numbers: Iterable[int], include_transactions: bool = False
    ) -> Dict[int, Dict[str, Any]]:
        unique_numbers = sorted(set(block_numbers))
        results: Dict[int, Dict[str, Any]] = {}
        chunk_size = 1 if include_transactions else 100

        for i in range(0, len(unique_numbers), chunk_size):
            chunk = unique_numbers[i : i + chunk_size]
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
            for response in responses:
                block = response.get("result")
                if not block:
                    continue
                results[int(block["number"], 16)] = block
        return results
