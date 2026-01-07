from collections import deque
from typing import Deque, Dict, List, Optional


class ReorgHandler:
    def __init__(self, buffer_size: int) -> None:
        self.buffer_size = buffer_size
        self.block_buffer: Deque[Dict] = deque(maxlen=buffer_size)

    def add_block(self, block: Dict) -> Optional[int]:
        if not self.block_buffer:
            self.block_buffer.append(block)
            return None

        last_block = self.block_buffer[-1]
        if block["parent_hash"] == last_block["hash"]:
            self.block_buffer.append(block)
            return None

        reorg_depth = self._find_fork_point(block["parent_hash"])
        self._rollback(reorg_depth)
        self.block_buffer.append(block)
        return reorg_depth

    def _find_fork_point(self, parent_hash: str) -> int:
        for depth, buffered in enumerate(reversed(self.block_buffer)):
            if buffered["hash"] == parent_hash:
                return depth
        return len(self.block_buffer)

    def _rollback(self, depth: int) -> None:
        for _ in range(depth):
            if self.block_buffer:
                self.block_buffer.pop()

    def get_confirmed_blocks(self) -> List[Dict]:
        if len(self.block_buffer) < self.buffer_size:
            return []
        return [self.block_buffer[0]]
