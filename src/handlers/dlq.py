import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


class DeadLetterQueue:
    def __init__(self, local_path: str = "./dlq") -> None:
        self.local_path = Path(local_path)
        self.local_path.mkdir(parents=True, exist_ok=True)

    def send(self, record: Dict[str, Any], error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "record": record,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context or {},
        }
        filename = self.local_path / f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.json"
        filename.write_text(json.dumps(payload))

    def list_entries(self) -> List[Tuple[Path, Dict[str, Any]]]:
        """List all DLQ entries sorted by timestamp."""
        entries = []
        for f in sorted(self.local_path.glob("*.json")):
            try:
                data = json.loads(f.read_text())
                entries.append((f, data))
            except Exception:
                continue
        return entries

    def count(self) -> int:
        """Count DLQ entries."""
        return len(list(self.local_path.glob("*.json")))

    def get_block_ranges(self) -> List[Tuple[int, int]]:
        """Extract unique block ranges from DLQ entries."""
        ranges = set()
        for _, data in self.list_entries():
            ctx = data.get("context", {})
            from_block = ctx.get("from_block")
            to_block = ctx.get("to_block")
            if from_block is not None and to_block is not None:
                ranges.add((int(from_block), int(to_block)))
        return sorted(ranges)

    def remove(self, path: Path) -> None:
        """Remove a processed DLQ entry."""
        try:
            path.unlink()
        except Exception:
            pass

    def replay(
        self,
        process_fn: Callable[[int, int], bool],
        max_entries: int = 0,
    ) -> Dict[str, int]:
        """
        Replay DLQ entries by calling process_fn(from_block, to_block).
        If process_fn returns True, the DLQ entry is removed.

        Returns {"replayed": N, "succeeded": N, "failed": N}
        """
        entries = self.list_entries()
        if max_entries > 0:
            entries = entries[:max_entries]

        stats = {"replayed": 0, "succeeded": 0, "failed": 0}

        for path, data in entries:
            ctx = data.get("context", {})
            from_block = ctx.get("from_block")
            to_block = ctx.get("to_block")

            if from_block is None or to_block is None:
                continue

            stats["replayed"] += 1
            try:
                success = process_fn(int(from_block), int(to_block))
                if success:
                    self.remove(path)
                    stats["succeeded"] += 1
                else:
                    stats["failed"] += 1
            except Exception as e:
                print(f"  DLQ replay failed for {from_block}-{to_block}: {e}")
                stats["failed"] += 1

        return stats
