import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


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
