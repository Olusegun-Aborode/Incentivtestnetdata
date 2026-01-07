import json
import random
import time
from typing import Any, Dict, Iterable, List, Optional

import requests


class HttpClient:
    def __init__(
        self,
        base_url: str,
        rate_limit_per_second: float = 5.0,
        max_retries: int = 5,
        base_delay: float = 0.5,
        max_delay: float = 20.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.rate_limit_per_second = rate_limit_per_second
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._last_request_at = 0.0
        self.session = requests.Session()

    def _sleep_for_rate_limit(self) -> None:
        min_interval = 1.0 / max(self.rate_limit_per_second, 0.1)
        elapsed = time.time() - self._last_request_at
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries):
            self._sleep_for_rate_limit()
            self._last_request_at = time.time()
            try:
                response = self.session.request(method, url, timeout=30, **kwargs)
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                last_exc = exc
                delay = min(self.base_delay * (2**attempt), self.max_delay)
                delay *= 0.5 + random.random()
                time.sleep(delay)
        raise RuntimeError(f"Request failed after retries: {last_exc}")

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(
            "POST",
            endpoint,
            json=payload,
            headers={"content-type": "application/json"},
        )

    def post_batch(self, endpoint: str, payloads: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        data = json.dumps(list(payloads))
        result = self._request(
            "POST",
            endpoint,
            data=data,
            headers={"content-type": "application/json"},
        )
        if isinstance(result, list):
            return result
        return [result]
