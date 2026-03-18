import json
import random
import time
import threading
from typing import Any, Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class HttpClient:
    """
    Hardened HTTP client with:
    - Transport-level retry for TCP resets (ConnectionResetError, etc.)
    - Exponential backoff with jitter
    - Connection pooling
    - Separate connect/read timeouts
    """

    def __init__(
        self,
        base_url: str,
        rate_limit_per_second: float = 5.0,
        max_retries: int = 15,
        base_delay: float = 1.0,
        max_delay: float = 120.0,
        connect_timeout: float = 15.0,
        read_timeout: float = 60.0,
        pool_connections: int = 10,
        pool_maxsize: int = 20,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.rate_limit_per_second = rate_limit_per_second
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.timeout = (connect_timeout, read_timeout)
        self._last_request_at = 0.0
        self._lock = threading.Lock()

        # Build session with transport-level retry and connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        })

        # urllib3 transport-level retry handles TCP resets, connection drops
        transport_retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=transport_retry,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _sleep_for_rate_limit(self) -> None:
        with self._lock:
            min_interval = 1.0 / max(self.rate_limit_per_second, 0.1)
            elapsed = time.time() - self._last_request_at
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_request_at = time.time()

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Any:
        url = f"{self.base_url}{endpoint}"
        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries):
            self._sleep_for_rate_limit()
            response = None
            try:
                response = self.session.request(
                    method, url, timeout=self.timeout, **kwargs
                )

                # Treat 429 (rate limit) specially — always retry with longer backoff
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    delay = max(retry_after, self.base_delay * (2 ** attempt))
                    if attempt < 3:
                        print(f"  [HTTP] 429 rate limited, waiting {delay:.0f}s...")
                    time.sleep(delay)
                    continue

                # Treat 5xx as retryable
                if response.status_code >= 500:
                    last_exc = requests.exceptions.HTTPError(
                        f"{response.status_code} Server Error for {url}",
                        response=response,
                    )
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    delay *= 0.5 + random.random()
                    time.sleep(delay)
                    continue

                response.raise_for_status()
                return response.json()

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                ConnectionResetError,
                ConnectionAbortedError,
                BrokenPipeError,
                OSError,
            ) as exc:
                # Network-level errors — always retry
                last_exc = exc
                if response is not None:
                    try:
                        response.close()
                    except Exception:
                        pass
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                delay *= 0.5 + random.random()
                if attempt < 3 or attempt % 5 == 0:
                    print(f"  [HTTP] {type(exc).__name__} on attempt {attempt+1}/{self.max_retries}, "
                          f"retrying in {delay:.1f}s...")
                time.sleep(delay)

            except Exception as exc:
                # Other errors (4xx, JSON decode, etc.)
                if response is not None:
                    try:
                        response.close()
                    except Exception:
                        pass
                last_exc = exc
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                delay *= 0.5 + random.random()
                time.sleep(delay)

        raise RuntimeError(f"Request failed after {self.max_retries} retries: {last_exc}")

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, payload: Any) -> Any:
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
