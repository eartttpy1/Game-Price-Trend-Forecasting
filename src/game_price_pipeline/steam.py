from __future__ import annotations

import time
from time import monotonic
from typing import Any

import httpx


class SteamClientError(RuntimeError):
    pass


class SteamStoreClient:
    def __init__(
        self,
        country: str = "US",
        retry_count: int = 3,
        retry_delay_seconds: float = 1,
        request_interval_seconds: float = 1,
        client: httpx.Client | None = None,
    ) -> None:
        if retry_count < 1:
            raise ValueError("retry_count must be positive")
        if retry_delay_seconds < 0 or request_interval_seconds < 0:
            raise ValueError("request delays cannot be negative")
        self.country = country
        self.retry_count = retry_count
        self.retry_delay_seconds = retry_delay_seconds
        self.request_interval_seconds = request_interval_seconds
        self._last_request_at: float | None = None
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(20),
            headers={"User-Agent": "GamePriceTrendForecasting/0.1"},
        )
        self._owns_client = client is None

    def fetch_app_details(self, app_id: int, include_metadata: bool = True) -> dict[str, Any]:
        url = "https://store.steampowered.com/api/appdetails"
        last_error: Exception | None = None
        params = {"appids": app_id, "cc": self.country}
        if not include_metadata:
            params["filters"] = "price_overview"

        for attempt in range(self.retry_count):
            try:
                if self._last_request_at is not None and self.request_interval_seconds:
                    elapsed = monotonic() - self._last_request_at
                    remaining = self.request_interval_seconds - elapsed
                    if remaining > 0:
                        time.sleep(remaining)
                self._last_request_at = monotonic()
                response = self._client.get(
                    url,
                    params=params,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    response.raise_for_status()
                    raise SteamClientError(f"Steam returned retryable HTTP {response.status_code}")
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not payload:
                    raise SteamClientError(f"Steam returned an empty or invalid response for app {app_id}")
                return payload
            except (httpx.HTTPError, ValueError, SteamClientError) as exc:
                last_error = exc
                retryable = (
                    not isinstance(exc, httpx.HTTPStatusError)
                    or exc.response.status_code == 429
                    or exc.response.status_code >= 500
                )
                if not retryable or attempt + 1 == self.retry_count:
                    break
                if self.retry_delay_seconds:
                    time.sleep(self.retry_delay_seconds * (2**attempt))

        raise SteamClientError(
            f"Failed to fetch Steam app {app_id} after {self.retry_count} attempt(s): {last_error}"
        ) from last_error

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> SteamStoreClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
