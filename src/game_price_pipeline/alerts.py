from __future__ import annotations

import httpx


def send_discord_alert(webhook_url: str, message: str) -> None:
    response = httpx.post(
        webhook_url,
        json={"content": message[:1900]},
        timeout=httpx.Timeout(10),
    )
    response.raise_for_status()
