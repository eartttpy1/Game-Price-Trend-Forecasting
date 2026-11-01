from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    database_url: str
    app_ids: tuple[int, ...]
    country: str
    timezone: ZoneInfo
    raw_data_dir: Path
    retry_count: int
    retry_delay_seconds: float
    request_interval_seconds: float
    metadata_refresh_days: int
    steam_sale_dates: tuple[date, ...]
    discord_webhook_url: str | None

    @classmethod
    def from_env(cls) -> Settings:
        load_dotenv()

        database_url = os.getenv("DATABASE_URL", "").strip()
        if not database_url:
            raise ValueError("DATABASE_URL must be set")

        raw_app_ids = os.getenv("STEAM_APP_IDS", "")
        try:
            app_ids = tuple(
                dict.fromkeys(int(value.strip()) for value in raw_app_ids.split(",") if value.strip())
            )
        except ValueError as exc:
            raise ValueError("STEAM_APP_IDS must be a comma-separated list of integers") from exc
        if not app_ids or any(app_id <= 0 for app_id in app_ids):
            raise ValueError("STEAM_APP_IDS must contain positive Steam app IDs")

        country = os.getenv("STEAM_COUNTRY", "US").strip().upper()
        if country != "US":
            raise ValueError("This pipeline version supports STEAM_COUNTRY=US only")

        timezone_name = os.getenv("PIPELINE_TIMEZONE", "Asia/Bangkok")
        try:
            timezone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Unknown PIPELINE_TIMEZONE: {timezone_name}") from exc

        try:
            retry_count = int(os.getenv("RETRY_COUNT", "3"))
            retry_delay_seconds = float(os.getenv("RETRY_DELAY_SECONDS", "1"))
            request_interval_seconds = float(os.getenv("STEAM_REQUEST_INTERVAL_SECONDS", "1"))
            metadata_refresh_days = int(os.getenv("STEAM_METADATA_REFRESH_DAYS", "30"))
        except ValueError as exc:
            raise ValueError("Retry settings must be numeric") from exc
        if (
            retry_count < 1
            or retry_delay_seconds < 0
            or request_interval_seconds < 0
            or metadata_refresh_days < 1
        ):
            raise ValueError("Retry count and metadata refresh days must be positive; delays cannot be negative")

        raw_sale_dates = os.getenv("STEAM_SALE_DATES", "")
        try:
            sale_dates = tuple(
                sorted(
                    {
                        date.fromisoformat(value.strip())
                        for value in raw_sale_dates.split(",")
                        if value.strip()
                    }
                )
            )
        except ValueError as exc:
            raise ValueError("STEAM_SALE_DATES must contain ISO dates (YYYY-MM-DD)") from exc

        webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip() or None
        return cls(
            database_url=database_url,
            app_ids=app_ids,
            country=country,
            timezone=timezone,
            raw_data_dir=Path(os.getenv("RAW_DATA_DIR", "data/raw")),
            retry_count=retry_count,
            retry_delay_seconds=retry_delay_seconds,
            request_interval_seconds=request_interval_seconds,
            metadata_refresh_days=metadata_refresh_days,
            steam_sale_dates=sale_dates,
            discord_webhook_url=webhook,
        )
