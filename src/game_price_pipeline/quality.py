from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any


class DataValidationError(ValueError):
    pass


@dataclass(frozen=True)
class NormalizedSnapshot:
    app_id: int
    title: str
    release_date: date | None
    genres: list[str]
    publishers: list[str]
    is_free: bool
    price_actual: Decimal
    price_old: Decimal
    cut: float
    currency: str
    observed_date: date
    fetched_at: datetime
    metadata_fetched: bool = True


def normalize_steam_response(
    app_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    observed_date: date,
    metadata_fetched: bool = True,
) -> NormalizedSnapshot:
    result = payload.get(str(app_id))
    if not isinstance(result, dict) or result.get("success") is not True:
        raise DataValidationError("Steam response is missing a successful app details result")

    data = result.get("data")
    if not isinstance(data, dict) or data.get("type") != "game":
        raise DataValidationError("Steam response does not contain a game")
    response_app_id = data.get("steam_appid")
    if response_app_id is not None and response_app_id != app_id:
        raise DataValidationError("Steam response app ID does not match the requested app ID")

    title = data.get("name")
    if not isinstance(title, str) or not title.strip():
        raise DataValidationError("Game title is missing")

    is_free = data.get("is_free") is True
    price = data.get("price_overview")
    if price is None and is_free:
        initial_minor = final_minor = 0
        cut = 0.0
        currency = "USD"
    elif isinstance(price, dict):
        initial_minor = price.get("initial")
        final_minor = price.get("final")
        cut_value = price.get("discount_percent")
        currency = price.get("currency")
        if (
            not isinstance(initial_minor, int)
            or isinstance(initial_minor, bool)
            or not isinstance(final_minor, int)
            or isinstance(final_minor, bool)
        ):
            raise DataValidationError("Steam price values must be integer minor currency units")
        if not isinstance(cut_value, int) or isinstance(cut_value, bool) or not 0 <= cut_value <= 100:
            raise DataValidationError("Discount percentage must be between 0 and 100")
        cut = float(cut_value)
        if not isinstance(currency, str) or len(currency) != 3:
            raise DataValidationError("Steam price currency is missing or invalid")
        if currency != "USD":
            raise DataValidationError("This pipeline version supports USD prices only")
    else:
        raise DataValidationError("Game price is missing and the game is not marked as free")

    if initial_minor < 0 or final_minor < 0:
        raise DataValidationError("Prices cannot be negative")
    if final_minor > initial_minor:
        raise DataValidationError("Current price cannot exceed the initial price")
    if (initial_minor == 0 or final_minor == 0) and not is_free:
        raise DataValidationError("Zero price must be confirmed by Steam's is_free flag")
    if initial_minor > 0:
        expected_cut = round((initial_minor - final_minor) / initial_minor * 100)
        if abs(expected_cut - cut) > 1:
            raise DataValidationError("Discount percentage does not match the listed prices")

    release_info = data.get("release_date")
    release_date: date | None = None
    if isinstance(release_info, dict):
        release_text = release_info.get("date")
        if isinstance(release_text, str) and release_text.strip():
            for date_format in ("%b %d, %Y", "%d %b, %Y", "%Y-%m-%d"):
                try:
                    release_date = datetime.strptime(release_text.strip(), date_format).date()
                    break
                except ValueError:
                    continue

    genres_data = data.get("genres", [])
    genres = [
        item["description"]
        for item in genres_data
        if isinstance(item, dict) and isinstance(item.get("description"), str)
    ] if isinstance(genres_data, list) else []
    publishers_data = data.get("publishers", [])
    publishers = (
        [publisher for publisher in publishers_data if isinstance(publisher, str)]
        if isinstance(publishers_data, list)
        else []
    )

    return NormalizedSnapshot(
        app_id=app_id,
        title=title.strip(),
        release_date=release_date,
        genres=genres,
        publishers=publishers,
        is_free=is_free,
        price_actual=Decimal(final_minor) / Decimal(100),
        price_old=Decimal(initial_minor) / Decimal(100),
        cut=cut,
        currency=currency,
        observed_date=observed_date,
        fetched_at=fetched_at,
        metadata_fetched=metadata_fetched,
    )
