from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from statistics import mean
from typing import Iterable

from game_price_pipeline.models import GamePriceHistory


def build_feature_values(
    release_date: date | None,
    prices: Iterable[GamePriceHistory],
    as_of: datetime,
    feature_date: date,
    sale_dates: tuple[date, ...] = (),
) -> dict[str, object]:
    def fetched_by_cutoff(fetched_at: datetime) -> bool:
        normalized_fetched_at = (
            fetched_at.replace(tzinfo=timezone.utc) if fetched_at.tzinfo is None else fetched_at
        )
        return normalized_fetched_at <= as_of

    eligible_prices = [price for price in prices if fetched_by_cutoff(price.fetched_at)]
    ninety_days_ago = feature_date - timedelta(days=90)
    one_year_ago = feature_date - timedelta(days=365)
    recent_prices = [
        price for price in eligible_prices if ninety_days_ago <= price.observed_date <= feature_date
    ]
    yearly_prices = [
        price for price in eligible_prices if one_year_ago <= price.observed_date <= feature_date
    ]
    next_sale = min((sale for sale in sale_dates if sale >= feature_date), default=None)
    discount_frequency = sum(
        price.cut > 0 and (index == 0 or yearly_prices[index - 1].cut <= 0)
        for index, price in enumerate(yearly_prices)
    )

    return {
        "days_since_release": (
            max((feature_date - release_date).days, 0) if release_date is not None else None
        ),
        "days_to_next_steam_sale": (next_sale - feature_date).days if next_sale else None,
        "is_major_sale_month": feature_date.month in {1, 6, 7, 12},
        "historical_low_cut": max((price.cut for price in eligible_prices), default=None),
        "rolling_avg_discount_90d": (
            mean(price.cut for price in recent_prices) if recent_prices else None
        ),
        "discount_frequency_year": discount_frequency,
    }
