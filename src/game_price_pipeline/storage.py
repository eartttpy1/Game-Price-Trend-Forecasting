from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import Engine, create_engine, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from game_price_pipeline.features import build_feature_values
from game_price_pipeline.models import (
    Base,
    DataQualityQuarantine,
    Game,
    GameFeature,
    GamePriceHistory,
    PipelineExecutionLog,
)
from game_price_pipeline.quality import NormalizedSnapshot


def _is_at_or_before(value: datetime, cutoff: datetime) -> bool:
    normalized = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    return normalized <= cutoff


def create_database_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True)


def initialize_database(engine: Engine) -> None:
    Base.metadata.create_all(engine)


def upsert_snapshot(session: Session, snapshot: NormalizedSnapshot, raw_json: str) -> None:
    game_values = {
        "app_id": snapshot.app_id,
        "title": snapshot.title,
        "release_date": snapshot.release_date,
        "genres": snapshot.genres,
        "publishers": snapshot.publishers,
        "is_free": snapshot.is_free,
        "metadata_fetched_at": snapshot.fetched_at,
    }
    price_values = {
        "app_id": snapshot.app_id,
        "shop_id": "steam",
        "observed_date": snapshot.observed_date,
        "price_actual": snapshot.price_actual,
        "price_old": snapshot.price_old,
        "cut": snapshot.cut,
        "currency": snapshot.currency,
        "is_free": snapshot.is_free,
        "fetched_at": snapshot.fetched_at,
        "raw_snapshot": raw_json,
    }
    dialect = session.get_bind().dialect.name
    insert = postgresql_insert if dialect == "postgresql" else sqlite_insert if dialect == "sqlite" else None
    if insert is None:
        raise SQLAlchemyError(f"Unsupported database dialect for idempotent upsert: {dialect}")

    if snapshot.metadata_fetched:
        game_statement = insert(Game).values(**game_values)
        session.execute(
            game_statement.on_conflict_do_update(
                index_elements=[Game.app_id],
                set_={key: value for key, value in game_values.items() if key != "app_id"},
                where=(
                    game_statement.excluded.metadata_fetched_at
                    > Game.metadata_fetched_at
                ),
            )
        )
    elif session.get(Game, snapshot.app_id) is None:
        raise ValueError(f"Metadata for Steam app {snapshot.app_id} has not been ingested")
    price_statement = insert(GamePriceHistory).values(**price_values)
    conflict_target = (
        {"constraint": "uq_game_shop_observed_date"}
        if dialect == "postgresql"
        else {
            "index_elements": [
                GamePriceHistory.app_id,
                GamePriceHistory.shop_id,
                GamePriceHistory.observed_date,
            ]
        }
    )
    session.execute(
        price_statement.on_conflict_do_update(
            **conflict_target,
            set_={key: value for key, value in price_values.items() if key not in {
                "app_id", "shop_id", "observed_date"
            }},
            where=price_statement.excluded.fetched_at > GamePriceHistory.fetched_at,
        )
    )


def save_quarantine(
    session: Session,
    run_id: str,
    app_id: int | None,
    reason: str,
    raw_record: dict[str, Any] | None,
    created_at: datetime,
) -> None:
    session.add(
        DataQualityQuarantine(
            run_id=run_id,
            app_id=app_id,
            source="steam_store",
            reason=reason,
            raw_record=raw_record,
            created_at=created_at,
        )
    )


def save_features(
    session: Session,
    as_of: datetime,
    feature_date: date,
    sale_dates: tuple[date, ...],
) -> int:
    games = session.scalars(select(Game)).all()
    saved = 0
    dialect = session.get_bind().dialect.name
    insert = postgresql_insert if dialect == "postgresql" else sqlite_insert if dialect == "sqlite" else None
    if insert is None:
        raise SQLAlchemyError(f"Unsupported database dialect for feature upsert: {dialect}")

    for game in games:
        prices = session.scalars(
            select(GamePriceHistory).where(
                GamePriceHistory.app_id == game.app_id,
                GamePriceHistory.fetched_at <= as_of,
            )
        ).all()
        release_date = (
            game.release_date
            if game.release_date is not None
            and _is_at_or_before(game.metadata_fetched_at, as_of)
            else None
        )
        values = {
            "app_id": game.app_id,
            "feature_date": feature_date,
            **build_feature_values(release_date, prices, as_of, feature_date, sale_dates),
            "as_of": as_of,
        }
        statement = insert(GameFeature).values(**values)
        conflict_target = (
            {"constraint": "uq_game_feature_date"}
            if dialect == "postgresql"
            else {"index_elements": [GameFeature.app_id, GameFeature.feature_date]}
        )
        session.execute(
            statement.on_conflict_do_update(
                **conflict_target,
                set_={key: value for key, value in values.items() if key not in {
                    "app_id", "feature_date"
                }},
                where=statement.excluded.as_of > GameFeature.as_of,
            )
        )
        saved += 1
    return saved


def start_execution(
    session: Session,
    run_id: str,
    started_at: datetime,
    requested_count: int,
) -> None:
    session.add(
        PipelineExecutionLog(
            run_id=run_id,
            started_at=started_at,
            status="running",
            requested_count=requested_count,
            ingested_count=0,
            quarantined_count=0,
            error_count=0,
            null_percentage=None,
            fallback_data_used=False,
        )
    )


def finish_execution(
    session: Session,
    run_id: str,
    finished_at: datetime,
    status: str,
    ingested_count: int,
    quarantined_count: int,
    error_count: int,
    null_percentage: float | None = None,
    error_message: str | None = None,
) -> None:
    log = session.get(PipelineExecutionLog, run_id)
    if log is None:
        raise LookupError(f"Pipeline execution log {run_id} was not found")
    log.finished_at = finished_at
    log.status = status
    log.ingested_count = ingested_count
    log.quarantined_count = quarantined_count
    log.error_count = error_count
    log.null_percentage = null_percentage
    log.error_message = error_message


def read_price_history(
    session: Session,
    app_id: int,
    as_of: datetime | None = None,
) -> list[GamePriceHistory]:
    statement = select(GamePriceHistory).where(GamePriceHistory.app_id == app_id)
    if as_of is not None:
        statement = statement.where(GamePriceHistory.fetched_at <= as_of)
    return list(session.scalars(statement.order_by(GamePriceHistory.observed_date)).all())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
