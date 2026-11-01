from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Game(Base):
    __tablename__ = "games"

    app_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    release_date: Mapped[date | None] = mapped_column(Date)
    genres: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    publishers: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    is_free: Mapped[bool] = mapped_column(Boolean, nullable=False)
    metadata_fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class GamePriceHistory(Base):
    __tablename__ = "game_price_history"
    __table_args__ = (
        UniqueConstraint("app_id", "shop_id", "observed_date", name="uq_game_shop_observed_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    app_id: Mapped[int] = mapped_column(ForeignKey("games.app_id"), nullable=False)
    shop_id: Mapped[str] = mapped_column(String(100), nullable=False)
    observed_date: Mapped[date] = mapped_column(Date, nullable=False)
    price_actual: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    price_old: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    cut: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    is_free: Mapped[bool] = mapped_column(Boolean, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_snapshot: Mapped[str] = mapped_column(Text, nullable=False)


class GameFeature(Base):
    __tablename__ = "game_features"
    __table_args__ = (UniqueConstraint("app_id", "feature_date", name="uq_game_feature_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    app_id: Mapped[int] = mapped_column(ForeignKey("games.app_id"), nullable=False)
    feature_date: Mapped[date] = mapped_column(Date, nullable=False)
    days_since_release: Mapped[int | None] = mapped_column(Integer)
    days_to_next_steam_sale: Mapped[int | None] = mapped_column(Integer)
    is_major_sale_month: Mapped[bool] = mapped_column(Boolean, nullable=False)
    historical_low_cut: Mapped[float | None] = mapped_column(Float)
    rolling_avg_discount_90d: Mapped[float | None] = mapped_column(Float)
    discount_frequency_year: Mapped[int] = mapped_column(Integer, nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PipelineExecutionLog(Base):
    __tablename__ = "pipeline_execution_logs"

    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    requested_count: Mapped[int] = mapped_column(Integer, nullable=False)
    ingested_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quarantined_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    null_percentage: Mapped[float | None] = mapped_column(Float)
    fallback_data_used: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    error_message: Mapped[str | None] = mapped_column(Text)


class DataQualityQuarantine(Base):
    __tablename__ = "data_quality_quarantine"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    app_id: Mapped[int | None] = mapped_column(Integer)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    raw_record: Mapped[dict[str, object] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
