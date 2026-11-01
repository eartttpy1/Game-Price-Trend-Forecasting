from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from game_price_pipeline.config import Settings
from game_price_pipeline.models import Game
from game_price_pipeline.quality import DataValidationError, NormalizedSnapshot, normalize_steam_response
from game_price_pipeline.storage import (
    finish_execution,
    save_features,
    save_quarantine,
    start_execution,
    upsert_snapshot,
)

logger = logging.getLogger(__name__)


class SteamProvider(Protocol):
    def fetch_app_details(
        self,
        app_id: int,
        include_metadata: bool = True,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class RunResult:
    run_id: str
    status: str
    ingested_count: int
    quarantined_count: int
    error_count: int


def _metadata_refresh_due(game: Game, run_at: datetime, refresh_days: int) -> bool:
    metadata_fetched_at = game.metadata_fetched_at
    if metadata_fetched_at.tzinfo is None:
        metadata_fetched_at = metadata_fetched_at.replace(tzinfo=timezone.utc)
    return metadata_fetched_at <= run_at - timedelta(days=refresh_days)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_file.write(encoded)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
            temporary_path = Path(temporary_file.name)
        os.link(temporary_path, path)
    except FileExistsError as exc:
        raise FileExistsError(f"Refusing to overwrite raw snapshot {path}") from exc
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return encoded


def _cutoff_for_run(run_at: datetime, timezone_info: ZoneInfo) -> datetime:
    local_run_at = run_at.astimezone(timezone_info)
    local_midnight = datetime.combine(local_run_at.date(), time.min, tzinfo=timezone_info)
    return local_midnight.astimezone(timezone.utc)


def _merge_cached_metadata(
    app_id: int,
    payload: dict[str, Any],
    game: Game,
) -> dict[str, Any]:
    result = payload.get(str(app_id))
    if not isinstance(result, dict):
        return payload
    data = result.get("data")
    if not isinstance(data, dict):
        if data != []:
            return payload
        data = {}

    merged_data = dict(data)
    merged_data.setdefault("type", "game")
    merged_data.setdefault("steam_appid", app_id)
    merged_data.setdefault("name", game.title)
    merged_data.setdefault("is_free", game.is_free)
    if game.release_date is not None:
        merged_data.setdefault("release_date", {"date": game.release_date.isoformat()})
    merged_data.setdefault(
        "genres",
        [{"description": genre} for genre in game.genres],
    )
    merged_data.setdefault("publishers", game.publishers)
    return {
        **payload,
        str(app_id): {
            **result,
            "data": merged_data,
        },
    }


def run_pipeline(
    settings: Settings,
    engine: Any,
    provider: SteamProvider,
    run_at: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> RunResult:
    now = clock or (lambda: datetime.now(timezone.utc))
    run_at = run_at or now()
    if run_at.tzinfo is None:
        raise ValueError("run_at must be timezone-aware")
    run_at = run_at.astimezone(timezone.utc)
    local_run_at = run_at.astimezone(settings.timezone)
    run_id = str(uuid.uuid4())
    raw_dir = settings.raw_data_dir
    cutoff = _cutoff_for_run(run_at, settings.timezone)
    snapshots: list[tuple[NormalizedSnapshot, str]] = []
    quarantines: list[tuple[int, str, dict[str, Any]]] = []
    errors: list[str] = []

    SessionFactory = sessionmaker(engine, expire_on_commit=False)
    with SessionFactory.begin() as session:
        start_execution(session, run_id, run_at, len(settings.app_ids))
    with SessionFactory() as session:
        cached_games = {
            game.app_id: game
            for game in session.scalars(
                select(Game).where(Game.app_id.in_(settings.app_ids))
            ).all()
        }

    for app_id in settings.app_ids:
        cached_game = cached_games.get(app_id)
        include_metadata = cached_game is None or _metadata_refresh_due(
            cached_game,
            run_at,
            settings.metadata_refresh_days,
        )
        try:
            payload = provider.fetch_app_details(
                app_id,
                include_metadata=include_metadata,
            )
        except (httpx.HTTPError, RuntimeError) as exc:
            message = f"app {app_id}: {exc}"
            logger.error("Steam ingestion failed: %s", message)
            errors.append(message)
            continue

        fetched_at = now().astimezone(timezone.utc)
        snapshot_path = raw_dir / f"steam_{app_id}_{run_at:%Y%m%dT%H%M%S}_{run_id}.json"
        try:
            raw_json = _atomic_write_json(snapshot_path, payload)
            normalized_payload = (
                _merge_cached_metadata(app_id, payload, cached_game)
                if cached_game is not None and not include_metadata
                else payload
            )
            normalized = normalize_steam_response(
                app_id=app_id,
                payload=normalized_payload,
                fetched_at=fetched_at,
                observed_date=local_run_at.date(),
                metadata_fetched=include_metadata,
            )
        except (DataValidationError, OSError, TypeError, ValueError) as exc:
            message = str(exc)
            logger.warning("Steam record quarantined for app %s: %s", app_id, message)
            errors.append(f"app {app_id}: {message}")
            quarantines.append((app_id, message, payload))
            continue
        snapshots.append((normalized, raw_json))

    status = "success"
    if errors:
        status = "partial" if snapshots else "failed"
    error_message = "; ".join(errors)[:4000] or None
    null_percentage = (
        sum(
            value is None or value == []
            for snapshot, _ in snapshots
            for value in (snapshot.release_date, snapshot.genres, snapshot.publishers)
        )
        / (len(snapshots) * 3)
        * 100
        if snapshots
        else None
    )
    try:
        with SessionFactory.begin() as session:
            for snapshot, raw_json in snapshots:
                upsert_snapshot(session, snapshot, raw_json)
            for app_id, reason, raw_record in quarantines:
                save_quarantine(session, run_id, app_id, reason, raw_record, run_at)
            save_features(session, cutoff, local_run_at.date(), settings.steam_sale_dates)
            finish_execution(
                session,
                run_id,
                now().astimezone(timezone.utc),
                status,
                len(snapshots),
                len(quarantines),
                len(errors),
                null_percentage,
                error_message,
            )
    except SQLAlchemyError as exc:
        logger.exception("Database stage failed for pipeline run %s", run_id)
        try:
            with SessionFactory.begin() as session:
                finish_execution(
                    session,
                    run_id,
                    now().astimezone(timezone.utc),
                    "failed",
                    0,
                    len(quarantines),
                    len(errors) + 1,
                    None,
                    f"Database stage failed: {exc}"[:4000],
                )
        except SQLAlchemyError:
            logger.exception("Could not persist failure status for pipeline run %s", run_id)
        raise

    return RunResult(
        run_id=run_id,
        status=status,
        ingested_count=len(snapshots),
        quarantined_count=len(quarantines),
        error_count=len(errors),
    )
