from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace

from game_price_pipeline.alerts import send_discord_alert
from game_price_pipeline.config import Settings
from game_price_pipeline.orchestrator import run_pipeline
from game_price_pipeline.steam import SteamStoreClient
from game_price_pipeline.storage import create_database_engine, initialize_database


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest daily Steam game price snapshots")
    parser.add_argument(
        "--app-ids",
        help="Comma-separated Steam app IDs; overrides STEAM_APP_IDS",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        settings = Settings.from_env()
        if args.app_ids:
            try:
                app_ids = tuple(dict.fromkeys(int(value.strip()) for value in args.app_ids.split(",")))
            except ValueError as exc:
                raise ValueError("--app-ids must be a comma-separated list of integers") from exc
            if not app_ids or any(app_id <= 0 for app_id in app_ids):
                raise ValueError("--app-ids must contain positive Steam app IDs")
            settings = replace(settings, app_ids=app_ids)
    except ValueError as exc:
        parser.error(str(exc))

    engine = create_database_engine(settings.database_url)
    initialize_database(engine)
    try:
        with SteamStoreClient(
            country=settings.country,
            retry_count=settings.retry_count,
            retry_delay_seconds=settings.retry_delay_seconds,
            request_interval_seconds=settings.request_interval_seconds,
        ) as provider:
            result = run_pipeline(settings, engine, provider)
        logging.getLogger(__name__).info(
            "Pipeline run %s finished with status=%s ingested=%d quarantined=%d errors=%d",
            result.run_id,
            result.status,
            result.ingested_count,
            result.quarantined_count,
            result.error_count,
        )
        if result.status != "success" and settings.discord_webhook_url:
            send_discord_alert(
                settings.discord_webhook_url,
                f"Game price pipeline {result.status}: run={result.run_id}, "
                f"ingested={result.ingested_count}, quarantined={result.quarantined_count}, "
                f"errors={result.error_count}",
            )
        return 0 if result.status == "success" else 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
