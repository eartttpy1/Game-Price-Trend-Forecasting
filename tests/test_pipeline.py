from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

import httpx

from game_price_pipeline.config import Settings
from game_price_pipeline.models import (
    Base,
    Game,
    GameFeature,
    GamePriceHistory,
    PipelineExecutionLog,
)
from game_price_pipeline.orchestrator import run_pipeline
from game_price_pipeline.quality import DataValidationError, normalize_steam_response
from game_price_pipeline.storage import read_price_history
from game_price_pipeline.steam import SteamStoreClient


def steam_payload(
    app_id: int = 570,
    *,
    final: int = 499,
    initial: int = 999,
    discount: int = 50,
    is_free: bool = False,
) -> dict[str, object]:
    data: dict[str, object] = {
        "type": "game",
        "name": "Dota 2",
        "is_free": is_free,
        "release_date": {"date": "Jul 09, 2013"},
        "genres": [{"description": "Free to Play"}],
        "publishers": ["Valve"],
    }
    if not is_free or initial > 0:
        data["price_overview"] = {
            "initial": initial,
            "final": final,
            "discount_percent": discount,
            "currency": "USD",
        }
    return {str(app_id): {"success": True, "data": data}}


class FakeSteamProvider:
    def __init__(self, payloads: dict[int, dict[str, object]]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[int, bool]] = []

    def fetch_app_details(
        self,
        app_id: int,
        include_metadata: bool = True,
    ) -> dict[str, object]:
        self.calls.append((app_id, include_metadata))
        payload = self.payloads[app_id]
        if include_metadata:
            return payload
        result = payload[str(app_id)]
        data = result["data"]
        price = data.get("price_overview")
        return {
            str(app_id): {
                "success": result["success"],
                "data": {"price_overview": price} if price is not None else [],
            }
        }


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.addCleanup(self.engine.dispose)
        self.settings = Settings(
            database_url="sqlite:///:memory:",
            app_ids=(570,),
            country="US",
            timezone=ZoneInfo("Asia/Bangkok"),
            raw_data_dir=Path(self.temporary_directory.name) / "raw",
            retry_count=3,
            retry_delay_seconds=0,
            request_interval_seconds=0,
            metadata_refresh_days=30,
            steam_sale_dates=(date(2026, 12, 1),),
            discord_webhook_url=None,
        )

    def test_pipeline_ingests_and_upserts_daily_snapshot(self) -> None:
        run_at = datetime(2026, 7, 1, 0, 5, tzinfo=ZoneInfo("Asia/Bangkok"))
        provider = FakeSteamProvider({570: steam_payload()})
        clock = lambda: run_at.astimezone(timezone.utc)

        first = run_pipeline(self.settings, self.engine, provider, run_at, clock)
        second = run_pipeline(self.settings, self.engine, provider, run_at, clock)

        self.assertEqual(first.status, "success")
        self.assertEqual(second.status, "success")
        self.assertEqual(provider.calls, [(570, True), (570, False)])
        with Session(self.engine) as session:
            self.assertEqual(session.scalar(select(func.count(Game.app_id))), 1)
            self.assertEqual(session.scalar(select(func.count(GamePriceHistory.id))), 1)
            execution = session.get(PipelineExecutionLog, first.run_id)
            self.assertIsNotNone(execution)
            assert execution is not None
            self.assertEqual(execution.status, "success")
        raw_files = list((Path(self.temporary_directory.name) / "raw").glob("*.json"))
        self.assertEqual(len(raw_files), 2)
        self.assertEqual(json.loads(raw_files[0].read_text(encoding="utf-8"))["570"]["success"], True)

    def test_invalid_zero_price_is_quarantined(self) -> None:
        payload = steam_payload(final=0, initial=999, discount=100)
        provider = FakeSteamProvider({570: payload})
        result = run_pipeline(
            self.settings,
            self.engine,
            provider,
            datetime(2026, 7, 1, 0, 5, tzinfo=timezone.utc),
            lambda: datetime(2026, 7, 1, 0, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.quarantined_count, 1)
        with Session(self.engine) as session:
            self.assertEqual(session.scalar(select(func.count(GamePriceHistory.id))), 0)
            execution = session.get(PipelineExecutionLog, result.run_id)
            self.assertIsNotNone(execution)
            assert execution is not None
            self.assertEqual(execution.status, "failed")

    def test_normalizer_accepts_confirmed_free_game(self) -> None:
        snapshot = normalize_steam_response(
            570,
            steam_payload(final=0, initial=0, discount=0, is_free=True),
            datetime(2026, 7, 1, tzinfo=timezone.utc),
            date(2026, 7, 1),
        )
        self.assertEqual(snapshot.price_actual, 0)
        self.assertEqual(snapshot.currency, "USD")

    def test_normalizer_rejects_mismatched_discount(self) -> None:
        with self.assertRaises(DataValidationError):
            normalize_steam_response(
                570,
                steam_payload(final=500, initial=1000, discount=20),
                datetime(2026, 7, 1, tzinfo=timezone.utc),
                date(2026, 7, 1),
            )

    def test_features_exclude_prices_and_metadata_after_cutoff(self) -> None:
        previous_run = datetime(2026, 6, 30, 0, 5, tzinfo=ZoneInfo("Asia/Bangkok"))
        current_run = datetime(2026, 7, 1, 0, 5, tzinfo=ZoneInfo("Asia/Bangkok"))
        run_pipeline(
            self.settings,
            self.engine,
            FakeSteamProvider({570: steam_payload()}),
            previous_run,
            lambda: previous_run.astimezone(timezone.utc),
        )
        run_pipeline(
            self.settings,
            self.engine,
            FakeSteamProvider({570: steam_payload(final=299, initial=999, discount=70)}),
            current_run,
            lambda: current_run.astimezone(timezone.utc),
        )
        with Session(self.engine) as session:
            history = read_price_history(
                session,
                570,
                datetime(2026, 6, 30, 17, 0, tzinfo=timezone.utc),
            )
            feature = session.scalar(
                select(GameFeature).where(
                    GameFeature.app_id == 570,
                    GameFeature.feature_date == date(2026, 7, 1),
                )
            )
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0].observed_date, date(2026, 6, 30))
            self.assertIsNotNone(feature)
            assert feature is not None
            self.assertEqual(feature.historical_low_cut, 50)
            self.assertEqual(
                feature.days_since_release,
                (date(2026, 7, 1) - date(2013, 7, 9)).days,
            )
            self.assertEqual(feature.feature_date, date(2026, 7, 1))

    def test_price_only_response_uses_cached_free_game_flag(self) -> None:
        run_at = datetime(2026, 6, 30, 0, 5, tzinfo=ZoneInfo("Asia/Bangkok"))
        provider = FakeSteamProvider(
            {570: steam_payload(final=0, initial=0, discount=0, is_free=True)}
        )
        for day in (0, 1):
            daily_run = run_at + timedelta(days=day)
            run_pipeline(
                self.settings,
                self.engine,
                provider,
                daily_run,
                lambda daily_run=daily_run: daily_run.astimezone(timezone.utc),
            )

        with Session(self.engine) as session:
            prices = session.scalars(select(GamePriceHistory).order_by(GamePriceHistory.observed_date)).all()
            self.assertEqual([price.price_actual for price in prices], [0, 0])
            self.assertEqual(provider.calls, [(570, True), (570, False)])

    def test_metadata_is_refetched_after_configured_interval(self) -> None:
        provider = FakeSteamProvider({570: steam_payload()})
        first_run = datetime(2026, 6, 1, 0, 5, tzinfo=ZoneInfo("Asia/Bangkok"))
        refresh_run = datetime(2026, 7, 1, 0, 5, tzinfo=ZoneInfo("Asia/Bangkok"))
        for run_at in (first_run, refresh_run):
            run_pipeline(
                self.settings,
                self.engine,
                provider,
                run_at,
                lambda run_at=run_at: run_at.astimezone(timezone.utc),
            )

        self.assertEqual(provider.calls, [(570, True), (570, True)])

    def test_steam_client_retries_server_errors(self) -> None:
        requests = 0

        def respond(request: httpx.Request) -> httpx.Response:
            nonlocal requests
            requests += 1
            if requests == 1:
                return httpx.Response(503, request=request)
            return httpx.Response(200, json=steam_payload(), request=request)

        client = httpx.Client(transport=httpx.MockTransport(respond))
        self.addCleanup(client.close)
        with SteamStoreClient(
            retry_count=3,
            retry_delay_seconds=0,
            request_interval_seconds=0,
            client=client,
        ) as steam:
            response = steam.fetch_app_details(570)

        self.assertEqual(requests, 2)
        self.assertTrue(response["570"]["success"])

    def test_steam_client_requests_only_price_for_cached_game(self) -> None:
        def respond(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.params["filters"], "price_overview")
            return httpx.Response(
                200,
                json={"570": {"success": True, "data": {"price_overview": {}}}},
                request=request,
            )

        client = httpx.Client(transport=httpx.MockTransport(respond))
        self.addCleanup(client.close)
        with SteamStoreClient(
            retry_delay_seconds=0,
            request_interval_seconds=0,
            client=client,
        ) as steam:
            response = steam.fetch_app_details(570, include_metadata=False)

        self.assertIn("price_overview", response["570"]["data"])


if __name__ == "__main__":
    unittest.main()
