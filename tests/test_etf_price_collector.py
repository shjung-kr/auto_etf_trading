from __future__ import annotations

from datetime import datetime
import unittest
from zoneinfo import ZoneInfo

from auto_etf_trading import ETFPriceCollector

LOGGER_NAME = "auto_etf_trading.etf_price_collector"
EASTERN = ZoneInfo("America/New_York")


def eastern_datetime(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int = 0,
) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=EASTERN)


def price_bar(timestamp: datetime, *, close: float = 520.5) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "open": 520.0,
        "high": 521.0,
        "low": 519.5,
        "close": close,
        "volume": 10_000,
    }


class RecordingProvider:
    def __init__(self, responses: list[object]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def fetch_prices(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: str,
    ) -> object:
        self.calls.append(
            {
                "symbol": symbol,
                "start": start,
                "end": end,
                "interval": interval,
            }
        )
        if not self._responses:
            raise AssertionError("No more provider responses configured")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class ETFPriceCollectorTests(unittest.TestCase):
    def test_collects_latest_bar_during_market_session(self) -> None:
        provider = RecordingProvider(
            [price_bar(eastern_datetime(2026, 3, 12, 10, 14))]
        )
        collector = ETFPriceCollector(provider)

        with self.assertLogs(LOGGER_NAME, level="INFO") as captured:
            result = collector.collect_latest(
                "SPY",
                current_time=eastern_datetime(2026, 3, 12, 10, 15, 27),
            )

        self.assertEqual(result.status, "collected")
        self.assertEqual(result.interval, "1m")
        self.assertEqual(result.attempts, 1)
        self.assertEqual(len(result.records), 1)
        self.assertEqual(provider.calls[0]["interval"], "1m")
        self.assertEqual(provider.calls[0]["start"], eastern_datetime(2026, 3, 12, 10, 14))
        self.assertEqual(provider.calls[0]["end"], eastern_datetime(2026, 3, 12, 10, 15))
        self.assertIn("Collected 1 1-minute bars for SPY", "\n".join(captured.output))

    def test_skips_collection_when_market_is_closed(self) -> None:
        provider = RecordingProvider([])
        collector = ETFPriceCollector(provider)

        with self.assertLogs(LOGGER_NAME, level="INFO") as captured:
            result = collector.collect_latest(
                "SPY",
                current_time=eastern_datetime(2026, 3, 12, 8, 45),
            )

        self.assertEqual(result.status, "market_closed")
        self.assertEqual(result.attempts, 0)
        self.assertEqual(provider.calls, [])
        self.assertIn(
            "Skipping SPY collection outside collectible market window",
            "\n".join(captured.output),
        )

    def test_skips_collection_before_first_complete_market_minute(self) -> None:
        provider = RecordingProvider([])
        collector = ETFPriceCollector(provider)

        with self.assertLogs(LOGGER_NAME, level="INFO") as captured:
            result = collector.collect_latest(
                "SPY",
                current_time=eastern_datetime(2026, 3, 12, 9, 30, 20),
            )

        self.assertEqual(result.status, "market_closed")
        self.assertEqual(result.records, ())
        self.assertEqual(provider.calls, [])
        self.assertIn(
            "Skipping SPY collection outside collectible market window",
            "\n".join(captured.output),
        )

    def test_collects_final_bar_at_market_close_boundary(self) -> None:
        provider = RecordingProvider(
            [price_bar(eastern_datetime(2026, 3, 12, 15, 59), close=530.25)]
        )
        collector = ETFPriceCollector(provider)

        with self.assertLogs(LOGGER_NAME, level="INFO") as captured:
            result = collector.collect_latest(
                "SPY",
                current_time=eastern_datetime(2026, 3, 12, 16, 0, 20),
            )

        self.assertEqual(result.status, "collected")
        self.assertEqual(result.attempts, 1)
        self.assertEqual(provider.calls[0]["start"], eastern_datetime(2026, 3, 12, 15, 59))
        self.assertEqual(provider.calls[0]["end"], eastern_datetime(2026, 3, 12, 16, 0))
        self.assertAlmostEqual(result.records[0].close, 530.25)
        self.assertIn("Collected 1 1-minute bars for SPY", "\n".join(captured.output))

    def test_retries_transient_failures_until_success(self) -> None:
        provider = RecordingProvider(
            [
                RuntimeError("temporary upstream timeout"),
                RuntimeError("temporary upstream timeout"),
                [price_bar(eastern_datetime(2026, 3, 12, 10, 14))],
            ]
        )
        delays: list[float] = []
        collector = ETFPriceCollector(
            provider,
            max_retries=2,
            retry_delay_seconds=0.25,
            sleep=delays.append,
        )

        with self.assertLogs(LOGGER_NAME, level="INFO") as captured:
            result = collector.collect_latest(
                "SPY",
                current_time=eastern_datetime(2026, 3, 12, 10, 15, 8),
            )

        self.assertEqual(result.status, "collected")
        self.assertEqual(result.attempts, 3)
        self.assertEqual(len(provider.calls), 3)
        self.assertEqual(delays, [0.25, 0.25])
        joined_logs = "\n".join(captured.output)
        self.assertIn("Retrying SPY collection after attempt 1/3 failed", joined_logs)
        self.assertIn("Retrying SPY collection after attempt 2/3 failed", joined_logs)

    def test_returns_missing_data_for_empty_payload(self) -> None:
        provider = RecordingProvider([[]])
        collector = ETFPriceCollector(provider)

        with self.assertLogs(LOGGER_NAME, level="WARNING") as captured:
            result = collector.collect_latest(
                "SPY",
                current_time=eastern_datetime(2026, 3, 12, 10, 15, 8),
            )

        self.assertEqual(result.status, "missing_data")
        self.assertTrue(result.missing_data)
        self.assertEqual(result.records, ())
        self.assertIn("Missing price data for SPY", "\n".join(captured.output))

    def test_skips_incomplete_rows_but_keeps_valid_data(self) -> None:
        provider = RecordingProvider(
            [
                [
                    {
                        "timestamp": eastern_datetime(2026, 3, 12, 10, 14),
                        "open": 520.0,
                        "high": 521.0,
                        "low": 519.5,
                    },
                    price_bar(eastern_datetime(2026, 3, 12, 10, 14), close=520.75),
                ]
            ]
        )
        collector = ETFPriceCollector(provider)

        with self.assertLogs(LOGGER_NAME, level="WARNING") as captured:
            result = collector.collect_latest(
                "SPY",
                current_time=eastern_datetime(2026, 3, 12, 10, 15, 55),
            )

        self.assertEqual(result.status, "collected")
        self.assertTrue(result.missing_data)
        self.assertEqual(len(result.records), 1)
        self.assertAlmostEqual(result.records[0].close, 520.75)
        self.assertIn("Skipping incomplete bar 0 for SPY", "\n".join(captured.output))


if __name__ == "__main__":
    unittest.main()
