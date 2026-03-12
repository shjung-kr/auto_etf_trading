from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, time, timedelta
import logging
from time import sleep as blocking_sleep
from typing import Any, Protocol
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)
INTERVAL = "1m"
MARKET_OPEN = time(hour=9, minute=30)
MARKET_CLOSE = time(hour=16, minute=0)
ONE_MINUTE = timedelta(minutes=1)
REQUIRED_FIELDS = ("timestamp", "open", "high", "low", "close")


class PriceFeed(Protocol):
    def fetch_prices(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: str,
    ) -> Iterable[Mapping[str, Any]] | None:
        """Fetch price bars for a symbol and interval."""


@dataclass(frozen=True)
class PriceBar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int | None = None


@dataclass(frozen=True)
class CollectionResult:
    symbol: str
    interval: str
    window_start: datetime
    window_end: datetime
    records: tuple[PriceBar, ...]
    attempts: int
    status: str
    missing_data: bool = False
    error: str | None = None


def _utc_now() -> datetime:
    return datetime.now(tz=ZoneInfo("UTC"))


class ETFPriceCollector:
    def __init__(
        self,
        provider: PriceFeed,
        *,
        market_timezone: str = "America/New_York",
        max_retries: int = 2,
        retry_delay_seconds: float = 0.0,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] = blocking_sleep,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be greater than or equal to zero")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be greater than or equal to zero")

        self.provider = provider
        self.market_timezone = ZoneInfo(market_timezone)
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.clock = clock or _utc_now
        self.sleep = sleep

    def is_market_session(self, current_time: datetime) -> bool:
        market_time = self._normalize_datetime(current_time).astimezone(self.market_timezone)
        if market_time.weekday() >= 5:
            return False
        wall_time = market_time.timetz().replace(tzinfo=None)
        return MARKET_OPEN <= wall_time < MARKET_CLOSE

    def collect_latest(
        self,
        symbol: str,
        *,
        current_time: datetime | None = None,
    ) -> CollectionResult:
        observed_at = self._normalize_datetime(current_time or self.clock())
        window_start, window_end = self._collection_window(observed_at)

        if not self._is_collectible_window(window_start, window_end):
            LOGGER.info(
                "Skipping %s collection outside collectible market window at %s",
                symbol,
                observed_at.astimezone(self.market_timezone).isoformat(),
            )
            return CollectionResult(
                symbol=symbol,
                interval=INTERVAL,
                window_start=window_start,
                window_end=window_end,
                records=(),
                attempts=0,
                status="market_closed",
            )

        attempts = 0
        total_attempts = self.max_retries + 1
        while attempts < total_attempts:
            attempts += 1
            try:
                payload = self.provider.fetch_prices(
                    symbol=symbol,
                    start=window_start,
                    end=window_end,
                    interval=INTERVAL,
                )
                records, missing_data = self._parse_payload(symbol, payload)
                if not records:
                    LOGGER.warning(
                        "Missing price data for %s between %s and %s",
                        symbol,
                        window_start.isoformat(),
                        window_end.isoformat(),
                    )
                    return CollectionResult(
                        symbol=symbol,
                        interval=INTERVAL,
                        window_start=window_start,
                        window_end=window_end,
                        records=(),
                        attempts=attempts,
                        status="missing_data",
                        missing_data=True,
                    )

                LOGGER.info(
                    "Collected %s 1-minute bars for %s between %s and %s",
                    len(records),
                    symbol,
                    window_start.isoformat(),
                    window_end.isoformat(),
                )
                return CollectionResult(
                    symbol=symbol,
                    interval=INTERVAL,
                    window_start=window_start,
                    window_end=window_end,
                    records=records,
                    attempts=attempts,
                    status="collected",
                    missing_data=missing_data,
                )
            except Exception as error:
                if attempts >= total_attempts:
                    LOGGER.error(
                        "Failed to collect %s after %s attempts: %s",
                        symbol,
                        attempts,
                        error,
                    )
                    return CollectionResult(
                        symbol=symbol,
                        interval=INTERVAL,
                        window_start=window_start,
                        window_end=window_end,
                        records=(),
                        attempts=attempts,
                        status="failed",
                        error=str(error),
                    )

                LOGGER.warning(
                    "Retrying %s collection after attempt %s/%s failed: %s",
                    symbol,
                    attempts,
                    total_attempts,
                    error,
                )
                if self.retry_delay_seconds:
                    self.sleep(self.retry_delay_seconds)

        raise AssertionError("unreachable")

    def _collection_window(self, current_time: datetime) -> tuple[datetime, datetime]:
        market_time = current_time.astimezone(self.market_timezone)
        window_end = market_time.replace(second=0, microsecond=0)
        return window_end - ONE_MINUTE, window_end

    def _is_collectible_window(self, window_start: datetime, window_end: datetime) -> bool:
        localized_start = self._normalize_datetime(window_start).astimezone(self.market_timezone)
        localized_end = self._normalize_datetime(window_end).astimezone(self.market_timezone)

        if localized_start.date() != localized_end.date():
            return False
        if localized_start.weekday() >= 5:
            return False

        start_time = localized_start.timetz().replace(tzinfo=None)
        end_time = localized_end.timetz().replace(tzinfo=None)
        return MARKET_OPEN <= start_time and end_time <= MARKET_CLOSE

    def _parse_payload(
        self,
        symbol: str,
        payload: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None,
    ) -> tuple[tuple[PriceBar, ...], bool]:
        if payload is None:
            return (), True

        rows: list[Mapping[str, Any]]
        if isinstance(payload, Mapping):
            rows = [payload]
        else:
            rows = list(payload)

        parsed_rows: list[PriceBar] = []
        missing_data = False
        for index, row in enumerate(rows):
            missing_fields = [field for field in REQUIRED_FIELDS if row.get(field) is None]
            if missing_fields:
                missing_data = True
                LOGGER.warning(
                    "Skipping incomplete bar %s for %s; missing %s",
                    index,
                    symbol,
                    ", ".join(missing_fields),
                )
                continue

            parsed_rows.append(
                PriceBar(
                    symbol=symbol,
                    timestamp=self._normalize_datetime(row["timestamp"]),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=None if row.get("volume") is None else int(row["volume"]),
                )
            )

        return tuple(parsed_rows), missing_data

    def _normalize_datetime(self, value: datetime | str) -> datetime:
        if isinstance(value, datetime):
            candidate = value
        elif isinstance(value, str):
            candidate = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            raise TypeError(f"Unsupported timestamp value: {value!r}")

        if candidate.tzinfo is None:
            return candidate.replace(tzinfo=self.market_timezone)
        return candidate
