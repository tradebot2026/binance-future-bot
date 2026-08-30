"""
Binance Futures WebSocket + REST cache hub.
Streams live miniTicker data and caches historical candles per bar period
to minimize REST calls and prevent -1003 IP bans.
"""

from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import pandas as pd
from binance import ThreadedWebsocketManager
from binance.client import Client
from binance.exceptions import BinanceAPIException

from config import Config
from logger import error_logger, system_logger
from utils import safe_float


TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "12h": 43200,
    "1d": 86400,
}

_BAN_UNTIL_RE = re.compile(r"banned until\s+(\d+)", re.IGNORECASE)


@dataclass
class BanStatus:
    is_banned: bool
    message: str = ""
    banned_until_ms: Optional[int] = None
    banned_until_iso: str = ""

    @property
    def seconds_remaining(self) -> int:
        if not self.banned_until_ms:
            return 0
        remaining = (self.banned_until_ms - int(time.time() * 1000)) // 1000
        return max(int(remaining), 0)


@dataclass
class _CandleCacheEntry:
    dataframe: pd.DataFrame
    last_bar_open_ms: int
    fetched_at: float = field(default_factory=time.monotonic)


def parse_ban_until_ms(message: str) -> Optional[int]:
    match = _BAN_UNTIL_RE.search(message or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def check_binance_ban_status(client: Client) -> BanStatus:
    """
    Lightweight startup connectivity check.
    Returns ban details without retry spam if IP is temporarily blocked.
    """
    try:
        client.futures_ping()
        return BanStatus(is_banned=False, message="API reachable")
    except BinanceAPIException as exc:
        message = str(exc.message)
        if exc.code in (-1003, 418):
            until_ms = parse_ban_until_ms(message)
            until_iso = ""
            if until_ms:
                until_iso = datetime.fromtimestamp(
                    until_ms / 1000.0, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
            return BanStatus(
                is_banned=True,
                message=message,
                banned_until_ms=until_ms,
                banned_until_iso=until_iso,
            )
        return BanStatus(is_banned=False, message=message)
    except Exception as exc:
        return BanStatus(is_banned=False, message=str(exc))


class MarketDataHub:
    """In-memory price/ticker cache (WebSocket) + bar-aligned candle cache (REST)."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self._lock = threading.RLock()
        self._tickers: dict[str, dict[str, Any]] = {}
        self._book_tickers: dict[str, dict[str, Any]] = {}
        self._book_fetched_at: float = 0.0
        self._candles: dict[tuple[str, str, int], _CandleCacheEntry] = {}
        self._scan_halted_until: float = 0.0
        self._scan_halt_reason: str = ""
        self._ban_status: Optional[BanStatus] = None
        self._ws_manager: Optional[ThreadedWebsocketManager] = None
        self._ws_conn_key: Optional[str] = None
        self._ws_running = False
        self._last_ticker_event_at: float = 0.0

    # ---------------- Lifecycle ----------------

    def start(self) -> None:
        if not Config.ENABLE_WEBSOCKET_STREAMS or self._ws_running:
            return
        try:
            self._ws_manager = ThreadedWebsocketManager(
                api_key=Config.BINANCE_API_KEY,
                api_secret=Config.BINANCE_API_SECRET,
                testnet=Config.USE_TESTNET,
            )
            self._ws_manager.start()
            self._ws_conn_key = self._ws_manager.start_futures_multiplex_socket(
                callback=self._on_ws_message,
                streams=["!miniTicker@arr"],
            )
            self._ws_running = True
            system_logger.info(
                "WebSocket miniTicker stream started (!miniTicker@arr)."
            )
        except Exception as exc:
            error_logger.error("Failed to start WebSocket stream: %s", exc)

    def stop(self) -> None:
        if self._ws_manager is None:
            return
        try:
            if self._ws_conn_key:
                self._ws_manager.stop_socket(self._ws_conn_key)
            self._ws_manager.stop()
        except Exception as exc:
            error_logger.warning("WebSocket shutdown error: %s", exc)
        finally:
            self._ws_running = False
            self._ws_manager = None
            self._ws_conn_key = None

    def apply_startup_ban(self, ban: BanStatus) -> None:
        if not ban.is_banned:
            return
        self._ban_status = ban
        halt_seconds = Config.RATE_LIMIT_HALT_SECONDS
        if ban.seconds_remaining > 0:
            halt_seconds = max(halt_seconds, ban.seconds_remaining)
        self.halt_scanning(halt_seconds, f"IP banned: {ban.message}")

    # ---------------- WebSocket handler ----------------

    def _on_ws_message(self, message: dict[str, Any]) -> None:
        try:
            payload = message.get("data", message)
            if isinstance(payload, list):
                rows = payload
            else:
                rows = [payload]

            now = time.monotonic()
            with self._lock:
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    symbol = str(row.get("s", "")).upper()
                    if not symbol:
                        continue
                    price = safe_float(row.get("c"))
                    if price <= 0:
                        continue
                    self._tickers[symbol] = {
                        "symbol": symbol,
                        "lastPrice": price,
                        "price": price,
                        "quoteVolume": safe_float(row.get("q")),
                        "volume": safe_float(row.get("v")),
                        "highPrice": safe_float(row.get("h")),
                        "lowPrice": safe_float(row.get("l")),
                        "openPrice": safe_float(row.get("o")),
                        "updated_at": now,
                    }
                self._last_ticker_event_at = now
        except Exception as exc:
            error_logger.warning("WebSocket message parse error: %s", exc)

    # ---------------- Scan halt / ban recovery ----------------

    def halt_scanning(self, seconds: int, reason: str) -> None:
        until = time.monotonic() + max(seconds, 0)
        with self._lock:
            self._scan_halted_until = max(self._scan_halted_until, until)
            self._scan_halt_reason = reason
        error_logger.critical(
            "Scanning halted for %ss | reason=%s", seconds, reason
        )

    def is_scan_halted(self) -> tuple[bool, str]:
        with self._lock:
            if time.monotonic() < self._scan_halted_until:
                remaining = int(self._scan_halted_until - time.monotonic())
                reason = self._scan_halt_reason or "rate_limit_halt"
                return True, f"{reason} (resumes in ~{remaining}s)"
            return False, ""

    def handle_rate_limit_error(self, exc: Exception) -> None:
        message = str(exc)
        until_ms = parse_ban_until_ms(message)
        halt_seconds = Config.RATE_LIMIT_HALT_SECONDS
        if until_ms:
            remaining = (until_ms - int(time.time() * 1000)) // 1000
            halt_seconds = max(halt_seconds, remaining)
            until_iso = datetime.fromtimestamp(
                until_ms / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
            self._ban_status = BanStatus(
                is_banned=True,
                message=message,
                banned_until_ms=until_ms,
                banned_until_iso=until_iso,
            )
        self.halt_scanning(halt_seconds, message)

    def get_ban_status(self) -> Optional[BanStatus]:
        return self._ban_status

    # ---------------- Ticker cache ----------------

    def get_price(self, symbol: str) -> Optional[float]:
        symbol = symbol.upper()
        with self._lock:
            row = self._tickers.get(symbol)
            if row:
                price = safe_float(row.get("lastPrice"))
                return price if price > 0 else None
        return None

    def get_ticker_map(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._tickers)

    def ws_is_stale(self) -> bool:
        if self._last_ticker_event_at <= 0:
            return True
        return (time.monotonic() - self._last_ticker_event_at) > Config.WS_STALE_SECONDS

    def get_book_ticker_map(
        self,
        rest_fetcher: Callable[[], dict[str, dict[str, Any]]],
    ) -> dict[str, dict[str, Any]]:
        """Return cached book tickers; refresh via REST at most once per TTL."""
        now = time.monotonic()
        with self._lock:
            if (
                self._book_tickers
                and (now - self._book_fetched_at) < Config.BOOK_TICKER_CACHE_SECONDS
            ):
                return dict(self._book_tickers)

        halted, _ = self.is_scan_halted()
        if halted:
            with self._lock:
                return dict(self._book_tickers)

        try:
            fresh = rest_fetcher() or {}
            with self._lock:
                if fresh:
                    self._book_tickers = fresh
                    self._book_fetched_at = now
                return dict(self._book_tickers)
        except Exception as exc:
            error_logger.warning("Book ticker REST refresh failed: %s", exc)
            with self._lock:
                return dict(self._book_tickers)

    # ---------------- Candle cache ----------------

    @staticmethod
    def _current_bar_open_ms(timeframe: str) -> int:
        tf_sec = TIMEFRAME_SECONDS.get(timeframe, 300)
        now_ms = int(time.time() * 1000)
        bar_ms = tf_sec * 1000
        return (now_ms // bar_ms) * bar_ms

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        rest_fetcher: Callable[[], pd.DataFrame],
    ) -> pd.DataFrame:
        """Return cached candles; refetch REST only when a new bar opens."""
        key = (symbol.upper(), timeframe, int(limit))
        bar_open_ms = self._current_bar_open_ms(timeframe)
        cached: Optional[_CandleCacheEntry] = None

        with self._lock:
            cached = self._candles.get(key)
            if cached and cached.last_bar_open_ms == bar_open_ms and not cached.dataframe.empty:
                return cached.dataframe.copy()

        halted, _ = self.is_scan_halted()
        if halted and cached is not None:
            return cached.dataframe.copy()

        df = rest_fetcher()
        if df.empty:
            if cached is not None:
                return cached.dataframe.copy()
            return pd.DataFrame()

        with self._lock:
            self._candles[key] = _CandleCacheEntry(
                dataframe=df.copy(),
                last_bar_open_ms=bar_open_ms,
            )
        return df.copy()

    def seed_tickers_from_rest(self, ticker_map: dict[str, dict[str, Any]]) -> None:
        now = time.monotonic()
        with self._lock:
            for symbol, row in ticker_map.items():
                price = safe_float(row.get("lastPrice"))
                if price <= 0:
                    continue
                sym = str(symbol).upper()
                self._tickers[sym] = {
                    "symbol": sym,
                    "lastPrice": price,
                    "price": price,
                    "quoteVolume": safe_float(row.get("quoteVolume")),
                    "volume": safe_float(row.get("volume")),
                    "updated_at": now,
                }

    def format_ban_message(self, ban: BanStatus) -> str:
        if ban.banned_until_iso:
            return (
                f"🚫 <b>Binance IP temporarily banned</b>\n\n"
                f"Until: {ban.banned_until_iso}\n"
                f"Remaining: ~{ban.seconds_remaining}s\n"
                f"Detail: {ban.message}\n\n"
                f"<i>Scanning paused; position monitoring continues.</i>"
            )
        return (
            f"🚫 <b>Binance rate limit / IP ban detected</b>\n\n"
            f"{ban.message}\n\n"
            f"<i>Scanning paused for {Config.RATE_LIMIT_HALT_SECONDS}s.</i>"
        )
