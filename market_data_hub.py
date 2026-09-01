"""
Binance Futures WebSocket hub — primary data plane.
Streams miniTicker, klines, and user-data (positions/PnL).
REST is fallback-only outside scan cycles and never during IP bans.
"""

from __future__ import annotations

import re
import threading
import time
from collections import deque
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
from ws_reconnect import (
    WsLogSuppressor,
    WsReconnectPolicy,
    configure_binance_ws_logging,
    is_read_loop_closed_error,
    is_ws_error_message,
)


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
_KLINE_MULTIPLEX_CHUNK = 180


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


class MarketDataHub:
    """In-memory WebSocket cache: tickers, klines, positions, unrealized PnL."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self._lock = threading.RLock()
        self._tickers: dict[str, dict[str, Any]] = {}
        self._book_tickers: dict[str, dict[str, Any]] = {}
        self._book_fetched_at: float = 0.0
        self._candles: dict[tuple[str, str, int], _CandleCacheEntry] = {}
        self._kline_bars: dict[tuple[str, str], deque[dict[str, Any]]] = {}
        self._scan_halted_until: float = 0.0
        self._scan_halt_reason: str = ""
        self._rest_blocked_until: float = 0.0
        self._rest_block_reason: str = ""
        self._ban_status: Optional[BanStatus] = None
        self._ws_manager: Optional[ThreadedWebsocketManager] = None
        self._ticker_conn_key: Optional[str] = None
        self._book_ticker_conn_key: Optional[str] = None
        self._user_conn_key: Optional[str] = None
        self._kline_conn_keys: list[str] = []
        self._subscribed_kline_streams: set[str] = set()
        self._ws_running = False
        self._last_ticker_event_at: float = 0.0
        self._last_user_event_at: float = 0.0
        self._positions: list[dict[str, Any]] = []
        self._unrealized_pnl_total: float = 0.0
        self._wallet_balances: dict[str, float] = {}
        self._ban_notice_logged: bool = False
        self._ws_started_at: float = 0.0
        self._reconnect_lock = threading.Lock()
        self._reconnect_in_progress = False
        self._last_reconnect_request_at: float = 0.0
        self._watchdog_stop = threading.Event()
        self._watchdog_thread: Optional[threading.Thread] = None
        self._reconnect_policy = WsReconnectPolicy(
            min_seconds=Config.WS_RECONNECT_MIN_SECONDS,
            max_seconds=Config.WS_RECONNECT_MAX_SECONDS,
        )
        self._ws_log = WsLogSuppressor(Config.WS_RECONNECT_LOG_INTERVAL_SECONDS)
        configure_binance_ws_logging()

    def ws_is_running(self) -> bool:
        return self._ws_running

    def wait_until_ready(
        self,
        timeout_seconds: Optional[int] = None,
        min_symbols: int = 30,
    ) -> bool:
        """
        Block until miniTicker WS cache has enough symbols (startup warm-up).
        Returns False on timeout — caller must NOT fall back to REST tickers.
        """
        timeout = timeout_seconds or Config.WS_STARTUP_WAIT_SECONDS
        deadline = time.monotonic() + max(timeout, 1)
        while time.monotonic() < deadline:
            count = len(self.get_ticker_map())
            if count >= min_symbols:
                system_logger.info(
                    "WebSocket ticker cache ready (%s symbols).", count
                )
                return True
            time.sleep(0.5)
        count = len(self.get_ticker_map())
        if count > 0:
            system_logger.warning(
                "WebSocket warm-up partial — %s symbols (wanted >= %s).",
                count,
                min_symbols,
            )
            return True
        system_logger.warning(
            "WebSocket ticker cache empty after %ss — REST ticker fallback disabled.",
            timeout,
        )
        return False

    def is_ws_warming_up(self) -> bool:
        if not self._ws_running:
            return False
        if self._ws_started_at <= 0:
            return True
        if self._last_ticker_event_at <= 0:
            return (time.monotonic() - self._ws_started_at) < Config.WS_WARMUP_SECONDS
        return False

    def get_rest_block_remaining_seconds(self) -> int:
        with self._lock:
            return max(int(self._rest_blocked_until - time.time()), 0)

    def get_ws_wallet_balance(self, asset: str = "USDT") -> float:
        asset = asset.upper()
        with self._lock:
            return safe_float(self._wallet_balances.get(asset))

    def start(self) -> None:
        if not Config.ENABLE_WEBSOCKET_STREAMS:
            return
        if self._ws_running:
            return
        try:
            self._start_ws_internal()
            self._start_watchdog()
        except Exception as exc:
            error_logger.error("Failed to start WebSocket streams: %s", exc)

    def _start_ws_internal(self) -> None:
        """Create ThreadedWebsocketManager and subscribe base + kline streams."""
        self._ws_manager = ThreadedWebsocketManager(
            api_key=Config.BINANCE_API_KEY,
            api_secret=Config.BINANCE_API_SECRET,
            testnet=Config.USE_TESTNET,
        )
        self._ws_manager.start()
        self._ticker_conn_key = self._ws_manager.start_futures_multiplex_socket(
            callback=self._wrap_ws_callback(self._on_ticker_message),
            streams=["!miniTicker@arr"],
        )
        if Config.ENABLE_WS_BOOK_STREAM:
            self._book_ticker_conn_key = self._ws_manager.start_futures_multiplex_socket(
                callback=self._wrap_ws_callback(self._on_book_ticker_message),
                streams=["!bookTicker@arr"],
            )
        self._user_conn_key = self._ws_manager.start_futures_user_socket(
            callback=self._wrap_ws_callback(self._on_user_message),
        )
        self._ws_running = True
        self._ws_started_at = time.monotonic()
        self._resubscribe_kline_streams()
        streams = "miniTicker + user data"
        if Config.ENABLE_WS_BOOK_STREAM:
            streams += " + bookTicker"
        system_logger.info("WebSocket streams started (%s).", streams)

    def _start_watchdog(self) -> None:
        if not Config.WS_RECONNECT_ENABLED:
            return
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop.clear()
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name="ws-health-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

    def _watchdog_loop(self) -> None:
        interval = max(Config.WS_HEALTH_CHECK_SECONDS, 10)
        while not self._watchdog_stop.wait(interval):
            if not self._ws_running or self._reconnect_in_progress:
                continue
            if self.is_ws_warming_up():
                continue
            if self.ws_is_stale():
                self._request_reconnect("ticker stream stale — no events received")

    def _wrap_ws_callback(
        self, handler: Callable[[dict[str, Any]], None]
    ) -> Callable[[dict[str, Any]], None]:
        """Catch library error passthrough and read-loop failures."""

        def _wrapped(message: dict[str, Any]) -> None:
            if is_ws_error_message(message):
                detail = str(message.get("m", message.get("type", "ws error")))
                if is_read_loop_closed_error(detail):
                    self._request_reconnect(detail)
                return
            try:
                handler(message)
            except Exception as exc:
                if is_read_loop_closed_error(exc):
                    self._request_reconnect(str(exc))
                elif self._ws_log.should_log(f"callback:{type(exc).__name__}"):
                    error_logger.warning("WebSocket callback error: %s", exc)

        return _wrapped

    def _request_reconnect(self, reason: str) -> None:
        if not Config.WS_RECONNECT_ENABLED or not Config.ENABLE_WEBSOCKET_STREAMS:
            return

        now = time.monotonic()
        if (now - self._last_reconnect_request_at) < Config.WS_RECONNECT_DEBOUNCE_SECONDS:
            return
        self._last_reconnect_request_at = now

        with self._reconnect_lock:
            if self._reconnect_in_progress:
                return
            self._reconnect_in_progress = True

        if self._ws_log.should_log(reason):
            system_logger.warning(
                "WebSocket disconnect detected (%s) — scheduling reconnect.",
                reason,
            )

        threading.Thread(
            target=self._reconnect_worker,
            args=(reason,),
            name="ws-reconnect",
            daemon=True,
        ).start()

    def _reconnect_worker(self, reason: str) -> None:
        try:
            delay = self._reconnect_policy.next_delay()
            if self._ws_log.should_log(f"backoff:{delay:.0f}s"):
                system_logger.info(
                    "WebSocket reconnect in %.1fs (attempt %s).",
                    delay,
                    self._reconnect_policy.attempt,
                )
            time.sleep(delay)

            self._stop_ws_internal(preserve_kline_subscriptions=True)
            self._start_ws_internal()
            self._reconnect_policy.reset()
            self._ws_log.reset()
            system_logger.info("WebSocket reconnected successfully.")
        except Exception as exc:
            if self._ws_log.should_log(f"reconnect_failed:{exc}"):
                error_logger.error(
                    "WebSocket reconnect failed (attempt %s): %s",
                    self._reconnect_policy.attempt,
                    exc,
                )
        finally:
            with self._reconnect_lock:
                self._reconnect_in_progress = False

    def _stop_ws_internal(self, *, preserve_kline_subscriptions: bool) -> None:
        if self._ws_manager is None:
            self._ws_running = False
            return
        try:
            for key in list(self._kline_conn_keys):
                try:
                    self._ws_manager.stop_socket(key)
                except Exception:
                    pass
            if self._user_conn_key:
                try:
                    self._ws_manager.stop_socket(self._user_conn_key)
                except Exception:
                    pass
            if self._book_ticker_conn_key:
                try:
                    self._ws_manager.stop_socket(self._book_ticker_conn_key)
                except Exception:
                    pass
            if self._ticker_conn_key:
                try:
                    self._ws_manager.stop_socket(self._ticker_conn_key)
                except Exception:
                    pass
            self._ws_manager.stop()
        except Exception as exc:
            if self._ws_log.should_log(f"ws_stop:{exc}"):
                error_logger.warning("WebSocket shutdown error: %s", exc)
        finally:
            self._ws_running = False
            self._ws_manager = None
            self._ticker_conn_key = None
            self._book_ticker_conn_key = None
            self._user_conn_key = None
            self._kline_conn_keys.clear()
            if not preserve_kline_subscriptions:
                self._subscribed_kline_streams.clear()

    def _resubscribe_kline_streams(self) -> None:
        """Re-open kline multiplex sockets after reconnect."""
        if not self._ws_manager or not self._subscribed_kline_streams:
            return
        streams = sorted(self._subscribed_kline_streams)
        opened = 0
        for i in range(0, len(streams), _KLINE_MULTIPLEX_CHUNK):
            chunk = streams[i : i + _KLINE_MULTIPLEX_CHUNK]
            try:
                conn_key = self._ws_manager.start_futures_multiplex_socket(
                    callback=self._wrap_ws_callback(self._on_kline_multiplex),
                    streams=chunk,
                )
                self._kline_conn_keys.append(conn_key)
                opened += len(chunk)
            except Exception as exc:
                error_logger.error("Kline WS resubscribe failed: %s", exc)
        if opened:
            system_logger.info(
                "Re-subscribed %s kline WS streams after reconnect.", opened
            )

    def stop(self) -> None:
        self._watchdog_stop.set()
        self._stop_ws_internal(preserve_kline_subscriptions=False)

    def _on_book_ticker_message(self, message: dict[str, Any]) -> None:
        try:
            payload = message.get("data", message)
            rows = payload if isinstance(payload, list) else [payload]
            now = time.monotonic()
            with self._lock:
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    symbol = str(row.get("s", "")).upper()
                    if not symbol:
                        continue
                    bid = safe_float(row.get("b"))
                    ask = safe_float(row.get("a"))
                    if bid <= 0 or ask <= 0:
                        continue
                    self._book_tickers[symbol] = {
                        "symbol": symbol,
                        "bidPrice": bid,
                        "askPrice": ask,
                        "is_proxy": False,
                        "updated_at": now,
                    }
                self._book_fetched_at = now
        except Exception as exc:
            error_logger.warning("Book ticker WS parse error: %s", exc)

    def get_ws_book_ticker_map(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._book_tickers)

    def has_ws_book_data(self) -> bool:
        with self._lock:
            return len(self._book_tickers) > 0

    def apply_startup_ban(self, ban: BanStatus) -> None:
        if not ban.is_banned:
            return
        self._ban_status = ban
        self.block_rest_for_ban(ban.message, ban.banned_until_ms)
        halt_seconds = Config.RATE_LIMIT_HALT_SECONDS
        if ban.seconds_remaining > 0:
            halt_seconds = max(halt_seconds, ban.seconds_remaining)
        self.halt_scanning(halt_seconds, f"IP banned: {ban.message}")

    # ---------------- REST ban gate ----------------

    def block_rest_for_ban(
        self, message: str, banned_until_ms: Optional[int] = None
    ) -> None:
        until_ms = banned_until_ms or parse_ban_until_ms(message)
        halt_seconds = max(Config.REST_BAN_MIN_SLEEP_SECONDS, Config.RATE_LIMIT_HALT_SECONDS)
        if until_ms:
            remaining = max((until_ms / 1000.0) - time.time(), 0.0)
            halt_seconds = max(halt_seconds, int(remaining))
            until_iso = datetime.fromtimestamp(
                until_ms / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
            self._ban_status = BanStatus(
                is_banned=True,
                message=message,
                banned_until_ms=int(until_ms),
                banned_until_iso=until_iso,
            )
        with self._lock:
            was_active = time.time() < self._rest_blocked_until
            self._rest_blocked_until = max(
                self._rest_blocked_until, time.time() + halt_seconds
            )
            self._rest_block_reason = message
        self.halt_scanning(halt_seconds, message)
        if not was_active:
            self._ban_notice_logged = False
            error_logger.critical(
                "ALL REST API calls blocked for ~%ss | %s", halt_seconds, message
            )

    def is_rest_blocked(self) -> tuple[bool, str]:
        with self._lock:
            if time.time() < self._rest_blocked_until:
                remaining = int(self._rest_blocked_until - time.time())
                reason = self._rest_block_reason or "rate_limit_ban"
                return True, f"{reason} (REST resumes in ~{remaining}s)"
            if self._ban_notice_logged:
                self._ban_notice_logged = False
            return False, ""

    def log_ban_pause_once(self, remaining_seconds: int) -> None:
        """Log a single pause notice per ban window (avoids log spam)."""
        with self._lock:
            if self._ban_notice_logged:
                return
            self._ban_notice_logged = True
        ban = self._ban_status
        until = ban.banned_until_iso if ban and ban.banned_until_iso else "unknown"
        system_logger.warning(
            "Binance REST/IP ban active — main loop paused ~%ss (until %s). "
            "Scanning and REST polling suspended; WebSocket cache only.",
            remaining_seconds,
            until,
        )

    def handle_rate_limit_error(self, exc: Exception) -> None:
        message = str(exc)
        until_ms = parse_ban_until_ms(message)
        self.block_rest_for_ban(message, until_ms)

    # ---------------- Scan halt ----------------

    def halt_scanning(self, seconds: int, reason: str) -> None:
        until = time.monotonic() + max(seconds, 0)
        with self._lock:
            self._scan_halted_until = max(self._scan_halted_until, until)
            self._scan_halt_reason = reason

    def is_scan_halted(self) -> tuple[bool, str]:
        with self._lock:
            if time.monotonic() < self._scan_halted_until:
                remaining = int(self._scan_halted_until - time.monotonic())
                reason = self._scan_halt_reason or "rate_limit_halt"
                return True, f"{reason} (resumes in ~{remaining}s)"
            return False, ""

    def get_ban_status(self) -> Optional[BanStatus]:
        return self._ban_status

    # ---------------- WebSocket handlers ----------------

    def _on_ticker_message(self, message: dict[str, Any]) -> None:
        try:
            payload = message.get("data", message)
            rows = payload if isinstance(payload, list) else [payload]
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
            error_logger.warning("Ticker WS parse error: %s", exc)

    def _on_user_message(self, message: dict[str, Any]) -> None:
        try:
            event = message.get("e")
            if event == "ACCOUNT_UPDATE":
                account = message.get("a", {})
                balances_raw = account.get("B", [])
                positions_raw = account.get("P", [])
                parsed: list[dict[str, Any]] = []
                unrealized_total = 0.0
                for pos in positions_raw:
                    quantity = abs(safe_float(pos.get("pa")))
                    if quantity <= 0:
                        continue
                    upnl = safe_float(pos.get("up"))
                    unrealized_total += upnl
                    parsed.append(
                        {
                            "symbol": str(pos.get("s", "")).upper(),
                            "positionSide": str(pos.get("ps", "")),
                            "quantity": quantity,
                            "entry_price": safe_float(pos.get("ep")),
                            "unrealized_pnl": upnl,
                        }
                    )
                with self._lock:
                    for bal in balances_raw:
                        asset = str(bal.get("a", "")).upper()
                        if not asset:
                            continue
                        cross_wallet = safe_float(bal.get("cw"))
                        wallet = safe_float(bal.get("wb"))
                        value = cross_wallet if cross_wallet > 0 else wallet
                        if value > 0:
                            self._wallet_balances[asset] = value
                    self._positions = parsed
                    self._unrealized_pnl_total = unrealized_total
                    self._last_user_event_at = time.monotonic()
            elif event in ("ORDER_TRADE_UPDATE", "ACCOUNT_CONFIG_UPDATE"):
                self._last_user_event_at = time.monotonic()
        except Exception as exc:
            error_logger.warning("User WS parse error: %s", exc)

    def _on_kline_multiplex(self, message: dict[str, Any]) -> None:
        payload = message.get("data", message)
        if isinstance(payload, dict):
            self._on_kline_message(payload)

    def _on_kline_message(self, message: dict[str, Any]) -> None:
        try:
            kline = message.get("k") or message
            symbol = str(kline.get("s", message.get("s", ""))).upper()
            interval = str(kline.get("i", ""))
            if not symbol or not interval:
                return

            row = {
                "timestamp": pd.to_datetime(int(kline["t"]), unit="ms"),
                "open": safe_float(kline.get("o")),
                "high": safe_float(kline.get("h")),
                "low": safe_float(kline.get("l")),
                "close": safe_float(kline.get("c")),
                "volume": safe_float(kline.get("v")),
                "open_ms": int(kline["t"]),
                "closed": bool(kline.get("x")),
            }
            key = (symbol, interval)
            with self._lock:
                bars = self._kline_bars.setdefault(
                    key, deque(maxlen=Config.WS_KLINE_BUFFER_LIMIT)
                )
                if bars and bars[-1]["open_ms"] == row["open_ms"]:
                    bars[-1] = row
                elif row["closed"] or not bars:
                    if bars and bars[-1]["open_ms"] == row["open_ms"]:
                        bars[-1] = row
                    else:
                        bars.append(row)
                else:
                    bars.append(row)
                self._sync_candles_from_klines(symbol, interval)
        except Exception as exc:
            error_logger.warning("Kline WS parse error: %s", exc)

    def _sync_candles_from_klines(self, symbol: str, interval: str) -> None:
        """Rebuild bar-aligned candle cache entry from WS kline buffer."""
        key = (symbol, interval)
        bars = self._kline_bars.get(key)
        if not bars:
            return
        limit = Config.CANDLE_FETCH_LIMIT
        rows = list(bars)[-limit:]
        if len(rows) < 10:
            return
        df = pd.DataFrame(rows)[["timestamp", "open", "high", "low", "close", "volume"]]
        bar_open_ms = self._current_bar_open_ms(interval)
        cache_key = (symbol, interval, limit)
        self._candles[cache_key] = _CandleCacheEntry(
            dataframe=df.copy(),
            last_bar_open_ms=bar_open_ms,
        )

    def subscribe_kline_streams(
        self, symbols: list[str], intervals: list[str]
    ) -> None:
        """Subscribe WS kline streams for scan universe (multiplex, chunked)."""
        if not self._ws_manager or not self._ws_running:
            return

        new_streams: list[str] = []
        for symbol in symbols:
            sym = symbol.lower()
            for interval in intervals:
                stream = f"{sym}@kline_{interval}"
                if stream not in self._subscribed_kline_streams:
                    new_streams.append(stream)
                    self._subscribed_kline_streams.add(stream)

        if not new_streams:
            return

        for i in range(0, len(new_streams), _KLINE_MULTIPLEX_CHUNK):
            chunk = new_streams[i : i + _KLINE_MULTIPLEX_CHUNK]
            try:
                conn_key = self._ws_manager.start_futures_multiplex_socket(
                    callback=self._wrap_ws_callback(self._on_kline_multiplex),
                    streams=chunk,
                )
                self._kline_conn_keys.append(conn_key)
            except Exception as exc:
                error_logger.error("Kline WS subscribe failed: %s", exc)

        system_logger.info(
            "Subscribed %s new kline WS streams (%s total).",
            len(new_streams),
            len(self._subscribed_kline_streams),
        )

    def seed_klines_from_dataframe(
        self, symbol: str, interval: str, df: pd.DataFrame
    ) -> None:
        """Seed WS kline buffer from a one-time REST bootstrap (outside scan loops)."""
        if df.empty:
            return
        symbol = symbol.upper()
        key = (symbol, interval)
        with self._lock:
            bars: deque[dict[str, Any]] = deque(maxlen=Config.WS_KLINE_BUFFER_LIMIT)
            for _, row in df.iterrows():
                ts = row["timestamp"]
                open_ms = int(pd.Timestamp(ts).timestamp() * 1000)
                bars.append(
                    {
                        "timestamp": ts,
                        "open": safe_float(row.get("open")),
                        "high": safe_float(row.get("high")),
                        "low": safe_float(row.get("low")),
                        "close": safe_float(row.get("close")),
                        "volume": safe_float(row.get("volume")),
                        "open_ms": open_ms,
                        "closed": True,
                    }
                )
            self._kline_bars[key] = bars
            self._sync_candles_from_klines(symbol, interval)

    # ---------------- Ticker / book ----------------

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
        if not self._ws_running:
            return True
        if self.is_ws_warming_up():
            return False
        if self._last_ticker_event_at <= 0:
            return True
        return (time.monotonic() - self._last_ticker_event_at) > Config.WS_STALE_SECONDS

    def user_stream_is_stale(self) -> bool:
        if not self._ws_running:
            return True
        if self._ws_started_at > 0 and self._last_user_event_at <= 0:
            warming = (time.monotonic() - self._ws_started_at) < Config.WS_WARMUP_SECONDS
            if warming:
                return False
        if self._last_user_event_at <= 0:
            return True
        return (time.monotonic() - self._last_user_event_at) > Config.WS_USER_STALE_SECONDS

    def get_book_ticker_map(
        self,
        rest_fetcher: Optional[Callable[[], dict[str, dict[str, Any]]]] = None,
        *,
        allow_rest: bool = True,
    ) -> dict[str, dict[str, Any]]:
        now = time.monotonic()
        with self._lock:
            if (
                self._book_tickers
                and (now - self._book_fetched_at) < Config.BOOK_TICKER_CACHE_SECONDS
            ):
                return dict(self._book_tickers)

        blocked, _ = self.is_rest_blocked()
        if blocked or not allow_rest or rest_fetcher is None:
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

    # ---------------- Positions (user stream) ----------------

    def get_ws_positions(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._positions)

    def get_ws_unrealized_pnl_total(self) -> float:
        with self._lock:
            return self._unrealized_pnl_total

    def get_ws_position_quantity(self, symbol: str, position_side: str) -> Optional[float]:
        symbol = symbol.upper()
        position_side = position_side.upper()
        with self._lock:
            for pos in self._positions:
                if pos.get("symbol") == symbol and pos.get("positionSide") == position_side:
                    return safe_float(pos.get("quantity"))
        return None

    # ---------------- Candles ----------------

    @staticmethod
    def _current_bar_open_ms(timeframe: str) -> int:
        tf_sec = TIMEFRAME_SECONDS.get(timeframe, 300)
        now_ms = int(time.time() * 1000)
        bar_ms = tf_sec * 1000
        return (now_ms // bar_ms) * bar_ms

    def get_candles_cached_only(
        self, symbol: str, timeframe: str, limit: int
    ) -> pd.DataFrame:
        """WebSocket / memory cache ONLY — never calls REST."""
        symbol = symbol.upper()
        key = (symbol, timeframe, int(limit))
        with self._lock:
            cached = self._candles.get(key)
            if cached and not cached.dataframe.empty:
                return cached.dataframe.copy()

            kline_key = (symbol, timeframe)
            bars = self._kline_bars.get(kline_key)
            if bars and len(bars) >= 10:
                rows = list(bars)[-limit:]
                df = pd.DataFrame(rows)[
                    ["timestamp", "open", "high", "low", "close", "volume"]
                ]
                return df.copy()

        return pd.DataFrame()

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        rest_fetcher: Optional[Callable[[], pd.DataFrame]] = None,
        *,
        allow_rest: bool = True,
    ) -> pd.DataFrame:
        """Return cached candles; optional REST refresh when allowed and not banned."""
        cached = self.get_candles_cached_only(symbol, timeframe, limit)
        bar_open_ms = self._current_bar_open_ms(timeframe)
        key = (symbol.upper(), timeframe, int(limit))

        with self._lock:
            entry = self._candles.get(key)
            if (
                entry
                and entry.last_bar_open_ms == bar_open_ms
                and not entry.dataframe.empty
            ):
                return entry.dataframe.copy()

        blocked, _ = self.is_rest_blocked()
        if blocked or not allow_rest or rest_fetcher is None:
            return cached

        df = rest_fetcher()
        if df.empty:
            return cached

        with self._lock:
            self._candles[key] = _CandleCacheEntry(
                dataframe=df.copy(),
                last_bar_open_ms=bar_open_ms,
            )
        self.seed_klines_from_dataframe(symbol, timeframe, df)
        return df.copy()

    def bootstrap_candles(
        self,
        symbols: list[str],
        intervals: list[str],
        limit: int,
        rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    ) -> int:
        """
        One-time REST seed for kline buffers (outside scan loops only).
        Returns count of successful seeds.
        """
        blocked, reason = self.is_rest_blocked()
        if blocked:
            system_logger.warning("Bootstrap skipped — REST blocked: %s", reason)
            return 0

        seeded = 0
        for symbol in symbols:
            for interval in intervals:
                key = (symbol.upper(), interval, limit)
                with self._lock:
                    if key in self._candles and not self._candles[key].dataframe.empty:
                        continue
                df = rest_fetcher(symbol, interval, limit)
                if not df.empty:
                    self.seed_klines_from_dataframe(symbol, interval, df)
                    bar_open_ms = self._current_bar_open_ms(interval)
                    with self._lock:
                        self._candles[key] = _CandleCacheEntry(
                            dataframe=df.copy(),
                            last_bar_open_ms=bar_open_ms,
                        )
                    seeded += 1
                if Config.INIT_REST_DELAY_SECONDS > 0:
                    time.sleep(Config.INIT_REST_DELAY_SECONDS)
        return seeded

    def format_ban_message(self, ban: BanStatus) -> str:
        if ban.banned_until_iso:
            return (
                f"🚫 <b>Binance IP temporarily banned</b>\n\n"
                f"Until: {ban.banned_until_iso}\n"
                f"Remaining: ~{ban.seconds_remaining}s\n"
                f"Detail: {ban.message}\n\n"
                f"<i>Scanning paused; position monitoring uses WS cache.</i>"
            )
        return (
            f"🚫 <b>Binance rate limit / IP ban detected</b>\n\n"
            f"{ban.message}\n\n"
            f"<i>REST paused for {Config.RATE_LIMIT_HALT_SECONDS}s.</i>"
        )


def parse_ban_until_ms(message: str) -> Optional[int]:
    match = _BAN_UNTIL_RE.search(message or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def check_binance_ban_status(client: Client) -> BanStatus:
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
