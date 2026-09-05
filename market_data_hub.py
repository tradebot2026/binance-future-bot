"""
Binance Futures WebSocket hub — primary data plane.
Streams miniTicker, klines, and user-data (positions/PnL).
REST is fallback-only outside scan cycles and never during IP bans.
"""

from __future__ import annotations

import asyncio
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
from kline_bootstrap import run_parallel_kline_bootstrap, run_paced_kline_bootstrap
from logger import error_logger, system_logger
from utils import safe_float
from ws_reconnect import (
    WsLogSuppressor,
    WsReconnectPolicy,
    configure_binance_ws_logging,
    is_read_loop_closed_error,
    is_ws_error_message,
)


class _WsThreadedWebsocketManager(ThreadedWebsocketManager):
    """
    python-binance binds get_loop() per thread — set the dedicated loop on the
    worker thread before run_until_complete so socket tasks schedule correctly.
    """

    def run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self.socket_listener())


def _install_ws_ping_defaults() -> None:
    """Configure python-binance ReconnectingWebsocket to send WS ping frames."""
    try:
        from binance.ws.reconnecting_websocket import ReconnectingWebsocket
    except ImportError:
        return

    if getattr(ReconnectingWebsocket, "_hub_ping_configured", False):
        return

    interval = max(Config.WS_PING_INTERVAL_SECONDS, 0.0)
    if interval <= 0:
        return

    original_init = ReconnectingWebsocket.__init__

    def _init_with_ping(self, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        self._ws_kwargs.setdefault("ping_interval", interval)
        self._ws_kwargs.setdefault("ping_timeout", max(interval + 10.0, 20.0))

    ReconnectingWebsocket.__init__ = _init_with_ping  # type: ignore[method-assign]
    ReconnectingWebsocket._hub_ping_configured = True


_install_ws_ping_defaults()

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
class _KlineMultiplexSocket:
    """One multiplex WS connection carrying up to N kline stream subscriptions."""

    streams: list[str]
    conn_key: Optional[str] = None
    last_event_at: float = field(default_factory=time.monotonic)
    reconnect_in_progress: bool = False
    _reconnect_lock: threading.Lock = field(default_factory=threading.Lock)


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
        self._ws_loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws_lifecycle_lock = threading.Lock()
        self._ws_reconnect_join_timeout = Config.WS_RECONNECT_JOIN_TIMEOUT_SECONDS
        self._ws_shutdown_join_timeout = Config.WS_SHUTDOWN_JOIN_TIMEOUT_SECONDS
        self._ticker_rest_fetcher: Optional[
            Callable[[], dict[str, dict[str, Any]]]
        ] = None
        self._last_ticker_rest_at: float = 0.0
        self._ticker_rest_seeded: bool = False
        self._ticker_conn_key: Optional[str] = None
        self._book_ticker_conn_key: Optional[str] = None
        self._user_conn_key: Optional[str] = None
        self._kline_sockets: list[_KlineMultiplexSocket] = []
        self._subscribed_kline_streams: set[str] = set()
        self._bootstrapped_pairs: set[tuple[str, str]] = set()
        self._candle_close_listeners: list[
            Callable[[str, str, int], None]
        ] = []
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

    def set_ticker_rest_fetcher(
        self,
        fetcher: Callable[[], dict[str, dict[str, Any]]],
    ) -> None:
        self._ticker_rest_fetcher = fetcher

    def ticker_cache_age_seconds(self) -> float:
        """Seconds since last WS ticker event (or since WS start if none yet)."""
        if self._last_ticker_event_at > 0:
            return time.monotonic() - self._last_ticker_event_at
        if self._ws_started_at > 0:
            return time.monotonic() - self._ws_started_at
        return float("inf")

    @staticmethod
    def _effective_ticker_stale_seconds() -> float:
        """Testnet streams are often quiet — use a longer stale threshold."""
        if Config.USE_TESTNET:
            return float(max(Config.WS_STALE_SECONDS_TESTNET, 60))
        return float(max(Config.WS_STALE_SECONDS, 30))

    def _preserve_cache_on_reconnect(self) -> bool:
        """True when cached tickers/klines should survive a WS reconnect."""
        return self.is_ticker_cache_usable(min_symbols=10) or bool(self._kline_bars)
    def needs_ticker_rest_fallback(self) -> bool:
        """True when WS ticker cache is empty or stale beyond the REST threshold."""
        if not Config.ENABLE_REST_TICKER_FALLBACK and not Config.STARTUP_TICKER_REST_SEED:
            return False
        threshold = max(Config.TICKER_REST_FALLBACK_AFTER_SECONDS, 1.0)
        if not self._tickers:
            return self.ticker_cache_age_seconds() >= threshold
        if self._last_ticker_event_at <= 0:
            return self.ticker_cache_age_seconds() >= threshold
        return (time.monotonic() - self._last_ticker_event_at) >= threshold

    def refresh_ticker_cache_from_rest(self, *, force: bool = False) -> int:
        """
        Populate ticker cache via one futures_ticker() REST call.
        Returns symbol count after refresh.
        """
        fetcher = self._ticker_rest_fetcher
        if fetcher is None:
            return len(self._tickers)

        blocked, reason = self.is_rest_blocked()
        if blocked:
            if self._ws_log.should_log(f"ticker_rest_blocked:{reason}"):
                system_logger.debug(
                    "Ticker REST fallback skipped — REST blocked: %s", reason
                )
            return len(self._tickers)

        now = time.monotonic()
        min_interval = max(Config.TICKER_REST_MIN_INTERVAL_SECONDS, 5.0)
        if (
            not force
            and self._tickers
            and (now - self._last_ticker_rest_at) < min_interval
        ):
            return len(self._tickers)

        try:
            result = fetcher()
        except Exception as exc:
            if self._ws_log.should_log(f"ticker_rest_fail:{exc}"):
                error_logger.warning("Ticker REST fallback failed: %s", exc)
            return len(self._tickers)

        if not result:
            return len(self._tickers)

        self.seed_tickers_from_rest(result)
        self._last_ticker_rest_at = now
        self._ticker_rest_seeded = True
        count = len(self._tickers)
        if self._ws_log.should_log("ticker_rest_fallback"):
            system_logger.info(
                "Ticker cache refreshed from REST (%s symbols).", count
            )
        return count

    def _rest_quiet_mode(self) -> bool:
        """During IP/rate-limit ban, avoid REST polling and noisy WS churn."""
        blocked, _ = self.is_rest_blocked()
        return blocked

    def _should_reconnect_for_stale_ticker(self) -> bool:
        if not self.ws_is_stale():
            return False
        # Testnet miniTicker can go silent for long stretches — keep cached data.
        if Config.USE_TESTNET and self.is_ticker_cache_usable(min_symbols=10):
            return False
        if self._rest_quiet_mode() and self.is_ticker_cache_usable(min_symbols=10):
            return False
        return True

    def is_ticker_cache_usable(self, min_symbols: int = 1) -> bool:
        """Scanner may proceed when tickers are present (WS or REST)."""
        return len(self._tickers) >= max(min_symbols, 1)

    def wait_until_ready(
        self,
        timeout_seconds: Optional[int] = None,
        min_symbols: int = 30,
    ) -> bool:
        """
        Wait briefly for WS tickers, then REST fallback if still empty/stale.
        """
        min_syms = max(min_symbols, 1)
        if len(self.get_ticker_map()) >= min_syms:
            system_logger.info(
                "WebSocket ticker cache ready (%s symbols).",
                len(self.get_ticker_map()),
            )
            return True

        ws_wait = min(
            timeout_seconds or Config.WS_STARTUP_WAIT_SECONDS,
            max(int(Config.TICKER_REST_FALLBACK_AFTER_SECONDS), 1),
        )
        deadline = time.monotonic() + ws_wait
        while time.monotonic() < deadline:
            count = len(self.get_ticker_map())
            if count >= min_syms:
                system_logger.info("WebSocket ticker cache ready (%s symbols).", count)
                return True
            time.sleep(0.25)

        if self.refresh_ticker_cache_from_rest(force=True) >= min_syms:
            return True

        count = len(self.get_ticker_map())
        if count > 0:
            system_logger.warning(
                "Ticker cache partial after REST fallback — %s symbols (wanted >= %s).",
                count,
                min_syms,
            )
            return True

        system_logger.warning(
            "Ticker cache empty after WS (%ss) and REST fallback.",
            ws_wait,
        )
        return False

    def ensure_ticker_cache_ready(
        self,
        *,
        min_symbols: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        rest_seeder: Optional[Callable[[], dict[str, dict[str, Any]]]] = None,
    ) -> bool:
        """
        Ensure ticker cache is usable for universe build — WS first, REST after 10s.
        """
        if rest_seeder is not None:
            self._ticker_rest_fetcher = rest_seeder

        min_syms = min_symbols or max(min(Config.MIN_SCAN_UNIVERSE, 10), 1)
        if len(self.get_ticker_map()) >= min_syms:
            return True

        return self.wait_until_ready(
            timeout_seconds=timeout_seconds,
            min_symbols=min_syms,
        )

    def is_ws_warming_up(self) -> bool:
        if not self._ws_running:
            return False
        if self.is_ticker_cache_usable(min_symbols=10):
            return False
        if self._ws_started_at <= 0:
            return True
        if self._last_ticker_event_at <= 0:
            return (
                time.monotonic() - self._ws_started_at
            ) < Config.TICKER_REST_FALLBACK_AFTER_SECONDS
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

    def _start_ws_internal(self, *, preserve_cache: bool = False) -> None:
        """Create ThreadedWebsocketManager on a dedicated event loop (never main thread)."""
        with self._ws_lifecycle_lock:
            if self._ws_manager is not None:
                raise RuntimeError("WebSocket manager already running")

            ws_loop = asyncio.new_event_loop()
            self._ws_loop = ws_loop
            self._ws_manager = _WsThreadedWebsocketManager(
                api_key=Config.BINANCE_API_KEY,
                api_secret=Config.BINANCE_API_SECRET,
                testnet=Config.USE_TESTNET,
                loop=ws_loop,
            )
            self._ws_manager.start()
            if preserve_cache:
                if self._tickers:
                    self._last_ticker_event_at = time.monotonic()
            else:
                self._last_ticker_event_at = 0.0
                self._last_user_event_at = 0.0

            manager = self._ws_manager
            self._ticker_conn_key = manager.start_futures_multiplex_socket(
                callback=self._wrap_ws_callback(self._on_ticker_message),
                streams=["!miniTicker@arr"],
            )
            if Config.ENABLE_WS_BOOK_STREAM:
                self._book_ticker_conn_key = manager.start_futures_multiplex_socket(
                    callback=self._wrap_ws_callback(self._on_book_ticker_message),
                    streams=["!bookTicker@arr"],
                )
            self._user_conn_key = manager.start_futures_user_socket(
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
            self._check_kline_sockets_health()
            if not self._rest_quiet_mode() and self.needs_ticker_rest_fallback():
                self.refresh_ticker_cache_from_rest()
            if self._should_reconnect_for_stale_ticker():
                self._request_reconnect("ticker stream stale — no events received")

    @staticmethod
    def _kline_socket_chunk_size() -> int:
        return max(Config.WS_KLINE_MAX_STREAMS_PER_SOCKET, 1)

    @classmethod
    def _chunk_stream_list(cls, streams: list[str]) -> list[list[str]]:
        size = cls._kline_socket_chunk_size()
        return [streams[i : i + size] for i in range(0, len(streams), size)]

    def _wrap_kline_socket_callback(
        self,
        handler: Callable[[dict[str, Any]], None],
        socket_idx: int,
    ) -> Callable[[dict[str, Any]], None]:
        """Per-socket kline callback — reconnects only the affected multiplex socket."""

        def _wrapped(message: dict[str, Any]) -> None:
            if is_ws_error_message(message):
                detail = str(message.get("m", message.get("type", "ws error")))
                if is_read_loop_closed_error(detail):
                    self._request_kline_socket_reconnect(socket_idx, detail)
                return
            try:
                if 0 <= socket_idx < len(self._kline_sockets):
                    self._kline_sockets[socket_idx].last_event_at = time.monotonic()
                handler(message)
            except Exception as exc:
                if is_read_loop_closed_error(exc):
                    self._request_kline_socket_reconnect(socket_idx, str(exc))
                elif self._ws_log.should_log(f"kline_cb:{type(exc).__name__}"):
                    error_logger.warning("Kline WebSocket callback error: %s", exc)

        return _wrapped

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
        if self.is_ws_warming_up():
            return

        is_stale_reason = "stale" in reason.lower()
        preserve_cache = self._preserve_cache_on_reconnect()
        if is_stale_reason:
            if Config.USE_TESTNET and self.is_ticker_cache_usable(min_symbols=10):
                return
            if self._rest_quiet_mode() and self.is_ticker_cache_usable(min_symbols=10):
                return

        now = time.monotonic()
        if (now - self._last_reconnect_request_at) < Config.WS_RECONNECT_DEBOUNCE_SECONDS:
            return
        self._last_reconnect_request_at = now

        with self._reconnect_lock:
            if self._reconnect_in_progress:
                return
            self._reconnect_in_progress = True

        silent = preserve_cache
        if self._ws_log.should_log(reason):
            if silent:
                system_logger.debug(
                    "WebSocket reconnect scheduled (%s) — preserving cached data.",
                    reason,
                )
            else:
                system_logger.warning(
                    "WebSocket disconnect detected (%s) — scheduling reconnect.",
                    reason,
                )

        threading.Thread(
            target=self._reconnect_worker,
            args=(reason, silent),
            name="ws-reconnect",
            daemon=True,
        ).start()

    def _reconnect_worker(self, reason: str, silent: bool = False) -> None:
        try:
            delay = self._reconnect_policy.next_delay()
            if self._ws_log.should_log(f"backoff:{delay:.0f}s"):
                log_fn = system_logger.debug if silent else system_logger.info
                log_fn(
                    "WebSocket reconnect in %.1fs (attempt %s).",
                    delay,
                    self._reconnect_policy.attempt,
                )
            time.sleep(delay)

            preserve_cache = self._preserve_cache_on_reconnect()
            self._stop_ws_internal(
                preserve_kline_subscriptions=True,
                blocking=False,
            )
            self._start_ws_internal(preserve_cache=preserve_cache)
            self._ws_log.reset()
            if not self._rest_quiet_mode() and not preserve_cache:
                self.refresh_ticker_cache_from_rest(force=True)
            elif preserve_cache and self._ws_log.should_log("ws_reconnect_quiet"):
                system_logger.debug(
                    "WebSocket reconnected — continuing with cached tickers/klines."
                )
            if silent:
                system_logger.debug("WebSocket reconnected successfully (silent).")
            else:
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

    def _check_kline_sockets_health(self) -> None:
        stale_after = max(Config.WS_KLINE_SOCKET_STALE_SECONDS, 60)
        if Config.USE_TESTNET:
            stale_after = max(stale_after, Config.WS_STALE_SECONDS_TESTNET, 60)
        now = time.monotonic()
        for idx, sock in enumerate(self._kline_sockets):
            if not sock.streams or sock.reconnect_in_progress:
                continue
            if (now - sock.last_event_at) > stale_after:
                self._request_kline_socket_reconnect(
                    idx, f"kline socket stale ({len(sock.streams)} streams)"
                )

    def _request_kline_socket_reconnect(self, socket_idx: int, reason: str) -> None:
        if not Config.WS_RECONNECT_ENABLED or not Config.ENABLE_WEBSOCKET_STREAMS:
            return
        if socket_idx < 0 or socket_idx >= len(self._kline_sockets):
            return

        sock = self._kline_sockets[socket_idx]
        with sock._reconnect_lock:
            if sock.reconnect_in_progress:
                return
            sock.reconnect_in_progress = True

        if self._ws_log.should_log(f"kline_sock:{socket_idx}:{reason}"):
            system_logger.warning(
                "Kline WS socket %s dropped (%s streams) — reconnecting socket only.",
                socket_idx,
                len(sock.streams),
            )

        threading.Thread(
            target=self._reconnect_kline_socket_worker,
            args=(socket_idx, reason),
            name=f"ws-kline-reconnect-{socket_idx}",
            daemon=True,
        ).start()

    def _reconnect_kline_socket_worker(self, socket_idx: int, reason: str) -> None:
        sock = self._kline_sockets[socket_idx] if socket_idx < len(self._kline_sockets) else None
        try:
            if sock is None or not sock.streams:
                return
            time.sleep(max(Config.WS_RECONNECT_MIN_SECONDS, 0.5))
            if not self._ws_manager or not self._ws_running or self._reconnect_in_progress:
                return
            self._close_kline_multiplex(sock)
            sock.conn_key = self._start_kline_socket(sock, socket_idx)
            sock.last_event_at = time.monotonic()
            system_logger.info(
                "Kline WS socket %s reconnected (%s streams).",
                socket_idx,
                len(sock.streams),
            )
        except Exception as exc:
            if self._ws_log.should_log(f"kline_sock_fail:{socket_idx}:{exc}"):
                error_logger.error(
                    "Kline WS socket %s reconnect failed: %s", socket_idx, exc
                )
        finally:
            if sock is not None:
                sock.reconnect_in_progress = False

    def _start_kline_socket(
        self, sock: _KlineMultiplexSocket, socket_idx: int
    ) -> str:
        if not self._ws_manager:
            raise RuntimeError("WebSocket manager not running")
        return self._ws_manager.start_futures_multiplex_socket(
            callback=self._wrap_kline_socket_callback(
                self._on_kline_multiplex, socket_idx
            ),
            streams=sock.streams,
        )

    def _close_kline_multiplex(self, sock: _KlineMultiplexSocket) -> None:
        if sock.conn_key and self._ws_manager:
            try:
                self._ws_manager.stop_socket(sock.conn_key)
            except Exception:
                pass
        sock.conn_key = None

    def _open_kline_socket_pool(self, streams: list[str]) -> int:
        """Open multiplex kline sockets (chunked) and return stream count opened."""
        if not self._ws_manager or not streams:
            return 0
        opened = 0
        for chunk in self._chunk_stream_list(streams):
            socket_idx = len(self._kline_sockets)
            sock = _KlineMultiplexSocket(streams=list(chunk))
            try:
                sock.conn_key = self._start_kline_socket(sock, socket_idx)
                self._kline_sockets.append(sock)
                opened += len(chunk)
            except Exception as exc:
                error_logger.error(
                    "Kline WS socket open failed (%s streams): %s", len(chunk), exc
                )
        return opened

    def _stop_ws_internal(
        self,
        *,
        preserve_kline_subscriptions: bool,
        blocking: bool = True,
    ) -> None:
        join_timeout = (
            self._ws_shutdown_join_timeout
            if blocking
            else self._ws_reconnect_join_timeout
        )
        with self._ws_lifecycle_lock:
            manager = self._ws_manager
            loop = self._ws_loop
            if manager is None:
                self._ws_running = False
                return
            try:
                for sock in list(self._kline_sockets):
                    self._close_kline_multiplex(sock)
                if self._user_conn_key:
                    try:
                        manager.stop_socket(self._user_conn_key)
                    except Exception:
                        pass
                if self._book_ticker_conn_key:
                    try:
                        manager.stop_socket(self._book_ticker_conn_key)
                    except Exception:
                        pass
                if self._ticker_conn_key:
                    try:
                        manager.stop_socket(self._ticker_conn_key)
                    except Exception:
                        pass
                manager.stop()
                if manager.is_alive() and join_timeout > 0:
                    manager.join(timeout=join_timeout)
                    if manager.is_alive():
                        system_logger.debug(
                            "WS manager thread still running after %.1fs — detaching.",
                            join_timeout,
                        )
            except Exception as exc:
                if self._ws_log.should_log(f"ws_stop:{exc}"):
                    error_logger.warning("WebSocket shutdown error: %s", exc)
            finally:
                self._ws_running = False
                self._ws_manager = None
                self._ws_loop = None
                self._ticker_conn_key = None
                self._book_ticker_conn_key = None
                self._user_conn_key = None
                self._kline_sockets.clear()
                if not preserve_kline_subscriptions:
                    self._subscribed_kline_streams.clear()
                if (
                    loop is not None
                    and not loop.is_closed()
                    and (manager is None or not manager.is_alive())
                ):
                    try:
                        loop.close()
                    except Exception:
                        pass

    def _resubscribe_kline_streams(self) -> None:
        """Re-open kline multiplex sockets after reconnect (chunked connection pool)."""
        if not self._ws_manager or not self._subscribed_kline_streams:
            return
        self._kline_sockets.clear()
        streams = sorted(self._subscribed_kline_streams)
        opened = self._open_kline_socket_pool(streams)
        if opened:
            system_logger.info(
                "Re-subscribed %s kline WS streams across %s socket(s) (max %s/socket).",
                opened,
                len(self._kline_sockets),
                self._kline_socket_chunk_size(),
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
        halt_seconds = max(
            Config.REST_BAN_MIN_SLEEP_SECONDS,
            Config.RATE_LIMIT_HALT_SECONDS,
            Config.IP_BAN_HALT_SECONDS,
        )
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
            updated = False
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
                    updated = True
                if updated:
                    self._last_ticker_event_at = now
            if updated:
                self._reconnect_policy.reset()
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
            if row["closed"]:
                self._emit_candle_close(symbol, interval, row["open_ms"])
        except Exception as exc:
            error_logger.warning("Kline WS parse error: %s", exc)

    def register_candle_close_listener(
        self, listener: Callable[[str, str, int], None]
    ) -> None:
        """Register callback(symbol, interval, bar_open_ms) on closed kline WS events."""
        if listener not in self._candle_close_listeners:
            self._candle_close_listeners.append(listener)

    def _emit_candle_close(self, symbol: str, interval: str, bar_open_ms: int) -> None:
        for listener in list(self._candle_close_listeners):
            try:
                listener(symbol, interval, bar_open_ms)
            except Exception as exc:
                error_logger.warning("Candle close listener error: %s", exc)

    def get_last_closed_bar_open_ms(
        self, symbol: str, timeframe: str
    ) -> Optional[int]:
        """Return open_ms of the most recent closed bar in WS cache."""
        symbol = symbol.upper()
        with self._lock:
            bars = self._kline_bars.get((symbol, timeframe))
            if not bars:
                return None
            for bar in reversed(bars):
                if bar.get("closed"):
                    return int(bar.get("open_ms", 0))
        return None

    def demote_symbol_klines(self, symbol: str) -> None:
        """GC demoted symbol — flush buffers and rebuild WS kline subscriptions."""
        symbol = symbol.upper()
        sym_lower = symbol.lower()
        ws_intervals = Config.get_ws_kline_intervals()
        to_remove = {f"{sym_lower}@kline_{iv}" for iv in ws_intervals}

        with self._lock:
            self._subscribed_kline_streams -= to_remove
            for key in [k for k in self._kline_bars if k[0] == symbol]:
                del self._kline_bars[key]
            for key in [k for k in self._candles if k[0] == symbol]:
                del self._candles[key]

        if to_remove and self._ws_manager and self._ws_running:
            self._rebuild_kline_socket_pool()

        system_logger.debug(
            "GC demoted symbol %s — removed %s kline stream(s).",
            symbol,
            len(to_remove),
        )

    def _rebuild_kline_socket_pool(self) -> None:
        """Close and reopen all kline multiplex sockets from subscription set."""
        if not self._ws_manager:
            return
        for sock in list(self._kline_sockets):
            self._close_kline_multiplex(sock)
        self._kline_sockets.clear()
        if self._subscribed_kline_streams:
            streams = sorted(self._subscribed_kline_streams)
            self._open_kline_socket_pool(streams)

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
        self,
        symbols: list[str],
        intervals: Optional[list[str]] = None,
    ) -> None:
        """
        Subscribe WS kline streams for scan universe (pooled multiplex sockets).
        Uses Config.get_ws_kline_intervals() by default (entry TF only when enabled).
        """
        if not self._ws_manager or not self._ws_running:
            return

        ws_intervals = intervals or Config.get_ws_kline_intervals()
        new_streams: list[str] = []
        for symbol in symbols:
            sym = symbol.lower()
            for interval in ws_intervals:
                stream = f"{sym}@kline_{interval}"
                if stream not in self._subscribed_kline_streams:
                    new_streams.append(stream)
                    self._subscribed_kline_streams.add(stream)

        if not new_streams:
            return

        opened = self._open_kline_socket_pool(new_streams)
        system_logger.info(
            "Subscribed %s new kline WS streams (%s total, %s socket(s), "
            "intervals=%s, max %s/socket).",
            len(new_streams),
            len(self._subscribed_kline_streams),
            len(self._kline_sockets),
            ",".join(ws_intervals),
            self._kline_socket_chunk_size(),
        )
        if opened < len(new_streams):
            system_logger.warning(
                "Kline WS subscribe incomplete — opened %s/%s new streams.",
                opened,
                len(new_streams),
            )

    def subscribe_and_bootstrap_klines(
        self,
        symbols: list[str],
        intervals: list[str],
        rest_fetcher: Optional[Callable[[str, str, int], pd.DataFrame]] = None,
    ) -> int:
        """
        Subscribe WS kline streams, then one-time REST bootstrap for new pairs.
        Must be called outside scan_context (via exchange.bootstrap_context()).
        """
        self.subscribe_kline_streams(symbols)
        if not rest_fetcher or not Config.ENABLE_WS_KLINE_STARTUP_BOOTSTRAP:
            return 0
        return self.bootstrap_klines_on_subscribe(symbols, intervals, rest_fetcher)

    def bootstrap_klines_on_subscribe(
        self,
        symbols: list[str],
        intervals: list[str],
        rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    ) -> int:
        """
        One-time REST historical kline load per (symbol, interval) into WS cache.
        Skips pairs already bootstrapped or with sufficient cached bars.
        """
        blocked, reason = self.is_rest_blocked()
        if blocked:
            system_logger.warning(
                "Kline startup bootstrap skipped — REST blocked: %s", reason
            )
            return 0

        limit = Config.CANDLE_FETCH_LIMIT
        min_bars = max(Config.WS_KLINE_BOOTSTRAP_MIN_BARS, 10)
        pending: list[tuple[str, str]] = []

        for symbol in symbols:
            sym = symbol.upper()
            for interval in intervals:
                pair = (sym, interval)
                if pair in self._bootstrapped_pairs:
                    continue
                cached = self.get_candles_cached_only(sym, interval, limit)
                if not cached.empty and len(cached) >= min_bars:
                    self._bootstrapped_pairs.add(pair)
                    continue
                pending.append(pair)

        if not pending:
            return 0

        def _mark_bootstrapped(sym: str, interval: str) -> None:
            self._bootstrapped_pairs.add((sym.upper(), interval))

        use_paced = (
            Config.ENABLE_PACED_KLINE_BOOTSTRAP
            or Config.WS_KLINE_BOOTSTRAP_CONCURRENCY <= 1
        )
        if use_paced:
            result = run_paced_kline_bootstrap(
                pending,
                rest_fetcher,
                limit,
                min_bars,
                seed_fn=self.seed_klines_from_dataframe,
                mark_bootstrapped=_mark_bootstrapped,
            )
        else:
            result = run_parallel_kline_bootstrap(
                pending,
                rest_fetcher,
                limit,
                min_bars,
                seed_fn=self.seed_klines_from_dataframe,
                mark_bootstrapped=_mark_bootstrapped,
            )
        return result.seeded

    def bootstrap_klines_for_symbols(
        self,
        symbols: list[str],
        intervals: list[str],
        rest_fetcher: Callable[[str, str, int], pd.DataFrame],
        *,
        max_pairs: int | None = None,
    ) -> int:
        """Paced REST bootstrap for a subset of symbols (background tier seeding)."""
        if not symbols or not rest_fetcher:
            return 0
        blocked, reason = self.is_rest_blocked()
        if blocked:
            system_logger.debug(
                "Background kline bootstrap skipped — REST blocked: %s", reason
            )
            return 0

        limit = Config.CANDLE_FETCH_LIMIT
        min_bars = max(Config.WS_KLINE_BOOTSTRAP_MIN_BARS, 10)
        pending: list[tuple[str, str]] = []

        for symbol in symbols:
            sym = symbol.upper()
            for interval in intervals:
                pair = (sym, interval)
                if pair in self._bootstrapped_pairs:
                    continue
                cached = self.get_candles_cached_only(sym, interval, limit)
                if not cached.empty and len(cached) >= min_bars:
                    self._bootstrapped_pairs.add(pair)
                    continue
                pending.append(pair)

        if not pending:
            return 0

        def _mark_bootstrapped(sym: str, interval: str) -> None:
            self._bootstrapped_pairs.add((sym.upper(), interval))

        result = run_paced_kline_bootstrap(
            pending,
            rest_fetcher,
            limit,
            min_bars,
            seed_fn=self.seed_klines_from_dataframe,
            mark_bootstrapped=_mark_bootstrapped,
            max_pairs=max_pairs,
        )
        return result.seeded

    def is_kline_bootstrapped(self, symbol: str, interval: str) -> bool:
        return (symbol.upper(), interval) in self._bootstrapped_pairs

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
        return (
            time.monotonic() - self._last_ticker_event_at
        ) > self._effective_ticker_stale_seconds()

    def user_stream_has_account_data(self) -> bool:
        """True after at least one user-data WS event (ACCOUNT_UPDATE / order fill)."""
        return self._last_user_event_at > 0

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
                    "highPrice": safe_float(row.get("highPrice")),
                    "lowPrice": safe_float(row.get("lowPrice")),
                    "openPrice": safe_float(row.get("openPrice")),
                    "updated_at": now,
                }

    # ---------------- Positions (user stream) ----------------

    def get_ws_positions(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._positions)

    def get_ws_unrealized_pnl_total(self) -> float:
        with self._lock:
            return self._unrealized_pnl_total

    def get_ws_position_quantity(self, symbol: str, position_side: str) -> float:
        symbol = symbol.upper()
        position_side = position_side.upper()
        with self._lock:
            if self._last_user_event_at <= 0:
                return 0.0
            for pos in self._positions:
                if pos.get("symbol") == symbol and pos.get("positionSide") == position_side:
                    return safe_float(pos.get("quantity"))
        return 0.0

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
