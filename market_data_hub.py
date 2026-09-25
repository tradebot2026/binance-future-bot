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
from kline_bootstrap import run_batched_kline_bootstrap
from logger import error_logger, system_logger
from utils import safe_float
from core.fill_pnl_tracker import FillPnlRecord, FillPnlTracker
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
    """Configure python-binance ReconnectingWebsocket ping/pong (15s / 10s)."""
    try:
        from binance.ws.reconnecting_websocket import ReconnectingWebsocket
    except ImportError:
        return

    if getattr(ReconnectingWebsocket, "_hub_ping_configured", False):
        return

    original_init = ReconnectingWebsocket.__init__

    def _init_with_ping(self, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        interval = max(float(Config.WS_PING_INTERVAL_SECONDS), 0.0)
        timeout = max(float(Config.WS_PING_TIMEOUT_SECONDS), 1.0)
        if interval <= 0:
            return
        self._ws_kwargs.setdefault("ping_interval", interval)
        self._ws_kwargs.setdefault("ping_timeout", timeout)

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
        self._rest_governor: Any = None
        self._last_ticker_rest_at: float = 0.0
        self._cache_miss_rest_at: dict[tuple[str, str], float] = {}
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
        self._price_tick_listeners: list[Callable[[str, float], None]] = []
        self._ws_running = False
        self._last_ticker_event_at: float = 0.0
        self._last_real_ticker_at: float = 0.0
        self._last_book_event_at: float = 0.0
        self._last_ws_seen_at: float = 0.0
        self._pending_order_fills: list[dict[str, Any]] = []
        self._bootstrap_series_window: deque[float] = deque()
        self._last_user_event_at: float = 0.0
        self._last_user_socket_at: float = 0.0
        self._listen_key: str = ""
        self._last_listen_keepalive_at: float = 0.0
        self._positions: list[dict[str, Any]] = []
        self._unrealized_pnl_total: float = 0.0
        self._wallet_balances: dict[str, float] = {}
        self._ban_notice_logged: bool = False
        self._ws_started_at: float = 0.0
        self._reconnect_lock = threading.Lock()
        self._reconnect_in_progress = False
        self._last_reconnect_request_at: float = 0.0
        self._last_stale_reconnect_success_at: float = 0.0
        self._watchdog_stop = threading.Event()
        self._watchdog_thread: Optional[threading.Thread] = None
        self._reconnect_policy = WsReconnectPolicy(
            min_seconds=Config.WS_RECONNECT_MIN_SECONDS,
            max_seconds=Config.WS_RECONNECT_MAX_SECONDS,
        )
        self._ws_log = WsLogSuppressor(Config.WS_RECONNECT_LOG_INTERVAL_SECONDS)
        self.fill_tracker = FillPnlTracker()
        configure_binance_ws_logging()

    def ws_is_running(self) -> bool:
        return self._ws_running

    def get_ws_health_snapshot(self) -> dict[str, Any]:
        """Connection summary for /health diagnostics."""
        with self._lock:
            active_feeds = 0
            if self._ticker_conn_key:
                active_feeds += 1
            if self._book_ticker_conn_key:
                active_feeds += 1
            if self._user_conn_key:
                active_feeds += 1
            active_feeds += sum(1 for sock in self._kline_sockets if sock.streams)

            ticker_age = self.ticker_cache_age_seconds()
            user_age = (
                max(time.monotonic() - self._last_user_event_at, 0.0)
                if self._last_user_event_at > 0
                else float("inf")
            )

            if self._reconnect_in_progress:
                state = "RECONNECTING"
            elif not self._ws_running:
                state = "STOPPED"
            elif self._last_health_stamp() <= 0:
                state = "WARMING" if self.is_ws_warming_up() else "STALE"
            elif self.ws_is_stale() or self._book_stream_is_stale():
                if self._kline_feeds_healthy() and self.is_ticker_cache_usable():
                    state = "DEGRADED"
                else:
                    state = "STALE"
            else:
                state = "HEALTHY"

            return {
                "state": state,
                "active_feeds": active_feeds,
                "ticker_symbols": len(self._tickers),
                "kline_streams": len(self._subscribed_kline_streams),
                "ticker_age_seconds": ticker_age,
                "book_age_seconds": self.book_cache_age_seconds(),
                "user_stream_age_seconds": user_age,
                "user_stream_ok": self.user_stream_has_account_data()
                and not self.user_stream_is_stale(),
                "reconnect_in_progress": self._reconnect_in_progress,
            }

    def set_ticker_rest_fetcher(
        self,
        fetcher: Callable[[], dict[str, dict[str, Any]]],
    ) -> None:
        self._ticker_rest_fetcher = fetcher

    def set_rest_governor(self, governor: Any) -> None:
        """Attach exchange used-weight governor so hub fallbacks honor it."""
        self._rest_governor = governor

    def ws_is_degraded(self) -> bool:
        """True during reconnect or warmup — REST fallbacks must not storm."""
        return bool(self._reconnect_in_progress or self.is_ws_warming_up())

    def _governor_blocks_background_rest(self, weight: int = 40) -> bool:
        gov = self._rest_governor
        if gov is None:
            return False
        check = getattr(gov, "can_make_background_rest_call", None)
        if callable(check):
            try:
                return not bool(check(weight))
            except Exception:
                return True
        blocked_fn = getattr(gov, "is_rest_blocked", None)
        if callable(blocked_fn):
            try:
                return bool(blocked_fn()[0])
            except Exception:
                return True
        return False

    def ticker_cache_age_seconds(self) -> float:
        """Seconds since last WS ticker event (or since WS start if none yet)."""
        if self._last_ticker_event_at > 0:
            return time.monotonic() - self._last_ticker_event_at
        if self._ws_started_at > 0:
            return time.monotonic() - self._ws_started_at
        return float("inf")

    def book_cache_age_seconds(self) -> float:
        """Seconds since last WS bookTicker event (or since WS start if none yet)."""
        if self._last_book_event_at > 0:
            return time.monotonic() - self._last_book_event_at
        if self._ws_started_at > 0:
            return time.monotonic() - self._ws_started_at
        return float("inf")

    @staticmethod
    def _effective_ticker_stale_seconds() -> float:
        """Testnet uses a longer window to absorb quiet bursts; mainnet stays at 30s."""
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
        if self.is_ticker_cache_usable(min_symbols=30):
            threshold = max(Config.TICKER_REST_FALLBACK_AFTER_SECONDS, 120.0)
            if self._last_ticker_event_at > 0:
                return (time.monotonic() - self._last_ticker_event_at) >= threshold
            return self.ticker_cache_age_seconds() >= threshold
        threshold = max(Config.TICKER_REST_FALLBACK_AFTER_SECONDS, 90.0)
        if not self._tickers:
            return self.ticker_cache_age_seconds() >= threshold
        if self._last_ticker_event_at <= 0:
            return self.ticker_cache_age_seconds() >= threshold
        return (time.monotonic() - self._last_ticker_event_at) >= threshold

    def refresh_ticker_cache_from_rest(
        self, *, force: bool = False, silent: bool = False
    ) -> int:
        """
        Populate ticker cache via one futures_ticker() REST call.
        Returns symbol count after refresh. Does not stamp WS freshness.
        """
        fetcher = self._ticker_rest_fetcher
        if fetcher is None:
            return len(self._tickers)

        if self.ws_is_degraded():
            if self._ws_log.should_log("ticker_rest_skip_degraded"):
                system_logger.debug(
                    "Ticker REST skipped — WS reconnect/warmup (cache only)."
                )
            return len(self._tickers)

        blocked, reason = self.is_rest_blocked()
        if blocked:
            if self._ws_log.should_log(f"ticker_rest_blocked:{reason}"):
                system_logger.debug(
                    "Ticker REST fallback skipped — REST blocked: %s", reason
                )
            return len(self._tickers)

        if self._governor_blocks_background_rest(40):
            if self._ws_log.should_log("ticker_rest_governor"):
                system_logger.debug(
                    "Ticker REST skipped — used-weight governor (cache only)."
                )
            return len(self._tickers)

        now = time.monotonic()
        min_interval = max(
            Config.TICKER_REST_MIN_INTERVAL_SECONDS,
            60.0 if silent else 90.0,
        )
        if (
            not force
            and self._tickers
            and (now - self._last_ticker_rest_at) < min_interval
        ):
            return len(self._tickers)

        if (
            not force
            and not silent
            and self.is_ticker_cache_usable(min_symbols=30)
        ):
            return len(self._tickers)

        try:
            result = fetcher()
        except Exception as exc:
            from binance.exceptions import BinanceAPIException

            if isinstance(exc, BinanceAPIException) and exc.code == -1003:
                self.handle_rate_limit_error(exc)
            if self._ws_log.should_log(f"ticker_rest_fail:{exc}"):
                error_logger.warning("Ticker REST fallback failed: %s", exc)
            return len(self._tickers)

        if not result:
            return len(self._tickers)

        self.seed_tickers_from_rest(result)
        self._last_ticker_rest_at = now
        self._ticker_rest_seeded = True
        count = len(self._tickers)
        if silent:
            system_logger.debug(
                "Ticker cache silently refreshed from REST (%s symbols).", count
            )
        elif self._ws_log.should_log("ticker_rest_fallback"):
            system_logger.info(
                "Ticker cache refreshed from REST (%s symbols).", count
            )
        return count

    def _rest_quiet_mode(self) -> bool:
        """During IP/rate-limit ban, avoid REST polling and noisy WS churn."""
        blocked, _ = self.is_rest_blocked()
        return blocked

    def _kline_feeds_healthy(self) -> bool:
        """True when at least one kline multiplex socket still received data recently."""
        if not self._kline_sockets:
            return False
        stale_after = max(Config.WS_KLINE_SOCKET_STALE_SECONDS, 60)
        if Config.USE_TESTNET:
            stale_after = max(stale_after, Config.WS_STALE_SECONDS_TESTNET, 60)
        now = time.monotonic()
        return any(
            bool(sock.streams) and (now - sock.last_event_at) < stale_after
            for sock in self._kline_sockets
        )

    def _stale_reconnect_on_cooldown(self) -> bool:
        cooldown = float(max(Config.WS_STALE_RECONNECT_COOLDOWN_SECONDS, 0.0))
        if cooldown <= 0 or self._last_stale_reconnect_success_at <= 0:
            return False
        return (time.monotonic() - self._last_stale_reconnect_success_at) < cooldown

    def _should_reconnect_for_stale_ticker(self) -> bool:
        """True when miniTicker/bookTicker age exceeds stale threshold.

        Testnet miniTicker/bookTicker often idle while klines still tick. Do not
        tear down the whole hub (and wipe kline sockets) in that case — REST-refresh
        tickers instead. Also honor a cooldown after a successful stale reconnect.
        """
        if not self.ws_is_stale() and not self._book_stream_is_stale():
            return False
        if self._kline_feeds_healthy() or self._stale_reconnect_on_cooldown():
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

        if self.ws_is_degraded() or self._governor_blocks_background_rest(40):
            count = len(self.get_ticker_map())
            return count > 0
        if self.refresh_ticker_cache_from_rest(force=False) >= min_syms:
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

    def is_market_data_ready_for_entry(self, symbol: str = "") -> tuple[bool, str]:
        """NEW-entry gate. Open-position monitoring must not use this."""
        if self._reconnect_in_progress:
            return False, "WS_RECONNECTING"
        if not self._ws_running:
            return False, "WS_DISCONNECTED"
        if self.is_ws_warming_up():
            return False, "WS_WARMUP"

        stale_after = self._effective_ticker_stale_seconds()
        cache_ok = self.is_ticker_cache_usable()
        real_ok = (
            self._last_real_ticker_at > 0
            and (time.monotonic() - self._last_real_ticker_at) <= stale_after
        )
        # Testnet miniTicker is often idle; cached lastPrice is usable once WS
        # is running and not in warmup/reconnect. Mainnet still requires ticks.
        if not real_ok and not (Config.USE_TESTNET and cache_ok):
            if self._last_real_ticker_at <= 0:
                return False, "WS_WARMUP"
            return False, "STALE_DATA"
        if not cache_ok:
            return False, "RESYNC"
        if symbol:
            fresh = self.get_fresh_ticker_price(symbol, max_age_seconds=stale_after)
            if fresh is None or fresh <= 0:
                if Config.USE_TESTNET:
                    cached = self.get_price(symbol)
                    if cached is None or cached <= 0:
                        return False, "STALE_DATA"
                else:
                    return False, "STALE_DATA"
        return True, ""

    def drain_pending_order_fills(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._pending_order_fills)
            self._pending_order_fills.clear()
            return rows

    def is_ws_warming_up(self) -> bool:
        """True only while reconnect/start is in flight before freshness is stamped."""
        if self._reconnect_in_progress:
            return True
        if not self._ws_running:
            return False
        if self._last_health_stamp() > 0:
            return False
        if self._ws_started_at <= 0:
            return True
        grace = max(self._effective_ticker_stale_seconds(), 15.0)
        return (time.monotonic() - self._ws_started_at) < grace

    def _last_health_stamp(self) -> float:
        return max(
            self._last_ticker_event_at,
            self._last_book_event_at,
            self._last_ws_seen_at,
        )

    def _mark_stream_freshness(self) -> None:
        """Stamp ticker/book/kline clocks so reconnect cannot look instantly stale."""
        now = time.monotonic()
        self._last_ticker_event_at = now
        self._last_book_event_at = now
        self._last_ws_seen_at = now
        for sock in self._kline_sockets:
            sock.last_event_at = now

    def _note_ws_frame(self, stream: str = "") -> None:
        """Any inbound frame (data, heartbeat, empty payload) counts as connection life."""
        now = time.monotonic()
        self._last_ws_seen_at = now
        if stream in ("ticker", "all", ""):
            self._last_ticker_event_at = now
        if stream in ("book", "all"):
            self._last_book_event_at = now
        if stream in ("user", "all"):
            self._last_user_socket_at = now

    def execution_requires_rest_price(self, symbol: str = "") -> bool:
        """True when execution must not trust WS last price (stale/warming/no fresh tick)."""
        if self._reconnect_in_progress or not self._ws_running:
            return True
        if self.is_ws_warming_up() or self.ws_is_stale() or self._book_stream_is_stale():
            return True
        snap = self.get_ws_health_snapshot()
        if str(snap.get("state", "")).upper() in {"STALE", "WARMING", "RECONNECTING"}:
            return True
        if symbol:
            fresh = self.get_fresh_ticker_price(
                symbol,
                max_age_seconds=self._effective_ticker_stale_seconds(),
            )
            if fresh is None or fresh <= 0:
                return True
        return False

    def get_execution_ticker_price(self, symbol: str) -> Optional[float]:
        """WS last price only if the miniTicker row is within the stale window."""
        return self.get_fresh_ticker_price(
            symbol,
            max_age_seconds=self._effective_ticker_stale_seconds(),
        )

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
            if not preserve_cache:
                self._last_user_event_at = 0.0

            manager = self._ws_manager
            self._ticker_conn_key = manager.start_futures_multiplex_socket(
                callback=self._wrap_ws_callback(self._on_ticker_message, stream="ticker"),
                streams=["!miniTicker@arr"],
            )
            if Config.ENABLE_WS_BOOK_STREAM:
                self._book_ticker_conn_key = manager.start_futures_multiplex_socket(
                    callback=self._wrap_ws_callback(
                        self._on_book_ticker_message, stream="book"
                    ),
                    streams=["!bookTicker@arr"],
                )
            self._user_conn_key = manager.start_futures_user_socket(
                callback=self._wrap_ws_callback(self._on_user_message, stream="user"),
            )
            self._capture_listen_key()
            self._ws_running = True
            self._ws_started_at = time.monotonic()
            self._resubscribe_kline_streams()
            self._mark_stream_freshness()
            streams = "miniTicker + user data"
            if Config.ENABLE_WS_BOOK_STREAM:
                streams += " + bookTicker"
            system_logger.info("[WS_CONNECTED] streams started (%s).", streams)
            system_logger.info("[WS_SUBSCRIPTION_READY] miniTicker/userData subscribed.")

    def _capture_listen_key(self) -> None:
        """Remember the user-data listen-key so we can keepalive it without reconnects."""
        manager = self._ws_manager
        candidates = (
            manager,
            getattr(manager, "_bsm", None) if manager is not None else None,
            getattr(manager, "_client", None) if manager is not None else None,
            self.client,
        )
        for obj in candidates:
            if obj is None:
                continue
            for attr in ("_listen_key", "listen_key", "_user_listen_key"):
                raw = getattr(obj, attr, None)
                if isinstance(raw, str) and raw:
                    self._listen_key = raw
                    self._last_listen_keepalive_at = time.monotonic()
                    return
            keys = getattr(obj, "_listen_keys", None)
            if isinstance(keys, dict) and keys:
                first = next(iter(keys.values()), None)
                if first:
                    self._listen_key = str(first)
                    self._last_listen_keepalive_at = time.monotonic()
                    return

    def _maybe_keepalive_user_listen_key(self) -> None:
        """PUT listenKey about every 30 minutes (Binance expires at 60). Weight 1.

        This is not background market-data REST. Skip only on a real IP/HTTP
        halt so the used-weight governor cannot silently expire the stream.
        """
        if self._reconnect_in_progress or not self._ws_running:
            return
        if self._rest_quiet_mode():
            return
        if not self._listen_key:
            self._capture_listen_key()
        if not self._listen_key:
            getter = getattr(self.client, "futures_stream_get_listen_key", None)
            if callable(getter):
                try:
                    created = getter()
                    if isinstance(created, str) and created:
                        self._listen_key = created
                    elif isinstance(created, dict):
                        self._listen_key = str(created.get("listenKey") or "")
                except Exception as exc:
                    if self._ws_log.should_log(f"listen_key_create:{exc}"):
                        system_logger.debug("Listen-key capture skipped: %s", exc)
        if not self._listen_key:
            return
        if (
            self._last_listen_keepalive_at > 0
            and (time.monotonic() - self._last_listen_keepalive_at) < 1800.0
        ):
            return
        keepalive = getattr(self.client, "futures_stream_keepalive", None)
        if not callable(keepalive):
            return
        try:
            keepalive(listenKey=self._listen_key)
            self._last_listen_keepalive_at = time.monotonic()
            system_logger.debug("User-data listen-key keepalive sent.")
        except Exception as exc:
            if self._ws_log.should_log(f"listen_keepalive:{exc}"):
                system_logger.debug("Listen-key keepalive skipped: %s", exc)

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
        interval = max(float(Config.WS_HEALTH_CHECK_SECONDS), 5.0)
        while not self._watchdog_stop.wait(interval):
            if not self._ws_running:
                continue
            if self._reconnect_in_progress or self.is_ws_warming_up():
                continue
            self._maybe_keepalive_user_listen_key()
            self._check_kline_sockets_health()
            scan_warm = Config.scan_warmup_seconds()
            if (
                scan_warm > 0
                and self._ws_started_at > 0
                and (time.monotonic() - self._ws_started_at) < scan_warm
            ):
                continue
            blocked, _ = self.is_rest_blocked()
            if (
                not blocked
                and not self._rest_quiet_mode()
                and not self._governor_blocks_background_rest(40)
                and self.needs_ticker_rest_fallback()
            ):
                self.refresh_ticker_cache_from_rest()
            if self._should_reconnect_for_stale_ticker():
                age = self.ticker_cache_age_seconds()
                self._request_reconnect(
                    f"ticker stream stale — age {age:.0f}s "
                    f"(threshold {self._effective_ticker_stale_seconds():.0f}s) "
                    "resetting miniTicker/bookTicker/userData"
                )
            elif self._should_reconnect_for_stale_user_stream():
                self._request_reconnect(
                    "user data stream stale — no account events received"
                )

    def _should_reconnect_for_stale_user_stream(self) -> bool:
        """Reconnect user-data only when we need ACCOUNT_UPDATE and the socket died.

        Testnet often sends no account events when flat. A quiet listen-key is not
        a stale multiplex manager — do not tear down miniTicker/klines for that.
        """
        if self._rest_quiet_mode():
            return False
        if not self.user_stream_has_account_data():
            return False
        if not self.user_stream_is_stale():
            return False
        with self._lock:
            has_open_positions = any(
                safe_float(p.get("quantity")) > 0 for p in self._positions
            )
        if has_open_positions:
            return True
        idle = time.monotonic() - self._last_user_event_at
        return idle >= float(Config.WS_USER_IDLE_RECONNECT_SECONDS)

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
            self._note_ws_frame("kline")
            try:
                if 0 <= socket_idx < len(self._kline_sockets):
                    self._kline_sockets[socket_idx].last_event_at = time.monotonic()
                if not isinstance(message, dict):
                    return
                handler(message)
            except Exception as exc:
                if is_read_loop_closed_error(exc):
                    self._request_kline_socket_reconnect(socket_idx, str(exc))
                elif self._ws_log.should_log(f"kline_cb:{type(exc).__name__}"):
                    error_logger.warning("Kline WebSocket callback error: %s", exc)

        return _wrapped

    def _wrap_ws_callback(
        self,
        handler: Callable[[dict[str, Any]], None],
        stream: str = "ticker",
    ) -> Callable[[dict[str, Any]], None]:
        """Catch library error passthrough and treat any frame as connection life."""

        def _wrapped(message: dict[str, Any]) -> None:
            if is_ws_error_message(message):
                detail = str(message.get("m", message.get("type", "ws error")))
                system_logger.warning("[WS_ERROR] %s stream=%s", detail, stream)
                if is_read_loop_closed_error(detail):
                    self._request_reconnect(detail)
                return
            self._note_ws_frame(stream)
            if not isinstance(message, dict):
                return
            try:
                handler(message)
            except Exception as exc:
                if is_read_loop_closed_error(exc):
                    self._request_reconnect(str(exc))
                else:
                    system_logger.warning(
                        "[WS_ERROR] callback %s on %s stream: %s",
                        type(exc).__name__,
                        stream,
                        exc,
                    )

        return _wrapped

    def reconnect_stale_streams(self, reason: str) -> None:
        """Public entry: schedule WS reconnect when monitor/watchdog detects stale data."""
        self._request_reconnect(reason)

    def _request_reconnect(self, reason: str) -> None:
        if not Config.WS_RECONNECT_ENABLED or not Config.ENABLE_WEBSOCKET_STREAMS:
            return
        if self.is_ws_warming_up():
            return

        preserve_cache = self._preserve_cache_on_reconnect()

        now = time.monotonic()
        if (now - self._last_reconnect_request_at) < Config.WS_RECONNECT_DEBOUNCE_SECONDS:
            return
        self._last_reconnect_request_at = now

        with self._reconnect_lock:
            if self._reconnect_in_progress:
                return
            self._reconnect_in_progress = True
            self._last_real_ticker_at = 0.0

        silent = preserve_cache and "stale" not in reason.lower()
        if "stale" in reason.lower():
            system_logger.info("[WS_RECONNECT_ATTEMPT] stale stream: %s", reason)
        elif silent:
            system_logger.info("[WS_RECONNECT_ATTEMPT] cache-preserving refresh: %s", reason)
        else:
            system_logger.warning("[WS_DISCONNECTED] %s", reason)
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
            system_logger.info(
                "[WS_RECONNECT_ATTEMPT] in %.1fs (attempt %s) reason=%s",
                delay,
                self._reconnect_policy.attempt,
                reason,
            )
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
            self._mark_stream_freshness()
            self._last_stale_reconnect_success_at = time.monotonic()
            self._ws_log.reset()
            system_logger.info(
                "[WS_RECONNECTED] streams re-subscribed (miniTicker + bookTicker + userData) after: %s",
                reason,
            )
            system_logger.info("[WS_SUBSCRIPTION_READY] post-reconnect subscriptions active.")
            if preserve_cache and self._ws_log.should_log("ws_reconnect_quiet"):
                system_logger.debug(
                    "WebSocket reconnected — continuing with cached tickers/klines."
                )
            if silent:
                system_logger.debug("WebSocket reconnected successfully (silent).")
            else:
                system_logger.info("WebSocket reconnected successfully.")
        except Exception as exc:
            system_logger.error(
                "[WS_ERROR] reconnect failed (attempt %s): %s",
                self._reconnect_policy.attempt,
                exc,
            )
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
            except Exception as exc:
                error_logger.debug("Kline multiplex stop failed: %s", exc)
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
                    except Exception as exc:
                        error_logger.debug("User-data socket stop failed: %s", exc)
                if self._book_ticker_conn_key:
                    try:
                        manager.stop_socket(self._book_ticker_conn_key)
                    except Exception as exc:
                        error_logger.debug("BookTicker socket stop failed: %s", exc)
                if self._ticker_conn_key:
                    try:
                        manager.stop_socket(self._ticker_conn_key)
                    except Exception as exc:
                        error_logger.debug("Ticker socket stop failed: %s", exc)
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
                    self._last_book_event_at = now
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
            tick_prices: dict[str, float] = {}
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
                    tick_prices[symbol] = price
                    updated = True
                if updated:
                    self._last_ticker_event_at = now
                    self._last_real_ticker_at = now
            if updated:
                self._reconnect_policy.reset()
                for sym, tick_price in tick_prices.items():
                    self.update_position_mark_from_ticker(sym, tick_price)
                    self._emit_price_tick(sym, tick_price)
        except Exception as exc:
            error_logger.warning("Ticker WS parse error: %s", exc)

    def _on_user_message(self, message: dict[str, Any]) -> None:
        try:
            event = message.get("e")
            if event == "ACCOUNT_UPDATE":
                account = message.get("a", {})
                balances_raw = account.get("B", []) or []
                positions_raw = account.get("P", []) or []
                with self._lock:
                    if balances_raw:
                        for bal in balances_raw:
                            asset = str(bal.get("a", "")).upper()
                            if not asset:
                                continue
                            cross_wallet = safe_float(bal.get("cw"))
                            wallet = safe_float(bal.get("wb"))
                            value = cross_wallet if cross_wallet > 0 else wallet
                            if value > 0:
                                self._wallet_balances[asset] = value
                    if positions_raw:
                        pos_by_key: dict[tuple[str, str], dict[str, Any]] = {
                            (
                                str(p.get("symbol", "")).upper(),
                                str(p.get("positionSide", "")).upper(),
                            ): dict(p)
                            for p in self._positions
                        }
                        for pos in positions_raw:
                            symbol = str(pos.get("s", "")).upper()
                            position_side = str(pos.get("ps", "")).upper()
                            if not symbol or not position_side:
                                continue
                            quantity = abs(safe_float(pos.get("pa")))
                            key = (symbol, position_side)
                            if quantity <= 0:
                                pos_by_key.pop(key, None)
                                continue
                            existing = pos_by_key.get(key, {})
                            pos_by_key[key] = {
                                "symbol": symbol,
                                "positionSide": position_side,
                                "quantity": quantity,
                                "entry_price": safe_float(pos.get("ep"))
                                or safe_float(existing.get("entry_price")),
                                "unrealized_pnl": (
                                    safe_float(pos.get("up"))
                                    if pos.get("up") is not None
                                    else safe_float(existing.get("unrealized_pnl"))
                                ),
                                "mark_price": safe_float(pos.get("mp"))
                                or safe_float(existing.get("mark_price")),
                            }
                        self._positions = list(pos_by_key.values())
                        self._unrealized_pnl_total = sum(
                            safe_float(p.get("unrealized_pnl")) for p in self._positions
                        )
                    self._last_user_event_at = time.monotonic()
            elif event == "ORDER_TRADE_UPDATE":
                self._last_user_event_at = time.monotonic()
                self._record_order_trade_update(message)
            elif event == "ACCOUNT_CONFIG_UPDATE":
                self._last_user_event_at = time.monotonic()
        except Exception as exc:
            error_logger.warning("User WS parse error: %s", exc)

    def _record_order_trade_update(self, message: dict[str, Any]) -> None:
        """Parse ORDER_TRADE_UPDATE fill and store Binance realized PnL (rp field)."""
        order = message.get("o") or {}
        exec_type = str(order.get("x", "")).upper()
        if exec_type not in ("TRADE", "CALCULATED"):
            return
        order_id = str(order.get("i", ""))
        if not order_id:
            return
        realized = safe_float(order.get("rp"))
        fill_price = safe_float(order.get("L")) or safe_float(order.get("ap"))
        fill_qty = safe_float(order.get("l"))
        commission = safe_float(order.get("n"))
        symbol = str(order.get("s", "")).upper()
        status = str(order.get("X", "")).upper()
        self.fill_tracker.record(
            FillPnlRecord(
                order_id=order_id,
                symbol=symbol,
                position_side=str(order.get("ps", "BOTH")).upper(),
                realized_pnl=realized,
                commission=commission,
                fill_price=fill_price,
                fill_qty=fill_qty,
                trade_id=str(order.get("t", "")),
                timestamp_ms=int(safe_float(order.get("T"))),
                source="ws",
            )
        )
        if status in {"FILLED", "PARTIALLY_FILLED"} or fill_qty > 0:
            with self._lock:
                self._pending_order_fills.append(
                    {
                        "symbol": symbol,
                        "orderId": order_id,
                        "clientOrderId": str(order.get("c") or ""),
                        "status": status or "FILLED",
                        "avgPrice": fill_price,
                        "executedQty": fill_qty or safe_float(order.get("z")),
                        "positionSide": str(order.get("ps", "")).upper(),
                    }
                )

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
                if row["closed"]:
                    self._sync_candles_from_klines(symbol, interval)
                else:
                    self._patch_candle_cache_last_row(symbol, interval, row)
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

    def register_price_tick_listener(
        self, listener: Callable[[str, float], None]
    ) -> None:
        """Register callback(symbol, price) on miniTicker WS updates."""
        if listener not in self._price_tick_listeners:
            self._price_tick_listeners.append(listener)

    def _emit_price_tick(self, symbol: str, price: float) -> None:
        for listener in list(self._price_tick_listeners):
            try:
                listener(symbol, price)
            except Exception as exc:
                error_logger.warning("Price tick listener error for %s: %s", symbol, exc)

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
            for pair in [p for p in self._bootstrapped_pairs if p[0] == symbol]:
                self._bootstrapped_pairs.discard(pair)

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
            dataframe=df,
            last_bar_open_ms=bar_open_ms,
        )

    def _patch_candle_cache_last_row(
        self, symbol: str, interval: str, row: dict[str, Any]
    ) -> None:
        """Update the forming bar in-place instead of rebuilding the full DataFrame."""
        limit = Config.CANDLE_FETCH_LIMIT
        cache_key = (symbol, interval, limit)
        entry = self._candles.get(cache_key)
        if entry is None or entry.dataframe.empty:
            self._sync_candles_from_klines(symbol, interval)
            return

        df = entry.dataframe
        ts = row["timestamp"]
        last_idx = len(df) - 1
        last_ts = df.iloc[last_idx]["timestamp"]
        if pd.Timestamp(last_ts) != pd.Timestamp(ts):
            new_row = {
                "timestamp": ts,
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            if len(df) > limit:
                df = df.iloc[-limit:].reset_index(drop=True)
            entry.dataframe = df
            return

        df.iloc[last_idx, df.columns.get_loc("open")] = row["open"]
        df.iloc[last_idx, df.columns.get_loc("high")] = row["high"]
        df.iloc[last_idx, df.columns.get_loc("low")] = row["low"]
        df.iloc[last_idx, df.columns.get_loc("close")] = row["close"]
        df.iloc[last_idx, df.columns.get_loc("volume")] = row["volume"]

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
        Subscribe WS kline streams first, warm up from live WS cache, then paced REST.
        Must be called outside scan_context (via exchange.bootstrap_context()).
        """
        self.subscribe_kline_streams(symbols)
        if not rest_fetcher or not Config.ENABLE_WS_KLINE_STARTUP_BOOTSTRAP:
            return self._seed_bootstrapped_from_ws_cache(symbols, intervals)

        warmup = max(Config.WS_KLINE_BOOTSTRAP_WARMUP_SECONDS, 0.0)
        if warmup > 0:
            self._wait_for_ws_kline_warmup(symbols, intervals, warmup)

        return self.bootstrap_klines_on_subscribe(symbols, intervals, rest_fetcher)

    def _seed_bootstrapped_from_ws_cache(
        self,
        symbols: list[str],
        intervals: list[str],
    ) -> int:
        """Mark pairs ready when WS kline buffer already has enough bars."""
        limit = Config.CANDLE_FETCH_LIMIT
        min_bars = max(Config.WS_KLINE_BOOTSTRAP_MIN_BARS, 10)
        seeded = 0
        for symbol in symbols:
            sym = symbol.upper()
            for interval in intervals:
                pair = (sym, interval)
                if pair in self._bootstrapped_pairs:
                    continue
                cached = self.get_candles_cached_only(sym, interval, limit)
                if not cached.empty and len(cached) >= min_bars:
                    self._bootstrapped_pairs.add(pair)
                    seeded += 1
        if seeded:
            system_logger.info(
                "WS kline cache satisfied %s/%s series — REST bootstrap skipped for those.",
                seeded,
                len(symbols) * len(intervals),
            )
        return seeded

    def _wait_for_ws_kline_warmup(
        self,
        symbols: list[str],
        intervals: list[str],
        timeout_seconds: float,
    ) -> None:
        """Allow live WS kline streams to populate buffers before REST backfill."""
        deadline = time.monotonic() + timeout_seconds
        min_partial = max(Config.WS_KLINE_BOOTSTRAP_MIN_BARS // 5, 20)
        limit = min(Config.CANDLE_FETCH_LIMIT, 120)
        check_symbols = [s.upper() for s in symbols[: Config.HOT_SCAN_SIZE]]

        while time.monotonic() < deadline:
            ready = 0
            for sym in check_symbols:
                for interval in intervals:
                    cached = self.get_candles_cached_only(sym, interval, limit)
                    if len(cached) >= min_partial:
                        ready += 1
            if ready >= min(len(check_symbols) * len(intervals), 3):
                system_logger.info(
                    "WS kline warmup ready — %s series have partial history.",
                    ready,
                )
                return
            time.sleep(0.5)

        system_logger.debug(
            "WS kline warmup timeout (%.1fs) — proceeding with paced REST backfill.",
            timeout_seconds,
        )

    def _pending_bootstrap_pairs(
        self,
        symbols: list[str],
        intervals: list[str],
        *,
        min_bars: int,
        limit: int,
    ) -> list[tuple[str, str]]:
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
        return pending

    def bootstrap_klines_on_subscribe(
        self,
        symbols: list[str],
        intervals: list[str],
        rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    ) -> int:
        """
        WS-first historical warmup — REST backfill only for missing series.
        """
        blocked, reason = self.is_rest_blocked()
        if blocked:
            system_logger.warning(
                "Kline REST backfill skipped — REST blocked: %s (WS cache only).",
                reason,
            )
            return self._seed_bootstrapped_from_ws_cache(symbols, intervals)

        limit = Config.CANDLE_FETCH_LIMIT
        min_bars = max(Config.WS_KLINE_BOOTSTRAP_MIN_BARS, 10)

        ws_seeded = self._seed_bootstrapped_from_ws_cache(symbols, intervals)
        pending = self._pending_bootstrap_pairs(
            symbols, intervals, min_bars=min_bars, limit=limit
        )

        if not pending:
            return ws_seeded

        exchange = getattr(rest_fetcher, "__self__", None)
        can_fetch = None
        if exchange is not None and hasattr(exchange, "can_bootstrap_klines_rest"):
            can_fetch = exchange.can_bootstrap_klines_rest

        def _mark_bootstrapped(sym: str, interval: str) -> None:
            self._bootstrapped_pairs.add((sym.upper(), interval))

        result = run_batched_kline_bootstrap(
            pending,
            rest_fetcher,
            limit,
            min_bars,
            seed_fn=self.seed_klines_from_dataframe,
            mark_bootstrapped=_mark_bootstrapped,
            can_fetch=can_fetch,
            request_delay_seconds=max(
                Config.KLINE_REST_MIN_INTERVAL_SECONDS,
                Config.WS_KLINE_BOOTSTRAP_REST_DELAY_SECONDS,
                Config.KLINE_BOOTSTRAP_INTER_REQUEST_DELAY_SECONDS,
                0.3,
            ),
        )
        if result.aborted:
            system_logger.warning(
                "Kline REST backfill aborted — continuing with WebSocket live candles."
            )
        return ws_seeded + result.seeded

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
        pending = self._pending_bootstrap_pairs(
            symbols, intervals, min_bars=min_bars, limit=limit
        )
        if not pending:
            return 0

        now = time.monotonic()
        while self._bootstrap_series_window and self._bootstrap_series_window[0] < now - 60.0:
            self._bootstrap_series_window.popleft()
        per_minute = max(int(getattr(Config, "KLINE_BOOTSTRAP_MAX_SERIES_PER_MINUTE", 12)), 1)
        remaining = per_minute - len(self._bootstrap_series_window)
        if remaining <= 0:
            system_logger.info(
                "Kline bootstrap paced — %s series already requested in the last 60s.",
                per_minute,
            )
            return 0
        pending = pending[:remaining]
        for _ in pending:
            self._bootstrap_series_window.append(now)

        exchange = getattr(rest_fetcher, "__self__", None)
        can_fetch = None
        if exchange is not None and hasattr(exchange, "can_bootstrap_klines_rest"):
            can_fetch = exchange.can_bootstrap_klines_rest

        def _mark_bootstrapped(sym: str, interval: str) -> None:
            self._bootstrapped_pairs.add((sym.upper(), interval))

        result = run_batched_kline_bootstrap(
            pending,
            rest_fetcher,
            limit,
            min_bars,
            seed_fn=self.seed_klines_from_dataframe,
            mark_bootstrapped=_mark_bootstrapped,
            can_fetch=can_fetch,
            max_pairs=max_pairs,
            request_delay_seconds=max(
                Config.KLINE_REST_MIN_INTERVAL_SECONDS,
                Config.WS_KLINE_BOOTSTRAP_REST_DELAY_SECONDS,
                Config.KLINE_BOOTSTRAP_INTER_REQUEST_DELAY_SECONDS,
                0.3,
            ),
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

    def get_fresh_ticker_price(
        self,
        symbol: str,
        *,
        max_age_seconds: float = 30.0,
    ) -> Optional[float]:
        """Last miniTicker price if updated within max_age_seconds."""
        symbol = symbol.upper()
        now = time.monotonic()
        with self._lock:
            row = self._tickers.get(symbol)
            if not row:
                return None
            updated_at = safe_float(row.get("updated_at"))
            if updated_at <= 0 or (now - updated_at) > max_age_seconds:
                return None
            price = safe_float(row.get("lastPrice"))
            return price if price > 0 else None

    def update_position_mark_from_ticker(self, symbol: str, price: float) -> None:
        """Refresh cached mark prices between sparse ACCOUNT_UPDATE events."""
        if price <= 0:
            return
        symbol = symbol.upper()
        with self._lock:
            updated = False
            for pos in self._positions:
                if pos.get("symbol") == symbol:
                    pos["mark_price"] = price
                    updated = True
            if updated:
                self._last_user_event_at = time.monotonic()

    def get_ticker_map(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._tickers)

    def ws_is_stale(self) -> bool:
        if not self._ws_running:
            return True
        if self._reconnect_in_progress:
            return False
        stamp = self._last_health_stamp()
        if stamp <= 0:
            return not self.is_ws_warming_up()
        return (time.monotonic() - stamp) > self._effective_ticker_stale_seconds()

    def _book_stream_is_stale(self) -> bool:
        if not Config.ENABLE_WS_BOOK_STREAM:
            return False
        if not self._ws_running or self._reconnect_in_progress:
            return False
        if self._last_book_event_at <= 0:
            return self.ws_is_stale()
        return (
            time.monotonic() - self._last_book_event_at
        ) > self._effective_ticker_stale_seconds()

    def user_stream_has_account_data(self) -> bool:
        """True after at least one user-data WS event (ACCOUNT_UPDATE / order fill)."""
        return self._last_user_event_at > 0

    def user_stream_is_stale(self) -> bool:
        if not self._ws_running:
            return True
        now = time.monotonic()
        socket_stamp = max(self._last_user_socket_at, self._last_user_event_at)
        if socket_stamp <= 0:
            if self._ws_started_at > 0 and (now - self._ws_started_at) < Config.WS_WARMUP_SECONDS:
                return False
            # Quiet listen-key (no ACCOUNT_UPDATE yet) is not a dead socket.
            return False
        return (now - socket_stamp) > Config.WS_USER_STALE_SECONDS

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
            for pos in self._positions:
                if pos.get("symbol") == symbol and pos.get("positionSide") == position_side:
                    return safe_float(pos.get("quantity"))
        return 0.0

    def get_ws_mark_price(self, symbol: str, position_side: str = "LONG") -> Optional[float]:
        """Mark price from the latest user-stream position update."""
        symbol = symbol.upper()
        position_side = position_side.upper()
        with self._lock:
            for pos in self._positions:
                if pos.get("symbol") != symbol:
                    continue
                if pos.get("positionSide") != position_side:
                    continue
                mark = safe_float(pos.get("mark_price"))
                if mark > 0:
                    return mark
        return None

    def clear_position(self, symbol: str, position_side: str) -> None:
        """Remove a closed position from the user-stream cache."""
        symbol = symbol.upper()
        position_side = position_side.upper()
        with self._lock:
            self._positions = [
                pos
                for pos in self._positions
                if not (
                    pos.get("symbol") == symbol
                    and pos.get("positionSide") == position_side
                )
            ]
            self._unrealized_pnl_total = sum(
                safe_float(p.get("unrealized_pnl")) for p in self._positions
            )

    def seed_open_position(
        self,
        symbol: str,
        position_side: str,
        quantity: float,
        entry_price: float,
    ) -> None:
        """Seed user-stream position cache immediately after local fill (no REST)."""
        symbol = symbol.upper()
        position_side = position_side.upper()
        with self._lock:
            self._last_user_event_at = time.monotonic()
            for pos in self._positions:
                if pos.get("symbol") == symbol and pos.get("positionSide") == position_side:
                    pos["quantity"] = quantity
                    pos["entry_price"] = entry_price
                    return
            self._positions.append(
                {
                    "symbol": symbol,
                    "positionSide": position_side,
                    "quantity": quantity,
                    "entry_price": entry_price,
                    "unrealized_pnl": 0.0,
                }
            )

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
        if self.ws_is_degraded() or self._governor_blocks_background_rest(5):
            return cached

        miss_key = (symbol.upper(), timeframe)
        last_miss = self._cache_miss_rest_at.get(miss_key, 0.0)
        if cached.empty and (time.monotonic() - last_miss) < max(
            float(Config.WS_DEGRADED_REST_MIN_INTERVAL_SECONDS), 60.0
        ):
            return cached
        if cached.empty:
            self._cache_miss_rest_at[miss_key] = time.monotonic()

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
