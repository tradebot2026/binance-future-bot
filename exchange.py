"""
Binance USDT-M Futures exchange adapter.
Thread-safe symbol rules cache, rate limiting, balance caching, and order execution.
"""

from __future__ import annotations

import json
import random
import inspect
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Generator, Optional

import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException

from rest_rate_guard import (
    RestBlockLogSuppressor,
    RestUsageTracker,
    build_default_token_bucket,
    weight_for_call,
)
from config import Config
from core.rest_budget import RestBudgetManager, RestLane
from exceptions import (
    ExchangeError,
    ExchangeRateLimitError,
    OrderExecutionError,
    PositionAlreadyClosedError,
)
from logger import error_logger, system_logger, trade_logger
from utils import amount_to_precision, round_step_size, safe_float


@dataclass
class SymbolRules:
    price_precision: int
    quantity_precision: int
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "price_precision": self.price_precision,
            "quantity_precision": self.quantity_precision,
            "tick_size": self.tick_size,
            "step_size": self.step_size,
            "min_qty": self.min_qty,
            "min_notional": self.min_notional,
        }


@dataclass
class LiveAccountSnapshot:
    """Authoritative USDT-M account fields from fapi/v2/account (+ optional income)."""

    wallet_balance: float = 0.0
    margin_balance: float = 0.0
    unrealized_pnl: float = 0.0
    available_balance: float = 0.0
    today_realized_pnl: float = 0.0
    source: str = "unknown"


@dataclass
class ClosedPositionPnl:
    """Realized PnL for a closed position from Binance trade/income history."""

    realized_pnl: float = 0.0
    commission: float = 0.0
    exit_price: float = 0.0
    fill_count: int = 0
    source: str = "unknown"


@dataclass
class BalanceCache:
    value: float = 0.0
    updated_at: float = 0.0
    ttl_seconds: int = Config.BALANCE_CACHE_TTL_SECONDS

    def is_valid(self) -> bool:
        return self.updated_at > 0 and (time.monotonic() - self.updated_at) < self.ttl_seconds

    def last_known(self) -> float:
        """Last non-zero wallet even if the TTL has expired (rate-limit fallback)."""
        return self.value if self.value > 0 else 0.0

    def set(self, balance: float) -> None:
        if balance > 0:
            self.value = balance
            self.updated_at = time.monotonic()

    def invalidate(self) -> None:
        self.updated_at = 0.0


@dataclass
class PositionCache:
    """TTL cache for bulk futures_position_information REST responses."""

    positions: list[dict[str, Any]] = field(default_factory=list)
    unrealized_pnl_total: float = 0.0
    updated_at: float = 0.0
    ttl_seconds: int = Config.POSITION_CACHE_TTL_SECONDS

    def is_valid(self) -> bool:
        return self.updated_at > 0 and (time.monotonic() - self.updated_at) < self.ttl_seconds


@dataclass
class AccountRestCache:
    """Cached futures_account() payload — fallback when REST is banned or throttled."""

    account_info: dict[str, Any] = field(default_factory=dict)
    updated_at: float = 0.0

    def is_valid(self) -> bool:
        return bool(self.account_info) and (
            time.monotonic() - self.updated_at
        ) < Config.BALANCE_CACHE_TTL_SECONDS


@dataclass
class DerivativesCacheEntry:
    """Per-symbol funding + open interest snapshot."""

    funding_rate: float = 0.0
    open_interest: float = 0.0
    oi_change_pct: float = 0.0
    mark_price: float = 0.0
    updated_at: float = 0.0

    def is_valid(self) -> bool:
        return self.updated_at > 0 and (
            time.monotonic() - self.updated_at
        ) < Config.DERIVATIVES_CACHE_TTL_SECONDS

    def as_dict(self) -> dict[str, Any]:
        return {
            "funding_rate": self.funding_rate,
            "open_interest": self.open_interest,
            "oi_change_pct": self.oi_change_pct,
            "mark_price": self.mark_price,
        }


ACCOUNT_REST_ENDPOINTS = frozenset(
    {"futures_account", "futures_position_information", "futures_account_balance"}
)


class RateLimiter:
    """Simple inter-request throttle to reduce ban risk."""

    def __init__(self, min_interval_ms: int) -> None:
        self._min_interval = max(min_interval_ms, 0) / 1000.0
        self._lock = threading.Lock()
        self._last_request = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request = time.monotonic()


class BinanceExchangeManager:
    """
    Binance Futures integration with caching, retries, and thread-safe rule access.

    Strict rate-limit protection (equivalent to enableRateLimit=True in CCXT) is
    enforced via MIN_REQUEST_INTERVAL_MS throttling plus exponential backoff with
    jitter on HTTP 429/418 and Binance codes -1003/-1015.
    """

    RATE_LIMIT_CODES = frozenset({-1003, -1015, 429, 418})

    DEFAULT_RULES = SymbolRules(
        price_precision=2,
        quantity_precision=3,
        tick_size=0.01,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
    )

    def __init__(self) -> None:
        self.api_key = Config.BINANCE_API_KEY
        self.api_secret = Config.BINANCE_API_SECRET
        self.testnet = Config.USE_TESTNET

        client_kwargs: dict[str, Any] = {
            "api_key": self.api_key,
            "api_secret": self.api_secret,
            "testnet": self.testnet,
            "requests_params": {"timeout": Config.REQUEST_TIMEOUT},
        }
        client_params = inspect.signature(Client.__init__).parameters
        if "strict_rate_limit" in client_params:
            client_kwargs["strict_rate_limit"] = Config.ENABLE_STRICT_RATE_LIMIT
        self._rest_usage = RestUsageTracker()
        self.client = Client(**client_kwargs)
        self.recv_window_param = {"recvWindow": 60000}
        self._install_rest_usage_hook()

        self._rules_lock = threading.RLock()
        self._symbol_rules_cache: dict[str, SymbolRules] = {}
        self._leverage_cache: dict[str, int] = {}
        self._rate_limiter = RateLimiter(Config.MIN_REQUEST_INTERVAL_MS)
        self._execution_rate_limiter = RateLimiter(
            Config.EXECUTION_MIN_REQUEST_INTERVAL_MS
        )
        self._rest_token_bucket = build_default_token_bucket()
        self._rest_budget = RestBudgetManager()
        self._rest_block_log = RestBlockLogSuppressor(
            Config.REST_BLOCK_LOG_INTERVAL_SECONDS
        )
        self._balance_cache = BalanceCache()
        self._balance_rest_backoff_until: float = 0.0
        self._cold_start_balance_rest_done: bool = False
        self._last_balance_rest_at: float = 0.0
        self._last_account_rest_at: float = 0.0
        self._last_degraded_rest_at: dict[str, float] = {}
        self._account_rest_cache = AccountRestCache()
        self._position_cache = PositionCache()
        self._position_refresh_lock = threading.Lock()
        self._position_backoff_until: float = 0.0
        self._scan_mode = False
        self._scan_mode_lock = threading.Lock()
        self._execution_depth = 0
        self._execution_lock = threading.Lock()
        self._bootstrap_depth = 0
        self._bootstrap_lock = threading.Lock()
        self._rest_mark_fetched_at: dict[str, float] = {}
        self._rest_mark_cache: dict[str, float] = {}
        self._rest_mark_lock = threading.Lock()
        self._kline_bootstrap_halted = False
        self._kline_rest_lock = threading.Lock()
        self._last_kline_rest_at: float = 0.0
        self._symbol_position_rest_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._all_positions_rest_at: float = 0.0
        self._all_positions_rest_data: Optional[list[dict[str, Any]]] = None
        self._derivatives_cache: dict[str, DerivativesCacheEntry] = {}
        self._derivatives_lock = threading.Lock()
        self._critical_alerts: Any = None
        self._market_data: Any = None
        self._full_init_done = False
        self._ws_rest_ready = False

        ban = self.check_startup_ban()
        if ban.is_banned:
            error_logger.critical(
                "Binance IP ban detected at startup | until=%s | %s",
                ban.banned_until_iso or "unknown",
                ban.message,
            )
            system_logger.warning(
                "Deferring ALL REST init until ban expires (~%ss). WebSocket-only mode.",
                ban.seconds_remaining,
            )
            if ban.seconds_remaining > 0:
                halt = max(float(ban.seconds_remaining), float(Config.REST_BAN_MIN_SLEEP_SECONDS))
                self._rest_token_bucket.trigger_hard_stop(halt)

        mode = "TESTNET" if self.testnet else "MAINNET"
        strict = "ON" if Config.ENABLE_STRICT_RATE_LIMIT else "OFF"
        system_logger.info(
            "Binance Futures exchange initialized (%s, strict_rate_limit=%s).",
            mode,
            strict,
        )

    def attach_critical_alerts(self, alerts: Any) -> None:
        """Optional hook for immediate Telegram critical notifications."""
        self._critical_alerts = alerts

    def attach_market_data(self, hub: Any) -> None:
        """Attach WebSocket + candle cache hub."""
        self._market_data = hub
        if hub is not None:
            hub.set_ticker_rest_fetcher(self.fetch_futures_ticker_map_rest)
            if hasattr(hub, "set_rest_governor"):
                hub.set_rest_governor(self)
        if hub and hasattr(self, "_startup_ban") and self._startup_ban.is_banned:
            hub.apply_startup_ban(self._startup_ban)

    def get_market_data_hub(self) -> Any:
        """Return the attached MarketDataHub (if any)."""
        return self._market_data

    def mark_ws_rest_ready(self) -> None:
        """Called after WebSocket cache warm-up — allows deferred REST init."""
        self._ws_rest_ready = True

    def _rest_gate_open(self, execution_priority: bool = False) -> tuple[bool, str]:
        """Return False if REST must not be attempted."""
        if (
            Config.DEFER_REST_UNTIL_WS_READY
            and not execution_priority
            and not self._ws_rest_ready
        ):
            return False, "deferred_until_ws_ready"
        if self._market_data:
            blocked, reason = self._market_data.is_rest_blocked()
            if blocked:
                if execution_priority:
                    ban = self._market_data.get_ban_status()
                    if ban and ban.is_banned and ban.seconds_remaining > 0:
                        return False, reason
                    return True, ""
                return False, reason
        if not execution_priority and self._rest_usage.in_safety_mode():
            remaining = int(self._rest_usage.safety_remaining())
            return False, (
                f"API SAFETY MODE — REST halted "
                f"({remaining}s remaining, {self._rest_usage.snapshot().get('state')})"
            )
        if not execution_priority and not self._rest_usage.allows_background_rest():
            snap = self._rest_usage.snapshot()
            remaining = int(snap.get("weight_throttle_remaining_seconds") or 0)
            return False, (
                f"REST weight governor — {snap.get('state')} "
                f"(used_weight_1m={snap.get('used_weight_1m')}, pause {remaining}s)"
            )
        return True, ""

    @contextmanager
    def scan_context(self) -> Generator[None, None, None]:
        """Block REST reads during market scan / strategy evaluation loops."""
        with self._scan_mode_lock:
            self._scan_mode = True
        try:
            yield
        finally:
            with self._scan_mode_lock:
                self._scan_mode = False

    @contextmanager
    def execution_context(self) -> Generator[None, None, None]:
        """High-priority path for live orders — bypasses scan REST guards."""
        with self._execution_lock:
            self._execution_depth += 1
        try:
            yield
        finally:
            with self._execution_lock:
                self._execution_depth = max(0, self._execution_depth - 1)

    @contextmanager
    def bootstrap_context(self) -> Generator[None, None, None]:
        """
        Allow one-time REST kline seeding outside scan cycles only.
        Used when symbols are first subscribed to WS kline streams.
        """
        if self.in_scan_mode:
            raise ExchangeError(
                "Kline bootstrap REST is forbidden during active scan cycle."
            )
        with self._bootstrap_lock:
            self._bootstrap_depth += 1
        try:
            yield
        finally:
            with self._bootstrap_lock:
                self._bootstrap_depth = max(0, self._bootstrap_depth - 1)

    def _install_rest_usage_hook(self) -> None:
        """Count every HTTP call and capture used-weight / Retry-After headers."""
        session = getattr(self.client, "session", None)
        if session is None or getattr(session, "_bfb_usage_hooked", False):
            return
        original = session.request
        tracker = self._rest_usage

        def _hooked(method: str, url: str, **kwargs: Any) -> Any:
            try:
                response = original(method, url, **kwargs)
            except Exception as exc:
                tracker.note_transport_error(exc)
                error_logger.warning(
                    "REST transport error %s %s: %s", method, url, exc
                )
                raise
            try:
                tracker.note_http_response(response)
            except Exception as exc:
                error_logger.debug("REST usage tracker failed: %s", exc)
            return response

        session.request = _hooked
        session._bfb_usage_hooked = True

    def rest_usage_snapshot(self) -> dict[str, Any]:
        return self._rest_usage.snapshot()

    def get_execution_safety(self, symbol: str = "") -> tuple[str, str]:
        """Infrastructure gate for NEW entries only. Position monitoring stays active."""
        usage = self._rest_usage.snapshot()
        state = str(usage.get("state", "HEALTHY"))
        if state in {"IP_BANNED", "API_RATE_LIMITED"}:
            remaining = int(usage.get("safety_remaining_seconds") or 0)
            reason = usage.get("reason") or f"REST safety mode ({remaining}s)"
            self._log_entry_block("RATE_LIMITED", reason, symbol)
            return state, reason
        hub = self._market_data
        if hub is not None:
            ready_fn = getattr(hub, "is_market_data_ready_for_entry", None)
            if callable(ready_fn):
                ready, detail = ready_fn(symbol)
                if not ready:
                    mapped = {
                        "WS_DISCONNECTED": "WS_DISCONNECTED",
                        "WS_RECONNECTING": "RESYNC_REQUIRED",
                        "WS_WARMUP": "WS_WARMING",
                        "STALE_DATA": "STALE_DATA",
                        "RESYNC": "RESYNC_REQUIRED",
                    }.get(detail, "RESYNC_REQUIRED")
                    self._log_entry_block(detail or mapped, mapped, symbol)
                    return mapped, detail or mapped
            else:
                if getattr(hub, "_reconnect_in_progress", False):
                    self._log_entry_block("WS_RECONNECTING", "reconnect", symbol)
                    return "RESYNC_REQUIRED", "WebSocket reconnecting — waiting to resync"
                if getattr(hub, "is_ws_warming_up", lambda: False)():
                    self._log_entry_block("WS_WARMUP", "warming", symbol)
                    return "WS_WARMING", "market-data WebSocket is warming up"
                if not hub.ws_is_running():
                    self._log_entry_block("WS_DISCONNECTED", "ws down", symbol)
                    return "WS_DISCONNECTED", "market-data WebSocket is down"
                if not hub.is_ticker_cache_usable():
                    self._log_entry_block("RESYNC", "empty ticker cache", symbol)
                    return "RESYNC_REQUIRED", "ticker cache empty after disconnect"
        if not hasattr(self, "_entry_ready_logged"):
            self._entry_ready_logged = False
        if not self._entry_ready_logged:
            trade_logger.info("[ENTRY_DATA_READY] market data + API healthy for new entries")
            self._entry_ready_logged = True
        return "EXECUTION_SAFE", ""

    def _log_entry_block(self, kind: str, detail: str, symbol: str = "") -> None:
        tag = {
            "WS_DISCONNECTED": "ENTRY_BLOCKED_WS_DISCONNECTED",
            "WS_RECONNECTING": "ENTRY_BLOCKED_WS_RECONNECTING",
            "WS_WARMUP": "ENTRY_BLOCKED_WS_WARMUP",
            "STALE_DATA": "ENTRY_BLOCKED_STALE_DATA",
            "RESYNC": "ENTRY_BLOCKED_RESYNC",
            "RATE_LIMITED": "ENTRY_BLOCKED_RESYNC",
        }.get(kind, f"ENTRY_BLOCKED_{kind}")
        key = f"{tag}:{symbol or '*'}"
        if not hasattr(self, "_entry_block_log"):
            self._entry_block_log = RestBlockLogSuppressor(90)
        if self._entry_block_log.should_log(key):
            trade_logger.warning("[%s] %s %s", tag, symbol or "ALL", detail)
        self._entry_ready_logged = False

    @property
    def in_scan_mode(self) -> bool:
        with self._scan_mode_lock:
            return self._scan_mode

    def _is_bootstrap_priority(self) -> bool:
        with self._bootstrap_lock:
            return self._bootstrap_depth > 0

    def _is_execution_priority(self) -> bool:
        with self._execution_lock:
            return self._execution_depth > 0

    def _scan_ws_only(self) -> bool:
        return bool(getattr(Config, "SCAN_WS_ONLY", True))

    def _background_ws_only(self) -> bool:
        """Scan/monitor/risk paths must not hit REST when this is enabled."""
        if self._is_bootstrap_priority():
            return False
        return bool(Config.BACKGROUND_WS_ONLY) and not self._is_execution_priority()

    def _rest_reads_allowed(self) -> bool:
        if self._background_ws_only():
            return False
        if self._scan_mode and self._scan_ws_only():
            return False
        allowed, _ = self._rest_gate_open()
        return allowed

    def _rest_block_applies(self, execution_priority: bool) -> tuple[bool, str]:
        allowed, reason = self._rest_gate_open(execution_priority)
        if not allowed:
            return True, reason
        if self._rest_token_bucket.is_hard_stopped() and not execution_priority:
            remaining = int(self._rest_token_bucket.hard_stop_remaining())
            return True, f"REST hard-stop (~{remaining}s remaining)"
        return False, ""

    def check_startup_ban(self) -> Any:
        from market_data_hub import check_binance_ban_status

        self._startup_ban = check_binance_ban_status(self.client)
        return self._startup_ban

    @staticmethod
    def _init_rest_pause() -> None:
        if Config.INIT_REST_DELAY_SECONDS > 0:
            time.sleep(Config.INIT_REST_DELAY_SECONDS)

    def get_startup_ban_status(self) -> Any:
        return getattr(self, "_startup_ban", None)

    def ensure_initialized(self) -> bool:
        """Run deferred REST init (account mode + symbol rules) once API is reachable."""
        if self._full_init_done:
            return True

        allowed, reason = self._rest_gate_open()
        if not allowed:
            system_logger.debug("Deferred REST init: %s", reason)
            return False

        ban = self.get_startup_ban_status()
        if ban and ban.is_banned:
            return False

        self._init_rest_pause()
        self._configure_account()
        self._init_rest_pause()
        self.refresh_symbol_rules()
        self._full_init_done = True
        return True

    def _apply_rate_limit_halt(self, exc: BinanceAPIException) -> int:
        """Hard-stop all REST for ban duration — no retries."""
        from market_data_hub import parse_ban_until_ms

        message = str(exc.message)
        until_ms = parse_ban_until_ms(message)
        banned = bool(until_ms) or exc.code == 418 or "banned until" in message.lower()
        if until_ms:
            halt_seconds = max(
                int((until_ms / 1000.0) - time.time()),
                Config.RATE_LIMIT_HALT_SECONDS,
            )
        elif exc.code == 418:
            halt_seconds = max(Config.RATE_LIMIT_HALT_SECONDS, 600)
        elif exc.code == -1003:
            halt_seconds = max(
                Config.RATE_LIMIT_SOFT_HALT_SECONDS,
                Config.RATE_LIMIT_HALT_SECONDS,
                300,
            )
        else:
            halt_seconds = max(
                Config.REST_BAN_MIN_SLEEP_SECONDS,
                Config.RATE_LIMIT_HALT_SECONDS,
            )

        self._rest_usage.note_binance_error(
            code=int(exc.code),
            message=message,
            halt_seconds=float(halt_seconds),
            banned=banned,
        )
        self._rest_token_bucket.trigger_hard_stop(float(halt_seconds))
        already_blocked = False
        if self._market_data:
            already_blocked = self._market_data.is_rest_blocked()[0]
            self._market_data.handle_rate_limit_error(exc)
        if not already_blocked and self._rest_block_log.should_log("rate_limit_halt"):
            error_logger.warning(
                "REST hard-stop for ~%ss after rate limit (code=%s). WebSocket-only until clear.",
                halt_seconds,
                exc.code,
            )
        if (
            not already_blocked
            and self._critical_alerts
            and exc.code in (-1003, 418, 429)
        ):
            self._critical_alerts.notify(
                "RATE_LIMIT",
                f"Binance IP/rate limit — REST halted ~{halt_seconds}s",
                exc=exc,
            )
        return halt_seconds

    # ---------------- Internal helpers ----------------

    def _backoff_seconds(self, attempt: int) -> float:
        base = min(2 ** attempt, Config.API_BACKOFF_MAX_SECONDS)
        jitter = random.uniform(0.0, 1.0) if Config.ENABLE_STRICT_RATE_LIMIT else 0.0
        return base + jitter

    def _is_rate_limit_error(self, exc: BinanceAPIException) -> bool:
        if exc.code in self.RATE_LIMIT_CODES:
            return True
        message = str(exc.message).lower()
        return "429" in message or "418" in message or "rate limit" in message

    @staticmethod
    def _is_rate_limit_error_text(exc: BaseException) -> bool:
        if isinstance(exc, ExchangeRateLimitError):
            return True
        if isinstance(exc, BinanceAPIException) and exc.code in (-1003, -1015, 429, 418):
            return True
        message = str(exc).lower()
        return (
            "-1003" in message
            or "too many requests" in message
            or "rate limit" in message
            or "used weight" in message
            or "banned until" in message
        )

    def _resolve_rest_lane(
        self, *, execution_priority: bool
    ) -> RestLane:
        if execution_priority or self._is_execution_priority():
            return RestLane.EXECUTION
        if self._is_bootstrap_priority():
            return RestLane.BOOTSTRAP
        return RestLane.BACKGROUND

    def background_account_rest_allowed(self) -> bool:
        """True when a non-execution account/status REST read may proceed."""
        if self.is_rest_blocked()[0]:
            return False
        if not self._rest_usage.allows_background_rest():
            return False
        if not self._degraded_rest_allowed("account", consume=False):
            return False
        return True

    def _ws_reconnect_or_warmup(self) -> bool:
        hub = self._market_data
        if hub is None:
            return False
        degraded = getattr(hub, "ws_is_degraded", None)
        if callable(degraded):
            try:
                return bool(degraded())
            except Exception:
                return False
        return bool(
            getattr(hub, "_reconnect_in_progress", False)
            or (getattr(hub, "is_ws_warming_up", lambda: False)())
        )

    def _degraded_rest_allowed(self, kind: str = "account", *, consume: bool = True) -> bool:
        """During WS reconnect/warmup, at most one REST of this kind per 60s."""
        if not self._ws_reconnect_or_warmup():
            return True
        interval = max(float(Config.WS_DEGRADED_REST_MIN_INTERVAL_SECONDS), 60.0)
        now = time.monotonic()
        last = self._last_degraded_rest_at.get(kind, 0.0)
        if last > 0 and (now - last) < interval:
            return False
        if consume:
            self._last_degraded_rest_at[kind] = now
        return True

    def wallet_is_hydrated(self) -> bool:
        """True when WS or last-known cache already has a usable wallet."""
        ws_balance = self._hydrate_balance_from_ws()
        if ws_balance is not None and ws_balance > 0:
            return True
        return self._balance_cache.last_known() > 0

    def can_make_background_rest_call(self, weight: int = 1) -> bool:
        """True when a non-execution REST call is allowed (ban, hard-stop, budget)."""
        if self._ws_reconnect_or_warmup():
            return False
        if self._market_data and self._market_data.is_rest_blocked()[0]:
            return False
        if self._rest_token_bucket.is_hard_stopped():
            return False
        if self._rest_usage.in_safety_mode() or not self._rest_usage.allows_background_rest():
            return False
        if not Config.ENABLE_STRICT_RATE_LIMIT:
            return True
        lane = RestLane.BOOTSTRAP if self._is_bootstrap_priority() else RestLane.BACKGROUND
        return self._rest_budget.has_budget_for(max(weight, 1), lane)

    def is_rest_blocked(self) -> tuple[bool, str]:
        """True when REST must not be attempted (IP ban / hard-stop)."""
        try:
            usage = self._rest_usage
            in_safety = bool(getattr(usage, "in_safety_mode", lambda: False)())
            if in_safety:
                remaining = int(getattr(usage, "safety_remaining", lambda: 0)() or 0)
                snap = usage.snapshot() if callable(getattr(usage, "snapshot", None)) else {}
                state = ""
                if isinstance(snap, dict):
                    state = str(snap.get("state") or "")
                if not state:
                    raw_state = getattr(usage, "health_state", None) or getattr(
                        usage, "_state", None
                    )
                    state = str(getattr(raw_state, "value", raw_state) or "UNKNOWN")
                return True, (
                    f"API SAFETY MODE — REST halted "
                    f"({remaining}s remaining, {state})"
                )
            if not bool(getattr(usage, "allows_background_rest", lambda: True)()):
                snap = usage.snapshot() if callable(getattr(usage, "snapshot", None)) else {}
                if not isinstance(snap, dict):
                    snap = {}
                remaining = int(snap.get("weight_throttle_remaining_seconds") or 0)
                state = str(snap.get("state") or "RATE_LIMIT_WARNING")
                return True, (
                    f"REST weight governor — {state} "
                    f"(used_weight_1m={snap.get('used_weight_1m', 0)}, pause {remaining}s)"
                )
            if self._market_data:
                blocked, reason = self._market_data.is_rest_blocked()
                if blocked:
                    return True, reason
            if self._rest_token_bucket.is_hard_stopped():
                remaining = int(self._rest_token_bucket.hard_stop_remaining())
                return True, f"REST hard-stop (~{remaining}s remaining)"
            return False, ""
        except Exception as exc:
            return True, f"REST safety check failed ({type(exc).__name__})"

    def rest_account_reads_blocked(self) -> bool:
        """True when account/position REST reads must use WS/cache only."""
        blocked, _ = self.is_rest_blocked()
        if blocked:
            return True
        if not Config.ENABLE_STRICT_RATE_LIMIT:
            return False
        reserve = max(Config.REST_BUDGET_ACCOUNT_RESERVE_FRACTION, 0.0)
        return self._rest_budget.remaining_fraction() < reserve

    @staticmethod
    def _is_account_rest_call(func: Any) -> bool:
        return getattr(func, "__name__", "") in ACCOUNT_REST_ENDPOINTS

    def _account_rest_interval_elapsed(self) -> bool:
        interval = max(float(Config.ACCOUNT_REST_MIN_INTERVAL_SECONDS), 60.0)
        if self._last_account_rest_at <= 0:
            return True
        return (time.monotonic() - self._last_account_rest_at) >= interval

    def _hydrate_balance_from_ws(self) -> Optional[float]:
        if not self._market_data or not self._market_data.user_stream_has_account_data():
            return None
        quote = Config.QUOTE_ASSET
        ws_balance = self._market_data.get_ws_wallet_balance(quote)
        if ws_balance <= 0:
            return None
        self._balance_cache.set(ws_balance)
        return ws_balance

    def _fallback_quote_balance(self) -> float:
        """Last-known USDT wallet: WS estimate, then cache (including expired TTL)."""
        margin_est = self._ws_margin_balance_estimate()
        if margin_est is not None and margin_est > 0:
            return margin_est
        last_known = self._balance_cache.last_known()
        if last_known > 0:
            return last_known
        cached = self._account_rest_cache.account_info
        if cached:
            balance = self._extract_quote_balance(cached, Config.QUOTE_ASSET)
            if balance > 0:
                return balance
        return 0.0

    @staticmethod
    def _coerce_api_payload(data: Any) -> Any:
        """
        Normalize Binance REST payloads that may arrive as dict, list, or JSON string.
        Returns None for HTML/plain-text error bodies and other unsupported shapes.
        """
        if isinstance(data, (dict, list)):
            return data
        if isinstance(data, str):
            text = data.strip()
            if not text or text[0] not in "{[":
                return None
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return None
        return None

    @staticmethod
    def _parse_futures_account_payload(
        data: Any,
        quote: str,
    ) -> Optional[dict[str, Any]]:
        """Convert futures_account or futures_account_balance responses to account dict."""
        payload = BinanceExchangeManager._coerce_api_payload(data)
        if payload is None:
            return None

        if isinstance(payload, dict):
            if (
                payload.get("assets")
                or payload.get("totalWalletBalance") is not None
                or payload.get("totalMarginBalance") is not None
                or payload.get("totalCrossWalletBalance") is not None
            ):
                return payload
            return None

        if isinstance(payload, list):
            account_info = BinanceExchangeManager._account_info_from_asset_balances(
                payload, quote
            )
            if (
                safe_float(account_info.get("totalWalletBalance")) > 0
                or safe_float(account_info.get("totalMarginBalance")) > 0
                or BinanceExchangeManager._extract_quote_balance(account_info, quote) > 0
            ):
                return account_info
        return None

    @staticmethod
    def _extract_quote_balance(account_info: Any, quote: str) -> float:
        if not isinstance(account_info, dict):
            parsed = BinanceExchangeManager._parse_futures_account_payload(
                account_info, quote
            )
            if not parsed:
                return 0.0
            account_info = parsed

        for key in (
            "availableBalance",
            "totalCrossWalletBalance",
            "totalWalletBalance",
        ):
            value = safe_float(account_info.get(key))
            if value > 0:
                return value

        assets = account_info.get("assets", [])
        if not isinstance(assets, list):
            return 0.0

        for asset in assets:
            if not isinstance(asset, dict):
                continue
            if str(asset.get("asset", "")).upper() != quote.upper():
                continue
            for field in (
                "availableBalance",
                "crossWalletBalance",
                "walletBalance",
                "balance",
            ):
                value = safe_float(asset.get(field))
                if value > 0:
                    return value
        return 0.0

    @staticmethod
    def _extract_margin_balance(account_info: Any, quote: str) -> float:
        """Return total margin balance (wallet + unrealized) when present."""
        if not isinstance(account_info, dict):
            parsed = BinanceExchangeManager._parse_futures_account_payload(
                account_info, quote
            )
            if not parsed:
                return 0.0
            account_info = parsed

        margin = safe_float(account_info.get("totalMarginBalance"))
        if margin > 0:
            return margin

        wallet = safe_float(account_info.get("totalWalletBalance"))
        if wallet <= 0:
            wallet = BinanceExchangeManager._extract_quote_balance(account_info, quote)
        unrealized = safe_float(account_info.get("totalUnrealizedProfit"))
        if wallet > 0:
            return wallet + unrealized
        return wallet

    def _ws_margin_balance_estimate(self) -> Optional[float]:
        """Wallet + unrealized PnL from user stream when REST balance is unavailable."""
        if not self._market_data or not self._market_data.user_stream_has_account_data():
            return None
        quote = Config.QUOTE_ASSET
        wallet = self._market_data.get_ws_wallet_balance(quote)
        if wallet <= 0:
            return None
        unrealized = self._market_data.get_ws_unrealized_pnl_total()
        return wallet + unrealized

    def _sync_account_cache_from_ws(self) -> None:
        if not self._market_data or not self._market_data.user_stream_has_account_data():
            return
        quote = Config.QUOTE_ASSET
        balance = self._market_data.get_ws_wallet_balance(quote)
        if balance <= 0:
            return
        assets = [
            {
                "asset": quote,
                "availableBalance": str(balance),
                "crossWalletBalance": str(balance),
            }
        ]
        self._account_rest_cache.account_info = {"assets": assets}
        self._account_rest_cache.updated_at = time.monotonic()
        self._balance_cache.set(balance)

    def _cached_futures_account_response(self) -> Optional[dict[str, Any]]:
        if self._account_rest_cache.is_valid():
            return dict(self._account_rest_cache.account_info)
        self._sync_account_cache_from_ws()
        if self._account_rest_cache.account_info:
            return dict(self._account_rest_cache.account_info)
        last_known = self._balance_cache.last_known()
        if last_known > 0:
            quote = Config.QUOTE_ASSET
            return {
                "assets": [
                    {
                        "asset": quote,
                        "availableBalance": str(last_known),
                        "crossWalletBalance": str(last_known),
                    }
                ]
            }
        return None

    def _cached_futures_position_information(self) -> list[dict[str, Any]]:
        if self._market_data and self._market_data.user_stream_has_account_data():
            raw: list[dict[str, Any]] = []
            for pos in self._market_data.get_ws_positions():
                raw.append(
                    {
                        "symbol": pos.get("symbol"),
                        "positionSide": pos.get("positionSide"),
                        "positionAmt": str(safe_float(pos.get("quantity"))),
                        "entryPrice": str(safe_float(pos.get("entry_price"))),
                        "unRealizedProfit": str(safe_float(pos.get("unrealized_pnl"))),
                    }
                )
            return raw
        raw_cached: list[dict[str, Any]] = []
        for pos in self._position_cache.positions:
            raw_cached.append(
                {
                    "symbol": pos.get("symbol"),
                    "positionSide": pos.get("positionSide"),
                    "positionAmt": str(safe_float(pos.get("quantity"))),
                    "entryPrice": str(safe_float(pos.get("entry_price"))),
                    "unRealizedProfit": str(safe_float(pos.get("unrealized_pnl"))),
                }
            )
        return raw_cached

    @staticmethod
    def _is_kline_rest_call(func: Any) -> bool:
        return getattr(func, "__name__", "") == "futures_klines"

    def is_kline_bootstrap_halted(self) -> bool:
        return self._kline_bootstrap_halted

    def halt_kline_bootstrap(self, reason: str) -> None:
        self._kline_bootstrap_halted = True
        if self._rest_block_log.should_log(f"kline_bootstrap_halt:{reason[:40]}"):
            system_logger.warning(
                "Kline bootstrap REST circuit breaker tripped — %s. "
                "Waiting for live WebSocket candles.",
                reason,
            )

    def can_bootstrap_klines_rest(self) -> bool:
        """True when bootstrap may issue another futures_klines REST call."""
        if self._kline_bootstrap_halted:
            return False
        if self._ws_reconnect_or_warmup():
            return False
        if self._market_data and self._market_data.is_rest_blocked()[0]:
            return False
        if self._rest_token_bucket.is_hard_stopped():
            return False
        if self._rest_usage.in_safety_mode() or not self._rest_usage.allows_background_rest():
            return False
        if not Config.ENABLE_STRICT_RATE_LIMIT:
            return True
        reserve = max(Config.REST_BUDGET_MIN_REMAINING_FRACTION, 0.0)
        return self._rest_budget.remaining_fraction() >= reserve

    def _enforce_kline_rest_pace(self) -> None:
        """Minimum gap between consecutive futures_klines REST calls."""
        min_gap = max(
            Config.KLINE_REST_MIN_INTERVAL_SECONDS,
            Config.KLINE_BOOTSTRAP_INTER_REQUEST_DELAY_SECONDS,
            0.3,
        )
        with self._kline_rest_lock:
            elapsed = time.monotonic() - self._last_kline_rest_at
            if elapsed < min_gap:
                time.sleep(min_gap - elapsed)
            self._last_kline_rest_at = time.monotonic()

    @staticmethod
    def _default_kline_request_delay() -> float:
        return max(
            Config.KLINE_REST_MIN_INTERVAL_SECONDS,
            Config.WS_KLINE_BOOTSTRAP_REST_DELAY_SECONDS,
            Config.KLINE_BOOTSTRAP_INTER_REQUEST_DELAY_SECONDS,
            0.2,
        )

    def _account_endpoint_gate_reason(
        self,
        func: Any,
        *,
        execution_priority: bool,
        bypass_account_cache: bool = False,
    ) -> str:
        name = getattr(func, "__name__", "")
        if name not in ACCOUNT_REST_ENDPOINTS:
            return ""
        if bypass_account_cache and execution_priority:
            blocked, reason = self.is_rest_blocked()
            if blocked:
                return reason
            return ""
        blocked, reason = self.is_rest_blocked()
        if blocked:
            return reason
        if name in ("futures_account", "futures_account_balance"):
            if not execution_priority and not self._degraded_rest_allowed(
                "account", consume=False
            ):
                return "ws_degraded_rest_interval"
            if not execution_priority and not self._account_rest_interval_elapsed():
                return "account_rest_interval"
            if Config.ENABLE_STRICT_RATE_LIMIT:
                reserve = max(Config.REST_BUDGET_ACCOUNT_RESERVE_FRACTION, 0.0)
                if self._rest_budget.remaining_fraction() < reserve:
                    return "rest_budget_reserve"
        elif name == "futures_position_information":
            if not Config.ENABLE_REST_POSITION_POLL:
                return "rest_position_poll_disabled"
            if not execution_priority and not self._degraded_rest_allowed(
                "position", consume=False
            ):
                return "ws_degraded_rest_interval"
            if not execution_priority and not self._account_rest_interval_elapsed():
                return "account_rest_interval"
            if Config.ENABLE_STRICT_RATE_LIMIT:
                reserve = max(Config.REST_BUDGET_ACCOUNT_RESERVE_FRACTION, 0.0)
                if self._rest_budget.remaining_fraction() < reserve:
                    return "rest_budget_reserve"
        return ""

    def _return_cached_account_call(self, func: Any) -> Any:
        name = getattr(func, "__name__", "")
        if name == "futures_account_balance":
            cached = self._cached_futures_account_response()
            if cached and isinstance(cached.get("assets"), list):
                return list(cached["assets"])
            return []
        if name == "futures_account":
            cached = self._cached_futures_account_response()
            if cached is not None:
                return cached
            return {"assets": []}
        if name == "futures_position_information":
            return self._cached_futures_position_information()
        return None

    def _store_account_rest_response(self, account_info: dict[str, Any]) -> None:
        self._account_rest_cache.account_info = dict(account_info)
        self._account_rest_cache.updated_at = time.monotonic()
        self._last_account_rest_at = time.monotonic()
        self._last_balance_rest_at = self._last_account_rest_at

    def _throttled_call(
        self,
        func: Any,
        *args: Any,
        allow_during_scan: bool = False,
        execution_priority: bool = False,
        bypass_account_cache: bool = False,
        **kwargs: Any,
    ) -> Any:
        priority = (
            execution_priority or allow_during_scan or self._is_execution_priority()
        )

        account_gate = self._account_endpoint_gate_reason(
            func,
            execution_priority=priority,
            bypass_account_cache=bypass_account_cache,
        )
        if account_gate and self._is_account_rest_call(func):
            if bypass_account_cache and priority:
                if self._rest_block_log.should_log(f"account_rest_required:{account_gate}"):
                    system_logger.warning(
                        "%s required but blocked (%s).",
                        getattr(func, "__name__", "account"),
                        account_gate,
                    )
                raise ExchangeRateLimitError(account_gate)
            cached = self._return_cached_account_call(func)
            if self._rest_block_log.should_log(f"account_cache:{account_gate}"):
                system_logger.debug(
                    "%s blocked (%s) — returning cached account/position state.",
                    getattr(func, "__name__", "account"),
                    account_gate,
                )
            return cached

        is_bootstrap_kline = (
            self._is_kline_rest_call(func) and self._is_bootstrap_priority()
        )
        if is_bootstrap_kline:
            if not self.can_bootstrap_klines_rest():
                self.halt_kline_bootstrap("budget_or_ban_gate")
                return []

        blocked, reason = self._rest_block_applies(priority)
        if blocked:
            if self._is_account_rest_call(func):
                if bypass_account_cache and priority:
                    raise ExchangeRateLimitError(reason)
                return self._return_cached_account_call(func)
            if is_bootstrap_kline:
                self.halt_kline_bootstrap(reason)
                return []
            if self._market_data:
                remaining = self._market_data.get_rest_block_remaining_seconds()
                if remaining > 0:
                    self._rest_token_bucket.trigger_hard_stop(float(remaining))
            if self._rest_block_log.should_log(reason):
                error_logger.warning("REST call blocked (ban active): %s", reason)
            raise ExchangeRateLimitError(reason)

        if self._rest_token_bucket.is_hard_stopped():
            if self._is_account_rest_call(func):
                if bypass_account_cache and priority:
                    remaining = self._rest_token_bucket.hard_stop_remaining()
                    raise ExchangeRateLimitError(
                        f"REST hard-stopped (~{int(remaining)}s remaining)"
                    )
                return self._return_cached_account_call(func)
            if is_bootstrap_kline:
                self.halt_kline_bootstrap("rest_hard_stop")
                return []
            remaining = self._rest_token_bucket.hard_stop_remaining()
            raise ExchangeRateLimitError(
                f"REST hard-stopped due to rate limit (~{int(remaining)}s remaining)"
            )

        if self._scan_mode and self._scan_ws_only() and not priority:
            if self._is_account_rest_call(func):
                return self._return_cached_account_call(func)
            if is_bootstrap_kline:
                return []
            raise ExchangeError(
                "REST API call rejected during scan cycle — use WebSocket cache."
            )

        call_weight = weight_for_call(func, **kwargs)
        lane = self._resolve_rest_lane(execution_priority=priority)

        if self._rest_usage.in_safety_mode() and lane != RestLane.EXECUTION:
            if self._is_account_rest_call(func):
                if bypass_account_cache and priority:
                    raise ExchangeRateLimitError("API SAFETY MODE — REST halted")
                return self._return_cached_account_call(func)
            if is_bootstrap_kline:
                self.halt_kline_bootstrap("api_safety_mode")
                return []
            raise ExchangeRateLimitError("API SAFETY MODE — non-execution REST halted")

        if Config.ENABLE_STRICT_RATE_LIMIT:
            if lane != RestLane.EXECUTION:
                if not self._rest_budget.acquire(call_weight, lane):
                    if self._is_account_rest_call(func):
                        if bypass_account_cache and priority:
                            raise ExchangeRateLimitError(
                                "REST budget below reserve threshold"
                            )
                        return self._return_cached_account_call(func)
                    if is_bootstrap_kline:
                        self.halt_kline_bootstrap("rest_budget_reserve")
                        return []
                    if self._rest_block_log.should_log("rest_budget_reserve"):
                        error_logger.warning(
                            "REST call skipped — budget below %.0f%% reserve "
                            "(used=%s/%s weight, endpoint=%s).",
                            Config.REST_BUDGET_MIN_REMAINING_FRACTION * 100,
                            self._rest_budget.current_window_weight(),
                            self._rest_budget.max_weight_per_minute,
                            getattr(func, "__name__", "unknown"),
                        )
                    raise ExchangeRateLimitError(
                        "REST budget below reserve threshold"
                    )
            self._rest_token_bucket.acquire(call_weight)

        limiter = self._execution_rate_limiter if priority else self._rate_limiter
        func_name = getattr(func, "__name__", "")
        is_order_submit = func_name == "futures_create_order"
        network_retries = 1 if is_order_submit else max(Config.REST_NETWORK_MAX_RETRIES, 1)
        for attempt in range(1, network_retries + 1):
            if Config.ENABLE_STRICT_RATE_LIMIT:
                limiter.wait()
            if is_bootstrap_kline:
                self._enforce_kline_rest_pace()
            try:
                result = func(*args, **kwargs)
                if getattr(func, "__name__", "") == "futures_account":
                    parsed = self._parse_futures_account_payload(
                        result, Config.QUOTE_ASSET
                    )
                    if parsed:
                        self._store_account_rest_response(parsed)
                        result = parsed
                elif getattr(func, "__name__", "") == "futures_position_information":
                    self._last_account_rest_at = time.monotonic()
                return result
            except BinanceAPIException as exc:
                if exc.code == -2022 or "reduceonly order is rejected" in str(
                    exc.message
                ).lower():
                    raise PositionAlreadyClosedError(
                        str(exc.message), code=int(exc.code)
                    ) from exc
                if self._is_rate_limit_error(exc):
                    self._apply_rate_limit_halt(exc)
                    if self._is_account_rest_call(func):
                        return self._return_cached_account_call(func)
                    if is_bootstrap_kline:
                        self.halt_kline_bootstrap(str(exc.message))
                        return []
                    raise ExchangeRateLimitError(str(exc.message)) from exc
                if self._critical_alerts and exc.code in (-1021, -2015, -2014):
                    self._critical_alerts.notify(
                        "API_DISCONNECT",
                        f"Binance API error code {exc.code}: {exc.message}",
                        exc=exc,
                    )
                raise ExchangeError(str(exc.message)) from exc
            except (ConnectionError, TimeoutError, OSError) as exc:
                if attempt >= network_retries:
                    if self._critical_alerts:
                        self._critical_alerts.notify(
                            "API_DISCONNECT",
                            "Binance API connection failed after retries",
                            exc=exc,
                        )
                    raise ExchangeError(str(exc)) from exc
                sleep_seconds = self._backoff_seconds(attempt)
                error_logger.warning(
                    "Network error on API call (attempt %s/%s): %s — retry in %.1fs",
                    attempt,
                    network_retries,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
            except Exception as exc:
                raise ExchangeError(str(exc)) from exc
        raise ExchangeError("REST call failed")

    def _parse_symbol_rules(self, symbol_data: dict[str, Any]) -> SymbolRules:
        filters = {item["filterType"]: item for item in symbol_data.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        market_lot = filters.get("MARKET_LOT_SIZE", lot_filter)
        notional_filter = filters.get("MIN_NOTIONAL", {})

        return SymbolRules(
            price_precision=int(symbol_data.get("pricePrecision", 2)),
            quantity_precision=int(symbol_data.get("quantityPrecision", 3)),
            tick_size=safe_float(price_filter.get("tickSize"), 0.01),
            step_size=safe_float(lot_filter.get("stepSize"), 0.001),
            min_qty=max(
                safe_float(lot_filter.get("minQty"), 0.001),
                safe_float(market_lot.get("minQty"), 0.001),
            ),
            min_notional=safe_float(notional_filter.get("notional"), 5.0),
        )

    # ---------------- Account setup ----------------

    def _configure_account(self) -> None:
        """Ensure hedge (dual-side) position mode is enabled for LONG/SHORT orders."""
        try:
            position_mode = self._throttled_call(
                self.client.futures_get_position_mode,
                **self.recv_window_param,
            )
            if not position_mode.get("dualSidePosition", False):
                self._throttled_call(
                    self.client.futures_change_position_mode,
                    dualSidePosition="true",
                    **self.recv_window_param,
                )
                system_logger.info("Hedge mode enabled on Binance Futures account.")
            else:
                system_logger.info("Hedge mode already active.")
        except BinanceAPIException as exc:
            # -4046: No need to change position side (already in requested mode)
            if exc.code != -4046:
                error_logger.error("Failed to configure hedge mode: %s", exc.message)

    def refresh_symbol_rules(self) -> None:
        for attempt in range(1, 4):
            try:
                exchange_info = self._throttled_call(self.client.futures_exchange_info)
                temp_cache: dict[str, SymbolRules] = {}
                for symbol_data in exchange_info.get("symbols", []):
                    symbol_name = symbol_data.get("symbol")
                    if not symbol_name:
                        continue
                    temp_cache[symbol_name] = self._parse_symbol_rules(symbol_data)

                with self._rules_lock:
                    self._symbol_rules_cache = temp_cache

                system_logger.info(
                    "Cached trading rules for %s symbols.", len(temp_cache)
                )
                return
            except Exception as exc:
                error_logger.warning(
                    "Symbol rules refresh failed (attempt %s/3): %s", attempt, exc
                )
                time.sleep(2)

        error_logger.error("Critical: symbol rules cache could not be initialized.")

    # ---------------- Public symbol rule accessors ----------------

    def get_symbol_precision(self, symbol: str) -> dict[str, Any]:
        """
        Thread-safe getter used by manager/executor.
        Returns precision dict with safe fallbacks if cache miss occurs.
        """
        rules = self.get_symbol_rules(symbol)
        return rules.to_dict()

    def get_symbol_rules(self, symbol: str) -> SymbolRules:
        with self._rules_lock:
            cached = self._symbol_rules_cache.get(symbol)
            if cached is not None:
                return cached

        error_logger.warning(
            "Symbol rules cache miss for %s — using conservative defaults.", symbol
        )
        return self.DEFAULT_RULES

    def format_quantity(self, symbol: str, quantity: float) -> float:
        """Format quantity to Binance LOT_SIZE step / precision for symbol."""
        rules = self.get_symbol_rules(symbol)
        return amount_to_precision(
            quantity, rules.step_size, rules.quantity_precision
        )

    # ---------------- Balance ----------------

    def invalidate_balance_cache(self) -> None:
        self._balance_cache.invalidate()

    def refresh_wallet_after_trade(self) -> float:
        """Refresh wallet after a fill/close — WS first, then background REST only."""
        ws_balance = self._hydrate_balance_from_ws()
        if ws_balance is not None and ws_balance > 0:
            return ws_balance
        if not self.background_account_rest_allowed():
            return self._fallback_quote_balance()
        self._account_rest_cache.updated_at = 0.0
        self._last_account_rest_at = 0.0
        account_info, _ = self._fetch_futures_account_rest(
            force_live=False,
            max_attempts=1,
        )
        if account_info:
            wallet = self._extract_quote_balance(account_info, Config.QUOTE_ASSET)
            if wallet <= 0:
                wallet = safe_float(account_info.get("totalWalletBalance"))
            if wallet > 0:
                self._balance_cache.set(wallet)
                return wallet
        return self.get_futures_balance(force_refresh=False)

    def invalidate_position_cache(self) -> None:
        self._position_cache.updated_at = 0.0

    def clear_position_cache(self, symbol: str, position_side: str) -> None:
        """Drop a closed position from WS and local caches immediately."""
        symbol = symbol.upper()
        position_side = position_side.upper()
        if self._market_data:
            self._market_data.clear_position(symbol, position_side)
        remaining: list[dict[str, Any]] = []
        unrealized_total = 0.0
        for pos in self._position_cache.positions:
            if pos.get("symbol") == symbol and pos.get("positionSide") == position_side:
                continue
            remaining.append(pos)
            unrealized_total += safe_float(pos.get("unrealized_pnl"))
        self._position_cache.positions = remaining
        self._position_cache.unrealized_pnl_total = unrealized_total
        self._position_cache.updated_at = time.monotonic()

    def _parse_open_positions(self, raw_positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        open_positions: list[dict[str, Any]] = []
        unrealized_total = 0.0
        for pos in raw_positions:
            quantity = abs(safe_float(pos.get("positionAmt")))
            if quantity <= 0:
                continue
            unrealized = safe_float(pos.get("unRealizedProfit"))
            unrealized_total += unrealized
            open_positions.append(
                {
                    "symbol": str(pos.get("symbol", "")),
                    "positionSide": str(pos.get("positionSide", "")),
                    "quantity": quantity,
                    "entry_price": safe_float(pos.get("entryPrice")),
                    "unrealized_pnl": unrealized,
                }
            )
        self._position_cache.positions = open_positions
        self._position_cache.unrealized_pnl_total = unrealized_total
        self._position_cache.updated_at = time.monotonic()
        return open_positions

    def _merge_ws_positions_into_cache(
        self, ws_positions: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Merge WS snapshot into local cache without dropping seeded REST/cache rows."""
        if not ws_positions:
            return list(self._position_cache.positions)

        by_key: dict[tuple[str, str], dict[str, Any]] = {
            (
                str(p.get("symbol", "")).upper(),
                str(p.get("positionSide", "")).upper(),
            ): dict(p)
            for p in self._position_cache.positions
        }
        for pos in ws_positions:
            key = (
                str(pos.get("symbol", "")).upper(),
                str(pos.get("positionSide", "")).upper(),
            )
            by_key[key] = pos
        merged = list(by_key.values())
        unrealized_total = sum(safe_float(p.get("unrealized_pnl")) for p in merged)
        self._position_cache.positions = merged
        self._position_cache.unrealized_pnl_total = unrealized_total
        self._position_cache.updated_at = time.monotonic()
        return merged

    def _refresh_positions_cache(self, force: bool = False) -> list[dict[str, Any]]:
        """Prefer user-data WebSocket; REST refresh throttled to ACCOUNT_REST_MIN_INTERVAL."""
        now = time.monotonic()

        if self._market_data and self._market_data.user_stream_has_account_data():
            ws_positions = self._market_data.get_ws_positions()
            if ws_positions:
                self._position_cache.unrealized_pnl_total = (
                    self._market_data.get_ws_unrealized_pnl_total()
                )
                return self._merge_ws_positions_into_cache(ws_positions)
            if self._position_cache.positions:
                return list(self._position_cache.positions)
            if not self._market_data.user_stream_is_stale():
                return []

        if not force and self._position_cache.is_valid():
            return self._position_cache.positions

        if now < self._position_backoff_until:
            return self._position_cache.positions

        if (
            not force
            and self._last_account_rest_at > 0
            and (now - self._last_account_rest_at)
            < max(float(Config.ACCOUNT_REST_MIN_INTERVAL_SECONDS), 60.0)
        ):
            return self._position_cache.positions

        if not self._degraded_rest_allowed("position"):
            return self._position_cache.positions

        if not Config.ENABLE_REST_POSITION_POLL or not self._rest_reads_allowed():
            return self._position_cache.positions

        if self.rest_account_reads_blocked():
            return self._position_cache.positions

        with self._position_refresh_lock:
            if not force and self._position_cache.is_valid():
                return self._position_cache.positions

            try:
                raw = self._throttled_call(self.client.futures_position_information)
                return self._parse_open_positions(raw or [])
            except ExchangeRateLimitError as exc:
                self._position_backoff_until = now + Config.POSITION_CACHE_BACKOFF_SECONDS
                if self._rest_block_log.should_log("position_refresh_blocked"):
                    error_logger.warning(
                        "Position REST refresh blocked — using WS/stale cache: %s",
                        exc,
                    )
                return self._position_cache.positions
            except Exception as exc:
                error_logger.error("Failed to refresh position cache: %s", exc)
                return self._position_cache.positions

    def ensure_positions_cached(self, force: bool = False) -> None:
        """Warm position cache once per monitor/risk cycle."""
        self._refresh_positions_cache(force=force)

    def fetch_derivatives_context(self, symbol: str) -> dict[str, Any]:
        """
        Cached funding rate + open interest for institutional context modules.
        Non-blocking when cache is warm; skips REST during scan WS-only mode.
        """
        sym = str(symbol).upper()
        with self._derivatives_lock:
            cached = self._derivatives_cache.get(sym)
            if cached and cached.is_valid():
                return cached.as_dict()

        if self.in_scan_mode and Config.SCAN_WS_ONLY:
            if cached:
                return cached.as_dict()
            return {}

        if self._ws_reconnect_or_warmup() or not self.can_make_background_rest_call(3):
            if cached:
                return cached.as_dict()
            return {}

        if self._market_data and self._market_data.is_rest_blocked()[0]:
            if cached:
                return cached.as_dict()
            return {}

        try:
            premium = self._throttled_call(
                self.client.futures_mark_price,
                symbol=sym,
                **self.recv_window_param,
            )
            funding = safe_float(premium.get("lastFundingRate"))
            mark_price = safe_float(premium.get("markPrice"))

            oi_raw = self._throttled_call(
                self.client.futures_open_interest,
                symbol=sym,
                **self.recv_window_param,
            )
            oi = safe_float(oi_raw.get("openInterest"))

            oi_change_pct = 0.0
            try:
                hist = self._throttled_call(
                    self.client.futures_open_interest_hist,
                    symbol=sym,
                    period="5m",
                    limit=2,
                    **self.recv_window_param,
                )
                if isinstance(hist, list) and len(hist) >= 2:
                    prev_oi = safe_float(hist[-2].get("sumOpenInterest"))
                    last_oi = safe_float(hist[-1].get("sumOpenInterest"))
                    if prev_oi > 0:
                        oi_change_pct = (last_oi - prev_oi) / prev_oi * 100.0
                    if oi <= 0:
                        oi = last_oi
            except Exception as exc:
                error_logger.debug(
                    "Open-interest history unavailable for %s: %s", sym, exc
                )

            entry = DerivativesCacheEntry(
                funding_rate=funding,
                open_interest=oi,
                oi_change_pct=oi_change_pct,
                mark_price=mark_price,
                updated_at=time.monotonic(),
            )
            with self._derivatives_lock:
                self._derivatives_cache[sym] = entry
            return entry.as_dict()
        except ExchangeRateLimitError:
            if cached:
                return cached.as_dict()
            return {}
        except Exception as exc:
            if self._rest_block_log.should_log(f"derivatives_{sym}"):
                error_logger.debug("Derivatives context fetch failed for %s: %s", sym, exc)
            if cached:
                return cached.as_dict()
            return {}

    def fetch_futures_ticker_map_rest(self) -> dict[str, dict[str, Any]]:
        """
        Single futures_ticker() REST call to warm the cache (weight=40 all-symbols).
        Skipped entirely during IP ban, hard-stop, weight governor, or low reserve.
        """
        if self._market_data and self._market_data.is_rest_blocked()[0]:
            return {}
        if self.in_scan_mode and Config.SCAN_WS_ONLY:
            return {}
        if not self.can_make_background_rest_call(
            weight_for_call(self.client.futures_ticker)
        ):
            return {}

        try:
            tickers = self._throttled_call(
                self.client.futures_ticker,
                **self.recv_window_param,
            )
            return {
                str(row["symbol"]): row for row in tickers if row.get("symbol")
            }
        except ExchangeRateLimitError:
            return {}
        except Exception as exc:
            if self._rest_block_log.should_log("ticker_rest_fallback"):
                error_logger.warning("Ticker REST fallback failed: %s", exc)
            return {}

    def fetch_startup_ticker_map(self) -> dict[str, dict[str, Any]]:
        """Alias for startup seeding — same single lightweight REST request."""
        return self.fetch_futures_ticker_map_rest()

    def fetch_startup_balance(self) -> float:
        """
        Startup-only balance lookup — single REST futures_account when WS empty.
        """
        quote = Config.QUOTE_ASSET

        ws_balance = self._hydrate_balance_from_ws()
        if ws_balance is not None and ws_balance > 0:
            return ws_balance

        if self._balance_cache.is_valid():
            return self._balance_cache.value

        allowed, reason = self._rest_gate_open()
        if not allowed:
            system_logger.debug("Startup balance deferred: %s", reason)
            return self._balance_cache.value

        if self.rest_account_reads_blocked():
            cached = self._cached_futures_account_response()
            if cached:
                balance = self._extract_quote_balance(cached, quote)
                if balance >= 0:
                    self._balance_cache.set(balance)
                    return balance
            return self._balance_cache.value

        try:
            raw = self._throttled_call(
                self.client.futures_account,
                **self.recv_window_param,
            )
            account_info = self._parse_futures_account_payload(raw, quote)
            if account_info:
                balance = self._extract_quote_balance(account_info, quote)
                if balance >= 0:
                    self._balance_cache.set(balance)
                    return balance
        except ExchangeRateLimitError:
            return self._fallback_quote_balance()
        except Exception as exc:
            error_logger.warning("Startup balance fetch failed: %s", exc)

        return self._fallback_quote_balance()

    def fetch_account_balance(self, force_refresh: bool = False) -> float:
        """
        Return USDT margin balance from Binance Futures account endpoints.
        Safe against dict/list/JSON-string responses; falls back to cache/WS on failure.
        """
        quote = Config.QUOTE_ASSET

        ws_balance = self._hydrate_balance_from_ws()
        if ws_balance is not None and ws_balance > 0:
            margin_est = self._ws_margin_balance_estimate()
            return margin_est if margin_est is not None and margin_est > 0 else ws_balance

        try:
            account_info, _ = self._fetch_futures_account_rest(
                force_live=force_refresh,
                max_attempts=max(Config.STARTUP_BALANCE_MAX_ATTEMPTS, 2),
            )
            if account_info:
                margin = self._extract_margin_balance(account_info, quote)
                if margin > 0:
                    self._balance_cache.set(margin)
                    return margin
                wallet = self._extract_quote_balance(account_info, quote)
                if wallet > 0:
                    self._balance_cache.set(wallet)
                    return wallet
        except ExchangeRateLimitError:
            return self._fallback_quote_balance()
        except Exception as exc:
            if self._rest_block_log.should_log("fetch_account_balance"):
                error_logger.warning("fetch_account_balance failed: %s", exc)

        fallback = self._fallback_quote_balance()
        if fallback > 0:
            return fallback
        return self.get_futures_balance(force_refresh=force_refresh)

    def get_futures_balance(self, force_refresh: bool = False) -> float:
        """
        Return available USDT balance.
        Primary: user-data WebSocket wallet updates.
        REST futures_account(): startup + at most once per ACCOUNT_REST_MIN_INTERVAL_SECONDS.
        """
        quote = Config.QUOTE_ASSET

        ws_balance = self._hydrate_balance_from_ws()
        if ws_balance is not None and ws_balance > 0:
            return ws_balance

        if force_refresh and not self._is_execution_priority():
            force_refresh = False

        if not force_refresh and self._balance_cache.is_valid():
            cached = self._balance_cache.value
            if cached > 0:
                return cached

        if self.is_rest_blocked()[0] and not self._is_execution_priority():
            return self._fallback_quote_balance()

        now = time.monotonic()
        poll_interval = max(float(Config.ACCOUNT_REST_MIN_INTERVAL_SECONDS), 180.0)
        last_known = self._balance_cache.last_known()
        needs_rest = force_refresh or last_known <= 0

        if last_known <= 0:
            if now < self._balance_rest_backoff_until:
                return 0.0
            if self._cold_start_balance_rest_done or not self.background_account_rest_allowed():
                self._balance_rest_backoff_until = max(
                    self._balance_rest_backoff_until, now + min(poll_interval, 60.0)
                )
                return 0.0

        if (
            not needs_rest
            and self._last_account_rest_at > 0
            and (now - self._last_account_rest_at) < poll_interval
        ):
            return self._fallback_quote_balance()

        if now < self._balance_rest_backoff_until and not needs_rest:
            return self._fallback_quote_balance()

        if not Config.ENABLE_REST_BALANCE_POLL and not needs_rest:
            return self._fallback_quote_balance()

        if not self._rest_reads_allowed() and not self._is_execution_priority():
            if last_known <= 0:
                self._cold_start_balance_rest_done = True
                self._balance_rest_backoff_until = now + min(poll_interval, 60.0)
            return self._fallback_quote_balance()

        if self.rest_account_reads_blocked() and not self._is_execution_priority():
            cached = self._cached_futures_account_response()
            if cached:
                balance = self._extract_quote_balance(cached, quote)
                if balance > 0:
                    self._balance_cache.set(balance)
                    return balance
            return self._fallback_quote_balance()

        try:
            if last_known <= 0:
                self._cold_start_balance_rest_done = True
            account_info, _ = self._fetch_futures_account_rest(
                force_live=False,
                max_attempts=1,
            )
            if account_info:
                balance = self._extract_quote_balance(account_info, quote)
                if balance <= 0 and isinstance(account_info, dict):
                    balance = safe_float(account_info.get("totalWalletBalance"))
                if balance > 0:
                    return balance
            if last_known <= 0:
                self._balance_rest_backoff_until = now + min(poll_interval, 60.0)
            return self._fallback_quote_balance()
        except ExchangeRateLimitError:
            self._balance_rest_backoff_until = now + poll_interval
            if self._rest_block_log.should_log("balance_rate_limited"):
                error_logger.warning(
                    "Balance REST skipped after rate limit — using last-known wallet."
                )
            cached = self._cached_futures_account_response()
            if cached:
                balance = self._extract_quote_balance(cached, quote)
                if balance > 0:
                    return balance
            return self._fallback_quote_balance()
        except Exception as exc:
            self._balance_rest_backoff_until = now + min(poll_interval, 120.0)
            if self._is_rate_limit_error_text(exc):
                if self._rest_block_log.should_log("balance_rate_limited"):
                    error_logger.warning(
                        "Balance REST hit -1003/rate limit — using last-known wallet: %s",
                        exc,
                    )
                return self._fallback_quote_balance()
            if self._rest_block_log.should_log("balance_fetch_failed"):
                error_logger.warning("Balance REST fetch failed (cached fallback): %s", exc)
            return self._fallback_quote_balance()

    @staticmethod
    def _normalize_balance_rows(data: Any) -> list[dict[str, Any]]:
        """Accept fapi/v2/balance list or assets[] from a cached account dict."""
        payload = BinanceExchangeManager._coerce_api_payload(data)
        if payload is None:
            return []
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            assets = payload.get("assets")
            if isinstance(assets, list):
                return [row for row in assets if isinstance(row, dict)]
        return []

    @staticmethod
    def _account_info_from_asset_balances(
        rows: Any,
        quote: str,
    ) -> dict[str, Any]:
        """Build a futures_account-like payload from fapi/v2/balance rows."""
        quote = quote.upper()
        wallet = 0.0
        available = 0.0
        unrealized = 0.0
        assets: list[dict[str, Any]] = []
        for row in BinanceExchangeManager._normalize_balance_rows(rows):
            if str(row.get("asset", "")).upper() != quote:
                continue
            wallet = safe_float(row.get("balance"))
            if wallet <= 0:
                wallet = safe_float(row.get("crossWalletBalance"))
            available = safe_float(row.get("availableBalance"))
            unrealized = safe_float(row.get("crossUnPnl"))
            assets.append(dict(row))
        margin = wallet + unrealized if wallet > 0 else 0.0
        return {
            "totalWalletBalance": str(wallet),
            "totalMarginBalance": str(margin),
            "totalCrossWalletBalance": str(wallet),
            "totalUnrealizedProfit": str(unrealized),
            "availableBalance": str(available),
            "assets": assets,
        }

    def _apply_account_info_to_snapshot(
        self,
        snapshot: LiveAccountSnapshot,
        account_info: dict[str, Any],
    ) -> None:
        quote = Config.QUOTE_ASSET
        if not isinstance(account_info, dict):
            parsed = self._parse_futures_account_payload(account_info, quote)
            if not parsed:
                return
            account_info = parsed

        snapshot.wallet_balance = safe_float(account_info.get("totalWalletBalance"))
        if snapshot.wallet_balance <= 0:
            snapshot.wallet_balance = self._extract_quote_balance(account_info, quote)
        snapshot.margin_balance = safe_float(account_info.get("totalMarginBalance"))
        if snapshot.margin_balance <= 0:
            snapshot.margin_balance = safe_float(
                account_info.get("totalCrossWalletBalance")
            )
        if snapshot.margin_balance <= 0 and snapshot.wallet_balance > 0:
            snapshot.margin_balance = snapshot.wallet_balance + safe_float(
                account_info.get("totalUnrealizedProfit")
            )
        snapshot.unrealized_pnl = safe_float(
            account_info.get("totalUnrealizedProfit")
        )
        if snapshot.unrealized_pnl == 0 and snapshot.margin_balance > snapshot.wallet_balance:
            snapshot.unrealized_pnl = snapshot.margin_balance - snapshot.wallet_balance
        snapshot.available_balance = safe_float(account_info.get("availableBalance"))
        if snapshot.available_balance <= 0:
            snapshot.available_balance = self._extract_quote_balance(
                account_info, quote
            )

    def _fetch_futures_account_rest(
        self,
        *,
        force_live: bool = False,
        max_attempts: Optional[int] = None,
    ) -> tuple[Optional[dict[str, Any]], str]:
        """
        Fetch USDT-M account state via fapi/v2/account with retries.
        Falls back to fapi/v2/balance, then WS/cache.
        """
        attempts = max(max_attempts or Config.STARTUP_BALANCE_MAX_ATTEMPTS, 1)
        if self._ws_reconnect_or_warmup():
            attempts = 1
        delay = max(Config.STARTUP_BALANCE_RETRY_SECONDS, 0.5)
        quote = Config.QUOTE_ASSET
        last_exc: Optional[Exception] = None

        for attempt in range(1, attempts + 1):
            if not self.background_account_rest_allowed():
                break
            self._degraded_rest_allowed("account")
            try:
                raw = self._throttled_call(
                    self.client.futures_account,
                    execution_priority=False,
                    bypass_account_cache=False,
                    **self.recv_window_param,
                )
                account_info = self._parse_futures_account_payload(raw, quote)
                if account_info:
                    self._store_account_rest_response(account_info)
                    wallet = safe_float(account_info.get("totalWalletBalance"))
                    if wallet <= 0:
                        wallet = self._extract_quote_balance(account_info, quote)
                    if wallet > 0:
                        self._balance_cache.set(wallet)
                    return account_info, "REST (live)"
                if raw is not None and not isinstance(raw, (dict, list)):
                    last_exc = TypeError(
                        f"futures_account returned unsupported {type(raw).__name__}"
                    )
            except ExchangeRateLimitError as exc:
                last_exc = exc
                break
            except Exception as exc:
                last_exc = exc
                if self._is_rate_limit_error_text(exc):
                    break
                if self._rest_block_log.should_log("account_fetch_retry"):
                    error_logger.warning(
                        "futures_account fetch attempt %s/%s failed: %s",
                        attempt,
                        attempts,
                        exc,
                    )
            if attempt < attempts:
                time.sleep(delay * attempt)

        rate_limited = isinstance(last_exc, ExchangeRateLimitError) or (
            last_exc is not None and self._is_rate_limit_error_text(last_exc)
        )
        if not rate_limited and self.background_account_rest_allowed():
            try:
                raw = self._throttled_call(
                    self.client.futures_account_balance,
                    execution_priority=False,
                    bypass_account_cache=False,
                    **self.recv_window_param,
                )
                account_info = self._parse_futures_account_payload(raw, quote)
                if account_info:
                    self._store_account_rest_response(account_info)
                    wallet = safe_float(account_info.get("totalWalletBalance"))
                    if wallet <= 0:
                        wallet = self._extract_quote_balance(account_info, quote)
                    if wallet > 0:
                        self._balance_cache.set(wallet)
                    return account_info, "REST balance endpoint"
            except Exception as exc:
                last_exc = exc
                if self._rest_block_log.should_log("account_balance_fallback"):
                    error_logger.warning(
                        "futures_account_balance fallback failed: %s", exc
                    )

        cached = self._cached_futures_account_response()
        if cached:
            return cached, "WS/cache"

        if last_exc and self._rest_block_log.should_log("account_fetch_exhausted"):
            error_logger.warning("All account REST fetch attempts failed: %s", last_exc)
        return None, "unavailable"

    def fetch_live_account_snapshot(
        self,
        *,
        include_today_income: bool = True,
        force_refresh: bool = False,
    ) -> LiveAccountSnapshot:
        """
        Wallet/margin/unrealized for Telegram /status and daily PnL.
        Uses WS/cache first. REST is a background read and never uses
        execution_priority, so the used-weight governor still applies.
        """
        snapshot = LiveAccountSnapshot()
        source = "unavailable"

        cached = self._cached_futures_account_response()
        if cached:
            self._apply_account_info_to_snapshot(snapshot, cached)
            source = "WS/cache"

        allow_rest = self.background_account_rest_allowed()
        if snapshot.wallet_balance <= 0 or (force_refresh and allow_rest):
            if allow_rest:
                account_info, rest_source = self._fetch_futures_account_rest(
                    force_live=False,
                    max_attempts=1,
                )
                if account_info:
                    self._apply_account_info_to_snapshot(snapshot, account_info)
                    source = rest_source

        if snapshot.wallet_balance <= 0:
            ws_wallet = self._hydrate_balance_from_ws()
            if ws_wallet is not None and ws_wallet > 0:
                snapshot.wallet_balance = ws_wallet
                margin_est = self._ws_margin_balance_estimate()
                snapshot.margin_balance = (
                    margin_est if margin_est is not None else ws_wallet
                )
                if margin_est is not None and margin_est >= ws_wallet:
                    snapshot.unrealized_pnl = margin_est - ws_wallet
                elif self._market_data:
                    snapshot.unrealized_pnl = (
                        self._market_data.get_ws_unrealized_pnl_total()
                    )
                source = "WS user stream"
            else:
                fallback = self._fallback_quote_balance()
                if fallback > 0:
                    snapshot.wallet_balance = fallback
                    if snapshot.margin_balance <= 0:
                        snapshot.margin_balance = fallback
                    source = "last-known cache"

        if (
            include_today_income
            and allow_rest
            and self.can_make_background_rest_call(30)
        ):
            snapshot.today_realized_pnl = self._fetch_today_realized_income_rest()

        snapshot.source = source
        return snapshot

    def _fetch_today_realized_income_rest(self) -> float:
        """Sum REALIZED_PNL income rows for the current UTC day."""
        from datetime import datetime, timezone

        try:
            now = datetime.now(timezone.utc)
            start_ms = int(
                datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp()
                * 1000
            )
            rows = self._throttled_call(
                self.client.futures_income_history,
                startTime=start_ms,
                limit=1000,
            )
            total = 0.0
            for row in rows or []:
                if str(row.get("incomeType", "")).upper() == "REALIZED_PNL":
                    total += safe_float(row.get("income"))
            return total
        except Exception as exc:
            if self._rest_block_log.should_log("today_realized_income"):
                error_logger.debug("Today realized income fetch failed: %s", exc)
            return 0.0

    @staticmethod
    def _closing_trade_side(position_side: str) -> str:
        return "SELL" if position_side.upper() == "LONG" else "BUY"

    def fetch_closed_position_realized_pnl(
        self,
        symbol: str,
        position_side: str,
        opened_at_ms: int,
        *,
        until_ms: Optional[int] = None,
    ) -> ClosedPositionPnl:
        """
        Sum realized PnL from Binance userTrades (preferred) or income history.
        Used when a DB trade is closed externally on the exchange.
        """
        symbol = symbol.upper()
        position_side = position_side.upper()
        close_side = self._closing_trade_side(position_side)
        result = ClosedPositionPnl()

        if self.is_rest_blocked()[0]:
            return result

        start_ms = max(opened_at_ms - 60_000, 0)
        end_ms = until_ms or int(time.time() * 1000)

        try:
            rows = self._throttled_call(
                self.client.futures_account_trades,
                symbol=symbol,
                startTime=start_ms,
                limit=1000,
            )
            close_qty = 0.0
            close_notional = 0.0
            for row in rows or []:
                trade_time = int(safe_float(row.get("time")))
                if trade_time < start_ms or trade_time > end_ms:
                    continue
                row_ps = str(row.get("positionSide", "BOTH")).upper()
                if row_ps not in (position_side, "BOTH"):
                    continue
                realized = safe_float(row.get("realizedPnl"))
                commission = safe_float(row.get("commission"))
                result.realized_pnl += realized
                result.commission += commission
                result.fill_count += 1
                side = str(row.get("side", "")).upper()
                if side == close_side:
                    qty = safe_float(row.get("qty"))
                    price = safe_float(row.get("price"))
                    if qty > 0 and price > 0:
                        close_qty += qty
                        close_notional += price * qty

            if result.fill_count > 0:
                result.source = "userTrades"
                if close_qty > 0:
                    result.exit_price = close_notional / close_qty
                return result
        except Exception as exc:
            if self._rest_block_log.should_log(f"user_trades_pnl:{symbol}"):
                error_logger.warning(
                    "userTrades PnL fetch failed for %s %s: %s",
                    symbol,
                    position_side,
                    exc,
                )

        try:
            rows = self._throttled_call(
                self.client.futures_income_history,
                symbol=symbol,
                startTime=start_ms,
                incomeType="REALIZED_PNL",
                limit=1000,
            )
            income_total = 0.0
            income_rows = 0
            for row in rows or []:
                trade_time = int(safe_float(row.get("time")))
                if trade_time < start_ms or trade_time > end_ms:
                    continue
                if str(row.get("incomeType", "")).upper() != "REALIZED_PNL":
                    continue
                income_total += safe_float(row.get("income"))
                income_rows += 1

            if income_rows > 0:
                result.realized_pnl = income_total
                result.fill_count = income_rows
                result.source = "income"
                return result
        except Exception as exc:
            if self._rest_block_log.should_log(f"income_pnl:{symbol}"):
                error_logger.warning(
                    "Income PnL fetch failed for %s %s: %s",
                    symbol,
                    position_side,
                    exc,
                )

        return result

    def resolve_order_fill_pnl(
        self,
        symbol: str,
        order_id: str,
        *,
        position_side: str = "",
        wait_seconds: float = 5.0,
        rest_retries: int = 3,
    ) -> ClosedPositionPnl:
        """
        Authoritative realized PnL for a single close order fill.
        WS ORDER_TRADE_UPDATE (rp) → REST userTrades by orderId (with retries).
        """
        symbol = symbol.upper()
        order_id = str(order_id or "").strip()
        result = ClosedPositionPnl(source="unknown")

        if not order_id:
            return result

        hub = self._market_data
        if hub is not None and hasattr(hub, "fill_tracker"):
            ws_fill = hub.fill_tracker.wait_for(order_id, timeout=wait_seconds)
            if ws_fill is not None:
                result.realized_pnl = ws_fill.realized_pnl
                result.commission = ws_fill.commission
                result.exit_price = ws_fill.fill_price
                result.fill_count = 1
                result.source = "ws"
                trade_logger.info(
                    "[%s] Fill PnL from WS order %s | rp=%.4f | px=%.6f",
                    symbol,
                    order_id,
                    result.realized_pnl,
                    result.exit_price,
                )
                return result

        if self.is_rest_blocked()[0]:
            return result

        delay = max(Config.STARTUP_BALANCE_RETRY_SECONDS, 0.5)
        attempts = max(rest_retries, 1)
        for attempt in range(1, attempts + 1):
            try:
                kwargs: dict[str, Any] = {"symbol": symbol, "orderId": int(order_id)}
                raw = self._throttled_call(
                    self.client.futures_account_trades,
                    execution_priority=True,
                    bypass_account_cache=True,
                    **kwargs,
                )
                payload = self._coerce_api_payload(raw)
                if isinstance(payload, list):
                    rows = [row for row in payload if isinstance(row, dict)]
                else:
                    rows = []

                close_qty = 0.0
                close_notional = 0.0
                result = ClosedPositionPnl(source="unknown")
                for row in rows:
                    row_ps = str(row.get("positionSide", "BOTH")).upper()
                    if position_side and row_ps not in (position_side.upper(), "BOTH"):
                        continue
                    result.realized_pnl += safe_float(row.get("realizedPnl"))
                    result.commission += safe_float(row.get("commission"))
                    result.fill_count += 1
                    qty = safe_float(row.get("qty"))
                    price = safe_float(row.get("price"))
                    if qty > 0 and price > 0:
                        close_qty += qty
                        close_notional += price * qty

                if result.fill_count > 0:
                    result.source = "userTrades"
                    if close_qty > 0:
                        result.exit_price = close_notional / close_qty
                    trade_logger.info(
                        "[%s] Fill PnL from REST order %s | rp=%.4f | fills=%s",
                        symbol,
                        order_id,
                        result.realized_pnl,
                        result.fill_count,
                    )
                    return result
            except Exception as exc:
                if self._rest_block_log.should_log(f"order_pnl:{symbol}:{order_id}"):
                    error_logger.warning(
                        "Order fill PnL REST attempt %s/%s failed for %s order %s: %s",
                        attempt,
                        attempts,
                        symbol,
                        order_id,
                        exc,
                    )
            if attempt < attempts:
                time.sleep(delay * attempt)

        return result

    # ---------------- Market data ----------------

    def fetch_historical_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int | None = None,
        *,
        allow_rest: Optional[bool] = None,
    ) -> pd.DataFrame:
        fetch_limit = limit or Config.CANDLE_FETCH_LIMIT
        if allow_rest is None:
            if self.in_scan_mode or self._background_ws_only():
                allow_rest = False
            else:
                allow_rest = self._rest_reads_allowed()

        def _rest_fetch() -> pd.DataFrame:
            klines = self._throttled_call(
                self.client.futures_klines,
                symbol=symbol,
                interval=timeframe,
                limit=fetch_limit,
            )
            df = pd.DataFrame(
                klines,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_asset_volume",
                    "number_of_trades",
                    "taker_buy_base_asset_volume",
                    "taker_buy_quote_asset_volume",
                    "ignore",
                ],
            )
            if df.empty:
                return pd.DataFrame()

            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            for col in ("open", "high", "low", "close", "volume"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            return df[["timestamp", "open", "high", "low", "close", "volume"]]

        if self._market_data:
            try:
                if not allow_rest:
                    return self._market_data.get_candles_cached_only(
                        symbol, timeframe, fetch_limit
                    )
                return self._market_data.get_candles(
                    symbol,
                    timeframe,
                    fetch_limit,
                    _rest_fetch,
                    allow_rest=True,
                )
            except ExchangeRateLimitError:
                return self._market_data.get_candles_cached_only(
                    symbol, timeframe, fetch_limit
                )
            except Exception as exc:
                error_logger.error(
                    "Cached candle fetch failed for %s %s: %s",
                    symbol,
                    timeframe,
                    exc,
                )
                return self._market_data.get_candles_cached_only(
                    symbol, timeframe, fetch_limit
                )

        if not allow_rest:
            return pd.DataFrame()

        try:
            return _rest_fetch()
        except ExchangeRateLimitError:
            return pd.DataFrame()
        except Exception as exc:
            error_logger.error(
                "Failed to fetch candles for %s %s: %s", symbol, timeframe, exc
            )
            return pd.DataFrame()

    def rest_fetch_klines_df(
        self, symbol: str, timeframe: str, limit: int
    ) -> pd.DataFrame:
        """REST kline fetch for bootstrap only (outside scan loops)."""
        return self.fetch_historical_candles(
            symbol, timeframe, limit=limit, allow_rest=True
        )

    def fetch_bootstrap_klines_df(
        self, symbol: str, timeframe: str, limit: int | None = None
    ) -> pd.DataFrame:
        """One-time REST kline fetch — must run inside bootstrap_context()."""
        if not self._is_bootstrap_priority():
            raise ExchangeError(
                "fetch_bootstrap_klines_df requires exchange.bootstrap_context()"
            )
        fetch_limit = limit or Config.CANDLE_FETCH_LIMIT
        return self._fetch_bootstrap_klines_direct(symbol, timeframe, fetch_limit)

    def _fetch_bootstrap_klines_direct(
        self, symbol: str, timeframe: str, limit: int
    ) -> pd.DataFrame:
        """
        Rate-limited REST kline fetch for startup bootstrap only.
        Returns empty DataFrame on ban/budget — never raises uncaught exceptions.
        """
        if not self._is_bootstrap_priority():
            raise ExchangeError(
                "Bootstrap kline fetch requires exchange.bootstrap_context()"
            )
        if not self.can_bootstrap_klines_rest():
            return pd.DataFrame()

        try:
            klines = self._throttled_call(
                self.client.futures_klines,
                symbol=symbol,
                interval=timeframe,
                limit=limit,
            )
        except ExchangeRateLimitError as exc:
            self.halt_kline_bootstrap(str(exc))
            return pd.DataFrame()
        except BinanceAPIException as exc:
            if self._is_rate_limit_error(exc):
                self._apply_rate_limit_halt(exc)
                self.halt_kline_bootstrap(str(exc.message))
                return pd.DataFrame()
            error_logger.warning(
                "Bootstrap kline fetch failed for %s %s: %s",
                symbol,
                timeframe,
                exc.message,
            )
            return pd.DataFrame()
        except ExchangeError as exc:
            error_logger.warning(
                "Bootstrap kline fetch failed for %s %s: %s",
                symbol,
                timeframe,
                exc,
            )
            return pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError) as exc:
            error_logger.warning(
                "Bootstrap kline network error for %s %s: %s",
                symbol,
                timeframe,
                exc,
            )
            return pd.DataFrame()

        if not klines:
            return pd.DataFrame()

        df = pd.DataFrame(
            klines,
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "ignore",
            ],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df[["timestamp", "open", "high", "low", "close", "volume"]]

    def bootstrap_scan_candles(self, symbols: list[str], timeframes: list[str]) -> int:
        """Seed WS kline buffers via REST before scan evaluation (not during scan)."""
        if not self._market_data or self.in_scan_mode:
            return 0
        if not self._rest_reads_allowed():
            return 0
        return self._market_data.bootstrap_candles(
            symbols,
            timeframes,
            Config.CANDLE_FETCH_LIMIT,
            self.rest_fetch_klines_df,
        )

    def get_mark_price(
        self, symbol: str, position_side: str = "LONG"
    ) -> Optional[float]:
        """Mark price from user-stream position cache."""
        if self._market_data:
            mark = self._market_data.get_ws_mark_price(symbol, position_side)
            if mark is not None and mark > 0:
                return mark
        return None

    def fetch_mark_price_rest(self, symbol: str) -> Optional[float]:
        """REST mark price — background only; never bypasses the weight governor."""
        if self.is_rest_blocked()[0]:
            return None
        if not self.can_make_background_rest_call(1) and not self._is_execution_priority():
            return None
        if not self._degraded_rest_allowed("mark"):
            return None
        try:
            data = self._throttled_call(
                self.client.futures_mark_price,
                symbol=symbol.upper(),
                **self.recv_window_param,
                execution_priority=self._is_execution_priority(),
            )
            mark = safe_float(data.get("markPrice"))
            return mark if mark > 0 else None
        except Exception as exc:
            error_logger.warning("REST mark price failed for %s: %s", symbol, exc)
            return None

    def _fetch_mark_price_rest_throttled(self, symbol: str) -> Optional[float]:
        """Per-symbol REST mark fetch with throttle + short-lived cache."""
        symbol = symbol.upper()
        min_interval = max(Config.MONITOR_REST_MARK_INTERVAL_SECONDS, 3.0)
        now = time.monotonic()
        with self._rest_mark_lock:
            last_fetch = self._rest_mark_fetched_at.get(symbol, 0.0)
            cached = self._rest_mark_cache.get(symbol)
            if cached and cached > 0 and (now - last_fetch) < min_interval:
                return cached

        mark = self.fetch_mark_price_rest(symbol)
        with self._rest_mark_lock:
            self._rest_mark_fetched_at[symbol] = now
            if mark and mark > 0:
                self._rest_mark_cache[symbol] = mark
                return mark
            return self._rest_mark_cache.get(symbol)

    def get_live_mark_price(
        self,
        symbol: str,
        position_side: str = "LONG",
        *,
        allow_rest: bool = True,
    ) -> Optional[float]:
        """
        Authoritative live mark for TP/SL and /active display.
        Never returns an unbounded stale WS cache entry.
        """
        symbol = symbol.upper()
        max_age = Config.VIRTUAL_TP_TICKER_MAX_AGE_SECONDS

        if self._market_data:
            fresh = self._market_data.get_fresh_ticker_price(
                symbol, max_age_seconds=max_age
            )
            if fresh is not None and fresh > 0:
                return fresh

        if allow_rest and not self.is_rest_blocked()[0]:
            rest_mark = self._fetch_mark_price_rest_throttled(symbol)
            if rest_mark is not None and rest_mark > 0:
                if self._market_data:
                    self._market_data.update_position_mark_from_ticker(
                        symbol, rest_mark
                    )
                if self._rest_block_log.should_log(f"live_mark_rest:{symbol}"):
                    trade_logger.debug(
                        "[%s] Live mark via REST fallback: %.6f (WS ticker stale)",
                        symbol,
                        rest_mark,
                    )
                return rest_mark

        return None

    def get_tp_monitor_price(
        self, symbol: str, position_side: str = "LONG"
    ) -> Optional[float]:
        """Best available live price for virtual TP/SL — rejects stale internal cache."""
        live = self.get_live_mark_price(symbol, position_side, allow_rest=True)
        if live is not None and live > 0:
            return live
        return None

    def get_ticker(self, symbol: str, *, force_rest: bool = False) -> Optional[float]:
        """
        Last traded price for execution. Uses WS only when the miniTicker row is
        fresh; otherwise an immediate futures_symbol_ticker REST call.
        """
        symbol = symbol.upper()
        hub = self._market_data
        needs_rest = force_rest
        if hub is not None and not needs_rest:
            requires_fn = getattr(hub, "execution_requires_rest_price", None)
            if callable(requires_fn):
                try:
                    needs_rest = bool(requires_fn(symbol))
                except Exception as exc:
                    error_logger.debug(
                        "execution_requires_rest_price failed for %s: %s", symbol, exc
                    )
                    needs_rest = True
            elif hasattr(hub, "ws_is_stale") and hub.ws_is_stale():
                needs_rest = True
            elif hasattr(hub, "is_ws_warming_up") and hub.is_ws_warming_up():
                needs_rest = True
        if hub is not None and not needs_rest:
            max_age = float(max(Config.WS_STALE_SECONDS, 30))
            fresh = hub.get_fresh_ticker_price(symbol, max_age_seconds=max_age)
            if fresh is not None and fresh > 0:
                return fresh
            needs_rest = True
        if not needs_rest:
            return None
        exec_lane = self._is_execution_priority()
        if not exec_lane:
            if self.is_rest_blocked()[0]:
                return None
            if not self.can_make_background_rest_call(1):
                return None
        try:
            ticker = self._throttled_call(
                self.client.futures_symbol_ticker,
                symbol=symbol,
                execution_priority=exec_lane,
            )
            price = safe_float(ticker.get("price"))
            if price > 0 and self._market_data is not None:
                try:
                    self._market_data.seed_tickers_from_rest(
                        {symbol: {"lastPrice": price}}
                    )
                except Exception:
                    pass
            return price if price > 0 else None
        except ExchangeRateLimitError:
            return None
        except Exception as exc:
            if self._rest_block_log.should_log(f"get_ticker_{symbol}"):
                error_logger.warning("REST ticker fetch failed for %s: %s", symbol, exc)
            return None

    def fetch_ticker(self, symbol: str) -> Optional[float]:
        """Force REST last price for execution (alias of get_ticker(force_rest=True))."""
        return self.get_ticker(symbol, force_rest=True)

    def get_symbol_price(self, symbol: str) -> Optional[float]:
        """Force REST last price for execution (alias of fetch_ticker)."""
        return self.fetch_ticker(symbol)

    def get_market_price(self, symbol: str, position_side: str = "LONG") -> Optional[float]:
        if self._market_data:
            cached = self._market_data.get_price(symbol)
            if cached is not None and cached > 0:
                return cached

        mark = self.get_mark_price(symbol, position_side)
        if mark is not None and mark > 0:
            return mark

        if self._market_data:
            df = self._market_data.get_candles_cached_only(symbol, "1m", 3)
            if not df.empty:
                close = safe_float(df.iloc[-1]["close"])
                if close > 0:
                    return close

        if not Config.ENABLE_REST_PRICE_FALLBACK or not self._rest_reads_allowed():
            return None

        try:
            ticker = self._throttled_call(
                self.client.futures_symbol_ticker,
                symbol=symbol,
            )
            price = safe_float(ticker.get("price"))
            return price if price > 0 else None
        except ExchangeRateLimitError:
            return None
        except Exception as exc:
            if self._rest_block_log.should_log(f"price_fetch_{symbol}"):
                error_logger.warning("Price fetch failed for %s: %s", symbol, exc)
            return None

    def get_book_spread_percent(self, symbol: str) -> Optional[float]:
        """Bid/ask spread % via cached bookTicker map when available."""
        try:
            book_map = self.get_book_ticker_map()
            ticker = book_map.get(symbol.upper(), {})
            bid = safe_float(ticker.get("bidPrice"))
            ask = safe_float(ticker.get("askPrice"))
            if bid <= 0 or ask <= 0:
                return None
            return ((ask - bid) / bid) * 100.0
        except ExchangeRateLimitError:
            raise
        except Exception as exc:
            error_logger.warning("Spread fetch failed for %s: %s", symbol, exc)
            return None

    def get_24h_quote_volume(self, symbol: str) -> float:
        try:
            if self._market_data and not self._market_data.ws_is_stale():
                row = self._market_data.get_ticker_map().get(symbol.upper(), {})
                volume = safe_float(row.get("quoteVolume"))
                if volume > 0:
                    return volume

            ticker_map = self.get_futures_ticker_map()
            row = ticker_map.get(symbol.upper(), {})
            return safe_float(row.get("quoteVolume"))
        except ExchangeRateLimitError:
            raise
        except Exception as exc:
            error_logger.warning("24h volume fetch failed for %s: %s", symbol, exc)
            return 0.0

    def get_futures_ticker_map(self) -> dict[str, dict[str, Any]]:
        """Return futures tickers — WS cache first; REST only when WS is empty/stale."""
        if self._market_data:
            cached = self._market_data.get_ticker_map()
            if cached and self._market_data.is_ticker_cache_usable(min_symbols=10):
                if not self._market_data.needs_ticker_rest_fallback():
                    return cached
            if self.is_rest_blocked()[0] or self._ws_reconnect_or_warmup():
                return cached
            if (
                Config.ENABLE_REST_TICKER_FALLBACK
                and self._market_data.needs_ticker_rest_fallback()
                and self.can_make_background_rest_call(
                    weight_for_call(self.client.futures_ticker)
                )
            ):
                self._market_data.refresh_ticker_cache_from_rest()
                cached = self._market_data.get_ticker_map()
                if cached:
                    return cached
            if self._market_data.ws_is_running():
                return cached

        if not Config.ENABLE_REST_TICKER_FALLBACK or self.is_rest_blocked()[0]:
            return self._market_data.get_ticker_map() if self._market_data else {}

        if not self.can_make_background_rest_call(
            weight_for_call(self.client.futures_ticker)
        ):
            return self._market_data.get_ticker_map() if self._market_data else {}

        result = self.fetch_futures_ticker_map_rest()
        if self._market_data and result:
            self._market_data.seed_tickers_from_rest(result)
        return result if result else (
            self._market_data.get_ticker_map() if self._market_data else {}
        )

    def get_book_ticker_map(self) -> dict[str, dict[str, Any]]:
        """Return book tickers — WS proxy by default; REST only when explicitly allowed."""
        if self._market_data and self._market_data.has_ws_book_data():
            return self._market_data.get_ws_book_ticker_map()
        allow_rest = self._rest_reads_allowed() and not Config.USE_WS_BOOK_PROXY
        if self._market_data:
            if Config.USE_WS_BOOK_PROXY:
                return self._build_book_proxy_from_tickers()
            return self._market_data.get_book_ticker_map(
                self._fetch_book_ticker_map_rest if allow_rest else None,
                allow_rest=allow_rest,
            )
        if not allow_rest:
            return {}
        return self._fetch_book_ticker_map_rest()

    def _build_book_proxy_from_tickers(self) -> dict[str, dict[str, Any]]:
        """Synthetic bid/ask from WS miniTicker high/low/last (zero REST)."""
        ticker_map: dict[str, dict[str, Any]] = {}
        if self._market_data:
            ticker_map = self._market_data.get_ticker_map()
        if not ticker_map:
            return {}

        proxy: dict[str, dict[str, Any]] = {}
        for symbol, row in ticker_map.items():
            last = safe_float(row.get("lastPrice"))
            high = safe_float(row.get("highPrice"))
            low = safe_float(row.get("lowPrice"))
            if last <= 0:
                continue
            bid = low if low > 0 else last
            ask = high if high > 0 else last
            if bid > ask:
                bid, ask = ask, bid
            proxy[str(symbol).upper()] = {
                "symbol": str(symbol).upper(),
                "bidPrice": bid,
                "askPrice": ask,
                "is_proxy": True,
            }
        return proxy

    def _fetch_book_ticker_map_rest(self) -> dict[str, dict[str, Any]]:
        try:
            tickers = self._throttled_call(self.client.futures_orderbook_ticker)
            return {str(t["symbol"]): t for t in tickers if t.get("symbol")}
        except ExchangeRateLimitError:
            raise
        except Exception as exc:
            error_logger.error("Failed to fetch book ticker map: %s", exc)
            return {}

    # ---------------- Leverage ----------------

    def optimize_and_set_leverage(self, symbol: str) -> int:
        with self._rules_lock:
            if symbol in self._leverage_cache:
                return self._leverage_cache[symbol]

        target_leverage = Config.MAX_LEVERAGE
        try:
            with self.execution_context():
                if Config.AUTO_DETECT_MAX_LEVERAGE:
                    brackets = self._throttled_call(
                        self.client.futures_leverage_bracket,
                        symbol=symbol,
                        **self.recv_window_param,
                        execution_priority=True,
                    )
                    if brackets and isinstance(brackets, list):
                        max_exchange_leverage = int(
                            brackets[0]["brackets"][0]["initialLeverage"]
                        )
                        target_leverage = min(Config.MAX_LEVERAGE, max_exchange_leverage)

                self._throttled_call(
                    self.client.futures_change_leverage,
                    symbol=symbol,
                    leverage=target_leverage,
                    **self.recv_window_param,
                    execution_priority=True,
                )
            with self._rules_lock:
                self._leverage_cache[symbol] = target_leverage
            system_logger.info("Leverage set for %s: %sx", symbol, target_leverage)
            return target_leverage
        except BinanceAPIException as exc:
            fallback = min(Config.MAX_LEVERAGE, 10)
            error_logger.error(
                "Leverage API error for %s (%s). Fallback %sx.",
                symbol,
                exc.message,
                fallback,
            )
            try:
                with self.execution_context():
                    self._throttled_call(
                        self.client.futures_change_leverage,
                        symbol=symbol,
                        leverage=fallback,
                        **self.recv_window_param,
                        execution_priority=True,
                    )
                with self._rules_lock:
                    self._leverage_cache[symbol] = fallback
                return fallback
            except Exception as fallback_exc:
                raise ExchangeError(
                    f"Leverage fallback failed for {symbol}: {fallback_exc}"
                ) from fallback_exc

    # ---------------- Positions ----------------

    def fetch_open_positions(self, force_refresh: bool = False) -> list[dict[str, Any]]:
        """Alias for get_all_open_positions — live exchange state."""
        return self.get_all_open_positions(force_refresh=force_refresh)

    def get_open_positions_count(self, force_refresh: bool = False) -> int:
        """Count non-zero hedge-mode positions on the exchange."""
        return len(self.get_all_open_positions(force_refresh=force_refresh))

    def get_all_open_positions(self, force_refresh: bool = False) -> list[dict[str, Any]]:
        """Return cached non-zero hedge-mode positions (REST refresh throttled)."""
        return self._refresh_positions_cache(force=force_refresh)

    def fetch_symbol_positions_rest(
        self, symbol: str, *, force: bool = False
    ) -> Optional[list[dict[str, Any]]]:
        """
        Authoritative per-symbol REST snapshot via futures_position_information.
        Returns None when REST is unavailable (never falls back to WS/cache).
        """
        symbol = symbol.upper()
        if self.is_rest_blocked()[0]:
            return None
        if not self._is_execution_priority() and not self._degraded_rest_allowed(
            "position", consume=False
        ):
            cached = self._symbol_position_rest_cache.get(symbol)
            return cached[1] if cached else None

        now = time.monotonic()
        min_interval = max(Config.POSITION_REST_VERIFY_MIN_INTERVAL_SECONDS, 5.0)
        if self._ws_reconnect_or_warmup():
            min_interval = max(min_interval, float(Config.WS_DEGRADED_REST_MIN_INTERVAL_SECONDS), 60.0)
        if not force or self._ws_reconnect_or_warmup():
            cached = self._symbol_position_rest_cache.get(symbol)
            if cached and (now - cached[0]) < min_interval:
                return cached[1]

        if not self._is_execution_priority() and not self._degraded_rest_allowed(
            "position"
        ):
            cached = self._symbol_position_rest_cache.get(symbol)
            return cached[1] if cached else None

        try:
            raw = self._throttled_call(
                self.client.futures_position_information,
                symbol=symbol,
                execution_priority=self._is_execution_priority(),
                bypass_account_cache=self._is_execution_priority(),
            )
        except ExchangeRateLimitError as exc:
            error_logger.warning(
                "REST position verification rate-limited for %s: %s", symbol, exc
            )
            cached = self._symbol_position_rest_cache.get(symbol)
            return cached[1] if cached else None
        except Exception as exc:
            error_logger.warning(
                "REST position verification failed for %s: %s", symbol, exc
            )
            cached = self._symbol_position_rest_cache.get(symbol)
            return cached[1] if cached else None

        if not isinstance(raw, list):
            return None

        self._last_account_rest_at = now
        self._symbol_position_rest_cache[symbol] = (now, raw)
        return raw

    def get_position_quantity_rest(
        self, symbol: str, position_side: str
    ) -> Optional[float]:
        """
        REST-backed position quantity for reconciliation.
        None = REST unavailable (defer close decision).
        0.0 = REST confirms positionAmt == 0.
        """
        symbol = symbol.upper()
        position_side = position_side.upper()
        raw = self.fetch_symbol_positions_rest(symbol)
        if raw is None:
            return None
        for pos in raw:
            if str(pos.get("positionSide", "")).upper() == position_side:
                return abs(safe_float(pos.get("positionAmt")))
        return 0.0

    def symbol_has_open_position_rest(self, symbol: str) -> Optional[bool]:
        """
        True when REST confirms any non-zero positionAmt for symbol.
        None when REST unavailable.
        """
        symbol = symbol.upper()
        raw = self.fetch_symbol_positions_rest(symbol)
        if raw is None:
            return None
        for pos in raw:
            if abs(safe_float(pos.get("positionAmt"))) > 0:
                return True
        return False

    def fetch_all_open_positions_rest(
        self, *, force: bool = False
    ) -> Optional[list[dict[str, Any]]]:
        """Full REST snapshot of open positions; None when REST unavailable."""
        if self.is_rest_blocked()[0]:
            return None
        if not self._is_execution_priority() and not self._degraded_rest_allowed(
            "position", consume=False
        ):
            return self._all_positions_rest_data

        now = time.monotonic()
        min_interval = max(Config.POSITION_REST_FULL_MIN_INTERVAL_SECONDS, 30.0)
        if self._ws_reconnect_or_warmup():
            min_interval = max(
                min_interval, float(Config.WS_DEGRADED_REST_MIN_INTERVAL_SECONDS), 60.0
            )
        if (
            (not force or self._ws_reconnect_or_warmup())
            and self._all_positions_rest_data is not None
            and (now - self._all_positions_rest_at) < min_interval
        ):
            return self._all_positions_rest_data

        if not self._is_execution_priority() and not self._degraded_rest_allowed(
            "position"
        ):
            return self._all_positions_rest_data

        try:
            raw = self._throttled_call(
                self.client.futures_position_information,
                execution_priority=self._is_execution_priority(),
                bypass_account_cache=self._is_execution_priority(),
            )
        except ExchangeRateLimitError as exc:
            error_logger.warning("REST open-positions fetch rate-limited: %s", exc)
            return self._all_positions_rest_data
        except Exception as exc:
            error_logger.warning("REST open-positions fetch failed: %s", exc)
            return self._all_positions_rest_data

        if not isinstance(raw, list):
            return self._all_positions_rest_data

        parsed = self._parse_open_positions(raw or [])
        self._all_positions_rest_at = now
        self._all_positions_rest_data = parsed
        return parsed

    def _mark_to_market_unrealized(self, positions: list[dict[str, Any]]) -> float:
        """Estimate uPnL from mark/ticker when exchange-reported uPnL is missing."""
        total = 0.0
        for pos in positions:
            reported = safe_float(pos.get("unrealized_pnl"))
            if reported != 0:
                total += reported
                continue
            symbol = str(pos.get("symbol", "")).upper()
            side = str(pos.get("positionSide", "LONG")).upper()
            entry = safe_float(pos.get("entry_price"))
            qty = safe_float(pos.get("quantity"))
            if qty <= 0 or entry <= 0:
                continue
            mark = safe_float(pos.get("mark_price"))
            if mark <= 0:
                mark = safe_float(self.get_mark_price(symbol, side))
            if mark <= 0:
                mark = safe_float(self.get_market_price(symbol, side))
            if mark <= 0:
                continue
            if side == "LONG":
                total += (mark - entry) * qty
            else:
                total += (entry - mark) * qty
        return total

    def get_unrealized_pnl_total(self, force_refresh: bool = False) -> float:
        """Sum unrealized PnL from positions; mark-to-market when WS reports zero."""
        positions = self._refresh_positions_cache(force=force_refresh)
        if self._market_data and self._market_data.get_ws_positions():
            ws_total = self._market_data.get_ws_unrealized_pnl_total()
            if ws_total != 0:
                self._position_cache.unrealized_pnl_total = ws_total
                return ws_total

        reported_total = sum(safe_float(p.get("unrealized_pnl")) for p in positions)
        if reported_total != 0:
            self._position_cache.unrealized_pnl_total = reported_total
            return reported_total

        estimated = self._mark_to_market_unrealized(positions)
        if estimated != 0:
            self._position_cache.unrealized_pnl_total = estimated
            return estimated
        return self._position_cache.unrealized_pnl_total

    def get_position_quantity_cached(
        self, symbol: str, position_side: str
    ) -> float:
        """WS + local cache only — never blocks on REST (for TP/SL tick path)."""
        symbol = symbol.upper()
        position_side = position_side.upper()
        if self._market_data:
            ws_qty = self._market_data.get_ws_position_quantity(symbol, position_side)
            if ws_qty > 0:
                return ws_qty
        for pos in self._position_cache.positions:
            if pos.get("symbol") == symbol and pos.get("positionSide") == position_side:
                return safe_float(pos.get("quantity"))
        return 0.0

    def get_fill_price_from_order(
        self, symbol: str, order_response: dict[str, Any], fallback: float
    ) -> float:
        """Resolve a reliable fill price from an order response."""
        avg_price = safe_float(order_response.get("avgPrice"))
        if avg_price > 0:
            return avg_price

        order_id = order_response.get("orderId")
        if order_id is not None and not self.is_rest_blocked()[0]:
            try:
                with self.execution_context():
                    order_info = self._throttled_call(
                        self.client.futures_get_order,
                        symbol=symbol,
                        orderId=order_id,
                        execution_priority=True,
                    )
                queried = safe_float(order_info.get("avgPrice"))
                if queried > 0:
                    return queried
                executed = safe_float(order_info.get("executedQty"))
                cum_quote = safe_float(order_info.get("cumQuote"))
                if executed > 0 and cum_quote > 0:
                    return cum_quote / executed
            except Exception as exc:
                error_logger.warning(
                    "Could not query fill price for %s order %s: %s",
                    symbol,
                    order_id,
                    exc,
                )

        live_price = self.get_market_price(symbol)
        if live_price is not None and live_price > 0:
            return live_price
        return fallback

    def seed_position_after_fill(
        self,
        symbol: str,
        position_side: str,
        quantity: float,
        entry_price: float,
    ) -> None:
        """Update WS/local position caches immediately after a fill (no REST)."""
        symbol = symbol.upper()
        position_side = position_side.upper()
        if self._market_data:
            self._market_data.seed_open_position(
                symbol, position_side, quantity, entry_price
            )

        merged = False
        for pos in self._position_cache.positions:
            if pos.get("symbol") == symbol and pos.get("positionSide") == position_side:
                pos["quantity"] = quantity
                pos["entry_price"] = entry_price
                merged = True
                break
        if not merged:
            self._position_cache.positions.append(
                {
                    "symbol": symbol,
                    "positionSide": position_side,
                    "quantity": quantity,
                    "entry_price": entry_price,
                    "unrealized_pnl": 0.0,
                }
            )
        self._position_cache.unrealized_pnl_total = sum(
            safe_float(p.get("unrealized_pnl")) for p in self._position_cache.positions
        )
        self._position_cache.updated_at = time.monotonic()

    def has_open_position(self, symbol: str, position_side: str) -> bool:
        return self.get_position_quantity(symbol, position_side) > 0

    def get_position_quantity(self, symbol: str, position_side: str) -> float:
        symbol = symbol.upper()
        position_side = position_side.upper()
        cached = self.get_position_quantity_cached(symbol, position_side)
        if cached > 0:
            return cached
        for pos in self._refresh_positions_cache():
            if pos.get("symbol") == symbol and pos.get("positionSide") == position_side:
                return safe_float(pos.get("quantity"))
        return 0.0

    # ---------------- Orders ----------------

    def lookup_order_by_client_id(
        self, symbol: str, client_order_id: str
    ) -> Optional[dict[str, Any]]:
        """Reconcile an uncertain submit using origClientOrderId (no duplicate create)."""
        if not client_order_id:
            return None
        try:
            with self.execution_context():
                response = self._throttled_call(
                    self.client.futures_get_order,
                    symbol=symbol,
                    origClientOrderId=client_order_id,
                    execution_priority=True,
                    **self.recv_window_param,
                )
            return response if isinstance(response, dict) else None
        except ExchangeRateLimitError as exc:
            error_logger.warning(
                "Order lookup rate-limited for %s clientOrderId=%s: %s",
                symbol,
                client_order_id,
                exc,
            )
            return None
        except Exception as exc:
            error_logger.warning(
                "Order lookup failed for %s clientOrderId=%s: %s",
                symbol,
                client_order_id,
                exc,
            )
            return None

    def lookup_order_by_id(
        self, symbol: str, order_id: Any
    ) -> Optional[dict[str, Any]]:
        if not order_id:
            return None
        try:
            with self.execution_context():
                response = self._throttled_call(
                    self.client.futures_get_order,
                    symbol=symbol,
                    orderId=order_id,
                    execution_priority=True,
                    **self.recv_window_param,
                )
            return response if isinstance(response, dict) else None
        except Exception as exc:
            error_logger.warning(
                "Order lookup failed for %s orderId=%s: %s", symbol, order_id, exc
            )
            return None

    def _confirm_order_status(
        self,
        symbol: str,
        response: dict[str, Any],
        client_order_id: str = "",
    ) -> dict[str, Any]:
        status = str(response.get("status") or "").upper()
        if status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
            return response
        order_id = response.get("orderId")
        for attempt in range(3):
            time.sleep(0.35 * (attempt + 1))
            looked = None
            if client_order_id:
                looked = self.lookup_order_by_client_id(symbol, client_order_id)
            if looked is None and order_id:
                looked = self.lookup_order_by_id(symbol, order_id)
            if not looked:
                continue
            response = looked
            status = str(response.get("status") or "").upper()
            if status in {
                "FILLED",
                "PARTIALLY_FILLED",
                "CANCELED",
                "EXPIRED",
                "REJECTED",
            }:
                return response
        return response

    def _finalize_order_response(
        self,
        symbol: str,
        response: dict[str, Any],
        quantity_label: str,
        client_order_id: str = "",
    ) -> dict[str, Any]:
        response = self._confirm_order_status(symbol, response, client_order_id)
        status = str(response.get("status") or "").upper()
        if status == "FILLED":
            trade_logger.info(
                "[ORDER_FILLED] %s | orderId=%s avg=%s qty=%s",
                symbol,
                response.get("orderId"),
                response.get("avgPrice"),
                quantity_label,
            )
        elif status == "PARTIALLY_FILLED":
            trade_logger.info(
                "[PARTIALLY_FILLED] %s | orderId=%s executedQty=%s",
                symbol,
                response.get("orderId"),
                response.get("executedQty"),
            )
        elif status in {"CANCELED", "EXPIRED", "REJECTED"}:
            raise OrderExecutionError(
                f"ORDER_REJECTED: Binance status={status} orderId={response.get('orderId')}"
            )
        else:
            trade_logger.info(
                "[ORDER_ACCEPTED_NOT_FILLED] %s | orderId=%s status=%s",
                symbol,
                response.get("orderId"),
                status or "UNKNOWN",
            )
        return response

    def execute_futures_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        quantity: float,
        price: Optional[float] = None,
        reduce_only: bool = False,
        new_client_order_id: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        rules = self.get_symbol_rules(symbol)
        clean_qty = amount_to_precision(
            quantity, rules.step_size, rules.quantity_precision
        )
        if clean_qty < rules.min_qty:
            raise OrderExecutionError(
                f"Quantity {clean_qty} below min_qty {rules.min_qty} for {symbol}"
            )

        notional = clean_qty * (price or safe_float(self.get_market_price(symbol)))
        if notional < rules.min_notional:
            raise OrderExecutionError(
                f"Notional ${notional:.2f} below minimum ${rules.min_notional:.2f}"
            )

        order_params: dict[str, Any] = {
            "symbol": symbol,
            "side": side.upper(),
            "positionSide": position_side.upper(),
            "quantity": f"{clean_qty:.{rules.quantity_precision}f}",
        }

        if price is not None:
            clean_price = round_step_size(price, rules.tick_size, rules.price_precision)
            order_params["type"] = "LIMIT"
            order_params["price"] = f"{clean_price:.{rules.price_precision}f}"
            order_params["timeInForce"] = "GTC"
        else:
            order_params["type"] = "MARKET"

        if new_client_order_id:
            order_params["newClientOrderId"] = new_client_order_id

        trade_logger.info(
            "[ORDER_SUBMITTING] %s | %s %s | type=%s qty=%s reduce_only=%s clientOrderId=%s",
            symbol,
            side.upper(),
            position_side.upper(),
            order_params["type"],
            order_params["quantity"],
            reduce_only,
            new_client_order_id or "",
        )

        try:
            with self.execution_context():
                response = self._throttled_call(
                    self.client.futures_create_order,
                    **order_params,
                    **self.recv_window_param,
                    execution_priority=True,
                )
            self.invalidate_balance_cache()
            self.invalidate_position_cache()
            if not isinstance(response, dict):
                raise OrderExecutionError("SUBMISSION_FAILED: empty Binance order response")
            trade_logger.info(
                "[BINANCE_ACK] %s | orderId=%s status=%s clientOrderId=%s",
                symbol,
                response.get("orderId"),
                response.get("status"),
                response.get("clientOrderId") or new_client_order_id or "",
            )
            return self._finalize_order_response(
                symbol,
                response,
                order_params["quantity"],
                new_client_order_id or "",
            )
        except PositionAlreadyClosedError:
            raise
        except ExchangeRateLimitError as exc:
            recovered = (
                self.lookup_order_by_client_id(symbol, new_client_order_id)
                if new_client_order_id
                else None
            )
            if recovered:
                trade_logger.warning(
                    "[BINANCE_ACK] %s recovered after rate-limit via clientOrderId=%s",
                    symbol,
                    new_client_order_id,
                )
                return self._finalize_order_response(
                    symbol, recovered, order_params["quantity"], new_client_order_id or ""
                )
            raise OrderExecutionError(f"RATE_LIMITED: {exc}") from exc
        except BinanceAPIException as exc:
            if exc.code == -2022 or "reduceonly order is rejected" in str(
                exc.message
            ).lower():
                raise PositionAlreadyClosedError(
                    str(exc.message), code=int(exc.code)
                ) from exc
            recovered = (
                self.lookup_order_by_client_id(symbol, new_client_order_id)
                if new_client_order_id
                else None
            )
            if recovered:
                return self._finalize_order_response(
                    symbol, recovered, order_params["quantity"], new_client_order_id or ""
                )
            error_logger.error(
                "[ORDER_REJECTED] Binance rejected %s %s %s: %s (code=%s) payload=%s",
                symbol,
                side.upper(),
                position_side.upper(),
                exc.message,
                exc.code,
                {k: order_params.get(k) for k in ("symbol", "side", "positionSide", "type", "quantity", "price", "newClientOrderId")},
            )
            if self._critical_alerts:
                self._critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Order rejected on {symbol} {side} {position_side}: {exc.message}",
                    exc=exc,
                )
            raise OrderExecutionError(f"ORDER_REJECTED: {exc.message}") from exc
        except ExchangeError as exc:
            if PositionAlreadyClosedError.matches(exc):
                raise PositionAlreadyClosedError(str(exc)) from exc
            recovered = (
                self.lookup_order_by_client_id(symbol, new_client_order_id)
                if new_client_order_id
                else None
            )
            if recovered:
                trade_logger.warning(
                    "[BINANCE_ACK] %s recovered after uncertain submit via clientOrderId=%s",
                    symbol,
                    new_client_order_id,
                )
                return self._finalize_order_response(
                    symbol, recovered, order_params["quantity"], new_client_order_id or ""
                )
            raise OrderExecutionError(f"SUBMISSION_FAILED: {exc}") from exc
        except BinanceOrderException as exc:
            if self._critical_alerts:
                self._critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Order exception on {symbol}: {exc.message}",
                    exc=exc,
                )
            raise OrderExecutionError(f"ORDER_REJECTED: {exc.message}") from exc
        except Exception as exc:
            recovered = (
                self.lookup_order_by_client_id(symbol, new_client_order_id)
                if new_client_order_id
                else None
            )
            if recovered:
                return self._finalize_order_response(
                    symbol, recovered, order_params["quantity"], new_client_order_id or ""
                )
            if self._critical_alerts:
                self._critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Unexpected order failure on {symbol}: {exc}",
                    exc=exc,
                )
            raise OrderExecutionError(f"SUBMISSION_FAILED: {exc}") from exc

    # ---------------- Native TP/SL (exchange conditional orders) ----------------

    def _format_stop_price(self, symbol: str, stop_price: float) -> str:
        rules = self.get_symbol_rules(symbol)
        clean = round_step_size(stop_price, rules.tick_size, rules.price_precision)
        return f"{clean:.{rules.price_precision}f}"

    def place_conditional_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        order_type: str,
        stop_price: float,
        *,
        quantity: Optional[float] = None,
        close_position: bool = False,
    ) -> dict[str, Any]:
        """Place STOP_MARKET or TAKE_PROFIT_MARKET (hedge-mode positionSide)."""
        symbol = symbol.upper()
        position_side = position_side.upper()
        side = side.upper()
        order_type = order_type.upper()
        rules = self.get_symbol_rules(symbol)

        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": order_type,
            "stopPrice": self._format_stop_price(symbol, stop_price),
            "workingType": Config.NATIVE_TP_WORKING_TYPE,
        }
        if close_position:
            params["closePosition"] = "true"
        else:
            if quantity is None or quantity <= 0:
                raise OrderExecutionError(
                    f"Quantity required for {order_type} on {symbol}"
                )
            clean_qty = amount_to_precision(
                quantity, rules.step_size, rules.quantity_precision
            )
            if clean_qty < rules.min_qty:
                raise OrderExecutionError(
                    f"Conditional qty {clean_qty} below min_qty for {symbol}"
                )
            params["quantity"] = f"{clean_qty:.{rules.quantity_precision}f}"

        with self.execution_context():
            response = self._throttled_call(
                self.client.futures_create_order,
                **params,
                **self.recv_window_param,
                execution_priority=True,
            )
        trade_logger.info(
            "Native %s placed | %s %s | stop=%s | qty=%s | closePosition=%s | id=%s",
            order_type,
            symbol,
            position_side,
            params["stopPrice"],
            params.get("quantity", "ALL"),
            close_position,
            response.get("orderId"),
        )
        return response

    def place_native_exit_bracket(
        self,
        symbol: str,
        position_side: str,
        sl_price: float,
        tp_specs: list[tuple[str, float, float]],
    ) -> dict[str, str]:
        """
        Place exchange-native SL (close entire position) + partial TP market orders.
        Returns map of level -> orderId (includes key 'SL').
        """
        position_side = position_side.upper()
        close_side = "SELL" if position_side == "LONG" else "BUY"
        placed: dict[str, str] = {}
        tp_order_ids: list[str] = []

        if sl_price > 0:
            sl_resp = self.place_conditional_order(
                symbol,
                close_side,
                position_side,
                "STOP_MARKET",
                sl_price,
                close_position=True,
            )
            sl_id = str(sl_resp.get("orderId", ""))
            if sl_id:
                placed["SL"] = sl_id

        for level, tp_price, tp_qty in tp_specs:
            if tp_price <= 0 or tp_qty <= 0:
                continue
            try:
                tp_resp = self.place_conditional_order(
                    symbol,
                    close_side,
                    position_side,
                    "TAKE_PROFIT_MARKET",
                    tp_price,
                    quantity=tp_qty,
                )
                tp_id = str(tp_resp.get("orderId", ""))
                if tp_id:
                    placed[level.upper()] = tp_id
                    tp_order_ids.append(tp_id)
            except OrderExecutionError as exc:
                error_logger.warning(
                    "Native %s order failed for %s %s: %s",
                    level,
                    symbol,
                    position_side,
                    exc,
                )

        if sl_price > 0 and "SL" not in placed and not tp_order_ids:
            raise OrderExecutionError(f"Failed to place any native exit orders on {symbol}")

        return placed

    def cancel_order_by_id(self, symbol: str, order_id: str) -> bool:
        symbol = symbol.upper()
        if not order_id:
            return False
        try:
            with self.execution_context():
                self._throttled_call(
                    self.client.futures_cancel_order,
                    symbol=symbol,
                    orderId=int(order_id),
                    **self.recv_window_param,
                    execution_priority=True,
                )
            trade_logger.info("Canceled order %s on %s.", order_id, symbol)
            return True
        except BinanceAPIException as exc:
            if exc.code in (-2011, -2013):
                return False
            error_logger.warning(
                "Cancel order %s on %s failed: %s", order_id, symbol, exc.message
            )
            return False
        except Exception as exc:
            error_logger.warning(
                "Cancel order %s on %s failed: %s", order_id, symbol, exc
            )
            return False

    def get_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        symbol = symbol.upper()
        try:
            with self.execution_context():
                orders = self._throttled_call(
                    self.client.futures_get_open_orders,
                    symbol=symbol,
                    **self.recv_window_param,
                    execution_priority=True,
                )
            return list(orders or [])
        except Exception as exc:
            error_logger.warning("Failed to fetch open orders for %s: %s", symbol, exc)
            return []

    def is_order_still_open(self, symbol: str, order_id: str) -> bool:
        if not order_id:
            return False
        for order in self.get_open_orders(symbol):
            if str(order.get("orderId")) == str(order_id):
                return True
        return False

    def cancel_native_exit_orders(
        self,
        symbol: str,
        position_side: str,
        order_ids: Optional[dict[str, str]] = None,
    ) -> int:
        """Cancel native SL/TP orders for a position side. Returns cancel count."""
        symbol = symbol.upper()
        position_side = position_side.upper()
        canceled = 0
        target_ids: set[str] = set()
        if order_ids:
            target_ids = {str(v) for v in order_ids.values() if v}

        open_orders = self.get_open_orders(symbol)
        for order in open_orders:
            if str(order.get("positionSide", "")).upper() != position_side:
                continue
            order_type = str(order.get("type", "")).upper()
            if order_type not in (
                "STOP_MARKET",
                "TAKE_PROFIT_MARKET",
                "STOP",
                "TAKE_PROFIT",
                "LIMIT",
            ):
                continue
            oid = str(order.get("orderId", ""))
            if target_ids and oid not in target_ids:
                continue
            if self.cancel_order_by_id(symbol, oid):
                canceled += 1
        return canceled

    def refresh_native_stop_loss(
        self,
        symbol: str,
        position_side: str,
        new_sl_price: float,
        old_sl_order_id: Optional[str] = None,
    ) -> Optional[str]:
        """Replace native SL after break-even / trailing update."""
        if old_sl_order_id:
            self.cancel_order_by_id(symbol, old_sl_order_id)
        if new_sl_price <= 0:
            return None
        close_side = "SELL" if position_side.upper() == "LONG" else "BUY"
        resp = self.place_conditional_order(
            symbol,
            close_side,
            position_side,
            "STOP_MARKET",
            new_sl_price,
            close_position=True,
        )
        return str(resp.get("orderId", "")) or None

    def close_position_quantity(
        self,
        symbol: str,
        position_side: str,
        quantity: float,
    ) -> Optional[dict[str, Any]]:
        symbol = symbol.upper()
        position_side = position_side.upper()
        live_qty = self.get_position_quantity_cached(symbol, position_side)
        if live_qty <= 0:
            live_qty = self.get_position_quantity(symbol, position_side)
        if live_qty <= 0:
            raise PositionAlreadyClosedError(
                f"No open {position_side} position on {symbol} — skip reduce-only close."
            )
        close_qty = min(quantity, live_qty)
        close_side = "SELL" if position_side == "LONG" else "BUY"
        try:
            response = self.execute_futures_order(
                symbol=symbol,
                side=close_side,
                position_side=position_side,
                quantity=close_qty,
            )
        except PositionAlreadyClosedError:
            self.clear_position_cache(symbol, position_side)
            raise
        self.invalidate_balance_cache()
        self.invalidate_position_cache()
        return response
