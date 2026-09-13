"""
Binance USDT-M Futures exchange adapter.
Thread-safe symbol rules cache, rate limiting, balance caching, and order execution.
"""

from __future__ import annotations

import random
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
class BalanceCache:
    value: float = 0.0
    updated_at: float = 0.0
    ttl_seconds: int = Config.BALANCE_CACHE_TTL_SECONDS

    def is_valid(self) -> bool:
        return self.updated_at > 0 and (time.monotonic() - self.updated_at) < self.ttl_seconds

    def set(self, balance: float) -> None:
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


ACCOUNT_REST_ENDPOINTS = frozenset(
    {"futures_account", "futures_position_information"}
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

        self.client = Client(
            api_key=self.api_key,
            api_secret=self.api_secret,
            testnet=self.testnet,
            requests_params={"timeout": Config.REQUEST_TIMEOUT},
        )
        self.recv_window_param = {"recvWindow": 60000}

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
        self._last_balance_rest_at: float = 0.0
        self._last_account_rest_at: float = 0.0
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
        if hub and hasattr(self, "_startup_ban") and self._startup_ban.is_banned:
            hub.apply_startup_ban(self._startup_ban)

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
        if until_ms:
            halt_seconds = max(
                int((until_ms / 1000.0) - time.time()),
                Config.RATE_LIMIT_HALT_SECONDS,
            )
        elif exc.code == -1003:
            halt_seconds = max(
                Config.RATE_LIMIT_SOFT_HALT_SECONDS,
                Config.RATE_LIMIT_HALT_SECONDS,
            )
            halt_seconds = min(halt_seconds, 120)
        else:
            halt_seconds = max(
                Config.REST_BAN_MIN_SLEEP_SECONDS,
                Config.RATE_LIMIT_HALT_SECONDS,
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

    def _resolve_rest_lane(
        self, *, execution_priority: bool
    ) -> RestLane:
        if execution_priority or self._is_execution_priority():
            return RestLane.EXECUTION
        if self._is_bootstrap_priority():
            return RestLane.BOOTSTRAP
        return RestLane.BACKGROUND

    def can_make_background_rest_call(self, weight: int = 1) -> bool:
        """True when a non-execution REST call is allowed (ban, hard-stop, budget)."""
        if self._market_data and self._market_data.is_rest_blocked()[0]:
            return False
        if self._rest_token_bucket.is_hard_stopped():
            return False
        if not Config.ENABLE_STRICT_RATE_LIMIT:
            return True
        lane = RestLane.BOOTSTRAP if self._is_bootstrap_priority() else RestLane.BACKGROUND
        return self._rest_budget.has_budget_for(max(weight, 1), lane)

    def is_rest_blocked(self) -> tuple[bool, str]:
        """True when REST must not be attempted (IP ban / hard-stop)."""
        if self._market_data:
            blocked, reason = self._market_data.is_rest_blocked()
            if blocked:
                return True, reason
        if self._rest_token_bucket.is_hard_stopped():
            remaining = int(self._rest_token_bucket.hard_stop_remaining())
            return True, f"REST hard-stop (~{remaining}s remaining)"
        return False, ""

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

    @staticmethod
    def _extract_quote_balance(account_info: dict[str, Any], quote: str) -> float:
        for key in (
            "availableBalance",
            "totalCrossWalletBalance",
            "totalWalletBalance",
        ):
            value = safe_float(account_info.get(key))
            if value > 0:
                return value
        for asset in account_info.get("assets", []):
            if asset.get("asset") != quote:
                continue
            for field in ("availableBalance", "crossWalletBalance", "walletBalance"):
                value = safe_float(asset.get(field))
                if value > 0:
                    return value
        return 0.0

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
        if self._balance_cache.is_valid():
            quote = Config.QUOTE_ASSET
            balance = self._balance_cache.value
            return {
                "assets": [
                    {
                        "asset": quote,
                        "availableBalance": str(balance),
                        "crossWalletBalance": str(balance),
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
        if self._market_data and self._market_data.is_rest_blocked()[0]:
            return False
        if self._rest_token_bucket.is_hard_stopped():
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
        if name == "futures_account":
            if not execution_priority and not self._account_rest_interval_elapsed():
                return "account_rest_interval"
            if Config.ENABLE_STRICT_RATE_LIMIT:
                reserve = max(Config.REST_BUDGET_ACCOUNT_RESERVE_FRACTION, 0.0)
                if self._rest_budget.remaining_fraction() < reserve:
                    return "rest_budget_reserve"
        elif name == "futures_position_information":
            if not Config.ENABLE_REST_POSITION_POLL:
                return "rest_position_poll_disabled"
            if not execution_priority and not self._account_rest_interval_elapsed():
                return "account_rest_interval"
            if Config.ENABLE_STRICT_RATE_LIMIT:
                reserve = max(Config.REST_BUDGET_ACCOUNT_RESERVE_FRACTION, 0.0)
                if self._rest_budget.remaining_fraction() < reserve:
                    return "rest_budget_reserve"
        return ""

    def _return_cached_account_call(self, func: Any) -> Any:
        name = getattr(func, "__name__", "")
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

        call_weight = weight_for_call(func)
        lane = self._resolve_rest_lane(execution_priority=priority)

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
        network_retries = max(Config.REST_NETWORK_MAX_RETRIES, 1)
        for attempt in range(1, network_retries + 1):
            if Config.ENABLE_STRICT_RATE_LIMIT:
                limiter.wait()
            if is_bootstrap_kline:
                self._enforce_kline_rest_pace()
            try:
                result = func(*args, **kwargs)
                if getattr(func, "__name__", "") == "futures_account" and isinstance(
                    result, dict
                ):
                    self._store_account_rest_response(result)
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

    def fetch_futures_ticker_map_rest(self) -> dict[str, dict[str, Any]]:
        """
        Single futures_ticker() REST call to warm the cache (weight=1, batched).
        Skipped entirely during IP ban, hard-stop, or low REST budget reserve.
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
            account_info = self._throttled_call(
                self.client.futures_account,
                **self.recv_window_param,
            )
            balance = self._extract_quote_balance(account_info, quote)
            if balance >= 0:
                self._balance_cache.set(balance)
                return balance
        except ExchangeRateLimitError:
            pass
        except Exception as exc:
            error_logger.warning("Startup balance fetch failed: %s", exc)

        return self._balance_cache.value

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

        now = time.monotonic()
        poll_interval = max(float(Config.ACCOUNT_REST_MIN_INTERVAL_SECONDS), 60.0)

        needs_rest = force_refresh or self._balance_cache.value <= 0

        if (
            not needs_rest
            and self._last_account_rest_at > 0
            and (now - self._last_account_rest_at) < poll_interval
        ):
            cached = self._balance_cache.value
            if cached > 0:
                return cached
            margin_est = self._ws_margin_balance_estimate()
            return margin_est if margin_est is not None else cached

        if now < self._balance_rest_backoff_until and not needs_rest:
            cached = self._balance_cache.value
            if cached > 0:
                return cached
            margin_est = self._ws_margin_balance_estimate()
            return margin_est if margin_est is not None else cached

        if not Config.ENABLE_REST_BALANCE_POLL and not needs_rest:
            margin_est = self._ws_margin_balance_estimate()
            if margin_est is not None and margin_est > 0:
                return margin_est
            return self._balance_cache.value

        if not self._rest_reads_allowed() and not self._is_execution_priority():
            margin_est = self._ws_margin_balance_estimate()
            if margin_est is not None and margin_est > 0:
                return margin_est
            return self._balance_cache.value

        if self.rest_account_reads_blocked() and not self._is_execution_priority():
            cached = self._cached_futures_account_response()
            if cached:
                balance = self._extract_quote_balance(cached, quote)
                if balance > 0:
                    self._balance_cache.set(balance)
                    return balance
            margin_est = self._ws_margin_balance_estimate()
            if margin_est is not None and margin_est > 0:
                return margin_est
            return self._balance_cache.value

        try:
            account_info = self._throttled_call(
                self.client.futures_account,
                **self.recv_window_param,
            )
            balance = self._extract_quote_balance(account_info, quote)
            if balance > 0:
                self._balance_cache.set(balance)
                self._account_rest_cache.account_info = account_info
                self._account_rest_cache.updated_at = now
                return balance
            margin_est = self._ws_margin_balance_estimate()
            if margin_est is not None and margin_est > 0:
                return margin_est
            return self._balance_cache.value
        except ExchangeRateLimitError:
            self._balance_rest_backoff_until = now + poll_interval
            cached = self._cached_futures_account_response()
            if cached:
                balance = self._extract_quote_balance(cached, quote)
                if balance > 0:
                    return balance
            margin_est = self._ws_margin_balance_estimate()
            if margin_est is not None and margin_est > 0:
                return margin_est
            return self._balance_cache.value
        except Exception as exc:
            self._balance_rest_backoff_until = now + min(poll_interval, 120.0)
            if self._rest_block_log.should_log("balance_fetch_failed"):
                error_logger.warning("Balance REST fetch failed (cached fallback): %s", exc)
            margin_est = self._ws_margin_balance_estimate()
            if margin_est is not None and margin_est > 0:
                return margin_est
            return self._balance_cache.value

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
        """REST mark price — used by watchdog when WS hub is unavailable."""
        if self.is_rest_blocked()[0]:
            return None
        try:
            with self.execution_context():
                data = self._throttled_call(
                    self.client.futures_mark_price,
                    symbol=symbol.upper(),
                    **self.recv_window_param,
                    execution_priority=True,
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

        if self._market_data and self._market_data.ws_is_stale():
            if allow_rest and not self.is_rest_blocked()[0]:
                forced = self.fetch_mark_price_rest(symbol)
                if forced and forced > 0:
                    with self._rest_mark_lock:
                        self._rest_mark_cache[symbol] = forced
                        self._rest_mark_fetched_at[symbol] = time.monotonic()
                    return forced

        return None

    def get_tp_monitor_price(
        self, symbol: str, position_side: str = "LONG"
    ) -> Optional[float]:
        """Best available live price for virtual TP/SL — rejects stale internal cache."""
        live = self.get_live_mark_price(symbol, position_side, allow_rest=True)
        if live is not None and live > 0:
            return live
        return None

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
            if self.is_rest_blocked()[0]:
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

        now = time.monotonic()
        min_interval = max(Config.POSITION_REST_VERIFY_MIN_INTERVAL_SECONDS, 5.0)
        if not force:
            cached = self._symbol_position_rest_cache.get(symbol)
            if cached and (now - cached[0]) < min_interval:
                return cached[1]

        try:
            with self.execution_context():
                raw = self._throttled_call(
                    self.client.futures_position_information,
                    symbol=symbol,
                    execution_priority=True,
                    allow_during_scan=True,
                    bypass_account_cache=True,
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

        now = time.monotonic()
        min_interval = max(Config.POSITION_REST_FULL_MIN_INTERVAL_SECONDS, 30.0)
        if (
            not force
            and self._all_positions_rest_data is not None
            and (now - self._all_positions_rest_at) < min_interval
        ):
            return self._all_positions_rest_data

        try:
            with self.execution_context():
                raw = self._throttled_call(
                    self.client.futures_position_information,
                    execution_priority=True,
                    allow_during_scan=True,
                    bypass_account_cache=True,
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

    def execute_futures_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        quantity: float,
        price: Optional[float] = None,
        reduce_only: bool = False,
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
            trade_logger.info(
                "Order filled | %s | %s %s | qty=%s | reduce_only=%s",
                symbol,
                side,
                position_side,
                order_params["quantity"],
                reduce_only,
            )
            return response
        except PositionAlreadyClosedError:
            raise
        except BinanceAPIException as exc:
            if exc.code == -2022 or "reduceonly order is rejected" in str(
                exc.message
            ).lower():
                raise PositionAlreadyClosedError(
                    str(exc.message), code=int(exc.code)
                ) from exc
            error_logger.error(
                "Binance rejected order on %s: %s (code=%s)",
                symbol,
                exc.message,
                exc.code,
            )
            if self._critical_alerts:
                self._critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Order rejected on {symbol} {side} {position_side}: {exc.message}",
                    exc=exc,
                )
            raise OrderExecutionError(exc.message) from exc
        except ExchangeError as exc:
            if PositionAlreadyClosedError.matches(exc):
                raise PositionAlreadyClosedError(str(exc)) from exc
            raise OrderExecutionError(str(exc)) from exc
        except BinanceOrderException as exc:
            if self._critical_alerts:
                self._critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Order exception on {symbol}: {exc.message}",
                    exc=exc,
                )
            raise OrderExecutionError(exc.message) from exc
        except Exception as exc:
            if self._critical_alerts:
                self._critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Unexpected order failure on {symbol}: {exc}",
                    exc=exc,
                )
            raise OrderExecutionError(str(exc)) from exc

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
