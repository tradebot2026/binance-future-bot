"""
Global REST rate-limit guard for Binance Futures.
Token-bucket weight budgeting with hard-stop on 429 / -1003.
Tracks HTTP request count and X-MBX-USED-WEIGHT-1M headers.
"""

from __future__ import annotations

import enum
import threading
import time
from collections import deque
from typing import Any, Optional

from config import Config
from logger import system_logger


# Approximate Binance USD-M Futures endpoint weights (request weight units).
ENDPOINT_WEIGHTS: dict[str, int] = {
    "futures_ping": 1,
    "futures_exchange_info": 1,
    "futures_ticker": 1,
    "futures_orderbook_ticker": 2,
    "futures_klines": 5,
    "futures_account": 5,
    "futures_position_information": 5,
    "futures_get_order": 1,
    "futures_create_order": 1,
    "futures_change_leverage": 1,
    "futures_leverage_bracket": 1,
    "futures_symbol_ticker": 1,
    "futures_get_position_mode": 1,
    "futures_change_position_mode": 1,
    "futures_mark_price": 1,
    "futures_open_interest": 1,
    "futures_open_interest_hist": 1,
    "futures_account_balance": 5,
    "futures_income_history": 30,
    "futures_account_trades": 5,
}


def weight_for_call(func: Any, default: int = 1, **kwargs: Any) -> int:
    name = getattr(func, "__name__", "") or ""
    if name == "futures_ticker" and not kwargs.get("symbol"):
        return 40
    if name == "futures_klines":
        try:
            limit = int(kwargs.get("limit") or 500)
        except (TypeError, ValueError):
            limit = 500
        if limit < 100:
            return 1
        if limit < 500:
            return 2
        if limit <= 1000:
            return 5
        return 10
    return ENDPOINT_WEIGHTS.get(name, default)


class ApiHealthState(str, enum.Enum):
    HEALTHY = "HEALTHY"
    HIGH_USAGE = "HIGH_USAGE"
    RATE_LIMIT_WARNING = "RATE_LIMIT_WARNING"
    API_RATE_LIMITED = "API_RATE_LIMITED"
    IP_BANNED = "IP_BANNED"


def _weight_thresholds() -> tuple[int, int, int]:
    """Return (throttle_at, hard_at, limit) for X-MBX-USED-WEIGHT-1M."""
    limit = Config.rest_used_weight_limit()
    throttle = Config.rest_weight_throttle_threshold()
    hard = Config.rest_weight_hard_threshold()
    return throttle, hard, limit


def _weight_pause_seconds(used_weight: int) -> float:
    """How long to pause background REST after a used-weight header."""
    throttle, hard, limit = _weight_thresholds()
    if used_weight >= limit:
        return max(float(Config.REST_WEIGHT_OVER_LIMIT_PAUSE_SECONDS), 45.0)
    if used_weight >= hard:
        return max(float(Config.REST_WEIGHT_HARD_THROTTLE_SECONDS), 8.0)
    if used_weight >= throttle:
        return max(float(Config.REST_WEIGHT_THROTTLE_SECONDS), 5.0)
    return 0.0


class RestUsageTracker:
    """
    Sliding 60s HTTP request counter + last used-weight header.
    Drives API SAFETY MODE and the used-weight REST governor.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._window: deque[float] = deque()
        self._used_weight_1m: int = 0
        self._used_weight_updated_at: float = 0.0
        self._weight_throttle_until: float = 0.0
        self._last_http_status: int = 0
        self._last_error_code: int = 0
        self._safety_until: float = 0.0
        self._state: ApiHealthState = ApiHealthState.HEALTHY
        self._reason: str = ""
        self._retry_after_seconds: float = 0.0
        self._logged_state: ApiHealthState = ApiHealthState.HEALTHY

    def requests_last_minute(self) -> int:
        self._purge()
        with self._lock:
            return len(self._window)

    def snapshot(self) -> dict[str, Any]:
        self._purge()
        with self._lock:
            remaining = max(self._safety_until - time.monotonic(), 0.0)
            state = self._effective_state_locked(remaining)
            throttle_remaining = max(self._weight_throttle_until - time.monotonic(), 0.0)
            return {
                "state": state.value,
                "reason": self._reason,
                "requests_1m": len(self._window),
                "used_weight_1m": self._used_weight_1m,
                "ip_limit": Config.rest_ip_request_limit(),
                "weight_limit": Config.rest_used_weight_limit(),
                "weight_throttle_remaining_seconds": throttle_remaining,
                "safety_remaining_seconds": remaining,
                "last_http_status": self._last_http_status,
                "last_error_code": self._last_error_code,
                "retry_after_seconds": self._retry_after_seconds,
            }

    def allows_background_rest(self) -> bool:
        snap = self.snapshot()
        return snap["state"] in {
            ApiHealthState.HEALTHY.value,
            ApiHealthState.HIGH_USAGE.value,
        }

    def allows_new_entries(self) -> bool:
        snap = self.snapshot()
        return snap["state"] in {
            ApiHealthState.HEALTHY.value,
            ApiHealthState.HIGH_USAGE.value,
        }

    def in_safety_mode(self) -> bool:
        snap = self.snapshot()
        return snap["state"] in {
            ApiHealthState.API_RATE_LIMITED.value,
            ApiHealthState.IP_BANNED.value,
        }

    def safety_remaining(self) -> float:
        with self._lock:
            return max(self._safety_until - time.monotonic(), 0.0)

    def weight_throttle_remaining(self) -> float:
        with self._lock:
            return max(self._weight_throttle_until - time.monotonic(), 0.0)

    def used_weight_1m(self) -> int:
        self._purge()
        with self._lock:
            self._decay_used_weight_locked()
            return int(self._used_weight_1m)

    def note_http_response(self, response: Any) -> None:
        """Record one HTTP round-trip (python-binance session response)."""
        now = time.monotonic()
        status = 0
        used_weight = 0
        retry_after = 0.0
        try:
            status = int(getattr(response, "status_code", 0) or 0)
        except (TypeError, ValueError):
            status = 0
        headers = getattr(response, "headers", None) or {}
        used_weight = _header_int(
            headers,
            "X-MBX-USED-WEIGHT-1M",
            "x-mbx-used-weight-1m",
        )
        retry_after = _header_float(headers, "Retry-After", "retry-after")
        with self._lock:
            self._window.append(now)
            self._last_http_status = status
            if used_weight > 0:
                self._used_weight_1m = used_weight
                self._used_weight_updated_at = now
                pause = _weight_pause_seconds(used_weight)
                if pause > 0:
                    self._weight_throttle_until = max(
                        self._weight_throttle_until, now + pause
                    )
                    throttle, hard, limit = _weight_thresholds()
                    if used_weight >= limit:
                        self._reason = (
                            f"used_weight_1m={used_weight} at/over limit {limit} "
                            f"— REST paused {int(pause)}s"
                        )
                    elif used_weight >= hard:
                        self._reason = (
                            f"used_weight_1m={used_weight} near weight cap "
                            f"(hard {hard}) — REST paused {int(pause)}s"
                        )
                    else:
                        self._reason = (
                            f"used_weight_1m={used_weight} over throttle "
                            f"{throttle} — REST paused {int(pause)}s"
                        )
            if retry_after > 0:
                self._retry_after_seconds = retry_after
            if status in (418, 429):
                halt = max(retry_after, float(Config.RATE_LIMIT_HALT_SECONDS), 180.0)
                if status == 418:
                    halt = max(halt, 600.0)
                    self._state = ApiHealthState.IP_BANNED
                    self._reason = f"HTTP {status} — IP banned / WAF"
                else:
                    self._state = ApiHealthState.API_RATE_LIMITED
                    self._reason = f"HTTP {status} — too many requests"
                self._safety_until = max(self._safety_until, now + halt)

    def note_transport_error(self, exc: BaseException) -> None:
        with self._lock:
            self._reason = f"transport error: {exc}"[:160]

    def note_binance_error(
        self,
        *,
        code: int,
        message: str,
        halt_seconds: float,
        banned: bool = False,
    ) -> None:
        now = time.monotonic()
        halt = max(float(halt_seconds), 0.0)
        with self._lock:
            self._last_error_code = int(code)
            self._reason = (message or f"Binance code {code}")[:200]
            if halt > 0:
                self._safety_until = max(self._safety_until, now + halt)
                self._retry_after_seconds = halt
            if banned or code == 418:
                self._state = ApiHealthState.IP_BANNED
            elif code in (-1003, -1015, 429):
                self._state = ApiHealthState.API_RATE_LIMITED

    def _purge(self) -> None:
        cutoff = time.monotonic() - 60.0
        with self._lock:
            while self._window and self._window[0] < cutoff:
                self._window.popleft()
            self._decay_used_weight_locked()

    def _decay_used_weight_locked(self) -> None:
        """Drop stale used-weight headers after Binance's rolling 60s window."""
        if self._used_weight_updated_at <= 0:
            return
        if time.monotonic() - self._used_weight_updated_at >= 60.0:
            self._used_weight_1m = 0
            self._used_weight_updated_at = 0.0

    def _effective_state_locked(self, remaining: float) -> ApiHealthState:
        self._decay_used_weight_locked()
        if remaining > 0 and self._state in {
            ApiHealthState.API_RATE_LIMITED,
            ApiHealthState.IP_BANNED,
        }:
            self._log_state_locked(len(self._window), Config.rest_ip_request_limit())
            return self._state
        ip_limit = max(Config.rest_ip_request_limit(), 1)
        count = len(self._window)
        throttle_at, hard_at, weight_limit = _weight_thresholds()
        used_weight = int(self._used_weight_1m)
        throttle_remaining = max(self._weight_throttle_until - time.monotonic(), 0.0)

        if used_weight >= weight_limit and throttle_remaining > 0:
            self._state = ApiHealthState.RATE_LIMIT_WARNING
            self._reason = (
                f"used_weight_1m={used_weight} at/over limit {weight_limit} "
                f"(pause {int(throttle_remaining)}s)"
            )
            self._log_state_locked(count, ip_limit)
            return self._state
        if used_weight >= throttle_at and throttle_remaining > 0:
            self._state = ApiHealthState.RATE_LIMIT_WARNING
            if not self._reason.startswith("used_weight_1m="):
                self._reason = (
                    f"used_weight_1m={used_weight} over throttle {throttle_at} "
                    f"(pause {int(throttle_remaining)}s)"
                )
            self._log_state_locked(count, ip_limit)
            return self._state
        if count >= int(ip_limit * 0.85):
            self._state = ApiHealthState.RATE_LIMIT_WARNING
            self._reason = f"{count} HTTP requests in 60s (limit {ip_limit})"
            self._log_state_locked(count, ip_limit)
            return self._state
        if used_weight >= hard_at:
            self._state = ApiHealthState.HIGH_USAGE
            self._reason = (
                f"used_weight_1m={used_weight} near weight cap {weight_limit}"
            )
            self._log_state_locked(count, ip_limit)
            return self._state
        if used_weight >= throttle_at:
            self._state = ApiHealthState.HIGH_USAGE
            self._reason = (
                f"used_weight_1m={used_weight} (throttle {throttle_at}, probe allowed)"
            )
            self._log_state_locked(count, ip_limit)
            return self._state
        if count >= int(ip_limit * 0.55):
            self._state = ApiHealthState.HIGH_USAGE
            self._reason = f"{count} HTTP requests in 60s (limit {ip_limit})"
            self._log_state_locked(count, ip_limit)
            return self._state
        self._state = ApiHealthState.HEALTHY
        if remaining <= 0 and throttle_remaining <= 0:
            self._reason = ""
        self._log_state_locked(count, ip_limit)
        return self._state

    def _log_state_locked(self, count: int, ip_limit: int) -> None:
        if self._state == self._logged_state:
            return
        self._logged_state = self._state
        system_logger.info(
            "[API_HEALTH] %s | requests_1m=%s used_weight_1m=%s ip_limit=%s | %s",
            self._state.value,
            count,
            self._used_weight_1m,
            ip_limit,
            self._reason or "ok",
        )


def _header_int(headers: Any, *keys: str) -> int:
    for key in keys:
        try:
            raw = headers.get(key)
        except Exception:
            raw = None
        if raw is None:
            continue
        try:
            return int(float(str(raw)))
        except (TypeError, ValueError):
            continue
    return 0


def _header_float(headers: Any, *keys: str) -> float:
    for key in keys:
        try:
            raw = headers.get(key)
        except Exception:
            raw = None
        if raw is None:
            continue
        try:
            return float(str(raw))
        except (TypeError, ValueError):
            continue
    return 0.0


class RestTokenBucket:
    """
    Thread-safe token bucket for Binance REST request-weight budgeting.
    Triggers a hard stop (zero tokens + sleep gate) on rate-limit violations.
    """

    def __init__(self, capacity: float, refill_per_second: float) -> None:
        self._capacity = max(capacity, 1.0)
        self._tokens = self._capacity
        self._refill_rate = max(refill_per_second, 0.01)
        self._last_refill = time.monotonic()
        self._hard_stop_until = 0.0
        self._lock = threading.Lock()

    def trigger_hard_stop(self, seconds: float) -> None:
        """Drain bucket and block all REST until cooldown elapses."""
        pause = max(seconds, 0.0)
        with self._lock:
            self._hard_stop_until = max(
                self._hard_stop_until, time.monotonic() + pause
            )
            self._tokens = 0.0

    def hard_stop_remaining(self) -> float:
        with self._lock:
            return max(self._hard_stop_until - time.monotonic(), 0.0)

    def is_hard_stopped(self) -> bool:
        return self.hard_stop_remaining() > 0.0

    def acquire(self, weight: int = 1) -> None:
        """Block until `weight` tokens are available (never spin-retry on ban)."""
        weight = max(weight, 1)
        while True:
            with self._lock:
                now = time.monotonic()
                stop_remaining = self._hard_stop_until - now
                if stop_remaining > 0:
                    wait = stop_remaining
                else:
                    elapsed = now - self._last_refill
                    if elapsed > 0:
                        self._tokens = min(
                            self._capacity,
                            self._tokens + elapsed * self._refill_rate,
                        )
                        self._last_refill = now
                    if self._tokens >= weight:
                        self._tokens -= weight
                        return
                    deficit = weight - self._tokens
                    wait = deficit / self._refill_rate
            time.sleep(min(max(wait, 0.05), 30.0))


class RestBlockLogSuppressor:
    """Emit repeated REST-block warnings at most once per interval."""

    def __init__(self, interval_seconds: int) -> None:
        self._interval = max(interval_seconds, 30)
        self._last_logged_at = 0.0
        self._last_reason = ""
        self._lock = threading.Lock()

    def should_log(self, reason: str) -> bool:
        now = time.monotonic()
        with self._lock:
            if (
                now - self._last_logged_at < self._interval
                and reason == self._last_reason
            ):
                return False
            self._last_logged_at = now
            self._last_reason = reason
            return True

    def reset(self) -> None:
        with self._lock:
            self._last_logged_at = 0.0
            self._last_reason = ""


def build_default_token_bucket() -> RestTokenBucket:
    per_minute = max(Config.REST_BUDGET_WEIGHT_PER_MINUTE, 1)
    return RestTokenBucket(
        capacity=float(min(Config.REST_TOKEN_BUCKET_CAPACITY, per_minute)),
        refill_per_second=min(
            Config.REST_TOKEN_REFILL_PER_SECOND, per_minute / 60.0
        ),
    )
