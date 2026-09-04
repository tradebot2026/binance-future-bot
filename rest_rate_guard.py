"""
Global REST rate-limit guard for Binance Futures.
Token-bucket weight budgeting with hard-stop on 429 / -1003.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Optional

from config import Config


# Approximate Binance Futures endpoint weights (request weight units).
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
}


def weight_for_call(func: Any, default: int = 1) -> int:
    name = getattr(func, "__name__", "") or ""
    return ENDPOINT_WEIGHTS.get(name, default)


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
