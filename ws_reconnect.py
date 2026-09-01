"""
WebSocket reconnect policy and binance.ws log spam suppression.
"""

from __future__ import annotations

import logging
import threading
import time


class WsReconnectPolicy:
    """Exponential backoff for WebSocket reconnect attempts."""

    def __init__(
        self,
        min_seconds: float,
        max_seconds: float,
        max_attempts: int = 0,
    ) -> None:
        self._min = max(min_seconds, 0.5)
        self._max = max(max_seconds, self._min)
        self._max_attempts = max_attempts
        self._attempt = 0
        self._lock = threading.Lock()

    def next_delay(self) -> float:
        with self._lock:
            delay = min(self._min * (2 ** self._attempt), self._max)
            if self._max_attempts > 0:
                self._attempt = min(self._attempt + 1, self._max_attempts)
            else:
                self._attempt += 1
            return delay

    def reset(self) -> None:
        with self._lock:
            self._attempt = 0

    @property
    def attempt(self) -> int:
        with self._lock:
            return self._attempt


class WsLogSuppressor:
    """Rate-limit repeated WebSocket disconnect / reconnect log lines."""

    def __init__(self, interval_seconds: float) -> None:
        self._interval = max(interval_seconds, 15.0)
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


def is_read_loop_closed_error(exc_or_message: object) -> bool:
    text = str(exc_or_message).lower()
    needles = (
        "read loop has been closed",
        "readloopclosed",
        "connection closed",
        "connectionclosed",
        "websocket connection is closed",
    )
    return any(n in text for n in needles)


def is_ws_error_message(message: object) -> bool:
    if not isinstance(message, dict):
        return False
    if message.get("e") == "error":
        return True
    msg_type = str(message.get("type", "")).lower()
    return "readloopclosed" in msg_type or "connectionclosed" in msg_type


def configure_binance_ws_logging() -> None:
    """
    Suppress python-binance internal WS error spam.
    MarketDataHub logs disconnect/reconnect at a controlled rate instead.
    """
    spam_filter = _BinanceWsSpamFilter()
    for logger_name in (
        "binance.ws",
        "binance.ws.threaded_stream",
        "binance.ws.reconnecting_websocket",
        "binance.ws.streams",
        "binance.ws.keepalive_websocket",
    ):
        ws_log = logging.getLogger(logger_name)
        ws_log.setLevel(logging.CRITICAL)
        ws_log.propagate = False
        if not any(isinstance(f, _BinanceWsSpamFilter) for f in ws_log.filters):
            ws_log.addFilter(spam_filter)


class _BinanceWsSpamFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage().lower()
        blocked = (
            "error receiving message" in msg
            or "read loop has been closed" in msg
            or "connection closed" in msg
        )
        return not blocked
