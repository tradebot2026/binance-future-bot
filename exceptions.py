"""Custom exception hierarchy for the trading bot."""

from __future__ import annotations


class TradingBotError(Exception):
    """Base exception for all bot errors."""


class ConfigurationError(TradingBotError):
    """Invalid or missing configuration."""


class DatabaseError(TradingBotError):
    """SQLite persistence failures."""


class ExchangeError(TradingBotError):
    """Binance API communication failures."""


class ExchangeRateLimitError(ExchangeError):
    """Rate limit or IP ban risk from excessive requests."""


class OrderExecutionError(ExchangeError):
    """Order rejected or failed validation."""


class PositionAlreadyClosedError(OrderExecutionError):
    """Reduce-only close rejected — no open position on the exchange (Binance -2022)."""

    def __init__(self, message: str, *, code: int = -2022) -> None:
        super().__init__(message)
        self.code = code

    @classmethod
    def matches(cls, exc: BaseException) -> bool:
        if isinstance(exc, cls):
            return True
        if isinstance(exc, OrderExecutionError):
            text = str(exc).lower()
            if "reduceonly order is rejected" in text:
                return True
        text = str(exc).lower()
        return "reduceonly order is rejected" in text or "code=-2022" in text


class InsufficientBalanceError(ExchangeError):
    """Account balance too low for requested operation."""
