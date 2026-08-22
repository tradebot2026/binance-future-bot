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


class InsufficientBalanceError(ExchangeError):
    """Account balance too low for requested operation."""
