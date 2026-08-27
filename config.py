"""
Central configuration module.
Loads environment variables and exposes typed settings.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()


def _env_bool(key: str, default: bool) -> bool:
    return os.getenv(key, str(default)).lower() in ("true", "1", "t", "yes")


def _env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def _env_float(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


class Config:
    """Runtime configuration loaded from environment variables."""

    # ---------------- API & network ----------------
    BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
    BINANCE_API_SECRET: str = os.getenv("BINANCE_API_SECRET", "")
    USE_TESTNET: bool = _env_bool("USE_TESTNET", True)
    REQUEST_TIMEOUT: int = _env_int("REQUEST_TIMEOUT", 15)
    MAX_RETRIES: int = _env_int("MAX_RETRIES", 5)
    MIN_REQUEST_INTERVAL_MS: int = _env_int("MIN_REQUEST_INTERVAL_MS", 100)
    ENABLE_STRICT_RATE_LIMIT: bool = _env_bool("ENABLE_STRICT_RATE_LIMIT", True)
    API_BACKOFF_MAX_SECONDS: int = _env_int("API_BACKOFF_MAX_SECONDS", 60)
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    TIMEZONE: str = os.getenv("TIMEZONE", "UTC")

    # ---------------- Telegram ----------------
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # ---------------- Trading ----------------
    QUOTE_ASSET: str = os.getenv("QUOTE_ASSET", "USDT")
    ENTRY_TIMEFRAME: str = os.getenv("ENTRY_TIMEFRAME", "5m")
    CONFIRM_TIMEFRAME: str = os.getenv("CONFIRM_TIMEFRAME", "15m")
    TREND_TIMEFRAME: str = os.getenv("TREND_TIMEFRAME", "1h")
    TIMEFRAMES: list[str] = [
        os.getenv("ENTRY_TIMEFRAME", "5m"),
        os.getenv("TREND_TIMEFRAME", "1h"),
    ]
    SCAN_INTERVAL_SECONDS: int = _env_int("SCAN_INTERVAL_SECONDS", 15)
    MONITOR_INTERVAL_SECONDS: int = _env_int("MONITOR_INTERVAL_SECONDS", 7)
    RECONCILIATION_INTERVAL_SECONDS: int = _env_int("RECONCILIATION_INTERVAL_SECONDS", 900)
    MAX_POSITIONS: int = _env_int("MAX_OPEN_POSITIONS", 12)
    MAX_LEVERAGE: int = _env_int("MAX_LEVERAGE", 25)
    AUTO_DETECT_MAX_LEVERAGE: bool = _env_bool("AUTO_DETECT_MAX_LEVERAGE", True)

    # ---------------- Risk ----------------
    RISK_PER_TRADE_PERCENT: float = _env_float("RISK_PER_TRADE_PERCENT", 2.0)
    DAILY_TARGET_PERCENT: float = _env_float("DAILY_TARGET_PERCENT", 20.0)
    DAILY_STOP_PERCENT: float = _env_float("DAILY_STOP_PERCENT", 10.0)
    MAX_DAILY_TRADES: int = _env_int("MAX_DAILY_TRADES", 40)
    MAX_CONSECUTIVE_LOSSES: int = _env_int("MAX_CONSECUTIVE_LOSSES", 5)
    MAX_ACCOUNT_DRAWDOWN: float = _env_float("MAX_ACCOUNT_DRAWDOWN", 20.0)
    MAX_POSITION_VALUE_MULTIPLIER: float = _env_float("MAX_POSITION_VALUE_MULTIPLIER", 1.5)
    BALANCE_CACHE_TTL_SECONDS: int = _env_int("BALANCE_CACHE_TTL_SECONDS", 45)
    RESTART_DELAY_SECONDS: int = _env_int("RESTART_DELAY_SECONDS", 10)
    MAX_RESTART_DELAY_SECONDS: int = _env_int("MAX_RESTART_DELAY_SECONDS", 120)
    MAX_AUTO_RESTARTS: int = _env_int("MAX_AUTO_RESTARTS", 0)
    ERROR_ALERT_THRESHOLD: int = _env_int("ERROR_ALERT_THRESHOLD", 3)
    CRITICAL_ALERT_COOLDOWN_SECONDS: int = _env_int("CRITICAL_ALERT_COOLDOWN_SECONDS", 300)

    # ---------------- Database maintenance ----------------
    DB_RETENTION_DAYS: int = _env_int("DB_RETENTION_DAYS", 30)
    DB_MAINTENANCE_INTERVAL_SECONDS: int = _env_int("DB_MAINTENANCE_INTERVAL_SECONDS", 86400)

    # ---------------- ATR exits ----------------
    SL_ATR_MULTIPLIER: float = _env_float("SL_ATR_MULTIPLIER", 2.0)
    TP1_ATR_MULTIPLIER: float = _env_float("TP1_ATR_MULTIPLIER", 1.5)
    TP2_ATR_MULTIPLIER: float = _env_float("TP2_ATR_MULTIPLIER", 2.5)
    TP3_ATR_MULTIPLIER: float = _env_float("TP3_ATR_MULTIPLIER", 4.0)
    ENABLE_BREAK_EVEN: bool = _env_bool("ENABLE_BREAK_EVEN", True)
    ENABLE_TRAILING_STOP: bool = _env_bool("ENABLE_TRAILING_STOP", True)
    ENABLE_PARTIAL_TP: bool = _env_bool("ENABLE_PARTIAL_TP", True)

    # ---------------- Strategy / SMC ----------------
    STRATEGY_MIN_SCORE: float = _env_float("STRATEGY_MIN_SCORE", 70.0)
    SCORE_FULL_SIZE: float = _env_float("SCORE_FULL_SIZE", 80.0)
    HALF_SIZE_MULTIPLIER: float = _env_float("HALF_SIZE_MULTIPLIER", 0.5)
    VOLUME_BONUS_POINTS: float = _env_float("VOLUME_BONUS_POINTS", 5.0)
    RETEST_LOOKBACK_BARS: int = _env_int("RETEST_LOOKBACK_BARS", 12)
    RETEST_ZONE_ATR_TOLERANCE: float = _env_float("RETEST_ZONE_ATR_TOLERANCE", 0.25)
    MAX_CHASE_ATR: float = _env_float("MAX_CHASE_ATR", 0.35)
    PD_LOOKBACK_BARS: int = _env_int("PD_LOOKBACK_BARS", 48)
    SCORE_HIGH_CONFIDENCE: float = _env_float("HIGH_CONFIDENCE_SCORE", 90.0)
    SCORE_MEDIUM_CONFIDENCE: float = _env_float("MEDIUM_CONFIDENCE_SCORE", 80.0)
    SCORE_LOW_CONFIDENCE: float = _env_float("LOW_CONFIDENCE_SCORE", 70.0)

    # ---------------- R:R & structural exits ----------------
    SL_BUFFER_ATR: float = _env_float("SL_BUFFER_ATR", 0.15)
    MAX_SL_ATR_MULTIPLIER: float = _env_float("MAX_SL_ATR_MULTIPLIER", 2.0)
    TP1_R_MULTIPLE: float = _env_float("TP1_R_MULTIPLE", 1.0)
    TP2_R_MULTIPLE: float = _env_float("TP2_R_MULTIPLE", 2.0)
    TP3_R_MULTIPLE: float = _env_float("TP3_R_MULTIPLE", 3.5)
    MIN_OPPOSING_RR: float = _env_float("MIN_OPPOSING_RR", 1.5)
    SYMBOL_COOLDOWN_MINUTES: int = _env_int("SYMBOL_COOLDOWN_MINUTES", 20)

    # ---------------- Market filters ----------------
    MIN_24H_VOLUME_USDT: float = _env_float("MIN_24H_VOLUME_USDT", 15_000_000.0)
    MAX_SPREAD_PERCENT: float = _env_float("MAX_SPREAD_PERCENT", 0.15)
    WATCHLIST_SCORE: float = _env_float("WATCHLIST_SCORE", 70.0)
    BLACKLIST_SCORE: float = _env_float("BLACKLIST_SCORE", 40.0)

    # ---------------- Scanner ----------------
    MAX_WORKERS: int = _env_int("MAX_WORKERS", 3)
    SCAN_TIMEOUT_SEC: int = _env_int("SCAN_TIMEOUT_SEC", 45)
    CANDLE_FETCH_LIMIT: int = _env_int("CANDLE_FETCH_LIMIT", 300)

    # ---------------- Ops / VPS ----------------
    HEARTBEAT_SECONDS: int = _env_int("HEARTBEAT_SECONDS", 60)
    ENABLE_DAILY_REPORT: bool = _env_bool("ENABLE_DAILY_REPORT", True)
    ENABLE_WEEKLY_REPORT: bool = _env_bool("ENABLE_WEEKLY_REPORT", True)
    ENABLE_MONTHLY_REPORT: bool = _env_bool("ENABLE_MONTHLY_REPORT", True)
    MAX_SCAN_UNIVERSE: int = _env_int("MAX_SCAN_UNIVERSE", 60)
    MAX_ENTRIES_PER_CYCLE: int = _env_int("MAX_ENTRIES_PER_CYCLE", 2)
    DATA_DIR: str = "data"
    LOGS_DIR: str = "logs"
    REPORTS_DIR: str = "reports"
    DB_PATH: str = os.path.join(
        DATA_DIR,
        os.getenv("DATABASE_NAME", "trading_bot.sqlite"),
    )

    @classmethod
    def setup_directories(cls) -> None:
        for directory in (cls.DATA_DIR, cls.LOGS_DIR, cls.REPORTS_DIR):
            os.makedirs(directory, exist_ok=True)

    @classmethod
    def validate_config(cls) -> bool:
        valid = True
        placeholders = {"your_binance_api_key", "your_binance_api_secret", ""}
        if cls.BINANCE_API_KEY in placeholders or cls.BINANCE_API_SECRET in placeholders:
            valid = False
        if cls.MAX_POSITIONS <= 0:
            valid = False
        if cls.SCAN_INTERVAL_SECONDS <= 0:
            valid = False
        return valid


Config.setup_directories()
