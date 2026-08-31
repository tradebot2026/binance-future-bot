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
    MIN_REQUEST_INTERVAL_MS: int = _env_int("MIN_REQUEST_INTERVAL_MS", 1000)
    INIT_REST_DELAY_SECONDS: float = _env_float("INIT_REST_DELAY_SECONDS", 0.5)
    ENABLE_WEBSOCKET_STREAMS: bool = _env_bool("ENABLE_WEBSOCKET_STREAMS", True)
    WS_STALE_SECONDS: int = _env_int("WS_STALE_SECONDS", 120)
    WS_USER_STALE_SECONDS: int = _env_int("WS_USER_STALE_SECONDS", 120)
    WS_KLINE_BUFFER_LIMIT: int = _env_int("WS_KLINE_BUFFER_LIMIT", 320)
    SCAN_WS_ONLY: bool = _env_bool("SCAN_WS_ONLY", True)
    BOOTSTRAP_KLINE_BATCH_SIZE: int = _env_int("BOOTSTRAP_KLINE_BATCH_SIZE", 9)
    BOOK_TICKER_CACHE_SECONDS: int = _env_int("BOOK_TICKER_CACHE_SECONDS", 90)
    RATE_LIMIT_HALT_SECONDS: int = _env_int("RATE_LIMIT_HALT_SECONDS", 300)
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
    POSITION_CACHE_TTL_SECONDS: int = _env_int("POSITION_CACHE_TTL_SECONDS", 45)
    POSITION_CACHE_BACKOFF_SECONDS: int = _env_int("POSITION_CACHE_BACKOFF_SECONDS", 60)
    MIN_NOTIONAL_RISK_TOLERANCE: float = _env_float("MIN_NOTIONAL_RISK_TOLERANCE", 1.35)
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
    RETEST_ZONE_ATR_TOLERANCE: float = _env_float("RETEST_ZONE_ATR_TOLERANCE", 0.35)
    MAX_CHASE_ATR: float = _env_float("MAX_CHASE_ATR", 0.45)
    PD_LOOKBACK_BARS: int = _env_int("PD_LOOKBACK_BARS", 48)
    ALLOW_NEUTRAL_MACRO_SETUPS: bool = _env_bool("ALLOW_NEUTRAL_MACRO_SETUPS", True)
    NEUTRAL_MACRO_MIN_SCORE: float = _env_float("NEUTRAL_MACRO_MIN_SCORE", 75.0)
    ALLOW_PD_EQUILIBRIUM: bool = _env_bool("ALLOW_PD_EQUILIBRIUM", True)
    PD_EQUILIBRIUM_TOLERANCE_PCT: float = _env_float("PD_EQUILIBRIUM_TOLERANCE_PCT", 5.0)
    STRUCTURE_NEARBY_ATR_MULT: float = _env_float("STRUCTURE_NEARBY_ATR_MULT", 1.5)
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

    # ---------------- Range regime ----------------
    ENABLE_RANGE_REGIME: bool = _env_bool("ENABLE_RANGE_REGIME", True)
    MAX_RANGE_POSITIONS: int = _env_int("MAX_RANGE_POSITIONS", 4)
    RANGE_COOLDOWN_MINUTES: int = _env_int("RANGE_COOLDOWN_MINUTES", 5)
    RANGE_SIZE_MULTIPLIER: float = _env_float("RANGE_SIZE_MULTIPLIER", 0.5)
    RANGE_DAILY_MAX_LOSS_PERCENT: float = _env_float("RANGE_DAILY_MAX_LOSS_PERCENT", 3.0)
    RANGE_MAX_CONSECUTIVE_LOSSES: int = _env_int("RANGE_MAX_CONSECUTIVE_LOSSES", 2)
    RANGE_REGIME_MAX_ADX_1H: float = _env_float("RANGE_REGIME_MAX_ADX_1H", 22.0)
    RANGE_EXIT_ADX_15M: float = _env_float("RANGE_EXIT_ADX_15M", 25.0)
    RANGE_BREAKOUT_ATR_MULT: float = _env_float("RANGE_BREAKOUT_ATR_MULT", 0.3)
    RANGE_TIME_STOP_BARS: int = _env_int("RANGE_TIME_STOP_BARS", 16)
    RANGE_EDGE_ATR_TOLERANCE: float = _env_float("RANGE_EDGE_ATR_TOLERANCE", 0.35)
    RANGE_LOOKBACK_BARS: int = _env_int("RANGE_LOOKBACK_BARS", 48)
    RANGE_MIN_SCORE: float = _env_float("RANGE_MIN_SCORE", 65.0)

    # ---------------- Market filters ----------------
    MIN_24H_VOLUME_USDT: float = _env_float("MIN_24H_VOLUME_USDT", 15_000_000.0)
    MAX_SPREAD_PERCENT: float = _env_float("MAX_SPREAD_PERCENT", 0.15)
    WATCHLIST_SCORE: float = _env_float("WATCHLIST_SCORE", 70.0)
    BLACKLIST_SCORE: float = _env_float("BLACKLIST_SCORE", 40.0)

    # ---------------- Scanner ----------------
    MAX_WORKERS: int = _env_int("MAX_WORKERS", 1)
    SCAN_TIMEOUT_SEC: int = _env_int("SCAN_TIMEOUT_SEC", 120)
    SCAN_PAIR_DELAY_SECONDS: float = _env_float("SCAN_PAIR_DELAY_SECONDS", 0.25)
    SCAN_TIMEFRAME_DELAY_SECONDS: float = _env_float("SCAN_TIMEFRAME_DELAY_SECONDS", 0.05)
    CANDLE_FETCH_LIMIT: int = _env_int("CANDLE_FETCH_LIMIT", 280)

    # ---------------- Ops / VPS ----------------
    HEARTBEAT_SECONDS: int = _env_int("HEARTBEAT_SECONDS", 60)
    ENABLE_DAILY_REPORT: bool = _env_bool("ENABLE_DAILY_REPORT", True)
    ENABLE_WEEKLY_REPORT: bool = _env_bool("ENABLE_WEEKLY_REPORT", True)
    ENABLE_MONTHLY_REPORT: bool = _env_bool("ENABLE_MONTHLY_REPORT", True)
    MAX_SCAN_UNIVERSE: int = _env_int("MAX_SCAN_UNIVERSE", 80)
    MAX_ENTRIES_PER_CYCLE: int = _env_int("MAX_ENTRIES_PER_CYCLE", 3)
    MULTI_CONFLUENCE_MIN_SCORE: float = _env_float("MULTI_CONFLUENCE_MIN_SCORE", 65.0)
    NEAR_MISS_SCORE_MIN: float = _env_float("NEAR_MISS_SCORE_MIN", 65.0)
    NEAR_MISS_SCORE_MAX: float = _env_float("NEAR_MISS_SCORE_MAX", 69.0)
    NEAR_MISS_PRIORITY_MAX: int = _env_int("NEAR_MISS_PRIORITY_MAX", 25)
    DIRECTION_WIN_MARGIN: float = _env_float("DIRECTION_WIN_MARGIN", 5.0)
    DIRECTION_EQUILIBRIUM_MIN_MARGIN: float = _env_float(
        "DIRECTION_EQUILIBRIUM_MIN_MARGIN", 3.0
    )
    ENABLE_DIRECTION_EQUILIBRIUM_RESOLVE: bool = _env_bool(
        "ENABLE_DIRECTION_EQUILIBRIUM_RESOLVE", True
    )
    SYMBOL_COOLDOWN_SOFT_MINUTES: int = _env_int("SYMBOL_COOLDOWN_SOFT_MINUTES", 5)
    RANGE_COOLDOWN_SOFT_MINUTES: int = _env_int("RANGE_COOLDOWN_SOFT_MINUTES", 2)
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
