"""
Central configuration module.
Loads environment variables and exposes typed settings.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("true", "1", "t", "yes")


def _env_use_testnet() -> bool:
    """
    Testnet is the default. Live mainnet requires BOTH:
      BINANCE_ENV=MAINNET (or LIVE/PROD) and USE_TESTNET=false.
    BINANCE_ENV=TESTNET always wins so a coding mistake cannot go live.
    """
    env = str(os.getenv("BINANCE_ENV", "") or "").strip().upper()
    if env in ("TESTNET", "TEST", "PAPER"):
        return True
    if env in ("MAINNET", "LIVE", "PROD", "PRODUCTION"):
        raw = os.getenv("USE_TESTNET")
        if raw is None or not str(raw).strip():
            return True
        return str(raw).strip().lower() in ("true", "1", "t", "yes")
    raw = os.getenv("USE_TESTNET")
    if raw is None or not str(raw).strip():
        return True
    return str(raw).strip().lower() in ("true", "1", "t", "yes")


def _env_dry_run() -> bool:
    """
    Explicit DRY_RUN wins. If missing/empty: False on testnet (live test orders),
    True on mainnet (fail-closed — no live money without an explicit flag).
    Unrecognized values stay True.
    """
    raw = os.getenv("DRY_RUN")
    if raw is None or not str(raw).strip():
        return not _env_use_testnet()
    value = str(raw).strip().lower()
    if value in ("false", "0", "f", "no"):
        return False
    if value in ("true", "1", "t", "yes"):
        return True
    return True


def _env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def _env_float(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


def _env_csv_list(key: str, default: list[str]) -> list[str]:
    raw = os.getenv(key)
    if not raw:
        return [item.strip().upper() for item in default if item.strip()]
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


class Config:
    """Runtime configuration loaded from environment variables."""

    # ---------------- API & network ----------------
    BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
    BINANCE_API_SECRET: str = os.getenv("BINANCE_API_SECRET", "")
    USE_TESTNET: bool = _env_use_testnet()
    BINANCE_ENV: str = "TESTNET" if _env_use_testnet() else "MAINNET"
    DRY_RUN: bool = _env_dry_run()
    ALLOW_MAINNET_FORCE_RESUME: bool = _env_bool(
        "ALLOW_MAINNET_FORCE_RESUME", False
    )
    REQUEST_TIMEOUT: int = _env_int("REQUEST_TIMEOUT", 15)
    MAX_RETRIES: int = _env_int("MAX_RETRIES", 5)
    MIN_REQUEST_INTERVAL_MS: int = _env_int("MIN_REQUEST_INTERVAL_MS", 1000)
    EXECUTION_MIN_REQUEST_INTERVAL_MS: int = _env_int(
        "EXECUTION_MIN_REQUEST_INTERVAL_MS", 200
    )
    INIT_REST_DELAY_SECONDS: float = _env_float("INIT_REST_DELAY_SECONDS", 0.5)
    ENABLE_WEBSOCKET_STREAMS: bool = _env_bool("ENABLE_WEBSOCKET_STREAMS", True)
    WS_STALE_SECONDS: int = _env_int("WS_STALE_SECONDS", 30)
    WS_STALE_SECONDS_TESTNET: int = _env_int("WS_STALE_SECONDS_TESTNET", 180)
    WS_STALE_RECONNECT_COOLDOWN_SECONDS: float = _env_float(
        "WS_STALE_RECONNECT_COOLDOWN_SECONDS", 180.0
    )
    WS_PING_INTERVAL_SECONDS: float = _env_float("WS_PING_INTERVAL_SECONDS", 15.0)
    WS_PING_TIMEOUT_SECONDS: float = _env_float("WS_PING_TIMEOUT_SECONDS", 10.0)
    WS_USER_STALE_SECONDS: int = _env_int("WS_USER_STALE_SECONDS", 120)
    WS_USER_IDLE_RECONNECT_SECONDS: int = _env_int(
        "WS_USER_IDLE_RECONNECT_SECONDS", 1800
    )
    WS_WARMUP_SECONDS: int = _env_int("WS_WARMUP_SECONDS", 120)
    WS_STARTUP_WAIT_SECONDS: int = _env_int("WS_STARTUP_WAIT_SECONDS", 45)
    TICKER_REST_FALLBACK_AFTER_SECONDS: float = _env_float(
        "TICKER_REST_FALLBACK_AFTER_SECONDS", 60.0
    )
    TICKER_REST_MIN_INTERVAL_SECONDS: float = _env_float(
        "TICKER_REST_MIN_INTERVAL_SECONDS", 60.0
    )
    WS_RECONNECT_JOIN_TIMEOUT_SECONDS: float = _env_float(
        "WS_RECONNECT_JOIN_TIMEOUT_SECONDS", 2.0
    )
    WS_SHUTDOWN_JOIN_TIMEOUT_SECONDS: float = _env_float(
        "WS_SHUTDOWN_JOIN_TIMEOUT_SECONDS", 10.0
    )
    STARTUP_TICKER_REST_SEED: bool = _env_bool("STARTUP_TICKER_REST_SEED", True)
    STARTUP_BALANCE_MAX_ATTEMPTS: int = _env_int("STARTUP_BALANCE_MAX_ATTEMPTS", 5)
    STARTUP_BALANCE_RETRY_SECONDS: float = _env_float(
        "STARTUP_BALANCE_RETRY_SECONDS", 3.0
    )
    WS_RECONNECT_ENABLED: bool = _env_bool("WS_RECONNECT_ENABLED", True)
    WS_RECONNECT_MIN_SECONDS: float = _env_float("WS_RECONNECT_MIN_SECONDS", 5.0)
    WS_RECONNECT_MAX_SECONDS: float = _env_float("WS_RECONNECT_MAX_SECONDS", 30.0)
    WS_RECONNECT_LOG_INTERVAL_SECONDS: int = _env_int(
        "WS_RECONNECT_LOG_INTERVAL_SECONDS", 60
    )
    WS_HEALTH_CHECK_SECONDS: int = _env_int("WS_HEALTH_CHECK_SECONDS", 10)
    WS_RECONNECT_DEBOUNCE_SECONDS: float = _env_float(
        "WS_RECONNECT_DEBOUNCE_SECONDS", 5.0
    )
    WS_KLINE_BUFFER_LIMIT: int = _env_int("WS_KLINE_BUFFER_LIMIT", 320)
    WS_KLINE_MAX_STREAMS_PER_SOCKET: int = _env_int(
        "WS_KLINE_MAX_STREAMS_PER_SOCKET", 100
    )
    WS_KLINE_LIVE_TIMEFRAMES_ONLY: bool = _env_bool(
        "WS_KLINE_LIVE_TIMEFRAMES_ONLY", True
    )
    WS_KLINE_WS_TIMEFRAMES: str = os.getenv("WS_KLINE_WS_TIMEFRAMES", "")
    WS_KLINE_SOCKET_STALE_SECONDS: int = _env_int(
        "WS_KLINE_SOCKET_STALE_SECONDS", 600
    )
    SCAN_WS_ONLY: bool = _env_bool("SCAN_WS_ONLY", True)
    BOOTSTRAP_KLINE_BATCH_SIZE: int = _env_int("BOOTSTRAP_KLINE_BATCH_SIZE", 9)
    BOOK_TICKER_CACHE_SECONDS: int = _env_int("BOOK_TICKER_CACHE_SECONDS", 90)
    RATE_LIMIT_HALT_SECONDS: int = _env_int("RATE_LIMIT_HALT_SECONDS", 300)
    RATE_LIMIT_SOFT_HALT_SECONDS: int = _env_int("RATE_LIMIT_SOFT_HALT_SECONDS", 180)
    REST_IP_REQUEST_LIMIT_MAINNET: int = _env_int("REST_IP_REQUEST_LIMIT_MAINNET", 2400)
    REST_IP_REQUEST_LIMIT_TESTNET: int = _env_int("REST_IP_REQUEST_LIMIT_TESTNET", 6000)
    ENABLE_STRICT_RATE_LIMIT: bool = _env_bool("ENABLE_STRICT_RATE_LIMIT", True)
    API_BACKOFF_MAX_SECONDS: int = _env_int("API_BACKOFF_MAX_SECONDS", 60)
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    TIMEZONE: str = os.getenv("TIMEZONE", "UTC")

    # ---------------- Telegram ----------------
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    TELEGRAM_ERROR_LOG_MAX_AGE_HOURS: float = _env_float(
        "TELEGRAM_ERROR_LOG_MAX_AGE_HOURS", 48.0
    )

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
    POSITION_GRACE_PERIOD_SECONDS: float = _env_float(
        "POSITION_GRACE_PERIOD_SECONDS", 45.0
    )
    POSITION_RECONCILE_MISS_THRESHOLD: int = _env_int(
        "POSITION_RECONCILE_MISS_THRESHOLD", 3
    )
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
    BALANCE_CACHE_TTL_SECONDS: int = _env_int("BALANCE_CACHE_TTL_SECONDS", 600)
    BALANCE_REST_POLL_SECONDS: int = _env_int("BALANCE_REST_POLL_SECONDS", 600)
    ACCOUNT_REST_MIN_INTERVAL_SECONDS: int = _env_int(
        "ACCOUNT_REST_MIN_INTERVAL_SECONDS", 300
    )
    IP_BAN_HALT_SECONDS: int = _env_int("IP_BAN_HALT_SECONDS", 600)
    REST_BUDGET_ACCOUNT_RESERVE_FRACTION: float = _env_float(
        "REST_BUDGET_ACCOUNT_RESERVE_FRACTION", 0.15
    )
    POSITION_CACHE_TTL_SECONDS: int = _env_int("POSITION_CACHE_TTL_SECONDS", 300)
    POSITION_CACHE_BACKOFF_SECONDS: int = _env_int("POSITION_CACHE_BACKOFF_SECONDS", 600)
    REST_TOKEN_BUCKET_CAPACITY: int = _env_int("REST_TOKEN_BUCKET_CAPACITY", 50)
    REST_TOKEN_REFILL_PER_SECOND: float = _env_float(
        "REST_TOKEN_REFILL_PER_SECOND", 0.8
    )
    REST_BAN_MIN_SLEEP_SECONDS: int = _env_int("REST_BAN_MIN_SLEEP_SECONDS", 600)
    REST_BLOCK_LOG_INTERVAL_SECONDS: int = _env_int(
        "REST_BLOCK_LOG_INTERVAL_SECONDS", 600
    )
    REST_NETWORK_MAX_RETRIES: int = _env_int("REST_NETWORK_MAX_RETRIES", 1)
    ENABLE_REST_KLINE_BOOTSTRAP: bool = _env_bool("ENABLE_REST_KLINE_BOOTSTRAP", False)
    ENABLE_WS_KLINE_STARTUP_BOOTSTRAP: bool = _env_bool(
        "ENABLE_WS_KLINE_STARTUP_BOOTSTRAP", True
    )
    WS_KLINE_BOOTSTRAP_MIN_BARS: int = _env_int("WS_KLINE_BOOTSTRAP_MIN_BARS", 250)
    WS_KLINE_BOOTSTRAP_CONCURRENCY: int = _env_int(
        "WS_KLINE_BOOTSTRAP_CONCURRENCY", 1
    )
    WS_KLINE_BOOTSTRAP_REQUEST_TIMEOUT_SECONDS: float = _env_float(
        "WS_KLINE_BOOTSTRAP_REQUEST_TIMEOUT_SECONDS", 3.0
    )
    WS_KLINE_BOOTSTRAP_OVERALL_TIMEOUT_SECONDS: float = _env_float(
        "WS_KLINE_BOOTSTRAP_OVERALL_TIMEOUT_SECONDS", 90.0
    )
    WS_KLINE_BOOTSTRAP_REST_DELAY_SECONDS: float = _env_float(
        "WS_KLINE_BOOTSTRAP_REST_DELAY_SECONDS", 1.0
    )
    KLINE_REST_MIN_INTERVAL_SECONDS: float = _env_float(
        "KLINE_REST_MIN_INTERVAL_SECONDS", 1.0
    )
    KLINE_BOOTSTRAP_BATCH_SYMBOLS: int = _env_int("KLINE_BOOTSTRAP_BATCH_SYMBOLS", 5)
    KLINE_BOOTSTRAP_BATCH_COOLDOWN_SECONDS: float = _env_float(
        "KLINE_BOOTSTRAP_BATCH_COOLDOWN_SECONDS", 3.0
    )
    KLINE_BOOTSTRAP_INTER_REQUEST_DELAY_SECONDS: float = _env_float(
        "KLINE_BOOTSTRAP_INTER_REQUEST_DELAY_SECONDS", 0.3
    )
    POSITION_REST_VERIFY_MIN_INTERVAL_SECONDS: float = _env_float(
        "POSITION_REST_VERIFY_MIN_INTERVAL_SECONDS", 30.0
    )
    POSITION_REST_FULL_MIN_INTERVAL_SECONDS: float = _env_float(
        "POSITION_REST_FULL_MIN_INTERVAL_SECONDS", 120.0
    )
    WS_KLINE_BOOTSTRAP_WARMUP_SECONDS: float = _env_float(
        "WS_KLINE_BOOTSTRAP_WARMUP_SECONDS", 3.0
    )
    ENABLE_PACED_KLINE_BOOTSTRAP: bool = _env_bool("ENABLE_PACED_KLINE_BOOTSTRAP", True)
    KLINE_BOOTSTRAP_MAX_SERIES_PER_MINUTE: int = _env_int(
        "KLINE_BOOTSTRAP_MAX_SERIES_PER_MINUTE", 12
    )
    UNCERTAIN_ORDER_RECONCILE_SECONDS: float = _env_float(
        "UNCERTAIN_ORDER_RECONCILE_SECONDS", 30.0
    )
    ENABLE_REST_TICKER_FALLBACK: bool = _env_bool("ENABLE_REST_TICKER_FALLBACK", True)
    ENABLE_REST_BALANCE_POLL: bool = _env_bool("ENABLE_REST_BALANCE_POLL", False)
    ENABLE_REST_POSITION_POLL: bool = _env_bool("ENABLE_REST_POSITION_POLL", False)
    ENABLE_REST_PRICE_FALLBACK: bool = _env_bool("ENABLE_REST_PRICE_FALLBACK", False)
    DEFER_REST_UNTIL_WS_READY: bool = _env_bool("DEFER_REST_UNTIL_WS_READY", True)
    BACKGROUND_WS_ONLY: bool = _env_bool("BACKGROUND_WS_ONLY", True)
    USE_WS_BOOK_PROXY: bool = _env_bool("USE_WS_BOOK_PROXY", True)
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
    TP1_ATR_MULTIPLIER: float = _env_float("TP1_ATR_MULTIPLIER", 2.0)
    TP2_ATR_MULTIPLIER: float = _env_float("TP2_ATR_MULTIPLIER", 4.0)
    TP3_ATR_MULTIPLIER: float = _env_float("TP3_ATR_MULTIPLIER", 6.0)
    TP_MIN_SPACING_ATR: float = _env_float("TP_MIN_SPACING_ATR", 0.75)
    USE_DYNAMIC_TP_LADDER: bool = _env_bool("USE_DYNAMIC_TP_LADDER", True)
    ENABLE_TP3_RUNNER: bool = _env_bool("ENABLE_TP3_RUNNER", True)
    RUNNER_TRAIL_ATR_MULTIPLIER: float = _env_float("RUNNER_TRAIL_ATR_MULTIPLIER", 1.5)
    ENABLE_BREAK_EVEN: bool = _env_bool("ENABLE_BREAK_EVEN", True)
    ENABLE_TRAILING_STOP: bool = _env_bool("ENABLE_TRAILING_STOP", True)
    ENABLE_PARTIAL_TP: bool = _env_bool("ENABLE_PARTIAL_TP", True)
    ENABLE_NATIVE_TP_SL: bool = _env_bool("ENABLE_NATIVE_TP_SL", False)
    ENABLE_SOFT_TP_SL: bool = _env_bool("ENABLE_SOFT_TP_SL", True)
    VIRTUAL_TP_TICKER_MAX_AGE_SECONDS: float = _env_float(
        "VIRTUAL_TP_TICKER_MAX_AGE_SECONDS", 15.0
    )
    MONITOR_LOOP_STALL_SECONDS: float = _env_float("MONITOR_LOOP_STALL_SECONDS", 5.0)
    MONITOR_WATCHDOG_INTERVAL_SECONDS: float = _env_float(
        "MONITOR_WATCHDOG_INTERVAL_SECONDS", 5.0
    )
    MONITOR_REST_MARK_INTERVAL_SECONDS: float = _env_float(
        "MONITOR_REST_MARK_INTERVAL_SECONDS", 8.0
    )
    NATIVE_TP_WORKING_TYPE: str = os.getenv("NATIVE_TP_WORKING_TYPE", "MARK_PRICE")
    CONFLICT_REJECT_LOG_INTERVAL_SECONDS: int = _env_int(
        "CONFLICT_REJECT_LOG_INTERVAL_SECONDS", 300
    )

    # ---------------- Strategy / SMC ----------------
    STRATEGY_MIN_SCORE: float = _env_float("STRATEGY_MIN_SCORE", 70.0)
    SCORE_FULL_SIZE: float = _env_float("SCORE_FULL_SIZE", 80.0)
    HALF_SIZE_MULTIPLIER: float = _env_float("HALF_SIZE_MULTIPLIER", 0.5)
    VOLUME_BONUS_POINTS: float = _env_float("VOLUME_BONUS_POINTS", 5.0)
    RETEST_LOOKBACK_BARS: int = _env_int("RETEST_LOOKBACK_BARS", 12)
    RETEST_ZONE_ATR_TOLERANCE: float = _env_float("RETEST_ZONE_ATR_TOLERANCE", 0.35)
    MAX_CHASE_ATR: float = _env_float("MAX_CHASE_ATR", 0.45)
    PD_LOOKBACK_BARS: int = _env_int("PD_LOOKBACK_BARS", 48)
    ALLOW_NEUTRAL_MACRO_SETUPS: bool = _env_bool("ALLOW_NEUTRAL_MACRO_SETUPS", False)
    REQUIRE_BULLISH_MTF_FOR_LONG: bool = _env_bool("REQUIRE_BULLISH_MTF_FOR_LONG", True)
    REQUIRE_BEARISH_MTF_FOR_SHORT: bool = _env_bool("REQUIRE_BEARISH_MTF_FOR_SHORT", True)
    ENABLE_MOMENTUM_CRASH_VETO: bool = _env_bool("ENABLE_MOMENTUM_CRASH_VETO", True)
    CRASH_VETO_DROP_PCT_5M: float = _env_float("CRASH_VETO_DROP_PCT_5M", 1.5)
    CRASH_VETO_DROP_PCT_15M: float = _env_float("CRASH_VETO_DROP_PCT_15M", 1.5)
    CRASH_BEARISH_BODY_ATR_MULT: float = _env_float("CRASH_BEARISH_BODY_ATR_MULT", 1.75)
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
    ENABLE_TP1_RR_OPTIMIZER: bool = _env_bool("ENABLE_TP1_RR_OPTIMIZER", True)
    MIN_TP1_RR_ACCEPT: float = _env_float("MIN_TP1_RR_ACCEPT", 0.75)
    MIN_TP1_RISK_REWARD: float = _env_float("MIN_TP1_RISK_REWARD", 1.0)
    MIN_SL_ATR_NOISE: float = _env_float("MIN_SL_ATR_NOISE", 0.35)
    SYMBOL_COOLDOWN_MINUTES: int = _env_int("SYMBOL_COOLDOWN_MINUTES", 60)
    POST_TRADE_COOLDOWN_MINUTES: int = _env_int("POST_TRADE_COOLDOWN_MINUTES", 60)
    ENTRY_IN_FLIGHT_TTL_SECONDS: float = _env_float("ENTRY_IN_FLIGHT_TTL_SECONDS", 60.0)

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
    LSC_MIN_SCORE: float = _env_float("LSC_MIN_SCORE", 68.0)
    VWAP_MIN_SCORE: float = _env_float("VWAP_MIN_SCORE", 70.0)
    VWAP_MAX_DISTANCE_ATR: float = _env_float("VWAP_MAX_DISTANCE_ATR", 0.6)
    VWAP_MAX_DISTANCE_ATR_TESTNET: float = _env_float(
        "VWAP_MAX_DISTANCE_ATR_TESTNET", 1.25
    )
    VPB_MIN_SCORE: float = _env_float("VPB_MIN_SCORE", 72.0)
    VEMR_MIN_SCORE: float = _env_float("VEMR_MIN_SCORE", 65.0)
    BREAKOUT_RETEST_MIN_SCORE: float = _env_float("BREAKOUT_RETEST_MIN_SCORE", 70.0)
    FALSE_BREAKOUT_SFP_MIN_SCORE: float = _env_float("FALSE_BREAKOUT_SFP_MIN_SCORE", 72.0)
    VOL_SQUEEZE_MIN_SCORE: float = _env_float("VOL_SQUEEZE_MIN_SCORE", 70.0)
    TREND_MOMENTUM_MIN_SCORE: float = _env_float("TREND_MOMENTUM_MIN_SCORE", 70.0)
    PRICE_ACTION_REVERSAL_MIN_SCORE: float = _env_float(
        "PRICE_ACTION_REVERSAL_MIN_SCORE", 68.0
    )

    # ---------------- Multi-strategy modular system ----------------
    ENABLE_STRATEGY_SMC: bool = _env_bool("ENABLE_STRATEGY_SMC", True)
    ENABLE_STRATEGY_RANGE: bool = _env_bool("ENABLE_STRATEGY_RANGE", True)
    ENABLE_STRATEGY_LIQUIDITY_SWEEP: bool = _env_bool(
        "ENABLE_STRATEGY_LIQUIDITY_SWEEP", True
    )
    ENABLE_STRATEGY_VWAP_PULLBACK: bool = _env_bool(
        "ENABLE_STRATEGY_VWAP_PULLBACK", True
    )
    ENABLE_STRATEGY_VP_BREAKOUT: bool = _env_bool("ENABLE_STRATEGY_VP_BREAKOUT", True)
    ENABLE_STRATEGY_VOL_EXPANSION: bool = _env_bool(
        "ENABLE_STRATEGY_VOL_EXPANSION", True
    )
    # Valid Phase-2 price-action engines (closed-bar evaluation)
    ENABLE_STRATEGY_BREAKOUT_RETEST: bool = _env_bool(
        "ENABLE_STRATEGY_BREAKOUT_RETEST", True
    )
    ENABLE_STRATEGY_TREND_MOMENTUM: bool = _env_bool(
        "ENABLE_STRATEGY_TREND_MOMENTUM", True
    )
    ENABLE_STRATEGY_VOL_SQUEEZE: bool = _env_bool("ENABLE_STRATEGY_VOL_SQUEEZE", True)
    ENABLE_STRATEGY_FALSE_BREAKOUT_SFP: bool = _env_bool(
        "ENABLE_STRATEGY_FALSE_BREAKOUT_SFP", True
    )
    ENABLE_STRATEGY_PRICE_ACTION_REVERSAL: bool = _env_bool(
        "ENABLE_STRATEGY_PRICE_ACTION_REVERSAL", True
    )
    # Context / REST-dependent — stay disabled as standalone entry strategies
    ENABLE_STRATEGY_ORDER_FLOW: bool = _env_bool("ENABLE_STRATEGY_ORDER_FLOW", False)
    ENABLE_STRATEGY_OI_FUNDING: bool = _env_bool("ENABLE_STRATEGY_OI_FUNDING", False)
    ENABLE_STRATEGY_VWAP_MR: bool = _env_bool("ENABLE_STRATEGY_VWAP_MR", False)
    ENABLE_STRATEGY_VP_KEYLEVEL: bool = _env_bool("ENABLE_STRATEGY_VP_KEYLEVEL", False)
    ENABLE_STRATEGY_MTF_ALIGNMENT: bool = _env_bool(
        "ENABLE_STRATEGY_MTF_ALIGNMENT", False
    )
    # Confluence / correlation scoring
    ENABLE_CONFLUENCE_SCORING: bool = _env_bool("ENABLE_CONFLUENCE_SCORING", True)
    ENABLE_ASYNC_STRATEGY_SCAN: bool = _env_bool("ENABLE_ASYNC_STRATEGY_SCAN", False)
    CONFLUENCE_BONUS_MAX: float = _env_float("CONFLUENCE_BONUS_MAX", 8.0)
    CONFLUENCE_BONUS_PER_STRATEGY: float = _env_float(
        "CONFLUENCE_BONUS_PER_STRATEGY", 3.0
    )
    CORRELATION_LEVEL_TOLERANCE_ATR: float = _env_float(
        "CORRELATION_LEVEL_TOLERANCE_ATR", 0.35
    )
    CORRELATION_BONUS_PENALTY: float = _env_float("CORRELATION_BONUS_PENALTY", 0.25)
    # Institutional context & microstructure (Phase 3)
    ENABLE_INSTITUTIONAL_CONTEXT: bool = _env_bool("ENABLE_INSTITUTIONAL_CONTEXT", True)
    ENABLE_ASYNC_CONTEXT_EVAL: bool = _env_bool("ENABLE_ASYNC_CONTEXT_EVAL", True)
    ENABLE_CONTEXT_MTF: bool = _env_bool("ENABLE_CONTEXT_MTF", True)
    ENABLE_CONTEXT_VP: bool = _env_bool("ENABLE_CONTEXT_VP", True)
    ENABLE_CONTEXT_ORDER_FLOW: bool = _env_bool("ENABLE_CONTEXT_ORDER_FLOW", True)
    ENABLE_CONTEXT_OI_FUNDING: bool = _env_bool("ENABLE_CONTEXT_OI_FUNDING", True)
    INSTITUTIONAL_CONTEXT_BONUS_MAX: float = _env_float(
        "INSTITUTIONAL_CONTEXT_BONUS_MAX", 12.0
    )
    INSTITUTIONAL_CONTEXT_BONUS_PER_MODULE: float = _env_float(
        "INSTITUTIONAL_CONTEXT_BONUS_PER_MODULE", 3.5
    )
    INSTITUTIONAL_CONTEXT_MULT_MAX: float = _env_float(
        "INSTITUTIONAL_CONTEXT_MULT_MAX", 1.12
    )
    INSTITUTIONAL_CONTEXT_MULT_MIN: float = _env_float(
        "INSTITUTIONAL_CONTEXT_MULT_MIN", 0.88
    )
    INSTITUTIONAL_CONTEXT_MULT_BOOST: float = _env_float(
        "INSTITUTIONAL_CONTEXT_MULT_BOOST", 0.06
    )
    INSTITUTIONAL_CONTEXT_CONFLICT_PENALTY: float = _env_float(
        "INSTITUTIONAL_CONTEXT_CONFLICT_PENALTY", 0.06
    )
    CONTEXT_MODULE_MIN_SCORE: float = _env_float("CONTEXT_MODULE_MIN_SCORE", 55.0)
    MTF_ALIGNMENT_MIN_SCORE: float = _env_float("MTF_ALIGNMENT_MIN_SCORE", 68.0)
    VP_KEYLEVEL_MIN_SCORE: float = _env_float("VP_KEYLEVEL_MIN_SCORE", 65.0)
    ORDER_FLOW_MIN_SCORE: float = _env_float("ORDER_FLOW_MIN_SCORE", 68.0)
    OI_FUNDING_MIN_SCORE: float = _env_float("OI_FUNDING_MIN_SCORE", 65.0)
    VP_CONTEXT_BINS: int = _env_int("VP_CONTEXT_BINS", 24)
    VP_CONTEXT_LOOKBACK_BARS: int = _env_int("VP_CONTEXT_LOOKBACK_BARS", 96)
    VP_HVN_STD_MULT: float = _env_float("VP_HVN_STD_MULT", 1.0)
    VP_LVN_STD_MULT: float = _env_float("VP_LVN_STD_MULT", 0.5)
    VP_KEYLEVEL_ATR_TOLERANCE: float = _env_float("VP_KEYLEVEL_ATR_TOLERANCE", 0.35)
    ORDER_FLOW_LOOKBACK_BARS: int = _env_int("ORDER_FLOW_LOOKBACK_BARS", 20)
    ORDER_FLOW_DELTA_THRESHOLD: float = _env_float("ORDER_FLOW_DELTA_THRESHOLD", 0.12)
    ORDER_FLOW_VOL_SPIKE_MULT: float = _env_float("ORDER_FLOW_VOL_SPIKE_MULT", 1.8)
    ORDER_FLOW_AGGRESSIVE_BODY_PCT: float = _env_float(
        "ORDER_FLOW_AGGRESSIVE_BODY_PCT", 0.55
    )
    ORDER_FLOW_ABSORPTION_RANGE_ATR: float = _env_float(
        "ORDER_FLOW_ABSORPTION_RANGE_ATR", 0.45
    )
    OI_FUNDING_EXTREME_POSITIVE: float = _env_float("OI_FUNDING_EXTREME_POSITIVE", 0.0003)
    OI_FUNDING_EXTREME_NEGATIVE: float = _env_float("OI_FUNDING_EXTREME_NEGATIVE", -0.0003)
    OI_FUNDING_MODERATE: float = _env_float("OI_FUNDING_MODERATE", 0.0001)
    OI_BUILDUP_MIN_PCT: float = _env_float("OI_BUILDUP_MIN_PCT", 2.5)
    DERIVATIVES_CACHE_TTL_SECONDS: int = _env_int("DERIVATIVES_CACHE_TTL_SECONDS", 90)
    MAX_MTF_ALIGNMENT_POSITIONS: int = _env_int("MAX_MTF_ALIGNMENT_POSITIONS", 2)
    MAX_VP_KEYLEVEL_POSITIONS: int = _env_int("MAX_VP_KEYLEVEL_POSITIONS", 2)
    MAX_ORDER_FLOW_POSITIONS: int = _env_int("MAX_ORDER_FLOW_POSITIONS", 2)
    MAX_OI_FUNDING_POSITIONS: int = _env_int("MAX_OI_FUNDING_POSITIONS", 2)
    STRATEGY_PRIORITY_MTF_ALIGNMENT: float = _env_float("STRATEGY_PRIORITY_MTF_ALIGNMENT", 1.05)
    STRATEGY_PRIORITY_VP_KEYLEVEL: float = _env_float("STRATEGY_PRIORITY_VP_KEYLEVEL", 1.0)
    STRATEGY_PRIORITY_ORDER_FLOW: float = _env_float("STRATEGY_PRIORITY_ORDER_FLOW", 1.08)
    STRATEGY_PRIORITY_OI_FUNDING: float = _env_float("STRATEGY_PRIORITY_OI_FUNDING", 1.05)
    ALLOW_CROSS_STRATEGY_SCALE_IN: bool = _env_bool(
        "ALLOW_CROSS_STRATEGY_SCALE_IN", False
    )
    ENABLE_PORTFOLIO_ALLOCATOR: bool = _env_bool("ENABLE_PORTFOLIO_ALLOCATOR", True)
    ENABLE_GLOBAL_STRATEGY_KILL_SWITCH: bool = _env_bool(
        "ENABLE_GLOBAL_STRATEGY_KILL_SWITCH", True
    )
    USE_UNIFIED_SCAN_PIPELINE: bool = _env_bool("USE_UNIFIED_SCAN_PIPELINE", False)

    # ---------------- Event-driven scan & tier management ----------------
    ENABLE_EVENT_DRIVEN_SCAN: bool = _env_bool("ENABLE_EVENT_DRIVEN_SCAN", True)
    SCAN_TRIGGER_TIMEFRAMES: str = os.getenv("SCAN_TRIGGER_TIMEFRAMES", "5m,15m")
    TIER1_WATCHLIST_SIZE: int = _env_int("TIER1_WATCHLIST_SIZE", 100)
    HOT_SCAN_SIZE: int = _env_int("HOT_SCAN_SIZE", 20)
    HOT_SCAN_INTERVAL_SECONDS: float = _env_float("HOT_SCAN_INTERVAL_SECONDS", 20.0)
    BACKGROUND_SCAN_BATCH_SIZE: int = _env_int("BACKGROUND_SCAN_BATCH_SIZE", 16)
    BACKGROUND_SCAN_BATCH_DELAY_SECONDS: float = _env_float(
        "BACKGROUND_SCAN_BATCH_DELAY_SECONDS", 1.0
    )
    TIER2_HOT_SIZE: int = _env_int("TIER2_HOT_SIZE", 20)
    TIER2_PROMOTE_SCORE: float = _env_float("TIER2_PROMOTE_SCORE", 80.0)
    TIER2_PROMOTE_SCORE_TESTNET: float = _env_float("TIER2_PROMOTE_SCORE_TESTNET", 65.0)
    TIER2_DEMOTE_SCORE: float = _env_float("TIER2_DEMOTE_SCORE", 70.0)
    # Normalized Tier-2 gates: (raw - min) / (100 - min) * 100 — fair across strategies.
    TIER2_PROMOTE_NORMALIZED: float = _env_float("TIER2_PROMOTE_NORMALIZED", 70.0)
    TIER2_PROMOTE_NORMALIZED_TESTNET: float = _env_float(
        "TIER2_PROMOTE_NORMALIZED_TESTNET", 40.0
    )
    TIER2_DEMOTE_NORMALIZED: float = _env_float("TIER2_DEMOTE_NORMALIZED", 50.0)
    EVENT_EVAL_STAGGER_MS: float = _env_float("EVENT_EVAL_STAGGER_MS", 50.0)
    EVENT_CATCHUP_INTERVAL_SECONDS: int = _env_int("EVENT_CATCHUP_INTERVAL_SECONDS", 60)
    TIER1_REFRESH_INTERVAL_SECONDS: int = _env_int(
        "TIER1_REFRESH_INTERVAL_SECONDS", 1800
    )
    REST_BUDGET_WEIGHT_PER_MINUTE: int = _env_int("REST_BUDGET_WEIGHT_PER_MINUTE", 200)
    REST_BUDGET_MIN_REMAINING_FRACTION: float = _env_float(
        "REST_BUDGET_MIN_REMAINING_FRACTION", 0.20
    )
    VP_BREAKOUT_TOP_VOLUME_LIMIT: int = _env_int("VP_BREAKOUT_TOP_VOLUME_LIMIT", 20)
    VWAP_SESSION_ANCHOR_UTC: bool = _env_bool("VWAP_SESSION_ANCHOR_UTC", True)

    MAX_SMC_POSITIONS: int = _env_int("MAX_SMC_POSITIONS", 8)
    MAX_LSC_POSITIONS: int = _env_int("MAX_LSC_POSITIONS", 3)
    MAX_VWAP_POSITIONS: int = _env_int("MAX_VWAP_POSITIONS", 4)
    MAX_BREAKOUT_RETEST_POSITIONS: int = _env_int("MAX_BREAKOUT_RETEST_POSITIONS", 3)
    MAX_FALSE_BREAKOUT_SFP_POSITIONS: int = _env_int("MAX_FALSE_BREAKOUT_SFP_POSITIONS", 3)
    MAX_VOL_SQUEEZE_POSITIONS: int = _env_int("MAX_VOL_SQUEEZE_POSITIONS", 3)
    MAX_TREND_MOMENTUM_POSITIONS: int = _env_int("MAX_TREND_MOMENTUM_POSITIONS", 4)
    MAX_PRICE_ACTION_REVERSAL_POSITIONS: int = _env_int(
        "MAX_PRICE_ACTION_REVERSAL_POSITIONS", 3
    )
    MAX_VPB_POSITIONS: int = _env_int("MAX_VPB_POSITIONS", 3)
    MAX_VEMR_POSITIONS: int = _env_int("MAX_VEMR_POSITIONS", 2)
    MAX_VWAP_MR_POSITIONS: int = _env_int("MAX_VWAP_MR_POSITIONS", 2)

    STRATEGY_PRIORITY_SMC: float = _env_float("STRATEGY_PRIORITY_SMC", 1.0)
    STRATEGY_PRIORITY_RANGE: float = _env_float("STRATEGY_PRIORITY_RANGE", 0.8)
    STRATEGY_PRIORITY_LSC: float = _env_float("STRATEGY_PRIORITY_LSC", 0.95)
    STRATEGY_PRIORITY_VWAP: float = _env_float("STRATEGY_PRIORITY_VWAP", 0.9)
    STRATEGY_PRIORITY_VPB: float = _env_float("STRATEGY_PRIORITY_VPB", 0.85)
    STRATEGY_PRIORITY_VEMR: float = _env_float("STRATEGY_PRIORITY_VEMR", 0.7)
    STRATEGY_PRIORITY_BREAKOUT_RETEST: float = _env_float(
        "STRATEGY_PRIORITY_BREAKOUT_RETEST", 0.88
    )
    STRATEGY_PRIORITY_FALSE_BREAKOUT_SFP: float = _env_float(
        "STRATEGY_PRIORITY_FALSE_BREAKOUT_SFP", 0.86
    )
    STRATEGY_PRIORITY_VOL_SQUEEZE: float = _env_float("STRATEGY_PRIORITY_VOL_SQUEEZE", 0.84)
    STRATEGY_PRIORITY_TREND_MOMENTUM: float = _env_float(
        "STRATEGY_PRIORITY_TREND_MOMENTUM", 0.92
    )
    STRATEGY_PRIORITY_PRICE_ACTION_REVERSAL: float = _env_float(
        "STRATEGY_PRIORITY_PRICE_ACTION_REVERSAL", 0.82
    )
    STRATEGY_BUDGET_BREAKOUT_RETEST: float = _env_float(
        "STRATEGY_BUDGET_BREAKOUT_RETEST", 0.12
    )
    STRATEGY_BUDGET_FALSE_BREAKOUT_SFP: float = _env_float(
        "STRATEGY_BUDGET_FALSE_BREAKOUT_SFP", 0.10
    )
    STRATEGY_BUDGET_VOL_SQUEEZE: float = _env_float("STRATEGY_BUDGET_VOL_SQUEEZE", 0.10)
    STRATEGY_BUDGET_TREND_MOMENTUM: float = _env_float(
        "STRATEGY_BUDGET_TREND_MOMENTUM", 0.14
    )
    STRATEGY_BUDGET_PRICE_ACTION_REVERSAL: float = _env_float(
        "STRATEGY_BUDGET_PRICE_ACTION_REVERSAL", 0.10
    )

    STRATEGY_BUDGET_SMC: float = _env_float("STRATEGY_BUDGET_SMC", 0.35)
    STRATEGY_BUDGET_RANGE: float = _env_float("STRATEGY_BUDGET_RANGE", 0.10)
    STRATEGY_BUDGET_LSC: float = _env_float("STRATEGY_BUDGET_LSC", 0.15)
    STRATEGY_BUDGET_VWAP: float = _env_float("STRATEGY_BUDGET_VWAP", 0.20)
    STRATEGY_BUDGET_VPB: float = _env_float("STRATEGY_BUDGET_VPB", 0.15)
    STRATEGY_BUDGET_VEMR: float = _env_float("STRATEGY_BUDGET_VEMR", 0.05)
    STRATEGY_BUDGET_MTF_ALIGNMENT: float = _env_float(
        "STRATEGY_BUDGET_MTF_ALIGNMENT", 0.08
    )
    STRATEGY_BUDGET_VP_KEYLEVEL: float = _env_float(
        "STRATEGY_BUDGET_VP_KEYLEVEL", 0.08
    )
    STRATEGY_BUDGET_ORDER_FLOW: float = _env_float("STRATEGY_BUDGET_ORDER_FLOW", 0.08)
    STRATEGY_BUDGET_OI_FUNDING: float = _env_float("STRATEGY_BUDGET_OI_FUNDING", 0.08)
    STRATEGY_BUDGET_VWAP_MR: float = _env_float("STRATEGY_BUDGET_VWAP_MR", 0.05)

    MAX_ACCOUNT_MARGIN_UTILIZATION: float = _env_float(
        "MAX_ACCOUNT_MARGIN_UTILIZATION", 0.65
    )
    MIN_LIQUIDATION_BUFFER_PCT: float = _env_float("MIN_LIQUIDATION_BUFFER_PCT", 25.0)
    MAX_GROSS_EXPOSURE_PCT: float = _env_float("MAX_GROSS_EXPOSURE_PCT", 2.5)
    MAX_NET_LONG_EXPOSURE_PCT: float = _env_float("MAX_NET_LONG_EXPOSURE_PCT", 1.5)
    MAX_NET_SHORT_EXPOSURE_PCT: float = _env_float("MAX_NET_SHORT_EXPOSURE_PCT", 1.5)

    SMC_DAILY_MAX_LOSS_PERCENT: float = _env_float("SMC_DAILY_MAX_LOSS_PERCENT", 5.0)
    SMC_MAX_CONSECUTIVE_LOSSES: int = _env_int("SMC_MAX_CONSECUTIVE_LOSSES", 3)

    # ---------------- Market filters / universe ----------------
    DEFAULT_MEGA_CAP_BLACKLIST: list[str] = _env_csv_list(
        "DEFAULT_MEGA_CAP_BLACKLIST",
        ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"],
    )
    BLACKLIST_CACHE_SECONDS: float = _env_float("BLACKLIST_CACHE_SECONDS", 60.0)
    TOP_UNIVERSE_POOL_SIZE: int = _env_int("TOP_UNIVERSE_POOL_SIZE", 60)
    ROTATION_EXTENDED_POOL_SIZE: int = _env_int("ROTATION_EXTENDED_POOL_SIZE", 60)
    ROTATION_EVALUATED_MEMORY_MIN_MINUTES: int = _env_int(
        "ROTATION_EVALUATED_MEMORY_MIN_MINUTES", 45
    )
    ROTATION_EVALUATED_MEMORY_MAX_MINUTES: int = _env_int(
        "ROTATION_EVALUATED_MEMORY_MAX_MINUTES", 60
    )
    ROTATION_MEMORY_PURGE_HOURS: float = _env_float("ROTATION_MEMORY_PURGE_HOURS", 3.5)
    ENABLE_DYNAMIC_SYMBOL_ROTATION: bool = _env_bool(
        "ENABLE_DYNAMIC_SYMBOL_ROTATION", True
    )
    UNIVERSE_RANK_VOLUME_WEIGHT: float = _env_float("UNIVERSE_RANK_VOLUME_WEIGHT", 0.40)
    UNIVERSE_RANK_RANGE_WEIGHT: float = _env_float("UNIVERSE_RANK_RANGE_WEIGHT", 0.35)
    UNIVERSE_RANK_ATR_WEIGHT: float = _env_float("UNIVERSE_RANK_ATR_WEIGHT", 0.25)
    OPPORTUNITY_MOMENTUM_WEIGHT: float = _env_float(
        "OPPORTUNITY_MOMENTUM_WEIGHT", 0.40
    )
    OPPORTUNITY_REVERSAL_WEIGHT: float = _env_float(
        "OPPORTUNITY_REVERSAL_WEIGHT", 0.30
    )
    OPPORTUNITY_STRUCTURE_WEIGHT: float = _env_float(
        "OPPORTUNITY_STRUCTURE_WEIGHT", 0.30
    )
    OPPORTUNITY_CANDIDATE_MIN: float = _env_float("OPPORTUNITY_CANDIDATE_MIN", 35.0)
    OPPORTUNITY_WATCH_MIN: float = _env_float("OPPORTUNITY_WATCH_MIN", 50.0)
    OPPORTUNITY_ACTIVE_MIN: float = _env_float("OPPORTUNITY_ACTIVE_MIN", 60.0)
    OPPORTUNITY_HOT_MIN: float = _env_float("OPPORTUNITY_HOT_MIN", 72.0)
    OPPORTUNITY_SETUP_MIN: float = _env_float("OPPORTUNITY_SETUP_MIN", 78.0)
    OPPORTUNITY_DORMANT_MAX: float = _env_float("OPPORTUNITY_DORMANT_MAX", 22.0)
    OPPORTUNITY_WEAKEN_DROP: float = _env_float("OPPORTUNITY_WEAKEN_DROP", 15.0)
    OPPORTUNITY_SCORE_HALF_LIFE_MINUTES: float = _env_float(
        "OPPORTUNITY_SCORE_HALF_LIFE_MINUTES", 20.0
    )
    OPPORTUNITY_STALE_SECONDS: float = _env_float("OPPORTUNITY_STALE_SECONDS", 2700.0)
    HOT_SCORE_VELOCITY: float = _env_float("HOT_SCORE_VELOCITY", 12.0)
    HOT_PRICE_MOVE_PCT: float = _env_float("HOT_PRICE_MOVE_PCT", 0.85)
    HOT_VOLUME_RATIO: float = _env_float("HOT_VOLUME_RATIO", 1.18)
    HOT_RANGE_EXPANSION_PCT: float = _env_float("HOT_RANGE_EXPANSION_PCT", 0.35)
    HOT_FAST_TRACK_MAX_PER_CYCLE: int = _env_int("HOT_FAST_TRACK_MAX_PER_CYCLE", 8)
    ROTATING_SCAN_BATCH_SIZE: int = _env_int("ROTATING_SCAN_BATCH_SIZE", 20)
    MIN_24H_VOLUME_USDT: float = _env_float("MIN_24H_VOLUME_USDT", 10_000_000.0)
    MAX_SPREAD_PERCENT: float = _env_float("MAX_SPREAD_PERCENT", 0.08)
    MIN_SCAN_UNIVERSE: int = _env_int("MIN_SCAN_UNIVERSE", 50)
    MIN_24H_RANGE_PCT: float = _env_float("MIN_24H_RANGE_PCT", 0.75)
    MIN_UNIVERSE_ATR_PCT: float = _env_float("MIN_UNIVERSE_ATR_PCT", 0.08)
    UNIVERSE_ATR_LOOKBACK_BARS: int = _env_int("UNIVERSE_ATR_LOOKBACK_BARS", 20)
    ENABLE_UNIVERSE_ATR_FILTER: bool = _env_bool("ENABLE_UNIVERSE_ATR_FILTER", True)
    UNIVERSE_LOG_SYMBOL_PREVIEW: int = _env_int("UNIVERSE_LOG_SYMBOL_PREVIEW", 15)
    ENABLE_UNIVERSE_FILTER_RELAXATION: bool = _env_bool(
        "ENABLE_UNIVERSE_FILTER_RELAXATION", True
    )
    TESTNET_RELAX_UNIVERSE_FILTERS: bool = _env_bool(
        "TESTNET_RELAX_UNIVERSE_FILTERS", True
    )
    TESTNET_RELAX_STRATEGY_THRESHOLDS: bool = _env_bool(
        "TESTNET_RELAX_STRATEGY_THRESHOLDS", True
    )
    TESTNET_MIN_SCORE_RELAX: float = _env_float("TESTNET_MIN_SCORE_RELAX", 10.0)
    TESTNET_MIN_24H_VOLUME_USDT: float = _env_float("TESTNET_MIN_24H_VOLUME_USDT", 0.0)
    TESTNET_MIN_24H_RANGE_PCT: float = _env_float("TESTNET_MIN_24H_RANGE_PCT", 0.0)
    TESTNET_DISABLE_UNIVERSE_ATR_FILTER: bool = _env_bool(
        "TESTNET_DISABLE_UNIVERSE_ATR_FILTER", True
    )
    RELAXED_MIN_24H_VOLUME_USDT: float = _env_float(
        "RELAXED_MIN_24H_VOLUME_USDT", 1_000_000.0
    )
    RELAXED_MIN_24H_RANGE_PCT: float = _env_float("RELAXED_MIN_24H_RANGE_PCT", 0.1)
    UNIVERSE_RELAXED_MIN_SYMBOLS: int = _env_int("UNIVERSE_RELAXED_MIN_SYMBOLS", 30)
    UNIVERSE_FALLBACK_TOP_N: int = _env_int("UNIVERSE_FALLBACK_TOP_N", 50)
    ENABLE_WS_BOOK_STREAM: bool = _env_bool("ENABLE_WS_BOOK_STREAM", True)
    WATCHLIST_SCORE: float = _env_float("WATCHLIST_SCORE", 70.0)
    BLACKLIST_SCORE: float = _env_float("BLACKLIST_SCORE", 40.0)

    # ---------------- Scanner ----------------
    MAX_WORKERS: int = _env_int("MAX_WORKERS", 1)
    SCAN_TIMEOUT_SEC: int = _env_int("SCAN_TIMEOUT_SEC", 120)
    SCAN_PAIR_DELAY_SECONDS: float = _env_float("SCAN_PAIR_DELAY_SECONDS", 0.25)
    SCAN_TIMEFRAME_DELAY_SECONDS: float = _env_float("SCAN_TIMEFRAME_DELAY_SECONDS", 0.05)
    CANDLE_FETCH_LIMIT: int = _env_int("CANDLE_FETCH_LIMIT", 280)

    # ---------------- Ops / VPS ----------------
    HEARTBEAT_SECONDS: int = _env_int("HEARTBEAT_SECONDS", 30)
    MONITOR_HEARTBEAT_SECONDS: int = _env_int("MONITOR_HEARTBEAT_SECONDS", 5)

    # ---------------- Watchdog (watchdog.py) ----------------
    WATCHDOG_INTERVAL_SECONDS: int = _env_int("WATCHDOG_INTERVAL_SECONDS", 30)
    WATCHDOG_BREACH_GRACE_SECONDS: int = _env_int("WATCHDOG_BREACH_GRACE_SECONDS", 60)
    WATCHDOG_MAIN_STALE_SECONDS: int = _env_int("WATCHDOG_MAIN_STALE_SECONDS", 60)
    WATCHDOG_AUTO_RESTART_MAIN: bool = _env_bool("WATCHDOG_AUTO_RESTART_MAIN", True)
    WATCHDOG_EMERGENCY_ONLY_WHEN_MAIN_DOWN: bool = _env_bool(
        "WATCHDOG_EMERGENCY_ONLY_WHEN_MAIN_DOWN", True
    )
    WATCHDOG_MAIN_SCRIPT: str = os.getenv("WATCHDOG_MAIN_SCRIPT", "main.py")
    ENABLE_DAILY_REPORT: bool = _env_bool("ENABLE_DAILY_REPORT", True)
    ENABLE_WEEKLY_REPORT: bool = _env_bool("ENABLE_WEEKLY_REPORT", True)
    ENABLE_MONTHLY_REPORT: bool = _env_bool("ENABLE_MONTHLY_REPORT", True)
    MAX_SCAN_UNIVERSE: int = _env_int("MAX_SCAN_UNIVERSE", 60)
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
    WATCHDOG_HEARTBEAT_FILE: str = os.path.join(
        DATA_DIR, os.getenv("WATCHDOG_HEARTBEAT_FILE", "bot_heartbeat.json")
    )
    WATCHDOG_STATE_FILE: str = os.path.join(
        DATA_DIR, os.getenv("WATCHDOG_STATE_FILE", "watchdog_state.json")
    )
    WATCHDOG_FORCE_REST: bool = _env_bool("WATCHDOG_FORCE_REST", False)
    EXIT_CLAIM_TTL_SECONDS: int = _env_int("EXIT_CLAIM_TTL_SECONDS", 120)

    @classmethod
    def is_mega_cap_blacklisted(cls, symbol: str) -> bool:
        return symbol.upper() in cls.DEFAULT_MEGA_CAP_BLACKLIST

    @classmethod
    def rest_ip_request_limit(cls) -> int:
        """Binance IP HTTP-request cap (not weight). Testnet reports 6000/min."""
        if cls.USE_TESTNET:
            return max(int(cls.REST_IP_REQUEST_LIMIT_TESTNET), 1)
        return max(int(cls.REST_IP_REQUEST_LIMIT_MAINNET), 1)

    @classmethod
    def testnet_strategy_relax(cls) -> bool:
        """True when Testnet should use looser entry floors (quiet ticker streams)."""
        return bool(cls.USE_TESTNET and cls.TESTNET_RELAX_STRATEGY_THRESHOLDS)

    @classmethod
    def effective_min_score(cls, base: float) -> float:
        """Strategy execution floor; Testnet subtracts TESTNET_MIN_SCORE_RELAX."""
        value = float(base)
        if cls.testnet_strategy_relax():
            value = max(50.0, value - float(cls.TESTNET_MIN_SCORE_RELAX))
        return value

    @classmethod
    def vwap_max_distance_atr(cls) -> float:
        if cls.testnet_strategy_relax():
            return float(cls.VWAP_MAX_DISTANCE_ATR_TESTNET)
        return float(cls.VWAP_MAX_DISTANCE_ATR)

    @classmethod
    def tier2_promote_score(cls) -> float:
        if cls.testnet_strategy_relax():
            return float(min(cls.TIER2_PROMOTE_SCORE, cls.TIER2_PROMOTE_SCORE_TESTNET))
        return float(cls.TIER2_PROMOTE_SCORE)

    @classmethod
    def tier2_promote_normalized(cls) -> float:
        if cls.testnet_strategy_relax():
            return float(
                min(cls.TIER2_PROMOTE_NORMALIZED, cls.TIER2_PROMOTE_NORMALIZED_TESTNET)
            )
        return float(cls.TIER2_PROMOTE_NORMALIZED)

    @classmethod
    def get_scan_kline_intervals(cls) -> list[str]:
        """All timeframes required for indicator snapshots (REST bootstrap + cache)."""
        return [cls.ENTRY_TIMEFRAME, cls.CONFIRM_TIMEFRAME, cls.TREND_TIMEFRAME]

    @classmethod
    def get_scan_trigger_timeframes(cls) -> list[str]:
        """Timeframes that trigger event-driven evaluation on candle close."""
        if cls.SCAN_TRIGGER_TIMEFRAMES.strip():
            return [
                part.strip()
                for part in cls.SCAN_TRIGGER_TIMEFRAMES.split(",")
                if part.strip()
            ]
        return [cls.ENTRY_TIMEFRAME, cls.CONFIRM_TIMEFRAME]

    @classmethod
    def get_ws_kline_intervals(cls) -> list[str]:
        """Timeframes subscribed live via WebSocket (keep total streams under limit)."""
        if cls.WS_KLINE_WS_TIMEFRAMES.strip():
            return [
                part.strip()
                for part in cls.WS_KLINE_WS_TIMEFRAMES.split(",")
                if part.strip()
            ]
        if cls.ENABLE_EVENT_DRIVEN_SCAN:
            return cls.get_scan_trigger_timeframes()
        if cls.WS_KLINE_LIVE_TIMEFRAMES_ONLY:
            return [cls.ENTRY_TIMEFRAME]
        return cls.get_scan_kline_intervals()

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
