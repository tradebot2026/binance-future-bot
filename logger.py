"""Rotating file loggers with console output."""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

from config import Config
from utils import utc_now

_LOG_LINE_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def setup_logger(name: str, log_file: str, level: int | None = None) -> logging.Logger:
    if level is None:
        level = getattr(logging, Config.LOG_LEVEL, logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    os.makedirs(Config.LOGS_DIR, exist_ok=True)
    file_path = os.path.join(Config.LOGS_DIR, log_file)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if logger.handlers:
        return logger

    file_handler = RotatingFileHandler(
        file_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


system_logger = setup_logger("System", "system.log")
scanner_logger = setup_logger("Scanner", "scanner.log")
trade_logger = setup_logger("Trade", "trades.log")
signal_logger = setup_logger("Signal", "signals.log")
performance_logger = setup_logger("Performance", "performance.log")
error_logger = setup_logger("Error", "errors.log", level=logging.WARNING)


def log_strategy_approved(
    symbol: str,
    action: str,
    strategy: str,
    score: float,
    extra: str = "",
) -> None:
    """Strategy/risk gate only — does not mean a Binance order was sent."""
    tail = f" | {extra}" if extra else ""
    signal_logger.info(
        "[TRADE_APPROVED] %s %s | strategy=%s | score=%.1f | "
        "(strategy gate only — no Binance order yet)%s",
        symbol,
        action,
        strategy,
        score,
        tail,
    )


def error_log_cutoff(max_age_hours: float | None = None) -> datetime:
    """UTC cutoff for Telegram /errors reports (default last 48 hours)."""
    hours = (
        Config.TELEGRAM_ERROR_LOG_MAX_AGE_HOURS
        if max_age_hours is None
        else max_age_hours
    )
    return utc_now() - timedelta(hours=max(hours, 0.0))


def parse_log_line_timestamp(line: str) -> datetime | None:
    """Parse leading `YYYY-MM-DD HH:MM:SS` from a logger line as UTC."""
    match = _LOG_LINE_TS.match(line)
    if not match:
        return None
    try:
        parsed = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def read_recent_error_log_lines(
    *,
    max_age_hours: float | None = None,
    limit: int = 16,
    include_rotated: bool = True,
) -> list[str]:
    """
    Return the newest in-window lines from errors.log (and rotated backups).
    Stack-trace continuation lines stay attached to the preceding in-window event.
    """
    cutoff = error_log_cutoff(max_age_hours)
    names = ["errors.log"]
    if include_rotated:
        names.extend(f"errors.log.{idx}" for idx in range(1, 6))

    kept: list[str] = []
    for name in reversed(names):
        path = os.path.join(Config.LOGS_DIR, name)
        if not os.path.isfile(path):
            continue
        in_window = False
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as handle:
                for raw in handle:
                    line = raw.rstrip("\n")
                    if not line:
                        continue
                    ts = parse_log_line_timestamp(line)
                    if ts is not None:
                        in_window = ts >= cutoff
                    if in_window:
                        kept.append(line)
        except OSError:
            continue

    if limit > 0:
        return kept[-limit:]
    return kept

