"""Rotating file loggers with console output."""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler

from config import Config


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
